package liteserver

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xssnick/gton/internal/ratelimit"
)

const (
	requestLimitPruneInterval           = time.Second
	liteserverConnectionCleanupInterval = 5 * time.Second
)

type RequestLimitOptions struct {
	CapacityPerIP       int
	CoolingPerSec       float64
	MaxConnectionsPerIP int
	MaxKeepAlive        time.Duration
	// MaxWaitsPerIP caps parked waitMasterchainSeqno requests per client
	// address. Zero means the default of 256.
	MaxWaitsPerIP int
}

func validateRequestLimitOptions(opts RequestLimitOptions) error {
	if opts.CapacityPerIP < 0 {
		return fmt.Errorf("liteserver request capacity per IP cannot be negative")
	}
	if opts.CoolingPerSec < 0 || math.IsNaN(opts.CoolingPerSec) || math.IsInf(opts.CoolingPerSec, 0) {
		return fmt.Errorf("liteserver request cooling per second must be finite and non-negative")
	}
	if opts.MaxConnectionsPerIP < 0 {
		return fmt.Errorf("liteserver max connections per IP cannot be negative")
	}
	if opts.MaxKeepAlive < 0 {
		return fmt.Errorf("liteserver max keep alive cannot be negative")
	}
	if opts.MaxWaitsPerIP < 0 {
		return fmt.Errorf("liteserver max waits per IP cannot be negative")
	}
	if (opts.CapacityPerIP == 0) != (opts.CoolingPerSec == 0) {
		return fmt.Errorf("liteserver request capacity per IP and cooling per second must be configured together")
	}
	return nil
}

// requestLimiter is the per-client-IP budget. The mechanism lives in
// internal/ratelimit; this type keeps the liteserver's own vocabulary
// (capacity/cooling, a nil limiter meaning disabled) over it.
type requestLimiter struct {
	limiter *ratelimit.Limiter
}

func newRequestLimiter(opts RequestLimitOptions) *requestLimiter {
	if opts.CapacityPerIP == 0 {
		return nil
	}
	limiter := ratelimit.New(opts.CoolingPerSec, opts.CapacityPerIP)
	if limiter == nil {
		return nil
	}
	return &requestLimiter{limiter: limiter}
}

func (l *requestLimiter) allow(ip string, now time.Time) bool {
	return l.limiter.Allow(ip, now)
}

func (l *requestLimiter) prune(now time.Time) int {
	return l.limiter.Prune(now)
}

type trackedLiteClient interface {
	IP() string
	Port() uint16
	Close()
}

type clientConnectionTracker struct {
	mx                  sync.Mutex
	maxConnectionsPerIP int
	maxKeepAlive        time.Duration
	ips                 map[string]*clientIPConnections
	connections         sync.Map
}

type clientIPConnections struct {
	active map[uint16]*trackedClientConnection
}

type clientConnectionKey struct {
	ip   string
	port uint16
}

type trackedClientConnection struct {
	client           trackedLiteClient
	lastRequestNanos atomic.Int64
	inflight         atomic.Int32
	closing          atomic.Bool
}

func newClientConnectionTracker(opts RequestLimitOptions) *clientConnectionTracker {
	if opts.MaxConnectionsPerIP == 0 && opts.MaxKeepAlive == 0 {
		return nil
	}
	return &clientConnectionTracker{
		maxConnectionsPerIP: opts.MaxConnectionsPerIP,
		maxKeepAlive:        opts.MaxKeepAlive,
		ips:                 map[string]*clientIPConnections{},
	}
}

func (t *clientConnectionTracker) keepAliveEnabled() bool {
	return t.maxKeepAlive > 0
}

func (t *clientConnectionTracker) accept(client trackedLiteClient, now time.Time) (int, error) {
	t.mx.Lock()
	defer t.mx.Unlock()

	ip := client.IP()
	info := t.ips[ip]
	if info == nil {
		info = &clientIPConnections{active: map[uint16]*trackedClientConnection{}}
		t.ips[ip] = info
	}
	if t.maxConnectionsPerIP > 0 && len(info.active) >= t.maxConnectionsPerIP {
		return len(info.active), fmt.Errorf("too many connections")
	}

	key := clientConnectionKey{ip: ip, port: client.Port()}
	conn := &trackedClientConnection{client: client}
	conn.lastRequestNanos.Store(now.UnixNano())
	info.active[key.port] = conn
	if t.keepAliveEnabled() {
		t.connections.Store(key, conn)
	}
	return len(info.active), nil
}

func (t *clientConnectionTracker) disconnect(client trackedLiteClient) int {
	t.mx.Lock()
	defer t.mx.Unlock()

	key := clientConnectionKey{ip: client.IP(), port: client.Port()}
	ip := key.ip
	info := t.ips[ip]
	if info == nil {
		return 0
	}

	conn := info.active[key.port]
	delete(info.active, key.port)
	if conn != nil && t.keepAliveEnabled() {
		t.connections.Delete(key)
	}
	remaining := len(info.active)
	if remaining == 0 {
		delete(t.ips, ip)
	}
	return remaining
}

func (t *clientConnectionTracker) beginRequest(client trackedLiteClient, now time.Time) {
	conn := t.connection(client)
	if conn == nil || conn.closing.Load() {
		return
	}
	conn.lastRequestNanos.Store(now.UnixNano())
	conn.inflight.Add(1)
	if conn.closing.Load() {
		conn.inflight.Add(-1)
	}
}

func (t *clientConnectionTracker) endRequest(client trackedLiteClient, now time.Time) {
	conn := t.connection(client)
	if conn == nil {
		return
	}
	conn.lastRequestNanos.Store(now.UnixNano())
	for {
		inflight := conn.inflight.Load()
		if inflight <= 0 {
			return
		}
		if conn.inflight.CompareAndSwap(inflight, inflight-1) {
			return
		}
	}
}

func (t *clientConnectionTracker) connection(client trackedLiteClient) *trackedClientConnection {
	loaded, ok := t.connections.Load(clientConnectionKey{ip: client.IP(), port: client.Port()})
	if !ok {
		return nil
	}
	return loaded.(*trackedClientConnection)
}

func (t *clientConnectionTracker) closeIdle(now time.Time) int {
	var stale []trackedLiteClient
	cutoffNanos := now.Add(-t.maxKeepAlive).UnixNano()
	t.connections.Range(func(_, value any) bool {
		conn := value.(*trackedClientConnection)
		if conn.inflight.Load() > 0 || conn.lastRequestNanos.Load() >= cutoffNanos {
			return true
		}
		if conn.closing.CompareAndSwap(false, true) {
			stale = append(stale, conn.client)
		}
		return true
	})

	for _, client := range stale {
		client.Close()
	}
	return len(stale)
}
