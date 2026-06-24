package liteserver

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

const (
	requestLimitPruneInterval           = time.Second
	liteserverConnectionCleanupInterval = 5 * time.Second
	maxRequestLimitNanos                = int64(1<<63 - 1)
)

type RequestLimitOptions struct {
	CapacityPerIP       int
	CoolingPerSec       float64
	MaxConnectionsPerIP int
	MaxKeepAlive        time.Duration
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
	if (opts.CapacityPerIP == 0) != (opts.CoolingPerSec == 0) {
		return fmt.Errorf("liteserver request capacity per IP and cooling per second must be configured together")
	}
	return nil
}

type requestLimiter struct {
	requestNanos int64
	burstNanos   int64
	buckets      sync.Map
}

type requestLimitBucket struct {
	until   atomic.Int64
	deleted atomic.Bool
}

func newRequestLimiter(opts RequestLimitOptions) *requestLimiter {
	if opts.CapacityPerIP == 0 {
		return nil
	}
	requestNanos := requestLimitCostNanos(opts.CoolingPerSec)
	return &requestLimiter{
		requestNanos: requestNanos,
		burstNanos:   requestLimitBurstNanos(requestNanos, opts.CapacityPerIP),
	}
}

func (l *requestLimiter) allow(ip string, now time.Time) bool {
	nowNanos := now.UnixNano()
	for {
		bucket := l.bucket(ip)
		if bucket.deleted.Load() {
			l.buckets.CompareAndDelete(ip, bucket)
			continue
		}

		allowed, until := l.reserve(bucket, nowNanos)
		if !allowed {
			return false
		}
		if !bucket.deleted.Load() {
			return true
		}

		l.buckets.CompareAndDelete(ip, bucket)
		replacement := &requestLimitBucket{}
		replacement.until.Store(until)
		loaded, stored := l.buckets.LoadOrStore(ip, replacement)
		if !stored {
			return true
		}
		active := loaded.(*requestLimitBucket)
		if !active.deleted.Load() {
			raiseRequestLimitBucketUntil(active, until)
			return true
		}
	}
}

func (l *requestLimiter) bucket(ip string) *requestLimitBucket {
	loaded, ok := l.buckets.Load(ip)
	if ok {
		return loaded.(*requestLimitBucket)
	}

	bucket := &requestLimitBucket{}
	loaded, ok = l.buckets.LoadOrStore(ip, bucket)
	if ok {
		return loaded.(*requestLimitBucket)
	}
	return bucket
}

func (l *requestLimiter) reserve(bucket *requestLimitBucket, nowNanos int64) (bool, int64) {
	for {
		until := bucket.until.Load()
		base := until
		if base < nowNanos {
			base = nowNanos
		}
		if base > maxRequestLimitNanos-l.requestNanos {
			return false, until
		}

		next := base + l.requestNanos
		if next-nowNanos > l.burstNanos {
			return false, until
		}
		if bucket.until.CompareAndSwap(until, next) {
			return true, next
		}
	}
}

func (l *requestLimiter) prune(now time.Time) int {
	nowNanos := now.UnixNano()
	deleted := 0
	l.buckets.Range(func(key, value any) bool {
		bucket := value.(*requestLimitBucket)
		if bucket.until.Load() > nowNanos {
			return true
		}
		if !bucket.deleted.CompareAndSwap(false, true) {
			return true
		}
		if l.buckets.CompareAndDelete(key, bucket) {
			deleted++
		}
		return true
	})
	return deleted
}

func requestLimitCostNanos(coolingPerSecond float64) int64 {
	if coolingPerSecond <= 0 || math.IsNaN(coolingPerSecond) {
		return maxRequestLimitNanos
	}
	if math.IsInf(coolingPerSecond, 1) {
		return 1
	}

	nanos := math.Ceil(float64(time.Second) / coolingPerSecond)
	if nanos < 1 {
		return 1
	}
	if nanos >= float64(maxRequestLimitNanos) {
		return maxRequestLimitNanos
	}
	return int64(nanos)
}

func requestLimitBurstNanos(requestNanos int64, capacity int) int64 {
	if requestNanos <= 0 || capacity <= 0 {
		return 0
	}
	capacityNanos := int64(capacity)
	if requestNanos > maxRequestLimitNanos/capacityNanos {
		return maxRequestLimitNanos
	}
	return requestNanos * capacityNanos
}

func raiseRequestLimitBucketUntil(bucket *requestLimitBucket, until int64) {
	for {
		current := bucket.until.Load()
		if current >= until || bucket.deleted.Load() {
			return
		}
		if bucket.until.CompareAndSwap(current, until) {
			return
		}
	}
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
