package liteserver

import (
	"fmt"
	"math"
	"sync"
	"time"
)

const (
	requestLimitIdleTTL                 = 10 * time.Minute
	requestLimitPruneInterval           = time.Minute
	liteserverConnectionCleanupInterval = 5 * time.Second
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
	mx               sync.Mutex
	capacity         float64
	coolingPerSecond float64
	buckets          map[string]*requestLimitBucket
	lastPrune        time.Time
}

type requestLimitBucket struct {
	tokens  float64
	updated time.Time
	seenAt  time.Time
}

func newRequestLimiter(opts RequestLimitOptions) *requestLimiter {
	if opts.CapacityPerIP == 0 {
		return nil
	}
	return &requestLimiter{
		capacity:         float64(opts.CapacityPerIP),
		coolingPerSecond: opts.CoolingPerSec,
		buckets:          map[string]*requestLimitBucket{},
	}
}

func (l *requestLimiter) allow(ip string, now time.Time) bool {
	l.mx.Lock()
	defer l.mx.Unlock()

	l.pruneLocked(now)

	bucket := l.buckets[ip]
	if bucket == nil {
		bucket = &requestLimitBucket{
			tokens:  l.capacity,
			updated: now,
			seenAt:  now,
		}
		l.buckets[ip] = bucket
	}

	l.refillLocked(bucket, now)
	bucket.seenAt = now
	if bucket.tokens < 1 {
		return false
	}

	bucket.tokens--
	return true
}

func (l *requestLimiter) refillLocked(bucket *requestLimitBucket, now time.Time) {
	elapsed := now.Sub(bucket.updated).Seconds()
	if elapsed > 0 {
		bucket.tokens += elapsed * l.coolingPerSecond
		if bucket.tokens > l.capacity {
			bucket.tokens = l.capacity
		}
		bucket.updated = now
	}
}

func (l *requestLimiter) pruneLocked(now time.Time) {
	if !l.lastPrune.IsZero() && now.Sub(l.lastPrune) < requestLimitPruneInterval {
		return
	}
	l.lastPrune = now

	for ip, bucket := range l.buckets {
		l.refillLocked(bucket, now)
		if bucket.tokens >= l.capacity && now.Sub(bucket.seenAt) >= requestLimitIdleTTL {
			delete(l.buckets, ip)
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
}

type clientIPConnections struct {
	active map[uint16]*trackedClientConnection
}

type trackedClientConnection struct {
	client      trackedLiteClient
	lastRequest time.Time
	closing     bool
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

	info.active[client.Port()] = &trackedClientConnection{
		client:      client,
		lastRequest: now,
	}
	return len(info.active), nil
}

func (t *clientConnectionTracker) disconnect(client trackedLiteClient) int {
	t.mx.Lock()
	defer t.mx.Unlock()

	ip := client.IP()
	info := t.ips[ip]
	if info == nil {
		return 0
	}

	delete(info.active, client.Port())
	remaining := len(info.active)
	if remaining == 0 {
		delete(t.ips, ip)
	}
	return remaining
}

func (t *clientConnectionTracker) markRequest(client trackedLiteClient, now time.Time) {
	t.mx.Lock()
	defer t.mx.Unlock()

	info := t.ips[client.IP()]
	if info == nil {
		return
	}
	conn := info.active[client.Port()]
	if conn == nil {
		return
	}
	conn.lastRequest = now
}

func (t *clientConnectionTracker) closeIdle(now time.Time) int {
	t.mx.Lock()
	var stale []trackedLiteClient
	cutoff := now.Add(-t.maxKeepAlive)
	for _, info := range t.ips {
		for _, conn := range info.active {
			if conn.closing || !conn.lastRequest.Before(cutoff) {
				continue
			}
			conn.closing = true
			stale = append(stale, conn.client)
		}
	}
	t.mx.Unlock()

	for _, client := range stale {
		client.Close()
	}
	return len(stale)
}
