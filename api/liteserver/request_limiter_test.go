package liteserver

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestRequestLimiterLimitsPerIP(t *testing.T) {
	limiter := newRequestLimiter(RequestLimitOptions{
		CapacityPerIP: 2,
		CoolingPerSec: 1,
	})
	now := time.Unix(100, 0)

	if !limiter.allow("203.0.113.1", now) {
		t.Fatal("first request should be allowed")
	}
	if !limiter.allow("203.0.113.1", now) {
		t.Fatal("second request should be allowed")
	}
	if limiter.allow("203.0.113.1", now) {
		t.Fatal("third request should be rate limited")
	}
	if !limiter.allow("203.0.113.2", now) {
		t.Fatal("different IP should have an independent bucket")
	}
	if !limiter.allow("203.0.113.1", now.Add(time.Second)) {
		t.Fatal("request should be allowed after refill")
	}
}

func TestRequestLimiterPrunesIdleBuckets(t *testing.T) {
	limiter := newRequestLimiter(RequestLimitOptions{
		CapacityPerIP: 1,
		CoolingPerSec: 1,
	})
	now := time.Unix(100, 0)

	if !limiter.allow("203.0.113.1", now) {
		t.Fatal("request should be allowed")
	}
	if count := requestLimiterBucketCount(limiter); count != 1 {
		t.Fatalf("unexpected bucket count %d", count)
	}

	if deleted := limiter.prune(now.Add(time.Second)); deleted != 1 {
		t.Fatalf("unexpected deleted bucket count %d", deleted)
	}
	if count := requestLimiterBucketCount(limiter); count != 0 {
		t.Fatalf("unexpected bucket count after prune %d", count)
	}
}

func TestRequestLimiterKeepsActiveBuckets(t *testing.T) {
	limiter := newRequestLimiter(RequestLimitOptions{
		CapacityPerIP: 2,
		CoolingPerSec: 1,
	})
	now := time.Unix(100, 0)

	if !limiter.allow("203.0.113.1", now) {
		t.Fatal("request should be allowed")
	}
	if deleted := limiter.prune(now.Add(500 * time.Millisecond)); deleted != 0 {
		t.Fatalf("unexpected deleted bucket count %d", deleted)
	}
	if count := requestLimiterBucketCount(limiter); count != 1 {
		t.Fatalf("unexpected bucket count after prune %d", count)
	}
}

func TestRequestLimiterConcurrentSameIPHonorsCapacity(t *testing.T) {
	const capacity = 16

	limiter := newRequestLimiter(RequestLimitOptions{
		CapacityPerIP: capacity,
		CoolingPerSec: 1,
	})
	now := time.Unix(100, 0)
	start := make(chan struct{})
	done := make(chan bool, 128)

	for i := 0; i < cap(done); i++ {
		go func() {
			<-start
			done <- limiter.allow("203.0.113.1", now)
		}()
	}
	close(start)

	allowed := 0
	for i := 0; i < cap(done); i++ {
		if <-done {
			allowed++
		}
	}
	if allowed != capacity {
		t.Fatalf("unexpected allowed requests %d, want %d", allowed, capacity)
	}
}

func TestClientConnectionTrackerLimitsPerIP(t *testing.T) {
	tracker := newClientConnectionTracker(RequestLimitOptions{MaxConnectionsPerIP: 2})
	now := time.Unix(100, 0)
	first := &fakeTrackedLiteClient{ip: "203.0.113.1", port: 1}
	second := &fakeTrackedLiteClient{ip: "203.0.113.1", port: 2}
	third := &fakeTrackedLiteClient{ip: "203.0.113.1", port: 3}
	otherIP := &fakeTrackedLiteClient{ip: "203.0.113.2", port: 1}

	if n, err := tracker.accept(first, now); err != nil || n != 1 {
		t.Fatalf("accept first: n=%d err=%v", n, err)
	}
	if n, err := tracker.accept(second, now); err != nil || n != 2 {
		t.Fatalf("accept second: n=%d err=%v", n, err)
	}
	if _, err := tracker.accept(third, now); err == nil {
		t.Fatal("expected third connection from same IP to fail")
	}
	if n, err := tracker.accept(otherIP, now); err != nil || n != 1 {
		t.Fatalf("accept other IP: n=%d err=%v", n, err)
	}

	if n := tracker.disconnect(first); n != 1 {
		t.Fatalf("unexpected remaining connections %d", n)
	}
	if n, err := tracker.accept(third, now); err != nil || n != 2 {
		t.Fatalf("accept after disconnect: n=%d err=%v", n, err)
	}
}

func TestClientConnectionTrackerClosesIdleConnections(t *testing.T) {
	tracker := newClientConnectionTracker(RequestLimitOptions{MaxKeepAlive: time.Minute})
	now := time.Unix(100, 0)
	first := &fakeTrackedLiteClient{ip: "203.0.113.1", port: 1}
	second := &fakeTrackedLiteClient{ip: "203.0.113.1", port: 2}

	if _, err := tracker.accept(first, now); err != nil {
		t.Fatalf("accept first: %v", err)
	}
	if _, err := tracker.accept(second, now); err != nil {
		t.Fatalf("accept second: %v", err)
	}
	tracker.beginRequest(second, now.Add(90*time.Second))
	tracker.endRequest(second, now.Add(90*time.Second))

	if closed := tracker.closeIdle(now.Add(2 * time.Minute)); closed != 1 {
		t.Fatalf("unexpected closed count %d", closed)
	}
	if !first.closed {
		t.Fatal("idle connection should be closed")
	}
	if second.closed {
		t.Fatal("recent connection should stay open")
	}
}

func TestClientConnectionTrackerKeepsInflightConnection(t *testing.T) {
	tracker := newClientConnectionTracker(RequestLimitOptions{MaxKeepAlive: time.Minute})
	now := time.Unix(100, 0)
	client := &fakeTrackedLiteClient{ip: "203.0.113.1", port: 1}

	if _, err := tracker.accept(client, now); err != nil {
		t.Fatalf("accept client: %v", err)
	}
	tracker.beginRequest(client, now)

	if closed := tracker.closeIdle(now.Add(2 * time.Minute)); closed != 0 {
		t.Fatalf("closed active request count %d", closed)
	}
	if client.closed {
		t.Fatal("active request connection should stay open")
	}

	tracker.endRequest(client, now.Add(2*time.Minute))
	if closed := tracker.closeIdle(now.Add(150 * time.Second)); closed != 0 {
		t.Fatalf("closed recently completed request count %d", closed)
	}
	if closed := tracker.closeIdle(now.Add(4 * time.Minute)); closed != 1 {
		t.Fatalf("closed completed idle request count %d", closed)
	}
	if !client.closed {
		t.Fatal("idle connection should be closed after request completes")
	}
}

type fakeTrackedLiteClient struct {
	ip     string
	port   uint16
	closed bool
}

func (c *fakeTrackedLiteClient) IP() string {
	return c.ip
}

func (c *fakeTrackedLiteClient) Port() uint16 {
	return c.port
}

func (c *fakeTrackedLiteClient) Close() {
	c.closed = true
}

func requestLimiterBucketCount(limiter *requestLimiter) int {
	return limiter.limiter.Len()
}

func BenchmarkRequestLimiterSameIPParallel(b *testing.B) {
	limiter := newRequestLimiter(RequestLimitOptions{
		CapacityPerIP: 1 << 30,
		CoolingPerSec: 1 << 30,
	})
	now := time.Unix(100, 0)

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if !limiter.allow("203.0.113.1", now) {
				b.Fatal("request should be allowed")
			}
		}
	})
}

func BenchmarkRequestLimiterManyIPsParallel(b *testing.B) {
	limiter := newRequestLimiter(RequestLimitOptions{
		CapacityPerIP: 1 << 30,
		CoolingPerSec: 1 << 30,
	})
	now := time.Unix(100, 0)
	ips := benchmarkRequestLimiterIPs(1 << 16)
	var next atomic.Uint64

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ip := ips[next.Add(1)&uint64(len(ips)-1)]
			if !limiter.allow(ip, now) {
				b.Fatal("request should be allowed")
			}
		}
	})
}

func BenchmarkRequestLimiterAllowAfterManyIdleBuckets(b *testing.B) {
	const buckets = 100000

	ips := benchmarkRequestLimiterIPs(buckets + 1)
	base := time.Unix(100, 0)
	triggerAt := base.Add(30 * time.Minute)

	limiter := newRequestLimiter(RequestLimitOptions{
		CapacityPerIP: 1,
		CoolingPerSec: 1,
	})
	for j := 0; j < buckets; j++ {
		if !limiter.allow(ips[j], base) {
			b.Fatal("request should be allowed")
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// step one second per call so the capacity-1 bucket refills each time
		if !limiter.allow(ips[buckets], triggerAt.Add(time.Duration(i)*time.Second)) {
			b.Fatal("request should be allowed")
		}
	}
}

func BenchmarkClientConnectionTrackerBeginEndParallel(b *testing.B) {
	const clientsCount = 4096

	tracker := newClientConnectionTracker(RequestLimitOptions{MaxKeepAlive: time.Minute})
	now := time.Unix(100, 0)
	clients := make([]*fakeTrackedLiteClient, clientsCount)
	for i := range clients {
		client := &fakeTrackedLiteClient{ip: "203.0.113.1", port: uint16(i)}
		if _, err := tracker.accept(client, now); err != nil {
			b.Fatalf("accept client: %v", err)
		}
		clients[i] = client
	}
	var next atomic.Uint64

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			client := clients[next.Add(1)&uint64(clientsCount-1)]
			tracker.beginRequest(client, now)
			tracker.endRequest(client, now)
		}
	})
}

func benchmarkRequestLimiterIPs(n int) []string {
	ips := make([]string, n)
	for i := range ips {
		ips[i] = fmt.Sprintf("203.0.%d.%d", i>>8, i&0xff)
	}
	return ips
}
