package liteserver

import (
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
	if len(limiter.buckets) != 1 {
		t.Fatalf("unexpected bucket count %d", len(limiter.buckets))
	}

	if !limiter.allow("203.0.113.2", now.Add(requestLimitIdleTTL+requestLimitPruneInterval)) {
		t.Fatal("request should be allowed")
	}
	if _, ok := limiter.buckets["203.0.113.1"]; ok {
		t.Fatal("idle full bucket should be pruned")
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
	tracker.markRequest(second, now.Add(90*time.Second))

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
