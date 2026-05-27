package extmsg

import (
	"errors"
	"testing"
	"time"
)

func TestAddressLimiterLimitsWithinWindow(t *testing.T) {
	limiter := NewAddressLimiter(3, 10*time.Second, 16)
	now := time.Unix(100, 0)
	key := AddressKey{Workchain: 0}
	key.Account[0] = 1

	for i := 0; i < 3; i++ {
		if err := limiter.Check(key, now); err != nil {
			t.Fatalf("check %d: %v", i, err)
		}
		if err := limiter.Add(key, now); err != nil {
			t.Fatalf("add %d: %v", i, err)
		}
	}
	if err := limiter.Check(key, now); !errors.Is(err, ErrAddressRateLimited) {
		t.Fatalf("check over limit err=%v, want rate limited", err)
	}
	if err := limiter.Add(key, now); !errors.Is(err, ErrAddressRateLimited) {
		t.Fatalf("add over limit err=%v, want rate limited", err)
	}
}

func TestAddressLimiterExpiresEntries(t *testing.T) {
	limiter := NewAddressLimiter(1, 10*time.Second, 16)
	now := time.Unix(100, 0)
	key := AddressKey{Workchain: 0}
	key.Account[0] = 1

	if err := limiter.Add(key, now); err != nil {
		t.Fatalf("add: %v", err)
	}
	if err := limiter.Add(key, now.Add(11*time.Second)); err != nil {
		t.Fatalf("add after window: %v", err)
	}
}

func TestAddressLimiterRemoveReleasesSlot(t *testing.T) {
	limiter := NewAddressLimiter(1, 10*time.Second, 16)
	now := time.Unix(100, 0)
	key := AddressKey{Workchain: 0}
	key.Account[0] = 1

	if err := limiter.Add(key, now); err != nil {
		t.Fatalf("add: %v", err)
	}
	limiter.Remove(key, now)
	if err := limiter.Add(key, now); err != nil {
		t.Fatalf("add after remove: %v", err)
	}
}

func TestAddressLimiterRejectsNewAddressesWhenFull(t *testing.T) {
	limiter := NewAddressLimiter(3, 10*time.Second, 2)
	now := time.Unix(100, 0)
	first := AddressKey{Workchain: 0}
	first.Account[0] = 1
	second := AddressKey{Workchain: 0}
	second.Account[0] = 2
	third := AddressKey{Workchain: 0}
	third.Account[0] = 3

	if err := limiter.Add(first, now); err != nil {
		t.Fatalf("add first: %v", err)
	}
	if err := limiter.Add(second, now); err != nil {
		t.Fatalf("add second: %v", err)
	}
	if err := limiter.Check(third, now); !errors.Is(err, ErrAddressLimiterFull) {
		t.Fatalf("check third err=%v, want full", err)
	}
	if err := limiter.Add(third, now); !errors.Is(err, ErrAddressLimiterFull) {
		t.Fatalf("add third err=%v, want full", err)
	}
	if err := limiter.Add(first, now); err != nil {
		t.Fatalf("existing address should still pass: %v", err)
	}
}
