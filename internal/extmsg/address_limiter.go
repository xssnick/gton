package extmsg

import (
	"container/list"
	"errors"
	"sync"
	"time"
)

const (
	DefaultAddressLimit      = 30
	DefaultAddressWindow     = 10 * time.Second
	DefaultAddressMaxEntries = 1 << 18
)

var (
	ErrAddressRateLimited                = errors.New("too many external messages to address")
	ErrAddressLimiterFull                = errors.New("external message address limiter is full")
	ErrExternalBroadcastCapacityExceeded = errors.New("external message broadcast capacity exceeded")
)

type AddressKey struct {
	Workchain int32
	Account   [32]byte
}

type AddressLimiter struct {
	limit      uint16
	window     time.Duration
	bucket     time.Duration
	maxEntries int

	mx      sync.Mutex
	entries map[AddressKey]*addressLimitEntry
	order   *list.List
}

type addressLimitEntry struct {
	key     AddressKey
	epoch   int64
	current uint16
	prev    uint16
	seenAt  time.Time
	element *list.Element
}

func NewAddressLimiter(limit int, window time.Duration, maxEntries int) *AddressLimiter {
	return &AddressLimiter{
		limit:      uint16(limit),
		window:     window,
		bucket:     window / 2,
		maxEntries: maxEntries,
		entries:    map[AddressKey]*addressLimitEntry{},
		order:      list.New(),
	}
}

func NewDefaultAddressLimiter() *AddressLimiter {
	return NewAddressLimiter(DefaultAddressLimit, DefaultAddressWindow, DefaultAddressMaxEntries)
}

func (l *AddressLimiter) Check(key AddressKey, now time.Time) error {
	l.mx.Lock()
	defer l.mx.Unlock()

	l.pruneExpiredLocked(now)
	entry := l.entries[key]
	if entry == nil {
		if len(l.entries) >= l.maxEntries {
			return ErrAddressLimiterFull
		}
		return nil
	}

	l.rotateEntryLocked(entry, now)
	if entry.current+entry.prev >= l.limit {
		return ErrAddressRateLimited
	}
	return nil
}

func (l *AddressLimiter) Add(key AddressKey, now time.Time) error {
	l.mx.Lock()
	defer l.mx.Unlock()

	l.pruneExpiredLocked(now)
	entry := l.entries[key]
	if entry == nil {
		if len(l.entries) >= l.maxEntries {
			return ErrAddressLimiterFull
		}
		entry = &addressLimitEntry{
			key:   key,
			epoch: l.epoch(now),
		}
		entry.element = l.order.PushBack(entry)
		l.entries[key] = entry
	} else {
		l.rotateEntryLocked(entry, now)
	}

	if entry.current+entry.prev >= l.limit {
		return ErrAddressRateLimited
	}
	entry.current++
	entry.seenAt = now
	l.order.MoveToBack(entry.element)
	return nil
}

func (l *AddressLimiter) Remove(key AddressKey, now time.Time) {
	l.mx.Lock()
	defer l.mx.Unlock()

	entry := l.entries[key]
	if entry == nil {
		return
	}
	l.rotateEntryLocked(entry, now)
	if entry.current > 0 {
		entry.current--
	} else if entry.prev > 0 {
		entry.prev--
	}
	if entry.current == 0 && entry.prev == 0 {
		l.deleteEntryLocked(entry)
	}
}

func (l *AddressLimiter) rotateEntryLocked(entry *addressLimitEntry, now time.Time) {
	epoch := l.epoch(now)
	switch {
	case epoch == entry.epoch:
	case epoch == entry.epoch+1:
		entry.prev = entry.current
		entry.current = 0
		entry.epoch = epoch
	case epoch > entry.epoch+1:
		entry.prev = 0
		entry.current = 0
		entry.epoch = epoch
	}
}

func (l *AddressLimiter) pruneExpiredLocked(now time.Time) {
	for elem := l.order.Front(); elem != nil; {
		entry := elem.Value.(*addressLimitEntry)
		if now.Sub(entry.seenAt) < l.window {
			return
		}
		next := elem.Next()
		l.deleteEntryLocked(entry)
		elem = next
	}
}

func (l *AddressLimiter) deleteEntryLocked(entry *addressLimitEntry) {
	delete(l.entries, entry.key)
	l.order.Remove(entry.element)
	entry.element = nil
}

func (l *AddressLimiter) epoch(now time.Time) int64 {
	return now.UnixNano() / l.bucket.Nanoseconds()
}
