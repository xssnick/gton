package simplex

import (
	"fmt"
	"sort"
)

// slotMap is the consensus state keyed by slot: a lazily populated map
// of per-slot states that prunes everything below the first non-finalized
// slot. at() returns nil for pruned (finalized) slots and creates missing
// ones on access.
type slotMap[T any] struct {
	firstNonFinalized uint32
	slots             map[uint32]*T
	newSlot           func() *T
}

func newSlotMap[T any](newSlot func() *T) *slotMap[T] {
	return &slotMap[T]{slots: map[uint32]*T{}, newSlot: newSlot}
}

func (m *slotMap[T]) at(slot uint32) *T {
	if slot < m.firstNonFinalized {
		return nil
	}
	s, ok := m.slots[slot]
	if !ok {
		s = m.newSlot()
		m.slots[slot] = s
	}
	return s
}

// peek returns the slot state without creating it.
func (m *slotMap[T]) peek(slot uint32) *T {
	if slot < m.firstNonFinalized {
		return nil
	}
	return m.slots[slot]
}

func (m *slotMap[T]) notifyFinalized(slot uint32) {
	if slot+1 <= m.firstNonFinalized {
		return
	}
	old := m.firstNonFinalized
	m.firstNonFinalized = slot + 1
	// Pick the cheaper of the two sweeps. Pruning by range costs O(newly
	// finalized) and keeps a deep catch-up from degenerating into a quadratic
	// cascade of full sweeps; the full sweep stays for the opposite shape, a
	// finalization that jumps far ahead of a few tracked slots. Range pruning
	// stays correct on a sparse key set (certificates from far ahead track
	// slots without filling the gap) — a missing key is simply a no-op delete.
	if delta := m.firstNonFinalized - old; int64(delta) <= int64(len(m.slots)) {
		for k := old; k < m.firstNonFinalized; k++ {
			delete(m.slots, k)
		}
		return
	}
	for k := range m.slots {
		if k < m.firstNonFinalized {
			delete(m.slots, k)
		}
	}
}

// interval returns the tracked half-open slot range [begin, end).
func (m *slotMap[T]) interval() (uint32, uint32) {
	begin, end := m.firstNonFinalized, m.firstNonFinalized
	for k := range m.slots {
		if k+1 > end {
			end = k + 1
		}
	}
	return begin, end
}

// sortedU32 is a tiny ordered set of uint32 used for skip-run ends.
type sortedU32 struct {
	v []uint32
}

func (s *sortedU32) lowerBound(x uint32) (uint32, error) {
	i := sort.Search(len(s.v), func(i int) bool { return s.v[i] >= x })
	if i == len(s.v) {
		return 0, fmt.Errorf("simplex: internal: skipped slot %d has no run end", x)
	}
	return s.v[i], nil
}

func (s *sortedU32) insert(x uint32) {
	i := sort.Search(len(s.v), func(i int) bool { return s.v[i] >= x })
	if i < len(s.v) && s.v[i] == x {
		return
	}
	s.v = append(s.v, 0)
	copy(s.v[i+1:], s.v[i:])
	s.v[i] = x
}

func (s *sortedU32) erase(x uint32) {
	i := sort.Search(len(s.v), func(i int) bool { return s.v[i] >= x })
	if i < len(s.v) && s.v[i] == x {
		s.v = append(s.v[:i], s.v[i+1:]...)
	}
}

// eraseBelow removes all elements <= x (used on finalization).
func (s *sortedU32) eraseUpTo(x uint32) {
	i := sort.Search(len(s.v), func(i int) bool { return s.v[i] > x })
	s.v = s.v[i:]
}
