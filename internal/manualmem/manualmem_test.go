package manualmem

import (
	"testing"
	"unsafe"
)

// The two contract points every consumer leans on: regions are 8-byte aligned
// (atomic.Uint64 views are overlaid on them) and zeroed (an all-zero index
// slot means "empty"), and the balance closes when they come back.
func TestAllocAlignedZeroedAndBalanced(t *testing.T) {
	before := Allocated()

	region := Alloc(4097) // deliberately not a multiple of 8
	if len(region) != 4097 {
		t.Fatalf("len = %d, want 4097", len(region))
	}
	if addr := uintptr(unsafe.Pointer(unsafe.SliceData(region))); addr%8 != 0 {
		t.Fatalf("region is not 8-byte aligned: %#x", addr)
	}
	for i, b := range region {
		if b != 0 {
			t.Fatalf("byte %d is %#x, want zeroed memory", i, b)
		}
	}
	if got := Allocated() - before; got != 4097 {
		t.Fatalf("balance grew by %d, want 4097", got)
	}

	Free(region)
	if got := Allocated() - before; got != 0 {
		t.Fatalf("balance after free = %+d, want 0", got)
	}

	Free(nil) // must be a no-op for unconditional error-path frees
	if got := Allocated() - before; got != 0 {
		t.Fatalf("balance after Free(nil) = %+d, want 0", got)
	}
}
