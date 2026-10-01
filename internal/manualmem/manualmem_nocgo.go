//go:build !cgo

package manualmem

import "unsafe"

// ManagedByGC reports where Alloc's regions live: true here — without cgo the
// regions are ordinary Go-heap allocations. They are pointer-free (noscan), so
// the GC never scans them, but they are LIVE HEAP BYTES: a 4 GiB region raises
// the live set by 4 GiB, GOGC paces the next collection off that, and
// GOMEMLIMIT counts it. See the package comment.
const ManagedByGC = true

// alloc backs the region with a []uint64 so the bytes are 8-byte aligned, the
// same guarantee malloc gives the cgo path; callers overlay atomic.Uint64
// views on these regions. Go zeroes heap allocations, matching calloc.
func alloc(n int) []byte {
	words := make([]uint64, (n+7)/8)
	return unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(words))), n)
}

// free has nothing to do without cgo: dropping the last reference returns the
// region to the Go heap. It exists so the caller contract — every Alloc gets a
// Free, balance returns to zero — holds on both builds and stays assertable.
func free(region []byte) {
	_ = region
}
