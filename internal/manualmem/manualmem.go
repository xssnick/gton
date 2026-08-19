// Package manualmem hands out large, long-lived byte regions that are — under
// cgo — allocated with malloc, OUTSIDE the Go heap, mirroring what pebble's
// internal/manual does for its block cache. The Go GC neither scans nor
// accounts them: a multi-GiB cache region costs no mark work and does not
// inflate the live-byte figure that GOGC paces against.
//
// Under cgo=0 there is no malloc to reach, so Alloc falls back to Go-heap
// noscan byte slices ([]uint64-backed, so they are 8-byte aligned either way).
// Pointer-free memory is never scanned by the GC mark phase, but it IS live
// heap: a 4 GiB region becomes 4 GiB of live bytes that GOGC multiplies into
// the heap target and that counts against GOMEMLIMIT. A nocgo build carrying
// caches sized for the malloc path must budget for that (the deployed binary
// builds CGO_ENABLED=1, so production takes the malloc path).
//
// The package tracks its outstanding balance so a Close path can assert it
// returned everything it took.
package manualmem

import "sync/atomic"

var allocated atomic.Int64

// Allocated reports the bytes currently handed out and not yet freed.
func Allocated() int64 {
	return allocated.Load()
}

// Alloc returns a zeroed region of n bytes, 8-byte aligned. It panics when n
// is not positive, and — like every large allocator — when the underlying
// allocation fails, because a cache region that silently came back nil would
// only defer the crash to the first write.
func Alloc(n int) []byte {
	if n <= 0 {
		panic("manualmem: allocation size must be positive")
	}
	region := alloc(n)
	allocated.Add(int64(n))
	return region
}

// Free returns a region obtained from Alloc. The full slice must be passed
// back; the caller must not retain any view into it afterwards. Freeing nil is
// a no-op so error paths can free unconditionally.
func Free(region []byte) {
	if region == nil {
		return
	}
	free(region)
	allocated.Add(-int64(len(region)))
}
