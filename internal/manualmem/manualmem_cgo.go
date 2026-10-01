//go:build cgo

package manualmem

// #include <stdlib.h>
// #include <string.h>
import "C"

import "unsafe"

// ManagedByGC reports where Alloc's regions live: false here — malloc'd C
// memory the Go GC never sees. Logged at open so a deployment can tell which
// path its binary took.
const ManagedByGC = false

// alloc uses calloc rather than malloc: callers rely on zeroed regions (an
// all-zero index slot means "empty"), and calloc's zero pages are provided
// lazily by the OS, so an oversized region costs address space, not resident
// memory, until it is written.
func alloc(n int) []byte {
	ptr := C.calloc(1, C.size_t(n))
	if ptr == nil {
		panic("manualmem: calloc failed")
	}
	return unsafe.Slice((*byte)(ptr), n)
}

func free(region []byte) {
	C.free(unsafe.Pointer(unsafe.SliceData(region)))
}
