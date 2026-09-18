package validator

// candidateDecodedBOCBytes charges a validated BOC's cell arena without walking
// its DAG. All supported BOC headers put the cell count at offset 6, with the
// low three bits of byte 4 giving its width. Callers have already parsed the
// input or serialized these bytes themselves; this is not an input validator.
//
// 256 bytes per cell cover Cell, optional metadata and three extra hashes in
// the current cell layout; the encoded size covers the retained payload body.
// Sharing is deliberately not discounted. This is a conservative cache weight,
// not an allocator/RSS measurement, and no serializer presizing clamp applies.
func candidateDecodedBOCBytes(boc []byte) int64 {
	if len(boc) == 0 {
		return 0
	}

	var cells int64
	for _, b := range boc[6 : 6+int(boc[4]&7)] {
		cells = cells<<8 | int64(b)
	}

	return int64(len(boc)) + cells*256
}

// The decoded cache is separate from the compact payload retention floor.
// Demotion preserves BOCs, identity and certificates, including above the
// finalized tip. The limits are per session, not a process-memory guarantee:
// active borrowers and store/load claims may hold additional arenas. One MRU
// candidate is also protected, even if it alone is larger than the budget, so
// admission never discards the decode just before its first consumer claims it.
const (
	candidateDecodedShardBudget  int64 = 256 << 20
	candidateDecodedMasterBudget int64 = 64 << 20
)

func (r *candidateResolver) retainDecodedRootsLocked(entry *candidateEntry, bytes int64) {
	if bytes == 0 {
		// Locally built successors have their own ownership/lifetime; this cache
		// bounds received cell arenas, whose size the codec records at decode.
		return
	}
	entry.decodedRootBytes = bytes
	r.cache.DecodedBytes += bytes
	r.linkDecodedRootsLocked(entry)
	r.trimDecodedRootsLocked()
}

func (r *candidateResolver) linkDecodedRootsLocked(entry *candidateEntry) {
	entry.decodedNext = r.decodedHead
	if r.decodedHead != nil {
		r.decodedHead.decodedPrev = entry
	} else {
		r.decodedTail = entry
	}
	r.decodedHead = entry
}

func (r *candidateResolver) unlinkDecodedRootsLocked(entry *candidateEntry) {
	if entry.decodedPrev != nil {
		entry.decodedPrev.decodedNext = entry.decodedNext
	} else {
		r.decodedHead = entry.decodedNext
	}
	if entry.decodedNext != nil {
		entry.decodedNext.decodedPrev = entry.decodedPrev
	} else {
		r.decodedTail = entry.decodedPrev
	}
	entry.decodedPrev = nil
	entry.decodedNext = nil
}

func (r *candidateResolver) forgetDecodedRootsLocked(entry *candidateEntry) {
	if entry.decodedRootBytes == 0 {
		return
	}
	r.unlinkDecodedRootsLocked(entry)
	r.cache.DecodedBytes -= entry.decodedRootBytes
	entry.decodedRootBytes = 0
}

func (r *candidateResolver) touchDecodedRootsLocked(entry *candidateEntry) {
	if entry.decodedRootBytes == 0 || r.decodedHead == entry {
		return
	}
	r.unlinkDecodedRootsLocked(entry)
	r.linkDecodedRootsLocked(entry)
}

func (r *candidateResolver) syncDecodedRootsLocked(entry *candidateEntry) {
	if entry.validationRoots != nil {
		return
	}
	if entry.lazyWire != nil && entry.lazyWire.roots.Load() != nil {
		return
	}
	r.forgetDecodedRootsLocked(entry)
}

// Only cached references are dropped. A validation/state borrower has already
// taken its own immutable roots; materialize atomically takes wire roots before
// building. Never wait for either operation under the resolver mutex. Pending
// store claims are also left hot so demotion cannot add parsing ahead of a vote.
func (r *candidateResolver) trimDecodedRootsLocked() {
	for entry := r.decodedTail; entry != nil && r.cache.DecodedBytes > r.decodedBudget; {
		previous := entry.decodedPrev
		if entry != r.decodedHead && entry.load == nil && entry.store == nil && entry.storeBuilds == 0 {
			entry.validationRoots = nil
			if entry.lazyWire != nil {
				entry.lazyWire.releaseRoots()
			}
			r.forgetDecodedRootsLocked(entry)
			r.cache.DecodedDemotions++
		}
		entry = previous
	}
}
