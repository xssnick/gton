package collator

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"sync"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const estimatedPrunedBranchBytes = uint64(41)

// A new key can replace the terminal old node with a fork and add two leaves.
// Charging three maximum-sized cells bounds that local Patricia rewrite without
// constructing it. Existing keys use their exact predecessor path instead.
const estimatedAccountInsertPathBytes = uint64(3 * (12 + 1023/8 + 3))

const (
	estimatedAccountDeletePathBytes = uint64(12 + 1023/8 + 3)
)

// proofSizeEstimator reproduces the canonical admission estimate:
// loaded cells replace their pruned boundary and unseen children contribute a
// fixed-size pruned branch. It is fed by the read set's record callback.
//
// A cell is either loaded or only referenced, and a referenced one always
// contributes the same fixed pruned-branch size, so the set records that one bit
// rather than a per-cell size. The callback fires on every first cell load of
// the whole collation — around 28k entries for a full block — which is why it
// holds no more state than it must.
//
// It is sharded because those calls arrive from parallel lanes: the dictionary
// batches replay account and queue paths on collationParallelism goroutines and
// traceValidationClosure walks five predecessor views at once, all recording
// through one read set into this one estimator. Held on a single mutex the
// callback was the largest source of blocking anywhere in a collated collation:
// two thirds of the whole build's aggregate mutex delay, and sharding leaves it
// at three percent of a total that is itself a third of what it was. The read
// set beside it answered the identical problem the identical way, and its shard
// is the model for this one (tonutils tvm/cell/readset.go, readSetShard).
//
// Sharding splits the state; it does not split the two answers this estimator
// gives, and both of them decide consensus values. The byte total decides which
// messages the block admits, and the seen set decides which predecessor cells
// enter the collated proof. Each is read as a whole, so each is read under every
// shard lock at once — a reader that walked the shards one at a time could
// return a mixture of two instants, which is a block whose contents depend on
// goroutine timing. For the same reason a charge holds every shard its cell and
// that cell's children land in, rather than one shard at a time: parent counted,
// children not, is one of those mixtures. Every acquisition of more than one
// shard goes in ascending index order, which is what keeps holding several of
// them deadlock-free.
//
// None of that puts the recording lanes back on a shared lock. A charge takes
// the shards its own cell lands in, which two lanes rarely both need, and the
// readers run on the collation goroutine between the parallel phases rather than
// inside them.
type proofSizeEstimator struct {
	shards [proofEstimatorShards]proofEstimatorShard

	// sealed stops the estimator recording once the collated-proof selector has
	// taken its view of the seen set. See loadedHashes. Written under every
	// shard lock and read under the lock of whatever shard a record lands in, so
	// no record can straddle it.
	sealed bool
}

// proofEstimatorShards is chosen against collationParallelism rather than
// against the core count: the worker budget is what bounds how many lanes can be
// inside the estimator at once.
const proofEstimatorShards = 16

// proofEstimatorShard is one independent slice of the estimator: the dedup memo
// for the hashes that land in it, and the bytes those hashes contributed.
//
// The byte total splits across shards without changing, because every term of it
// belongs to exactly one hash: a cell is charged its loaded size when its own
// hash is first observed as loaded, a child is charged a pruned branch when the
// child's hash is first observed at all, and the promote that replaces the
// second by the first observes that same child hash again. No term depends on
// two hashes at once, so the shard that owns a hash owns every byte that hash
// ever moves, and the sum over shards is the number the single counter held.
type proofEstimatorShard struct {
	mu sync.Mutex
	// bytes and seen are read and written under mu alone — never atomically,
	// never lock-free. A lock-free total would be summed across shards while a
	// charge was part-way through them.
	bytes uint64
	seen  proofSeenHashSet
	// Parallel lanes write neighbouring shards at the same moment, and without
	// the padding they share a cache line: the shards then give back as false
	// sharing part of what they removed as contention, measured at 16% of the
	// estimator's own time on a ten-goroutine feed of 20k cells.
	_ [64]byte
}

// proofEstimatorShardOf picks a shard from the LAST byte of the hash, because
// proofCellFingerprint reads the first four. Sharding on a byte the fingerprint
// reads would leave every fingerprint within a shard sharing its low bits, and
// the slot index is the fingerprint masked: the table would use one slot in
// sixteen and probe runs would grow without bound. The read set makes the same
// split the other way round, sharding on byte 0 and fingerprinting bytes 1-4
// (readset.go readSetFingerprint).
func proofEstimatorShardOf(hash cell.Hash) int {
	return int(hash[len(hash)-1]) & (proofEstimatorShards - 1)
}

// proofEstimatorAllShards is the mask a reader of the whole estimator takes.
const proofEstimatorAllShards = uint32(1)<<proofEstimatorShards - 1

// lockShards takes the shards named by mask in ascending index order, and
// unlockShards gives them back. Ascending is the one order every caller uses —
// a charge over a subset and a reader over all sixteen alike — so no two
// acquisitions can hold what the other is waiting for.
func (e *proofSizeEstimator) lockShards(mask uint32) {
	for rest := mask; rest != 0; rest &= rest - 1 {
		e.shards[bits.TrailingZeros32(rest)].mu.Lock()
	}
}

func (e *proofSizeEstimator) unlockShards(mask uint32) {
	for rest := mask; rest != 0; rest &= rest - 1 {
		e.shards[bits.TrailingZeros32(rest)].mu.Unlock()
	}
}

// proofEstimatorChargeMask names every shard one charge touches: the cell's own
// and its children's. Duplicates collapse into the one bit they share, which is
// also what keeps a child landing in its parent's shard from being the same lock
// twice.
func proofEstimatorChargeMask(hash cell.Hash, refs []cell.Hash) uint32 {
	mask := uint32(1) << proofEstimatorShardOf(hash)
	for _, ref := range refs {
		mask |= uint32(1) << proofEstimatorShardOf(ref)
	}
	return mask
}

// newProofSizeEstimator opens an estimator sized for a block whose read set is
// expected to reach expectedReadCells. The set it keeps is larger than the read
// set — it holds every cell the record saw plus the pruned boundary of every
// child of one — and measured over mainnet blocks it lands at 1.22-1.39x the
// read count, so the table is asked for 1.4x and doubled for the half-full rule
// the probe relies on. It is a capacity and nothing else: a wrong estimate costs
// only the growth it avoided, and the set is dropped with the collation.
//
// The share is split evenly over the shards. Cells are spread by a byte of a
// cryptographic hash, so an even split is what they arrive as, and a shard that
// outgrows its share still grows.
func newProofSizeEstimator(expectedReadCells int) *proofSizeEstimator {
	e := &proofSizeEstimator{}
	perShard := expectedReadCells * 7 / 5 / proofEstimatorShards
	for i := range e.shards {
		e.shards[i].seen.presize(perShard)
	}
	return e
}

// A batch is deliberately smaller than the estimator. Taking all sixteen
// shards around a cached traversal would make the batch fast in isolation but
// stop every other validation-closure task until it completed. Eight shards
// preserve parallel progress, while the cell ceiling prevents a pathologically
// concentrated batch from holding even that subset for an unbounded interval.
const (
	proofEstimatorBatchMaxShards = proofEstimatorShards / 2
	proofEstimatorBatchMaxCells  = 64
)

func (e *proofSizeEstimator) addLoadedCell(root *cell.Cell) {
	charge := makeProofEstimatorLoadedCellCharge(root)
	e.lockShards(charge.mask)
	if !e.sealed {
		e.addLoadedCellLocked(&charge)
	}
	e.unlockShards(charge.mask)
}

// addLoadedCells is the callback for ReadSet.RecordMany. Consecutive cells are
// coalesced while their combined charge touches at most half the estimator
// shards, then applied under that one lock set. Each chunk remains an atomic
// sequence of the same per-cell charges addLoadedCell makes: size cannot observe
// a loaded parent without its child boundaries, but unrelated closure tasks can
// keep progressing through the other half of the shards.
func (e *proofSizeEstimator) addLoadedCells(roots []*cell.Cell) {
	var charges [proofEstimatorBatchMaxCells]proofEstimatorLoadedCellCharge
	for windowStart := 0; windowStart < len(roots); {
		windowEnd := min(windowStart+len(charges), len(roots))
		window := charges[:windowEnd-windowStart]
		for i, root := range roots[windowStart:windowEnd] {
			window[i] = makeProofEstimatorLoadedCellCharge(root)
		}

		for start := 0; start < len(window); {
			mask := uint32(0)
			end := start
			for end < len(window) {
				next := mask | window[end].mask
				if mask != 0 && bits.OnesCount32(next) > proofEstimatorBatchMaxShards {
					break
				}
				mask = next
				end++
			}

			e.lockShards(mask)
			if !e.sealed {
				for i := start; i < end; i++ {
					e.addLoadedCellLocked(&window[i])
				}
			}
			e.unlockShards(mask)
			start = end
		}
		windowStart = windowEnd
	}
}

type proofEstimatorLoadedCellCharge struct {
	hash        cell.Hash
	refs        [4]cell.Hash
	loadedBytes uint64
	mask        uint32
	refsCount   uint8
}

func makeProofEstimatorLoadedCellCharge(root *cell.Cell) proofEstimatorLoadedCellCharge {
	charge := proofEstimatorLoadedCellCharge{
		hash:        root.HashKey(),
		refsCount:   uint8(root.RefsNum()),
		loadedBytes: uint64(5+(root.BitsSize()+7)/8) + uint64(root.RefsNum())*3,
	}
	refs := charge.refs[:charge.refsCount]
	for i := range refs {
		refs[i] = root.MustRefHashAt(i)
	}
	charge.mask = proofEstimatorChargeMask(charge.hash, refs)
	return charge
}

// addLoadedCellLocked applies one loaded-cell charge. The caller holds every
// shard named by charge.mask, so this function may be shared by the single and
// batched paths without nested locking or recalculating cell hashes under lock.
func (e *proofSizeEstimator) addLoadedCellLocked(charge *proofEstimatorLoadedCellCharge) {
	shard := &e.shards[proofEstimatorShardOf(charge.hash)]
	prior := shard.seen.observe(charge.hash, proofSeenLoaded|proofSeenSelected, proofSeenBoundary)
	if prior&proofSeenLoaded != 0 {
		return
	}
	if prior&proofSeenBoundary != 0 {
		// The cell stood in as a pruned branch until now; it is charged at
		// its loaded size instead. The stand-in was charged to this same
		// shard, so the subtraction cannot take the shard below zero.
		shard.bytes -= estimatedPrunedBranchBytes
	}
	shard.bytes += charge.loadedBytes

	// The children are charged under the same held locks as the cell itself,
	// as the rest of one indivisible step. A size() landing between the two
	// halves would report a value the estimator never logically holds.
	for _, ref := range charge.refs[:charge.refsCount] {
		child := &e.shards[proofEstimatorShardOf(ref)]
		if child.seen.observe(ref, proofSeenBoundary, 0) == 0 {
			child.bytes += estimatedPrunedBranchBytes
		}
	}
}

// addExecutionRead marks a cell the transaction executor loaded as belonging in
// the emitted proof, without charging it.
//
// The machine reports every first cell load, which is an exact account of what
// execution read and, unlike the traversal record, survives a cell reaching the
// machine through a route that lost the recording trace. Selection has to see
// those cells: an account's code or data missing from the collated proof is what
// a peer reports back as a pruned branch.
//
// The size is deliberately not charged. Execution also loads the inbound message
// and cells it built itself, which are not part of the predecessor tree, so the
// proof walk never finds their hashes and never emits them; charging them would
// shrink the block for bytes that are never written. The estimate is a floor with
// a hard check on the real serialized size behind it, so leaving these uncharged
// errs on the side the collation can still recover from.
func (e *proofSizeEstimator) addExecutionRead(loaded *cell.Cell) {
	if loaded == nil {
		return
	}
	hash := loaded.HashKey()

	shard := &e.shards[proofEstimatorShardOf(hash)]
	shard.mu.Lock()
	if !e.sealed {
		shard.seen.observe(hash, proofSeenSelected, 0)
	}
	shard.mu.Unlock()
}

// proofSeenHashSet is the estimator's dedup memo as an open-addressed table
// rather than a map[cell.Hash]bool: the key is already a cryptographic hash, so
// a slot carries four of its bytes as a fingerprint and the full 32-byte
// compare runs only on a candidate hit. Never a truncated compare — the set
// decides how many bytes a message is charged, so a collision that passed for
// equality would change which messages the block admits.
//
// Modelled on cell_storage_stat.go's storageSeenSet, not shared with it: that
// table is keyed by *Cell and compares through the cell's own cached hash,
// while addLoadedCell holds nothing but the hashes MustRefHashAt returns.
//
// The loaded/pruned bit rides in the slot word beside the entry index, so
// promoting a referenced cell to a loaded one writes a single word and reads no
// hash. The table starts empty and doubles as it fills: a collation without
// full collated data never allocates one at all.
//
// The hashes live in fixed-size chunks rather than one growing slice. They are
// only ever appended and read by index, so nothing needs them contiguous, and a
// doubling slice of 32-byte entries would copy roughly four bytes for every one
// it stores — at 28k entries that churn alone outweighed everything the table
// saves over the map.
type proofSeenHashSet struct {
	// slots hold proofCellFingerprint(hash)<<32 | (index+1)<<3 | flags; zero is
	// empty. See the proofSeen* flags for what the three bits mean.
	slots  []uint64
	hashes [][]cell.Hash
	count  int
}

// The three things the set can know about a hash, independently. They are
// separate bits rather than one "loaded" flag because each answers a different
// question and the answers do not move together:
//
//	proofSeenLoaded    its loaded size is on the byte total
//	proofSeenBoundary  estimatedPrunedBranchBytes are on the byte total for it
//	proofSeenSelected  it belongs in the emitted proof, charged or not
//
// The last one exists for the cells the transaction executor reports. Those are
// selected — an account's code missing from the collated proof is what a peer
// reports back as a pruned branch — but they cannot be charged when they arrive,
// because execution also loads the inbound message and cells it built itself,
// which are not in the predecessor tree and are never emitted. Folding selection
// into the loaded bit, as this set used to, made every such cell look already
// charged: the traversal that later reached it through the predecessor tree
// found it "loaded" and skipped its size and its children's boundaries, so cells
// that really were emitted never reached the estimate at all. The estimate is
// meant to be a floor with a hard check behind it; that made it a floor well
// under what the block actually wrote.
const (
	proofSeenLoaded uint64 = 1 << iota
	proofSeenBoundary
	proofSeenSelected
)

const proofSeenFlagBits = 3
const proofSeenFlagMask = uint64(1)<<proofSeenFlagBits - 1

const proofSeenChunkBits = 10

func (s *proofSeenHashSet) hashAt(index uint32) cell.Hash {
	return s.hashes[index>>proofSeenChunkBits][index&(1<<proofSeenChunkBits-1)]
}

func (s *proofSeenHashSet) appendHash(hash cell.Hash) {
	last := len(s.hashes) - 1
	if last < 0 || len(s.hashes[last]) == 1<<proofSeenChunkBits {
		s.hashes = append(s.hashes, make([]cell.Hash, 0, 1<<proofSeenChunkBits))
		last++
	}
	s.hashes[last] = append(s.hashes[last], hash)
	s.count++
}

// proofSeenMaxPresizedCells bounds what presize honours, so an estimate wrong by
// orders of magnitude allocates a bounded table rather than an arbitrary one.
const proofSeenMaxPresizedCells = 1 << 20

// presize allocates the slot array for expected entries. Only the slots are
// sized: the hash chunks above are exact by construction and have nothing to
// gain. Growing into a block-sized table instead costs seventeen generations of
// which sixteen are discarded.
func (s *proofSeenHashSet) presize(expected int) {
	if expected <= 0 || s.slots != nil {
		return
	}
	if expected > proofSeenMaxPresizedCells {
		expected = proofSeenMaxPresizedCells
	}
	slots := 64
	for slots < 2*expected {
		slots *= 2
	}
	s.slots = make([]uint64, slots)
}

// observe records hash with the flags in set and returns the flags the entry
// carried before, or zero for one this call created. Flags only ever accumulate;
// clear names the one flag a promotion retires, the boundary a cell stops being
// once it is charged at its loaded size.
func (s *proofSeenHashSet) observe(hash cell.Hash, set, clear uint64) uint64 {
	if s.slots == nil {
		s.slots = make([]uint64, 64)
	}
	fingerprint := proofCellFingerprint(hash)
	mask := len(s.slots) - 1
	pos := int(fingerprint) & mask
	for {
		slot := s.slots[pos]
		if slot == 0 {
			break
		}
		if uint32(slot>>32) == fingerprint && s.hashAt((uint32(slot)>>proofSeenFlagBits)-1) == hash {
			prior := slot & proofSeenFlagMask
			if next := prior&^clear | set; next != prior {
				s.slots[pos] = slot&^proofSeenFlagMask | next
			}

			return prior
		}
		pos = (pos + 1) & mask
	}

	// Doubled at half full, so the table never gets dense enough for probe runs
	// to matter.
	if (s.count+1)*2 > len(s.slots) {
		s.grow()
		mask = len(s.slots) - 1
		pos = int(fingerprint) & mask
		for s.slots[pos] != 0 {
			pos = (pos + 1) & mask
		}
	}
	s.appendHash(hash)
	s.slots[pos] = uint64(fingerprint)<<32 | uint64(s.count)<<proofSeenFlagBits | set

	return 0
}

// flags reports what the set knows about hash, or zero for one it has not seen.
func (s proofSeenHashSet) flags(hash cell.Hash) uint64 {
	if len(s.slots) == 0 {
		return 0
	}
	fingerprint := proofCellFingerprint(hash)
	mask := len(s.slots) - 1
	pos := int(fingerprint) & mask
	for range s.slots {
		slot := s.slots[pos]
		if slot == 0 {
			return 0
		}
		if uint32(slot>>32) == fingerprint && s.hashAt((uint32(slot)>>proofSeenFlagBits)-1) == hash {
			return slot & proofSeenFlagMask
		}
		pos = (pos + 1) & mask
	}

	return 0
}

// emitted reports whether hash belongs in the proof: charged at its loaded size,
// or selected by an execution read that could not be charged. This is the
// predicate the collated-proof selector walks with, and it holds exactly the set
// the single loaded bit used to hold — splitting the bits moved what is charged,
// never what is emitted.
func (s proofSeenHashSet) emitted(hash cell.Hash) bool {
	return s.flags(hash)&(proofSeenLoaded|proofSeenSelected) != 0
}

// merkleUpdateSourceShape returns the non-leaf source cells whose structure
// ApplyMerkleUpdate compares against the predecessor before it can apply the
// destination proof. They must be materialized in full collated data even when
// no semantic validator read reached them independently. C++ records these
// cells while generating the state update and its collated proof from the same
// usage tree.
//
// A source proof can hold level-shifted ordinary cells above pruned boundaries.
// The predecessor proof contains the represented level-zero cells, not those
// proof cells' higher physical hashes, so selection is keyed by the visible
// level-zero hash that ApplyMerkleUpdate compares.
func merkleUpdateSourceShape(update *cell.Cell) (proofSeenHashSet, error) {
	if update == nil || update.GetType() != cell.MerkleUpdateCellType || update.RefsNum() != 2 {
		return proofSeenHashSet{}, fmt.Errorf("invalid Merkle update")
	}

	root, err := update.PeekRef(0)
	if err != nil {
		return proofSeenHashSet{}, fmt.Errorf("load source root: %w", err)
	}
	stack := []*cell.Cell{root}
	var shape proofSeenHashSet
	for len(stack) > 0 {
		last := len(stack) - 1
		current := stack[last]
		stack = stack[:last]
		if current.GetType() == cell.PrunedCellType || current.RefsNum() == 0 {
			continue
		}
		if shape.observe(current.HashKeyAt(0), proofSeenLoaded, 0) != 0 {
			continue
		}
		for i := 0; i < int(current.RefsNum()); i++ {
			ref, refErr := current.PeekRef(i)
			if refErr != nil {
				return proofSeenHashSet{}, fmt.Errorf("load source ref %d: %w", i, refErr)
			}
			stack = append(stack, ref)
		}
	}
	return shape, nil
}

// proofLoadedHashes is the estimator's seen set as one lookup across its shards.
type proofLoadedHashes struct {
	shards [proofEstimatorShards]proofSeenHashSet
}

func (l *proofLoadedHashes) loaded(hash cell.Hash) bool {
	return l.shards[proofEstimatorShardOf(hash)].emitted(hash)
}

// loadedHashes seals the estimator and returns its seen set as one view. It is
// the end of the estimator's life: the view IS the predicate that selects the
// cells of the previous-state proof, so what it answers must not move while the
// selection walk asks it, and a walk whose answers moved would emit a proof that
// depends on when a goroutine ran.
//
// Two things make it not move. The shards are taken all at once, so the view is
// the estimator at a single instant rather than a mixture of sixteen; and the
// seal means no later record can reach the tables the view shares, which is what
// lets it share them — copying tens of thousands of hashes for every block buys
// nothing a seal does not.
//
// A record arriving after the seal is dropped, deliberately and in silence. It
// can only come from a phase running past the point the collation stops loading
// predecessor cells, and both answers are finished by then: the byte total has
// already admitted the last message, and this view has already been handed to
// the selector. Dropping is what keeps those two answers the same on every run;
// letting the record through would put a cell in the proof on the runs where it
// arrived first.
func (e *proofSizeEstimator) loadedHashes() *proofLoadedHashes {
	view := &proofLoadedHashes{}
	if e == nil {
		return view
	}
	e.lockShards(proofEstimatorAllShards)
	e.sealed = true
	for i := range e.shards {
		view.shards[i] = e.shards[i].seen
	}
	e.unlockShards(proofEstimatorAllShards)
	return view
}

func (s *proofSeenHashSet) grow() {
	old := s.slots
	s.slots = make([]uint64, 2*len(old))
	mask := len(s.slots) - 1
	for _, slot := range old {
		if slot == 0 {
			continue
		}
		pos := int(uint32(slot>>32)) & mask
		for s.slots[pos] != 0 {
			pos = (pos + 1) & mask
		}
		s.slots[pos] = slot
	}
}

func proofCellFingerprint(hash cell.Hash) uint32 {
	return uint32(hash[0]) |
		uint32(hash[1])<<8 |
		uint32(hash[2])<<16 |
		uint32(hash[3])<<24
}

// size is nil-safe: the masterchain path carries no estimator, and reading zero
// from it is the same answer a never-fed estimator gave.
//
// The total is taken under every shard lock, not summed from sixteen loads.
// Admission asks for this after every message and compares it against the
// collated-data limit, so a total that caught a charge between a cell and that
// cell's children would drop or admit a message on nothing but timing.
//
// The locks are uncontended as the pipeline stands, because the phases that
// record in parallel join before the loop that asks resumes. What they cost is
// then 94ns a call against 30ns for the lock-free sum, over the ~700 calls a
// mainnet block makes: a twentieth of a millisecond, against a collated-data
// limit nobody can afford to have decided by a race.
func (e *proofSizeEstimator) size() uint64 {
	if e == nil {
		return 0
	}
	e.lockShards(proofEstimatorAllShards)
	var total uint64
	for i := range e.shards {
		total += e.shards[i].bytes
	}
	e.unlockShards(proofEstimatorAllShards)
	return total
}

// maxCollationAttempts bounds how many times one slot's collation is rebuilt
// after the finished block turned out not to fit the consensus size limit. It
// is collator.cpp:57's MAX_ATTEMPTS, and collationAttempt below escalates the
// way that file does, attempt for attempt.
//
// Bounded on purpose, and gated on the build deadline besides. The production
// loop retries a retryable error until its context is cancelled, so a size
// failure that never changes its mind would mean no block at all for the slot
// rather than a smaller one.
const maxCollationAttempts = 5

// collationAttempt is everything one pass through the collator knows about the
// passes that failed before it. Narrowing the byte budget is the first lever
// and the only one that acts on the axis that actually overflowed; the rest are
// categories of work dropped wholesale, because a block that will not fit is
// worth less than a smaller block delivered inside the slot.
//
// The escalation is collator.cpp's, ported attempt for attempt so that a shard
// under the same load makes the same concessions in the same order here and
// there. Nothing about it is a heuristic of ours.
type collationAttempt struct {
	// index is the zero-based attempt number, collator.cpp's attempt_idx.
	index int
	// cap is the ceiling derived by aimBelow from the attempt that overflowed.
	// Zero on the first attempt and whenever no attempt has produced a usable
	// ratio yet, which leaves the configured limits standing.
	cap sizeBudgetCap
}

// skipDispatchTail drops the two policy-bounded dispatch-queue passes, keeping
// only the mandatory one that takes a single message per account.
// collator.cpp:4382-4385.
func (a collationAttempt) skipDispatchTail() bool { return a.index >= 1 }

// skipExternals drops inbound external messages entirely. They are the only
// class of work a block can decline without consequence: an external that is
// not included stays in the pool and is offered again next block, while an
// internal that is not imported holds up its queue.
// collator.cpp:4193-4196 and 4229-4232.
func (a collationAttempt) skipExternals() bool { return a.index >= 2 }

// limitDivisor scales the block limits themselves once dropping categories has
// not been enough. collator.cpp:863-873 halves bytes, gas and collated data on
// attempt 3 and quarters them on attempt 4; logical time is left alone in both,
// because lt delta has no bearing on how large the serialized block is.
func (a collationAttempt) limitDivisor() uint64 {
	switch {
	case a.index >= 4:
		return 4
	case a.index == 3:
		return 2
	default:
		return 1
	}
}

// sizeBudgetCap is an absolute ceiling on the limiter's byte estimate for one
// attempt. Zero means the configured limits stand as they are.
//
// A ceiling, rather than a fraction of the configured budget, because the two
// quantities are unrelated: the collator stops on the block limits from the
// network config, while ErrSizeLimit compares the serialized block against the
// consensus maximum. A workload can overflow the second while never coming near
// the first — it simply ran out of messages — and scaling an unreached budget
// would rebuild the identical block forever.
type sizeBudgetCap uint64

// aimBelow derives the ceiling for the next attempt from what the failed one
// actually did: it reached estimate while producing produced bytes, so the
// estimate that corresponds to the limit is scaled by the same ratio, with an
// eighth held back because the relationship is not exact.
func aimBelow(estimate, produced, limit uint64) sizeBudgetCap {
	if produced == 0 || estimate == 0 {
		return 0
	}
	cap := estimate * limit / produced * 875 / 1000
	if cap == 0 {
		cap = 1
	}
	return sizeBudgetCap(cap)
}

// retryUnderSizeLimit runs attempt until it produces a block or fails for a
// reason the next attempt's concessions cannot fix.
//
// Every index from 1 to maxCollationAttempts-1 withdraws something the previous
// one offered, so unlike the pure byte-ceiling narrowing this replaced, there is
// no attempt that can only rebuild the identical block: a ceiling that stops
// biting is no longer a dead end. The two ways out are the bound and the build
// deadline — collator.cpp:359 declines to repeat once the timeout has passed,
// for the same reason we do, that a narrower block delivered after the slot is
// worth no more than no block at all and costs the next slot its CPU.
//
// The attempt count is stamped on the candidate it returns. Retries are
// invisible from the outside otherwise: the block that finally fits looks
// exactly like a block that fit the first time, which is why a retry that
// succeeded left nothing at all in six hours of production logs.
func retryUnderSizeLimit(
	ctx context.Context,
	attempt func(collationAttempt) (*Candidate, error),
) (*Candidate, error) {
	current := collationAttempt{}
	for {
		candidate, err := attempt(current)
		if err == nil {
			if candidate != nil {
				candidate.Stats.CollationAttempts = uint32(current.index) + 1
			}
			return candidate, nil
		}

		var overflow sizeLimitError
		if !errors.As(err, &overflow) {
			return candidate, err
		}
		next := collationAttempt{index: current.index + 1, cap: current.cap}
		if next.index >= maxCollationAttempts {
			return candidate, collationAttemptsError{attempts: next.index, err: err}
		}
		if err := ctx.Err(); err != nil {
			// Both errors, and the deadline one first: callers upstream test the
			// returned error for context.Canceled and context.DeadlineExceeded to
			// tell a lost slot from a producer fault, and a chain that unwraps only
			// to ErrSizeLimit reports a clean shutdown as a collation failure.
			return candidate, collationAttemptsError{
				attempts: current.index + 1,
				err: fmt.Errorf(
					"the collation deadline passed before another attempt could run: %w: %w",
					err, overflow,
				),
			}
		}
		if ceiling := aimBelow(overflow.estimate, overflow.produced, overflow.limit); ceiling != 0 &&
			(next.cap == 0 || ceiling < next.cap) {
			next.cap = ceiling
		}
		// aimBelow stalls. It scales the ceiling by produced/limit measured on the
		// admission estimate, and once an attempt overshoots by a little the ratio
		// it derives is a little under one, so the eighth it holds back is the
		// whole of the movement: traced on the heavy fixture the ceiling walked
		// 327426 -> 204076 -> 198087 while the block it produced stayed within
		// 1.1% of the limit it had to clear. An attempt that rebuilds the identical
		// block is an attempt spent, so the ceiling is made to move whether or not
		// the ratio says it should.
		if current.cap != 0 {
			if forced := current.cap - current.cap/8; next.cap == 0 || next.cap > forced {
				next.cap = forced
			}
		}
		current = next
	}
}

// collationAttemptsError records how many passes through the collator a failure
// actually cost. Without it the only evidence is that the error unwraps to
// ErrSizeLimit, which is true after one attempt and after five alike — so a
// collation abandoned on the deadline after its first overflow was reported as
// one that had exhausted every concession, naming cuts it never made.
type collationAttemptsError struct {
	attempts int
	err      error
}

func (e collationAttemptsError) Error() string {
	return fmt.Sprintf("%s after %d collation attempts", e.err, e.attempts)
}

func (e collationAttemptsError) Unwrap() error { return e.err }

// collationAttemptsSpent reports how many attempts a failed collation ran, and
// whether that number is known at all: a failure raised outside the retry ladder
// carries no count and must not be described as if it had one.
func collationAttemptsSpent(err error) (int, bool) {
	var spent collationAttemptsError
	if !errors.As(err, &spent) {
		return 0, false
	}

	return spent.attempts, true
}

// sizeLimitError carries what overflowed and how far the limiter had counted, so
// the rebuild can aim for a block that fits.
type sizeLimitError struct {
	what     string
	produced uint64
	limit    uint64
	estimate uint64
}

func (e sizeLimitError) Error() string {
	return fmt.Sprintf("%s: %s is %d bytes, limit is %d", ErrSizeLimit, e.what, e.produced, e.limit)
}

func (e sizeLimitError) Unwrap() error { return ErrSizeLimit }

type limitThresholds [4]uint64

// blockLimitScale lifts the marks at which a block stops taking work, without
// touching the boundary a block may not cross.
//
// The three lower marks are this node's own admission policy: underload,
// where internals stop, and the soft and medium marks between which externals
// are allowed to carry the block further. The HARD threshold is not policy —
// it is what a validator replaying the block checks, so a block past it is a
// rejected block, and it stays exactly where the network config put it.
//
// Measured on the stand (2026-09-04): with the committee estimator raised, its
// cap went to 450-500 transactions and the blocks stayed at 382, stopping at
// the size marks instead — 1.06 MB on average against a 1.048 MB soft mark and
// a 1.57 MB medium one, with the collated proof on the same axis at 1.06-1.23
// MB. The marks, not the pace, were the binding limit.
//
// The step is deliberately small. Lifting the limits by half on this stand once
// cost the network a third of its throughput: bigger blocks are slower for the
// committee to validate, and the leader window is a fixed span of wall clock,
// so a block that buys transactions with bytes can lose whole slots.
//
// Measured on the stand (2026-09-04, five minutes per point, one loaded shard,
// same generator), scale against what a leader gets out of a window:
//
//	scale  tx/block  blocks/window  tx/window  TPS while leading
//	1.00      382.6           11.7       4464                880
//	1.05      392.1           10.0       3922                902
//	1.20      406.1            8.5       3452                926
//
// Both directions are monotone and they disagree: a bigger block does carry
// more transactions and does raise the rate while this node is producing, and
// it costs slots faster than it gains density. A leader gets a fixed share of
// windows, so what it contributes is transactions per window — and that falls.
// Left at 1: the marks the network config states are, on this committee, the
// ones that pay.
const blockLimitScale = 1.0

func scaleAdmissionLimits(p tlb.ParamLimits) tlb.ParamLimits {
	scaled := p
	scaled.Underload = uint32(float64(p.Underload) * blockLimitScale)
	scaled.SoftLimit = uint32(float64(p.SoftLimit) * blockLimitScale)
	if scaled.SoftLimit > p.HardLimit {
		scaled.SoftLimit = p.HardLimit
	}
	if scaled.Underload > scaled.SoftLimit {
		scaled.Underload = scaled.SoftLimit
	}

	return scaled
}

func newLimitThresholds(p tlb.ParamLimits) (limitThresholds, error) {
	if p.Underload > p.SoftLimit || p.SoftLimit > p.HardLimit {
		return limitThresholds{}, fmt.Errorf("%w: unordered block limits", ErrInvalidInput)
	}

	soft := uint64(p.SoftLimit)
	hard := uint64(p.HardLimit)
	return limitThresholds{
		uint64(p.Underload),
		soft,
		soft + (hard-soft)/2,
		hard,
	}, nil
}

func (l limitThresholds) classify(value uint64) LoadClass {
	for class, threshold := range l {
		if value < threshold {
			return LoadClass(class)
		}
	}
	return LoadHard
}

func (l limitThresholds) fits(class LoadClass, value uint64) bool {
	return class >= LoadHard || value < l[class]
}

type blockLimits struct {
	bytes        limitThresholds
	gas          limitThresholds
	ltDelta      limitThresholds
	collatedData limitThresholds
}

func blockLimitsAtTime(limits blockLimits, previousGenUtime, genUtime uint32) (blockLimits, error) {
	if uint64(genUtime) <= uint64(previousGenUtime)+15 || limits.ltDelta[3] <= 200 {
		return limits, nil
	}

	adjusted, err := newLimitThresholds(tlb.ParamLimits{Underload: 20, SoftLimit: 180, HardLimit: 200})
	if err != nil {
		return blockLimits{}, err
	}
	limits.ltDelta = adjusted
	return limits, nil
}

func parseBlockLimits(raw *tlb.BlockLimits) (blockLimits, error) {
	var source tlb.BlockLimitsV2
	switch limits := raw.Limits.(type) {
	case tlb.BlockLimitsV1:
		source.Bytes = limits.Bytes
		source.Gas = limits.Gas
		source.LTDelta = limits.LTDelta
		source.CollatedData = limits.Bytes
	case tlb.BlockLimitsV2:
		source = limits
	default:
		return blockLimits{}, fmt.Errorf("%w: unknown block limits type %T", ErrInvalidInput, raw.Limits)
	}

	// Size axes take the lifted admission marks; gas and logical time are left
	// as the config states them, because neither is what stops our blocks.
	bytesLimit, err := newLimitThresholds(scaleAdmissionLimits(source.Bytes))
	if err != nil {
		return blockLimits{}, fmt.Errorf("bytes: %w", err)
	}
	gasLimit, err := newLimitThresholds(source.Gas)
	if err != nil {
		return blockLimits{}, fmt.Errorf("gas: %w", err)
	}
	ltLimit, err := newLimitThresholds(source.LTDelta)
	if err != nil {
		return blockLimits{}, fmt.Errorf("lt delta: %w", err)
	}
	collatedLimit, err := newLimitThresholds(scaleAdmissionLimits(source.CollatedData))
	if err != nil {
		return blockLimits{}, fmt.Errorf("collated data: %w", err)
	}

	return blockLimits{
		bytes:        bytesLimit,
		gas:          gasLimit,
		ltDelta:      ltLimit,
		collatedData: collatedLimit,
	}, nil
}

// narrowBytes shrinks the byte thresholds this collation will admit against, so
// a rebuild after ErrSizeLimit stops earlier than the one that overflowed. Only
// the byte budget moves: gas, logical time and collated data had nothing to do
// with the failure, and lowering them would drop transactions for no reason.
//
// The HARD threshold is left where the network config put it, and that exclusion
// is load-bearing rather than tidy. The ceiling is an admission device — it
// decides when to stop taking messages — while the hard threshold is a statement
// about what a block may contain, and hardOverflow reads it to decide whether the
// mandatory own-queue drain still fits. Narrowing all four collapses the two:
// admission stops at the ceiling, which is now also the hard bound, so the drain
// that runs afterwards finds the block already at its hard limit and refuses with
// ErrMandatoryDequeueOverflow. That error is not a sizeLimitError, so it ends the
// retry ladder instead of narrowing it further, and the slot is lost with the
// blame pointing at the predecessor's queue. The reference has no hard-class
// check at all — grep cl_hard in collator.cpp and nothing comes back — so leaving
// this one bound alone is also the closer reading of it.
//
// classify reads the same threshold, and it is the load class the network would
// have given this block rather than the one our own rebuild ceiling implies,
// which is what the overload history and the split signal behind it want.
func (s *blockLimitStatus) narrowBytes(ceiling sizeBudgetCap) {
	if ceiling == 0 {
		return
	}
	for i := range s.limits.bytes[:LoadHard-1] {
		s.limits.bytes[i] = min(s.limits.bytes[i], uint64(ceiling))
	}
}

// applyAttempt is narrowBytes plus the wholesale scaling the late attempts do.
// The divisor lands on bytes, gas and collated data and not on logical time,
// which is collator.cpp:863-873's set: lt delta does not describe how large the
// serialized block is, and shrinking it would only refuse transactions that
// would have fit.
//
// ORDER MATTERS, and it is the opposite of the obvious one. The divisor scales
// the configured budget and the ceiling narrows it, so the budget is
// min(config/divisor, cap) and not min(config, cap)/divisor. Dividing the
// already-narrowed ceiling compounds the two: on a workload where the ceiling is
// the binding term — which is every workload that got this far, since the block
// overflowed the consensus maximum while the configured budget still had room —
// attempt 3 would admit half of what fits and attempt 4 a quarter. Measured on
// the heavy mainnet fixture, the compounded order shipped blocks at 50-54% of
// the limit where this order ships them at 88-94%.
//
// Division keeps the thresholds ordered, so a scaled limitThresholds is still a
// valid one and classify still walks it from underload to hard.
func (s *blockLimitStatus) applyAttempt(attempt collationAttempt) {
	if divisor := attempt.limitDivisor(); divisor > 1 {
		for _, thresholds := range []*limitThresholds{
			&s.limits.bytes,
			&s.limits.gas,
			&s.limits.collatedData,
		} {
			for i := range thresholds {
				thresholds[i] /= divisor
			}
		}
	}
	s.narrowBytes(attempt.cap)
}

type blockLimitStatus struct {
	limits           blockLimits
	startLT          uint64
	usage            *cell.ReadSet
	storage          *cell.CellStorageStat
	accountPaths     accountPathSizeEstimator
	accountPathBytes uint64

	transactions uint64
	// maxTransactions, when non-zero, is a ceiling on transactions below which
	// the block already counts as full for every admission class under hard.
	// It is the one axis the reference does not have, and it exists for the
	// first slot of a leader window: see firstSlotTransactions.
	maxTransactions   uint64
	extraOutMsgs      uint64
	publicLibraryDiff uint64
	gas               uint64
	endLT             uint64
	collatedData      uint64

	// admissionEstimate is estimatedBytes() as it stood when admission ended,
	// before the finished-state proof widened it. A rebuild narrows the same
	// thresholds admission compares against, so aimBelow has to scale a ratio
	// measured on the scale admission sees; taking it from the final estimate
	// instead loosens every ceiling by the whole state-proof share, which on a
	// thousand-transaction block exceeds the eighth aimBelow holds back and
	// leaves the rebuild landing above the limit it was aiming under.
	admissionEstimate uint64
}

// newBlockLimitStatus opens the limiter for a block whose storage walk is
// expected to reach expectedCells distinct ordinary cells and expectedProofCells
// cells outside the read set — the counts the previous build reported. Both are
// capacities; zero simply means the tables grow as they fill, which is what the
// first build of a chain does.
func newBlockLimitStatus(
	limits blockLimits,
	startLT uint64,
	usage *cell.ReadSet,
	expectedCells, expectedProofCells int,
) *blockLimitStatus {
	return &blockLimitStatus{
		limits:  limits,
		startLT: startLT,
		endLT:   startLT,
		usage:   usage,
		storage: cell.NewCellStorageStatSized(expectedCells, expectedProofCells),
	}
}

// estimatedBytes is the canonical block size estimate the limit classes are
// compared against. Touched accounts are counted elsewhere but deliberately
// left out of this sum; masterchain public-library visibility changes are
// weighed in instead.
func (s *blockLimitStatus) estimatedBytes() uint64 {
	stat := s.storage.TotalStat()
	return uint64(2000) + stat.Bits/8 + stat.Cells*12 + stat.InternalRefs*3 +
		stat.ExternalRefs*40 + s.accountPathBytes + s.transactions*200 +
		s.extraOutMsgs*300 + s.publicLibraryDiff*700
}

// rebuildEstimate is the estimate a size-limit rebuild must do its arithmetic
// with. It falls back to the live value for any caller that overflowed before
// admission closed, where the two are the same number anyway.
func (s *blockLimitStatus) rebuildEstimate() uint64 {
	if s.admissionEstimate != 0 {
		return s.admissionEstimate
	}
	return s.estimatedBytes()
}

func (s *blockLimitStatus) addProof(root *cell.Cell) error {
	return s.storage.AddProof(root, s.usage)
}

// accountPathSizeEstimator counts the union of predecessor ShardAccounts paths
// with the same weights estimatedBytes applies to a real proof walk. A child is
// initially a 40-byte external boundary; if another changed account descends
// into it, the boundary is replaced by that child's loaded-cell cost.
type accountPathSizeEstimator struct {
	seen  proofSeenHashSet
	bytes uint64
}

func (e *accountPathSizeEstimator) addLoadedCell(loaded *cell.Cell) {
	hash := loaded.HashKey()
	prior := e.seen.observe(hash, proofSeenLoaded, proofSeenBoundary)
	if prior&proofSeenLoaded != 0 {
		return
	}
	if prior&proofSeenBoundary != 0 {
		e.bytes -= 40
	}

	// One full-cell edge belongs to every loaded cell: the root edge for the
	// first one, and the parent edge for descendants.
	e.bytes += 12 + uint64(loaded.BitsSize())/8 + 3
	for i := 0; i < int(loaded.RefsNum()); i++ {
		ref := loaded.MustRefHashAt(i)
		prior = e.seen.observe(ref, proofSeenBoundary, 0)
		switch {
		case prior == 0:
			e.bytes += 40
		case prior&proofSeenLoaded != 0:
			// A new edge to content already reached by another path.
			e.bytes += 3
		default:
			// A repeated external edge is serialized again. Patricia paths do
			// not normally share it, but counting it keeps the estimate safe
			// for content-addressed augmentation references.
			e.bytes += 40
		}
	}
}

func (e *accountPathSizeEstimator) satisfyReference(hash cell.Hash) {
	prior := e.seen.flags(hash)
	if prior == 0 || prior&proofSeenLoaded != 0 {
		return
	}
	e.seen.observe(hash, proofSeenLoaded, proofSeenBoundary)
	e.bytes -= 40
}

func (e *accountPathSizeEstimator) reset() {
	clear(e.seen.slots)
	e.seen.count = 0
	if len(e.seen.hashes) > 0 {
		e.seen.hashes[0] = e.seen.hashes[0][:0]
		e.seen.hashes = e.seen.hashes[:1]
	}
	e.bytes = 0
}

func (s *blockLimitStatus) addAccountPath(
	path []*cell.Cell,
	existed bool,
	deleted bool,
	oldAccount *cell.Cell,
) {
	for _, loaded := range path {
		s.accountPaths.addLoadedCell(loaded)
	}
	if existed {
		// The old leaf points at the old Account. A changed leaf points at the
		// new Account, whose full tree addTransaction already charged.
		s.accountPaths.satisfyReference(oldAccount.HashKey())
		if deleted {
			// Removing a compressed Patricia tail can re-encode its surviving
			// sibling. One maximum-sized cell bounds that local rewrite.
			s.accountPaths.bytes += estimatedAccountDeletePathBytes
		}
		return
	}
	s.accountPaths.bytes += estimatedAccountInsertPathBytes
}

func (s *blockLimitStatus) commitAccountPaths() {
	s.accountPathBytes += s.accountPaths.bytes
	s.accountPaths.reset()
}

func (s *blockLimitStatus) addTransaction(account, transaction *cell.Cell, endLT uint64, gas uint64) error {
	if err := s.addProof(account); err != nil {
		return err
	}
	if err := s.storage.AddCell(transaction); err != nil {
		return err
	}
	if math.MaxUint64-s.gas < gas {
		return fmt.Errorf("%w: gas counter overflow", ErrInvalidInput)
	}

	s.gas += gas
	s.endLT = max(s.endLT, endLT)
	s.transactions++
	return nil
}

func (s *blockLimitStatus) classify() LoadClass {
	class := s.limits.bytes.classify(s.estimatedBytes())
	class = max(class, s.limits.gas.classify(s.gas))
	class = max(class, s.limits.ltDelta.classify(s.endLT-s.startLT))
	return max(class, s.limits.collatedData.classify(s.collatedData))
}

// hardOverflow reports the first limit axis standing at or past its HARD
// threshold, with the two numbers an operator needs and a flag for "none of
// them is". It is not the admission gate — fits() answers that, and answers it
// against the class a phase is allowed to reach — but the diagnosis for the one
// caller that has no admission decision to make:
// cleanupClaimedLocalDequeues, which must close the prefix the block already
// claimed and can only report that the result does not fit.
//
// All four axes are checked even though a dequeue can only move two of them.
// The drain does not own the block: it runs after the predecessor queue and
// dispatch roots are already charged, and naming the axis that actually went
// over is the whole point of the message.
func (s *blockLimitStatus) hardOverflow() (string, uint64, uint64, bool) {
	const hard = LoadHard - 1
	if bytes := s.estimatedBytes(); bytes >= s.limits.bytes[hard] {
		return "estimated block bytes", bytes, s.limits.bytes[hard], true
	}
	if s.collatedData >= s.limits.collatedData[hard] {
		return "collated data bytes", s.collatedData, s.limits.collatedData[hard], true
	}
	if s.gas >= s.limits.gas[hard] {
		return "gas", s.gas, s.limits.gas[hard], true
	}
	if delta := s.endLT - s.startLT; delta >= s.limits.ltDelta[hard] {
		return "logical time delta", delta, s.limits.ltDelta[hard], true
	}

	return "", 0, 0, false
}

func (s *blockLimitStatus) fits(class LoadClass) bool {
	// The transaction ceiling is an admission cap, not a limit axis: a capped
	// block reads as full to every phase that asks whether more work fits, so
	// its generated messages are enqueued the way a full block's are, while the
	// hard-overflow diagnosis and the size retry never see it.
	if class < LoadHard && s.maxTransactions != 0 && s.transactions >= s.maxTransactions {
		return false
	}
	return s.limits.bytes.fits(class, s.estimatedBytes()) &&
		s.limits.gas.fits(class, s.gas) &&
		s.limits.ltDelta.fits(class, s.endLT-s.startLT) &&
		s.limits.collatedData.fits(class, s.collatedData)
}
