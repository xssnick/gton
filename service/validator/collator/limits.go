package collator

import (
	"errors"
	"fmt"
	"math"
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
type proofSizeEstimator struct {
	mu    sync.Mutex
	seen  proofSeenHashSet
	bytes uint64
}

// newProofSizeEstimator opens an estimator sized for a block whose read set is
// expected to reach expectedReadCells. The set it keeps is larger than the read
// set — it holds every cell the record saw plus the pruned boundary of every
// child of one — and measured over mainnet blocks it lands at 1.22-1.39x the
// read count, so the table is asked for 1.4x and doubled for the half-full rule
// the probe relies on. It is a capacity and nothing else: a wrong estimate costs
// only the growth it avoided, and the set is dropped with the collation.
func newProofSizeEstimator(expectedReadCells int) *proofSizeEstimator {
	e := &proofSizeEstimator{}
	e.seen.presize(expectedReadCells * 7 / 5)
	return e
}

func (e *proofSizeEstimator) addLoadedCell(root *cell.Cell) {
	hash := root.HashKey()
	var refBuf [4]cell.Hash
	refs := refBuf[:root.RefsNum()]
	for i := range refs {
		refs[i] = root.MustRefHashAt(i)
	}
	loadedBytes := uint64(5+(root.BitsSize()+7)/8) + uint64(len(refs))*3

	// Explicit unlock: this runs tens of thousands of times per block.
	e.mu.Lock()
	seen, wasLoaded := e.seen.observe(hash, true)
	if seen && wasLoaded {
		e.mu.Unlock()
		return
	}
	if seen {
		// The cell stood in as a pruned branch until now; it is charged at its
		// loaded size instead.
		e.bytes -= estimatedPrunedBranchBytes
	}
	e.bytes += loadedBytes
	for _, ref := range refs {
		if seen, _ = e.seen.observe(ref, false); seen {
			continue
		}
		e.bytes += estimatedPrunedBranchBytes
	}
	e.mu.Unlock()
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

	e.mu.Lock()
	e.seen.observe(hash, true)
	e.mu.Unlock()
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
	// slots hold proofCellFingerprint(hash)<<32 | (index+1)<<1 | loaded bit;
	// zero is empty.
	slots  []uint64
	hashes [][]cell.Hash
	count  int
}

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

// observe records hash and reports whether it was already present and, if so,
// whether it was present as loaded. A loaded observation promotes an entry that
// was standing in as a pruned branch; a referenced one never demotes.
func (s *proofSeenHashSet) observe(hash cell.Hash, loaded bool) (bool, bool) {
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
		if uint32(slot>>32) == fingerprint && s.hashAt((uint32(slot)>>1)-1) == hash {
			if loaded && slot&1 == 0 {
				s.slots[pos] = slot | 1
			}
			return true, slot&1 != 0
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
	entry := uint64(s.count) << 1
	if loaded {
		entry |= 1
	}
	s.slots[pos] = uint64(fingerprint)<<32 | entry
	return false, false
}

func (s proofSeenHashSet) loaded(hash cell.Hash) bool {
	_, loaded := s.status(hash)
	return loaded
}

func (s proofSeenHashSet) status(hash cell.Hash) (bool, bool) {
	if len(s.slots) == 0 {
		return false, false
	}
	fingerprint := proofCellFingerprint(hash)
	mask := len(s.slots) - 1
	pos := int(fingerprint) & mask
	for range s.slots {
		slot := s.slots[pos]
		if slot == 0 {
			return false, false
		}
		if uint32(slot>>32) == fingerprint && s.hashAt((uint32(slot)>>1)-1) == hash {
			return true, slot&1 != 0
		}
		pos = (pos + 1) & mask
	}
	return false, false
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
		if seen, _ := shape.observe(current.HashKeyAt(0), true); seen {
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

// loadedHashes returns an immutable view after collation has stopped loading
// predecessor cells. The proof serializer runs only in that phase, so sharing
// the table avoids copying tens of thousands of hashes for every block.
func (e *proofSizeEstimator) loadedHashes() proofSeenHashSet {
	if e == nil {
		return proofSeenHashSet{}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.seen
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
func (e *proofSizeEstimator) size() uint64 {
	if e == nil {
		return 0
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.bytes
}

// sizeLimitRetries is how many times a collation is rebuilt after the finished
// block turned out not to fit the consensus size limit. The check that raises
// ErrSizeLimit runs on the serialized block, so the only way a retry can end
// differently is by admitting less: each attempt narrows the byte budget the
// limiter enforces, and after the last one the collation fails as before.
//
// Bounded on purpose. The production loop retries a retryable error until its
// context is cancelled, so a size failure that never changes its mind would
// mean no block at all for the slot rather than a smaller one.
const sizeLimitRetries = 2

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
// reason a narrower budget cannot fix.
func retryUnderSizeLimit(attempt func(sizeBudgetCap) (*Candidate, error)) (*Candidate, error) {
	var cap sizeBudgetCap
	for n := 0; ; n++ {
		candidate, err := attempt(cap)
		if err == nil || n >= sizeLimitRetries {
			return candidate, err
		}
		var overflow sizeLimitError
		if !errors.As(err, &overflow) {
			return candidate, err
		}
		next := aimBelow(overflow.estimate, overflow.produced, overflow.limit)
		if cap != 0 && next >= cap {
			// The ceiling stopped biting; nothing further to try.
			return candidate, err
		}
		cap = next
	}
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

	bytesLimit, err := newLimitThresholds(source.Bytes)
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
	collatedLimit, err := newLimitThresholds(source.CollatedData)
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
func (s *blockLimitStatus) narrowBytes(ceiling sizeBudgetCap) {
	if ceiling == 0 {
		return
	}
	for i := range s.limits.bytes {
		s.limits.bytes[i] = min(s.limits.bytes[i], uint64(ceiling))
	}
}

type blockLimitStatus struct {
	limits           blockLimits
	startLT          uint64
	usage            *cell.ReadSet
	storage          *cell.CellStorageStat
	accountPaths     accountPathSizeEstimator
	accountPathBytes uint64

	transactions      uint64
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
	seen, wasLoaded := e.seen.observe(hash, true)
	if seen && wasLoaded {
		return
	}
	if seen {
		e.bytes -= 40
	}

	// One full-cell edge belongs to every loaded cell: the root edge for the
	// first one, and the parent edge for descendants.
	e.bytes += 12 + uint64(loaded.BitsSize())/8 + 3
	for i := 0; i < int(loaded.RefsNum()); i++ {
		ref := loaded.MustRefHashAt(i)
		seen, wasLoaded = e.seen.observe(ref, false)
		switch {
		case !seen:
			e.bytes += 40
		case wasLoaded:
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
	seen, loaded := e.seen.status(hash)
	if !seen || loaded {
		return
	}
	e.seen.observe(hash, true)
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
	return s.limits.bytes.fits(class, s.estimatedBytes()) &&
		s.limits.gas.fits(class, s.gas) &&
		s.limits.ltDelta.fits(class, s.endLT-s.startLT) &&
		s.limits.collatedData.fits(class, s.collatedData)
}
