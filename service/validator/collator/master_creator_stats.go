package collator

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/big"
	"slices"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	blockCreateStatsTag       = uint64(0x17)
	creatorStatsTag           = uint64(0x4)
	creatorStatsKeyBits       = uint(256)
	discountedCounterFraction = uint(32)
	discountedCounterBits     = uint(256)

	// round(exp(-1/65536) * 2^256). A wider fixed-point base keeps the
	// deterministic result within the validator's +/-1 tolerance without
	// relying on platform-dependent floating-point rounding.
	discountedDecayBaseQ256 = "ffff00007fffd5555ffffdddde38e38138152151e6e58f6a0745f31d2b44ffd2"
)

var (
	discountedCounterHalfQ256 = new(big.Int).Lsh(big.NewInt(1), discountedCounterBits-1)
	discountedDecayPowersQ256 = buildDiscountedDecayPowersQ256()
)

type discountedCounter struct {
	lastUpdated uint32
	total       uint64
	count2048   uint64
	count65536  uint64
}

type creatorStats struct {
	masterchain discountedCounter
	shardchain  discountedCounter
}

// blockCreateStats is the block_create_stats#17 wrapper around the creator
// dictionary, kept as a dictionary rather than a decoded map on purpose.
//
// The dictionary holds one entry per validator that produced a block recently,
// while one masterchain block touches only the handful of creators named by its
// shard tops plus the aggregate key. Materializing it costs a full traversal per
// candidate — three of them on the masterchain validation path alone — to read
// a few dozen entries. Both the collation update and the validation check
// therefore work per key, exactly as the reference collator and validator do.
type blockCreateStats struct {
	dict *cell.Dictionary
}

type blockCreateStatsInput struct {
	enabled bool
	// previous is the statistics parseMasterStateInfoWithStats already opened
	// from the predecessor. A nil dictionary represents a predecessor without
	// the capability flag and therefore an empty creator dictionary.
	previous           blockCreateStats
	now                uint32
	shardBlockCreators map[[32]byte]uint32
	masterchainCreator [32]byte
}

type creatorStatsIncrement struct {
	masterchain uint32
	shardchain  uint32
}

func countMasterShardCreators(tops []ShardTop) (map[[32]byte]uint32, error) {
	counts := make(map[[32]byte]uint32)
	for i := range tops {
		for _, creator := range tops[i].Creators {
			if creator == ([32]byte{}) {
				continue
			}
			if counts[creator] == math.MaxUint32 {
				return nil, fmt.Errorf("%w: shard block creator %x count overflows uint32", ErrInvalidInput, creator)
			}
			counts[creator]++
		}
	}

	return counts, nil
}

// updateBlockCreateStats applies the creator increments collected for one
// masterchain block. A disabled capability deliberately returns no stats cell;
// the caller must clear both the McStateExtra flag and its optional value.
//
// The predecessor dictionary is copied and then mutated one touched key at a
// time, mirroring Collator::update_block_creator_count. Rebuilding the whole
// dictionary instead would rehash every node on every path for entries that did
// not move, and it would re-encode the predecessor's untouched nodes — a
// canonical Hashmap is determined by its key set, but preserving the exact
// predecessor subtrees keeps the result independent of any label-encoding
// choice the reference collator made.
func updateBlockCreateStats(input blockCreateStatsInput) (*cell.Cell, error) {
	if !input.enabled {
		return nil, nil
	}

	dict := cell.NewDict(creatorStatsKeyBits)
	if input.previous.dict != nil {
		// Copy is copy-on-write: it shares the predecessor root and only
		// replaces it locally as keys are set, so the caller's decoded state is
		// left untouched.
		dict = input.previous.dict.Copy()
	}

	increments, err := creatorStatsIncrements(input.shardBlockCreators, input.masterchainCreator)
	if err != nil {
		return nil, err
	}
	for _, creator := range sortedCreatorKeys(increments) {
		increment := increments[creator]
		entry, _, err := lookupCreatorStats(dict, creator)
		if err != nil {
			return nil, fmt.Errorf("%w: decode block creator statistics %x: %v", ErrInvalidInput, creator, err)
		}
		// The zero-increment guards are load-bearing: the reference collator
		// calls increase_by only for the component it is actually incrementing,
		// so relaxing an untouched counter here would move its lastUpdated and
		// decay its components for a creator the block never named.
		if increment.masterchain != 0 {
			if err = entry.masterchain.increaseBy(increment.masterchain, input.now); err != nil {
				return nil, fmt.Errorf("%w: increase masterchain creator counter %x: %v", ErrInvalidInput, creator, err)
			}
		}
		if increment.shardchain != 0 {
			if err = entry.shardchain.increaseBy(increment.shardchain, input.now); err != nil {
				return nil, fmt.Errorf("%w: increase shardchain creator counter %x: %v", ErrInvalidInput, creator, err)
			}
		}
		value, err := entry.toBuilder()
		if err != nil {
			return nil, fmt.Errorf("%w: serialize creator statistics %x: %v", ErrInvalidInput, creator, err)
		}
		if err = dict.SetBuilderByBytesKey(creator[:], value); err != nil {
			return nil, fmt.Errorf("%w: store creator statistics %x: %v", ErrInvalidInput, creator, err)
		}
	}

	builder := cell.BeginCell()
	if err = builder.StoreUInt(blockCreateStatsTag, 8); err != nil {
		return nil, fmt.Errorf("%w: serialize block creator statistics tag: %v", ErrInvalidInput, err)
	}
	if err = builder.StoreDict(dict); err != nil {
		return nil, fmt.Errorf("%w: serialize block creator statistics: %v", ErrInvalidInput, err)
	}
	return builder.EndCell(), nil
}

// lookupCreatorStats reads one creator entry. An absent key is the zero entry,
// exactly as block::unpack_CreatorStats treats a null value, and is reported
// through the second result so callers that must distinguish "absent" from
// "present and zero" can.
func lookupCreatorStats(dict *cell.Dictionary, creator [32]byte) (creatorStats, bool, error) {
	if dict == nil {
		return creatorStats{}, false, nil
	}
	value, err := dict.LoadValueByBytesKey(creator[:])
	if err != nil {
		if errors.Is(err, cell.ErrNoSuchKeyInDict) {
			return creatorStats{}, false, nil
		}
		return creatorStats{}, false, err
	}
	entry, err := loadCreatorStats(value)
	if err != nil {
		return creatorStats{}, false, err
	}
	return entry, true, nil
}

// verifyBlockCreateStatsUpdate takes both sides already opened by
// parseMasterStateInfoWithStats; a nil dictionary means the statistics are
// absent from that state.
//
// It is a port of ValidateQuery::check_block_create_stats and runs in two
// passes for the same reasons the reference does. The structural pass diffs the
// two dictionaries, which reports every entry that actually moved while
// skipping equal subtrees by hash — an untouched entry is pinned to the
// already-accepted predecessor by that hash and needs no re-derivation. The
// second pass covers the converse mistake the diff cannot see: a creator that
// was required to move and whose entry the candidate left alone.
func verifyBlockCreateStatsUpdate(
	previous blockCreateStats,
	next blockCreateStats,
	now uint32,
	shardBlockCreators map[[32]byte]uint32,
	masterchainCreator [32]byte,
) error {
	if next.dict == nil {
		return fmt.Errorf("%w: resulting block creator statistics are absent", ErrInvalidInput)
	}
	increments, err := creatorStatsIncrements(shardBlockCreators, masterchainCreator)
	if err != nil {
		return err
	}
	previousDict := previous.dict
	if previousDict == nil {
		previousDict = cell.NewDict(creatorStatsKeyBits)
	}

	changed := make(map[[32]byte]struct{}, len(increments))
	if err = previousDict.ScanDiffRaw(next.dict, func(view cell.DictDiffRawView) error {
		var creator [32]byte
		if view.KeyBits != creatorStatsKeyBits || len(view.Key) != len(creator) {
			return fmt.Errorf("creator statistics key has invalid size")
		}
		copy(creator[:], view.Key)
		changed[creator] = struct{}{}

		var oldEntry creatorStats
		if view.HasOld {
			oldValue := view.OldValue
			var decodeErr error
			oldEntry, decodeErr = loadCreatorStats(&oldValue)
			if decodeErr != nil {
				return fmt.Errorf("previous creator statistics %x: %w", creator, decodeErr)
			}
		}
		var newEntry creatorStats
		if view.HasNew {
			newValue := view.NewValue
			var decodeErr error
			newEntry, decodeErr = loadCreatorStats(&newValue)
			if decodeErr != nil {
				return fmt.Errorf("resulting creator statistics %x: %w", creator, decodeErr)
			}
		}
		return verifyOneCreatorStatsUpdate(creator, oldEntry, newEntry, view.HasNew, increments[creator], now)
	}); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}

	// The zero key carries the aggregate and is checked whether or not it moved,
	// so a candidate cannot leave a stale or two-zero aggregate behind on a
	// block that created nothing.
	required := make(map[[32]byte]struct{}, len(increments)+1)
	for creator := range increments {
		required[creator] = struct{}{}
	}
	required[[32]byte{}] = struct{}{}
	for _, creator := range sortedCreatorKeys(required) {
		if _, moved := changed[creator]; moved {
			continue
		}
		oldEntry, _, lookupErr := lookupCreatorStats(previousDict, creator)
		if lookupErr != nil {
			return fmt.Errorf("%w: previous creator statistics %x: %v", ErrInvalidInput, creator, lookupErr)
		}
		newEntry, newExists, lookupErr := lookupCreatorStats(next.dict, creator)
		if lookupErr != nil {
			return fmt.Errorf("%w: resulting creator statistics %x: %v", ErrInvalidInput, creator, lookupErr)
		}
		if err = verifyOneCreatorStatsUpdate(creator, oldEntry, newEntry, newExists, increments[creator], now); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidInput, err)
		}
	}

	return nil
}

// verifyOneCreatorStatsUpdate is check_one_block_creator_update: both counters
// must follow their increment, and an entry that survives with two zero totals
// should have been deleted instead.
func verifyOneCreatorStatsUpdate(
	creator [32]byte,
	oldEntry creatorStats,
	newEntry creatorStats,
	newExists bool,
	increment creatorStatsIncrement,
	now uint32,
) error {
	if err := verifyDiscountedCounterUpdate(
		oldEntry.masterchain,
		newEntry.masterchain,
		increment.masterchain,
		now,
	); err != nil {
		return fmt.Errorf("masterchain creator counter %x: %v", creator, err)
	}
	if err := verifyDiscountedCounterUpdate(
		oldEntry.shardchain,
		newEntry.shardchain,
		increment.shardchain,
		now,
	); err != nil {
		return fmt.Errorf("shardchain creator counter %x: %v", creator, err)
	}
	if newExists && newEntry.masterchain.total == 0 && newEntry.shardchain.total == 0 {
		return fmt.Errorf("creator %x contains two zero counters", creator)
	}
	return nil
}

func verifyDiscountedCounterUpdate(
	previous discountedCounter,
	next discountedCounter,
	increment uint32,
	now uint32,
) error {
	if next.total == 0 {
		if increment != 0 {
			return fmt.Errorf("zero counter was expected to increase by %d", increment)
		}
		if previous.total == 0 {
			return nil
		}
		relaxed := previous
		if err := relaxed.increaseBy(0, now); err != nil {
			return err
		}
		if !relaxed.almostZero() {
			return fmt.Errorf("non-stale counter was removed")
		}
		return nil
	}
	if increment == 0 {
		if previous != next {
			return fmt.Errorf("counter changed without an increment")
		}
		return nil
	}
	if next.total != previous.total+uint64(increment) || next.total < previous.total {
		return fmt.Errorf("counter total increment is not %d", increment)
	}
	expected := previous
	if err := expected.increaseBy(increment, now); err != nil {
		return err
	}
	if !expected.almostEqual(next) {
		return fmt.Errorf("discounted components differ from the expected update")
	}

	return nil
}

func creatorStatsIncrements(
	shardCreators map[[32]byte]uint32,
	masterchainCreator [32]byte,
) (map[[32]byte]creatorStatsIncrement, error) {
	increments := make(map[[32]byte]creatorStatsIncrement, len(shardCreators)+2)
	var totalShardBlocks uint64
	for _, creator := range sortedCreatorKeys(shardCreators) {
		count := shardCreators[creator]
		// The all-zero key is reserved for aggregate statistics, and it is omitted
		// zero creator IDs from both the per-creator and aggregate shard counts.
		if creator == ([32]byte{}) || count == 0 {
			continue
		}
		if totalShardBlocks+uint64(count) > math.MaxUint32 {
			return nil, fmt.Errorf("%w: total shard block creator count overflows uint32", ErrInvalidInput)
		}
		totalShardBlocks += uint64(count)
		increments[creator] = creatorStatsIncrement{shardchain: count}
	}

	hasMasterchainCreator := masterchainCreator != ([32]byte{})
	if hasMasterchainCreator {
		increment := increments[masterchainCreator]
		increment.masterchain = 1
		increments[masterchainCreator] = increment
	}
	if hasMasterchainCreator || totalShardBlocks != 0 {
		aggregate := creatorStatsIncrement{shardchain: uint32(totalShardBlocks)}
		if hasMasterchainCreator {
			aggregate.masterchain = 1
		}
		increments[[32]byte{}] = aggregate
	}

	return increments, nil
}

// openBlockCreateStats decodes the block_create_stats#17 header and hands back
// the creator dictionary without walking it.
//
// The walk is deliberately not performed here. The predecessor side comes from
// an already-accepted state, and the candidate side has every untouched subtree
// pinned by hash to that same predecessor through verifyBlockCreateStatsUpdate's
// diff, which validates exactly the nodes that moved. This is the same bound the
// reference validator works under: DictionaryBase::validate checks the root
// shape only, and scan_diff parses nothing else.
func openBlockCreateStats(root *cell.Cell) (blockCreateStats, error) {
	var loader cell.Slice
	err := root.BeginParseInto(&loader)
	if err != nil {
		return blockCreateStats{}, fmt.Errorf("%w: decode block creator statistics: %v", ErrInvalidInput, err)
	}
	tag, err := loader.LoadUInt(8)
	if err != nil || tag != blockCreateStatsTag {
		return blockCreateStats{}, fmt.Errorf("%w: invalid block creator statistics tag", ErrInvalidInput)
	}
	dict, err := loader.LoadDict(creatorStatsKeyBits)
	if err != nil || loader.BitsLeft() != 0 || loader.RefsNum() != 0 {
		return blockCreateStats{}, fmt.Errorf("%w: malformed block creator statistics dictionary", ErrInvalidInput)
	}

	return blockCreateStats{dict: dict}, nil
}

func loadCreatorStats(loader *cell.Slice) (creatorStats, error) {
	tag, err := loader.LoadUInt(4)
	if err != nil || tag != creatorStatsTag {
		return creatorStats{}, fmt.Errorf("invalid creator_info tag")
	}
	masterchain, err := loadDiscountedCounter(loader)
	if err != nil {
		return creatorStats{}, fmt.Errorf("masterchain counter: %w", err)
	}
	shardchain, err := loadDiscountedCounter(loader)
	if err != nil {
		return creatorStats{}, fmt.Errorf("shardchain counter: %w", err)
	}
	if loader.BitsLeft() != 0 || loader.RefsNum() != 0 {
		return creatorStats{}, fmt.Errorf("trailing creator statistics data")
	}
	return creatorStats{masterchain: masterchain, shardchain: shardchain}, nil
}

// toBuilder is what the collation update stores. The dictionary takes a builder,
// so finalizing a cell here would hash a value that is about to be re-embedded
// anyway — the reference collator hands set_builder an unfinalized CellBuilder
// for the same reason.
func (s creatorStats) toBuilder() (*cell.Builder, error) {
	builder := cell.BeginCell()
	if err := builder.StoreUInt(creatorStatsTag, 4); err != nil {
		return nil, err
	}
	if err := s.masterchain.store(builder); err != nil {
		return nil, fmt.Errorf("masterchain counter: %w", err)
	}
	if err := s.shardchain.store(builder); err != nil {
		return nil, fmt.Errorf("shardchain counter: %w", err)
	}
	return builder, nil
}

func (s creatorStats) toCell() (*cell.Cell, error) {
	builder, err := s.toBuilder()
	if err != nil {
		return nil, err
	}
	return builder.EndCell(), nil
}

func loadDiscountedCounter(loader *cell.Slice) (discountedCounter, error) {
	lastUpdated, err := loader.LoadUInt(32)
	if err != nil {
		return discountedCounter{}, err
	}
	total, err := loader.LoadUInt(64)
	if err != nil {
		return discountedCounter{}, err
	}
	count2048, err := loader.LoadUInt(64)
	if err != nil {
		return discountedCounter{}, err
	}
	count65536, err := loader.LoadUInt(64)
	if err != nil {
		return discountedCounter{}, err
	}
	counter := discountedCounter{
		lastUpdated: uint32(lastUpdated),
		total:       total,
		count2048:   count2048,
		count65536:  count65536,
	}
	// This is the only place a touched entry's counter is checked for structural
	// validity. verifyDiscountedCounterUpdate has two early returns that call
	// neither increaseBy nor store — an unchanged counter, and a zero one that
	// stayed zero — so dropping this check would accept states the reference
	// rejects, which validates on every unpack.
	if err = counter.validate(); err != nil {
		return discountedCounter{}, err
	}
	return counter, nil
}

func (c discountedCounter) store(builder *cell.Builder) error {
	if err := c.validate(); err != nil {
		return err
	}
	if err := builder.StoreUInt(uint64(c.lastUpdated), 32); err != nil {
		return err
	}
	if err := builder.StoreUInt(c.total, 64); err != nil {
		return err
	}
	if err := builder.StoreUInt(c.count2048, 64); err != nil {
		return err
	}
	return builder.StoreUInt(c.count65536, 64)
}

func (c discountedCounter) validate() error {
	if c.total == 0 {
		if c.count2048 != 0 || c.count65536 != 0 {
			return fmt.Errorf("zero total has non-zero discounted components")
		}
		return nil
	}
	if c.lastUpdated == 0 {
		return fmt.Errorf("non-zero total has zero update time")
	}
	return nil
}

func (c *discountedCounter) increaseBy(count, now uint32) error {
	if err := c.validate(); err != nil {
		return err
	}
	if now == 0 && (c.total != 0 || count != 0) {
		return fmt.Errorf("non-zero counter cannot be updated at unix time zero")
	}

	scaled := uint64(count) << discountedCounterFraction
	if c.total == 0 {
		*c = discountedCounter{
			lastUpdated: now,
			total:       uint64(count),
			count2048:   scaled,
			count65536:  scaled,
		}
		return nil
	}
	if uint64(count) > math.MaxUint64-c.total ||
		c.count2048 > math.MaxUint64-scaled ||
		c.count65536 > math.MaxUint64-scaled {
		// Reference collation checks overflow before decay, even when elapsed
		// time would otherwise make room for the increment.
		return fmt.Errorf("counter increment overflows uint64")
	}

	delta := uint32(0)
	// A reversed timestamp suppresses decay but still becomes lastUpdated.
	if now >= c.lastUpdated {
		delta = now - c.lastUpdated
	}
	count2048 := uint64(0)
	if delta < 48*2048 {
		count2048 = decayDiscountedValue(c.count2048, delta<<5)
	}
	count65536 := decayDiscountedValue(c.count65536, delta)
	*c = discountedCounter{
		lastUpdated: now,
		total:       c.total + uint64(count),
		count2048:   count2048 + scaled,
		count65536:  count65536 + scaled,
	}
	return nil
}

func (c discountedCounter) almostZero() bool {
	return c.count2048|c.count65536 <= 1
}

func (c discountedCounter) almostEqual(other discountedCounter) bool {
	return c.lastUpdated == other.lastUpdated && c.total == other.total &&
		uint64DistanceAtMostOne(c.count2048, other.count2048) &&
		uint64DistanceAtMostOne(c.count65536, other.count65536)
}

// decayDiscountedValue computes round(value * exp(-exponent / 2^16)). The
// protocol permits any implementation with an absolute error below one, so
// validator comparison accepts neighboring integer results.
func decayDiscountedValue(value uint64, exponent uint32) uint64 {
	if value == 0 || exponent == 0 {
		return value
	}
	if exponent>>16 > 45 {
		return 0
	}

	result := new(big.Int).SetUint64(value)
	result.Lsh(result, discountedCounterBits)
	for bit := 0; exponent != 0; bit++ {
		if exponent&1 != 0 {
			result = multiplyQ256(result, discountedDecayPowersQ256[bit], discountedCounterHalfQ256)
		}
		exponent >>= 1
	}
	result.Add(result, discountedCounterHalfQ256)
	result.Rsh(result, discountedCounterBits)
	return result.Uint64()
}

func buildDiscountedDecayPowersQ256() [32]*big.Int {
	var powers [32]*big.Int
	base, ok := new(big.Int).SetString(discountedDecayBaseQ256, 16)
	if !ok {
		panic("invalid internal discounted decay constant")
	}
	powers[0] = base
	for i := 1; i < len(powers); i++ {
		powers[i] = multiplyQ256(powers[i-1], powers[i-1], discountedCounterHalfQ256)
	}

	return powers
}

func multiplyQ256(left, right, half *big.Int) *big.Int {
	product := new(big.Int).Mul(left, right)
	product.Add(product, half)
	return product.Rsh(product, discountedCounterBits)
}

func uint64DistanceAtMostOne(left, right uint64) bool {
	if left >= right {
		return left-right <= 1
	}
	return right-left <= 1
}

func sortedCreatorKeys[V any](entries map[[32]byte]V) [][32]byte {
	keys := make([][32]byte, 0, len(entries))
	for key := range entries {
		keys = append(keys, key)
	}
	slices.SortFunc(keys, func(left, right [32]byte) int {
		return bytes.Compare(left[:], right[:])
	})
	return keys
}
