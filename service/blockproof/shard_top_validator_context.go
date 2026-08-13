package blockproof

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/big"

	sharddomain "github.com/xssnick/gton/service/shard"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const shardTopValidatorMaxShardDepth = 60

const (
	shardTopDefaultMCCatchainLifetime    = 200
	shardTopDefaultShardCatchainLifetime = 200
	shardTopDefaultValidatorLifetime     = 3000
	shardTopDefaultShardValidators       = 7
)

// ErrShardTopValidatorContextNotReady reports that the selected masterchain
// state cannot yet derive the validator context for a shard.
var ErrShardTopValidatorContextNotReady = errors.New("blockproof: shard top validator context not ready")

// ShardTopValidatorContext is the immutable masterchain-state projection used
// to resolve the only two validator sessions accepted by TopBlockDescr.
type ShardTopValidatorContext struct {
	masterchain    ton.BlockIDExt
	config         tlb.BlockchainConfig
	catchainConfig tlb.CatchainConfig
	genUTime       uint32
	vertSeqno      uint32
	prevBlocks     *tlb.OldMcBlocksInfoAugDict
	shards         map[storage.ShardKey]shardTopRegistryEntry
}

type shardTopRegistryEntry struct {
	anchor            ShardTopAnchor
	nextCatchainSeqno uint32
}

// ShardTopAnchor is an immutable block identity from the masterchain shard
// registry. Fixed-size hashes keep the broadcast validation path allocation
// free without exposing mutable slice storage.
type ShardTopAnchor struct {
	Workchain int32
	Shard     int64
	Seqno     uint32
	RootHash  cell.Hash
	FileHash  cell.Hash
}

// Matches reports whether block is the exact registry block identity.
func (a ShardTopAnchor) Matches(block ton.BlockIDExt) bool {
	return a.Workchain == block.Workchain && a.Shard == block.Shard && a.Seqno == block.SeqNo &&
		bytes.Equal(a.RootHash[:], block.RootHash) && bytes.Equal(a.FileHash[:], block.FileHash)
}

// ShardTopAnchors is the current masterchain registry boundary for one
// TopBlockDescr target. Count is one for a linear/split continuation and two
// for an immediate merge.
type ShardTopAnchors struct {
	Left  ShardTopAnchor
	Right ShardTopAnchor
	Count uint8
}

// NewShardTopValidatorContext parses the exact masterchain state projection
// used for TopBlockDescr validator selection.
func NewShardTopValidatorContext(state *storage.BlockState) (*ShardTopValidatorContext, error) {
	if state == nil {
		return nil, errors.New("masterchain state is absent")
	}
	if state.Parsed == nil {
		return nil, fmt.Errorf("masterchain state %s is not parsed", storage.FormatBlockRef(state.Block))
	}
	if state.Parsed.McStateExtra == nil {
		return nil, fmt.Errorf("masterchain state %s has no extra", storage.FormatBlockRef(state.Block))
	}

	loader, err := state.Parsed.McStateExtra.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("parse masterchain state extra root: %w", err)
	}

	var extra tlb.McStateExtra
	if err = tlb.LoadFromCell(&extra, loader); err != nil {
		return nil, fmt.Errorf("parse masterchain state extra: %w", err)
	}
	if err = requireShardTopValidatorEmpty(loader, "masterchain state extra"); err != nil {
		return nil, err
	}
	if extra.ConfigParams.Config.Params == nil || extra.ConfigParams.Config.Params.AsCell() == nil {
		return nil, errors.New("masterchain state config is absent")
	}

	shards, err := parseShardTopRegistry(extra.ShardHashes)
	if err != nil {
		return nil, err
	}
	info, err := parseShardTopMasterchainInfo(extra.Info)
	if err != nil {
		return nil, err
	}

	config := tlb.BlockchainConfig{Root: extra.ConfigParams.Config.Params.AsCell()}
	return &ShardTopValidatorContext{
		masterchain:    state.Block,
		config:         config,
		catchainConfig: resolveShardTopCatchainConfig(config),
		genUTime:       state.Parsed.GenUTime,
		vertSeqno:      state.Parsed.VertSeqno,
		prevBlocks:     info.PrevBlocks,
		shards:         shards,
	}, nil
}

// BlockchainConfig returns a read-only view of the context's immutable config
// cell tree. A new wrapper prevents callers from replacing the stored root.
func (c *ShardTopValidatorContext) BlockchainConfig() *tlb.BlockchainConfig {
	return &tlb.BlockchainConfig{Root: c.config.Root}
}

// VerticalSeqno returns the vertical sequence number of the exact resident
// masterchain state used by this context.
func (c *ShardTopValidatorContext) VerticalSeqno() uint32 {
	return c.vertSeqno
}

// IsMasterchainAncestor checks a block reference against the exact
// OldMcBlocksInfo projection of this context. Missing heights are ordinary
// non-ancestors; malformed resident history is a context error.
func (c *ShardTopValidatorContext) IsMasterchainAncestor(ancestor ton.BlockIDExt) (bool, error) {
	if ancestor.Workchain != -1 || ancestor.Shard != sharddomain.Root || ancestor.SeqNo > c.masterchain.SeqNo {
		return false, nil
	}
	if ancestor.SeqNo == c.masterchain.SeqNo {
		return ancestor.Equals(&c.masterchain), nil
	}
	if c.prevBlocks == nil || c.prevBlocks.AugmentedDictionary == nil {
		return false, fmt.Errorf("%w: masterchain history is unavailable", ErrShardTopValidatorContextNotReady)
	}

	value, err := c.prevBlocks.LoadValueByIntKey(new(big.Int).SetUint64(uint64(ancestor.SeqNo)))
	if errors.Is(err, cell.ErrNoSuchKeyInDict) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("load masterchain ancestor %d: %w", ancestor.SeqNo, err)
	}

	var stored tlb.KeyExtBlkRef
	if err = tlb.LoadFromCell(&stored, value); err != nil {
		return false, fmt.Errorf("decode masterchain ancestor %d: %w", ancestor.SeqNo, err)
	}
	if err = requireShardTopValidatorEmpty(value, "masterchain ancestor"); err != nil {
		return false, err
	}
	if stored.BlkRef.SeqNo != ancestor.SeqNo || len(stored.BlkRef.RootHash) != 32 || len(stored.BlkRef.FileHash) != 32 {
		return false, fmt.Errorf("masterchain ancestor %d has an invalid block reference", ancestor.SeqNo)
	}

	return ancestor.Equals(&ton.BlockIDExt{
		Workchain: -1,
		Shard:     sharddomain.Root,
		SeqNo:     stored.BlkRef.SeqNo,
		RootHash:  stored.BlkRef.RootHash,
		FileHash:  stored.BlkRef.FileHash,
	}), nil
}

// ShardTopAnchors resolves the only registry layouts accepted for a target:
// an ancestor leaf, or the exact pair of its immediate children. Deeper merge
// layouts are not derivable by the current validator-session projection and
// are classified as a temporary context gap.
func (c *ShardTopValidatorContext) ShardTopAnchors(block ton.BlockIDExt) (ShardTopAnchors, error) {
	depth, err := sharddomain.PrefixLength(block.Shard)
	if err != nil {
		return ShardTopAnchors{}, fmt.Errorf("invalid block shard for %s: %w", storage.FormatBlockRef(block), err)
	}
	if depth > shardTopValidatorMaxShardDepth || block.Workchain == -1 || block.Workchain == math.MinInt32 {
		return ShardTopAnchors{}, fmt.Errorf("invalid shard top target %s", storage.FormatBlockRef(block))
	}

	key := storage.ShardKey{Workchain: block.Workchain}
	for ancestorDepth := depth; ; ancestorDepth-- {
		key.Shard, err = sharddomain.Ancestor(block.Shard, ancestorDepth)
		if err != nil {
			return ShardTopAnchors{}, fmt.Errorf("resolve shard ancestor for %s: %w", storage.FormatBlockRef(block), err)
		}
		if entry, found := c.shards[key]; found {
			return ShardTopAnchors{Left: entry.anchor, Count: 1}, nil
		}
		if ancestorDepth == 0 {
			break
		}
	}

	left, err := sharddomain.Child(block.Shard, true)
	if err != nil {
		return ShardTopAnchors{}, shardTopValidatorContextNotReady(block)
	}
	right, err := sharddomain.Child(block.Shard, false)
	if err != nil {
		return ShardTopAnchors{}, shardTopValidatorContextNotReady(block)
	}
	leftEntry, leftFound := c.shards[storage.ShardKey{Workchain: block.Workchain, Shard: left}]
	rightEntry, rightFound := c.shards[storage.ShardKey{Workchain: block.Workchain, Shard: right}]
	if !leftFound || !rightFound {
		return ShardTopAnchors{}, shardTopValidatorContextNotReady(block)
	}

	return ShardTopAnchors{Left: leftEntry.anchor, Right: rightEntry.anchor, Count: 2}, nil
}

// ValidatorsForCatchain resolves the exact current or next C++ validator-set
// pair for a shard and declared catchain sequence number.
func (c *ShardTopValidatorContext) ValidatorsForCatchain(
	block ton.BlockIDExt,
	declaredCC uint32,
) ([]*tlb.ValidatorAddr, error) {
	baseCC, err := c.baseCatchainSeqno(block)
	if err != nil {
		return nil, err
	}
	if baseCC == math.MaxUint32 {
		return nil, fmt.Errorf(
			"%w: %w: catchain context for %s is unavailable",
			ErrShardTopValidatorContextNotReady,
			storage.ErrNotFound,
			storage.FormatBlockRef(block),
		)
	}

	isNext := false
	switch {
	case declaredCC == baseCC:
	case declaredCC == baseCC+1:
		isNext = true
	default:
		return nil, fmt.Errorf(
			"%w: catchain seqno %d for %s is neither current %d nor next %d",
			storage.ErrNotFound,
			declaredCC,
			storage.FormatBlockRef(block),
			baseCC,
			baseCC+1,
		)
	}

	current, err := loadCurrentShardTopValidatorSet(c.config)
	if err != nil {
		return nil, shardTopValidatorRosterNotReady("current", err)
	}

	validators := current
	if isNext {
		validators, err = c.loadTentativeShardTopValidatorSet(current)
		if err != nil {
			return nil, err
		}
	}

	selected, err := validatorsForBlockWithCatchainConfig(
		&block,
		validators,
		declaredCC,
		c.catchainConfig,
	)
	if err != nil {
		return nil, shardTopValidatorRosterNotReady(
			"selected",
			fmt.Errorf("compute validators for %s catchain %d: %w", storage.FormatBlockRef(block), declaredCC, err),
		)
	}
	return selected, nil
}

func (c *ShardTopValidatorContext) baseCatchainSeqno(block ton.BlockIDExt) (uint32, error) {
	depth, err := sharddomain.PrefixLength(block.Shard)
	if err != nil {
		return 0, fmt.Errorf("invalid block shard for %s: %w", storage.FormatBlockRef(block), err)
	}
	if depth > shardTopValidatorMaxShardDepth {
		return 0, fmt.Errorf("block shard prefix depth %d exceeds %d", depth, shardTopValidatorMaxShardDepth)
	}
	if block.Workchain == -1 || block.Workchain == math.MinInt32 {
		return 0, fmt.Errorf("invalid shard block workchain %d", block.Workchain)
	}

	key := storage.ShardKey{Workchain: block.Workchain}
	for ancestorDepth := depth; ; ancestorDepth-- {
		key.Shard, err = sharddomain.Ancestor(block.Shard, ancestorDepth)
		if err != nil {
			return 0, fmt.Errorf("resolve shard ancestor for %s: %w", storage.FormatBlockRef(block), err)
		}
		if entry, found := c.shards[key]; found {
			return entry.nextCatchainSeqno, nil
		}
		if ancestorDepth == 0 {
			break
		}
	}

	left, err := sharddomain.Child(block.Shard, true)
	if err != nil {
		return 0, shardTopValidatorContextNotReady(block)
	}
	right, err := sharddomain.Child(block.Shard, false)
	if err != nil {
		return 0, shardTopValidatorContextNotReady(block)
	}

	leftEntry, leftFound := c.shards[storage.ShardKey{Workchain: block.Workchain, Shard: left}]
	rightEntry, rightFound := c.shards[storage.ShardKey{Workchain: block.Workchain, Shard: right}]
	if !leftFound || !rightFound {
		return 0, shardTopValidatorContextNotReady(block)
	}

	baseCC := max(leftEntry.nextCatchainSeqno, rightEntry.nextCatchainSeqno)
	if baseCC == math.MaxUint32 {
		return 0, fmt.Errorf(
			"%w: merged catchain seqno after %d overflows uint32",
			ErrShardTopValidatorContextNotReady,
			baseCC,
		)
	}
	return baseCC + 1, nil
}

func (c *ShardTopValidatorContext) loadTentativeShardTopValidatorSet(
	current tlb.ValidatorSetAny,
) (tlb.ValidatorSetAny, error) {
	next, err := loadShardTopValidatorSetParam(c.config, tlb.ConfigParamNextTempValidators)
	if errors.Is(err, tlb.ErrBlockchainConfigParamAbsent) {
		next, err = loadShardTopValidatorSetParam(c.config, tlb.ConfigParamNextValidators)
	}
	if errors.Is(err, tlb.ErrBlockchainConfigParamAbsent) {
		return current, nil
	}
	if err != nil {
		return tlb.ValidatorSetAny{}, shardTopValidatorRosterNotReady("next", err)
	}

	lifetime, err := shardCatchainLifetime(c.catchainConfig)
	if err != nil {
		return tlb.ValidatorSetAny{}, err
	}
	if lifetime == 0 {
		return tlb.ValidatorSetAny{}, errors.New("shard catchain lifetime must be positive")
	}

	boundary := (uint64(c.genUTime)/uint64(lifetime) + 1) * uint64(lifetime)
	if boundary > math.MaxUint32 {
		return tlb.ValidatorSetAny{}, fmt.Errorf("next shard catchain boundary %d overflows uint32", boundary)
	}

	since, err := shardTopValidatorSetSince(next)
	if err != nil {
		return tlb.ValidatorSetAny{}, err
	}
	if uint64(since) <= boundary {
		return next, nil
	}
	return current, nil
}

func shardTopValidatorContextNotReady(block ton.BlockIDExt) error {
	return fmt.Errorf(
		"%w: %w: catchain context for %s",
		ErrShardTopValidatorContextNotReady,
		storage.ErrNotFound,
		storage.FormatBlockRef(block),
	)
}

func shardTopValidatorRosterNotReady(name string, err error) error {
	return fmt.Errorf("%w: %s validator roster is unavailable: %w", ErrShardTopValidatorContextNotReady, name, err)
}

func loadCurrentShardTopValidatorSet(config tlb.BlockchainConfig) (tlb.ValidatorSetAny, error) {
	validators, err := loadShardTopValidatorSetParam(config, tlb.ConfigParamCurrentTempValidators)
	if errors.Is(err, tlb.ErrBlockchainConfigParamAbsent) {
		validators, err = loadShardTopValidatorSetParam(config, tlb.ConfigParamCurrentValidators)
	}
	if err != nil {
		return tlb.ValidatorSetAny{}, fmt.Errorf("load current validators: %w", err)
	}
	return validators, nil
}

func loadShardTopValidatorSetParam(config tlb.BlockchainConfig, id uint32) (tlb.ValidatorSetAny, error) {
	return loadExactShardTopConfigParam[tlb.ValidatorSetAny](config, id)
}

func loadExactShardTopConfigParam[T any](config tlb.BlockchainConfig, id uint32) (T, error) {
	var value T

	root, err := config.GetParam(id)
	if err != nil {
		return value, err
	}
	loader, err := root.BeginParse()
	if err != nil {
		return value, err
	}
	if err = tlb.LoadFromCell(&value, loader); err != nil {
		return value, err
	}
	if err = requireShardTopValidatorEmpty(loader, fmt.Sprintf("config parameter %d", id)); err != nil {
		return value, err
	}
	return value, nil
}

func resolveShardTopCatchainConfig(config tlb.BlockchainConfig) tlb.CatchainConfig {
	resolved, err := config.GetCatchainConfig()
	if err == nil {
		return resolved
	}

	// C++ deliberately uses these protocol defaults when parameter 28 is
	// absent or malformed. Retaining one resolved value keeps lifetime gating
	// and roster computation on the same configuration.
	return tlb.CatchainConfig{Config: tlb.CatchainConfigV2{
		McCatchainLifetime:      shardTopDefaultMCCatchainLifetime,
		ShardCatchainLifetime:   shardTopDefaultShardCatchainLifetime,
		ShardValidatorsLifetime: shardTopDefaultValidatorLifetime,
		ShardValidatorsNum:      shardTopDefaultShardValidators,
	}}
}

func shardCatchainLifetime(config tlb.CatchainConfig) (uint32, error) {
	switch value := config.Config.(type) {
	case tlb.CatchainConfigV1:
		return value.ShardCatchainLifetime, nil
	case tlb.CatchainConfigV2:
		return value.ShardCatchainLifetime, nil
	default:
		return 0, fmt.Errorf("unknown catchain config type %T", config.Config)
	}
}

func shardTopValidatorSetSince(validators tlb.ValidatorSetAny) (uint32, error) {
	switch value := validators.Validators.(type) {
	case tlb.ValidatorSet:
		return value.UTimeSince, nil
	case tlb.ValidatorSetExt:
		return value.UTimeSince, nil
	default:
		return 0, fmt.Errorf("unknown validator set type %T", validators.Validators)
	}
}

func parseShardTopRegistry(hashes *cell.Dictionary) (map[storage.ShardKey]shardTopRegistryEntry, error) {
	shards := make(map[storage.ShardKey]shardTopRegistryEntry)
	if hashes == nil || hashes.IsEmpty() {
		return shards, nil
	}

	workchains, err := hashes.LoadAll()
	if err != nil {
		return nil, fmt.Errorf("load shard hashes: %w", err)
	}
	for _, workchain := range workchains {
		if workchain.Key.BitsLeft() != 32 || workchain.Key.RefsNum() != 0 {
			return nil, errors.New("shard hashes workchain key has invalid shape")
		}
		workchainID, err := workchain.Key.LoadInt(32)
		if err != nil {
			return nil, fmt.Errorf("load shard hashes workchain key: %w", err)
		}
		if workchain.Key.BitsLeft() != 0 || workchainID == -1 || workchainID == math.MinInt32 {
			return nil, fmt.Errorf("shard hashes contain invalid workchain %d", workchainID)
		}
		if workchain.Value.BitsLeft() != 0 || workchain.Value.RefsNum() != 1 {
			return nil, fmt.Errorf("shard hashes workchain %d wrapper must contain exactly one reference", workchainID)
		}

		treeRoot, err := workchain.Value.LoadRefCell()
		if err != nil {
			return nil, fmt.Errorf("load shard hashes workchain %d tree: %w", workchainID, err)
		}
		if workchain.Value.BitsLeft() != 0 || workchain.Value.RefsNum() != 0 {
			return nil, fmt.Errorf("shard hashes workchain %d wrapper has trailing data", workchainID)
		}

		treeLoader, err := treeRoot.BeginParse()
		if err != nil {
			return nil, fmt.Errorf("parse shard hashes workchain %d tree root: %w", workchainID, err)
		}
		var tree tlb.BinTree
		if err = tree.LoadFromCell(treeLoader); err != nil {
			return nil, fmt.Errorf("parse shard hashes workchain %d tree: %w", workchainID, err)
		}

		err = tree.Walk(func(path *cell.Cell, value *cell.Cell) error {
			shardID, err := shardTopIDFromTreePath(path)
			if err != nil {
				return err
			}
			entry, err := parseShardTopRegistryEntry(int32(workchainID), shardID, value)
			if err != nil {
				return fmt.Errorf("parse shard %d:%016x: %w", workchainID, uint64(shardID), err)
			}

			key := storage.ShardKey{Workchain: int32(workchainID), Shard: shardID}
			if _, duplicate := shards[key]; duplicate {
				return fmt.Errorf("duplicate shard %d:%016x", workchainID, uint64(shardID))
			}
			shards[key] = entry
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("walk shard hashes workchain %d: %w", workchainID, err)
		}
	}

	return shards, nil
}

func shardTopIDFromTreePath(path *cell.Cell) (int64, error) {
	loader, err := path.BeginParse()
	if err != nil {
		return 0, fmt.Errorf("parse shard tree path: %w", err)
	}
	if loader.RefsNum() != 0 || loader.BitsLeft() > shardTopValidatorMaxShardDepth {
		return 0, fmt.Errorf("shard tree path depth %d is invalid", loader.BitsLeft())
	}

	shardID := sharddomain.Root
	for loader.BitsLeft() > 0 {
		bit, err := loader.LoadUInt(1)
		if err != nil {
			return 0, fmt.Errorf("load shard tree path: %w", err)
		}
		shardID, err = sharddomain.Child(shardID, bit == 0)
		if err != nil {
			return 0, fmt.Errorf("extend shard tree path: %w", err)
		}
	}
	return shardID, nil
}

func parseShardTopRegistryEntry(workchain int32, shard int64, root *cell.Cell) (shardTopRegistryEntry, error) {
	if root == nil || root.IsSpecial() {
		return shardTopRegistryEntry{}, errors.New("shard description is absent or special")
	}
	loader, err := root.BeginParse()
	if err != nil {
		return shardTopRegistryEntry{}, err
	}
	magic, err := loader.LoadUInt(4)
	if err != nil {
		return shardTopRegistryEntry{}, fmt.Errorf("load shard description magic: %w", err)
	}

	var seqno uint32
	var rootHash []byte
	var fileHash []byte
	var catchainSeqno uint32
	var flags uint8
	switch magic {
	case 0xa:
		var description tlb.ShardDesc
		if err = tlb.LoadFromCell(&description, loader, true); err != nil {
			return shardTopRegistryEntry{}, err
		}
		seqno = description.SeqNo
		rootHash = description.RootHash
		fileHash = description.FileHash
		catchainSeqno = description.NextCatchainSeqNo
		flags = description.Flags
	case 0xb:
		var description tlb.ShardDescB
		if err = tlb.LoadFromCell(&description, loader, true); err != nil {
			return shardTopRegistryEntry{}, err
		}
		seqno = description.SeqNo
		rootHash = description.RootHash
		fileHash = description.FileHash
		catchainSeqno = description.NextCatchainSeqNo
		flags = description.Flags
	default:
		return shardTopRegistryEntry{}, fmt.Errorf("unknown shard description magic %x", magic)
	}
	if err = requireShardTopValidatorEmpty(loader, "shard description"); err != nil {
		return shardTopRegistryEntry{}, err
	}
	if flags != 0 {
		return shardTopRegistryEntry{}, fmt.Errorf("shard description has unsupported flags %d", flags)
	}
	if len(rootHash) != 32 || len(fileHash) != 32 {
		return shardTopRegistryEntry{}, errors.New("shard description has invalid block hashes")
	}

	entry := shardTopRegistryEntry{
		anchor: ShardTopAnchor{
			Workchain: workchain,
			Shard:     shard,
			Seqno:     seqno,
		},
		nextCatchainSeqno: catchainSeqno,
	}
	copy(entry.anchor.RootHash[:], rootHash)
	copy(entry.anchor.FileHash[:], fileHash)

	return entry, nil
}

func parseShardTopMasterchainInfo(root *cell.Cell) (tlb.McStateExtraBlockInfo, error) {
	if root == nil {
		return tlb.McStateExtraBlockInfo{}, errors.New("masterchain state info is absent")
	}
	loader, err := root.BeginParse()
	if err != nil {
		return tlb.McStateExtraBlockInfo{}, fmt.Errorf("parse masterchain state info: %w", err)
	}

	var info tlb.McStateExtraBlockInfo
	if err = info.LoadFromCell(loader); err != nil {
		return tlb.McStateExtraBlockInfo{}, fmt.Errorf("parse masterchain state info: %w", err)
	}
	if info.PrevBlocks == nil || info.PrevBlocks.AugmentedDictionary == nil {
		return tlb.McStateExtraBlockInfo{}, errors.New("masterchain block history is absent")
	}

	return info, nil
}

func requireShardTopValidatorEmpty(loader *cell.Slice, name string) error {
	if loader.BitsLeft() != 0 || loader.RefsNum() != 0 {
		return fmt.Errorf("%s has %d trailing bits and %d trailing references", name, loader.BitsLeft(), loader.RefsNum())
	}
	return nil
}
