package groups

import (
	"errors"
	"fmt"
	"math"
	"math/bits"
	"sort"
	"time"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	masterchainWorkchain = int32(-1)
	basechainWorkchain   = int32(0)
	masterchainShard     = int64(math.MinInt64)
	masterchainShardBits = uint64(1) << 63
	invalidWorkchain     = int32(math.MinInt32)
	maxShardDepth        = 60
)

// ShardID is the comparable form of ton.ShardIdFull used by group tracking.
type ShardID struct {
	Workchain int32
	Shard     int64
}

// IsMasterchain reports whether the identifier is the masterchain root shard.
func (s ShardID) IsMasterchain() bool {
	return s.Workchain == masterchainWorkchain && s.Shard == masterchainShard
}

// PrefixBits returns the canonical shard prefix length. Masterchain and
// workchain root shards have a zero-length prefix.
func (s ShardID) PrefixBits() (uint32, error) {
	if s.Workchain == invalidWorkchain {
		return 0, fmt.Errorf("invalid workchain %d", s.Workchain)
	}
	if s.Shard == 0 {
		return 0, errors.New("shard prefix is zero")
	}

	prefixBits := 63 - bits.TrailingZeros64(uint64(s.Shard))
	if prefixBits > maxShardDepth {
		return 0, fmt.Errorf("shard prefix length %d exceeds %d", prefixBits, maxShardDepth)
	}
	if s.Workchain == masterchainWorkchain && !s.IsMasterchain() {
		return 0, errors.New("masterchain shard is not the root shard")
	}

	return uint32(prefixBits), nil
}

// IsValid reports whether the identifier has a canonical shard prefix.
func (s ShardID) IsValid() bool {
	_, err := s.PrefixBits()
	return err == nil
}

// ShardFSMKind identifies a scheduled shard topology transition.
type ShardFSMKind uint8

const (
	ShardFSMNone ShardFSMKind = iota
	ShardFSMSplit
	ShardFSMMerge
)

// ShardFSM is the scheduled topology transition from a ShardDescr.
type ShardFSM struct {
	Kind     ShardFSMKind
	UTime    uint32
	Interval uint32
}

// ShardDescription is the group-tracker projection of a ShardDescr.
type ShardDescription struct {
	Shard             ShardID
	Block             ton.BlockIDExt
	NextCatchainSeqno uint32
	BeforeSplit       bool
	BeforeMerge       bool
	FSM               ShardFSM
}

// StateInput identifies an applied masterchain state root.
type StateInput struct {
	Block ton.BlockIDExt
	Root  *cell.Cell
}

// State is the strictly parsed masterchain state projection used by Tracker.
type State struct {
	Block             ton.BlockIDExt
	GenUTime          uint32
	CatchainSeqno     uint32
	RotatedAllShards  bool
	IsKeyState        bool
	LastKeyBlockSeqno uint32
	ConfigRoot        *cell.Cell
	Shards            []ShardDescription
	configAddress     [32]byte
	accounts          *tlb.ShardAccountsAugDict
	shardIndex        map[ShardID]int
}

// Target describes an active group shard, the block or blocks from which its
// session starts, and the current masterchain descriptors governing its chain.
type Target struct {
	Shard      ShardID
	Genesis    []ton.BlockIDExt
	Registered []ShardDescription
}

type decodedShardDescription struct {
	Seqno             uint32
	RootHash          []byte
	FileHash          []byte
	BeforeSplit       bool
	BeforeMerge       bool
	Flags             uint8
	NextCatchainSeqno uint32
	SplitMergeAt      any
}

// ParseState strictly parses an applied masterchain state and every individual
// shard descriptor. Cross-descriptor split/merge consistency is checked when
// CurrentTargets or FutureShards resolves the corresponding topology.
func ParseState(input StateInput) (*State, error) {
	root, err := input.Root.Prewarm()
	if err != nil {
		return nil, fmt.Errorf("load masterchain state root: %w", err)
	}
	if root.IsSpecial() {
		return nil, errors.New("masterchain state root is special")
	}
	if err := validateMasterchainBlockID(input.Block); err != nil {
		return nil, err
	}

	stateLoader, err := root.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("parse masterchain state root: %w", err)
	}

	var shardState tlb.ShardStateUnsplit
	if err = tlb.LoadFromCell(&shardState, stateLoader); err != nil {
		return nil, fmt.Errorf("parse masterchain state: %w", err)
	}
	if err = requireEmptySlice(stateLoader, "masterchain state"); err != nil {
		return nil, err
	}
	if err = validateMasterchainStateHeader(shardState, input.Block); err != nil {
		return nil, err
	}
	if shardState.McStateExtra == nil {
		return nil, errors.New("masterchain state extra is absent")
	}

	extraLoader, err := shardState.McStateExtra.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("parse masterchain state extra root: %w", err)
	}

	var extra tlb.McStateExtra
	if err = tlb.LoadFromCell(&extra, extraLoader); err != nil {
		return nil, fmt.Errorf("parse masterchain state extra: %w", err)
	}
	if err = requireEmptySlice(extraLoader, "masterchain state extra"); err != nil {
		return nil, err
	}
	if extra.ConfigParams.Config.Params == nil || extra.ConfigParams.Config.Params.AsCell() == nil {
		return nil, errors.New("masterchain config dictionary is absent")
	}
	info, err := parseStateInfo(extra.Info)
	if err != nil {
		return nil, err
	}
	lastKeySeqno, err := lastKeyBlockSeqno(info, input.Block.SeqNo)
	if err != nil {
		return nil, err
	}
	shards, err := parseShardDescriptions(extra.ShardHashes, input.Block.SeqNo)
	if err != nil {
		return nil, err
	}
	shardIndex := make(map[ShardID]int, len(shards))
	for i := range shards {
		shardIndex[shards[i].Shard] = i
	}

	var configAddress [32]byte
	copy(configAddress[:], extra.ConfigParams.ConfigAddr)

	return &State{
		Block:             *input.Block.Copy(),
		GenUTime:          shardState.GenUTime,
		CatchainSeqno:     info.ValidatorInfo.CatchainSeqno,
		RotatedAllShards:  info.ValidatorInfo.NextCCUpdated,
		IsKeyState:        info.AfterKeyBlock,
		LastKeyBlockSeqno: lastKeySeqno,
		ConfigRoot:        extra.ConfigParams.Config.Params.AsCell(),
		Shards:            shards,
		configAddress:     configAddress,
		accounts:          shardState.Accounts.ShardAccounts,
		shardIndex:        shardIndex,
	}, nil
}

// CurrentTargets validates current cross-shard topology and returns targets in
// deterministic order. Merge genesis blocks are always ordered left then right.
func (s *State) CurrentTargets() ([]Target, error) {
	targets := []Target{{
		Shard:   masterchainShardID(),
		Genesis: []ton.BlockIDExt{s.Block},
	}}
	if s.Block.SeqNo == 0 {
		return targets, nil
	}

	basechainTargets := make([]Target, 0, len(s.Shards)+1)
	targetIDs := make(map[ShardID]struct{}, len(s.Shards)+1)
	merged := make(map[ShardID]struct{})
	for i := range s.Shards {
		description := &s.Shards[i]
		if description.Shard.Workchain != basechainWorkchain {
			continue
		}

		switch {
		case description.BeforeSplit:
			left, right, err := shardChildren(description.Shard)
			if err != nil {
				return nil, fmt.Errorf("split target %v: %w", description.Shard, err)
			}

			if err = appendTarget(&basechainTargets, targetIDs, Target{
				Shard:      left,
				Genesis:    []ton.BlockIDExt{description.Block},
				Registered: []ShardDescription{*description},
			}); err != nil {
				return nil, err
			}
			if err = appendTarget(&basechainTargets, targetIDs, Target{
				Shard:      right,
				Genesis:    []ton.BlockIDExt{description.Block},
				Registered: []ShardDescription{*description},
			}); err != nil {
				return nil, err
			}
		case description.BeforeMerge:
			parent, err := shardParent(description.Shard)
			if err != nil {
				return nil, fmt.Errorf("merge target %v: %w", description.Shard, err)
			}
			if _, done := merged[parent]; done {
				continue
			}

			left, right, err := shardChildren(parent)
			if err != nil {
				return nil, fmt.Errorf("merge target %v: %w", description.Shard, err)
			}
			leftIndex, leftFound := s.shardIndex[left]
			rightIndex, rightFound := s.shardIndex[right]
			if !leftFound || !rightFound {
				return nil, fmt.Errorf("merge target %v has incomplete sibling pair", parent)
			}
			leftDescription := &s.Shards[leftIndex]
			rightDescription := &s.Shards[rightIndex]
			if !leftDescription.BeforeMerge || !rightDescription.BeforeMerge {
				return nil, fmt.Errorf("merge target %v has mismatched sibling flags", parent)
			}

			if err = appendTarget(&basechainTargets, targetIDs, Target{
				Shard:   parent,
				Genesis: []ton.BlockIDExt{leftDescription.Block, rightDescription.Block},
				Registered: []ShardDescription{
					*leftDescription,
					*rightDescription,
				},
			}); err != nil {
				return nil, err
			}
			merged[parent] = struct{}{}
		default:
			if err := appendTarget(&basechainTargets, targetIDs, Target{
				Shard:      description.Shard,
				Genesis:    []ton.BlockIDExt{description.Block},
				Registered: []ShardDescription{*description},
			}); err != nil {
				return nil, err
			}
		}
	}

	sort.Slice(basechainTargets, func(i, j int) bool {
		return shardLess(basechainTargets[i].Shard, basechainTargets[j].Shard)
	})
	return append(targets, basechainTargets...), nil
}

// FutureShards returns masterchain and basechain shards whose tentative groups
// must be prepared at the explicit observation time and horizon.
func (s *State) FutureShards(asOf time.Time, horizon time.Duration) ([]ShardID, error) {
	deadline, err := futureDeadline(asOf, horizon)
	if err != nil {
		return nil, err
	}
	masterchain := masterchainShardID()
	if s.Block.SeqNo == 0 {
		return []ShardID{masterchain}, nil
	}

	result := []ShardID{masterchain}
	resultSet := make(map[ShardID]struct{}, len(s.Shards)+1)
	resultSet[masterchain] = struct{}{}
	merged := make(map[ShardID]struct{})
	for i := range s.Shards {
		description := &s.Shards[i]
		if description.Shard.Workchain != basechainWorkchain {
			continue
		}

		due := shardFSMDue(description.FSM, deadline)
		switch {
		case description.FSM.Kind == ShardFSMSplit && due:
			left, right, err := shardChildren(description.Shard)
			if err != nil {
				return nil, fmt.Errorf("future split %v: %w", description.Shard, err)
			}
			if err = appendShard(&result, resultSet, left); err != nil {
				return nil, err
			}
			if err = appendShard(&result, resultSet, right); err != nil {
				return nil, err
			}
		case description.FSM.Kind == ShardFSMMerge && due:
			parent, err := shardParent(description.Shard)
			if err != nil {
				return nil, fmt.Errorf("future merge %v: %w", description.Shard, err)
			}
			if _, done := merged[parent]; done {
				continue
			}

			left, right, err := shardChildren(parent)
			if err != nil {
				return nil, fmt.Errorf("future merge %v: %w", description.Shard, err)
			}
			leftIndex, leftFound := s.shardIndex[left]
			rightIndex, rightFound := s.shardIndex[right]
			leftDue := leftFound && s.Shards[leftIndex].FSM.Kind == ShardFSMMerge && shardFSMDue(s.Shards[leftIndex].FSM, deadline)
			rightDue := rightFound && s.Shards[rightIndex].FSM.Kind == ShardFSMMerge && shardFSMDue(s.Shards[rightIndex].FSM, deadline)
			if !leftDue || !rightDue {
				return nil, fmt.Errorf("future merge %v has incomplete sibling pair", parent)
			}

			if err = appendShard(&result, resultSet, parent); err != nil {
				return nil, err
			}
			merged[parent] = struct{}{}
		default:
			if err = appendShard(&result, resultSet, description.Shard); err != nil {
				return nil, err
			}
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return shardLess(result[i], result[j])
	})
	return result, nil
}

// CatchainSeqnoFor returns the catchain sequence number for a current or
// tentative group shard.
func (s *State) CatchainSeqnoFor(shard ShardID) (uint32, error) {
	if !shard.IsValid() {
		return 0, fmt.Errorf("invalid shard %v", shard)
	}
	if shard.IsMasterchain() {
		return s.CatchainSeqno, nil
	}

	for candidate := shard; ; candidate.Shard = int64(tlb.ShardID(uint64(candidate.Shard)).GetParent()) {
		if index, found := s.shardIndex[candidate]; found {
			return s.Shards[index].NextCatchainSeqno, nil
		}
		if uint64(candidate.Shard) == masterchainShardBits {
			break
		}
	}

	left, right, childrenErr := shardChildren(shard)
	if childrenErr != nil {
		return 0, fmt.Errorf("%w: catchain seqno for %v", ErrNotFound, shard)
	}
	leftIndex, leftFound := s.shardIndex[left]
	rightIndex, rightFound := s.shardIndex[right]
	if !leftFound && !rightFound {
		return 0, fmt.Errorf("%w: catchain seqno for %v", ErrNotFound, shard)
	}
	if !leftFound || !rightFound {
		return 0, fmt.Errorf("catchain seqno for %v has incomplete child pair", shard)
	}

	catchainSeqno := max(s.Shards[leftIndex].NextCatchainSeqno, s.Shards[rightIndex].NextCatchainSeqno)
	if catchainSeqno == math.MaxUint32 {
		return 0, fmt.Errorf("catchain seqno for %v overflows uint32", shard)
	}
	return catchainSeqno + 1, nil
}

func validateMasterchainBlockID(block ton.BlockIDExt) error {
	if block.Workchain != masterchainWorkchain || block.Shard != masterchainShard {
		return fmt.Errorf("block is not the masterchain root shard: %d:%016x", block.Workchain, uint64(block.Shard))
	}
	if len(block.RootHash) != 32 {
		return fmt.Errorf("masterchain block root hash length is %d", len(block.RootHash))
	}
	if len(block.FileHash) != 32 {
		return fmt.Errorf("masterchain block file hash length is %d", len(block.FileHash))
	}
	return nil
}

func validateMasterchainStateHeader(state tlb.ShardStateUnsplit, block ton.BlockIDExt) error {
	ident := state.ShardIdent
	if ident.PrefixBits < 0 || ident.PrefixBits > maxShardDepth {
		return fmt.Errorf("masterchain state shard prefix length is %d", ident.PrefixBits)
	}
	if ident.WorkchainID == invalidWorkchain {
		return errors.New("masterchain state has invalid workchain")
	}
	if !validShardIdentPrefix(ident) {
		return errors.New("masterchain state has non-canonical shard prefix")
	}
	if ident.PrefixBits != 0 || ident.WorkchainID != masterchainWorkchain || ident.ShardPrefix != 0 {
		return errors.New("state is not the masterchain root shard")
	}
	if state.Seqno != block.SeqNo {
		return fmt.Errorf("masterchain state seqno %d does not match block %d", state.Seqno, block.SeqNo)
	}
	return nil
}

func validShardIdentPrefix(ident tlb.ShardIdent) bool {
	if ident.PrefixBits == 0 {
		return ident.ShardPrefix == 0
	}
	mask := uint64(1)<<(64-uint(ident.PrefixBits)) - 1
	return ident.ShardPrefix&mask == 0
}

func parseStateInfo(root *cell.Cell) (*tlb.McStateExtraBlockInfo, error) {
	loader, err := root.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("parse masterchain state info root: %w", err)
	}

	var info tlb.McStateExtraBlockInfo
	if err = tlb.LoadFromCell(&info, loader); err != nil {
		return nil, fmt.Errorf("parse masterchain state info: %w", err)
	}
	if info.Flags&1 != 0 {
		magic, err := loader.LoadUInt(8)
		if err != nil {
			return nil, fmt.Errorf("parse masterchain block create stats magic: %w", err)
		}
		if magic != 0x17 && magic != 0x34 {
			return nil, fmt.Errorf("masterchain block create stats have unknown magic %x", magic)
		}

		// The payload is unrelated to group tracking and differs between the
		// #17 and #34 constructors. Validate the union tag, then keep the
		// constructor body opaque.
		if err = loader.SkipBitsAndRefs(loader.BitsLeft(), loader.RefsNum()); err != nil {
			return nil, fmt.Errorf("consume masterchain block create stats: %w", err)
		}
	}
	if err = requireEmptySlice(loader, "masterchain state info"); err != nil {
		return nil, err
	}
	return &info, nil
}

func lastKeyBlockSeqno(info *tlb.McStateExtraBlockInfo, currentSeqno uint32) (uint32, error) {
	if info.LastKeyBlock != nil && info.LastKeyBlock.SeqNo > currentSeqno {
		return 0, fmt.Errorf("last key block seqno %d is ahead of current block %d", info.LastKeyBlock.SeqNo, currentSeqno)
	}
	if info.AfterKeyBlock {
		return currentSeqno, nil
	}
	if info.LastKeyBlock == nil {
		return 0, nil
	}
	return info.LastKeyBlock.SeqNo, nil
}

func parseShardDescriptions(shardHashes *cell.Dictionary, masterchainSeqno uint32) ([]ShardDescription, error) {
	if shardHashes == nil || shardHashes.IsEmpty() {
		if masterchainSeqno == 0 {
			return []ShardDescription{}, nil
		}
		return nil, errors.New("masterchain state shard hashes are absent")
	}

	workchains, err := shardHashes.LoadAll()
	if err != nil {
		return nil, fmt.Errorf("load shard hashes: %w", err)
	}
	descriptions := make([]ShardDescription, 0)
	seen := make(map[ShardID]struct{})
	for _, workchain := range workchains {
		if workchain.Key.BitsLeft() != 32 || workchain.Key.RefsNum() != 0 {
			return nil, errors.New("shard hashes workchain key has invalid shape")
		}
		workchainID, err := workchain.Key.LoadInt(32)
		if err != nil {
			return nil, fmt.Errorf("load shard hashes workchain key: %w", err)
		}
		if workchain.Key.BitsLeft() != 0 || workchainID == int64(masterchainWorkchain) || workchainID == int64(invalidWorkchain) {
			return nil, fmt.Errorf("shard hashes contain invalid workchain %d", workchainID)
		}
		if workchain.Value.BitsLeft() != 0 || workchain.Value.RefsNum() != 1 {
			return nil, fmt.Errorf("shard hashes workchain %d wrapper must contain exactly one reference", workchainID)
		}
		treeRoot, err := workchain.Value.LoadRefCell()
		if err != nil {
			return nil, fmt.Errorf("load shard hashes workchain %d tree: %w", workchainID, err)
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
			shard, err := shardFromTreePath(int32(workchainID), path)
			if err != nil {
				return err
			}
			if _, duplicate := seen[shard]; duplicate {
				return fmt.Errorf("duplicate shard description %v", shard)
			}

			description, err := parseShardDescription(shard, value)
			if err != nil {
				return err
			}
			seen[shard] = struct{}{}
			descriptions = append(descriptions, description)
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("walk shard hashes workchain %d: %w", workchainID, err)
		}
	}

	sort.Slice(descriptions, func(i, j int) bool {
		return shardLess(descriptions[i].Shard, descriptions[j].Shard)
	})
	return descriptions, nil
}

func shardFromTreePath(workchain int32, path *cell.Cell) (ShardID, error) {
	loader, err := path.BeginParse()
	if err != nil {
		return ShardID{}, fmt.Errorf("parse shard tree path: %w", err)
	}
	if loader.RefsNum() != 0 || loader.BitsLeft() > maxShardDepth {
		return ShardID{}, fmt.Errorf("shard tree path depth %d is invalid", loader.BitsLeft())
	}

	shard := tlb.ShardID(masterchainShardBits)
	for loader.BitsLeft() > 0 {
		bit, err := loader.LoadUInt(1)
		if err != nil {
			return ShardID{}, fmt.Errorf("load shard tree path: %w", err)
		}
		shard = shard.GetChild(bit == 0)
	}
	return ShardID{Workchain: workchain, Shard: int64(shard)}, nil
}

func parseShardDescription(shard ShardID, root *cell.Cell) (ShardDescription, error) {
	root, err := root.Prewarm()
	if err != nil {
		return ShardDescription{}, fmt.Errorf("load shard description %v root: %w", shard, err)
	}
	if root.IsSpecial() {
		return ShardDescription{}, fmt.Errorf("shard description %v is special", shard)
	}
	loader, err := root.BeginParse()
	if err != nil {
		return ShardDescription{}, fmt.Errorf("parse shard description %v root: %w", shard, err)
	}
	magic, err := loader.LoadUInt(4)
	if err != nil {
		return ShardDescription{}, fmt.Errorf("parse shard description %v magic: %w", shard, err)
	}

	var decoded decodedShardDescription
	switch magic {
	case 0xa:
		var description tlb.ShardDesc
		if err = tlb.LoadFromCell(&description, loader, true); err != nil {
			return ShardDescription{}, fmt.Errorf("parse shard description %v: %w", shard, err)
		}
		decoded = decodedShardDescription{
			Seqno:             description.SeqNo,
			RootHash:          description.RootHash,
			FileHash:          description.FileHash,
			BeforeSplit:       description.BeforeSplit,
			BeforeMerge:       description.BeforeMerge,
			Flags:             description.Flags,
			NextCatchainSeqno: description.NextCatchainSeqNo,
			SplitMergeAt:      description.SplitMergeAt,
		}
	case 0xb:
		var description tlb.ShardDescB
		if err = tlb.LoadFromCell(&description, loader, true); err != nil {
			return ShardDescription{}, fmt.Errorf("parse shard description %v: %w", shard, err)
		}
		decoded = decodedShardDescription{
			Seqno:             description.SeqNo,
			RootHash:          description.RootHash,
			FileHash:          description.FileHash,
			BeforeSplit:       description.BeforeSplit,
			BeforeMerge:       description.BeforeMerge,
			Flags:             description.Flags,
			NextCatchainSeqno: description.NextCatchainSeqNo,
			SplitMergeAt:      description.SplitMergeAt,
		}
	default:
		return ShardDescription{}, fmt.Errorf("shard description %v has unknown magic %x", shard, magic)
	}
	if err = requireEmptySlice(loader, "shard description"); err != nil {
		return ShardDescription{}, fmt.Errorf("shard %v: %w", shard, err)
	}
	if decoded.Flags != 0 {
		return ShardDescription{}, fmt.Errorf("shard description %v has unsupported flags %d", shard, decoded.Flags)
	}
	if decoded.BeforeSplit && decoded.BeforeMerge {
		return ShardDescription{}, fmt.Errorf("shard description %v is both before split and merge", shard)
	}
	if len(decoded.RootHash) != 32 || len(decoded.FileHash) != 32 {
		return ShardDescription{}, fmt.Errorf("shard description %v has invalid block hashes", shard)
	}
	fsm, err := decodeShardFSM(decoded.SplitMergeAt)
	if err != nil {
		return ShardDescription{}, fmt.Errorf("shard description %v: %w", shard, err)
	}

	return ShardDescription{
		Shard: shard,
		Block: ton.BlockIDExt{
			Workchain: shard.Workchain,
			Shard:     shard.Shard,
			SeqNo:     decoded.Seqno,
			RootHash:  decoded.RootHash,
			FileHash:  decoded.FileHash,
		},
		NextCatchainSeqno: decoded.NextCatchainSeqno,
		BeforeSplit:       decoded.BeforeSplit,
		BeforeMerge:       decoded.BeforeMerge,
		FSM:               fsm,
	}, nil
}

func decodeShardFSM(value any) (ShardFSM, error) {
	switch fsm := value.(type) {
	case tlb.FutureSplitMergeNone:
		return ShardFSM{Kind: ShardFSMNone}, nil
	case tlb.FutureSplit:
		return ShardFSM{Kind: ShardFSMSplit, UTime: fsm.SplitUtime, Interval: fsm.Interval}, nil
	case tlb.FutureMerge:
		return ShardFSM{Kind: ShardFSMMerge, UTime: fsm.MergeUtime, Interval: fsm.Interval}, nil
	default:
		return ShardFSM{}, fmt.Errorf("unsupported split/merge FSM %T", value)
	}
}

func requireEmptySlice(loader *cell.Slice, name string) error {
	if loader.BitsLeft() != 0 || loader.RefsNum() != 0 {
		return fmt.Errorf("%s has %d trailing bits and %d trailing references", name, loader.BitsLeft(), loader.RefsNum())
	}
	return nil
}

func appendTarget(targets *[]Target, seen map[ShardID]struct{}, target Target) error {
	if _, duplicate := seen[target.Shard]; duplicate {
		return fmt.Errorf("duplicate group target %v", target.Shard)
	}
	seen[target.Shard] = struct{}{}
	*targets = append(*targets, target)
	return nil
}

func appendShard(shards *[]ShardID, seen map[ShardID]struct{}, shard ShardID) error {
	if _, duplicate := seen[shard]; duplicate {
		return fmt.Errorf("duplicate future shard %v", shard)
	}
	seen[shard] = struct{}{}
	*shards = append(*shards, shard)
	return nil
}

func masterchainShardID() ShardID {
	return ShardID{Workchain: masterchainWorkchain, Shard: masterchainShard}
}

func shardChildren(parent ShardID) (ShardID, ShardID, error) {
	if !parent.IsValid() || parent.IsMasterchain() {
		return ShardID{}, ShardID{}, fmt.Errorf("invalid basechain parent %v", parent)
	}
	depth := 63 - bits.TrailingZeros64(uint64(parent.Shard))
	if depth >= maxShardDepth {
		return ShardID{}, ShardID{}, fmt.Errorf("shard %v is at maximum depth", parent)
	}

	shard := tlb.ShardID(uint64(parent.Shard))
	return ShardID{Workchain: parent.Workchain, Shard: int64(shard.GetChild(true))},
		ShardID{Workchain: parent.Workchain, Shard: int64(shard.GetChild(false))}, nil
}

func shardParent(child ShardID) (ShardID, error) {
	if !child.IsValid() || child.IsMasterchain() {
		return ShardID{}, fmt.Errorf("invalid basechain child %v", child)
	}
	if uint64(child.Shard) == masterchainShardBits {
		return ShardID{}, fmt.Errorf("root shard %v has no parent", child)
	}
	parent := tlb.ShardID(uint64(child.Shard)).GetParent()
	return ShardID{Workchain: child.Workchain, Shard: int64(parent)}, nil
}

func shardIsAncestor(ancestor, shard ShardID) bool {
	return ancestor.Workchain == shard.Workchain &&
		tlb.ShardID(uint64(ancestor.Shard)).IsAncestor(tlb.ShardID(uint64(shard.Shard)))
}

func shardLess(left, right ShardID) bool {
	if left.Workchain != right.Workchain {
		return left.Workchain < right.Workchain
	}
	return uint64(left.Shard) < uint64(right.Shard)
}

func futureDeadline(asOf time.Time, horizon time.Duration) (time.Time, error) {
	if asOf.IsZero() {
		return time.Time{}, errors.New("future shard observation time is required")
	}
	if horizon < 0 {
		return time.Time{}, fmt.Errorf("future shard horizon is negative: %s", horizon)
	}

	deadline := asOf.Add(horizon)
	asOfUnix := asOf.Unix()
	deadlineUnix := deadline.Unix()
	if asOfUnix < 0 || asOfUnix > math.MaxUint32 || deadlineUnix < 0 || deadlineUnix > math.MaxUint32 {
		return time.Time{}, errors.New("future shard time is outside uint32 Unix range")
	}
	if deadline.Before(asOf) {
		return time.Time{}, errors.New("future shard deadline overflow")
	}
	return deadline, nil
}

func shardFSMDue(fsm ShardFSM, deadline time.Time) bool {
	return time.Unix(int64(fsm.UTime), 0).Before(deadline)
}
