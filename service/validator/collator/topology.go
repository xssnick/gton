package collator

import (
	"fmt"
	"math"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"

	"github.com/xssnick/gton/service/shard"
	"github.com/xssnick/gton/service/validator/groups"
)

type topologyKind uint8

const (
	topologyUnknown topologyKind = iota
	topologyLinear
	topologyAfterSplit
	topologyAfterMerge
)

type shardTopology struct {
	kind              topologyKind
	target            tlb.ShardIdent
	session           *groups.Session
	registered        registeredShardTopology
	seqno             uint32
	previousGenLT     uint64
	previousGenUtime  uint32
	previousVertSeqno uint32
}

type registeredShardTopology struct {
	kind   topologyKind
	first  *groups.ShardDescription
	second *groups.ShardDescription
}

func resolveShardTopologyState(
	req ShardRequest,
	first *tlb.ShardStateUnsplit,
	second *tlb.ShardStateUnsplit,
) (shardTopology, error) {
	if first == nil {
		return shardTopology{}, fmt.Errorf("%w: first predecessor state is absent", ErrInvalidInput)
	}
	if (req.Previous2 == nil) != (second == nil) {
		return shardTopology{}, fmt.Errorf("%w: second predecessor and its state must be present together", ErrInvalidInput)
	}
	if req.Shard.IsMasterchain() {
		return shardTopology{}, fmt.Errorf("%w: shard topology resolver does not accept masterchain", ErrInvalidInput)
	}

	target, err := topologyShardIdent(req.Shard)
	if err != nil {
		return shardTopology{}, err
	}
	if err = validateTopologyPredecessor("first", &req.Previous, first); err != nil {
		return shardTopology{}, err
	}
	if req.Previous2 != nil {
		if err = validateTopologyPredecessor("second", req.Previous2, second); err != nil {
			return shardTopology{}, err
		}
	}

	topology := shardTopology{
		target:            target,
		previousGenLT:     first.GenLT,
		previousGenUtime:  first.GenUTime,
		previousVertSeqno: first.VertSeqno,
	}
	maxSeqno := req.Previous.ID.SeqNo

	if req.Previous2 == nil {
		sameShard := req.Previous.ID.Workchain == req.Shard.Workchain &&
			req.Previous.ID.Shard == req.Shard.Shard
		if sameShard {
			if first.BeforeSplit {
				return shardTopology{}, fmt.Errorf("%w: linear predecessor is marked before split", ErrInvalidInput)
			}
			topology.kind = topologyLinear
		} else {
			if req.Previous.ID.Workchain != req.Shard.Workchain ||
				!shard.IsDirectChild(req.Previous.ID.Shard, req.Shard.Shard) {
				return shardTopology{}, fmt.Errorf("%w: split predecessor is not the target's direct parent", ErrInvalidInput)
			}
			if !first.BeforeSplit {
				return shardTopology{}, fmt.Errorf("%w: split predecessor is not marked before split", ErrInvalidInput)
			}
			topology.kind = topologyAfterSplit
		}
	} else {
		left, err := shard.Child(req.Shard.Shard, true)
		if err != nil {
			return shardTopology{}, fmt.Errorf("%w: derive merge children: %v", ErrInvalidInput, err)
		}
		right, err := shard.Child(req.Shard.Shard, false)
		if err != nil {
			return shardTopology{}, fmt.Errorf("%w: derive merge children: %v", ErrInvalidInput, err)
		}
		if req.Previous.ID.Workchain != req.Shard.Workchain || req.Previous.ID.Shard != left ||
			req.Previous2.ID.Workchain != req.Shard.Workchain || req.Previous2.ID.Shard != right {
			return shardTopology{}, fmt.Errorf("%w: merge predecessors are not the target's ordered direct children", ErrInvalidInput)
		}
		// A before-split state can only be consumed by one of its children. It
		// cannot also participate in a merge without skipping that transition.
		if first.BeforeSplit || second.BeforeSplit {
			return shardTopology{}, fmt.Errorf("%w: merge predecessor is marked before split", ErrInvalidInput)
		}
		if first.GlobalID != second.GlobalID {
			return shardTopology{}, fmt.Errorf("%w: merge predecessors have different global ids", ErrInvalidInput)
		}
		topology.kind = topologyAfterMerge
		maxSeqno = max(maxSeqno, req.Previous2.ID.SeqNo)
		topology.previousGenLT = max(topology.previousGenLT, second.GenLT)
		topology.previousGenUtime = max(topology.previousGenUtime, second.GenUTime)
		topology.previousVertSeqno = max(topology.previousVertSeqno, second.VertSeqno)
	}

	if maxSeqno == math.MaxUint32 {
		return shardTopology{}, fmt.Errorf("%w: predecessor block sequence number overflow", ErrInvalidInput)
	}
	topology.seqno = maxSeqno + 1

	return topology, nil
}

func bindShardTopologySession(req ShardRequest, topology *shardTopology) error {
	session, err := topologySession(req.Masterchain, req.Shard)
	if err != nil {
		return err
	}
	registered, err := resolveRegisteredShardTopology(session)
	if err != nil {
		return err
	}
	if err = validateRegisteredShardTopology(req, topology, registered); err != nil {
		return err
	}
	if err = validateTopologyBeforeSplit(registered, req.BeforeSplit, req.Header.GenUtime); err != nil {
		return err
	}

	switch topology.kind {
	case topologyAfterSplit:
		if !sameGenesis(session.Genesis, &req.Previous.ID) {
			return fmt.Errorf("%w: split session genesis differs from the parent block", ErrInvalidInput)
		}
	case topologyAfterMerge:
		if !sameGenesis(session.Genesis, &req.Previous.ID, &req.Previous2.ID) {
			return fmt.Errorf("%w: merge session genesis differs from the ordered child blocks", ErrInvalidInput)
		}
	}
	topology.session = session
	topology.registered = registered
	return nil
}

func resolveRegisteredShardTopology(session *groups.Session) (registeredShardTopology, error) {
	switch len(session.Registered) {
	case 1:
		registered := &session.Registered[0]
		if err := validateRegisteredShardDescription(registered); err != nil {
			return registeredShardTopology{}, err
		}

		kind := topologyUnknown
		switch {
		case registered.Shard == session.Shard:
			kind = topologyLinear
		case registered.Shard.Workchain == session.Shard.Workchain &&
			shard.IsDirectChild(registered.Shard.Shard, session.Shard.Shard):
			kind = topologyAfterSplit
		default:
			return registeredShardTopology{}, fmt.Errorf(
				"%w: registered shard is neither the target nor its direct parent",
				ErrInvalidInput,
			)
		}

		return registeredShardTopology{kind: kind, first: registered}, nil
	case 2:
		left := &session.Registered[0]
		right := &session.Registered[1]
		if err := validateRegisteredShardDescription(left); err != nil {
			return registeredShardTopology{}, err
		}
		if err := validateRegisteredShardDescription(right); err != nil {
			return registeredShardTopology{}, err
		}

		leftShard, err := shard.Child(session.Shard.Shard, true)
		if err != nil {
			return registeredShardTopology{}, fmt.Errorf("%w: derive registered left child: %v", ErrInvalidInput, err)
		}
		rightShard, err := shard.Child(session.Shard.Shard, false)
		if err != nil {
			return registeredShardTopology{}, fmt.Errorf("%w: derive registered right child: %v", ErrInvalidInput, err)
		}
		orderedChildren := left.Shard == (groups.ShardID{Workchain: session.Shard.Workchain, Shard: leftShard}) &&
			right.Shard == (groups.ShardID{Workchain: session.Shard.Workchain, Shard: rightShard})
		if !orderedChildren {
			return registeredShardTopology{}, fmt.Errorf(
				"%w: registered merge shards are not the target's ordered direct children",
				ErrInvalidInput,
			)
		}

		return registeredShardTopology{kind: topologyAfterMerge, first: left, second: right}, nil
	default:
		return registeredShardTopology{}, fmt.Errorf(
			"%w: active shard session has %d registered shard descriptions",
			ErrInvalidInput,
			len(session.Registered),
		)
	}
}

func validateRegisteredShardDescription(description *groups.ShardDescription) error {
	if !description.Shard.IsValid() || description.Shard.IsMasterchain() {
		return fmt.Errorf("%w: registered shard description has an invalid shard", ErrInvalidInput)
	}
	block := &description.Block
	if block.Workchain != description.Shard.Workchain || block.Shard != description.Shard.Shard {
		return fmt.Errorf("%w: registered shard description block belongs to another shard", ErrInvalidInput)
	}
	if len(block.RootHash) != 32 || len(block.FileHash) != 32 {
		return fmt.Errorf("%w: registered shard description block hashes must be 256 bits", ErrInvalidInput)
	}

	return nil
}

func validateRegisteredShardTopology(
	req ShardRequest,
	topology *shardTopology,
	registered registeredShardTopology,
) error {
	var second *ton.BlockIDExt
	if req.Previous2 != nil {
		second = &req.Previous2.ID
	}

	return admitRegisteredShardChain(registered, topology.kind, req.Previous.ID, second)
}

// admitRegisteredShardChain is the registry-versus-predecessor rule taken from
// block ids alone, so it can be asked before a build exists. It is the hard
// protocol rule of the reference validator — ValidateQuery::check_prev_block
// (cppnode/ton/validator/impl/validate-query.cpp:1197-1213) and
// check_prev_block_exact (:1224-1233) — and it stays one function on purpose:
// the per-slot masterchain view pick asks it as an admission test and the build
// asks it again as a non-retryable ErrInvalidInput, and the two may never
// drift. A view the pick admitted must be one the build accepts.
func admitRegisteredShardChain(
	registered registeredShardTopology,
	kind topologyKind,
	first ton.BlockIDExt,
	second *ton.BlockIDExt,
) error {
	switch registered.kind {
	case topologyLinear:
		if kind != topologyLinear {
			return fmt.Errorf("%w: split or merge transition is absent from the masterchain shard registry", ErrInvalidInput)
		}
		if err := validateRegisteredPredecessor(registered.first.Block, first); err != nil {
			return err
		}
		if registered.first.BeforeSplit {
			return fmt.Errorf("%w: registered linear predecessor is marked before split", ErrInvalidInput)
		}
		if registered.first.BeforeMerge {
			return fmt.Errorf("%w: registered linear predecessor is marked before merge", ErrInvalidInput)
		}
	case topologyAfterSplit:
		if !registered.first.BeforeSplit {
			return fmt.Errorf("%w: registered split parent is not marked before split", ErrInvalidInput)
		}

		switch kind {
		case topologyAfterSplit:
			return validateExactRegisteredPredecessor(registered.first.Block, first)
		case topologyLinear:
			return validateRegisteredPredecessor(registered.first.Block, first)
		default:
			return fmt.Errorf("%w: predecessor topology conflicts with the registered split", ErrInvalidInput)
		}
	case topologyAfterMerge:
		if !registered.first.BeforeMerge || !registered.second.BeforeMerge {
			return fmt.Errorf("%w: registered merge children are not both marked before merge", ErrInvalidInput)
		}

		switch kind {
		case topologyAfterMerge:
			if second == nil {
				return fmt.Errorf("%w: merge topology has no second predecessor", ErrInvalidInput)
			}
			if err := validateExactRegisteredPredecessor(registered.first.Block, first); err != nil {
				return err
			}
			return validateExactRegisteredPredecessor(registered.second.Block, *second)
		case topologyLinear:
			return validateMergedRegisteredPredecessor(registered, first)
		default:
			return fmt.Errorf("%w: predecessor topology conflicts with the registered merge", ErrInvalidInput)
		}
	default:
		return fmt.Errorf("%w: unknown registered shard topology", ErrInvalidInput)
	}

	return nil
}

// predecessorTopologyKind classifies the shard transition from block ids alone.
// resolveShardTopologyState reaches the same verdict from the predecessor
// states and stays the binding the build uses; this exists because the view
// pick runs before a topology is resolved.
//
// Its errors are view-independent — no other masterchain view repairs a
// malformed predecessor set — so the pick returns them unchanged instead of
// treating a refusal as a reason to step down to an older view. The
// before-split consistency checks resolveShardTopologyState makes against the
// predecessor STATES stay there for the same reason.
func predecessorTopologyKind(target groups.ShardID, previous []PreviousBlock) (topologyKind, error) {
	switch len(previous) {
	case 1:
		if previous[0].ID.Workchain == target.Workchain && previous[0].ID.Shard == target.Shard {
			return topologyLinear, nil
		}
		if previous[0].ID.Workchain != target.Workchain ||
			!shard.IsDirectChild(previous[0].ID.Shard, target.Shard) {
			return 0, fmt.Errorf("%w: split predecessor is not the target's direct parent", ErrInvalidInput)
		}
		return topologyAfterSplit, nil
	case 2:
		left, err := shard.Child(target.Shard, true)
		if err != nil {
			return 0, fmt.Errorf("%w: derive merge children: %v", ErrInvalidInput, err)
		}
		right, err := shard.Child(target.Shard, false)
		if err != nil {
			return 0, fmt.Errorf("%w: derive merge children: %v", ErrInvalidInput, err)
		}
		if previous[0].ID.Workchain != target.Workchain || previous[0].ID.Shard != left ||
			previous[1].ID.Workchain != target.Workchain || previous[1].ID.Shard != right {
			return 0, fmt.Errorf("%w: merge predecessors are not the target's ordered direct children", ErrInvalidInput)
		}
		return topologyAfterMerge, nil
	default:
		return 0, fmt.Errorf("%w: shard build needs one or two predecessors", ErrInvalidInput)
	}
}

func validateRegisteredPredecessor(listed, previous ton.BlockIDExt) error {
	switch {
	case listed.SeqNo > previous.SeqNo:
		return fmt.Errorf(
			"%w: masterchain shard registry contains block %d newer than predecessor %d",
			ErrInvalidInput,
			listed.SeqNo,
			previous.SeqNo,
		)
	case listed.SeqNo == previous.SeqNo && !listed.Equals(&previous):
		return fmt.Errorf(
			"%w: masterchain shard registry contains another block at predecessor height %d",
			ErrInvalidInput,
			previous.SeqNo,
		)
	case previous.SeqNo-listed.SeqNo >= 8:
		return fmt.Errorf(
			"%w: predecessor extends the registered shard chain by 8 or more blocks",
			ErrInvalidInput,
		)
	default:
		return nil
	}
}

func validateExactRegisteredPredecessor(listed, previous ton.BlockIDExt) error {
	if !listed.Equals(&previous) {
		return fmt.Errorf(
			"%w: immediate split or merge predecessor differs from the registered block",
			ErrInvalidInput,
		)
	}

	return nil
}

func validateMergedRegisteredPredecessor(registered registeredShardTopology, previous ton.BlockIDExt) error {
	registeredSeqno := max(registered.first.Block.SeqNo, registered.second.Block.SeqNo)
	if previous.SeqNo <= registeredSeqno {
		return fmt.Errorf(
			"%w: merged predecessor does not follow both registered child chains",
			ErrInvalidInput,
		)
	}
	if previous.SeqNo-registeredSeqno >= 8 {
		return fmt.Errorf(
			"%w: merged predecessor extends the registered child chains by 8 or more blocks",
			ErrInvalidInput,
		)
	}

	return nil
}

func validateTopologyBeforeSplit(
	registered registeredShardTopology,
	beforeSplit bool,
	genUtime uint32,
) error {
	if !beforeSplit {
		return nil
	}
	if registered.kind != topologyLinear || registered.first.FSM.Kind != groups.ShardFSMSplit {
		return fmt.Errorf("%w: before split is not enabled by the registered shard", ErrInvalidInput)
	}

	fsm := registered.first.FSM
	end := uint64(fsm.UTime) + uint64(fsm.Interval)
	if end > math.MaxUint32 || genUtime < fsm.UTime || uint64(genUtime) >= end {
		return fmt.Errorf("%w: before split is outside the registered split interval", ErrInvalidInput)
	}

	return nil
}

func topologyShardIdent(id groups.ShardID) (tlb.ShardIdent, error) {
	prefixBits, err := id.PrefixBits()
	if err != nil {
		return tlb.ShardIdent{}, fmt.Errorf("%w: invalid target shard: %v", ErrInvalidInput, err)
	}

	marker := uint64(1) << (63 - prefixBits)
	return tlb.ShardIdent{
		PrefixBits:  int8(prefixBits),
		WorkchainID: id.Workchain,
		ShardPrefix: uint64(id.Shard) &^ marker,
	}, nil
}

func validateTopologyPredecessor(
	label string,
	previous *PreviousBlock,
	state *tlb.ShardStateUnsplit,
) error {
	ident, err := topologyShardIdent(groups.ShardID{
		Workchain: previous.ID.Workchain,
		Shard:     previous.ID.Shard,
	})
	if err != nil {
		return fmt.Errorf("%w: %s previous block id differs from the previous state: invalid shard", ErrInvalidInput, label)
	}
	if state.ShardIdent != ident || state.Seqno != previous.ID.SeqNo {
		return fmt.Errorf("%w: %s previous block id differs from the previous state", ErrInvalidInput, label)
	}

	return nil
}

func topologySession(masterchain MasterchainContext, target groups.ShardID) (*groups.Session, error) {
	snapshot := masterchain.Groups
	if snapshot == nil || !snapshot.Ready {
		return nil, fmt.Errorf("%w: validator group snapshot is not ready", ErrInvalidInput)
	}
	if !masterchain.ID.Equals(&snapshot.MasterchainBlock) {
		return nil, fmt.Errorf("%w: validator group snapshot belongs to another masterchain block", ErrInvalidInput)
	}

	var matched *groups.Session
	for i := range snapshot.Active {
		session := &snapshot.Active[i]
		if session.Shard != target {
			continue
		}
		if matched != nil {
			return nil, fmt.Errorf("%w: validator group snapshot has duplicate active shard", ErrInvalidInput)
		}
		matched = session
	}
	if matched == nil {
		return nil, fmt.Errorf("%w: validator group snapshot has no active shard", ErrInvalidInput)
	}

	return matched, nil
}

func sameGenesis(genesis []ton.BlockIDExt, blocks ...*ton.BlockIDExt) bool {
	if len(genesis) != len(blocks) {
		return false
	}
	for i := range genesis {
		if !genesis[i].Equals(blocks[i]) {
			return false
		}
	}
	return true
}

// expectedShardNeighbors projects the neighbor set a shard block must import
// from, out of the masterchain group snapshot. Collation derives it, validation
// re-derives it, and the full-collated verifier checks the transmitted set
// against it — the three must agree by construction, so they share this one
// derivation.
//
// A foreign zerostate neighbor is skipped: it has produced no block to import
// from. That includes the masterchain zerostate, which can never be the target
// of a shard block.
func expectedShardNeighbors(
	masterchain MasterchainContext,
	target groups.ShardID,
) (map[neighborShardKey]ton.BlockIDExt, error) {
	expected := make(map[neighborShardKey]ton.BlockIDExt)
	if masterchain.ID.SeqNo != 0 {
		expected[neighborShardKey{workchain: masterchainWorkchainID, shard: shard.Root}] = masterchain.ID
	}
	for i := range masterchain.Groups.Active {
		session := &masterchain.Groups.Active[i]
		for j := range session.Registered {
			description := &session.Registered[j]
			if !shard.IsNeighbor(
				target.Workchain,
				target.Shard,
				description.Shard.Workchain,
				description.Shard.Shard,
			) {
				continue
			}
			if description.Block.SeqNo == 0 && description.Shard != target {
				continue
			}
			key := neighborShardKey{workchain: description.Shard.Workchain, shard: description.Shard.Shard}
			if block, exists := expected[key]; exists && !block.Equals(&description.Block) {
				return nil, fmt.Errorf("%w: validator group snapshot has conflicting neighbor blocks", ErrInvalidInput)
			}
			expected[key] = description.Block
		}
	}

	return expected, nil
}

// expectedMasterNeighbors projects the neighbor set a masterchain block must
// import from, out of the shard registry the block itself installs. Collation
// derives it, validation re-derives it, and verifyFullMasterNeighborSet checks
// the transmitted set against it — the three must agree by construction, so the
// first two share this one derivation. The third keeps its own loop because it
// additionally carries each leaf's registered end lt.
//
// A registered shard that has produced no block yet is skipped: there is no
// out-queue to import from. The masterchain's own predecessor is always a
// neighbor of itself.
func expectedMasterNeighbors(
	registry *ShardRegistry,
	previous ton.BlockIDExt,
) map[neighborShardKey]ton.BlockIDExt {
	expected := make(map[neighborShardKey]ton.BlockIDExt, len(registry.leaves)+1)
	expected[neighborShardKey{workchain: masterchainWorkchainID, shard: shard.Root}] = previous
	for _, top := range registry.Tops() {
		if top.Block.SeqNo != 0 {
			expected[neighborShardKey{workchain: top.Block.Workchain, shard: top.Block.Shard}] = top.Block
		}
	}

	return expected
}
