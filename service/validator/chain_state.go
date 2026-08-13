package validator

import (
	"bytes"
	"errors"
	"fmt"
	"slices"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	sharddomain "github.com/xssnick/gton/service/shard"
	"github.com/xssnick/gton/service/validator/groups"
)

// ChainStateRequest identifies the exact chain tips a backend must load.
type ChainStateRequest struct {
	Shard          groups.ShardID
	Blocks         []ton.BlockIDExt
	MinMasterchain ton.BlockIDExt
}

// ChainTip is one manager-loaded block and state root. BlockBOC is absent
// only for a zerostate.
type ChainTip struct {
	ID       ton.BlockIDExt
	BlockBOC []byte
	State    *cell.Cell
}

// ChainStateData is the backend result for ChainStateRequest. Tips must be in
// the same order as request.Blocks.
type ChainStateData struct {
	Tips []ChainTip
}

// ChainState is the local equivalent of consensus::ChainState. Its cells and
// BOCs are immutable and shared between cached descendants.
type ChainState struct {
	shard          groups.ShardID
	tips           []ChainTip
	root           *cell.Cell
	minMasterchain ton.BlockIDExt
}

func newChainState(request ChainStateRequest, data ChainStateData) (*ChainState, error) {
	if len(data.Tips) != len(request.Blocks) {
		return nil, fmt.Errorf(
			"validator runtime: loaded %d chain tips, want %d",
			len(data.Tips),
			len(request.Blocks),
		)
	}
	if len(data.Tips) == 0 || len(data.Tips) > 2 {
		return nil, fmt.Errorf("validator runtime: invalid chain tip count %d", len(data.Tips))
	}
	for i := range data.Tips {
		tip := &data.Tips[i]
		if !sameBlockID(tip.ID, request.Blocks[i]) {
			return nil, fmt.Errorf("validator runtime: loaded chain tip %d differs from request", i)
		}
		if tip.State == nil {
			return nil, fmt.Errorf("validator runtime: loaded chain tip %d has no state", i)
		}
		if tip.ID.SeqNo != 0 && len(tip.BlockBOC) == 0 {
			return nil, fmt.Errorf("validator runtime: loaded chain tip %d has no block data", i)
		}
	}

	state := &ChainState{
		shard:          request.Shard,
		tips:           slices.Clone(data.Tips),
		minMasterchain: request.MinMasterchain,
	}
	if len(data.Tips) == 2 {
		if err := state.validateMergeTips(); err != nil {
			return nil, err
		}
		state.root = cell.BeginCell().
			MustStoreUInt(0x5f327da5, 32).
			MustStoreRef(data.Tips[0].State).
			MustStoreRef(data.Tips[1].State).
			EndCell()

		return state, nil
	}

	tip := &data.Tips[0]
	if tip.ID.Workchain != request.Shard.Workchain {
		return nil, errors.New("validator runtime: chain tip belongs to another workchain")
	}
	if tip.ID.SeqNo == 0 && tip.ID.Shard != request.Shard.Shard {
		return nil, errors.New("validator runtime: zerostate belongs to another shard")
	}
	if tip.ID.Shard != request.Shard.Shard && !sharddomain.IsDirectChild(tip.ID.Shard, request.Shard.Shard) {
		return nil, errors.New("validator runtime: chain tip is not target shard or its direct parent")
	}
	state.root = tip.State

	return state, nil
}

func (s *ChainState) validateMergeTips() error {
	if s.tips[0].ID.SeqNo == 0 || s.tips[1].ID.SeqNo == 0 {
		return errors.New("validator runtime: merge tips must be ordinary blocks")
	}

	left, err := sharddomain.Child(s.shard.Shard, true)
	if err != nil {
		return fmt.Errorf("validator runtime: resolve left merge child: %w", err)
	}
	right, err := sharddomain.Child(s.shard.Shard, false)
	if err != nil {
		return fmt.Errorf("validator runtime: resolve right merge child: %w", err)
	}
	if s.tips[0].ID.Workchain != s.shard.Workchain || s.tips[1].ID.Workchain != s.shard.Workchain ||
		s.tips[0].ID.Shard != left || s.tips[1].ID.Shard != right {
		return errors.New("validator runtime: merge tips are not ordered target children")
	}

	return nil
}

// NormalBlock returns the only ordinary tip. Empty candidates are valid only
// when they reference this exact block.
func (s *ChainState) NormalBlock() (ton.BlockIDExt, error) {
	if len(s.tips) != 1 || s.tips[0].ID.SeqNo == 0 || s.tips[0].ID.Shard != s.shard.Shard {
		return ton.BlockIDExt{}, errors.New("validator runtime: chain state is not a normal tip")
	}

	return *s.tips[0].ID.Copy(), nil
}

func (s *ChainState) apply(artifact *CandidateArtifact) (*ChainState, error) {
	root, err := cell.FromBOC(artifact.BlockBOC)
	if err != nil {
		return nil, fmt.Errorf("validator runtime: decode applied block: %w", err)
	}
	if !bytes.Equal(root.Hash(), artifact.Candidate.Block.RootHash) {
		return nil, errors.New("validator runtime: applied block root hash mismatch")
	}

	loader, err := root.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("validator runtime: parse applied block root: %w", err)
	}
	var block tlb.Block
	if err = tlb.LoadFromCell(&block, loader); err != nil {
		return nil, fmt.Errorf("validator runtime: parse applied block: %w", err)
	}
	if loader.BitsLeft() != 0 || loader.RefsNum() != 0 {
		return nil, errors.New("validator runtime: applied block has trailing data")
	}
	if err = cell.ValidateMerkleUpdate(block.StateUpdate); err != nil {
		return nil, fmt.Errorf("validator runtime: invalid state update: %w", err)
	}
	nextRoot, err := cell.ApplyMerkleUpdate(s.root.WithoutTrace(), block.StateUpdate)
	if err != nil {
		return nil, fmt.Errorf("validator runtime: apply state update: %w", err)
	}

	return &ChainState{
		shard: s.shard,
		tips: []ChainTip{{
			ID:       *artifact.Candidate.Block.Copy(),
			BlockBOC: artifact.BlockBOC,
			State:    nextRoot,
		}},
		root:           nextRoot,
		minMasterchain: s.minMasterchain,
	}, nil
}

func sameBlockID(left, right ton.BlockIDExt) bool {
	return left.Workchain == right.Workchain && left.Shard == right.Shard && left.SeqNo == right.SeqNo &&
		bytes.Equal(left.RootHash, right.RootHash) && bytes.Equal(left.FileHash, right.FileHash)
}
