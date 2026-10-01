package collator

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"math"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
)

// MaxSpeculativeLineageBlocks bounds the uncommitted lineage carried by a bet,
// including its selected tip. The applied frontier normally trails by only a
// few blocks; a longer gap must catch up before speculation can use it.
const MaxSpeculativeLineageBlocks = 16

// SelectedBaseState is an immutable in-process capability binding one
// consensus candidate ID to its exact ordinary block and full successor state.
// Its fields stay private so a caller cannot relabel unrelated resident cells.
type SelectedBaseState struct {
	sessionID [32]byte
	candidate simplex.CandidateID
	block     PreviousBlock
	// ancestors contains only block roots, newest first. These roots are bound
	// to block by its authenticated parent references; no ancestor state is held.
	ancestors []PreviousBlock
	// successor is what the empty-block policy asks about the block that would
	// extend this base. Both values are read out of the state this constructor
	// has already decoded, so a caller that needs the verdict does not decode it
	// a second time — and, more importantly, does not have to reach into the
	// state to derive it.
	successor CandidateState
}

// NewSelectedBaseState binds a resolver-owned block and full state to the
// candidate selected by Simplex. BlockBOC is borrowed only long enough to bind
// its file hash; the returned capability retains the immutable cell roots.
func NewSelectedBaseState(
	sessionID [32]byte,
	candidate simplex.CandidateID,
	block ton.BlockIDExt,
	blockBOC []byte,
	blockRoot *cell.Cell,
	stateRoot *cell.Cell,
	ancestorBlocks []*cell.Cell,
) (*SelectedBaseState, error) {
	if len(ancestorBlocks) >= MaxSpeculativeLineageBlocks {
		return nil, fmt.Errorf("%w: selected base lineage exceeds %d blocks", ErrInvalidInput, MaxSpeculativeLineageBlocks)
	}
	if err := validateBlockID(block); err != nil || block.SeqNo == 0 {
		return nil, fmt.Errorf("%w: selected base block is invalid", ErrInvalidInput)
	}
	if len(blockBOC) == 0 || blockRoot == nil || stateRoot == nil ||
		blockRoot.IsSpecial() || blockRoot.Level() != 0 || stateRoot.IsSpecial() || stateRoot.Level() != 0 {
		return nil, fmt.Errorf("%w: selected base roots are invalid", ErrInvalidInput)
	}
	if !equalCellHashBytes(blockRoot, block.RootHash) {
		return nil, fmt.Errorf("%w: selected base block root differs from its id", ErrInvalidInput)
	}
	fileHash := sha256.Sum256(blockBOC)
	if !bytes.Equal(fileHash[:], block.FileHash) {
		return nil, fmt.Errorf("%w: selected base block data differs from its id", ErrInvalidInput)
	}

	var state tlb.ShardStateUnsplit
	if err := parseExact(&state, stateRoot); err != nil {
		return nil, fmt.Errorf("%w: decode selected base state: %v", ErrInvalidInput, err)
	}
	expected, err := topologyShardIdent(groups.ShardID{Workchain: block.Workchain, Shard: block.Shard})
	if err != nil || state.ShardIdent != expected || state.Seqno != block.SeqNo {
		return nil, fmt.Errorf("%w: selected base state differs from its block id", ErrInvalidInput)
	}

	var parsed tlb.Block
	if err = parseExact(&parsed, blockRoot); err != nil {
		return nil, fmt.Errorf("%w: decode selected base block: %v", ErrInvalidInput, err)
	}
	if parsed.BlockInfo.Shard != state.ShardIdent || parsed.BlockInfo.SeqNo != state.Seqno {
		return nil, fmt.Errorf("%w: selected base block header differs from state", ErrInvalidInput)
	}
	target, err := parsed.StateUpdate.PeekRef(1)
	if err != nil || target.HashKeyAt(0) != stateRoot.HashKeyAt(0) {
		return nil, fmt.Errorf("%w: selected base block does not produce supplied state", ErrInvalidInput)
	}
	queueSize, err := exactOutQueueSize(state.OutMsgQueueInfo)
	if err != nil {
		return nil, err
	}

	if state.Seqno == math.MaxUint32 {
		return nil, fmt.Errorf("%w: selected base seqno overflows", ErrInvalidInput)
	}

	var ancestors []PreviousBlock
	if len(ancestorBlocks) != 0 {
		ancestors = make([]PreviousBlock, len(ancestorBlocks))
		childID, childRoot := block, blockRoot
		for i, root := range ancestorBlocks {
			parent, _, parentErr := shardParentBlockID(childID, childRoot)
			if parentErr != nil {
				return nil, fmt.Errorf("%w: selected ancestor %d: %v", ErrInvalidInput, i, parentErr)
			}
			if root == nil || root.IsSpecial() || root.Level() != 0 || !equalCellHashBytes(root, parent.RootHash) {
				return nil, fmt.Errorf("%w: selected ancestor %d differs from its parent reference", ErrInvalidInput, i)
			}
			var parsed tlb.Block
			if err = parseExact(&parsed, root); err != nil {
				return nil, fmt.Errorf("%w: decode selected ancestor %d: %v", ErrInvalidInput, i, err)
			}
			if parsed.BlockInfo.SeqNo != parent.SeqNo || parsed.BlockInfo.Shard != state.ShardIdent {
				return nil, fmt.Errorf("%w: selected ancestor %d has another identity", ErrInvalidInput, i)
			}
			ancestors[i] = PreviousBlock{ID: parent, Block: root}
			childID, childRoot = parent, root
		}
	}

	return &SelectedBaseState{
		sessionID: sessionID,
		candidate: candidate,
		ancestors: ancestors,
		block: PreviousBlock{
			ID:           cloneBlockID(block),
			Block:        blockRoot,
			State:        stateRoot,
			OutQueueSize: uint64Pointer(queueSize),
		},
		successor: CandidateState{
			Block:       cloneBlockID(block),
			NextSeqno:   state.Seqno + 1,
			BeforeSplit: state.BeforeSplit,
		},
	}, nil
}

func (s *SelectedBaseState) matches(sessionID [32]byte, candidate simplex.CandidateID) bool {
	return s != nil && s.sessionID == sessionID && s.candidate == candidate
}
