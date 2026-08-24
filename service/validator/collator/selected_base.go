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

// SelectedBaseState is an immutable in-process capability binding one
// consensus candidate ID to its exact ordinary block and full successor state.
// Its fields stay private so a caller cannot relabel unrelated resident cells.
type SelectedBaseState struct {
	sessionID [32]byte
	candidate simplex.CandidateID
	block     PreviousBlock
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
) (*SelectedBaseState, error) {
	if err := validateBlockID(block); err != nil || block.SeqNo == 0 {
		return nil, fmt.Errorf("%w: selected base block is invalid", ErrInvalidInput)
	}
	if len(blockBOC) == 0 || blockRoot == nil || stateRoot == nil ||
		blockRoot.IsSpecial() || blockRoot.Level() != 0 || stateRoot.IsSpecial() || stateRoot.Level() != 0 {
		return nil, fmt.Errorf("%w: selected base roots are invalid", ErrInvalidInput)
	}
	if !bytes.Equal(blockRoot.Hash(), block.RootHash) {
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

	return &SelectedBaseState{
		sessionID: sessionID,
		candidate: candidate,
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
