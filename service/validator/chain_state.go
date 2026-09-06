package validator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	sharddomain "github.com/xssnick/gton/service/shard"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/groups"
)

// ChainStateRequest identifies the exact chain tips a backend must load.
type ChainStateRequest struct {
	Shard          groups.ShardID
	Blocks         []ton.BlockIDExt
	MinMasterchain ton.BlockIDExt
	// Wait lets the backend block until a publication makes the tip readable,
	// instead of reporting ErrBlockNotReady straight away. It replaces a caller
	// polling the backend: waiting on the publication edge is both faster and
	// quieter than asking again every second.
	//
	// It is opt-in per request because the two kinds of caller are opposites. The
	// finalized-state load exists in order to wait — a candidate whose parent is
	// not readable yet cannot be validated at all. The lineage walk must NOT wait:
	// a state it cannot read is a step it walks past, and blocking there would
	// convert a cheap descent into one 30-second stall per unreadable ancestor.
	Wait bool
}

// ChainTip is one manager-loaded block and state root. BlockBOC and Block are
// absent only for a zerostate.
type ChainTip struct {
	ID       ton.BlockIDExt
	BlockBOC []byte
	// Block is BlockBOC already decoded, with Hash() equal to ID.RootHash. It
	// is nil only for a zerostate tip, which has no block cell at all. Every
	// producer already parses this root for its own checks, so carrying it
	// spares each candidate validation one full BOC decode per predecessor.
	Block *cell.Cell
	State *cell.Cell
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
		// A level-0 ordinary cell is what ApplyMerkleUpdate requires of its
		// source, and the merge root built below is one only when both tip
		// states are.
		//
		// Both halves are needed and neither implies the other. A pruned branch
		// carries a level and is caught by the first; a Merkle proof is caught
		// only by the second, because a proof cell's level mask is its body's
		// shifted right by one, so a proof over any level-0 tree is ITSELF level
		// 0 and sails through a level check. That is the case worth naming: a
		// proof root is the shape a caller is most likely to have lying around
		// and hand over as a state.
		//
		// CAVEAT: this still does NOT catch narrowness. A proof root refused
		// here can be virtualized into an ordinary level-0 tree full of pruned
		// boundaries, which passes both halves. Only the source walk inside
		// ApplyMerkleUpdate rejects that.
		if tip.State.Level() != 0 || tip.State.IsSpecial() {
			return nil, fmt.Errorf(
				"validator runtime: loaded chain tip %d state is not an ordinary level-0 cell",
				i,
			)
		}
		if tip.ID.SeqNo == 0 {
			if tip.Block != nil {
				return nil, fmt.Errorf("validator runtime: loaded chain tip %d is a zerostate with a block", i)
			}
			continue
		}
		if len(tip.BlockBOC) == 0 {
			return nil, fmt.Errorf("validator runtime: loaded chain tip %d has no block data", i)
		}
		if tip.Block == nil {
			return nil, fmt.Errorf("validator runtime: loaded chain tip %d has no parsed block", i)
		}
		// The two halves of a tip's block have to be the same block, and each is
		// bound to the id by the digest that names it there: the root hash for
		// the cell, the file hash — which is by definition the digest of the
		// serialized block — for the bytes. Checked separately they only say
		// that each half is a block; together with the id equality above they
		// say both are this one.
		//
		// The alternative, decoding BlockBOC and comparing, is the work carrying
		// Block exists to remove. This costs one sha256 over the predecessor
		// payload per loaded tip, on the path that just read those bytes from
		// the store and parsed them, and nothing at all per candidate: the
		// producers inside this package build their ChainState directly.
		if !cellHashEquals(tip.Block, tip.ID.RootHash) {
			return nil, fmt.Errorf("validator runtime: loaded chain tip %d block differs from its id", i)
		}
		fileHash := sha256.Sum256(tip.BlockBOC)
		if !bytes.Equal(fileHash[:], tip.ID.FileHash) {
			return nil, fmt.Errorf("validator runtime: loaded chain tip %d block data differs from its id", i)
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
		// The collator's own constructor, not a local copy of it. Both sides
		// apply a merge candidate's update to this cell — the collator to the
		// one it builds from the predecessor list, this runtime to the one it
		// keeps as the state root — and a shape that differed by a bit would
		// make one of the two applies fail against a parent the other accepted.
		root, err := collator.MergedPredecessorStates(data.Tips[0].State, data.Tips[1].State)
		if err != nil {
			return nil, fmt.Errorf("validator runtime: combine merge tip states: %w", err)
		}
		state.root = root

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
	var root *cell.Cell
	if artifact.validationRoots != nil {
		root = artifact.validationRoots.block
	}
	if root == nil {
		var err error
		root, err = cell.FromBOC(artifact.BlockBOC)
		if err != nil {
			return nil, fmt.Errorf("validator runtime: decode applied block: %w", err)
		}
	}
	if !cellHashEquals(root, artifact.Candidate.Block.RootHash) {
		return nil, errors.New("validator runtime: applied block root hash mismatch")
	}

	if artifact.validationRoots != nil {
		if next, ok := artifact.validationRoots.builtSuccessor.Over(s.root.WithoutTrace(), s.tipStates()...); ok {
			return &ChainState{
				shard: s.shard,
				tips: []ChainTip{{
					ID:       *artifact.Candidate.Block.Copy(),
					BlockBOC: artifact.BlockBOC,
					Block:    root,
					State:    next,
				}},
				root:           next,
				minMasterchain: s.minMasterchain,
			}, nil
		}
	}

	var loader cell.Slice
	if err := root.BeginParseInto(&loader); err != nil {
		return nil, fmt.Errorf("validator runtime: parse applied block root: %w", err)
	}
	var block tlb.Block
	if err := tlb.LoadFromCell(&block, &loader); err != nil {
		return nil, fmt.Errorf("validator runtime: parse applied block: %w", err)
	}
	if loader.BitsLeft() != 0 || loader.RefsNum() != 0 {
		return nil, errors.New("validator runtime: applied block has trailing data")
	}
	// One capsule instead of a validate and an apply that each rebuild the same
	// two update-side walks. The verdict is decided exactly as
	// ValidateMerkleUpdate decided it — same walks, same order, same errors —
	// and the apply below then replays what that decision recorded instead of
	// re-deriving it. This is the path that has no verifier behind it, so the
	// validation is not optional here.
	prepared, err := cell.PrepareMerkleUpdatePlanned(block.StateUpdate)
	if err != nil {
		return nil, fmt.Errorf("validator runtime: invalid state update: %w", err)
	}
	nextRoot, err := prepared.ApplyTo(s.root.WithoutTrace())
	if err != nil {
		return nil, fmt.Errorf("validator runtime: apply state update: %w", err)
	}

	return &ChainState{
		shard: s.shard,
		tips: []ChainTip{{
			ID:       *artifact.Candidate.Block.Copy(),
			BlockBOC: artifact.BlockBOC,
			Block:    root,
			State:    nextRoot,
		}},
		root:           nextRoot,
		minMasterchain: s.minMasterchain,
	}, nil
}

// CandidateSuccessor mirrors collator.ValidatedSuccessor at the package
// boundary: the transition a semantically verified candidate asserts, and — on
// the paths where verification ran on this node's own predecessor trees — the
// successor it already built there. A raw state root is deliberately not part
// of it: on the full-collated shard path the root the collator executed against
// is proof-backed, built on the states the candidate's own collated data proves
// rather than on the ones this node holds, and publishing that as a lineage
// state is what this type exists to make impossible. Live is empty on exactly
// those runs and opens only against the trees it was built on, so the exception
// cannot be widened by a caller.
type CandidateSuccessor struct {
	BlockRoot   *cell.Cell
	StateUpdate *cell.Cell
	// Prepared is StateUpdate with its verdict already decided, as the verifier
	// decided it, and with the update-side walks recorded as replayable plans
	// on the one path that applies here — the proof-backed shard run announce
	// starts a walk for. Applying through it is the same apply against the same
	// parent; what it skips is rebuilding what the verifier already knows about
	// the update itself. Nil on the paths where no verifier produced one, where
	// the plain apply is used instead.
	Prepared  *cell.PreparedMerkleUpdate
	StateHash cell.Hash
	Live      collator.LiveSuccessorState
}

// applyTo is this transition applied to one parent, through the capsule when
// the verifier handed one back and through the plain apply otherwise. Both
// forms perform the same source materialization check against the same cells,
// which is the check a narrow or mispaired parent has to fail.
func (s CandidateSuccessor) applyTo(from *cell.Cell) (*cell.Cell, error) {
	if s.Prepared != nil {
		return s.Prepared.ApplyTo(from)
	}

	return cell.ApplyMerkleUpdate(from, s.StateUpdate)
}

// liveSuccessorApply is one candidate's apply of a validated state update onto
// the full parent root of one ChainState, started before the semantic
// validation that authorizes it.
//
// It exists for one path: a shard candidate carrying full collated data, whose
// verification runs on the proof the candidate itself supplies and so never
// produces a successor of the parent this node holds. There, and only there,
// the apply has to happen on this side, and giving it the update early — the
// collator announces it once the candidate has passed everything a lagging node
// retries on — lets the walk over the parent, and the disk reads it costs when
// that parent came from the node store, overlap with the semantic replay
// instead of extending it. On every other path the collator hands back the
// successor it already built over these very cells and nothing is announced at
// all, because a second apply there would recompute a result that already
// exists.
//
// Lifetime: the walk is bounded by the update's own source proof and reports
// into fields published by closing done, so the goroutine always completes and
// exits whether or not anyone reads it, and any number of joiners may read the
// result. Cancelling is therefore abandonment — a rejected candidate, or a dead
// context, simply leaves the result unread — and cannot leave a goroutine
// parked on a send or walking forever. ApplyMerkleUpdate takes no context, and
// threading one through it would put a cancellation check inside the hottest
// walk in the node for no gain over a walk that already terminates.
//
// A handle belongs to the one candidate-validation task: sessionRuntime opens
// it once before the backend call, and the task can keep it while exact input
// waits suspend and resume inside acquisition. It is never stored on the
// ChainState it reads — ChainState is immutable and shared
// between concurrent validations of competing children, which is also why the
// walk is safe: it only reads, and lazy references materialize into fresh cells
// rather than into the shared tree.
type liveSuccessorApply struct {
	ctx context.Context
	// state is the parent this handle belongs to, held for identity rather than
	// for reading. Comparing it is what makes the late join provably the same
	// pairing as the early walk, without depending on WithoutTrace returning the
	// same cell twice.
	state *ChainState
	from  *cell.Cell
	// update is the cell announce was called with, kept for the identity
	// comparison a late join makes, and nil until then.
	update *cell.Cell
	// prepared is the capsule the walk runs through.
	prepared *cell.PreparedMerkleUpdate
	// done is closed once root and err are final. Nil until an announcement.
	done chan struct{}
	root *cell.Cell
	err  error
}

// pendingSuccessor opens a handle for one validation of one candidate against
// this state. Nothing runs until the transition is announced, and on the paths
// that carry their successor back nothing ever is.
func (s *ChainState) pendingSuccessor(ctx context.Context) *liveSuccessorApply {
	// WithoutTrace is resolved here rather than at apply time so the walk and
	// any join use the identical cell. It returns the receiver itself for an
	// untraced root, which every ChainState root is.
	return &liveSuccessorApply{ctx: ctx, state: s, from: s.root.WithoutTrace()}
}

// announce is the TransitionAnnouncer handed to the collator. It runs on the
// goroutine performing the validation, before that call returns, and starts the
// apply the join at the end of the call would otherwise perform there. A second
// announcement for the same handle starts nothing: the one walk in flight is
// already the one this validation needs.
func (a *liveSuccessorApply) announce(prepared *cell.PreparedMerkleUpdate) {
	if a == nil || prepared == nil || a.update != nil || a.from == nil {
		return
	}
	if a.ctx != nil && a.ctx.Err() != nil {
		// The validation this would overlap with is already over. Doing nothing
		// is the whole of cancellation here: the late path is what runs, and it
		// is not reached on a dead context.
		return
	}
	a.update = prepared.Cell()
	a.prepared = prepared
	a.done = make(chan struct{})
	go func(from *cell.Cell, prepared *cell.PreparedMerkleUpdate) {
		root, err := prepared.ApplyTo(from)
		a.root, a.err = root, err
		close(a.done)
	}(a.from, prepared)
}

// successorOf returns the successor of state under update: the announced walk's
// result when one was started, and the walk performed here otherwise. Both
// produce ApplyMerkleUpdate over the same two cells, so the result — including
// the error — does not depend on which one ran, nor on whether the early walk
// finished before or after validation did.
//
// A nil handle is the no-hook case: exported entry points, tests and any
// backend that does not announce. It is the same apply, at the same point in
// the call, with nothing overlapped.
//
// A handle opened on another state is a wiring fault and is reported as one.
// The alternative — quietly performing the apply again — is how an announced
// walk ends up wasted on every single call with nothing to say so, which is
// precisely the failure this returns an error for. It cannot fire for a
// mismatch between two WithoutTrace results of one root, because the state is
// what is compared and the walk uses the handle's own cell.
func (a *liveSuccessorApply) successorOf(state *ChainState, successor CandidateSuccessor) (*cell.Cell, error) {
	if a == nil {
		return successor.applyTo(state.root.WithoutTrace())
	}
	if a.state != state {
		return nil, errors.New("validator runtime: successor apply handle belongs to another chain state")
	}
	if a.done == nil {
		return successor.applyTo(a.from)
	}
	if successor.StateUpdate == nil || a.update.HashKeyAt(0) != successor.StateUpdate.HashKeyAt(0) {
		return nil, errors.New("validator runtime: announced successor apply carries another transition")
	}
	<-a.done

	return a.root, a.err
}

// validatedCandidateState produces the successor of this state under a
// transition candidate validation has already found semantically valid.
//
// The invariant it exists to keep: a ChainState root is always a full live
// tree, and a proof-backed root never becomes one. The successor is therefore
// either the result of ApplyMerkleUpdate over this state's own root, or the
// result of an ApplyMerkleUpdate the verifier already performed over these very
// cells — Over accepts nothing else, and the parents it is given are this
// state's own tips. The reference node has only the first form, because it has
// no verifier that already applied to the same parent: resolve_state_inner is
// unconditionally an apply onto the predecessor's own state. Taking the result
// of an apply this node has provably already done is that apply, not a
// shortcut around it; what it skips is doing it twice.
//
// cell.ValidateMerkleUpdate is deliberately not called: it takes only the
// update cell, so its verdict is a pure function of that cell and cannot change
// with the parent, and candidate verification already paid that walk. The check
// that does need doing per parent is the source materialization ApplyMerkleUpdate
// performs — it descends the update's source proof against this actual tree and
// compares full boundary identity at every node, which is exactly what a narrow
// parent fails.
//
// The hash comparison is near-tautological: both sides are the update's target
// hash, the narrow one because verification asserted it and the full one by
// construction of the rebuilt root. It cannot fail for semantic reasons, which
// is what makes it worth keeping — it costs two already-computed hashes and it
// catches the faults that are otherwise silent, a mis-wired pairing of an
// update with a parent it does not belong to, or a defect in boundary
// substitution. It is kept on the carried-back root as well, where it is
// tautological by construction rather than nearly so: it is the one assertion
// that the root Over released is the root whose hash verification reported.
//
// There is no store fallback. If the apply fails, the validation task fails
// and this node abstains: repairing a speculative parent from storage would
// convert the one loud signal this invariant buys into silence, and the store
// structurally does not hold speculative states anyway.
func (s *ChainState) validatedCandidateState(
	artifact *CandidateArtifact,
	successor CandidateSuccessor,
	pending *liveSuccessorApply,
) (*ChainState, error) {
	if successor.BlockRoot == nil {
		return nil, errors.New("validator runtime: validated candidate has no parsed block root")
	}
	if successor.StateUpdate == nil {
		return nil, errors.New("validator runtime: validated candidate has no state update")
	}
	if !cellHashEquals(successor.BlockRoot, artifact.Candidate.Block.RootHash) {
		return nil, errors.New("validator runtime: validated block root differs from the candidate id")
	}
	// The verifier ran on these exact tips, so the successor of this state is
	// the root it already produced. Over is what establishes "these exact tips":
	// it takes the tip states by pointer and the combined root by hash, and
	// refuses everything else, including every proof-backed run.
	nextRoot, live := successor.Live.Over(s.root.WithoutTrace(), s.tipStates()...)
	if !live {
		var err error
		if nextRoot, err = pending.successorOf(s, successor); err != nil {
			return nil, fmt.Errorf("validator runtime: apply validated candidate to the live parent: %w", err)
		}
	}
	if nextRoot.HashKeyAt(0) != successor.StateHash {
		return nil, errors.New("validator runtime: live successor differs from the validated state")
	}

	return &ChainState{
		shard: s.shard,
		tips: []ChainTip{{
			ID:       *artifact.Candidate.Block.Copy(),
			BlockBOC: artifact.BlockBOC,
			Block:    successor.BlockRoot,
			State:    nextRoot,
		}},
		root:           nextRoot,
		minMasterchain: s.minMasterchain,
	}, nil
}

// acceptedTipState is this state expressed as the storage snapshot the live view
// publishes for a block this node has just accepted.
//
// It is the ONE producer of a published state, and that is deliberate: it can
// only be called on a ChainState, and every ChainState root is a full tree by
// construction — newChainState refuses a proof root or a pruned branch, apply and
// validatedCandidateState each produce the result of an ApplyMerkleUpdate over a
// full parent, and validatedCandidateState's Over accepts a carried-back
// successor only over this state's own tips. The narrow root the verifier
// produces on the proof-backed path is therefore not representable here, which is
// what makes publishing safe without restricting it to one code path: the
// restriction is on the TYPE, not on the caller.
//
// The state cell is passed through, never copied. chain_state.go compares tip
// states by pointer for the live-successor carry-back, and the live view returns
// a resident state as itself, so publishing this cell is what makes the reader's
// materialization the same object the producer used. Cloning it here would cost a
// full re-apply per candidate and nothing would report it.
//
// The block must be the state's single ordinary tip. A merge state has two tips
// and is the state of neither of them, so it is refused rather than guessed at.
func (s *ChainState) acceptedTipState(block ton.BlockIDExt) (*storage.BlockState, error) {
	if len(s.tips) != 1 {
		return nil, fmt.Errorf("validator runtime: accepted state has %d tips, want one", len(s.tips))
	}
	tip := &s.tips[0]
	if !sameBlockID(tip.ID, block) {
		return nil, errors.New("validator runtime: accepted state belongs to another block")
	}
	if tip.State == nil || tip.State != s.root {
		return nil, errors.New("validator runtime: accepted state tip is not this state's root")
	}
	rootHash := tip.State.HashKeyAt(0)

	return &storage.BlockState{
		Block:         *block.Copy(),
		StateRootHash: rootHash[:],
		Cell:          tip.State,
	}, nil
}

// tipStates is the predecessor list exactly as the collator received it, which
// is what LiveSuccessorState.Over is entitled to compare against.
func (s *ChainState) tipStates() []*cell.Cell {
	states := make([]*cell.Cell, len(s.tips))
	for i := range s.tips {
		states[i] = s.tips[i].State
	}

	return states
}

func sameBlockID(left, right ton.BlockIDExt) bool {
	return left.Workchain == right.Workchain && left.Shard == right.Shard && left.SeqNo == right.SeqNo &&
		bytes.Equal(left.RootHash, right.RootHash) && bytes.Equal(left.FileHash, right.FileHash)
}
