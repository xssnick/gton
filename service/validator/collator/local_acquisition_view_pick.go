package collator

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"time"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"

	"github.com/xssnick/gton/service/validator/groups"
)

// selectedMasterView is the effective masterchain view of one shard build: the
// view its master_ref will name, plus this session's registered descriptors
// taken from that exact snapshot.
//
// The two travel together because every consumer of one consumes the other. A
// before_split flag derived from one snapshot's registry inside a block whose
// master_ref names another is a block that contradicts itself, and the build
// re-derives both from ShardRequest.Masterchain before it signs.
type selectedMasterView struct {
	view       *localMasterView
	registered []groups.ShardDescription
	// pinnable records whether every neighbour top this view registers is
	// already committed in the session's branch. It is a preference, not an
	// admission rule: a view that fails it still produces a valid block, it
	// just makes the slot pay the from-state seed walk.
	pinnable bool
}

// admitShardMasterView runs every test that decides whether a candidate view
// may be the one this build stamps. A refusal is not an error for the slot: the
// caller steps down to an older view.
//
// Each test here is the acquisition-time mirror of a check that is fatal and
// non-retryable further down. Getting the block refused by our own builder
// costs the whole leader window; refusing the view costs one slot's worth of
// freshness. The caller holds managed.mu.
func (a *LocalAcquisition) admitShardMasterView(
	view *localMasterView,
	managed *localAcquisitionSession,
	session ActivatedSession,
	previous []PreviousBlock,
	refs []*tlb.ExtBlkRef,
	kind topologyKind,
) (selectedMasterView, error) {
	if view == nil {
		return selectedMasterView{}, fmt.Errorf("%w: candidate masterchain view is absent", ErrAcquisitionNotReady)
	}
	// The session's own view must be an ancestor of anything newer we adopt.
	// Our validator resolves a candidate's reference view by requiring the
	// session's base view to be an ancestor of it, so a view off that branch
	// produces a block this node refuses to validate for itself.
	if managed.master != nil && view != managed.master {
		if err := view.requireAncestor(managed.master.context.ID); err != nil {
			return selectedMasterView{}, err
		}
	}
	if err := view.requireAncestor(session.MinMasterchain); err != nil {
		return selectedMasterView{}, err
	}
	// Every predecessor's own masterchain reference has to resolve inside the
	// view this block would name; blockAt refuses a reference ahead of the view
	// with a non-retryable error at build time.
	if err := verifyShardPredecessorRefs(view, previous, refs); err != nil {
		return selectedMasterView{}, err
	}
	// Deliberately the build's own lookup, so the snapshot-ready, snapshot-owns-
	// this-block and duplicate-active-shard checks are taken here rather than
	// left to a non-retryable build failure.
	matched, err := topologySession(view.context, session.Shard)
	if err != nil {
		return selectedMasterView{}, err
	}
	if matched.ID != session.ID {
		return selectedMasterView{}, fmt.Errorf(
			"%w: masterchain view names another session for this shard",
			ErrInvalidInput,
		)
	}
	if err = requireSameSessionIdentity(session, matched); err != nil {
		return selectedMasterView{}, err
	}
	registered, err := resolveRegisteredShardTopology(matched)
	if err != nil {
		return selectedMasterView{}, err
	}
	var second *ton.BlockIDExt
	if len(previous) == 2 {
		second = &previous[1].ID
	}
	// The +8 rule and its siblings, asked before the build instead of after.
	if err = admitRegisteredShardChain(registered, kind, previous[0].ID, second); err != nil {
		return selectedMasterView{}, err
	}

	return selectedMasterView{
		view:       view,
		registered: matched.Registered,
		pinnable:   a.neighborTopsPinnable(view, managed, session.Shard),
	}, nil
}

// neighborTopsPinnable reports whether every neighbour top this view registers
// is already a committed run in the session's branch, i.e. whether a build on
// this view can take its inbound cut without the from-state seed walk.
func (a *LocalAcquisition) neighborTopsPinnable(
	view *localMasterView,
	managed *localAcquisitionSession,
	target groups.ShardID,
) bool {
	if managed.branch == nil {
		return true
	}
	expected, err := expectedShardNeighbors(view.context, target)
	if err != nil {
		return false
	}
	destination := targetShardIdent(target)
	for _, block := range expected {
		source := blockShardIdent(block)
		if block.SeqNo == 0 || source == destination {
			continue
		}
		ref, refErr := localSourceRef(block)
		if refErr != nil {
			return false
		}
		if !managed.branch.SourcePinnable(source, ref) {
			return false
		}
	}

	return true
}

// selectShardMasterView is Collator::request_top_masterchain_state for one
// shard slot: min_block_id = max(session minimum, the predecessors' own
// masterchain references), then the live top state, then the reference state
// when it is newer than the top (cppnode collator.cpp:661-669 and 703-735).
//
// What the reference gets for free and we do not is that its state is
// authoritative by construction. Here a view carries a validator-group snapshot
// and a shard registry, and adopting one that no longer covers this session — a
// roster rotation, or a shard top registered past our own head — would make the
// block we stamp one our own builder refuses with a non-retryable error,
// killing the whole leader window. So candidates are admitted in order and a
// refusal steps down to an older view rather than failing the slot.
//
// Called with managed.mu held. In the ordinary case it performs no I/O: the
// resident view is already decoded and the floor is below it.
func (a *LocalAcquisition) selectShardMasterView(
	ctx context.Context,
	managed *localAcquisitionSession,
	session ActivatedSession,
	previous []PreviousBlock,
	refs []*tlb.ExtBlkRef,
	asOf time.Time,
	seedAllowed bool,
) (selectedMasterView, error) {
	// View-independent: no other masterchain view repairs a malformed
	// predecessor set, so this stays fatal instead of stepping down.
	kind, err := predecessorTopologyKind(session.Shard, previous)
	if err != nil {
		return selectedMasterView{}, err
	}

	floorID := session.MinMasterchain
	for _, ref := range refs {
		if ref == nil || ref.SeqNo <= floorID.SeqNo {
			continue
		}
		floorID = ton.BlockIDExt{
			Workchain: masterchainWorkchainID,
			Shard:     math.MinInt64,
			SeqNo:     ref.SeqNo,
			RootHash:  bytes.Clone(ref.RootHash),
			FileHash:  bytes.Clone(ref.FileHash),
		}
	}

	top := a.master.resident()
	if managed.master != nil && (top == nil || top.context.ID.SeqNo < managed.master.context.ID.SeqNo) {
		top = managed.master
	}
	if top == nil {
		return selectedMasterView{}, fmt.Errorf("%w: no masterchain view is installed", ErrAcquisitionNotReady)
	}

	candidates := make([]*localMasterView, 0, 3)
	appendCandidate := func(view *localMasterView) {
		if view == nil || view.context.ID.SeqNo < floorID.SeqNo {
			return
		}
		for _, existing := range candidates {
			if existing == view || existing.context.ID.Equals(&view.context.ID) {
				return
			}
		}
		candidates = append(candidates, view)
	}
	if floorID.SeqNo > top.context.ID.SeqNo {
		// The reference-state override: a predecessor already named a
		// masterchain block newer than anything installed here, and a block may
		// never walk its reference backwards. This is the only branch that can
		// read storage, and its failure is the caller's to report unchanged.
		floorView, floorErr := a.projectedMasterView(ctx, floorID, asOf, acquisitionReadImmediate)
		if floorErr != nil {
			return selectedMasterView{}, floorErr
		}
		appendCandidate(floorView)
	}
	appendCandidate(top)
	appendCandidate(managed.master)

	var (
		chosen  *selectedMasterView
		relaxed *selectedMasterView
		cause   error
	)
	for _, view := range candidates {
		admitted, admitErr := a.admitShardMasterView(view, managed, session, previous, refs, kind)
		if admitErr != nil {
			cause = admitErr
			continue
		}
		if relaxed == nil {
			value := admitted
			relaxed = &value
		}
		if admitted.pinnable {
			value := admitted
			chosen = &value
			break
		}
	}

	selected := chosen
	switch {
	case selected != nil:
	case relaxed != nil && seedAllowed:
		// No view has all its neighbour tops committed here yet; this build is
		// allowed to seed them from state, so freshness wins over the walk.
		selected = relaxed
	case relaxed != nil:
		return selectedMasterView{}, fmt.Errorf(
			"%w: no masterchain view has every neighbour top committed for a build that may not seed",
			ErrAcquisitionNotReady,
		)
	default:
		// Every view refused. The shard chain has run past what any of them
		// registers, or none of them covers this session any more. Degrade the
		// slot to an empty candidate rather than spinning the window on a retry
		// that cannot succeed until the masterchain moves.
		return selectedMasterView{}, fmt.Errorf("%w: %v", errCollationMustBeEmpty, cause)
	}

	if floorID.SeqNo <= top.context.ID.SeqNo && selected.view != top {
		// Selecting the floor view above the resident one is reference
		// behaviour and not a fallback; being pushed below the resident view is.
		a.selectedViewFallbacks.Add(1)
		a.log.Debug().
			Uint32("resident_seqno", top.context.ID.SeqNo).
			Uint32("selected_seqno", selected.view.context.ID.SeqNo).
			Err(cause).
			Msg("masterchain view refused for this slot, building on an older one")
	}

	return *selected, nil
}
