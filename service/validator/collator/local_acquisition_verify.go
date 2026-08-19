package collator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
)

// TransitionAnnouncer receives the state update a candidate asserts, on the one
// path where the caller has to apply it to a predecessor tree of its own: a
// shard candidate with full collated data, whose verification runs on the
// states its own proof carries and never touches the tree this node holds.
// Everywhere else the successor the caller needs is the one verification
// already produced, and it is handed back through ValidatedSuccessor.Live
// instead of being computed twice — so nothing is announced there at all.
//
// It exists so that apply can overlap the tens of milliseconds of semantic
// validation that decide whether the transition is legitimate. It is therefore
// an overlap hint and never a result: the caller must not treat an announced
// update as validated, and the collator does not observe what the caller does
// with it.
//
// When it runs is part of the contract. It is called at most once per
// validation, on the goroutine running ValidateCandidate, and only once that
// call has passed every stage that can fail with ErrAcquisitionNotReady on a
// node that is merely behind — the masterchain view, the predecessor and the
// neighbour queues. A lagging node retries a not-ready candidate for as long as
// its consensus slot lasts, and announcing before those stages would start a
// whole abandoned apply per attempt: new store reads inside the slot, on the
// node least able to afford them. What remains after them is the semantic
// replay, which is the expensive part and the only part the overlap was ever
// meant to hide behind.
//
// The collator deliberately hands over the update and nothing else. It must not
// see the caller's full predecessor tree — the proof-backed validation path
// exists precisely to keep that tree out of its reach.
//
// What is handed over is the update with its verdict already decided and its
// update-side walks already recorded. That is still the update and nothing
// else: applying it remains a walk of the caller's own parent, and the capsule
// answers no question about that parent until it is given one.
type TransitionAnnouncer func(stateUpdate *cell.PreparedMerkleUpdate)

// ValidationRequest binds an ordinary consensus candidate to the exact,
// ordered predecessor states resolved by Simplex. Inputs are borrowed and
// immutable for the duration of ValidateCandidate.
type ValidationRequest struct {
	Session      ActivatedSession
	Update       SessionUpdate
	Previous     []PreviousBlock
	Candidate    simplex.Candidate
	BlockBOC     []byte
	CollatedData []byte
	// Digested is the caller's assertion that Candidate.CollatedFileHash is
	// already known to be the sha256 of CollatedData — not that it ought to be,
	// but that the digest was taken of these bytes and compared, or that these
	// bytes are where the digest came from.
	//
	// It exists because on every path that reaches here in production the
	// statement is a tautology and checking it costs a full sha256 of the
	// collated payload, twice per slot on a leader: the candidate codec derives
	// the hash from the payload it just decompressed and feeds it into the
	// candidate id the signature covers, and the local producer takes it inside
	// finish() from the buffer it just serialized. The reference re-checks
	// nothing here either — ValidateQuery::unpack_block_candidate
	// (validate-query.cpp:475-522) re-verifies the block file hash and uses
	// collated_file_hash for statistics only.
	//
	// Leave it false and the digest is taken, which is what every caller that
	// cannot make the statement should do. The block file hash is checked
	// unconditionally regardless; that one is parity with the reference.
	Digested bool
	// AnnounceTransition is optional. When absent, nothing about validation
	// changes: the caller simply performs its own work after this call instead
	// of alongside it.
	AnnounceTransition TransitionAnnouncer
}

// LiveSuccessorState is the successor tree verification itself produced, handed
// back on the runs where producing it again would be pure duplication.
//
// The distinction it encodes is which parent the verifier applied the update
// to. On the whole masterchain path, and for a shard candidate without full
// collated data, that parent is the caller's own resident tree — the very cells
// it passed in — so the result is its live successor and rebuilding it means
// walking the identical parent with the identical update a second time. On the
// full-collated shard path the verifier runs on the states the candidate's own
// proof carries; that root is narrow by construction and must never become a
// lineage state.
//
// So the type is empty exactly there, and cannot be filled from outside this
// package: its fields are unexported, its only producer refuses a proof-backed
// run, and Over hands the root out only to a caller that presents the trees it
// was built on. Carrying a narrow root back is unrepresentable rather than
// merely discouraged — the property ValidatedSuccessor was stripped of its
// State field to obtain, kept while giving the duplication back.
type LiveSuccessorState struct {
	root    *cell.Cell
	parents []*cell.Cell
	source  cell.Hash
}

// Over returns the successor state root, and only to a caller presenting the
// predecessor it was produced from: parents, by pointer and in the order they
// were handed to ValidateCandidate, and combined, the caller's own single root
// over them — for two predecessors the merged root, which must be the cell the
// verifier merged and not merely a tree of equal content.
//
// Pointer identity for the parents rather than hash equality, because a hash
// says two trees have the same content and not that they are the same
// materialization. The property being carried is that this root is the
// successor of a FULL parent, and the only tree known to be one is the caller's
// own; a hash comparison would accept a proof of it. The merged root is
// compared by hash instead because the caller builds its own, so identity is
// unavailable there — and a hash is exactly the right check for a two-reference
// wrapper whose whole content is those two references.
func (s LiveSuccessorState) Over(combined *cell.Cell, parents ...*cell.Cell) (*cell.Cell, bool) {
	if s.root == nil || combined == nil || len(parents) == 0 || len(parents) != len(s.parents) {
		return nil, false
	}
	for i := range parents {
		if parents[i] == nil || parents[i] != s.parents[i] {
			return nil, false
		}
	}
	if combined.HashKeyAt(0) != s.source {
		return nil, false
	}

	return s.root, true
}

// LiveSuccessorOf packages the successor of one caller's own predecessors as the
// carry-back token, by PERFORMING that apply here.
//
// It exists because the consumer of the token — the validator's
// validatedCandidateState — has a branch that only runs when a token opens, and
// that branch is where the "one materialization per block" property of the whole
// lineage is established. Covering it needs a token that opens, and the session
// producer above cannot be reached without a full masterchain fixture, so the
// consumer's own package had no way to exercise its own branch at all.
//
// It does not widen what a token means, which is the only thing that matters here.
// The token's guarantee is "this root is the successor of the trees named in it,
// and Over releases it to nobody else": established above by having applied the
// update to the caller's predecessors, and established here by doing exactly the
// same apply over exactly the parents named. A caller passing a proof-backed
// parent gets a token that opens for that proof-backed parent and for nothing
// else — and a ChainState root is never one, because newChainState refuses a proof
// root and every other producer of one is an apply over a full parent. So the
// property the unexported fields protect is preserved by construction rather than
// by the absence of this function.
//
// combined is the caller's single root over parents: the parent itself for one
// predecessor, and the merged root for two.
func LiveSuccessorOf(
	prepared *cell.PreparedMerkleUpdate,
	combined *cell.Cell,
	parents ...*cell.Cell,
) (LiveSuccessorState, error) {
	if prepared == nil || combined == nil || len(parents) == 0 {
		return LiveSuccessorState{}, fmt.Errorf("%w: live successor needs an update and a predecessor", ErrInvalidInput)
	}
	for i := range parents {
		if parents[i] == nil {
			return LiveSuccessorState{}, fmt.Errorf("%w: live successor predecessor %d is absent", ErrInvalidInput, i)
		}
	}
	root, err := prepared.ApplyTo(combined)
	if err != nil {
		return LiveSuccessorState{}, err
	}

	return LiveSuccessorState{
		root:    root,
		parents: append([]*cell.Cell(nil), parents...),
		source:  combined.HashKeyAt(0),
	}, nil
}

// ValidatedSuccessor is the transition a semantically verified candidate
// asserts, in a form that can be replayed onto any materialization of the same
// predecessor.
//
// It carries no state tree of its own, and that is the point: the root the
// verifier executed against may be proof-backed — narrow by construction, built
// on the states the candidate's own collated data proves rather than on the
// ones this node holds — and a narrow root must never become a lineage state.
// Handing back the transition instead of the result makes publishing that root
// unrepresentable, and leaves the caller to produce its successor from the
// parent it actually owns. Live is the bounded exception, and is empty on
// exactly the runs where the root would be narrow.
type ValidatedSuccessor struct {
	// BlockRoot is the parsed candidate block; BlockRoot.Hash() equals the
	// candidate ID's root hash.
	BlockRoot *cell.Cell
	// StateUpdate is the block's Merkle update. cell.ValidateMerkleUpdate has
	// already accepted it, so a caller applying it does not have to repeat that
	// walk — its verdict is a pure function of this cell and cannot change with
	// the parent it is applied to.
	StateUpdate *cell.Cell
	// Prepared is that same update as the capsule this verification decided it
	// with. It always carries the verdict; it carries the two update-side walks
	// as replayable plans only on the proof-backed shard path, which is the one
	// path where the caller applies the update again — the plans are order 32 B
	// per update node and are not built for a run that would never replay them.
	// Nil only where nothing here validated the update.
	Prepared *cell.PreparedMerkleUpdate
	// StateHash is the level-0 hash of the state the verifier actually produced
	// and replayed against, not the target hash the update claims. The two are
	// equal — verification asserts it — but only this one makes a comparison
	// against it a check on work that was performed.
	StateHash cell.Hash
	// Live is the successor tree itself, present only when the verifier built it
	// on the caller's own predecessors. It opens for no other parent.
	Live LiveSuccessorState
}

// ValidationResult contains the transition the candidate asserts and the
// earliest wall-clock instant at which the candidate may be voted for. The
// timestamp is the exact ConsensusExtraData millisecond value, matching
// ValidateCandidateResult::ok_from_utime.
type ValidationResult struct {
	ValidAfter time.Time
	Successor  ValidatedSuccessor
}

// ValidateCandidate acquires every authenticated auxiliary view required by
// deterministic validation. It never reads the shard-top inbox: a masterchain
// candidate carries the exact TopBlockDescrSet that its shard transition uses.
func (a *LocalAcquisition) ValidateCandidate(
	ctx context.Context,
	request ValidationRequest,
) (ValidationResult, error) {
	chain := metricChain(request.Session.Shard.IsMasterchain())
	inputWait := acquisitionInputWait{}
	readMode := acquisitionReadMode{waits: &inputWait}
	defer func() {
		a.observeValidationDuration(chain, ValidationCoreStageWaitInputs, inputWait.duration)
	}()
	if err := ctx.Err(); err != nil {
		return ValidationResult{}, err
	}
	if request.Candidate.Empty {
		return ValidationResult{}, fmt.Errorf("%w: local validation received an empty candidate", ErrInvalidInput)
	}
	if len(request.Previous) == 0 || len(request.Previous) > 2 {
		return ValidationResult{}, fmt.Errorf("%w: candidate has %d predecessors", ErrInvalidInput, len(request.Previous))
	}

	if err := validateSessionRecord(SessionRecord{
		Session: request.Session.Session,
		Activation: &SessionActivation{
			SessionID:      request.Session.ID,
			Genesis:        request.Session.Genesis,
			MinMasterchain: request.Session.MinMasterchain,
		},
		Update: request.Update,
	}); err != nil {
		return ValidationResult{}, err
	}
	if uint64(request.Candidate.Leader) >= uint64(len(request.Session.Validators)) {
		return ValidationResult{}, fmt.Errorf("%w: candidate leader index is outside the roster", ErrInvalidInput)
	}
	artifact := CandidateArtifact{
		SessionID:    request.Session.ID,
		Candidate:    request.Candidate,
		BlockBOC:     request.BlockBOC,
		CollatedData: request.CollatedData,
		digested:     request.Digested,
	}
	started, waited := a.validationStageStarted(), inputWait.duration
	// The capsule is this call's only parse of the candidate, and it dies with
	// the call: it pins the block DAG, the successor state tree, both
	// predecessor lists and the whole collated proof set.
	prepared, err := prepareValidationCandidate(
		ctx,
		artifact,
		request.Session.Validators[request.Candidate.Leader].PublicKey,
		request.Session.Shard.IsMasterchain(),
		request.Previous,
	)
	a.observeValidationWork(chain, ValidationCoreStageRestoreState, started, waited, inputWait.duration)
	if err != nil {
		return ValidationResult{}, err
	}
	header := &prepared.verified.block.BlockInfo
	genUtime := time.Unix(int64(header.GenUtime), 0)
	started, waited = a.validationStageStarted(), inputWait.duration
	base, err := a.validationMasterView(ctx, request.Session, request.Update, genUtime, readMode)
	a.observeValidationWork(chain, ValidationCoreStageMasterView, started, waited, inputWait.duration)
	if err != nil {
		return ValidationResult{}, err
	}
	// The first point at which the roster this candidate's creator is measured
	// against has been authenticated: validationMasterView compares every entry
	// of request.Session.Validators with the masterchain group snapshot. Before
	// it, a rejection here would be this node's local state voting down an honest
	// block.
	if err = prepared.verifyCreator(); err != nil {
		return ValidationResult{}, err
	}

	if request.Session.Shard.IsMasterchain() {
		if len(prepared.previous) != 1 {
			return ValidationResult{}, fmt.Errorf("%w: masterchain candidate requires one predecessor", ErrInvalidInput)
		}
		started, waited = a.validationStageStarted(), inputWait.duration
		master, previousState, viewErr := a.validationMasterPredecessor(
			ctx,
			base,
			prepared.previous[0],
			genUtime,
			readMode,
		)
		a.observeValidationWork(chain, ValidationCoreStageChainInputs, started, waited, inputWait.duration)
		if viewErr != nil {
			return ValidationResult{}, viewErr
		}
		started, waited = a.validationStageStarted(), inputWait.duration
		err = prepared.bindConfig(ctx, master.context.Config)
		a.observeValidationWork(chain, ValidationCoreStageDecode, started, waited, inputWait.duration)
		if err != nil {
			return ValidationResult{}, err
		}
		started, waited = a.validationStageStarted(), inputWait.duration
		if err = a.validateMasterCandidate(ctx, master, prepared, previousState, readMode); err != nil {
			a.observeValidationWork(chain, ValidationCoreStageTransition, started, waited, inputWait.duration)

			return ValidationResult{}, err
		}
		a.observeValidationWork(chain, ValidationCoreStageTransition, started, waited, inputWait.duration)
	} else {
		started, waited = a.validationStageStarted(), inputWait.duration
		master, viewErr := a.validationMasterReference(ctx, base, header.MasterRef, genUtime, readMode)
		a.observeValidationWork(chain, ValidationCoreStageChainInputs, started, waited, inputWait.duration)
		if viewErr != nil {
			return ValidationResult{}, viewErr
		}
		started, waited = a.validationStageStarted(), inputWait.duration
		err = prepared.bindConfig(ctx, master.context.Config)
		a.observeValidationWork(chain, ValidationCoreStageDecode, started, waited, inputWait.duration)
		if err != nil {
			return ValidationResult{}, err
		}
		started, waited = a.validationStageStarted(), inputWait.duration
		if err = a.validateShardCandidate(
			ctx,
			request.Session.Session,
			master,
			prepared,
			request.AnnounceTransition,
			readMode,
		); err != nil {
			a.observeValidationWork(chain, ValidationCoreStageTransition, started, waited, inputWait.duration)

			return ValidationResult{}, err
		}
		a.observeValidationWork(chain, ValidationCoreStageTransition, started, waited, inputWait.duration)
	}

	a.observeValidatedCandidate(chain, request, &prepared.verified)

	return ValidationResult{
		ValidAfter: time.UnixMilli(int64(prepared.verified.collated.genUtimeMS)),
		Successor: ValidatedSuccessor{
			BlockRoot:   prepared.root,
			StateUpdate: prepared.verified.block.StateUpdate,
			Prepared:    prepared.verified.stateUpdate,
			// The root this call produced and replayed against, which on the
			// full-collated shard path is the proof-backed one. Reporting its
			// hash rather than the update's own PeekRef(1) claim is what turns
			// the caller's comparison into a check on work performed here.
			StateHash: prepared.stateRoot.HashKeyAt(0),
			Live:      prepared.liveSuccessor(),
		},
	}, nil
}

// observeValidatedCandidate reports the shape of a block this node checked but
// did not build, onto the same metrics collation reports its own blocks to. The
// counts come from the replay, which walked the descriptors and transactions to
// verify them; nothing here re-reads the block. Only accepted candidates are
// reported: a rejected one has no meaningful shape, and the reasons it was
// rejected are their own metric.
func (a *LocalAcquisition) observeValidatedCandidate(
	chain MetricChain,
	request ValidationRequest,
	verified *verifiedCandidate,
) {
	if a.collationObserver == nil {
		return
	}

	a.collationObserver.ObserveCollationCandidate(CandidateObservation{
		Chain:         chain,
		Origin:        CandidateOriginValidation,
		Kind:          CandidateKindBlock,
		BlockBytes:    len(request.BlockBOC),
		CollatedBytes: len(request.CollatedData),
		Shape:         verified.shape,
	})
}

func (a *LocalAcquisition) validationStageStarted() time.Time {
	if a.validationObserver == nil {
		return time.Time{}
	}

	return time.Now()
}

func (a *LocalAcquisition) observeValidationWork(
	chain MetricChain,
	stage ValidationCoreStage,
	started time.Time,
	waitedBefore time.Duration,
	waitedAfter time.Duration,
) {
	if a.validationObserver == nil {
		return
	}
	duration := time.Since(started) - (waitedAfter - waitedBefore)
	if duration < 0 {
		duration = 0
	}

	a.observeValidationDuration(chain, stage, duration)
}

func (a *LocalAcquisition) observeValidationDuration(
	chain MetricChain,
	stage ValidationCoreStage,
	duration time.Duration,
) {
	if a.validationObserver == nil {
		return
	}

	a.validationObserver.ObserveValidationCoreStage(ValidationCoreStageObservation{
		Chain:    chain,
		Stage:    stage,
		Duration: duration,
	})
}

func (a *LocalAcquisition) validationMasterView(
	ctx context.Context,
	session ActivatedSession,
	update SessionUpdate,
	asOf time.Time,
	mode acquisitionReadMode,
) (*localMasterView, error) {
	master, err := a.projectedMasterView(ctx, update.MasterchainBlock, asOf, mode)
	if err != nil {
		return nil, err
	}
	if err = master.requireAncestor(session.MinMasterchain); err != nil {
		return nil, err
	}
	if err = validateAcquisitionGroup(session, update, master.context.Groups, true); err != nil {
		return nil, err
	}

	return master, nil
}

func (a *LocalAcquisition) projectedMasterView(
	ctx context.Context,
	id ton.BlockIDExt,
	asOf time.Time,
	mode acquisitionReadMode,
) (*localMasterView, error) {
	// The applied-block hook publishes the newest coherent block/state pair
	// before the node makes it visible through LocalStateStore. Session updates
	// run from that same hook, so an exact resident hit must precede storage.
	// localMasterView and everything reachable from it are immutable.
	cached, resident := a.master.lookup(id)
	if resident {
		return cached, nil
	}

	previous, state, err := a.loadPrevious(ctx, id, mode)
	if err != nil {
		return nil, err
	}
	input := groups.ApplyInput{
		Block: id,
		Root:  previous.State,
		AsOf:  asOf,
	}
	snapshot, err := a.projectValidatorGroups(ctx, input, mode)
	if err != nil {
		if mode.waitsForInputs() && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
			return nil, err
		}

		return nil, fmt.Errorf("%w: project validator groups: %v", ErrAcquisitionNotReady, err)
	}
	if snapshot == nil || !snapshot.MasterchainBlock.Equals(&id) {
		return nil, fmt.Errorf("%w: projected validator groups belong to another masterchain block", ErrInvalidInput)
	}
	if !snapshot.Ready {
		return nil, fmt.Errorf("%w: projected validator group snapshot is not ready", ErrAcquisitionNotReady)
	}
	// A non-resident hit is only reusable while the tracker still answers with
	// the very snapshot the view was built around. Once the tracker prunes past
	// a rotation it deliberately refuses that masterchain block, and a cached
	// view must not keep serving it; comparing the pointer is what enforces
	// that, and it is why the projection above runs before this check rather
	// than being skipped on a hit.
	if cached != nil && cached.context.Groups == snapshot {
		return cached, nil
	}
	master, err := a.masterView(previous, state, snapshot)
	if err != nil {
		return nil, err
	}
	if master.context.Config.execution.Root().HashKey() != snapshot.ConfigRootHash {
		return nil, fmt.Errorf("%w: projected validator group config differs from masterchain state", ErrInvalidInput)
	}
	a.master.store(master)

	return master, nil
}

func (a *LocalAcquisition) projectValidatorGroups(
	ctx context.Context,
	input groups.ApplyInput,
	mode acquisitionReadMode,
) (*groups.Snapshot, error) {
	snapshot, err := a.groups.Project(nil, input)
	if !mode.waitsForInputs() || (!errors.Is(err, groups.ErrNoSnapshot) &&
		(err != nil || (snapshot != nil && snapshot.Ready))) {
		return snapshot, err
	}

	err = mode.waitFor(func() error {
		snapshot, err = a.groups.WaitProject(ctx, nil, input)

		return err
	})

	return snapshot, err
}

func (a *LocalAcquisition) validationMasterReference(
	ctx context.Context,
	base *localMasterView,
	reference *tlb.ExtBlkRef,
	asOf time.Time,
	mode acquisitionReadMode,
) (*localMasterView, error) {
	if reference == nil {
		return nil, fmt.Errorf("%w: shard candidate has no masterchain reference", ErrInvalidInput)
	}
	if len(reference.RootHash) != 32 || len(reference.FileHash) != 32 {
		return nil, fmt.Errorf("%w: candidate masterchain reference has invalid hashes", ErrInvalidInput)
	}
	if reference.SeqNo > base.context.ID.SeqNo {
		// Validation is independent from the local collator actor in C++. Its
		// manager resolves the exact masterchain reference carried by the shard
		// candidate, which can legitimately be newer than this runtime's last
		// published collation view. Load that authenticated state directly and
		// require the cached view to be its ancestor.
		id := ton.BlockIDExt{
			Workchain: masterchainWorkchainID,
			Shard:     math.MinInt64,
			SeqNo:     reference.SeqNo,
			RootHash:  bytes.Clone(reference.RootHash),
			FileHash:  bytes.Clone(reference.FileHash),
		}
		view, err := a.projectedMasterView(ctx, id, asOf, mode)
		if err != nil {
			return nil, err
		}
		if err = view.requireAncestor(base.context.ID); err != nil {
			return nil, fmt.Errorf("%w: candidate masterchain reference does not extend the selected history", ErrInvalidInput)
		}

		return view, nil
	}
	id, err := base.blockAt(reference.SeqNo)
	if err != nil {
		return nil, err
	}
	if !equalBlockHash(id, reference.RootHash, reference.FileHash) {
		return nil, fmt.Errorf("%w: candidate masterchain reference is not in the selected history", ErrInvalidInput)
	}
	if id.Equals(&base.context.ID) {
		return base, nil
	}

	// Every shard candidate in a session names the same older masterchain block,
	// so this is the path that used to rebuild the whole view per candidate.
	// projectedMasterView performs the identical load and projection on a miss
	// and revalidates the group binding on a hit.
	view, err := a.projectedMasterView(ctx, id, asOf, mode)
	if err != nil {
		return nil, err
	}
	if err = validateProjectedMasterView(view, view.context.Groups); err != nil {
		return nil, err
	}

	return view, nil
}

// validationMasterPredecessor resolves the masterchain view a candidate's own
// predecessor defines. Its second result is that predecessor's state, and only
// when this function obtained it from verifyPredecessor on the very same value
// — the caller hands it back to verification so the call is not repeated. The
// other branches never run that check, so they return nil and verification
// runs it itself.
func (a *LocalAcquisition) validationMasterPredecessor(
	ctx context.Context,
	base *localMasterView,
	previous PreviousBlock,
	asOf time.Time,
	mode acquisitionReadMode,
) (*localMasterView, *acquiredPredecessorState, error) {
	if previous.ID.Equals(&base.context.ID) {
		return base, nil, nil
	}
	if previous.ID.SeqNo > base.context.ID.SeqNo {
		view, err := a.masterViewForPredecessor(ctx, base, previous, asOf)
		if err != nil {
			return nil, nil, err
		}
		if err = validateProjectedMasterView(view, view.context.Groups); err != nil {
			return nil, nil, err
		}

		return view, nil, nil
	}
	if err := base.requireAncestor(previous.ID); err != nil {
		return nil, nil, err
	}
	state, err := verifyPredecessor("master", &previous)
	if err != nil {
		return nil, nil, err
	}
	// This branch deliberately does not go through projectedMasterView: it
	// builds from the caller's own block/state pair, which is authoritative
	// here and need not be readable from local storage. The cache is still
	// consulted, but only as a source of an already decoded view for the exact
	// same state — hence the root-hash assertion.
	cached, _ := a.master.lookup(previous.ID)
	if cached != nil && cached.previous.State.HashKey() != previous.State.HashKey() {
		cached = nil
	}
	projected, err := a.projectValidatorGroups(ctx, groups.ApplyInput{
		Block: previous.ID,
		Root:  previous.State,
		AsOf:  asOf,
	}, mode)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, nil, err
		}

		return nil, nil, fmt.Errorf("%w: project predecessor validator groups: %v", ErrAcquisitionNotReady, err)
	}
	if projected == nil || !projected.MasterchainBlock.Equals(&previous.ID) {
		return nil, nil, fmt.Errorf("%w: projected validator groups belong to another masterchain block", ErrInvalidInput)
	}

	view := cached
	if view == nil || view.context.Groups != projected {
		view, err = a.masterView(previous, state, projected)
		if err != nil {
			return nil, nil, err
		}
		a.master.store(view)
	}
	if err = validateProjectedMasterView(view, projected); err != nil {
		return nil, nil, err
	}

	return view, &acquiredPredecessorState{root: previous.State, state: state}, nil
}

func validateProjectedMasterView(view *localMasterView, snapshot *groups.Snapshot) error {
	if snapshot == nil || !snapshot.Ready || !snapshot.MasterchainBlock.Equals(&view.context.ID) {
		return fmt.Errorf("%w: projected validator group snapshot is not ready for the masterchain view", ErrAcquisitionNotReady)
	}
	if view.context.Config.execution.Root().HashKey() != snapshot.ConfigRootHash {
		return fmt.Errorf("%w: projected validator group config differs from masterchain state", ErrInvalidInput)
	}

	return nil
}

func (a *LocalAcquisition) validateShardCandidate(
	ctx context.Context,
	session Session,
	master *localMasterView,
	prepared *preparedValidationCandidate,
	announce TransitionAnnouncer,
	mode acquisitionReadMode,
) error {
	previous := prepared.previous
	verified := &prepared.verified
	expected, err := expectedShardNeighbors(master.context, session.Shard)
	if err != nil {
		return err
	}
	// Candidate validation never emits a collated proof: no
	// FullCollatedProofProvider is constructed on this path, so tracing a
	// resident neighbor state would only pin a materialized cell per queue cell
	// the replay walks.
	neighbors, err := a.loadExpectedNeighbors(
		ctx,
		master,
		expected,
		previous,
		nil,
		&verified.collated,
		false,
		mode,
	)
	if err != nil {
		return err
	}
	endLT, err := a.historicalShardEndLT(
		ctx,
		master,
		previous,
		neighbors,
		&verified.collated,
		nil,
		mode,
	)
	if err != nil {
		return err
	}
	// The one announcement in the whole of validation, and the last point before
	// the semantic replay. Everything above it — the masterchain view, the
	// neighbour queues, the historical end LTs — is what a node that is merely
	// behind may still be waiting for, so announcing above it would start a
	// whole apply before this task can know it will reach replay. Only the
	// proof-backed run needs one at all:
	// on every other path the caller receives the successor this call already
	// built, and an apply on its side would be a second walk over the parent
	// this one just walked.
	if announce != nil && prepared.substituted {
		announce(verified.stateUpdate)
	}
	request := ShardVerificationRequest{
		Previous:           previous[0],
		Masterchain:        master.context,
		Neighbors:          neighbors,
		NeighborShardEndLT: endLT,
		Semantics:          a.semantics,
		Candidate:          prepared.candidate,
		stateProven:        verified.collated.full,
	}
	if len(previous) == 2 {
		request.Previous2 = &previous[1]
	}

	return verifyPreparedShardCandidate(ctx, request, verified)
}

func (a *LocalAcquisition) validateMasterCandidate(
	ctx context.Context,
	master *localMasterView,
	prepared *preparedValidationCandidate,
	previousState *acquiredPredecessorState,
	mode acquisitionReadMode,
) error {
	previous := prepared.previous[0]
	verified := &prepared.verified
	registry, tops, err := a.masterValidationTops(ctx, master, verified)
	if err != nil {
		return err
	}
	expected := expectedMasterNeighbors(registry, previous.ID)
	neighbors, err := a.loadExpectedNeighbors(
		ctx,
		master,
		expected,
		[]PreviousBlock{previous},
		nil,
		&verified.collated,
		false,
		mode,
	)
	if err != nil {
		return err
	}
	endLT, err := a.historicalShardEndLT(
		ctx,
		master,
		[]PreviousBlock{previous},
		neighbors,
		&verified.collated,
		registry,
		mode,
	)
	if err != nil {
		return err
	}

	return verifyPreparedMasterCandidate(ctx, MasterVerificationRequest{
		Previous:           previous,
		Config:             master.context.Config,
		Groups:             master.context.Groups,
		ShardTops:          tops,
		Neighbors:          neighbors,
		NeighborShardEndLT: endLT,
		Semantics:          a.semantics,
		Candidate:          prepared.candidate,
		previousState:      previousState,
	}, verified)
}

func equalBlockHash(id ton.BlockIDExt, rootHash, fileHash []byte) bool {
	return bytes.Equal(id.RootHash, rootHash) && bytes.Equal(id.FileHash, fileHash)
}
