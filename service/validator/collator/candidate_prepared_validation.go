package collator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"sync"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// preparedValidationCandidate is the one decoded representation of one
// candidate inside one validation call. A network candidate borrows the roots
// its codec already parsed and a locally produced one the roots its collation
// serialized; every other path parses its BOCs here exactly once.
// Everything downstream — the master view selection, the structural pass, the
// semantic replay — reads the block, the successor state and the collated proof
// set from here.
//
// It is deliberately not a cache and must never be stored: it transitively pins
// the block DAG, the successor state tree, both predecessor trees (the resident
// list and the proof-backed one), the merged split root and the whole collated
// proof set. Those are exactly the objects the finalized watermark sweep in the
// validator's state resolver exists to release, and one capsule per competing
// child — including every fork consensus abandons — would pin a full successor
// tree, a parent tree and a proof set each. Build it at the entry point, pass
// the pointer down, let it die with the call.
//
// It is also not nullable: there is no path that re-parses when it is absent.
// A cache may have a fallback because its absence costs work; this one carries
// the checks themselves, so a fallback would be a second, unchecked way in.
type preparedValidationCandidate struct {
	// ---- stage 1: a pure function of the candidate bytes and its ID ----

	// candidate is assembled here on the session path and supplied by the
	// caller on the exported one. State and StateUpdate are filled by stage 2.
	candidate *Candidate
	// root is the decoded block, hash-checked against candidate.ID.RootHash and
	// recording nothing. Two of its three producers attach no trace listener at
	// all — the network BOC decoder and the byte path below — and the third, our
	// own collation, sealed its read set before it released this tree, which
	// stops the trace those cells still carry from propagating or recording.
	root *cell.Cell

	// ---- stage 2: predecessor selection (still no update walk) ----

	// resident is exactly the predecessor list the caller handed in. It is
	// never overwritten: after a proof substitution it is the only remaining
	// handle on the trees this node actually holds.
	resident []PreviousBlock
	// previous is the effective predecessor list — proof-backed when the
	// candidate carries full collated data for a shard, resident otherwise.
	previous []PreviousBlock
	// substituted records that previous is NOT resident: the states came from
	// the candidate's own collated proof. It is the one bit that decides whether
	// stateRoot may be handed back to the caller as its own live successor —
	// and, because that is the same question as whether the caller still has to
	// apply the update itself, whether bindConfig records the apply plans. It is
	// stored rather than re-derived from a pointer comparison at the point of
	// use.
	substituted bool
	// stateRoot is filled by bindConfig after the authenticated config and byte
	// limits are known. It is the successor the update produced on previous.
	stateRoot *cell.Cell
	// sourceRoot is the level-0 hash of the single root the update was applied
	// to: previous[0].State, or the merged root over two predecessors.
	sourceRoot cell.Hash
	// stateApplied records that this capsule already applied stateUpdate to
	// sourceRoot. The structural pass can then bind its effective predecessor by
	// hash instead of walking the same update a second time with MayApply.
	stateApplied bool
	// masterPredecessor is the parse the acquisition stage produced while
	// resolving the exact masterchain predecessor view. Exported verification
	// leaves it nil and performs the standalone predecessor check itself.
	masterPredecessor *acquiredPredecessorState

	// ---- stage 3: config-bound update verdict/apply and structural views ----

	verified verifiedCandidate
	// stateParsed records that verified.state is already the parse of
	// stateRoot, which the successor apply produced on its way to checking the
	// recovered state against the candidate ID. bindConfig then does not repeat
	// it: the call it would make is parseExact on that very cell. Only the
	// exported path, where nothing was applied, still has to run it.
	stateParsed bool
}

// prepareValidationCandidate performs the input-only stages for a consensus
// candidate: it binds the codec's decoded roots or decodes the bytes once, then
// decides which predecessor tree its successor will be built on. It deliberately
// does not walk or apply the state update; the master view and candidate size
// limits are still unknown.
//
// With full collated data a shard candidate is self-contained, so the
// predecessors are re-pointed at the states it proves and the resident ones are
// never touched — the node can validate a shard it does not hold, and cannot
// accept a candidate whose proof is narrower than the reference validator
// requires. The masterchain keeps its resident predecessor: collated data
// carries no masterchain state proof, because every validator already holds
// that state.
//
// createdBy is the creator the session roster names for this candidate's
// leader. It is recorded here and compared with the block's own claim only in
// verifyCreator, once that roster has been authenticated. Recording the roster
// value rather than the block's claim is what keeps the checks that consume it
// downstream — the masterchain creator statistics above all — measured against
// the roster even on a path that somehow skipped verifyCreator.
func prepareValidationCandidate(
	ctx context.Context,
	artifact CandidateArtifact,
	createdBy [32]byte,
	isMasterchain bool,
	previous []PreviousBlock,
) (*preparedValidationCandidate, error) {
	p := &preparedValidationCandidate{resident: previous, previous: previous}
	if (artifact.blockRoot == nil) != (artifact.collatedRoots == nil) {
		return nil, fmt.Errorf("%w: parsed candidate payload is incomplete", ErrInvalidInput)
	}
	if artifact.blockRoot != nil && !artifact.digested {
		return nil, fmt.Errorf("%w: parsed candidate payload has no digest provenance", ErrInvalidInput)
	}
	if err := p.prepareBlock(ctx, &Candidate{
		ID:               cloneBlockID(artifact.Candidate.Block),
		CreatedBy:        createdBy,
		BlockBOC:         artifact.BlockBOC,
		CollatedData:     artifact.CollatedData,
		CollatedFileHash: artifact.Candidate.CollatedFileHash,
		digested:         artifact.digested,
	}, artifact.blockRoot); err != nil {
		return nil, err
	}
	if err := p.verifyCollated(artifact.collatedRoots); err != nil {
		return nil, err
	}
	if p.verified.collated.full && !isMasterchain {
		proven, err := provenPredecessorStates(&p.verified.collated, previous)
		if err != nil {
			return nil, err
		}
		p.previous = proven
		p.substituted = true
	}

	p.candidate.StateUpdate = p.verified.block.StateUpdate
	if p.candidate.StateUpdate == nil {
		return nil, fmt.Errorf("%w: candidate state update is absent", ErrInvalidInput)
	}

	return p, nil
}

// liveSuccessor offers the successor tree this capsule built back to the caller,
// and only from the runs where it is the caller's own.
//
// The whole question is which parent the update was applied to. Where nothing
// was substituted, previous IS the resident list the caller handed in, so the
// root produced here is the successor of the caller's live parent and rebuilding
// it there means walking the same parent with the same update a second time.
// Where the collated proof replaced the predecessors, the root is narrow by
// construction and must never leave as a lineage state; the empty value is the
// only thing that can be offered, and LiveSuccessorState cannot be filled from
// outside this package.
func (p *preparedValidationCandidate) liveSuccessor() LiveSuccessorState {
	// The empty predecessor list is the exported verification path, where
	// stateRoot is the state the caller supplied rather than one applied here.
	// Over would refuse it anyway — it has no parent to be released against —
	// but a root nothing here produced should not be described as a successor
	// this call built.
	if p.substituted || p.stateRoot == nil || len(p.previous) == 0 {
		return LiveSuccessorState{}
	}
	parents := make([]*cell.Cell, len(p.previous))
	for i := range p.previous {
		parents[i] = p.previous[i].State
	}

	return LiveSuccessorState{root: p.stateRoot, parents: parents, source: p.sourceRoot}
}

// prepareVerificationCandidate is the whole capsule for a caller that already
// holds the successor state: the exported entry points and the benchmarks. The
// check order matches the session path everywhere the two can be compared, and
// the size limits additionally run before the decode here because this caller
// has the config from line one — which is why the id hashes, which the limits
// must not precede, are checked here as well as inside the decode.
func prepareVerificationCandidate(
	ctx context.Context,
	config *Config,
	candidate *Candidate,
	previous []PreviousBlock,
) (*preparedValidationCandidate, error) {
	if config == nil {
		return nil, fmt.Errorf("%w: candidate verification config is absent", ErrInvalidInput)
	}
	if candidate == nil {
		return nil, fmt.Errorf("%w: candidate is absent", ErrInvalidInput)
	}
	if candidate.State == nil || candidate.StateUpdate == nil {
		return nil, fmt.Errorf("%w: candidate state or state update is absent", ErrInvalidInput)
	}
	if err := verifyCandidateIDHashes(candidate); err != nil {
		return nil, err
	}
	if err := verifyCandidateSizeLimits(config, candidate); err != nil {
		return nil, err
	}

	p := &preparedValidationCandidate{resident: previous, previous: previous}
	if err := p.prepareBlock(ctx, candidate, nil); err != nil {
		return nil, err
	}
	// This caller names the creator itself instead of deriving it from a roster
	// it has yet to authenticate, so the check keeps the position it always had.
	if err := p.verifyCreator(); err != nil {
		return nil, err
	}
	p.stateRoot = candidate.State
	if err := p.verifyCollated(nil); err != nil {
		return nil, err
	}
	if err := p.bindConfig(ctx, config); err != nil {
		return nil, err
	}

	return p, nil
}

// prepareBlock is stage 1: everything that is a pure function of the candidate
// bytes, its decoded root and its consensus-fixed ID. None of it can change
// with local readiness — nothing here reads a view this node might be behind on
// — which is why it runs before the master view is resolved on the session
// path. The creator check is the one that does not qualify; see verifyCreator.
func (p *preparedValidationCandidate) prepareBlock(
	ctx context.Context,
	candidate *Candidate,
	root *cell.Cell,
) error {
	if candidate == nil {
		return fmt.Errorf("%w: candidate is absent", ErrInvalidInput)
	}
	if err := verifyCandidateIDHashes(candidate); err != nil {
		return err
	}
	// The file hash is what the block is stored and requested under. On the
	// wire path the candidate codec derives the ID from this same digest, but a
	// storage-recovered artifact and both exported entry points carry no such
	// guarantee, so it is checked unconditionally.
	fileHash := sha256.Sum256(candidate.BlockBOC)
	if !bytes.Equal(candidate.ID.FileHash, fileHash[:]) {
		return fmt.Errorf("%w: candidate block file hash mismatch", ErrInvalidInput)
	}
	// A supplied root replaces the decode, never the two hash checks around it:
	// the digest above says these bytes are the block the ID names, and the root
	// hash below says this tree is. What the decode also used to establish — that
	// our own serializer's output parses back — is not a property of the
	// candidate, and neither producer of a supplied root can offer it: the
	// network path decodes the wire and re-serializes BlockBOC from the result,
	// and the local path serializes it from the tree it built.
	if root == nil {
		var err error
		// Candidate owns BlockBOC for the full validation lifetime. Parsed cells
		// can therefore borrow its immutable payload without an arena copy.
		root, err = cell.FromBOCWithOptions(candidate.BlockBOC, cell.BOCParseOptions{
			NoCopyPayload: true,
		})
		if err != nil {
			return fmt.Errorf("%w: decode candidate block boc: %v", ErrInvalidInput, err)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !equalCellHashBytes(root, candidate.ID.RootHash) {
		return fmt.Errorf("%w: candidate block root hash mismatch", ErrInvalidInput)
	}

	block := &p.verified.block
	if err := parseExact(block, root); err != nil {
		return fmt.Errorf("%w: decode candidate block: %v", ErrInvalidInput, err)
	}
	// The reflection parse above is the lax one. This replaces its header and
	// extra with an exact re-parse of the same cells, so everything downstream
	// — including the GenUtime that selects the masterchain view — reads the
	// canonical header rather than whatever the lax decoder accepted.
	if err := verifyExactBlockParts(root, block); err != nil {
		return err
	}
	if err := verifyHeaderAndID(&block.BlockInfo, candidate); err != nil {
		return err
	}
	if block.Extra == nil {
		return fmt.Errorf("%w: candidate block extra is absent", ErrInvalidInput)
	}
	var zeroSeed [32]byte
	if len(block.Extra.RandSeed) != 32 || bytes.Equal(block.Extra.RandSeed, zeroSeed[:]) {
		return fmt.Errorf("%w: candidate random seed is zero or malformed", ErrInvalidInput)
	}
	if len(block.Extra.CreatedBy) != 32 {
		return fmt.Errorf("%w: candidate creator is malformed", ErrInvalidInput)
	}
	p.root = root
	p.candidate = candidate

	return nil
}

// verifyCreator binds the creator the block names to the validator the roster
// names for this candidate's leader.
//
// It is deliberately not part of stage 1, and that is the one check of the
// original stage 1 that could not stay there. Every other check in decodeBlock
// reads the candidate bytes and its consensus-fixed ID, both of which the whole
// network agrees on; this one reads a roster that on the session path is LOCAL
// state until validationMasterView authenticates it against the masterchain
// group snapshot. Run before that authentication it converts a stale or
// corrupted local roster into a REJECTION of an honest candidate — a vote
// against a block the rest of the network will finalize — where the same node
// with the same knowledge is supposed to abstain. The alternative, downgrading
// its verdict to not-ready before authentication, would leave the check in a
// place where it can never legitimately fire and hide the reorder; running it
// where the roster becomes trustworthy is what the code should say.
//
// What the move costs: a node that cannot resolve the masterchain view no
// longer votes down a candidate whose creator is forged. It could not have
// judged the roster the forgery is measured against either, so the verdict it
// gives up was never one it was entitled to.
func (p *preparedValidationCandidate) verifyCreator() error {
	if !bytes.Equal(p.verified.block.Extra.CreatedBy, p.candidate.CreatedBy[:]) {
		return fmt.Errorf("%w: candidate creator differs from block extra", ErrInvalidInput)
	}

	return nil
}

// verifyCollated verifies the decoded collated roots against the exact header's
// GenUtime. The byte path decodes them here first. One capsule owns both, so
// collated data belonging to another block is not representable here and needs
// no separate check.
func (p *preparedValidationCandidate) verifyCollated(roots []*cell.Cell) error {
	collated, err := verifyCollatedData(p.candidate, roots, p.verified.block.BlockInfo.GenUtime)
	if err != nil {
		return err
	}
	p.verified.collated = collated

	return nil
}

// bindConfig is stage 3: every check that needs the masterchain config, which
// on the session path is only known once the master view has been resolved.
func (p *preparedValidationCandidate) bindConfig(ctx context.Context, config *Config) error {
	if config == nil {
		return fmt.Errorf("%w: candidate verification config is absent", ErrInvalidInput)
	}
	candidate := p.candidate
	if err := verifyCandidateSizeLimits(config, candidate); err != nil {
		return err
	}

	block := &p.verified.block
	// The two expensive halves of this phase are independent read walks over
	// disjoint subtrees: verifyBlockDictionaries walks the block extra and
	// ValidateMerkleUpdate walks the state update. They overlap for the same
	// reason finish() overlaps its two serialization tails — BOC parsing has
	// already computed every cell hash, which is what makes concurrent read
	// walks safe, so switching this decode to a lazy BOC would break it.
	//
	// Both units always run to completion and their errors are reported in the
	// order the serial code produced them, so the rejection reason a candidate
	// gets does not depend on which walk finished first. The cost is that a
	// candidate rejected by one unit now also pays for the other.
	var (
		merkleErr   error
		stateUpdate *cell.PreparedMerkleUpdate
		merkle      sync.WaitGroup
	)
	if p.verified.stateUpdate == nil {
		// The plans are recorded only where one is applied, and that is the
		// proof-backed shard path alone: there the successor this call built
		// stands on the candidate's own proof, so the caller has to apply the
		// same update a second time to the parent it holds, and the plans are
		// what make that second walk map-free. Every other path applies once and
		// hands the result back — the masterchain always, a shard without full
		// collated data always, and the exported entry points, which apply
		// nothing at all. Recording plans there pins one entry per update node
		// for the length of the call and nothing ever replays them.
		planned := p.substituted
		merkle.Add(1)
		go func(update *cell.Cell) {
			defer merkle.Done()
			if planned {
				stateUpdate, merkleErr = cell.PrepareMerkleUpdatePlanned(update)
				return
			}
			stateUpdate, merkleErr = cell.PrepareMerkleUpdate(update)
		}(block.StateUpdate)
	}

	dictionaries, dictionariesErr := verifyBlockDictionaries(block.Extra, config.globalVersion)

	var (
		flow    tlb.ValueFlow
		flowErr error
	)
	if flowErr = parseExact(&flow, block.ValueFlow); flowErr != nil {
		flowErr = fmt.Errorf("%w: decode candidate value flow: %v", ErrInvalidInput, flowErr)
	} else if flowErr = flow.Validate(); flowErr != nil {
		flowErr = fmt.Errorf("%w: invalid candidate value flow: %v", ErrInvalidInput, flowErr)
	}
	var updateErr error
	if block.StateUpdate.HashKeyAt(0) != candidate.StateUpdate.HashKeyAt(0) {
		updateErr = fmt.Errorf("%w: candidate state update differs from block", ErrInvalidInput)
	}
	merkle.Wait()

	switch {
	case dictionariesErr != nil:
		return dictionariesErr
	case flowErr != nil:
		return flowErr
	case updateErr != nil:
		return updateErr
	case merkleErr != nil:
		return fmt.Errorf("%w: invalid candidate state update: %v", ErrInvalidInput, merkleErr)
	}
	if stateUpdate != nil {
		p.verified.stateUpdate = stateUpdate
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Only now — after the authenticated master view selected this config and
	// after its cheap byte limits — apply the already-decided update. This reuses
	// the update-side verdict/plan instead of paying a classic Apply walk before
	// PrepareMerkleUpdate and then walking the update again here.
	if p.stateRoot == nil {
		stateRoot, state, sourceRoot, applyErr := applyPreparedCandidateStateUpdate(
			p.previous,
			p.verified.stateUpdate,
		)
		if applyErr != nil {
			return applyErr
		}
		id := &candidate.ID
		if state.Seqno != id.SeqNo || state.ShardIdent.WorkchainID != id.Workchain ||
			int64(state.ShardIdent.GetShardID()) != id.Shard {
			return fmt.Errorf("%w: recovered state differs from candidate ID", ErrInvalidInput)
		}
		p.stateRoot = stateRoot
		p.sourceRoot = sourceRoot
		p.stateApplied = true
		p.verified.state = state
		p.stateParsed = true
		candidate.State = stateRoot
	}
	newStateProof, err := block.StateUpdate.PeekRef(1)
	if err != nil {
		return fmt.Errorf("%w: load candidate state update target: %v", ErrInvalidInput, err)
	}
	// Implied by the apply on the session path, where the successor tree was
	// built from this very update. It stays because the exported path applies
	// nothing, and because it is the pin that keeps both endpoints of the
	// transition compared against the state the caller supplied.
	if newStateProof.HashKeyAt(0) != candidate.State.HashKeyAt(0) {
		return fmt.Errorf("%w: candidate state hash differs from state update", ErrInvalidInput)
	}

	if !p.stateParsed {
		var state tlb.ShardStateUnsplit
		if err = parseExact(&state, candidate.State); err != nil {
			return fmt.Errorf("%w: decode candidate state: %v", ErrInvalidInput, err)
		}
		p.verified.state = state
		p.stateParsed = true
	}
	state := &p.verified.state
	var stats tlb.ShardStateStats
	if err = parseExact(&stats, state.Stats); err != nil {
		return fmt.Errorf("%w: decode candidate state statistics: %v", ErrInvalidInput, err)
	}
	if !stats.TotalValidatorFees.ExtraCurrencies.IsEmpty() {
		return fmt.Errorf("%w: candidate validator fees contain extra currencies", ErrInvalidInput)
	}
	var queue tlb.OutMsgQueueInfo
	if err = parseExact(&queue, state.OutMsgQueueInfo); err != nil {
		return fmt.Errorf("%w: decode candidate outbound queue: %v", ErrInvalidInput, err)
	}
	// Only ProcInfo is walked: it holds a handful of records and
	// minProcessedMCSeqno consumes it before the semantic phase runs. The out
	// queue and the dispatch queue are rebuilt from the predecessor and pinned
	// by root cell in verifyQueueRoots, which is stronger than a structural
	// walk because it also fixes the node labelling; walking them here would be
	// O(entire queue) against a candidate state that is never pruned.
	if queue.OutQueue == nil || queue.ProcInfo == nil || !queue.ProcInfo.ValidateAll() ||
		(queue.Extra != nil && queue.Extra.DispatchQueue == nil) {
		return fmt.Errorf("%w: invalid candidate outbound queue dictionaries", ErrInvalidInput)
	}
	if err = verifyOutQueueSizePresence(config, &queue); err != nil {
		return err
	}
	accountsRoot, err := candidate.State.PeekRef(1)
	if err != nil {
		return fmt.Errorf("%w: load candidate accounts: %v", ErrInvalidInput, err)
	}
	var accounts tlb.ShardAccountsAugDict
	if err = parseExact(&accounts, accountsRoot); err != nil {
		return fmt.Errorf("%w: decode candidate accounts: %v", ErrInvalidInput, err)
	}
	// The accounts dictionary is not walked here. verifyAccounts rebuilds it
	// from the predecessor with one update per account block and compares the
	// resulting root cell, so every node of the candidate tree is pinned to a
	// canonically built one. A walk would be O(all accounts in the shard) plus
	// a DepthBalanceInfo recomputation per node, against a state that is never
	// pruned.
	if accounts.AugmentedDictionary == nil {
		return fmt.Errorf("%w: invalid candidate accounts dictionary", ErrInvalidInput)
	}
	if err = verifyAccountsShardPrefix(state.ShardIdent, &accounts); err != nil {
		return err
	}
	if err = verifyStateHeader(block, state, candidate); err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}

	p.verified.flow = flow
	p.verified.stats = stats
	p.verified.queue = queue
	p.verified.inMessages = dictionaries.inMessages
	p.verified.outMessages = dictionaries.outMessages
	p.verified.accountBlocks = dictionaries.accountBlocks
	p.verified.accountBlockIndex = dictionaries.accountBlockIndex

	return nil
}

func verifyCandidateIDHashes(candidate *Candidate) error {
	if len(candidate.ID.RootHash) != 32 || len(candidate.ID.FileHash) != 32 {
		return fmt.Errorf("%w: candidate id hashes must be 256 bits", ErrInvalidInput)
	}

	return nil
}

func verifyCandidateSizeLimits(config *Config, candidate *Candidate) error {
	if uint64(len(candidate.BlockBOC)) > uint64(config.maxBlockBytes) {
		return fmt.Errorf(
			"%w: candidate block is %d bytes, limit is %d",
			ErrSizeLimit, len(candidate.BlockBOC), config.maxBlockBytes,
		)
	}
	if uint64(len(candidate.CollatedData)) > uint64(config.maxCollatedBytes) {
		return fmt.Errorf(
			"%w: candidate collated data is %d bytes, limit is %d",
			ErrSizeLimit, len(candidate.CollatedData), config.maxCollatedBytes,
		)
	}

	return nil
}
