package collator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"math"
	"sync"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/msgpool"
)

type verifiedCandidate struct {
	block tlb.Block
	flow  tlb.ValueFlow
	state tlb.ShardStateUnsplit
	stats tlb.ShardStateStats
	queue tlb.OutMsgQueueInfo
	// The three block dictionaries, parsed and augmentation-checked once.
	// ValidateAll recomputes every augmentation, so re-parsing them for the
	// value-flow pass would be a second O(all messages) walk per candidate.
	inMessages    *tlb.InMsgDescrAugDict
	outMessages   *tlb.OutMsgDescrAugDict
	accountBlocks *tlb.ShardAccountBlocksAugDict
	collated      verifiedCollatedData
	// shape is filled by the semantic replay from walks it performs anyway, so
	// a caller that wants to report what it validated does not have to walk the
	// block again. It is written by the replay's single-threaded phases only.
	shape CandidateShape
}

type mcBlockDetails struct {
	PrevBlockSignatures *cell.Dictionary `tlb:"dict 16"`
	RecoverCreateMsg    *cell.Cell       `tlb:"maybe ^"`
	MintMsg             *cell.Cell       `tlb:"maybe ^"`
}

// VerifyShardCandidate checks the self-contained block data and binds its
// state update to the supplied ordered predecessor state or states.
//
// Production does not enter here: the session path calls the second half
// (verifyPreparedShardCandidate) once acquisition has assembled the neighbour
// queues. This entry point stays exported for the benchmarks and for the
// cross-package test in service/validator that pins every rejection from either
// half to ErrCandidateRejected — a rejection escaping that class would be
// classified as a local fault and retried instead of voted down.
func VerifyShardCandidate(ctx context.Context, req ShardVerificationRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	verified, err := verifyCandidate(ctx, req.Masterchain.Config, req.Candidate)
	if err != nil {
		return err
	}

	return verifyPreparedShardCandidate(ctx, req, &verified)
}

func verifyPreparedShardCandidate(
	ctx context.Context,
	req ShardVerificationRequest,
	prepared *verifiedCandidate,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if req.Semantics == nil {
		return fmt.Errorf("%w: candidate transition verifier is absent", ErrInvalidInput)
	}

	config := req.Masterchain.Config
	previous := req.Previous
	previous2 := req.Previous2
	candidate := req.Candidate
	verified := *prepared
	header := &verified.block.BlockInfo
	if !header.NotMaster {
		return fmt.Errorf("%w: shard candidate declares a masterchain block", ErrInvalidInput)
	}
	if header.Shard.WorkchainID != 0 {
		return fmt.Errorf("%w: only basechain blocks are supported", ErrUnsupported)
	}
	if err := verifyBasechainWorkchain(config, header.GenUtime); err != nil {
		return err
	}
	if header.KeyBlock {
		return fmt.Errorf("%w: shard candidate declares a key block", ErrInvalidInput)
	}
	if verified.block.Extra.Custom != nil {
		return fmt.Errorf("%w: shard candidate contains masterchain block extra", ErrInvalidInput)
	}
	if verified.state.McStateExtra != nil {
		return fmt.Errorf("%w: shard candidate state contains masterchain state extra", ErrInvalidInput)
	}
	if header.MasterRef == nil {
		return fmt.Errorf("%w: shard candidate has no masterchain reference", ErrInvalidInput)
	}

	// Full collated data is bound to the resident predecessor first and then
	// replaces it, before anything decodes a predecessor cell. Everything below
	// — topology, load history, value flow, the whole semantic replay — then
	// runs inside the closure the candidate actually proved, exactly as the
	// reference validator does.
	form := residentPredecessor
	if verified.collated.full {
		form = provenPredecessor
		if err := verifyCollatedPredecessors(&verified.collated, previous, previous2); err != nil {
			return err
		}
		proven, proven2, provenErr := proofBackedPredecessors(&verified.collated, previous, previous2)
		if provenErr != nil {
			return provenErr
		}
		previous, previous2 = proven, proven2

		// The candidate state is rebound here rather than after the predecessor
		// is prepared, so that the load history and the value flow read it from
		// the proof too. The effective root is the merge pair when there are
		// two, the single predecessor otherwise — the same root the preparation
		// below arrives at, and cheap enough to derive twice.
		provenRoot := previous.State
		if previous2 != nil {
			combined, combineErr := mechanicalSplitState(previous.State, previous2.State)
			if combineErr != nil {
				return fmt.Errorf("combine proven merge predecessors: %w", combineErr)
			}
			provenRoot = combined
		}
		if !req.stateProven {
			if err := bindProofBackedCandidateState(&verified, provenRoot); err != nil {
				return err
			}
		}
	}

	firstState, err := verifyPredecessorState(form, "first", &previous)
	if err != nil {
		return err
	}
	var secondState *tlb.ShardStateUnsplit
	if previous2 != nil {
		state, stateErr := verifyPredecessorState(form, "second", previous2)
		if stateErr != nil {
			return stateErr
		}
		secondState = &state
	}
	if verified.block.GlobalID != firstState.GlobalID {
		return fmt.Errorf("%w: block global id differs from predecessor state", ErrInvalidInput)
	}

	topologyRequest := ShardRequest{
		Shard: groups.ShardID{
			Workchain: header.Shard.WorkchainID,
			Shard:     int64(header.Shard.GetShardID()),
		},
		Previous:    previous,
		Previous2:   previous2,
		Masterchain: req.Masterchain,
		Header:      HeaderParams{GenUtime: header.GenUtime},
		BeforeSplit: header.BeforeSplit,
	}
	topology, err := resolveShardTopologyState(topologyRequest, &firstState, secondState)
	if err != nil {
		return fmt.Errorf("verify candidate topology: %w", err)
	}
	if err = bindShardTopologySession(topologyRequest, &topology); err != nil {
		return err
	}
	if err = validateNeighbors(req.Neighbors); err != nil {
		return err
	}
	if header.Shard != topology.target || header.SeqNo != topology.seqno ||
		header.AfterSplit != (topology.kind == topologyAfterSplit) ||
		header.AfterMerge != (topology.kind == topologyAfterMerge) {
		return fmt.Errorf("%w: block header disagrees with predecessor topology", ErrInvalidInput)
	}
	if err = verifyBlockLTDelta(config, header, false); err != nil {
		return err
	}
	if err = verifyShardLoadHistory(&topology, &firstState, &verified); err != nil {
		return err
	}
	if err = verifyShardMasterchainBindings(req, &topology, &firstState, secondState, &verified); err != nil {
		return err
	}
	if err = verifyBlockReference(&header.PrevRef.Prev1, &previous, &firstState, "first"); err != nil {
		return err
	}

	var predecessor preparedPredecessor
	if previous2 == nil {
		if header.PrevRef.Prev2 != nil {
			return fmt.Errorf("%w: non-merge block has a second predecessor reference", ErrInvalidInput)
		}
		predecessor, err = prepareSinglePredecessor(topologyRequest, topology, previous.State, firstState)
	} else {
		if header.PrevRef.Prev2 == nil {
			return fmt.Errorf("%w: merge block has no second predecessor reference", ErrInvalidInput)
		}
		if err = verifyBlockReference(header.PrevRef.Prev2, previous2, secondState, "second"); err != nil {
			return err
		}
		oldRoot, rootErr := mechanicalSplitState(previous.State, previous2.State)
		if rootErr != nil {
			return fmt.Errorf("verify merge predecessor root: %w", rootErr)
		}
		predecessor, err = mergePredecessors(topologyRequest, topology, oldRoot, firstState, *secondState)
	}
	if err != nil {
		return fmt.Errorf("prepare effective predecessor: %w", err)
	}
	if err = verifyStateTransition(predecessor.oldRoot, candidate.State, verified.block.StateUpdate); err != nil {
		return err
	}
	if err = verifyShardValueFlow(config, &predecessor, &verified); err != nil {
		return err
	}
	if err = verifyShardMinRefMCSeqno(header, &verified.queue); err != nil {
		return err
	}
	if verified.collated.full {
		if err = verifyFullCollatedNeighborSet(req.Masterchain, header.Shard, req.Neighbors); err != nil {
			return err
		}
		if err = verifyCollatedNeighborStates(&verified.collated, req.Neighbors); err != nil {
			return err
		}
	}
	if !equalBlockReference(verified.stats.MasterRef, header.MasterRef) {
		return fmt.Errorf("%w: shard state master reference differs from block header", ErrInvalidInput)
	}
	// Shardchain states carry no public libraries, so emptiness is the whole
	// structural gate here; the masterchain index is instead proven by root in
	// verifyMasterPublicLibraries.
	if verified.stats.Libraries != nil && !verified.stats.Libraries.IsEmpty() {
		return fmt.Errorf("%w: shard state contains public libraries", ErrInvalidInput)
	}

	if err = verifyStateBalance(&verified); err != nil {
		return err
	}

	if err = req.Semantics.VerifyCandidateTransition(ctx, CandidateTransition{
		Config:             config,
		Previous:           previous,
		Previous2:          previous2,
		Masterchain:        &req.Masterchain,
		Neighbors:          req.Neighbors,
		Candidate:          candidate,
		FullCollatedData:   verified.collated.full,
		CollatedProofs:     &verified.collated,
		NeighborShardEndLT: req.NeighborShardEndLT,
		prepared: &preparedCandidateTransition{
			candidate: &verified,
			previous:  &predecessor.state,
		},
	}); err != nil {
		return fmt.Errorf("verify candidate semantic transition: %w", err)
	}
	// This function works on its own copy so its mutations stay local, but the
	// shape the replay counted is an output: it is the only place the caller can
	// learn what it just validated without walking the block again.
	prepared.shape = verified.shape

	return nil
}

func verifyShardValueFlow(config *Config, predecessor *preparedPredecessor, candidate *verifiedCandidate) error {
	flow := &candidate.flow
	if !currencyZero(flow.Minted) || !currencyZero(flow.Recovered) || !currencyZero(flow.Burned) {
		return fmt.Errorf("%w: shard candidate has masterchain-only value flow", ErrInvalidInput)
	}
	if !currencyZero(flow.FeesImported) {
		return fmt.Errorf("%w: shard candidate imports masterchain fees", ErrInvalidInput)
	}

	created := tlb.CurrencyCollection{}
	if candidate.block.BlockInfo.Shard.WorkchainID == 0 {
		createdNano := config.basechain.createFee.Nano()
		createdNano.Rsh(createdNano, uint(candidate.block.BlockInfo.Shard.PrefixBits))
		created.Coins = tlb.FromNanoTON(createdNano)
	}
	if !flow.Created.Equals(created) {
		return fmt.Errorf("%w: shard candidate creation fee differs from config", ErrInvalidInput)
	}

	previousBalance, err := loadAccountsBalance(predecessor.state.Accounts.ShardAccounts)
	if err != nil {
		return fmt.Errorf("%w: load effective predecessor balance: %v", ErrInvalidInput, err)
	}
	if !flow.FromPrevBlock.Equals(previousBalance) || !predecessor.stats.TotalBalance.Equals(previousBalance) {
		return fmt.Errorf("%w: shard predecessor balance and value flow disagree", ErrInvalidInput)
	}

	augmentations, err := loadCandidateValueFlowAugmentations(candidate)
	if err != nil {
		return err
	}
	if !flow.Imported.Equals(augmentations.importFees.ValueImported) {
		return fmt.Errorf("%w: shard imported value differs from inbound descriptors", ErrInvalidInput)
	}
	if !flow.Exported.Equals(augmentations.exported) {
		return fmt.Errorf("%w: shard exported value differs from outbound descriptors", ErrInvalidInput)
	}

	expectedFees, err := created.Add(augmentations.transactionFees)
	if err != nil {
		return fmt.Errorf("%w: add block creation and transaction fees: %v", ErrInvalidInput, err)
	}
	expectedFees, err = expectedFees.Add(tlb.CurrencyCollection{Coins: augmentations.importFees.FeesCollected})
	if err != nil {
		return fmt.Errorf("%w: add message import fees: %v", ErrInvalidInput, err)
	}
	if !flow.FeesCollected.Equals(expectedFees) {
		return fmt.Errorf("%w: shard collected fees differ from descriptor augmentations", ErrInvalidInput)
	}

	validatorFees, err := predecessor.stats.TotalValidatorFees.Add(flow.FeesCollected)
	if err != nil {
		return fmt.Errorf("%w: add resulting validator fees: %v", ErrInvalidInput, err)
	}
	if !candidate.stats.TotalValidatorFees.Equals(validatorFees) {
		return fmt.Errorf("%w: resulting shard validator fees disagree with value flow", ErrInvalidInput)
	}

	return nil
}

type candidateValueFlowAugmentations struct {
	importFees      tlb.ImportFees
	exported        tlb.CurrencyCollection
	transactionFees tlb.CurrencyCollection
}

func loadCandidateValueFlowAugmentations(
	candidate *verifiedCandidate,
) (candidateValueFlowAugmentations, error) {
	importFees, err := loadImportFees(candidate.inMessages.AugmentedDictionary)
	if err != nil {
		return candidateValueFlowAugmentations{}, fmt.Errorf(
			"%w: load inbound value flow augmentation: %v",
			ErrInvalidInput,
			err,
		)
	}
	exported, err := loadCurrencyExtra(candidate.outMessages.AugmentedDictionary)
	if err != nil {
		return candidateValueFlowAugmentations{}, fmt.Errorf(
			"%w: load outbound value flow augmentation: %v",
			ErrInvalidInput,
			err,
		)
	}
	transactionFees, err := loadCurrencyExtra(candidate.accountBlocks.AugmentedDictionary)
	if err != nil {
		return candidateValueFlowAugmentations{}, fmt.Errorf(
			"%w: load transaction fee augmentation: %v",
			ErrInvalidInput,
			err,
		)
	}

	return candidateValueFlowAugmentations{
		importFees:      importFees,
		exported:        exported,
		transactionFees: transactionFees,
	}, nil
}

func verifyShardLoadHistory(
	topology *shardTopology,
	previous *tlb.ShardStateUnsplit,
	candidate *verifiedCandidate,
) error {
	var previousStats tlb.ShardStateStats
	if topology.kind == topologyLinear {
		if err := parseExact(&previousStats, previous.Stats); err != nil {
			return fmt.Errorf("%w: decode predecessor load history: %v", ErrInvalidInput, err)
		}
	}

	return verifyLoadHistory(
		&previousStats,
		&candidate.stats,
		&candidate.block.BlockInfo,
		topology.kind != topologyLinear,
	)
}

func verifyShardMasterchainBindings(
	req ShardVerificationRequest,
	topology *shardTopology,
	first *tlb.ShardStateUnsplit,
	second *tlb.ShardStateUnsplit,
	candidate *verifiedCandidate,
) error {
	context := &req.Masterchain
	if context.ID.Workchain != address.MasterchainID || context.ID.Shard != math.MinInt64 {
		return fmt.Errorf("%w: reference block is not the masterchain root shard", ErrInvalidInput)
	}
	if context.Groups.ConfigRootHash != context.Config.execution.Root().HashKey() ||
		context.Groups.GenUTime != context.GenUtime {
		return fmt.Errorf("%w: validator group snapshot differs from the masterchain context", ErrInvalidInput)
	}
	if context.Groups.LastKeyBlockSeqno > context.ID.SeqNo {
		return fmt.Errorf("%w: previous key block is ahead of the masterchain context", ErrInvalidInput)
	}

	expectedRef, err := blockReference(context.ID, context.EndLT)
	if err != nil {
		return fmt.Errorf("%w: masterchain block id: %v", ErrInvalidInput, err)
	}
	header := &candidate.block.BlockInfo
	if !equalBlockReference(header.MasterRef, &expectedRef) ||
		!equalBlockReference(candidate.stats.MasterRef, &expectedRef) {
		return fmt.Errorf("%w: candidate masterchain reference differs from the verified context", ErrInvalidInput)
	}
	if header.GenValidatorListHashShort != topology.session.ValidatorSetHash ||
		header.GenCatchainSeqno != topology.session.CatchainSeqno ||
		header.PrevKeyBlockSeqno != context.Groups.LastKeyBlockSeqno {
		return fmt.Errorf("%w: shard header differs from the active validator session", ErrInvalidInput)
	}
	if context.VertSeqno < topology.previousVertSeqno || header.VertSeqNo != context.VertSeqno {
		return fmt.Errorf("%w: shard vertical sequence number differs from the masterchain context", ErrInvalidInput)
	}
	minGenUtime := max(topology.previousGenUtime, context.GenUtime)
	if context.Config.globalVersion >= 13 {
		if header.GenUtime < minGenUtime {
			return fmt.Errorf("%w: shard generation time precedes its verified context", ErrInvalidInput)
		}
	} else if header.GenUtime <= minGenUtime {
		return fmt.Errorf("%w: shard generation time does not follow its verified context", ErrInvalidInput)
	}
	startBase := max(topology.previousGenLT, context.EndLT)
	if header.StartLt <= startBase || header.StartLt-startBase > masterMaxLTGap {
		return fmt.Errorf("%w: shard start lt is outside the permitted range", ErrInvalidInput)
	}

	if err = verifyPredecessorMasterchainReference(req.Previous.ID, first, &expectedRef, "first"); err != nil {
		return err
	}
	if second != nil {
		if err = verifyPredecessorMasterchainReference(req.Previous2.ID, second, &expectedRef, "second"); err != nil {
			return err
		}
	}

	return nil
}

func verifyPredecessorMasterchainReference(
	previous ton.BlockIDExt,
	state *tlb.ShardStateUnsplit,
	next *tlb.ExtBlkRef,
	label string,
) error {
	var stats tlb.ShardStateStats
	if err := parseExact(&stats, state.Stats); err != nil {
		return fmt.Errorf("%w: decode %s predecessor statistics: %v", ErrInvalidInput, label, err)
	}
	if err := validatePredecessorMasterchainReference(previous, stats.MasterRef, next); err != nil {
		return fmt.Errorf("%w: %s predecessor masterchain reference: %v", ErrInvalidInput, label, err)
	}

	return nil
}

// VerifyMasterCandidate checks a linear masterchain transition. Masterchain
// split and merge flags are invalid, so it has exactly one predecessor.
func VerifyMasterCandidate(ctx context.Context, req MasterVerificationRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	verified, err := verifyCandidate(ctx, req.Config, req.Candidate)
	if err != nil {
		return err
	}

	return verifyPreparedMasterCandidate(ctx, req, &verified)
}

func verifyPreparedMasterCandidate(
	ctx context.Context,
	req MasterVerificationRequest,
	prepared *verifiedCandidate,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if req.Semantics == nil {
		return fmt.Errorf("%w: candidate transition verifier is absent", ErrInvalidInput)
	}
	if err := validateNeighbors(req.Neighbors); err != nil {
		return err
	}

	config := req.Config
	previous := req.Previous
	candidate := req.Candidate
	verified := *prepared
	header := &verified.block.BlockInfo
	if header.NotMaster {
		return fmt.Errorf("%w: master candidate declares a shardchain block", ErrInvalidInput)
	}
	if candidate.ID.Workchain != address.MasterchainID || candidate.ID.Shard != math.MinInt64 {
		return fmt.Errorf("%w: master candidate id is not the masterchain root shard", ErrInvalidInput)
	}
	if header.MasterRef != nil {
		return fmt.Errorf("%w: master candidate contains a masterchain reference", ErrInvalidInput)
	}
	if header.AfterMerge || header.AfterSplit || header.BeforeSplit {
		return fmt.Errorf("%w: master candidate declares a shard split or merge", ErrInvalidInput)
	}
	if header.PrevRef.Prev2 != nil {
		return fmt.Errorf("%w: master candidate has a second predecessor reference", ErrInvalidInput)
	}
	if verified.block.Extra.Custom == nil {
		return fmt.Errorf("%w: master candidate has no masterchain block extra", ErrInvalidInput)
	}
	if verified.state.McStateExtra == nil {
		return fmt.Errorf("%w: master candidate state has no masterchain state extra", ErrInvalidInput)
	}
	if verified.stats.MasterRef != nil {
		return fmt.Errorf("%w: masterchain state contains a masterchain reference", ErrInvalidInput)
	}

	previousState, err := verifyPredecessor("master", &previous)
	if err != nil {
		return err
	}
	if previous.ID.Workchain != address.MasterchainID || previous.ID.Shard != math.MinInt64 ||
		previousState.McStateExtra == nil {
		return fmt.Errorf("%w: predecessor is not a masterchain state", ErrInvalidInput)
	}
	if verified.block.GlobalID != previousState.GlobalID {
		return fmt.Errorf("%w: block global id differs from predecessor state", ErrInvalidInput)
	}
	if previous.ID.SeqNo == math.MaxUint32 || header.SeqNo != previous.ID.SeqNo+1 {
		return fmt.Errorf("%w: master candidate sequence number does not follow its predecessor", ErrInvalidInput)
	}
	if err = verifyBlockReference(&header.PrevRef.Prev1, &previous, &previousState, "master"); err != nil {
		return err
	}
	if err = verifyBlockLTDelta(config, header, true); err != nil {
		return err
	}
	if err = verifyStateTransition(previous.State, candidate.State, verified.block.StateUpdate); err != nil {
		return err
	}
	masterState, err := loadMasterCandidateState(config, &previousState, &verified)
	if err != nil {
		return err
	}
	if err = verifyMasterExtras(&verified, &masterState); err != nil {
		return err
	}
	if err = verifyMasterValueFlow(config, &verified, &masterState); err != nil {
		return err
	}
	if err = verifyLoadHistory(
		&masterState.previousStats,
		&verified.stats,
		header,
		false,
	); err != nil {
		return err
	}
	if err = verifyMasterDeterministicTransition(req, &previousState, &verified, &masterState); err != nil {
		return err
	}
	if verified.collated.full {
		registry, registryErr := ParseShardRegistry(masterState.nextExtra.ShardHashes)
		if registryErr != nil {
			return registryErr
		}
		if err = verifyFullMasterNeighborSet(previous, &previousState, registry, req.Neighbors); err != nil {
			return err
		}
		if err = verifyCollatedNeighborStates(&verified.collated, req.Neighbors); err != nil {
			return err
		}
	}

	if err = verifyStateBalance(&verified); err != nil {
		return err
	}

	if err = req.Semantics.VerifyCandidateTransition(ctx, CandidateTransition{
		Config:             config,
		Previous:           previous,
		ShardTops:          req.ShardTops,
		Neighbors:          req.Neighbors,
		Candidate:          candidate,
		FullCollatedData:   verified.collated.full,
		CollatedProofs:     &verified.collated,
		NeighborShardEndLT: req.NeighborShardEndLT,
		prepared: &preparedCandidateTransition{
			candidate:     &verified,
			previous:      &previousState,
			minimumBurned: masterState.minimumBurned,
		},
	}); err != nil {
		return fmt.Errorf("verify candidate semantic transition: %w", err)
	}
	// This function works on its own copy so its mutations stay local, but the
	// shape the replay counted is an output: it is the only place the caller can
	// learn what it just validated without walking the block again.
	prepared.shape = verified.shape

	return nil
}

func verifyCandidate(ctx context.Context, config *Config, candidate *Candidate) (verifiedCandidate, error) {
	return verifyCandidateWith(ctx, config, candidate, nil)
}

// verifyCandidateWith accepts collated data the caller has already decoded.
// Production decodes it before this call because the candidate state is built
// on top of the predecessor it proves, and decoding a proof of a few hundred
// kilobytes twice per candidate is not free. A nil value keeps the original
// behaviour of decoding it here.
func verifyCandidateWith(
	ctx context.Context,
	config *Config,
	candidate *Candidate,
	collated *verifiedCollatedData,
) (verifiedCandidate, error) {
	if config == nil {
		return verifiedCandidate{}, fmt.Errorf("%w: candidate verification config is absent", ErrInvalidInput)
	}
	if candidate == nil {
		return verifiedCandidate{}, fmt.Errorf("%w: candidate is absent", ErrInvalidInput)
	}
	if candidate.State == nil || candidate.StateUpdate == nil {
		return verifiedCandidate{}, fmt.Errorf("%w: candidate state or state update is absent", ErrInvalidInput)
	}
	if len(candidate.ID.RootHash) != 32 || len(candidate.ID.FileHash) != 32 {
		return verifiedCandidate{}, fmt.Errorf("%w: candidate id hashes must be 256 bits", ErrInvalidInput)
	}
	if uint64(len(candidate.BlockBOC)) > uint64(config.maxBlockBytes) {
		return verifiedCandidate{}, fmt.Errorf(
			"%w: candidate block is %d bytes, limit is %d",
			ErrSizeLimit, len(candidate.BlockBOC), config.maxBlockBytes,
		)
	}
	if uint64(len(candidate.CollatedData)) > uint64(config.maxCollatedBytes) {
		return verifiedCandidate{}, fmt.Errorf(
			"%w: candidate collated data is %d bytes, limit is %d",
			ErrSizeLimit, len(candidate.CollatedData), config.maxCollatedBytes,
		)
	}

	fileHash := sha256.Sum256(candidate.BlockBOC)
	if !bytes.Equal(candidate.ID.FileHash, fileHash[:]) {
		return verifiedCandidate{}, fmt.Errorf("%w: candidate block file hash mismatch", ErrInvalidInput)
	}
	root, err := cell.FromBOC(candidate.BlockBOC)
	if err != nil {
		return verifiedCandidate{}, fmt.Errorf("%w: decode candidate block boc: %v", ErrInvalidInput, err)
	}
	if err = ctx.Err(); err != nil {
		return verifiedCandidate{}, err
	}
	if !bytes.Equal(candidate.ID.RootHash, root.Hash()) {
		return verifiedCandidate{}, fmt.Errorf("%w: candidate block root hash mismatch", ErrInvalidInput)
	}

	var block tlb.Block
	if err = parseExact(&block, root); err != nil {
		return verifiedCandidate{}, fmt.Errorf("%w: decode candidate block: %v", ErrInvalidInput, err)
	}
	if err = verifyExactBlockParts(root, &block); err != nil {
		return verifiedCandidate{}, err
	}
	if err = verifyHeaderAndID(&block.BlockInfo, candidate); err != nil {
		return verifiedCandidate{}, err
	}
	if block.Extra == nil {
		return verifiedCandidate{}, fmt.Errorf("%w: candidate block extra is absent", ErrInvalidInput)
	}
	var zeroSeed [32]byte
	if len(block.Extra.RandSeed) != 32 || bytes.Equal(block.Extra.RandSeed, zeroSeed[:]) {
		return verifiedCandidate{}, fmt.Errorf("%w: candidate random seed is zero or malformed", ErrInvalidInput)
	}
	if len(block.Extra.CreatedBy) != 32 || !bytes.Equal(block.Extra.CreatedBy, candidate.CreatedBy[:]) {
		return verifiedCandidate{}, fmt.Errorf("%w: candidate creator differs from block extra", ErrInvalidInput)
	}
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
		merkleErr error
		merkle    sync.WaitGroup
	)
	merkle.Add(1)
	go func() {
		defer merkle.Done()
		merkleErr = cell.ValidateMerkleUpdate(block.StateUpdate)
	}()

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
		return verifiedCandidate{}, dictionariesErr
	case flowErr != nil:
		return verifiedCandidate{}, flowErr
	case updateErr != nil:
		return verifiedCandidate{}, updateErr
	case merkleErr != nil:
		return verifiedCandidate{}, fmt.Errorf("%w: invalid candidate state update: %v", ErrInvalidInput, merkleErr)
	}
	if err = ctx.Err(); err != nil {
		return verifiedCandidate{}, err
	}
	newStateProof, err := block.StateUpdate.PeekRef(1)
	if err != nil {
		return verifiedCandidate{}, fmt.Errorf("%w: load candidate state update target: %v", ErrInvalidInput, err)
	}
	if newStateProof.HashKeyAt(0) != candidate.State.HashKeyAt(0) {
		return verifiedCandidate{}, fmt.Errorf("%w: candidate state hash differs from state update", ErrInvalidInput)
	}

	var state tlb.ShardStateUnsplit
	if err = parseExact(&state, candidate.State); err != nil {
		return verifiedCandidate{}, fmt.Errorf("%w: decode candidate state: %v", ErrInvalidInput, err)
	}
	var stats tlb.ShardStateStats
	if err = parseExact(&stats, state.Stats); err != nil {
		return verifiedCandidate{}, fmt.Errorf("%w: decode candidate state statistics: %v", ErrInvalidInput, err)
	}
	if !stats.TotalValidatorFees.ExtraCurrencies.IsEmpty() {
		return verifiedCandidate{}, fmt.Errorf("%w: candidate validator fees contain extra currencies", ErrInvalidInput)
	}
	var queue tlb.OutMsgQueueInfo
	if err = parseExact(&queue, state.OutMsgQueueInfo); err != nil {
		return verifiedCandidate{}, fmt.Errorf("%w: decode candidate outbound queue: %v", ErrInvalidInput, err)
	}
	// Only ProcInfo is walked: it holds a handful of records and
	// minProcessedMCSeqno consumes it before the semantic phase runs. The out
	// queue and the dispatch queue are rebuilt from the predecessor and pinned
	// by root cell in verifyQueueRoots, which is stronger than a structural
	// walk because it also fixes the node labelling; walking them here would be
	// O(entire queue) against a candidate state that is never pruned.
	if queue.OutQueue == nil || queue.ProcInfo == nil || !queue.ProcInfo.ValidateAll() ||
		(queue.Extra != nil && queue.Extra.DispatchQueue == nil) {
		return verifiedCandidate{}, fmt.Errorf("%w: invalid candidate outbound queue dictionaries", ErrInvalidInput)
	}
	if err = verifyOutQueueSizePresence(config, &queue); err != nil {
		return verifiedCandidate{}, err
	}
	accountsRoot, err := candidate.State.PeekRef(1)
	if err != nil {
		return verifiedCandidate{}, fmt.Errorf("%w: load candidate accounts: %v", ErrInvalidInput, err)
	}
	var accounts tlb.ShardAccountsAugDict
	if err = parseExact(&accounts, accountsRoot); err != nil {
		return verifiedCandidate{}, fmt.Errorf("%w: decode candidate accounts: %v", ErrInvalidInput, err)
	}
	// The accounts dictionary is not walked here. verifyAccounts rebuilds it
	// from the predecessor with one update per account block and compares the
	// resulting root cell, so every node of the candidate tree is pinned to a
	// canonically built one. A walk would be O(all accounts in the shard) plus
	// a DepthBalanceInfo recomputation per node, against a state that is never
	// pruned.
	if accounts.AugmentedDictionary == nil {
		return verifiedCandidate{}, fmt.Errorf("%w: invalid candidate accounts dictionary", ErrInvalidInput)
	}
	if err = verifyAccountsShardPrefix(state.ShardIdent, &accounts); err != nil {
		return verifiedCandidate{}, err
	}
	if err = verifyStateHeader(&block, &state, candidate); err != nil {
		return verifiedCandidate{}, err
	}
	if err = ctx.Err(); err != nil {
		return verifiedCandidate{}, err
	}
	var collatedData verifiedCollatedData
	if collated != nil {
		collatedData = *collated
		if collatedData.genUtimeMS/1000 != uint64(block.BlockInfo.GenUtime) {
			return verifiedCandidate{}, fmt.Errorf("%w: supplied collated data belongs to another block", ErrInvalidInput)
		}
	} else if collatedData, err = verifyCollatedData(candidate, block.BlockInfo.GenUtime); err != nil {
		return verifiedCandidate{}, err
	}

	return verifiedCandidate{
		block:         block,
		flow:          flow,
		state:         state,
		inMessages:    dictionaries.inMessages,
		outMessages:   dictionaries.outMessages,
		accountBlocks: dictionaries.accountBlocks,
		stats:         stats,
		queue:         queue,
		collated:      collatedData,
	}, nil
}

func verifyExactBlockParts(root *cell.Cell, block *tlb.Block) error {
	headerRoot, err := root.PeekRef(0)
	if err != nil {
		return fmt.Errorf("%w: load candidate block header: %v", ErrInvalidInput, err)
	}
	var header tlb.BlockHeader
	if err = parseExact(&header, headerRoot); err != nil {
		return fmt.Errorf("%w: decode exact candidate block header: %v", ErrInvalidInput, err)
	}
	if err = verifyHeaderReferenceCells(headerRoot, &header); err != nil {
		return err
	}
	block.BlockInfo = header

	extraRoot, err := root.PeekRef(3)
	if err != nil {
		return fmt.Errorf("%w: load candidate block extra: %v", ErrInvalidInput, err)
	}
	var extra tlb.BlockExtra
	if err = parseExact(&extra, extraRoot); err != nil {
		return fmt.Errorf("%w: decode exact candidate block extra: %v", ErrInvalidInput, err)
	}
	if extra.Custom != nil {
		customRoot, customErr := extraRoot.PeekRef(3)
		if customErr != nil {
			return fmt.Errorf("%w: load masterchain block extra: %v", ErrInvalidInput, customErr)
		}
		var custom tlb.McBlockExtra
		if customErr = parseExact(&custom, customRoot); customErr != nil {
			return fmt.Errorf("%w: decode exact masterchain block extra: %v", ErrInvalidInput, customErr)
		}
		if customErr = verifyMcBlockDetails(customRoot, &custom); customErr != nil {
			return customErr
		}
		extra.Custom = &custom
	}
	block.Extra = &extra

	return nil
}

func verifyHeaderReferenceCells(root *cell.Cell, header *tlb.BlockHeader) error {
	prevIndex := 0
	if header.NotMaster {
		masterRoot, err := root.PeekRef(0)
		if err != nil {
			return fmt.Errorf("%w: load exact masterchain reference: %v", ErrInvalidInput, err)
		}
		var master tlb.ExtBlkRef
		if err = parseExact(&master, masterRoot); err != nil || !equalBlockReference(&master, header.MasterRef) {
			return fmt.Errorf("%w: malformed masterchain reference in block header", ErrInvalidInput)
		}
		prevIndex = 1
	}

	previousRoot, err := root.PeekRef(prevIndex)
	if err != nil {
		return fmt.Errorf("%w: load exact predecessor references: %v", ErrInvalidInput, err)
	}
	if !header.AfterMerge {
		var previous tlb.ExtBlkRef
		if err = parseExact(&previous, previousRoot); err != nil ||
			!equalBlockReference(&previous, &header.PrevRef.Prev1) {
			return fmt.Errorf("%w: malformed predecessor reference in block header", ErrInvalidInput)
		}
		return nil
	}

	loader, err := previousRoot.BeginParse()
	if err != nil || loader.BitsLeft() != 0 || loader.RefsNum() != 2 {
		return fmt.Errorf("%w: malformed merge predecessor references", ErrInvalidInput)
	}
	firstRoot, err := loader.LoadRefCell()
	if err != nil {
		return fmt.Errorf("%w: load first merge predecessor reference: %v", ErrInvalidInput, err)
	}
	secondRoot, err := loader.LoadRefCell()
	if err != nil {
		return fmt.Errorf("%w: load second merge predecessor reference: %v", ErrInvalidInput, err)
	}
	var first, second tlb.ExtBlkRef
	if err = parseExact(&first, firstRoot); err != nil {
		return fmt.Errorf("%w: decode first merge predecessor reference: %v", ErrInvalidInput, err)
	}
	if err = parseExact(&second, secondRoot); err != nil || header.PrevRef.Prev2 == nil ||
		!equalBlockReference(&first, &header.PrevRef.Prev1) ||
		!equalBlockReference(&second, header.PrevRef.Prev2) {
		return fmt.Errorf("%w: malformed merge predecessor references", ErrInvalidInput)
	}

	return nil
}

func verifyMcBlockDetails(root *cell.Cell, extra *tlb.McBlockExtra) error {
	loader, err := root.BeginParse()
	if err != nil {
		return fmt.Errorf("%w: decode masterchain block extra: %v", ErrInvalidInput, err)
	}
	if _, err = loader.LoadUInt(16); err != nil {
		return fmt.Errorf("%w: decode masterchain block extra tag: %v", ErrInvalidInput, err)
	}
	if _, err = loader.LoadBoolBit(); err != nil {
		return fmt.Errorf("%w: decode masterchain key block flag: %v", ErrInvalidInput, err)
	}
	if _, err = loader.LoadDict(32); err != nil {
		return fmt.Errorf("%w: decode masterchain shard hashes: %v", ErrInvalidInput, err)
	}
	var fees tlb.ShardFeesAugDict
	if err = fees.LoadFromCell(loader); err != nil {
		return fmt.Errorf("%w: decode exact masterchain shard fees: %v", ErrInvalidInput, err)
	}
	detailsRoot, err := loader.LoadRefCell()
	if err != nil {
		return fmt.Errorf("%w: load masterchain block details: %v", ErrInvalidInput, err)
	}
	var details mcBlockDetails
	if err = parseExact(&details, detailsRoot); err != nil {
		return fmt.Errorf("%w: decode exact masterchain block details: %v", ErrInvalidInput, err)
	}
	if details.PrevBlockSignatures == nil || extra.Details.PrevBlockSignatures == nil ||
		!equalDictionary(details.PrevBlockSignatures, extra.Details.PrevBlockSignatures) ||
		!equalCell(details.RecoverCreateMsg, extra.Details.RecoverCreateMsg) ||
		!equalCell(details.MintMsg, extra.Details.MintMsg) {
		return fmt.Errorf("%w: masterchain block details disagree", ErrInvalidInput)
	}

	return nil
}

func verifyHeaderAndID(header *tlb.BlockHeader, candidate *Candidate) error {
	if header.Version != 0 {
		return fmt.Errorf("%w: unsupported block header version %d", ErrInvalidInput, header.Version)
	}
	if header.Flags&^uint32(1) != 0 {
		return fmt.Errorf("%w: unsupported block header flags %#x", ErrInvalidInput, header.Flags)
	}
	if (header.Flags&1 != 0) != (header.GenSoftware != nil) {
		return fmt.Errorf("%w: block software flag and value disagree", ErrInvalidInput)
	}
	if header.VertSeqnoIncr || header.PrevVertRef != nil {
		return fmt.Errorf("%w: candidate contains a vertical predecessor", ErrInvalidInput)
	}
	if header.PrevRef.Pruned {
		return fmt.Errorf("%w: candidate predecessor references are pruned", ErrInvalidInput)
	}
	if header.StartLt >= header.EndLt {
		return fmt.Errorf("%w: block start lt is not below end lt", ErrInvalidInput)
	}
	if header.AfterMerge && header.AfterSplit {
		return fmt.Errorf("%w: block is both after split and after merge", ErrInvalidInput)
	}
	if header.KeyBlock && header.NotMaster {
		return fmt.Errorf("%w: shardchain block is marked as key block", ErrInvalidInput)
	}
	if candidate.ID.Workchain != header.Shard.WorkchainID ||
		candidate.ID.Shard != int64(header.Shard.GetShardID()) ||
		candidate.ID.SeqNo != header.SeqNo {
		return fmt.Errorf("%w: candidate id differs from block header", ErrInvalidInput)
	}
	expected, err := topologyShardIdent(groups.ShardID{
		Workchain: candidate.ID.Workchain,
		Shard:     candidate.ID.Shard,
	})
	if err != nil || expected != header.Shard {
		return fmt.Errorf("%w: block header contains a non-canonical shard id", ErrInvalidInput)
	}

	return nil
}

func verifyStateHeader(block *tlb.Block, state *tlb.ShardStateUnsplit, candidate *Candidate) error {
	header := &block.BlockInfo
	if state.GlobalID != block.GlobalID {
		return fmt.Errorf("%w: candidate state and block have different global ids", ErrInvalidInput)
	}
	if state.ShardIdent != header.Shard || state.Seqno != header.SeqNo ||
		state.VertSeqno != header.VertSeqNo || state.GenUTime != header.GenUtime ||
		state.GenLT != header.EndLt || state.MinRefMCSeqno != header.MinRefMcSeqno ||
		state.BeforeSplit != header.BeforeSplit {
		return fmt.Errorf("%w: candidate state header differs from block header", ErrInvalidInput)
	}
	if state.Seqno != candidate.ID.SeqNo || state.ShardIdent.WorkchainID != candidate.ID.Workchain ||
		int64(state.ShardIdent.GetShardID()) != candidate.ID.Shard {
		return fmt.Errorf("%w: candidate state differs from candidate id", ErrInvalidInput)
	}

	return nil
}

// predecessorForm says which tree a predecessor state is, and it decides
// exactly one thing: how the state is decoded.
//
// The distinction cannot be dropped. Proof-aware decoding treats a pruned
// reference as a proven absence and skips the field, which is what the collated
// predecessor needs — a boundary parses without failing, its payload starting
// with the 0x01 type byte, so a field read straight through one yields a
// plausible zero value and a reader walking past what the proof carries goes
// unnoticed. But a resident state out of the node's own store is lazy: a
// reference not yet materialized is *also* a pruned-branch stub, and telling
// the decoder to skip those makes it drop live fields. That is not a
// theoretical hazard — decoding a lazy zerostate that way loses ShardAccounts
// entirely, and every validator then refuses to activate its first session and
// the network never produces block 1.
type predecessorForm uint8

const (
	// residentPredecessor is the node's own tree, where an unmaterialized
	// reference is indistinguishable from a pruned one and nothing may be
	// skipped.
	residentPredecessor predecessorForm = iota
	// provenPredecessor is the Merkle proof a candidate carries, where a pruned
	// reference is exactly the absence the proof is asserting.
	provenPredecessor
)

func verifyPredecessor(label string, previous *PreviousBlock) (tlb.ShardStateUnsplit, error) {
	return verifyPredecessorState(residentPredecessor, label, previous)
}

func verifyPredecessorState(
	form predecessorForm,
	label string,
	previous *PreviousBlock,
) (tlb.ShardStateUnsplit, error) {
	if previous.State == nil {
		return tlb.ShardStateUnsplit{}, fmt.Errorf("%w: %s predecessor state is absent", ErrInvalidInput, label)
	}
	if len(previous.ID.RootHash) != 32 || len(previous.ID.FileHash) != 32 {
		return tlb.ShardStateUnsplit{}, fmt.Errorf("%w: %s predecessor id hashes must be 256 bits", ErrInvalidInput, label)
	}

	decode := parseExact
	if form == provenPredecessor {
		decode = parseProofExact
	}
	var state tlb.ShardStateUnsplit
	if err := decode(&state, previous.State); err != nil {
		return tlb.ShardStateUnsplit{}, fmt.Errorf("%w: decode %s predecessor state: %v", ErrInvalidInput, label, err)
	}
	expected, err := topologyShardIdent(groups.ShardID{
		Workchain: previous.ID.Workchain,
		Shard:     previous.ID.Shard,
	})
	if err != nil || expected != state.ShardIdent || previous.ID.SeqNo != state.Seqno {
		return tlb.ShardStateUnsplit{}, fmt.Errorf("%w: %s predecessor id differs from state", ErrInvalidInput, label)
	}
	if err = verifyAccountsShardPrefix(state.ShardIdent, state.Accounts.ShardAccounts); err != nil {
		// The cause is kept: the three ways this fails — an absent dictionary, a
		// walk that could not open a cell, and accounts genuinely outside the
		// shard — call for completely different responses, and a node that
		// refuses to start over this one needs the log to say which it hit.
		return tlb.ShardStateUnsplit{}, fmt.Errorf(
			"%w: %s predecessor accounts do not match shard %d:%016x (prefix %d bits, block %d, state %x): %v",
			ErrInvalidInput,
			label,
			state.ShardIdent.WorkchainID,
			state.ShardIdent.GetShardID(),
			state.ShardIdent.PrefixBits,
			previous.ID.SeqNo,
			previous.State.Hash(),
			err,
		)
	}
	if previous.ID.SeqNo == 0 && !bytes.Equal(previous.ID.RootHash, previous.State.Hash()) {
		return tlb.ShardStateUnsplit{}, fmt.Errorf("%w: %s zerostate hash differs from its id", ErrInvalidInput, label)
	}
	if previous.Block != nil {
		if !bytes.Equal(previous.ID.RootHash, previous.Block.Hash()) {
			return tlb.ShardStateUnsplit{}, fmt.Errorf("%w: %s predecessor block root differs from its id", ErrInvalidInput, label)
		}
		var block tlb.Block
		if err = parseExact(&block, previous.Block); err != nil {
			return tlb.ShardStateUnsplit{}, fmt.Errorf("%w: decode %s predecessor block: %v", ErrInvalidInput, label, err)
		}
		if block.BlockInfo.Shard != state.ShardIdent || block.BlockInfo.SeqNo != state.Seqno {
			return tlb.ShardStateUnsplit{}, fmt.Errorf("%w: %s predecessor block header differs from state", ErrInvalidInput, label)
		}
		if err = cell.ValidateMerkleUpdate(block.StateUpdate); err != nil {
			return tlb.ShardStateUnsplit{}, fmt.Errorf("%w: invalid %s predecessor state update: %v", ErrInvalidInput, label, err)
		}
		to, refErr := block.StateUpdate.PeekRef(1)
		if refErr != nil || to.HashKeyAt(0) != previous.State.HashKeyAt(0) {
			return tlb.ShardStateUnsplit{}, fmt.Errorf("%w: %s predecessor block does not produce supplied state", ErrInvalidInput, label)
		}
	}

	return state, nil
}

func verifyBlockReference(
	reference *tlb.ExtBlkRef,
	previous *PreviousBlock,
	state *tlb.ShardStateUnsplit,
	label string,
) error {
	if reference == nil || reference.SeqNo != previous.ID.SeqNo || reference.EndLt != state.GenLT ||
		!bytes.Equal(reference.RootHash, previous.ID.RootHash) ||
		!bytes.Equal(reference.FileHash, previous.ID.FileHash) {
		return fmt.Errorf("%w: %s predecessor reference differs from supplied block", ErrInvalidInput, label)
	}

	return nil
}

func equalBlockReference(left, right *tlb.ExtBlkRef) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.EndLt == right.EndLt && left.SeqNo == right.SeqNo &&
		bytes.Equal(left.RootHash, right.RootHash) && bytes.Equal(left.FileHash, right.FileHash)
}

func verifyStateTransition(oldRoot, state, update *cell.Cell) error {
	// ValidateMerkleUpdate already proved the update internally consistent in
	// verifyCandidate: every destination pruned boundary resolves inside the
	// source proof, so an update whose source hash equals the predecessor root
	// is applicable by construction and applying it would produce exactly the
	// destination-proof root. Both endpoint hashes are compared here instead of
	// materializing the applied tree only to discard it.
	if err := cell.MayApplyMerkleUpdate(oldRoot.WithoutTrace(), update); err != nil {
		return fmt.Errorf("%w: candidate state update does not apply to predecessor: %v", ErrInvalidInput, err)
	}
	target, err := update.PeekRef(1)
	if err != nil {
		return fmt.Errorf("%w: load candidate state update target: %v", ErrInvalidInput, err)
	}
	if target.HashKeyAt(0) != state.HashKeyAt(0) {
		return fmt.Errorf("%w: applied candidate state differs from supplied state", ErrInvalidInput)
	}

	return nil
}

func verifyStateBalance(candidate *verifiedCandidate) error {
	balance, err := loadAccountsBalance(candidate.state.Accounts.ShardAccounts)
	if err != nil {
		return fmt.Errorf("%w: load candidate account balance: %v", ErrInvalidInput, err)
	}
	if !balance.Equals(candidate.stats.TotalBalance) || !balance.Equals(candidate.flow.ToNextBlock) {
		return fmt.Errorf("%w: account balance, state statistics and value flow disagree", ErrInvalidInput)
	}

	return nil
}

func verifyOutQueueSizePresence(config *Config, queue *tlb.OutMsgQueueInfo) error {
	storesQueueSize := config.capabilities&capStoreOutMsgQueueSize != 0
	if storesQueueSize != (queue.Extra != nil && queue.Extra.OutQueueSize != nil) {
		return fmt.Errorf("%w: candidate outbound queue size presence differs from capabilities", ErrInvalidInput)
	}

	return nil
}

func verifyShardMinRefMCSeqno(header *tlb.BlockHeader, queue *tlb.OutMsgQueueInfo) error {
	owner := msgpool.ShardIdent{
		Workchain: header.Shard.WorkchainID,
		Shard:     uint64(header.Shard.GetShardID()),
	}
	records, err := tlb.LoadProcessedUptoRecords(queue.ProcInfo, owner.Shard)
	if err != nil {
		return fmt.Errorf("%w: decode candidate processed info: %v", ErrInvalidInput, err)
	}
	expected := min(header.MasterRef.SeqNo, minRecordedMCSeqno(records))
	if header.MinRefMcSeqno != expected {
		return fmt.Errorf(
			"%w: minimum referenced masterchain seqno is %d, expected %d",
			ErrInvalidInput,
			header.MinRefMcSeqno,
			expected,
		)
	}

	return nil
}

func verifyBlockLTDelta(config *Config, header *tlb.BlockHeader, master bool) error {
	limits := config.basechain.limits
	if master {
		limits = config.masterchain.limits
	}
	if header.EndLt-header.StartLt > limits.ltDelta[3] {
		return fmt.Errorf(
			"%w: block logical-time delta is %d, hard limit is %d",
			ErrInvalidInput,
			header.EndLt-header.StartLt,
			limits.ltDelta[3],
		)
	}

	return nil
}

func verifyLoadHistory(
	previous *tlb.ShardStateStats,
	next *tlb.ShardStateStats,
	header *tlb.BlockHeader,
	topologyChanged bool,
) error {
	if next.OverloadHistory&next.UnderloadHistory&1 != 0 {
		return fmt.Errorf("%w: block is both overloaded and underloaded", ErrInvalidInput)
	}
	if topologyChanged {
		if (next.OverloadHistory|next.UnderloadHistory)&^uint64(1) != 0 {
			return fmt.Errorf("%w: load history was not cleared after shard topology change", ErrInvalidInput)
		}
	} else if (next.OverloadHistory^(previous.OverloadHistory<<1))&^uint64(1) != 0 ||
		(next.UnderloadHistory^(previous.UnderloadHistory<<1))&^uint64(1) != 0 {
		return fmt.Errorf("%w: resulting load history does not follow its predecessor", ErrInvalidInput)
	}

	wantSplit := historyWeight(next.OverloadHistory) >= 0
	wantMerge := !wantSplit && historyWeight(next.UnderloadHistory) >= 0
	if header.WantSplit != wantSplit || header.WantMerge != wantMerge {
		return fmt.Errorf("%w: block split/merge wishes disagree with load history", ErrInvalidInput)
	}

	return nil
}

func verifyMasterValueFlow(
	config *Config,
	candidate *verifiedCandidate,
	state *masterCandidateState,
) error {
	expected := masterCollation{
		oldExtra:  state.previousExtra,
		shardFees: candidate.block.Extra.Custom.ShardFees,
	}
	if err := expected.initValueFlow(config, &state.previousStats); err != nil {
		return fmt.Errorf("%w: derive expected masterchain value flow: %v", ErrInvalidInput, err)
	}
	flow := &candidate.flow
	if !flow.FromPrevBlock.Equals(state.previousStats.TotalBalance) ||
		!flow.FeesImported.Equals(expected.feesImported) ||
		!flow.Created.Equals(expected.created) ||
		!flow.Recovered.Equals(expected.recovered) ||
		!flow.Minted.Equals(expected.minted) {
		return fmt.Errorf("%w: masterchain value flow static components disagree with predecessor and config", ErrInvalidInput)
	}
	augmentations, err := loadCandidateValueFlowAugmentations(candidate)
	if err != nil {
		return err
	}
	state.minimumBurned, err = verifyMasterAugmentedValueFlow(config, &expected, augmentations, flow)
	if err != nil {
		return err
	}
	blockExtra := candidate.block.Extra.Custom
	if (blockExtra.Details.RecoverCreateMsg == nil) != currencyZero(flow.Recovered) ||
		(blockExtra.Details.MintMsg == nil) != currencyZero(flow.Minted) {
		return fmt.Errorf("%w: masterchain special messages disagree with value flow", ErrInvalidInput)
	}

	globalBalance, err := state.previousExtra.GlobalBalance.Add(flow.Created)
	if err != nil {
		return fmt.Errorf("%w: add expected created balance: %v", ErrInvalidInput, err)
	}
	globalBalance, err = globalBalance.Add(flow.Minted)
	if err != nil {
		return fmt.Errorf("%w: add expected minted balance: %v", ErrInvalidInput, err)
	}
	globalBalance, err = globalBalance.Add(expected.importedCreated)
	if err != nil {
		return fmt.Errorf("%w: add expected imported creation balance: %v", ErrInvalidInput, err)
	}
	if !state.nextExtra.GlobalBalance.Equals(globalBalance) {
		return fmt.Errorf("%w: resulting global balance disagrees with value flow", ErrInvalidInput)
	}
	validatorFees, err := state.previousStats.TotalValidatorFees.Add(flow.FeesCollected)
	if err != nil {
		return fmt.Errorf("%w: add expected validator fees: %v", ErrInvalidInput, err)
	}
	validatorFees, err = validatorFees.Sub(flow.Recovered)
	if err != nil || !candidate.stats.TotalValidatorFees.Equals(validatorFees) {
		return fmt.Errorf("%w: resulting validator fees disagree with value flow", ErrInvalidInput)
	}

	if candidate.block.BlockInfo.KeyBlock != state.config.keyBlock {
		return fmt.Errorf("%w: masterchain key block flag disagrees with the config transition", ErrInvalidInput)
	}

	return nil
}

func verifyMasterAugmentedValueFlow(
	config *Config,
	expected *masterCollation,
	augmentations candidateValueFlowAugmentations,
	flow *tlb.ValueFlow,
) (tlb.CurrencyCollection, error) {
	if !flow.Imported.Equals(augmentations.importFees.ValueImported) {
		return tlb.CurrencyCollection{}, fmt.Errorf("%w: masterchain imported value differs from inbound descriptors", ErrInvalidInput)
	}
	if !flow.Exported.Equals(augmentations.exported) {
		return tlb.CurrencyCollection{}, fmt.Errorf("%w: masterchain exported value differs from outbound descriptors", ErrInvalidInput)
	}

	totals, err := expected.feeTotals(config, augmentations.transactionFees, augmentations.importFees)
	if err != nil {
		return tlb.CurrencyCollection{}, fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}
	feesCollected := totals.feesCollected
	if !flow.FeesCollected.Equals(feesCollected) {
		return tlb.CurrencyCollection{}, fmt.Errorf("%w: masterchain collected fees differ from descriptor augmentations", ErrInvalidInput)
	}

	minimumBurned, err := expected.importBurned.Add(totals.ordinaryBurned)
	if err != nil {
		return tlb.CurrencyCollection{}, fmt.Errorf("%w: add deterministically burned masterchain fees: %v", ErrInvalidInput, err)
	}
	// Transaction replay accounts for the remaining blackhole burn. Statically,
	// the declared amount must at least contain all protocol fee burning.
	if _, err = flow.Burned.Sub(minimumBurned); err != nil {
		return tlb.CurrencyCollection{}, fmt.Errorf("%w: masterchain burned value omits deterministic fee burning", ErrInvalidInput)
	}

	return minimumBurned, nil
}

func verifyMasterExtras(candidate *verifiedCandidate, state *masterCandidateState) error {
	blockExtra := candidate.block.Extra.Custom
	if blockExtra.KeyBlock != candidate.block.BlockInfo.KeyBlock {
		return fmt.Errorf("%w: masterchain key block flags disagree", ErrInvalidInput)
	}
	if blockExtra.ShardHashes == nil || blockExtra.ShardHashes.GetKeySize() != 32 ||
		blockExtra.ShardFees == nil || blockExtra.ShardFees.GetKeySize() != 96 ||
		blockExtra.Details.PrevBlockSignatures == nil || blockExtra.Details.PrevBlockSignatures.GetKeySize() != 16 {
		return fmt.Errorf("%w: malformed masterchain block dictionaries", ErrInvalidInput)
	}
	if !blockExtra.ShardHashes.ValidateAll() || !blockExtra.ShardFees.ValidateAll() ||
		!blockExtra.Details.PrevBlockSignatures.ValidateAll() {
		return fmt.Errorf("%w: invalid masterchain block dictionary", ErrInvalidInput)
	}
	if !blockExtra.Details.PrevBlockSignatures.IsEmpty() {
		return fmt.Errorf("%w: previous masterchain block signatures are unsupported", ErrUnsupported)
	}

	stateExtra := &state.nextExtra
	if stateExtra.ShardHashes == nil || stateExtra.ShardHashes.GetKeySize() != 32 ||
		stateExtra.ConfigParams.Config.Params == nil ||
		stateExtra.ConfigParams.Config.Params.GetKeySize() != 32 ||
		len(stateExtra.ConfigParams.ConfigAddr) != 32 {
		return fmt.Errorf("%w: malformed masterchain state dictionaries", ErrInvalidInput)
	}
	if !stateExtra.ShardHashes.ValidateAll() || !stateExtra.ConfigParams.Config.Params.ValidateAll() {
		return fmt.Errorf("%w: invalid masterchain state dictionary", ErrInvalidInput)
	}
	stateInfo := &state.nextInfo
	// PrevBlocks holds one entry per masterchain block ever produced and is
	// never pruned, so a structural walk is unaffordable. The transition check
	// rebuilds it from the predecessor plus the single permitted new entry and
	// compares the root cell, which also proves the augmentation of the
	// inserted path.
	if stateInfo.PrevBlocks == nil || stateInfo.PrevBlocks.GetKeySize() != 32 {
		return fmt.Errorf("%w: invalid previous masterchain blocks dictionary", ErrInvalidInput)
	}
	if !equalDictionary(blockExtra.ShardHashes, stateExtra.ShardHashes) {
		return fmt.Errorf("%w: masterchain block and state shard hashes differ", ErrInvalidInput)
	}
	if blockExtra.KeyBlock {
		if blockExtra.ConfigParams == nil ||
			blockExtra.ConfigParams.Config.Params == nil ||
			!blockExtra.ConfigParams.Config.Params.ValidateAll() ||
			!bytes.Equal(blockExtra.ConfigParams.ConfigAddr, stateExtra.ConfigParams.ConfigAddr) ||
			!equalDictionary(blockExtra.ConfigParams.Config.Params, stateExtra.ConfigParams.Config.Params) {
			return fmt.Errorf("%w: key block configuration differs from state", ErrInvalidInput)
		}
	} else if blockExtra.ConfigParams != nil {
		return fmt.Errorf("%w: non-key masterchain block contains configuration", ErrInvalidInput)
	}

	return nil
}

func equalDictionary(left, right *cell.Dictionary) bool {
	if left == nil || right == nil || left.GetKeySize() != right.GetKeySize() {
		return left == right
	}
	leftRoot := left.AsCell()
	rightRoot := right.AsCell()
	if leftRoot == nil || rightRoot == nil {
		return leftRoot == rightRoot
	}
	return leftRoot.HashKeyAt(0) == rightRoot.HashKeyAt(0)
}

func equalCell(left, right *cell.Cell) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.HashKeyAt(0) == right.HashKeyAt(0)
}

func verifyCollatedData(candidate *Candidate, genUtime uint32) (verifiedCollatedData, error) {
	hash := sha256.Sum256(candidate.CollatedData)
	if hash != candidate.CollatedFileHash {
		return verifiedCollatedData{}, fmt.Errorf("%w: candidate collated data file hash mismatch", ErrInvalidInput)
	}
	roots, err := cell.FromBOCMultiRoot(candidate.CollatedData)
	if err != nil {
		return verifiedCollatedData{}, fmt.Errorf("%w: decode candidate collated data: %v", ErrInvalidInput, err)
	}
	if len(roots) == 0 {
		return verifiedCollatedData{}, fmt.Errorf("%w: candidate collated data has no roots", ErrInvalidInput)
	}

	return verifyCollatedRoots(roots, genUtime)
}

func isTopBlockDescrSet(root *cell.Cell) bool {
	loader, err := root.BeginParse()
	if err != nil {
		return false
	}
	tag, err := loader.LoadUInt(32)
	return err == nil && tag == masterTopBlockDescrSetTag
}

func verifyTopBlockDescrSet(root *cell.Cell) error {
	loader, err := root.BeginParse()
	if err != nil {
		return err
	}
	tag, err := loader.LoadUInt(32)
	if err != nil || tag != masterTopBlockDescrSetTag {
		return fmt.Errorf("invalid tag")
	}
	dict, err := loader.LoadDict(96)
	if err != nil {
		return err
	}
	if loader.BitsLeft() != 0 || loader.RefsNum() != 0 {
		return fmt.Errorf("trailing data")
	}
	budget := tlbValidationBudget{remaining: topBlockDescrValidationBudget}
	if dict.IsEmpty() {
		if err = budget.consume(1); err != nil { // TopBlockDescrSet root.
			return err
		}
	}
	if _, err = dict.ForEachRefValue(func(descriptor *cell.Cell) error {
		// For n leaves, validation spends 2n-1 operations on Hashmap
		// cells, n on ^TopBlockDescr values and one on the outer root: 3n.
		if budgetErr := budget.consume(3); budgetErr != nil {
			return budgetErr
		}
		return validateTopBlockDescr(descriptor, &budget)
	}); err != nil {
		return fmt.Errorf("invalid descriptor reference: %w", err)
	}

	return nil
}

const topBlockDescrValidationBudget = 10_000

type tlbValidationBudget struct {
	remaining int
}

func (b *tlbValidationBudget) consume(operations int) error {
	if operations > b.remaining {
		return fmt.Errorf("TL-B validation exceeds %d operations", topBlockDescrValidationBudget)
	}
	b.remaining -= operations
	return nil
}

// validateTopBlockDescr checks only the recursive TL-B shape. Proof contents
// and cryptographic validity are verified by the shard-top acquisition flow.
func validateTopBlockDescr(root *cell.Cell, budget *tlbValidationBudget) error {
	if root.IsSpecial() {
		return fmt.Errorf("special TopBlockDescr cell")
	}
	loader, err := root.BeginParse()
	if err != nil {
		return err
	}
	tag, err := loader.LoadUInt(8)
	if err != nil || tag != masterTopBlockDescrTag {
		return fmt.Errorf("invalid TopBlockDescr tag")
	}
	shardTag, err := loader.LoadUInt(2)
	if err != nil || shardTag != 0 {
		return fmt.Errorf("invalid TopBlockDescr shard tag")
	}
	prefixBits, err := loader.LoadUInt(6)
	if err != nil {
		return fmt.Errorf("load TopBlockDescr shard prefix length: %w", err)
	}
	if prefixBits > 60 {
		return fmt.Errorf("invalid TopBlockDescr shard prefix length %d", prefixBits)
	}
	if err = loader.SkipBits(32 + 64 + 32 + 256 + 256); err != nil {
		return fmt.Errorf("load TopBlockDescr target: %w", err)
	}
	hasSignatures, err := loader.LoadBoolBit()
	if err != nil {
		return fmt.Errorf("load TopBlockDescr signature flag: %w", err)
	}
	if hasSignatures {
		if err = budget.consume(1); err != nil {
			return err
		}
		signatures, refErr := loader.LoadRefCell()
		if refErr != nil {
			return fmt.Errorf("load TopBlockDescr signatures: %w", refErr)
		}
		if err = validateTopBlockSignatures(signatures, budget); err != nil {
			return fmt.Errorf("invalid TopBlockDescr signatures: %w", err)
		}
	}
	length, err := loader.LoadUInt(8)
	if err != nil {
		return fmt.Errorf("load TopBlockDescr chain length: %w", err)
	}
	if length == 0 || length > uint64(maxShardTopChainLength) {
		return fmt.Errorf("invalid TopBlockDescr chain length %d", length)
	}
	if err = validateTopBlockProofChain(loader, int(length), budget); err != nil {
		return err
	}

	return nil
}

func validateTopBlockProofChain(chain *cell.Slice, length int, budget *tlbValidationBudget) error {
	for i := 0; i < length; i++ {
		wantRefs := 1
		if i+1 < length {
			wantRefs = 2
		}
		if chain.BitsLeft() != 0 || chain.RefsNum() != wantRefs {
			return fmt.Errorf("TopBlockDescr proof link %d has %d bits and %d refs, want 0 and %d",
				i, chain.BitsLeft(), chain.RefsNum(), wantRefs)
		}
		// ProofChain.root is ^Cell, so its contents have no additional TL-B shape.
		if _, err := chain.LoadRefCell(); err != nil {
			return fmt.Errorf("load TopBlockDescr proof link %d: %w", i, err)
		}
		if i+1 < length {
			if err := budget.consume(1); err != nil {
				return err
			}
			var err error
			chain, err = chain.LoadRef()
			if err != nil {
				return fmt.Errorf("load TopBlockDescr previous link %d: %w", i, err)
			}
		}
	}

	return nil
}

func validateTopBlockSignatures(root *cell.Cell, budget *tlbValidationBudget) error {
	if root.IsSpecial() {
		return fmt.Errorf("special BlockSignatures cell")
	}
	loader, err := root.BeginParse()
	if err != nil {
		return err
	}
	tag, err := loader.LoadUInt(8)
	if err != nil || (tag != 0x11 && tag != 0x12) {
		return fmt.Errorf("invalid BlockSignatures tag")
	}
	if err = loader.SkipBits(32 + 32 + 32 + 64); err != nil {
		return fmt.Errorf("load BlockSignatures header: %w", err)
	}
	signatures, err := loader.LoadDict(16)
	if err != nil {
		return fmt.Errorf("load BlockSignatures dictionary: %w", err)
	}
	iterator, err := signatures.Iterator(false, false)
	if err != nil {
		return fmt.Errorf("iterate BlockSignatures dictionary: %w", err)
	}
	first := true
	for iterator.Next() {
		dictionaryOperations := 2
		if first {
			dictionaryOperations = 1
			first = false
		}
		if err = budget.consume(dictionaryOperations); err != nil {
			return err
		}
		value := iterator.View().Value
		if err = validateCryptoSignaturePair(&value, budget); err != nil {
			return fmt.Errorf("invalid BlockSignatures entry: %w", err)
		}
	}
	if err = iterator.Err(); err != nil {
		return fmt.Errorf("invalid BlockSignatures dictionary: %w", err)
	}
	if tag == 0x12 {
		if err = loader.SkipBits(256 + 32); err != nil {
			return fmt.Errorf("load simplex BlockSignatures fields: %w", err)
		}
		if _, err = loader.LoadRefCell(); err != nil {
			return fmt.Errorf("load simplex BlockSignatures candidate data: %w", err)
		}
	}
	if loader.BitsLeft() != 0 || loader.RefsNum() != 0 {
		return fmt.Errorf("trailing BlockSignatures data")
	}

	return nil
}

func validateCryptoSignaturePair(pair *cell.Slice, budget *tlbValidationBudget) error {
	if err := pair.SkipBits(256); err != nil {
		return fmt.Errorf("load signer id: %w", err)
	}
	if err := validateCryptoSignature(pair, budget); err != nil {
		return err
	}
	if pair.BitsLeft() != 0 || pair.RefsNum() != 0 {
		return fmt.Errorf("trailing signature pair data")
	}

	return nil
}

func validateCryptoSignature(signature *cell.Slice, budget *tlbValidationBudget) error {
	tag, err := signature.LoadUInt(4)
	if err != nil {
		return fmt.Errorf("load signature tag: %w", err)
	}
	switch tag {
	case 0x5:
		if err = signature.SkipBits(256 + 256); err != nil {
			return fmt.Errorf("load ed25519 signature: %w", err)
		}
	case 0xf:
		if err = budget.consume(1); err != nil {
			return err
		}
		certificate, refErr := signature.LoadRefCell()
		if refErr != nil {
			return fmt.Errorf("load chained signature certificate: %w", refErr)
		}
		if err = validateSignedCertificate(certificate, budget); err != nil {
			return fmt.Errorf("invalid chained signature certificate: %w", err)
		}
		if err = validateSimpleCryptoSignature(signature); err != nil {
			return fmt.Errorf("invalid temporary key signature: %w", err)
		}
	default:
		return fmt.Errorf("invalid signature tag %x", tag)
	}

	return nil
}

func validateSimpleCryptoSignature(signature *cell.Slice) error {
	tag, err := signature.LoadUInt(4)
	if err != nil || tag != 0x5 {
		return fmt.Errorf("invalid ed25519 signature tag")
	}
	if err = signature.SkipBits(256 + 256); err != nil {
		return fmt.Errorf("load ed25519 signature: %w", err)
	}

	return nil
}

func validateSignedCertificate(root *cell.Cell, budget *tlbValidationBudget) error {
	if root.IsSpecial() {
		return fmt.Errorf("special SignedCertificate cell")
	}
	loader, err := root.BeginParse()
	if err != nil {
		return err
	}
	certificateTag, err := loader.LoadUInt(4)
	if err != nil || certificateTag != 0x4 {
		return fmt.Errorf("invalid certificate tag")
	}
	publicKeyTag, err := loader.LoadUInt(32)
	if err != nil || publicKeyTag != 0x8e81278a {
		return fmt.Errorf("invalid certificate public key tag")
	}
	if err = loader.SkipBits(256 + 32 + 32); err != nil {
		return fmt.Errorf("load certificate: %w", err)
	}
	if err = validateCryptoSignature(loader, budget); err != nil {
		return fmt.Errorf("invalid certificate signature: %w", err)
	}
	if loader.BitsLeft() != 0 || loader.RefsNum() != 0 {
		return fmt.Errorf("trailing SignedCertificate data")
	}

	return nil
}

type blockDictionaries struct {
	inMessages    *tlb.InMsgDescrAugDict
	outMessages   *tlb.OutMsgDescrAugDict
	accountBlocks *tlb.ShardAccountBlocksAugDict
}

func verifyBlockDictionaries(extra *tlb.BlockExtra, globalVersion uint32) (blockDictionaries, error) {
	inMessages, err := verifyInMsgDescriptors(extra.InMsgDesc, globalVersion)
	if err != nil {
		return blockDictionaries{}, fmt.Errorf("%w: invalid inbound message descriptors: %v", ErrInvalidInput, err)
	}
	outMessages, err := verifyOutMsgDescriptors(extra.OutMsgDesc, globalVersion)
	if err != nil {
		return blockDictionaries{}, fmt.Errorf("%w: invalid outbound message descriptors: %v", ErrInvalidInput, err)
	}
	accountBlocks, err := verifyAccountBlocks(extra.ShardAccountBlocks)
	if err != nil {
		return blockDictionaries{}, fmt.Errorf("%w: invalid account blocks: %v", ErrInvalidInput, err)
	}

	return blockDictionaries{
		inMessages:    inMessages,
		outMessages:   outMessages,
		accountBlocks: accountBlocks,
	}, nil
}

// verifyInMsgDescriptors loads and augmentation-checks the dictionary, and
// deliberately does not decode its entries: the mandatory semantic replay walks
// the same dictionary with parseSemanticInDescriptor, whose per-entry checks are
// a strict superset of anything a structural pass could make. Two hand-written
// decoders of one TL-B constructor have to stay in lockstep forever, and this
// one had already drifted from block.tlb (msg_export_tr_req$111).
func verifyInMsgDescriptors(root *cell.Cell, globalVersion uint32) (*tlb.InMsgDescrAugDict, error) {
	return loadInMsgDescriptors(root, globalVersion)
}

func loadInMsgDescriptors(root *cell.Cell, globalVersion uint32) (*tlb.InMsgDescrAugDict, error) {
	if root == nil {
		return nil, fmt.Errorf("dictionary cell is absent")
	}
	loader, err := root.BeginParse()
	if err != nil {
		return nil, err
	}
	dict, err := tlb.LoadInMsgDescrAugDict(loader, globalVersion)
	if err != nil {
		return nil, err
	}
	if loader.BitsLeft() != 0 || loader.RefsNum() != 0 {
		return nil, fmt.Errorf("dictionary has trailing data")
	}
	if !dict.ValidateAll() {
		return nil, fmt.Errorf("dictionary augmentation is invalid")
	}

	return dict, nil
}

// verifyOutMsgDescriptors mirrors verifyInMsgDescriptors: parseSemanticOutDescriptor
// re-parses every entry during the replay.
func verifyOutMsgDescriptors(root *cell.Cell, globalVersion uint32) (*tlb.OutMsgDescrAugDict, error) {
	return loadOutMsgDescriptors(root, globalVersion)
}

func loadOutMsgDescriptors(root *cell.Cell, globalVersion uint32) (*tlb.OutMsgDescrAugDict, error) {
	if root == nil {
		return nil, fmt.Errorf("dictionary cell is absent")
	}
	loader, err := root.BeginParse()
	if err != nil {
		return nil, err
	}
	dict, err := tlb.LoadOutMsgDescrAugDict(loader, globalVersion)
	if err != nil {
		return nil, err
	}
	if loader.BitsLeft() != 0 || loader.RefsNum() != 0 {
		return nil, fmt.Errorf("dictionary has trailing data")
	}
	if !dict.ValidateAll() {
		return nil, fmt.Errorf("dictionary augmentation is invalid")
	}

	return dict, nil
}

func verifyAccountBlocks(root *cell.Cell) (*tlb.ShardAccountBlocksAugDict, error) {
	var blocks tlb.ShardAccountBlocks
	if err := parseExact(&blocks, root); err != nil {
		return nil, err
	}
	if blocks.Accounts == nil {
		return nil, fmt.Errorf("account block dictionary is absent")
	}
	if !blocks.Accounts.ValidateAll() {
		return nil, fmt.Errorf("account block dictionary augmentation is invalid")
	}
	// Only the transaction augmentation is checked here. decodeAccountLanes
	// re-decodes every account block, matches Addr against the dictionary key
	// and rejects trailing data, and replayAccount parses the state update as an
	// exact tlb.HashUpdate; the transaction augmentation is the one thing the
	// replay never recomputes, and the value-flow pass depends on it.
	iterator, err := blocks.Accounts.IteratorExtra(false, false)
	if err != nil {
		return nil, err
	}
	for iterator.Next() {
		value := iterator.View().Value
		var accountBlock tlb.AccountBlock
		if err = tlb.LoadFromCell(&accountBlock, &value); err != nil {
			return nil, err
		}
		if err = verifyAccountTransactions(&accountBlock); err != nil {
			return nil, err
		}
	}
	if err = iterator.Err(); err != nil {
		return nil, err
	}

	return blocks.Accounts, nil
}

func verifyAccountTransactions(accountBlock *tlb.AccountBlock) error {
	if accountBlock.Transactions == nil {
		return fmt.Errorf("account transaction dictionary is absent")
	}
	if !accountBlock.Transactions.ValidateAll() {
		return fmt.Errorf("account transaction dictionary augmentation is invalid")
	}

	return nil
}

func requireEmptyDescriptor(loader *cell.Slice) error {
	if loader.BitsLeft() != 0 || loader.RefsNum() != 0 {
		return fmt.Errorf("message descriptor has trailing data")
	}
	return nil
}
