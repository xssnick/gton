package collator

import (
	"bytes"
	"fmt"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/msgpool"
)

type masterCandidateState struct {
	previousStats tlb.ShardStateStats
	previousExtra tlb.McStateExtra
	previousInfo  tlb.McStateExtraBlockInfo
	nextExtra     tlb.McStateExtra
	nextInfo      tlb.McStateExtraBlockInfo
	// The creator statistics decoded together with previousInfo/nextInfo, so
	// verifyBlockCreateStatsUpdate does not walk either dictionary again.
	previousCreators blockCreateStats
	nextCreators     blockCreateStats
	config           masterConfigTransition
	minimumBurned    tlb.CurrencyCollection
}

func loadMasterCandidateState(
	config *Config,
	previous *tlb.ShardStateUnsplit,
	candidate *verifiedCandidate,
	snapshot *groups.Snapshot,
) (masterCandidateState, error) {
	var state masterCandidateState
	if err := parseExact(&state.previousStats, previous.Stats); err != nil {
		return masterCandidateState{}, fmt.Errorf(
			"%w: decode previous masterchain statistics: %v", ErrInvalidInput, err,
		)
	}
	if err := parseExact(&state.previousExtra, previous.McStateExtra); err != nil {
		return masterCandidateState{}, fmt.Errorf(
			"%w: decode previous masterchain extra: %v", ErrInvalidInput, err,
		)
	}
	if state.previousExtra.ConfigParams.Config.Params == nil ||
		state.previousExtra.ConfigParams.Config.Params.AsCell().HashKey() != config.execution.Root().HashKey() {
		return masterCandidateState{}, fmt.Errorf(
			"%w: verification config differs from predecessor state", ErrInvalidInput,
		)
	}
	previousInfo, previousCreators, err := parseMasterStateInfoWithStats(state.previousExtra.Info)
	if err != nil {
		return masterCandidateState{}, fmt.Errorf(
			"%w: decode previous masterchain block info: %v", ErrInvalidInput, err,
		)
	}
	state.previousInfo = previousInfo
	state.previousCreators = previousCreators

	if err = parseExact(&state.nextExtra, candidate.state.McStateExtra); err != nil {
		return masterCandidateState{}, fmt.Errorf(
			"%w: decode resulting masterchain extra: %v", ErrInvalidInput, err,
		)
	}
	if state.nextExtra.ConfigParams.Config.Params == nil {
		return masterCandidateState{}, fmt.Errorf("%w: resulting masterchain config is absent", ErrInvalidInput)
	}
	nextInfo, nextCreators, err := parseMasterStateInfoWithStats(state.nextExtra.Info)
	if err != nil {
		return masterCandidateState{}, fmt.Errorf(
			"%w: decode resulting masterchain block info: %v", ErrInvalidInput, err,
		)
	}
	state.nextInfo = nextInfo
	state.nextCreators = nextCreators

	state.config, err = deriveMasterConfigTransition(
		candidate.state.Accounts.ShardAccounts,
		&state.previousExtra,
		masterConfigPredecessor{config: config, groups: predecessorGroupConfig(snapshot, config)},
	)
	if err != nil {
		return masterCandidateState{}, fmt.Errorf("%w: derive resulting masterchain config: %v", ErrInvalidInput, err)
	}
	if !bytes.Equal(state.nextExtra.ConfigParams.ConfigAddr, state.config.params.ConfigAddr) ||
		!equalDictionary(state.nextExtra.ConfigParams.Config.Params, state.config.params.Config.Params) {
		return masterCandidateState{}, fmt.Errorf(
			"%w: resulting config differs from the configuration contract", ErrInvalidInput,
		)
	}

	return state, nil
}

// verifyMasterDeterministicTransition returns the registry it parsed out of the
// candidate block's ShardHashes, so the full-collated neighbour check does not
// parse the same dictionary a second time.
func verifyMasterDeterministicTransition(
	req MasterVerificationRequest,
	previous *tlb.ShardStateUnsplit,
	candidate *verifiedCandidate,
	state *masterCandidateState,
) (*ShardRegistry, error) {
	if err := verifyMasterHeaderAndGroups(req, previous, candidate, state); err != nil {
		return nil, err
	}
	registry, tops, err := verifyMasterShardTransition(req, previous, candidate, state)
	if err != nil {
		return nil, err
	}
	if err = verifyMasterStateInfoTransition(req, previous, candidate, state, tops); err != nil {
		return nil, err
	}
	if err = verifyMasterMinRefMCSeqno(candidate, registry); err != nil {
		return nil, err
	}

	return registry, nil
}

func verifyMasterHeaderAndGroups(
	req MasterVerificationRequest,
	previous *tlb.ShardStateUnsplit,
	candidate *verifiedCandidate,
	state *masterCandidateState,
) error {
	if req.Groups == nil {
		return fmt.Errorf("%w: validator group snapshot is absent", ErrInvalidInput)
	}
	configRoot := state.previousExtra.ConfigParams.Config.Params.AsCell()
	if err := validateMasterSnapshot(req.Previous, req.Groups, configRoot, previous.GenUTime); err != nil {
		return err
	}
	active, err := activeMasterSession(req.Groups)
	if err != nil {
		return err
	}
	header := &candidate.block.BlockInfo
	if header.VertSeqNo != previous.VertSeqno {
		return fmt.Errorf("%w: masterchain vertical sequence number changed", ErrInvalidInput)
	}
	if err = requireGenUtimeMonotonic(
		req.Config.globalVersion,
		header.GenUtime,
		previous.GenUTime,
		"the masterchain predecessor",
	); err != nil {
		return err
	}
	if header.GenCatchainSeqno != active.CatchainSeqno ||
		header.GenValidatorListHashShort != active.ValidatorSetHash {
		return fmt.Errorf("%w: masterchain header differs from the active validator session", ErrInvalidInput)
	}

	previousKeySeqno := uint32(0)
	if state.previousInfo.AfterKeyBlock {
		previousKeySeqno = req.Previous.ID.SeqNo
	} else if state.previousInfo.LastKeyBlock != nil {
		last := state.previousInfo.LastKeyBlock
		if last.SeqNo > req.Previous.ID.SeqNo || len(last.RootHash) != 32 || len(last.FileHash) != 32 {
			return fmt.Errorf("%w: previous key block reference is malformed", ErrInvalidInput)
		}
		previousKeySeqno = last.SeqNo
	}
	if req.Groups.LastKeyBlockSeqno != previousKeySeqno || header.PrevKeyBlockSeqno != previousKeySeqno {
		return fmt.Errorf("%w: masterchain previous key block bindings disagree", ErrInvalidInput)
	}

	return nil
}

func verifyMasterShardTransition(
	req MasterVerificationRequest,
	previous *tlb.ShardStateUnsplit,
	candidate *verifiedCandidate,
	state *masterCandidateState,
) (*ShardRegistry, []ShardTop, error) {
	header := &candidate.block.BlockInfo
	tops, maxEndLT, err := prepareMasterShardTopsForVerification(MasterRequest{
		Config:    req.Config,
		Header:    HeaderParams{GenUtime: header.GenUtime},
		ShardTops: req.ShardTops,
	}, header.SeqNo)
	if err != nil {
		return nil, nil, err
	}
	if err = verifyMasterCollatedShardTops(candidate.collated.roots, tops); err != nil {
		return nil, nil, err
	}
	startBase := max(previous.GenLT, maxEndLT)
	if header.StartLt <= startBase || header.StartLt-startBase > masterMaxLTGap ||
		header.StartLt-previous.GenLT > masterMaxLTGrowth {
		return nil, nil, fmt.Errorf(
			"%w: masterchain start lt %d is outside the permitted range after %d",
			ErrInvalidInput,
			header.StartLt,
			startBase,
		)
	}

	oldRegistry, err := ParseShardRegistry(state.previousExtra.ShardHashes)
	if err != nil {
		return nil, nil, err
	}
	blockExtra := candidate.block.Extra.Custom
	newRegistry, err := ParseShardRegistry(blockExtra.ShardHashes)
	if err != nil {
		return nil, nil, err
	}
	updateCatchain := state.config.keyBlock || crossedLifetime(
		previous.GenUTime,
		header.GenUtime,
		state.config.groups.Catchain.ShardLifetime,
	)
	if err = validateMasterShardLayout(masterShardLayoutValidationInput{
		oldRegistry:    oldRegistry,
		newRegistry:    newRegistry,
		workchains:     state.config.config.workchains,
		tops:           tops,
		now:            header.GenUtime,
		startLT:        header.StartLt,
		newBlockSeqno:  header.SeqNo,
		updateCatchain: updateCatchain,
	}); err != nil {
		return nil, nil, err
	}
	if _, err = validateMasterShardFees(masterShardFeesValidationInput{
		fees:          blockExtra.ShardFees,
		newRegistry:   newRegistry,
		tops:          tops,
		newBlockSeqno: header.SeqNo,
	}); err != nil {
		return nil, nil, err
	}

	return newRegistry, tops, nil
}

func verifyMasterCollatedShardTops(roots []*cell.Cell, tops []ShardTop) error {
	actual, err := loadMasterTopBlockDescrSet(roots)
	if err != nil {
		return err
	}
	if len(tops) != 0 && actual == nil {
		return fmt.Errorf("%w: candidate has no shard top descriptor set", ErrInvalidInput)
	}
	if actual == nil {
		return nil
	}
	// Only descriptors consumed by the shard transition are looked up. Other
	// well-formed entries are allowed to remain in the collated set.
	for i := range tops {
		top := &tops[i]
		descriptor, err := loadValidationTopBlockDescr(actual, top.Block)
		if err != nil {
			return fmt.Errorf("collated shard top descriptor %d: %w", i, err)
		}
		if descriptor.HashKey() != top.TopBlockDescr.HashKey() {
			return fmt.Errorf("%w: collated shard top descriptor %d differs from verified input", ErrInvalidInput, i)
		}
	}

	return nil
}

func verifyMasterStateInfoTransition(
	req MasterVerificationRequest,
	previous *tlb.ShardStateUnsplit,
	candidate *verifiedCandidate,
	state *masterCandidateState,
	tops []ShardTop,
) error {
	// The expectation is derived by the very code collation used, so the two
	// sides cannot drift; only the comparisons below belong to validation.
	expectedHistory, previousRef, err := nextMasterBlockHistory(
		state.previousInfo,
		req.Previous.ID,
		previous.GenLT,
	)
	if err != nil {
		return err
	}
	expectedHistoryRoot, err := (&tlb.OldMcBlocksInfoAugDict{
		AugmentedDictionary: expectedHistory,
	}).ToCell()
	if err != nil {
		return fmt.Errorf("serialize expected masterchain block history: %w", err)
	}
	actualHistoryRoot, err := state.nextInfo.PrevBlocks.ToCell()
	if err != nil {
		return fmt.Errorf("serialize candidate masterchain block history: %w", err)
	}
	if expectedHistoryRoot.HashKey() != actualHistoryRoot.HashKey() {
		return fmt.Errorf("%w: resulting masterchain block history is invalid", ErrInvalidInput)
	}

	expectedLastKey := state.previousInfo.LastKeyBlock
	if state.previousInfo.AfterKeyBlock {
		expectedLastKey = &previousRef
	}
	if state.nextInfo.AfterKeyBlock != state.config.keyBlock ||
		!equalBlockReference(state.nextInfo.LastKeyBlock, expectedLastKey) {
		return fmt.Errorf("%w: resulting key block history is invalid", ErrInvalidInput)
	}

	expectedValidatorInfo, _, err := nextMasterValidatorInfo(
		state.previousInfo,
		state.config.groups,
		state.config.keyBlock,
		previous.GenUTime,
		candidate.block.BlockInfo.GenUtime,
	)
	if err != nil {
		return err
	}
	if state.nextInfo.ValidatorInfo != expectedValidatorInfo {
		return fmt.Errorf("%w: resulting masterchain validator info is invalid", ErrInvalidInput)
	}

	createStatsEnabled := req.Config.capabilities&capCreateStats != 0
	var shardCreators map[[32]byte]uint32
	if createStatsEnabled {
		shardCreators, err = countMasterShardCreators(tops)
		if err != nil {
			return err
		}
	}
	expectedFlags := uint16(0)
	if createStatsEnabled {
		expectedFlags = 1
	}
	if state.nextInfo.Flags != expectedFlags {
		return fmt.Errorf("%w: resulting block creator statistics are invalid", ErrInvalidInput)
	}
	if !createStatsEnabled {
		if state.nextInfo.BlockCreateStats != nil {
			return fmt.Errorf("%w: disabled block creator statistics are present", ErrInvalidInput)
		}
		return nil
	}
	if err = verifyBlockCreateStatsUpdate(
		state.previousCreators,
		state.nextCreators,
		candidate.block.BlockInfo.GenUtime,
		shardCreators,
		req.Candidate.CreatedBy,
	); err != nil {
		return fmt.Errorf("%w: resulting block creator statistics are invalid: %v", ErrInvalidInput, err)
	}

	return nil
}

func verifyMasterMinRefMCSeqno(candidate *verifiedCandidate, registry *ShardRegistry) error {
	if candidate.queue.ProcInfo == nil {
		return fmt.Errorf("%w: masterchain processed info is absent", ErrInvalidInput)
	}
	owner := msgpool.ShardIdent{Workchain: address.MasterchainID, Shard: uint64(1) << 63}
	records, err := tlb.LoadProcessedUptoRecords(candidate.queue.ProcInfo, owner.Shard)
	if err != nil {
		return fmt.Errorf("%w: decode resulting masterchain processed info: %v", ErrInvalidInput, err)
	}
	minimum := min(candidate.block.BlockInfo.SeqNo, minRecordedMCSeqno(records))
	// The registry was parsed strictly when it was built, so the cached fields
	// carry the same rejection the re-parse would have.
	for _, leaf := range registry.leaves {
		minimum = min(minimum, leaf.fields.minRefMCSeqno)
	}
	if candidate.state.MinRefMCSeqno != minimum {
		return fmt.Errorf(
			"%w: masterchain min ref mc seqno is %d, want %d",
			ErrInvalidInput,
			candidate.state.MinRefMCSeqno,
			minimum,
		)
	}

	return nil
}

// predecessorGroupConfig returns the snapshot's validator group config only when
// it demonstrably belongs to the predecessor's configuration root.
//
// validateMasterSnapshot runs later than this decode, so the snapshot is still
// unproven here; comparing its config root hash against the config already bound
// to the predecessor state is what makes reusing its parse sound. Anything else
// returns nil and the configuration is parsed from scratch.
func predecessorGroupConfig(snapshot *groups.Snapshot, config *Config) *groups.Config {
	if snapshot == nil || snapshot.Config == nil || config == nil || config.execution == nil {
		return nil
	}
	if snapshot.ConfigRootHash != config.execution.Root().HashKey() {
		return nil
	}
	return snapshot.Config
}
