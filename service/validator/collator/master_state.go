package collator

import (
	"bytes"
	"errors"
	"fmt"
	"math"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/groups"
)

type oldMCBlocksAugmentation struct{}

func (oldMCBlocksAugmentation) SkipExtra(loader *cell.Slice) error {
	if _, err := loader.LoadBoolBit(); err != nil {
		return err
	}
	_, err := loader.LoadUInt(64)
	return err
}

func (oldMCBlocksAugmentation) EmptyExtra(dst *cell.Builder) error {
	if err := dst.StoreBoolBit(false); err != nil {
		return err
	}
	return dst.StoreUInt(0, 64)
}

func (oldMCBlocksAugmentation) LeafExtra(value *cell.Slice, dst *cell.Builder) error {
	var entry tlb.KeyExtBlkRef
	if err := tlb.LoadFromCell(&entry, value); err != nil {
		return err
	}
	if value.BitsLeft() != 0 || value.RefsNum() != 0 {
		return fmt.Errorf("old masterchain block entry has trailing data")
	}
	if err := dst.StoreBoolBit(entry.IsKey); err != nil {
		return err
	}
	return dst.StoreUInt(entry.BlkRef.EndLt, 64)
}

func (oldMCBlocksAugmentation) CombineExtra(left, right *cell.Slice, dst *cell.Builder) error {
	leftKey, leftLT, err := loadKeyMaxLT(left)
	if err != nil {
		return err
	}
	rightKey, rightLT, err := loadKeyMaxLT(right)
	if err != nil {
		return err
	}
	if err = dst.StoreBoolBit(leftKey || rightKey); err != nil {
		return err
	}
	return dst.StoreUInt(max(leftLT, rightLT), 64)
}

func loadKeyMaxLT(loader *cell.Slice) (bool, uint64, error) {
	key, err := loader.LoadBoolBit()
	if err != nil {
		return false, 0, err
	}
	endLT, err := loader.LoadUInt(64)
	if err != nil {
		return false, 0, err
	}
	if loader.BitsLeft() != 0 || loader.RefsNum() != 0 {
		return false, 0, fmt.Errorf("old masterchain augmentation has trailing data")
	}
	return key, endLT, nil
}

func (c *collation) buildMasterStateAndBlockParts() (blockParts, error) {
	queueInfo, err := c.buildQueueInfo()
	if err != nil {
		return blockParts{}, err
	}
	if err = c.deriveMasterConfig(); err != nil {
		return blockParts{}, err
	}
	updateShardCatchain := c.master.keyBlock || crossedLifetime(
		c.oldState.GenUTime,
		c.header.GenUtime,
		c.master.nextGroups.Catchain.ShardLifetime,
	)
	if err = postprocessMasterShardRegistry(masterShardRegistryPostprocessInput{
		registry:       c.master.registry,
		workchains:     c.master.nextConfig.workchains,
		now:            c.header.GenUtime,
		updateCatchain: updateShardCatchain,
	}); err != nil {
		return blockParts{}, err
	}
	// ShardFees was already built by prepareMaster from the same frozen
	// accepted set; only ShardHashes is re-derived from the committed leaves.
	c.master.shardHashes, err = c.master.registry.Build()
	if err != nil {
		return blockParts{}, err
	}
	createStatsEnabled := c.config.capabilities&capCreateStats != 0
	var shardCreators map[[32]byte]uint32
	if createStatsEnabled {
		shardCreators, err = countMasterShardCreators(c.master.shardTops)
		if err != nil {
			return blockParts{}, err
		}
	}
	c.master.creatorStats, err = updateBlockCreateStats(blockCreateStatsInput{
		enabled:            createStatsEnabled,
		previous:           c.master.oldCreatorStats,
		now:                c.header.GenUtime,
		shardBlockCreators: shardCreators,
		masterchainCreator: c.req.createdBy,
	})
	if err != nil {
		return blockParts{}, err
	}
	flow, err := c.buildMasterValueFlow()
	if err != nil {
		return blockParts{}, err
	}
	overloadHistory, underloadHistory, wantSplit, wantMerge := c.loadHistory()

	stateInfo, err := c.buildMasterStateInfo()
	if err != nil {
		return blockParts{}, err
	}
	stateInfoCell, err := stateInfo.ToCell()
	if err != nil {
		return blockParts{}, fmt.Errorf("serialize masterchain state block info: %w", err)
	}
	globalBalance, err := c.master.oldExtra.GlobalBalance.Add(c.master.created)
	if err != nil {
		return blockParts{}, fmt.Errorf("add masterchain created balance: %w", err)
	}
	globalBalance, err = globalBalance.Add(c.master.minted)
	if err != nil {
		return blockParts{}, fmt.Errorf("add masterchain minted balance: %w", err)
	}
	globalBalance, err = globalBalance.Add(c.master.importedCreated)
	if err != nil {
		return blockParts{}, fmt.Errorf("add imported shard creation balance: %w", err)
	}
	stateExtra := tlb.McStateExtra{
		ShardHashes:   c.master.shardHashes,
		ConfigParams:  c.master.configParams,
		Info:          stateInfoCell,
		GlobalBalance: globalBalance,
	}
	stateExtraCell, err := tlb.ToCell(&stateExtra)
	if err != nil {
		return blockParts{}, fmt.Errorf("serialize masterchain state extra: %w", err)
	}

	statsCell, err := (tlb.ShardStateStats{
		OverloadHistory:    overloadHistory,
		UnderloadHistory:   underloadHistory,
		TotalBalance:       flow.totalBalance,
		TotalValidatorFees: flow.validatorFees,
		Libraries:          c.master.libraries,
	}).ToCell()
	if err != nil {
		return blockParts{}, fmt.Errorf("serialize masterchain state statistics: %w", err)
	}
	minRefMCSeqno := c.masterMinRefMCSeqno()

	nextState := c.oldState
	nextState.Seqno = c.header.SeqNo
	nextState.VertSeqno = c.header.VertSeqNo
	nextState.GenUTime = c.header.GenUtime
	nextState.GenLT = c.maxLT
	nextState.MinRefMCSeqno = minRefMCSeqno
	nextState.OutMsgQueueInfo = queueInfo
	nextState.BeforeSplit = false
	nextState.Accounts.ShardAccounts = c.accounts
	nextState.Stats = statsCell
	nextState.McStateExtra = stateExtraCell

	stateRoot, err := tlb.ToCell(&nextState)
	if err != nil {
		return blockParts{}, fmt.Errorf("serialize masterchain state: %w", err)
	}
	// One pass produces both the update and the shared, trace-free state; see
	// buildShardStateAndBlockParts for why the published root comes from here.
	stateUpdate, stateRoot, memoCells, err := c.usage.CreateMerkleUpdateAppliedSized(
		stateRoot,
		c.builder.updateMemoHint(),
		collationParallelism,
	)
	if err != nil {
		return blockParts{}, fmt.Errorf("create masterchain state update: %w", err)
	}
	c.updateMemoCells = memoCells

	inMessages, err := c.inMessages.ToCell()
	if err != nil {
		return blockParts{}, fmt.Errorf("serialize inbound message descriptors: %w", err)
	}
	outMessages, err := c.outMessages.ToCell()
	if err != nil {
		return blockParts{}, fmt.Errorf("serialize outbound message descriptors: %w", err)
	}
	accountBlocks, err := c.accountBlocks.ToCell()
	if err != nil {
		return blockParts{}, fmt.Errorf("serialize account blocks: %w", err)
	}
	signatures := cell.NewDict(16)
	mcExtra := &tlb.McBlockExtra{
		KeyBlock:    c.master.keyBlock,
		ShardHashes: c.master.shardHashes,
		ShardFees:   c.master.shardFees,
		ConfigParams: func() *tlb.ConfigParams {
			if !c.master.keyBlock {
				return nil
			}
			return &c.master.configParams
		}(),
	}
	mcExtra.Details.PrevBlockSignatures = signatures
	mcExtra.Details.RecoverCreateMsg = c.master.recoverCreateMsg
	mcExtra.Details.MintMsg = c.master.mintMsg
	mcExtraCell, err := serializeMcBlockExtra(mcExtra)
	if err != nil {
		return blockParts{}, err
	}
	extraRoot, err := serializeMasterBlockExtra(
		inMessages,
		outMessages,
		accountBlocks,
		c.req.randSeed,
		c.req.createdBy,
		mcExtraCell,
	)
	if err != nil {
		return blockParts{}, err
	}

	header := c.header
	header.EndLt = c.maxLT
	header.MinRefMcSeqno = minRefMCSeqno
	header.KeyBlock = c.master.keyBlock
	header.WantSplit = wantSplit
	header.WantMerge = wantMerge
	return blockParts{
		state:       stateRoot,
		stateUpdate: stateUpdate,
		valueFlow:   flow.root,
		header:      header,
		extraRoot:   extraRoot,
	}, nil
}

func (c *collation) buildMasterValueFlow() (valueFlowParts, error) {
	totalBalance, err := loadAccountsBalance(c.accounts)
	if err != nil {
		return valueFlowParts{}, err
	}
	transactionFees, err := loadCurrencyExtra(c.accountBlocks.AugmentedDictionary)
	if err != nil {
		return valueFlowParts{}, fmt.Errorf("load masterchain transaction fees: %w", err)
	}
	importFees, err := loadImportFees(c.inMessages.AugmentedDictionary)
	if err != nil {
		return valueFlowParts{}, fmt.Errorf("load masterchain import fees: %w", err)
	}
	exported, err := loadCurrencyExtra(c.outMessages.AugmentedDictionary)
	if err != nil {
		return valueFlowParts{}, fmt.Errorf("load masterchain exported value: %w", err)
	}

	totals, err := c.master.feeTotals(c.config, transactionFees, importFees)
	if err != nil {
		return valueFlowParts{}, err
	}
	feesCollected := totals.feesCollected

	validatorFees, err := c.oldStats.TotalValidatorFees.Add(feesCollected)
	if err != nil {
		return valueFlowParts{}, fmt.Errorf("update masterchain validator fees: %w", err)
	}
	validatorFees, err = validatorFees.Sub(c.master.recovered)
	if err != nil {
		return valueFlowParts{}, fmt.Errorf("recover masterchain validator fees: %w", err)
	}
	burned, err := c.master.importBurned.Add(totals.ordinaryBurned)
	if err != nil {
		return valueFlowParts{}, err
	}
	burned, err = burned.Add(c.burned)
	if err != nil {
		return valueFlowParts{}, err
	}

	flow := tlb.ValueFlow{
		FromPrevBlock: c.oldStats.TotalBalance,
		ToNextBlock:   totalBalance,
		Imported:      importFees.ValueImported,
		Exported:      exported,
		FeesCollected: feesCollected,
		FeesImported:  c.master.feesImported,
		Recovered:     c.master.recovered,
		Created:       c.master.created,
		Minted:        c.master.minted,
		Burned:        burned,
	}
	if err = flow.Validate(); err != nil {
		return valueFlowParts{}, fmt.Errorf("%w: invalid masterchain value flow: %v", ErrInvalidInput, err)
	}
	root, err := flow.ToCell()
	if err != nil {
		return valueFlowParts{}, fmt.Errorf("serialize masterchain value flow: %w", err)
	}
	return valueFlowParts{root: root, totalBalance: totalBalance, validatorFees: validatorFees}, nil
}

func (c *collation) deriveMasterConfig() error {
	// This runs under the block's read set, which decides the Merkle update and
	// answers the collated-size estimate's membership questions. The three parses
	// read roughly 2700 cells below the configuration parameters that nothing
	// else on this path touches — validator entries, fundamental-smc entries,
	// storage price rows — so skipping them is sound only while the prepared
	// configuration's footprint replays exactly what they read.
	// TestDeriveMasterConfigTransitionReuseMatchesFreshParse and
	// TestMasterConfigFootprintMutationsAreDetected are the guards; both compare
	// the record and not the produced bytes, for the reason configFootprint
	// documents.
	transition, err := deriveMasterConfigTransition(
		c.accounts,
		&c.master.oldExtra,
		masterConfigPredecessor{
			config: c.config,
			groups: c.master.previousGroups,
			usage:  c.usage,
		},
	)
	if err != nil {
		return err
	}

	c.master.nextConfig = transition.config
	c.master.nextGroups = transition.groups
	c.master.configParams = transition.params
	c.master.keyBlock = transition.keyBlock
	return nil
}

type masterConfigTransition struct {
	config   *Config
	groups   *groups.Config
	params   tlb.ConfigParams
	keyBlock bool
}

// masterConfigPredecessor is the predecessor's prepared configuration, plus the
// read set on the collation path. Reuse is decided and replayed in one place:
// the footprint describes the parses the gate skips, so it must never be
// recorded when the gate does not fire.
type masterConfigPredecessor struct {
	config *Config
	groups *groups.Config
	usage  *cell.ReadSet // nil on the verification path, which records nothing
}

// deriveMasterConfigTransition resolves the configuration the candidate
// installs, from the configuration contract's own account.
//
// The predecessor's already prepared configuration is used only when the
// resulting configuration is the very same root at the very same address.
// Preparing a configuration means building the execution config, the collator
// config and the validator group config, which together are the bulk of this
// function's cost and are pure functions of the root — the reference collator
// likewise reuses its parsed config and only re-validates the raw data every
// block. Either half may be absent, in which case everything is derived from
// scratch.
func deriveMasterConfigTransition(
	accounts *tlb.ShardAccountsAugDict,
	oldExtra *tlb.McStateExtra,
	previous masterConfigPredecessor,
) (masterConfigTransition, error) {
	currentAddr := oldExtra.ConfigParams.ConfigAddr
	oldRoot := oldExtra.ConfigParams.Config.Params.AsCell()
	root, err := masterConfigRootFromAccounts(accounts, currentAddr)
	if err != nil {
		return masterConfigTransition{}, fmt.Errorf("current configuration contract: %w", err)
	}
	if err = validateMasterConfigData(root, currentAddr, oldRoot, true); err != nil {
		return masterConfigTransition{}, fmt.Errorf("current configuration contract: %w", err)
	}

	nextAddr := currentAddr
	requestedAddr, addressErr := exactConfigAddress(root)
	if addressErr != nil && !errors.Is(addressErr, errConfigAddressAbsent) {
		return masterConfigTransition{}, fmt.Errorf("%w: configuration parameter 0 is malformed", ErrInvalidInput)
	}
	if addressErr == nil && !bytes.Equal(requestedAddr, currentAddr) {
		candidateRoot, fetchErr := masterConfigRootFromAccounts(accounts, requestedAddr)
		// The current valid configuration is retained when the newly requested
		// contract is absent or does not hold a valid config.
		if fetchErr == nil && validateMasterConfigData(candidateRoot, requestedAddr, oldRoot, false) == nil {
			root = candidateRoot
			nextAddr = requestedAddr
		}
	}

	// The gate is deliberately narrow: same root cell and same contract address.
	// It matches important_config_parameters_changed, which is itself a plain
	// root comparison, and the address term is what keeps a configuration moved
	// to a new contract with identical contents a key block. Widening it to
	// "important parameters unchanged" would diverge from the reference.
	//
	// The raw validation above is NOT part of the gate and always runs, exactly
	// as block::valid_config_data does on every masterchain block.
	//
	// A recorder without a usable footprint is the last term, and it is the whole
	// safety argument: the reads the skipped parses would have taken are replayed
	// here, at the exact position they would have happened, or the parses run.
	nextConfig, nextGroups := previous.config, previous.groups
	reuse := previous.config != nil && previous.groups != nil &&
		previous.config.execution != nil &&
		bytes.Equal(currentAddr, nextAddr) &&
		root.HashKey() == oldRoot.HashKey() &&
		previous.config.execution.Root().HashKey() == oldRoot.HashKey() &&
		(previous.usage == nil || previous.config.footprint.covers(root))
	if reuse {
		previous.config.footprint.replay(previous.usage)
	} else {
		parsed, parseErr := parseMasterConfigEpoch(root)
		if parseErr != nil {
			return masterConfigTransition{}, parseErr
		}
		nextConfig, nextGroups = parsed.config, parsed.groups
	}
	// Asserted on both branches: a reused config was already checked when it was
	// prepared, and keeping the check here means the invariant holds for every
	// value this function returns rather than only for freshly parsed ones.
	if nextGroups.Catchain.MasterchainLifetime == 0 || nextGroups.Catchain.ShardLifetime == 0 {
		return masterConfigTransition{}, fmt.Errorf("%w: resulting catchain lifetime is zero", ErrInvalidInput)
	}
	// The config root is read through the config account, so it can carry the
	// account-path usage trace. It is then embedded into McStateExtra at a
	// different state path. Retaining that trace would make the Merkle-update
	// builder reuse a source boundary from the account path and change the
	// candidate state hash when the predecessor came from lazy CellDB.
	root, err = root.Prewarm()
	if err != nil {
		return masterConfigTransition{}, fmt.Errorf("%w: materialize resulting config root: %v", ErrInvalidInput, err)
	}
	dict := root.WithoutTrace().AsDict(32)
	if dict.GetKeySize() != 32 || dict.IsEmpty() {
		return masterConfigTransition{}, fmt.Errorf("%w: resulting config dictionary is malformed", ErrInvalidInput)
	}

	params := tlb.ConfigParams{ConfigAddr: nextAddr}
	params.Config.Params = dict
	return masterConfigTransition{
		config:   nextConfig,
		groups:   nextGroups,
		params:   params,
		keyBlock: !bytes.Equal(currentAddr, nextAddr) || root.HashKey() != oldRoot.HashKey(),
	}, nil
}

func masterConfigRootFromAccounts(accounts *tlb.ShardAccountsAugDict, accountID []byte) (*cell.Cell, error) {
	if len(accountID) != 32 {
		return nil, fmt.Errorf("%w: configuration account id is malformed", ErrInvalidInput)
	}
	key := cell.BeginCell().MustStoreSlice(accountID, 256).EndCell()
	value, err := accounts.LoadValue(key)
	if err != nil {
		return nil, fmt.Errorf("load configuration account: %w", err)
	}
	var shardAccount tlb.ShardAccount
	if err = loadExactSlice(&shardAccount, value); err != nil {
		return nil, fmt.Errorf("decode configuration account: %w", err)
	}
	addr := address.NewAddress(0, 0xff, accountID)
	prepared, err := tvm.PrepareAccount(&shardAccount, addr)
	if err != nil {
		return nil, err
	}
	state := prepared.State()
	if state.Status != tlb.AccountStatusActive || state.StateInit == nil || state.StateInit.Data == nil {
		return nil, fmt.Errorf("%w: configuration account is not active or has no data", ErrInvalidInput)
	}
	data, err := state.StateInit.Data.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("parse configuration account data: %w", err)
	}
	root, err := data.LoadRefCell()
	if err != nil {
		return nil, fmt.Errorf("%w: configuration is absent from the first data reference", ErrInvalidInput)
	}
	return root, nil
}

func (c *collation) buildMasterStateInfo() (tlb.McStateExtraBlockInfo, error) {
	prevBlocks, previous, err := nextMasterBlockHistory(c.master.oldInfo, c.req.previous.ID, c.oldState.GenLT)
	if err != nil {
		return tlb.McStateExtraBlockInfo{}, err
	}

	info := c.master.oldInfo
	info.PrevBlocks = &tlb.OldMcBlocksInfoAugDict{AugmentedDictionary: prevBlocks}
	info.AfterKeyBlock = c.master.keyBlock
	info.BlockCreateStats = c.master.creatorStats
	if c.config.capabilities&capCreateStats != 0 {
		info.Flags |= 1
	} else {
		info.Flags &^= 1
	}
	if c.master.oldInfo.AfterKeyBlock {
		last := previous
		info.LastKeyBlock = &last
	}

	info.ValidatorInfo, _, err = nextMasterValidatorInfo(
		c.master.oldInfo,
		c.master.nextGroups,
		c.master.keyBlock,
		c.oldState.GenUTime,
		c.header.GenUtime,
	)
	if err != nil {
		return tlb.McStateExtraBlockInfo{}, err
	}
	return info, nil
}

// nextMasterBlockHistory inserts the predecessor into the masterchain block
// history and returns the resulting dictionary together with the predecessor
// reference the LastKeyBlock ladder needs.
//
// Collation derives it to store it; validation derives it to compare — the two
// must not drift, so they share this one derivation, exactly like the value
// flow shares masterFeeTotals.
func nextMasterBlockHistory(
	previousInfo tlb.McStateExtraBlockInfo,
	previousID ton.BlockIDExt,
	previousGenLT uint64,
) (*cell.AugmentedDictionary, tlb.ExtBlkRef, error) {
	prevBlocks, err := writableOldMCBlocks(previousInfo.PrevBlocks)
	if err != nil {
		return nil, tlb.ExtBlkRef{}, err
	}
	previous, err := blockReference(previousID, previousGenLT)
	if err != nil {
		return nil, tlb.ExtBlkRef{}, err
	}
	value, err := tlb.ToCell(&tlb.KeyExtBlkRef{
		IsKey:  previousInfo.AfterKeyBlock,
		BlkRef: previous,
	})
	if err != nil {
		return nil, tlb.ExtBlkRef{}, fmt.Errorf("serialize previous masterchain reference: %w", err)
	}
	key := cell.BeginCell().MustStoreUInt(uint64(previousID.SeqNo), 32).EndCell()
	inserted, err := prevBlocks.SetWithMode(key, value, cell.DictSetModeAdd)
	if err != nil {
		return nil, tlb.ExtBlkRef{}, fmt.Errorf("update previous masterchain block history: %w", err)
	}
	if !inserted {
		return nil, tlb.ExtBlkRef{}, fmt.Errorf(
			"%w: predecessor already exists in masterchain block history", ErrInvalidInput,
		)
	}

	return prevBlocks, previous, nil
}

// nextMasterValidatorInfo derives the resulting masterchain validator info and
// reports whether the masterchain catchain rotated. Shared between collation
// and validation for the same reason as nextMasterBlockHistory.
func nextMasterValidatorInfo(
	previousInfo tlb.McStateExtraBlockInfo,
	cfg *groups.Config,
	keyBlock bool,
	previousGenUTime, genUtime uint32,
) (tlb.ValidatorInfo, bool, error) {
	catchainSeqno := previousInfo.ValidatorInfo.CatchainSeqno
	catchain := cfg.Catchain
	ccUpdated := keyBlock || crossedLifetime(previousGenUTime, genUtime, catchain.MasterchainLifetime)
	if ccUpdated {
		if catchainSeqno == math.MaxUint32 {
			return tlb.ValidatorInfo{}, false, fmt.Errorf(
				"%w: masterchain catchain seqno overflow", ErrInvalidInput,
			)
		}
		catchainSeqno++
	}
	validatorHash, err := groups.MasterchainStateValidatorHash(cfg, catchainSeqno)
	if err != nil {
		return tlb.ValidatorInfo{}, false, fmt.Errorf("compute resulting masterchain validator hash: %w", err)
	}

	return tlb.ValidatorInfo{
		ValidatorListHashShort: validatorHash,
		CatchainSeqno:          catchainSeqno,
		NextCCUpdated: ccUpdated && (keyBlock ||
			crossedLifetime(previousGenUTime, genUtime, catchain.ShardLifetime)),
	}, ccUpdated, nil
}

func writableOldMCBlocks(old *tlb.OldMcBlocksInfoAugDict) (*cell.AugmentedDictionary, error) {
	root, err := old.ToCell()
	if err != nil {
		return nil, fmt.Errorf("serialize old masterchain block history: %w", err)
	}
	loader, err := root.BeginParse()
	if err != nil {
		return nil, err
	}
	dict, err := loader.LoadAugDict(32, oldMCBlocksAugmentation{}, false)
	if err != nil {
		return nil, fmt.Errorf("load writable masterchain block history: %w", err)
	}
	if loader.BitsLeft() != 0 || loader.RefsNum() != 0 {
		return nil, fmt.Errorf("%w: masterchain block history has trailing data", ErrInvalidInput)
	}
	return dict, nil
}

func crossedLifetime(previous, current, lifetime uint32) bool {
	return lifetime > 0 && current/lifetime > previous/lifetime
}

// masterMinRefMCSeqno reads the registry this collation already built rather
// than re-parsing the ShardHashes dictionary it serialized from it. Map order
// does not matter for a minimum.
func (c *collation) masterMinRefMCSeqno() uint32 {
	minimum := min(c.header.SeqNo, c.processedMinMC)
	for _, leaf := range c.master.registry.leaves {
		minimum = min(minimum, leaf.fields.minRefMCSeqno)
	}

	return minimum
}

func serializeMasterBlockExtra(
	inMessages, outMessages, accountBlocks *cell.Cell,
	randSeed, createdBy [32]byte,
	masterExtra *cell.Cell,
) (*cell.Cell, error) {
	b := cell.BeginCell().MustStoreUInt(0x4a33f6fd, 32)
	if inMessages == nil || outMessages == nil || accountBlocks == nil {
		return nil, fmt.Errorf("%w: masterchain block dictionaries are absent", ErrInvalidInput)
	}
	if err := b.StoreRef(inMessages); err != nil {
		return nil, fmt.Errorf("serialize masterchain inbound descriptors: %w", err)
	}
	if err := b.StoreRef(outMessages); err != nil {
		return nil, fmt.Errorf("serialize masterchain outbound descriptors: %w", err)
	}
	if err := b.StoreRef(accountBlocks); err != nil {
		return nil, fmt.Errorf("serialize masterchain account blocks: %w", err)
	}
	if err := b.StoreSlice(randSeed[:], 256); err != nil {
		return nil, err
	}
	if err := b.StoreSlice(createdBy[:], 256); err != nil {
		return nil, err
	}
	if err := b.StoreBoolBit(true); err != nil {
		return nil, err
	}
	if err := b.StoreRef(masterExtra); err != nil {
		return nil, err
	}
	return b.EndCell(), nil
}

func serializeBlock(
	globalID int32,
	header tlb.BlockHeader,
	valueFlow, stateUpdate, extra *cell.Cell,
) (*cell.Cell, error) {
	headerCell, err := header.ToCell()
	if err != nil {
		return nil, fmt.Errorf("serialize block header: %w", err)
	}
	b := cell.BeginCell().MustStoreUInt(0x11ef55aa, 32).MustStoreInt(int64(globalID), 32)
	if valueFlow == nil || stateUpdate == nil || extra == nil {
		return nil, fmt.Errorf("%w: block body reference is absent", ErrInvalidInput)
	}
	if err = b.StoreRef(headerCell); err != nil {
		return nil, fmt.Errorf("serialize block header: %w", err)
	}
	if err = b.StoreRef(valueFlow); err != nil {
		return nil, fmt.Errorf("serialize block value flow: %w", err)
	}
	if err = b.StoreRef(stateUpdate); err != nil {
		return nil, fmt.Errorf("serialize block state update: %w", err)
	}
	if err = b.StoreRef(extra); err != nil {
		return nil, fmt.Errorf("serialize block extra: %w", err)
	}
	return b.EndCell(), nil
}

var _ cell.Augmentation = oldMCBlocksAugmentation{}
