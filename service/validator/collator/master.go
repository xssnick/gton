package collator

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/big"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/msgpool"
)

const maxShardTopChainLength = uint32(8)

type masterCollation struct {
	oldExtra tlb.McStateExtra
	oldInfo  tlb.McStateExtraBlockInfo
	// oldCreatorStats is the predecessor's BlockCreateStats as parsed with
	// oldInfo, so the update pass does not walk that dictionary a second time.
	oldCreatorStats blockCreateStats

	// previousGroups is the predecessor's parsed validator config, already bound
	// to the predecessor's config root by validateMasterSnapshot.
	previousGroups *groups.Config

	registry    *ShardRegistry
	shardHashes *cell.Dictionary
	shardFees   *tlb.ShardFeesAugDict
	shardTops   []ShardTop

	feesImported    tlb.CurrencyCollection
	importedCreated tlb.CurrencyCollection
	recovered       tlb.CurrencyCollection
	minted          tlb.CurrencyCollection
	created         tlb.CurrencyCollection
	importBurned    tlb.CurrencyCollection

	recoverCreateMsg *cell.Cell
	mintMsg          *cell.Cell
	keyBlock         bool
	nextConfig       *Config
	nextGroups       *groups.Config
	configParams     tlb.ConfigParams
	libraries        *cell.Dictionary
	creatorStats     *cell.Cell
}

// BuildMaster constructs a linear masterchain candidate. ShardTop cells are
// verified by the acquisition pipeline; their descriptor transitions and all
// value/state effects are revalidated by this core.
func (b *Builder) BuildMaster(ctx context.Context, req MasterRequest) (*Candidate, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if req.Config != nil && req.FullCollatedProofs != nil && req.Config.capabilities&capFullCollatedData == 0 {
		return nil, fmt.Errorf("%w: full collated proof provider supplied without capability", ErrInvalidInput)
	}
	common := masterCollationRequest(req)
	if err := validateCollationRequest(&common); err != nil {
		return nil, err
	}
	return retryUnderSizeLimit(func(narrowing sizeBudgetCap) (*Candidate, error) {
		return b.buildMasterAttempt(ctx, req, narrowing)
	})
}

func (b *Builder) buildMasterAttempt(ctx context.Context, req MasterRequest, narrowing sizeBudgetCap) (*Candidate, error) {
	c, err := b.prepareMasterPhases(ctx, req, narrowing)
	if err != nil {
		return nil, err
	}
	if err = c.processExternals(); err != nil {
		return nil, err
	}

	return c.finishMaster(req)
}

func (b *Builder) prepareMasterPhases(
	ctx context.Context,
	req MasterRequest,
	narrowing sizeBudgetCap,
) (*collation, error) {
	c, err := b.prepareMaster(ctx, req)
	if err != nil {
		return nil, err
	}
	c.limits.narrowBytes(narrowing)
	if err = c.limits.addProof(c.outQueue.RootCell()); err != nil {
		return nil, fmt.Errorf("initial outbound queue proof: %w", err)
	}
	if err = c.limits.addProof(c.dispatchQueue.RootCell()); err != nil {
		return nil, fmt.Errorf("initial dispatch queue proof: %w", err)
	}

	// Dispatch processing is shared with shardchain; the tick/special/tock
	// phases are masterchain-specific and are deliberately placed around
	// ordinary inbound processing.
	if err = c.cleanupOutQueue(); err != nil {
		return nil, err
	}
	if err = c.processDispatchQueue(); err != nil {
		return nil, err
	}
	if err = c.processMasterTickTock(false); err != nil {
		return nil, err
	}
	if err = c.processMasterSpecials(); err != nil {
		return nil, err
	}
	if err = c.processInternals(); err != nil {
		return nil, err
	}

	return c, nil
}

func (c *collation) finishMaster(req MasterRequest) (*Candidate, error) {
	common := masterCollationRequest(req)
	if err := c.processNewMessages(c.blockFull || c.haveUnprocessedDispatchQueue || common.internalsIncomplete()); err != nil {
		return nil, err
	}
	if err := c.processMasterTickTock(true); err != nil {
		return nil, err
	}
	if err := c.processNewMessages(true); err != nil {
		return nil, err
	}
	if err := c.updateMasterPublicLibraries(); err != nil {
		return nil, err
	}
	if err := c.updateProcessedInfo(); err != nil {
		return nil, err
	}
	if err := c.finishAccounts(); err != nil {
		return nil, err
	}

	return c.finish()
}

func (b *Builder) prepareMaster(ctx context.Context, req MasterRequest) (*collation, error) {
	if req.Config == nil || req.Groups == nil {
		return nil, fmt.Errorf("%w: masterchain config or group snapshot is absent", ErrInvalidInput)
	}
	if req.Previous.State == nil {
		return nil, fmt.Errorf("%w: previous masterchain state is absent", ErrInvalidInput)
	}
	if req.Previous.ID.Workchain != address.MasterchainID || req.Previous.ID.Shard != math.MinInt64 {
		return nil, fmt.Errorf("%w: predecessor is not the masterchain root shard", ErrInvalidInput)
	}
	if len(req.Previous.ID.RootHash) != 32 || len(req.Previous.ID.FileHash) != 32 {
		return nil, fmt.Errorf("%w: predecessor block hashes must be 256 bits", ErrInvalidInput)
	}
	if req.Previous.ID.SeqNo == math.MaxUint32 {
		return nil, fmt.Errorf("%w: masterchain sequence number overflow", ErrInvalidInput)
	}

	usage := cell.NewReadSetSized(req.Previous.State, b.readSetHint())
	storageCells, storageProofCells := b.storageHints()
	oldRoot := usage.Root()
	var oldState tlb.ShardStateUnsplit
	if err := parseExact(&oldState, oldRoot); err != nil {
		return nil, fmt.Errorf("%w: decode previous masterchain state: %v", ErrInvalidInput, err)
	}
	masterIdent, err := topologyShardIdent(groups.ShardID{Workchain: address.MasterchainID, Shard: math.MinInt64})
	if err != nil {
		return nil, err
	}
	if oldState.ShardIdent != masterIdent || oldState.Seqno != req.Previous.ID.SeqNo || oldState.McStateExtra == nil {
		return nil, fmt.Errorf("%w: predecessor id and masterchain state header disagree", ErrInvalidInput)
	}
	if req.Previous.ID.SeqNo == 0 && !bytes.Equal(req.Previous.ID.RootHash, req.Previous.State.Hash()) {
		return nil, fmt.Errorf("%w: masterchain zerostate hash differs from its id", ErrInvalidInput)
	}
	if req.Previous.Block != nil {
		if _, err = verifyPredecessor("master", &req.Previous); err != nil {
			return nil, err
		}
	}
	var oldStats tlb.ShardStateStats
	if err = parseExact(&oldStats, oldState.Stats); err != nil {
		return nil, fmt.Errorf("%w: decode previous masterchain statistics: %v", ErrInvalidInput, err)
	}
	if oldStats.MasterRef != nil {
		return nil, fmt.Errorf("%w: masterchain state contains a masterchain reference", ErrInvalidInput)
	}
	var queueInfo tlb.OutMsgQueueInfo
	if err = parseExact(&queueInfo, oldState.OutMsgQueueInfo); err != nil {
		return nil, fmt.Errorf("%w: decode previous masterchain queue: %v", ErrInvalidInput, err)
	}
	queueSize, err := existingQueueSize(&queueInfo, req.Previous.OutQueueSize)
	if err != nil {
		return nil, err
	}

	var oldExtra tlb.McStateExtra
	if err = parseExact(&oldExtra, oldState.McStateExtra); err != nil {
		return nil, fmt.Errorf("%w: decode previous masterchain extra: %v", ErrInvalidInput, err)
	}
	oldInfo, oldCreatorStats, err := parseMasterStateInfoWithStats(oldExtra.Info)
	if err != nil {
		return nil, fmt.Errorf("%w: decode previous masterchain block info: %v", ErrInvalidInput, err)
	}
	if oldInfo.PrevBlocks == nil || oldInfo.PrevBlocks.AugmentedDictionary == nil {
		return nil, fmt.Errorf("%w: previous masterchain block history is absent", ErrInvalidInput)
	}
	configRoot := oldExtra.ConfigParams.Config.Params
	if len(oldExtra.ConfigParams.ConfigAddr) != 32 || configRoot == nil || configRoot.GetKeySize() != 32 || configRoot.IsEmpty() {
		return nil, fmt.Errorf("%w: previous masterchain config params are malformed", ErrInvalidInput)
	}
	if configRoot.AsCell().HashKey() != req.Config.execution.Root().HashKey() {
		return nil, fmt.Errorf("%w: supplied config is not the config stored in predecessor state", ErrInvalidInput)
	}
	if err = validateMasterSnapshot(req.Previous, req.Groups, configRoot.AsCell(), oldState.GenUTime); err != nil {
		return nil, err
	}

	header, err := b.makeMasterHeader(req, &oldState, &oldInfo)
	if err != nil {
		return nil, err
	}
	registry, err := ParseShardRegistry(oldExtra.ShardHashes)
	if err != nil {
		return nil, err
	}
	shardTops, shardsMaxEndLT, err := prepareMasterShardTops(req, header.SeqNo)
	if err != nil {
		return nil, err
	}
	header.StartLt, err = nextMasterBlockStartLT(oldState.GenLT, shardsMaxEndLT)
	if err != nil {
		return nil, err
	}
	if err = registry.Apply(shardTops, header.StartLt); err != nil {
		return nil, err
	}
	if err = activateMasterShardWorkchains(masterShardWorkchainActivationInput{
		registry:      registry,
		workchains:    req.Config.workchains,
		now:           header.GenUtime,
		newBlockSeqno: header.SeqNo,
	}); err != nil {
		return nil, err
	}
	if req.Config.capabilities&capFullCollatedData != 0 {
		if err = verifyFullMasterNeighborSet(req.Previous, &oldState, registry, req.Neighbors); err != nil {
			return nil, err
		}
	}
	// Only ShardFees is read before the state pass; the ShardHashes dictionary
	// built here would be discarded by the rebuild at masterStateParts. accepted
	// is frozen once activateMasterShardWorkchains has run.
	shardFees, err := buildShardFeesDictionary(registry.accepted)
	if err != nil {
		return nil, fmt.Errorf("build shard fees: %w", err)
	}
	dispatchQueue, err := masterDispatchQueue(&queueInfo)
	if err != nil {
		return nil, err
	}
	owner := msgpool.ShardIdent{Workchain: address.MasterchainID, Shard: uint64(1) << 63}
	processedRecords, err := tlb.LoadProcessedUptoRecords(queueInfo.ProcInfo, owner.Shard)
	if err != nil {
		return nil, fmt.Errorf("%w: decode processed info: %v", ErrInvalidInput, err)
	}
	processedMinMC := minRecordedMCSeqno(processedRecords)
	prevBlocks, err := masterPrevBlocksTuple(req.Previous.ID, &oldInfo, req.Config.globalVersion)
	if err != nil {
		return nil, fmt.Errorf("build masterchain previous-block tuple: %w", err)
	}
	// Pass the traced LibDescr root itself, as the reference collator does. TVM
	// walks only a requested library's path; an unused immutable collection stays
	// a single predecessor boundary in the state update.
	executionLibraries, err := masterExecutionLibraries(oldStats.Libraries)
	if err != nil {
		return nil, err
	}
	blockCtx, err := req.Config.execution.NewBlockContext(tvm.BlockOptions{
		Now:        header.GenUtime,
		BlockLT:    int64(header.StartLt),
		RandSeed:   req.RandSeed[:],
		PrevBlocks: prevBlocks,
		GlobalID:   oldState.GlobalID,
		Libraries:  executionLibraries,
	})
	if err != nil {
		return nil, fmt.Errorf("prepare masterchain transaction context: %w", err)
	}

	accounts := &tlb.ShardAccountsAugDict{AugmentedDictionary: oldState.Accounts.ShardAccounts.Copy()}
	outQueue := &tlb.OutMsgQueueAugDict{AugmentedDictionary: queueInfo.OutQueue.Copy()}
	inMessages, err := tlb.NewInMsgDescrAugDict(req.Config.globalVersion)
	if err != nil {
		return nil, err
	}
	outMessages, err := tlb.NewOutMsgDescrAugDict(req.Config.globalVersion)
	if err != nil {
		return nil, err
	}
	accountBlocks, err := tlb.NewShardAccountBlocksAugDict()
	if err != nil {
		return nil, err
	}

	master := &masterCollation{
		oldExtra:        oldExtra,
		oldInfo:         oldInfo,
		oldCreatorStats: oldCreatorStats,
		previousGroups:  req.Groups.Config,
		registry:        registry,
		shardFees:       shardFees,
		shardTops:       shardTops,
	}
	if err = master.initValueFlow(req.Config, &oldStats); err != nil {
		return nil, err
	}
	limits, err := blockLimitsAtTime(req.Config.masterchain.limits, oldState.GenUTime, header.GenUtime)
	if err != nil {
		return nil, err
	}

	// Unlike the shard path (build.go prepare), no read-set record callback is
	// registered here and no proof-size estimator is built: masterchain collated
	// data carries no previous-state usage proof — buildCollatedRoots emits that
	// block only when c.master == nil — so an estimator would have nothing to
	// accumulate and the collated budget is the fixed estimate alone. The
	// callback cannot move into a shared constructor either: the shard path must
	// register it before preparePredecessor traces the previous state, while this
	// path has already traced oldRoot above.
	c := &collation{
		ctx:        ctx,
		builder:    b,
		req:        masterCollationRequest(req),
		header:     header,
		shard:      owner,
		config:     req.Config,
		master:     master,
		usage:      usage,
		shardEndLT: newShardEndLTResolver(req.NeighborShardEndLT),
		oldRoot:    oldRoot,
		oldState:   oldState,
		oldStats:   oldStats,
		accountSources: [2]predecessorAccountSource{{
			shard:    oldState.ShardIdent,
			accounts: oldState.Accounts.ShardAccounts,
		}},
		dispatchSources: [2]predecessorDispatchSource{{
			shard: oldState.ShardIdent,
			queue: predecessorDispatchQueue(&queueInfo),
		}},
		dispatchSourceCount: 1,
		blockCtx:            blockCtx,
		limits:              newBlockLimitStatus(limits, header.StartLt, usage, storageCells, storageProofCells),
		hardLTDelta:         limits.ltDelta[3],
		queueSize:           queueSize,
		oldQueueSize:        queueSize,
		accounts:            accounts,
		oldOutQueue: &tlb.OutMsgQueueAugDict{
			AugmentedDictionary: queueInfo.OutQueue.Copy(),
		},
		outQueue:         outQueue,
		processed:        queueInfo.ProcInfo,
		processedMinMC:   processedMinMC,
		processedRecords: processedRecords,
		processedParsed:  true,
		inMessages:       inMessages,
		outMessages:      outMessages,
		accountBlocks:    accountBlocks,
		oldDispatchQueue: &tlb.DispatchQueueAugDict{
			AugmentedDictionary: dispatchQueue.Copy(),
		},
		dispatchQueue:       dispatchQueue,
		lanes:               make(map[[32]byte]*accountLane),
		senderGenerated:     make(map[[32]byte]uint32),
		lastDispatchEmitted: make(map[[32]byte]uint64),
		unprocessedDeferred: make(map[[32]byte]uint32),
		maxLT:               header.StartLt + 1,
		externals:           make([]msgpool.ExternalFeedback, 0, len(req.Externals)),
		fullCollated:        req.Config.capabilities&capFullCollatedData != 0,
	}
	if err = c.prepareFullCollatedProofs(); err != nil {
		return nil, err
	}
	if err = c.prepareMasterCollatedEstimate(); err != nil {
		return nil, err
	}

	return c, nil
}

func masterDispatchQueue(info *tlb.OutMsgQueueInfo) (*tlb.DispatchQueueAugDict, error) {
	if info.Extra != nil {
		return &tlb.DispatchQueueAugDict{
			AugmentedDictionary: info.Extra.DispatchQueue.Copy(),
		}, nil
	}
	dispatch, err := tlb.NewDispatchQueueAugDict()
	if err != nil {
		return nil, fmt.Errorf("create masterchain dispatch queue: %w", err)
	}
	return dispatch, nil
}

func (b *Builder) makeMasterHeader(
	req MasterRequest,
	oldState *tlb.ShardStateUnsplit,
	oldInfo *tlb.McStateExtraBlockInfo,
) (tlb.BlockHeader, error) {
	previous, err := blockReference(req.Previous.ID, oldState.GenLT)
	if err != nil {
		return tlb.BlockHeader{}, fmt.Errorf("%w: previous masterchain block id: %v", ErrInvalidInput, err)
	}
	if req.Header.GenUtimeMS/1000 != uint64(req.Header.GenUtime) {
		return tlb.BlockHeader{}, fmt.Errorf("%w: millisecond generation time is outside the block second", ErrInvalidInput)
	}
	if err = requireGenUtimeMonotonic(
		req.Config.globalVersion,
		req.Header.GenUtime,
		oldState.GenUTime,
		"the predecessor",
	); err != nil {
		return tlb.BlockHeader{}, err
	}
	active, err := activeMasterSession(req.Groups)
	if err != nil {
		return tlb.BlockHeader{}, err
	}
	previousKeySeqno := uint32(0)
	if oldInfo.AfterKeyBlock {
		previousKeySeqno = req.Previous.ID.SeqNo
	} else if oldInfo.LastKeyBlock != nil {
		if oldInfo.LastKeyBlock.SeqNo > req.Previous.ID.SeqNo ||
			len(oldInfo.LastKeyBlock.RootHash) != 32 || len(oldInfo.LastKeyBlock.FileHash) != 32 {
			return tlb.BlockHeader{}, fmt.Errorf("%w: previous key block reference is malformed", ErrInvalidInput)
		}
		previousKeySeqno = oldInfo.LastKeyBlock.SeqNo
	}
	if req.Groups.LastKeyBlockSeqno != previousKeySeqno {
		return tlb.BlockHeader{}, fmt.Errorf("%w: group snapshot previous key seqno is %d, want %d",
			ErrInvalidInput, req.Groups.LastKeyBlockSeqno, previousKeySeqno)
	}

	var header tlb.BlockHeader
	header.SeqNo = req.Previous.ID.SeqNo + 1
	header.VertSeqNo = oldState.VertSeqno
	header.Shard = oldState.ShardIdent
	header.GenUtime = req.Header.GenUtime
	header.GenValidatorListHashShort = active.ValidatorSetHash
	header.GenCatchainSeqno = active.CatchainSeqno
	header.PrevKeyBlockSeqno = previousKeySeqno
	header.PrevRef = tlb.BlkPrevInfo{Prev1: previous}
	if req.Config.capabilities&capReportVersion != 0 {
		header.Flags = 1
		software := b.software
		header.GenSoftware = &software
	}

	return header, nil
}

func validateMasterSnapshot(
	previous PreviousBlock,
	snapshot *groups.Snapshot,
	configRoot *cell.Cell,
	previousGenUtime uint32,
) error {
	if !snapshot.MasterchainBlock.Equals(&previous.ID) {
		return fmt.Errorf("%w: validator group snapshot is not derived from the predecessor", ErrInvalidInput)
	}
	if snapshot.ConfigRootHash != configRoot.HashKey() {
		return fmt.Errorf("%w: validator group snapshot has a different config root", ErrInvalidInput)
	}
	if snapshot.GenUTime != previousGenUtime {
		return fmt.Errorf("%w: validator group snapshot time differs from predecessor", ErrInvalidInput)
	}
	if snapshot.Config == nil {
		return fmt.Errorf("%w: validator group snapshot has no parsed config", ErrInvalidInput)
	}
	_, err := activeMasterSession(snapshot)
	return err
}

func activeMasterSession(snapshot *groups.Snapshot) (*groups.Session, error) {
	var active *groups.Session
	for i := range snapshot.Active {
		session := &snapshot.Active[i]
		if !session.Shard.IsMasterchain() {
			continue
		}
		if active != nil {
			return nil, fmt.Errorf("%w: group snapshot has multiple active masterchain sessions", ErrInvalidInput)
		}
		active = session
	}
	if active == nil {
		return nil, fmt.Errorf("%w: group snapshot has no active masterchain session", ErrInvalidInput)
	}
	return active, nil
}

func prepareMasterShardTops(req MasterRequest, newMCSeqno uint32) ([]ShardTop, uint64, error) {
	allowSameGenUtime := req.Config.globalVersion >= 13
	return prepareMasterShardTopsWithTimePolicy(req, newMCSeqno, allowSameGenUtime)
}

func prepareMasterShardTopsForVerification(req MasterRequest, newMCSeqno uint32) ([]ShardTop, uint64, error) {
	// Candidate validation accepts equal shard/master timestamps for every version;
	// the pre-v13 strict ordering above is only a collation-side policy.
	return prepareMasterShardTopsWithTimePolicy(req, newMCSeqno, true)
}

func prepareMasterShardTopsWithTimePolicy(
	req MasterRequest,
	newMCSeqno uint32,
	allowSameGenUtime bool,
) ([]ShardTop, uint64, error) {
	tops := make([]ShardTop, len(req.ShardTops))
	copy(tops, req.ShardTops)
	var maxEndLT uint64
	seenDescriptors := make(map[cell.Hash]struct{}, len(req.ShardTops))
	for i := range tops {
		top := &tops[i]
		fields, err := parseShardDescriptorFields(top.Descriptor)
		if err != nil {
			return nil, 0, fmt.Errorf("%w: shard top %d descriptor: %v", ErrInvalidInput, i, err)
		}
		if fields.startLT >= fields.endLT {
			return nil, 0, fmt.Errorf("%w: shard top %d has an empty logical-time interval", ErrInvalidInput, i)
		}
		if fields.genUtime > req.Header.GenUtime {
			return nil, 0, fmt.Errorf("%w: shard top %d is newer than the masterchain block", ErrInvalidInput, i)
		}
		if !allowSameGenUtime && fields.genUtime == req.Header.GenUtime {
			return nil, 0, fmt.Errorf("%w: shard top %d is not older than the masterchain block", ErrInvalidInput, i)
		}

		var predecessorSeqno uint32
		for j := range top.Predecessors {
			predecessorSeqno = max(predecessorSeqno, top.Predecessors[j].SeqNo)
		}
		if top.Block.SeqNo <= predecessorSeqno || top.Block.SeqNo-predecessorSeqno > maxShardTopChainLength {
			return nil, 0, fmt.Errorf("%w: shard top %d chain length is outside 1..%d",
				ErrInvalidInput, i, maxShardTopChainLength)
		}
		chainLength := top.Block.SeqNo - predecessorSeqno
		if req.Config.capabilities&capCreateStats != 0 && uint32(len(top.Creators)) != chainLength {
			return nil, 0, fmt.Errorf("%w: shard top %d has %d creators, want %d",
				ErrInvalidInput, i, len(top.Creators), chainLength)
		}
		descriptor := top.TopBlockDescr
		if descriptor == nil || descriptor.IsSpecial() {
			return nil, 0, fmt.Errorf("%w: shard top %d TopBlockDescr is absent or special", ErrInvalidInput, i)
		}
		links, err := validateMasterTopBlockDescrBinding(descriptor, top.Block, chainLength)
		if err != nil {
			return nil, 0, fmt.Errorf("%w: shard top %d: %v", ErrInvalidInput, i, err)
		}
		descriptorHash := descriptor.HashKey()
		if _, duplicate := seenDescriptors[descriptorHash]; duplicate {
			return nil, 0, fmt.Errorf("%w: duplicate shard TopBlockDescr %x", ErrInvalidInput, descriptorHash)
		}
		seenDescriptors[descriptorHash] = struct{}{}

		if err = verifyMasterTopBlockDescrFields(links, top.Block, &fields); err != nil {
			return nil, 0, fmt.Errorf("%w: shard top %d: %v", ErrInvalidInput, i, err)
		}

		// A descriptor built from a block cannot carry these three: BlockInfo has no
		// before_merge and no split_merge_at, and the registration seqno is the
		// masterchain's own. before_merge is decided by the masterchain FSM and
		// an announcement may only be inherited from the ancestor by
		// inheritShardRegistryFSM, so both start neutral regardless of what the
		// acquisition pipeline supplied.
		fields.regMCSeqno = newMCSeqno
		fields.nextCCUpdated = false
		fields.beforeMerge = false
		fields.splitMerge = tlb.FutureSplitMergeNone{}
		top.Descriptor, err = storeMasterShardDescriptor(0xa, fields)
		if err != nil {
			return nil, 0, fmt.Errorf("serialize shard top %d descriptor: %w", i, err)
		}
		maxEndLT = max(maxEndLT, fields.endLT)
	}

	return tops, maxEndLT, nil
}

func (m *masterCollation) initValueFlow(config *Config, stats *tlb.ShardStateStats) error {
	fees, err := m.shardFees.LoadRootExtra()
	if err != nil {
		return fmt.Errorf("load imported shard fees: %w", err)
	}
	var imported tlb.ShardFeeCreated
	if err = loadExactSlice(&imported, fees); err != nil {
		return fmt.Errorf("decode imported shard fees: %w", err)
	}
	m.feesImported = imported.Fees
	m.importedCreated = imported.Create

	burnable, err := imported.Fees.Sub(imported.Create)
	if err != nil {
		return fmt.Errorf("%w: imported created value exceeds shard fees: %v", ErrInvalidInput, err)
	}
	m.importBurned, err = burnCurrency(config.burning, burnable)
	if err != nil {
		return err
	}
	netImported, err := imported.Fees.Sub(m.importBurned)
	if err != nil {
		return err
	}
	m.created = tlb.CurrencyCollection{Coins: config.masterchain.createFee}
	m.recovered, err = stats.TotalValidatorFees.Add(netImported)
	if err != nil {
		return err
	}
	m.recovered, err = m.recovered.Add(m.created)
	if err != nil {
		return err
	}

	// Both address lookups are epoch data; the two guards around them are not.
	// The one-TON floor reads this block's recovered fees, and the minted amount
	// below is a difference against the predecessor's global balance.
	if !config.fees.collector.ok || m.recovered.Coins.Nano().Cmp(big.NewInt(1_000_000_000)) < 0 {
		m.recovered = tlb.CurrencyCollection{}
	}
	m.minted, err = computeMinted(tlb.BlockchainConfig{Root: config.execution.Root()}, m.oldExtra.GlobalBalance)
	if err != nil {
		return err
	}
	if !currencyZero(m.minted) && !config.fees.minter.ok {
		m.minted = tlb.CurrencyCollection{}
	}

	return nil
}

func burnCurrency(config tlb.BurningConfig, value tlb.CurrencyCollection) (tlb.CurrencyCollection, error) {
	if config.FeeBurnDenom == 0 || config.FeeBurnNum > config.FeeBurnDenom {
		return tlb.CurrencyCollection{}, fmt.Errorf("%w: invalid fee burning fraction %d/%d",
			ErrInvalidInput, config.FeeBurnNum, config.FeeBurnDenom)
	}
	burned := new(big.Int).Mul(value.Coins.Nano(), new(big.Int).SetUint64(uint64(config.FeeBurnNum)))
	burned.Quo(burned, new(big.Int).SetUint64(uint64(config.FeeBurnDenom)))
	return tlb.CurrencyCollection{Coins: tlb.FromNanoTON(burned)}, nil
}

// computeMinted is the one configuration-derived value that deliberately stays
// per-block, and the reason is not that it depends on the block — the wanted
// amounts do not; only the delta against global does.
//
// The reason is the read set. deriveMasterConfigTransition runs PrepareConfig on
// the configuration root reached through the block's own accounts, under the
// block's read set. Every other epoch value in Config is derived from cells
// validateMasterConfigData or PrepareBlockchainConfig has already read there.
// The ENTRIES of parameter 7 are the exception: nothing on that path walks them,
// because GetExtraCurrencyToMint reads only the parameter cell and the
// dictionary root. Hoisting the LoadAll below into PrepareConfig would record
// entry cells the reference collator never reads, and on any chain that actually
// mints extra currencies the masterchain block bytes would change. Mainnet's
// parameter 7 is empty, so the defect would not show up here.
//
// It runs on config.execution.Root(), the untraced epoch root, on both the
// collation and the validation path. Rerouting it onto the in-state
// configuration root would change the proof.
func computeMinted(config tlb.BlockchainConfig, global tlb.CurrencyCollection) (tlb.CurrencyCollection, error) {
	wanted, err := config.GetExtraCurrencyToMint()
	if err != nil {
		// Config::get_extra_currency_config treats an absent or wholly invalid
		// parameter 7 as minting disabled. Once its outer collection decodes,
		// malformed individual amounts remain fatal below.
		return tlb.CurrencyCollection{}, nil
	}
	if wanted.ToMint == nil || wanted.ToMint.GetKeySize() != 32 {
		return tlb.CurrencyCollection{}, fmt.Errorf("%w: configured extra currency dictionary is malformed", ErrInvalidInput)
	}

	result := cell.NewDict(32)
	items, err := wanted.ToMint.LoadAll()
	if err != nil {
		return tlb.CurrencyCollection{}, fmt.Errorf("%w: load configured extra currencies: %v", ErrInvalidInput, err)
	}
	for _, item := range items {
		currencyID, loadErr := item.Key.LoadUInt(32)
		if loadErr != nil || item.Key.BitsLeft() != 0 || currencyID == 0 {
			if loadErr != nil {
				return tlb.CurrencyCollection{}, fmt.Errorf("%w: load extra currency id: %v", ErrInvalidInput, loadErr)
			}
			continue
		}
		wantedAmount, loadErr := item.Value.LoadVarUInt(32)
		if loadErr != nil || item.Value.BitsLeft() != 0 || item.Value.RefsNum() != 0 {
			return tlb.CurrencyCollection{}, fmt.Errorf("%w: decode configured currency %d", ErrInvalidInput, currencyID)
		}
		currentAmount, loadErr := extraCurrencyAmount(global.ExtraCurrencies, uint32(currencyID))
		if loadErr != nil {
			return tlb.CurrencyCollection{}, loadErr
		}
		delta := new(big.Int).Sub(wantedAmount, currentAmount)
		if delta.Sign() <= 0 {
			continue
		}
		if err = result.SetIntKey(new(big.Int).SetUint64(currencyID), cell.BeginCell().MustStoreBigVarUInt(delta, 32).EndCell()); err != nil {
			return tlb.CurrencyCollection{}, fmt.Errorf("store minted currency %d: %w", currencyID, err)
		}
	}
	if result.IsEmpty() {
		return tlb.CurrencyCollection{}, nil
	}
	return tlb.CurrencyCollection{ExtraCurrencies: result}, nil
}

func extraCurrencyAmount(dict *cell.Dictionary, id uint32) (*big.Int, error) {
	if dict == nil || dict.IsEmpty() {
		return new(big.Int), nil
	}
	value, err := dict.LoadValueByIntKey(new(big.Int).SetUint64(uint64(id)))
	if isMissingKey(err) {
		return new(big.Int), nil
	}
	if err != nil {
		return nil, fmt.Errorf("load global currency %d: %w", id, err)
	}
	amount, err := value.LoadVarUInt(32)
	if err != nil || value.BitsLeft() != 0 || value.RefsNum() != 0 {
		return nil, fmt.Errorf("%w: global currency %d is malformed", ErrInvalidInput, id)
	}
	return amount, nil
}

func (c *collation) prepareMasterCollatedEstimate() error {
	if !c.fullCollated {
		return nil
	}
	roots, err := c.masterCollatedPrefix(consensusExtraRoot(c.req.header.GenUtimeMS))
	if err != nil {
		return err
	}
	roots = append(roots, c.fullCollatedProofs...)
	boc, err := cell.ToBOCWithOptionsErr(roots, cell.BOCSerializeOptions{WithCRC32C: true})
	if err != nil {
		return fmt.Errorf("estimate fixed masterchain collated data: %w", err)
	}
	c.collatedFixedEstimate = uint64(len(boc))
	c.updateCollatedEstimate()
	return nil
}

func (c *collation) masterCollatedPrefix(consensus *cell.Cell) ([]*cell.Cell, error) {
	if len(c.master.shardTops) == 0 {
		return []*cell.Cell{consensus}, nil
	}
	set, err := buildMasterTopBlockDescrSet(c.master.shardTops)
	if err != nil {
		return nil, err
	}
	return []*cell.Cell{set, consensus}, nil
}

func (c *collation) processMasterTickTock(tock bool) error {
	addresses, err := c.masterSpecialAccounts()
	if err != nil {
		return err
	}

	if c.header.StartLt >= math.MaxInt64 {
		return fmt.Errorf("%w: tick/tock logical time exceeds signed TVM range", ErrInvalidInput)
	}
	requiredLT := c.header.StartLt + 1
	if tock {
		requiredLT = c.maxLT
	}
	if requiredLT > math.MaxInt64 {
		return fmt.Errorf("%w: tick/tock logical time exceeds signed TVM range", ErrInvalidInput)
	}

	for _, accountID := range addresses {
		if err = c.ctx.Err(); err != nil {
			return err
		}
		addr := address.NewAddress(0, 0xff, accountID[:])
		lane, accountErr := c.account(addr)
		if accountErr != nil {
			return fmt.Errorf("load masterchain special account %x: %w", accountID, accountErr)
		}
		state := lane.current.State()
		if state.Status != tlb.AccountStatusActive || state.StateInit == nil || state.StateInit.TickTock == nil {
			continue
		}
		if tock && !state.StateInit.TickTock.Tock || !tock && !state.StateInit.TickTock.Tick {
			continue
		}

		transactionLT := requiredLT
		if emittedLT, ok := c.lastDispatchEmitted[accountID]; ok {
			if emittedLT == math.MaxUint64 {
				return fmt.Errorf("%w: tick/tock logical time overflow", ErrInvalidInput)
			}
			transactionLT = max(transactionLT, emittedLT+1)
		}
		if transactionLT > math.MaxInt64 {
			return fmt.Errorf("%w: tick/tock logical time exceeds signed TVM range", ErrInvalidInput)
		}

		result, emulateErr := c.builder.machine.EmulateTickTockTransaction(c.blockCtx, lane.current, tock, tvm.TransactionOptions{
			LogicalTime:        int64(transactionLT),
			AccountStorageStat: lane.storageStat,
			OnCellLoad:         c.recordExecutionRead,
		})
		if emulateErr != nil {
			return fmt.Errorf("execute %s transaction for %x: %w", tickTockName(tock), accountID, emulateErr)
		}
		if err = c.ctx.Err(); err != nil {
			return err
		}
		// Block-limit accounting excludes gas for every tick/tock transaction.
		// Gas is still charged by TVM and remains in the serialized transaction
		// and value flow.
		if err = c.commitExecution(result, lane, false); err != nil {
			return err
		}
		if err = c.registerOutputs(result, lane, nil, true); err != nil {
			return err
		}
		c.updatePeakLoad()
	}

	return nil
}

func (c *collation) processMasterSpecials() error {
	fees := c.config.fees

	if !currencyZero(c.master.recovered) {
		if !fees.collector.ok {
			return fmt.Errorf("%w: fee collector address is malformed", ErrInvalidInput)
		}
		collector := fees.collector.addr
		message, err := c.processMasterSpecial(c.master.recovered, collector[:])
		if err != nil {
			return fmt.Errorf("create fee recovery transaction: %w", err)
		}
		c.master.recoverCreateMsg = message
	}

	if !currencyZero(c.master.minted) {
		if !fees.minter.ok {
			return fmt.Errorf("%w: minter address is malformed", ErrInvalidInput)
		}
		minter := fees.minter.addr
		message, err := c.processMasterSpecial(c.master.minted, minter[:])
		if err != nil {
			return fmt.Errorf("create mint transaction: %w", err)
		}
		c.master.mintMsg = message
	}

	return nil
}

// processMasterSpecial delivers one masterchain recover/mint message in-block,
// hand-rolling the msg_import_imm$011 pair rather than routing through
// deliverImmediate. That is not an accident of structure and must not be
// "de-duplicated": the reference exempts exactly these two messages from the
// processed bound, at collator.cpp:3667 `if (!is_special && ...)`, where
// is_special is non-null only for create_special_transaction (collator.cpp:3161,
// :3175-3178). Their created_lt is start_lt, ahead of every queue entry this
// block can import, so advancing the bound with one would make the next import
// fail the strict order check in advanceProcessedBound. Routing specials through
// deliverImmediate would therefore break every masterchain block that mints or
// recovers fees AND imports an internal message.
func (c *collation) processMasterSpecial(amount tlb.CurrencyCollection, destination []byte) (*cell.Cell, error) {
	zero := [32]byte{}
	message := tlb.InternalMessage{
		IHRDisabled:     true,
		Bounce:          true,
		SrcAddr:         address.NewAddress(0, 0xff, zero[:]),
		DstAddr:         address.NewAddress(0, 0xff, destination),
		Amount:          amount.Coins,
		ExtraCurrencies: amount.ExtraCurrencies,
		CreatedLT:       c.header.StartLt,
		CreatedAt:       c.header.GenUtime,
		Body:            cell.BeginCell().EndCell(),
	}
	root, err := message.ToCell()
	if err != nil {
		return nil, fmt.Errorf("serialize system message: %w", err)
	}
	prepared, err := tvm.PrepareMessage(root)
	if err != nil {
		return nil, fmt.Errorf("prepare system message: %w", err)
	}
	lane, err := c.account(message.DstAddr)
	if err != nil {
		return nil, err
	}
	if c.header.StartLt >= math.MaxInt64 {
		return nil, fmt.Errorf("%w: special transaction logical time overflow", ErrInvalidInput)
	}
	transactionLT := c.header.StartLt + 1
	var accountID [32]byte
	copy(accountID[:], destination)
	if emittedLT, ok := c.lastDispatchEmitted[accountID]; ok {
		if emittedLT == math.MaxUint64 {
			return nil, fmt.Errorf("%w: special transaction logical time overflow", ErrInvalidInput)
		}
		transactionLT = max(transactionLT, emittedLT+1)
	}
	if transactionLT > math.MaxInt64 {
		return nil, fmt.Errorf("%w: special transaction logical time overflow", ErrInvalidInput)
	}
	result, err := c.builder.machine.EmulateTransaction(c.blockCtx, lane.current, prepared, tvm.TransactionOptions{
		LogicalTime:        int64(transactionLT),
		AccountStorageStat: lane.storageStat,
		OnCellLoad:         c.recordExecutionRead,
	})
	if err != nil {
		return nil, fmt.Errorf("execute system message: %w", err)
	}
	if err = c.ctx.Err(); err != nil {
		return nil, err
	}
	// Since global version 5 a special transaction receives the full special
	// gas allowance, so its gas is deliberately excluded from block limits.
	if err = c.commitExecution(result, lane, c.config.globalVersion < 5); err != nil {
		return nil, err
	}

	envelope, err := (tlb.MsgEnvelope{
		CurAddr:         tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular, UseDestBits: routingAddressBits},
		NextAddr:        tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular, UseDestBits: routingAddressBits},
		FwdFeeRemaining: message.FwdFee,
		Msg:             root,
	}).ToCell()
	if err != nil {
		return nil, fmt.Errorf("serialize system message envelope: %w", err)
	}
	in, err := descriptorFee(0b011, 3, envelope, result.TransactionCell, message.FwdFee) // msg_import_imm$011
	if err != nil {
		return nil, err
	}
	if err = c.insert(c.inMessages.AugmentedDictionary, &c.inDescr, root, in); err != nil {
		return nil, err
	}
	if err = c.registerOutputs(result, lane, nil, true); err != nil {
		return nil, err
	}
	c.updatePeakLoad()

	return in, nil
}

// masterSpecialAccounts is the fundamental-contract list of this config epoch,
// in the order tick and tock execute. It is derived once when the configuration
// is prepared; what stays per-block is in the caller — which of these accounts
// is active, carries a tick/tock state init, and at which logical time.
func (c *collation) masterSpecialAccounts() ([][32]byte, error) {
	if c.config.specials.err != nil {
		return nil, c.config.specials.err
	}
	return c.config.specials.ordered, nil
}

// masterSpecialAccountIDs is the same list as a membership set.
func (c *collation) masterSpecialAccountIDs() (map[[32]byte]struct{}, error) {
	if c.config.specials.err != nil {
		return nil, c.config.specials.err
	}
	return c.config.specials.set, nil
}

func tickTockName(tock bool) string {
	if tock {
		return "tock"
	}
	return "tick"
}

// masterFeeTotals is the fee/burn chain a masterchain block's value flow is
// built from and validated against. Collation computes it to fill the flow;
// validation computes it to compare — the two must not drift, so they share
// this one derivation.
type masterFeeTotals struct {
	feesCollected  tlb.CurrencyCollection
	ordinaryBurned tlb.CurrencyCollection
}

func (m *masterCollation) feeTotals(
	config *Config,
	transactionFees tlb.CurrencyCollection,
	importFees tlb.ImportFees,
) (masterFeeTotals, error) {
	ordinaryFees, err := transactionFees.Add(tlb.CurrencyCollection{Coins: importFees.FeesCollected})
	if err != nil {
		return masterFeeTotals{}, fmt.Errorf("add masterchain transaction and import fees: %w", err)
	}
	ordinaryBurned, err := burnCurrency(config.burning, ordinaryFees)
	if err != nil {
		return masterFeeTotals{}, fmt.Errorf("derive burned masterchain block fees: %w", err)
	}
	netOrdinaryFees, err := ordinaryFees.Sub(ordinaryBurned)
	if err != nil {
		return masterFeeTotals{}, fmt.Errorf("subtract burned masterchain block fees: %w", err)
	}
	netShardFees, err := m.feesImported.Sub(m.importBurned)
	if err != nil {
		return masterFeeTotals{}, fmt.Errorf("subtract burned imported shard fees: %w", err)
	}
	feesCollected, err := m.created.Add(netShardFees)
	if err != nil {
		return masterFeeTotals{}, fmt.Errorf("add masterchain creation and imported shard fees: %w", err)
	}
	feesCollected, err = feesCollected.Add(netOrdinaryFees)
	if err != nil {
		return masterFeeTotals{}, fmt.Errorf("add masterchain ordinary fees: %w", err)
	}

	return masterFeeTotals{feesCollected: feesCollected, ordinaryBurned: ordinaryBurned}, nil
}
