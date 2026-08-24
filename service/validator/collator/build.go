package collator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
	"github.com/xssnick/gton/service/validator/simplex"
)

const (
	capCreateStats          = uint64(1 << 1)
	capBounceMsgBody        = uint64(1 << 2)
	capReportVersion        = uint64(1 << 3)
	capShortDequeue         = uint64(1 << 5)
	capStoreOutMsgQueueSize = uint64(1 << 6)
	capMsgMetadata          = uint64(1 << 7)
	capDeferMessages        = uint64(1 << 8)
	capFullCollatedData     = uint64(1 << 9)
	logicalTimeAlignment    = uint64(1_000_000)
	maxOutMsgQueueSize      = uint64(1<<48 - 1)
	candidateBOCHeadroom    = uint64(32)
	consensusExtraDataTag   = uint64(0x638eb292)
)

const supportedSoftwareVersion = uint32(15)

// SupportedSoftware reports the block software version and capabilities this
// builder emits. The list is part of the protocol contract: a capability
// claimed here must actually be honoured by collation, and one that is honoured
// but not claimed makes the block unacceptable to peers.
func SupportedSoftware() tlb.GlobalVersion {
	return tlb.GlobalVersion{
		Version: supportedSoftwareVersion,
		Capabilities: capCreateStats | capBounceMsgBody | capReportVersion |
			capShortDequeue | capStoreOutMsgQueueSize | capMsgMetadata |
			capDeferMessages | capFullCollatedData,
	}
}

type collation struct {
	// waves is the inbound-internal wave loop's reusable state: the plan arena,
	// the wave map and the worker pool, built once per collation instead of once
	// per wave.
	waves         waveState
	externalWaves externalWaveState
	// outboundVisited is recordOutboundMessageReads' dedup set, kept across the
	// messages of a collation rather than allocated per message. Only the retire
	// goroutine walks emitted messages, so one scratch set serves them all.
	outboundVisited map[cell.Hash]struct{}

	ctx      context.Context
	builder  *Builder
	req      collationRequest
	header   tlb.BlockHeader
	shard    msgpool.ShardIdent
	topology shardTopology
	config   *Config
	master   *masterCollation

	usage               *cell.ReadSet
	shardEndLT          *shardEndLTResolver
	oldRoot             *cell.Cell
	oldState            tlb.ShardStateUnsplit
	oldStats            tlb.ShardStateStats
	accountSources      [2]predecessorAccountSource
	dispatchSources     [2]predecessorDispatchSource
	dispatchSourceCount int
	blockCtx            *tvm.BlockContext
	limits              *blockLimitStatus
	hardLTDelta         uint64
	peakLoad            LoadClass
	queueSize           uint64
	// mediumMark raises this block's full mark from the soft boundary to the
	// medium one. See escalateToMediumMark.
	mediumMark bool
	// blockFullTimeout records that blockFull was latched by the internal
	// message timeout rather than by a limit, which is the one reason a raised
	// mark may not clear.
	blockFullTimeout bool
	// internalsCursor is the index of the first inbound internal this block has
	// not imported, so a later pass can resume the queue's canonical order
	// exactly where the first one stopped.
	internalsCursor int
	// toppedUp makes the top-up one-shot; see topUpInternals.
	toppedUp     bool
	oldQueueSize uint64
	queueOps     uint64
	dispatchOps  uint64
	inDescr      descriptorBatch
	outDescr     descriptorBatch

	accounts *tlb.ShardAccountsAugDict
	// accountMutationDiff is the exact closure captured by the one final
	// ShardAccounts bulk write. A destroyed account keeps this nil because its
	// later individual delete cannot be composed with that receipt without
	// retaining an intermediate Patricia shape; the post-state closure then uses
	// the canonical full diff for that rare block.
	accountMutationDiff *cell.AugmentedDictionaryDiff
	oldOutQueue         *tlb.OutMsgQueueAugDict
	outQueue            *tlb.OutMsgQueueAugDict
	// queuePendingDelete holds out-queue removals not yet applied; they are
	// flushed together before each root sample and at finish.
	queuePendingDelete  []*cell.Cell
	queuePendingSet     map[msgpool.QueueKey]struct{}
	processed           *cell.Dictionary
	processedMinMC      uint32
	inMessages          *tlb.InMsgDescrAugDict
	outMessages         *tlb.OutMsgDescrAugDict
	accountBlocks       *tlb.ShardAccountBlocksAugDict
	oldDispatchQueue    *tlb.DispatchQueueAugDict
	oldDispatchAccounts map[[32]byte]*tlb.AccountDispatchQueue
	dispatchQueue       *tlb.DispatchQueueAugDict
	dispatchChanged     map[[32]byte]struct{}

	lanes map[[32]byte]*accountLane
	// accountStorageProofs holds one traced storage-stat proof builder per
	// initial dictionary hash, shared by every account that binds it. See
	// sharedAccountStorageProof for why the sharing is what makes the emitted
	// proof serve all of them.
	accountStorageProofs map[cell.Hash]*accountStorageProof
	accountSiblings      *accountSiblingPrefetch
	new                  newMessageHeap
	prewarmedAccounts    map[prewarmAccountKey]struct{}

	senderGenerated              map[[32]byte]uint32
	lastDispatchEmitted          map[[32]byte]uint64
	unprocessedDeferred          map[[32]byte]uint32
	haveUnprocessedDispatchQueue bool

	// lastProcLT and lastProcHash track the newest processed inbound internal
	// message (queue imports and immediate deliveries) for the ProcessedUpto
	// bound; blockFull records that the normal limit class stopped inbound
	// processing, forcing the remaining generated messages into the queue.
	lastProcLT   uint64
	lastProcHash [32]byte
	blockFull    bool
	// processedClaim is the (lt, hash) bound updateProcessedInfo wrote into the
	// candidate's ProcessedInfo, and processedClaimed says the dictionary moved
	// because of it. Only the validation-closure replay reads them: the scan a
	// validator runs over the predecessor outbound queue is bounded by exactly
	// this pair, so the proof has to be widened to exactly this pair.
	processedClaim   semanticMessageBound
	processedClaimed bool
	localCleanup     *localCleanupFrontier
	// claimedPrefix hands the cells cleanupClaimedLocalDequeues parsed over that
	// same prefix to the closure, which is the only pass allowed to record them.
	claimedPrefix claimedPrefixCells
	// processedRecords caches the parsed parent ProcessedInfo entries; both
	// constructors seed it from the parse they already do for processedMinMC.
	// updateProcessedInfo hands the slice to InsertProcessedUpto and
	// CompactifyProcessedUpto, which sort and compact it in place, so a reader
	// added after that phase sees the compacted records rather than the parent's.
	processedRecords []tlb.ProcessedUptoRecord
	processedParsed  bool

	maxLT        uint64
	stats        Stats
	externals    []msgpool.ExternalFeedback
	storageStats AccountStorageStats
	burned       tlb.CurrencyCollection

	// fullCollated is capFullCollatedData as it stood when this collation was
	// prepared. It is a field rather than a config read because it also has to
	// answer for collations assembled directly in tests, which carry no config,
	// and because the capability must not be able to change mid-collation.
	fullCollated        bool
	previousBlockProofs [2]*cell.Cell
	fullCollatedProofs  []*cell.Cell
	// collatedProofEstimate accumulates the previous-state proof the shard
	// candidate will carry. Only the shard path installs the read-set record
	// callback that feeds it (see prepareMaster), so the masterchain path leaves
	// it nil and size() answers zero for it.
	collatedProofEstimate *proofSizeEstimator
	collatedFixedEstimate uint64
	// updateMemoCells is how many cells the state update's destination walk
	// memoised, carried from where the update is built to finish(), which is
	// where the builder's hints are updated — a build that never produced a
	// candidate must not resize the next one.
	updateMemoCells int

	// pace carries the wall-clock spans the split decision consults. Only the
	// live schedule installs one; a zero value disables the heuristic, which is
	// what every deterministic entry point leaves in place.
	pace collationPace

	// immediateQueueKeys are the outbound-queue keys of messages delivered in
	// this block without ever being queued. The validator proves each of them
	// absent from both queues, and collation has no other reason to touch those
	// paths, so they are collected here and replayed into the proof closure
	// once the block is built. Only full collated data needs them.
	immediateQueueKeys []msgpool.QueueKey
}

// BuildShard constructs a shardchain candidate from selected external and
// internal messages. It preserves inbound progress and queues generated
// internals.
func (b *Builder) BuildShard(ctx context.Context, req ShardRequest) (*Candidate, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if req.Masterchain.Config == nil || req.Masterchain.Groups == nil {
		return nil, fmt.Errorf("%w: masterchain config or group snapshot is absent", ErrInvalidInput)
	}
	if req.FullCollatedProofs != nil && req.Masterchain.Config.capabilities&capFullCollatedData == 0 {
		return nil, fmt.Errorf("%w: full collated proof provider supplied without capability", ErrInvalidInput)
	}
	common := shardCollationRequest(req)
	if err := validateCollationRequest(&common); err != nil {
		return nil, err
	}
	candidate, err := retryUnderSizeLimit(ctx, func(attempt collationAttempt) (*Candidate, error) {
		return b.buildShardAttempt(ctx, req, attempt)
	})
	if err != nil {
		// A block that was handed over and then failed to finish leaves a
		// successor speculating on a slot that will produce nothing.
		req.successor.revokeOffered(PipelineHandoffAbandonedFailed)

		return candidate, err
	}
	if err = sealBuiltCandidate(candidate); err != nil {
		return nil, err
	}

	return candidate, nil
}

func (b *Builder) buildShardAttempt(ctx context.Context, req ShardRequest, attempt collationAttempt) (*Candidate, error) {
	return b.buildShardAttemptPaced(ctx, req, attempt, collationPace{})
}

func (b *Builder) buildShardAttemptPaced(
	ctx context.Context,
	req ShardRequest,
	attempt collationAttempt,
	pace collationPace,
) (*Candidate, error) {
	if attempt.index > 0 {
		// This block is being rebuilt, so the root the previous attempt handed
		// over is one no candidate will carry. Withdraw it, and hand nothing over
		// from here: a rebuild is already the slot's last chance, and a successor
		// speculating on it would be competing with it for the same cores.
		req.successor.revokeOffered(PipelineHandoffAbandonedRetry)
		req.successor = nil
	}
	c, err := b.prepareShardPhases(ctx, req, attempt)
	if err != nil {
		return nil, err
	}
	defer c.stopWaves()
	c.pace = pace
	timer := c.req.assembly.start(CollationStageExecuteExternalMessages)
	defer func() {
		timer.stop()
	}()
	// The stage is still entered and still measured when the attempt declines
	// externals, because the assembly guard asks whether every stage was reached
	// and an attempt that skipped one would read as a broken build rather than a
	// narrowed one.
	if !attempt.skipExternals() {
		if err = c.processExternals(); err != nil {
			return nil, err
		}
	}
	timer.stop()

	timer = c.req.assembly.start(CollationStageExecuteInternalMessages)
	// Only on the first attempt. A size retry exists to make the block smaller,
	// and it replays what attempt zero admitted; topping it up again would grow
	// the very block the retry was called to shrink.
	if attempt.index == 0 {
		if err = c.topUpInternals(); err != nil {
			return nil, err
		}
	}
	if err = c.processNewMessages(c.blockFull || c.haveUnprocessedDispatchQueue || req.internalsIncomplete()); err != nil {
		return nil, err
	}
	timer.stop()

	return c.finishShard()
}

func (b *Builder) prepareShardPhases(
	ctx context.Context,
	req ShardRequest,
	attempt collationAttempt,
) (*collation, error) {
	timer := req.assembly.start(CollationStagePrepareState)
	defer func() {
		timer.stop()
	}()
	// req is this attempt's own copy, so the index reaches the dispatch queue
	// without any attempt writing into the caller's request. Assigned every
	// attempt and not only the ones that concede something: the field is exported
	// on an operator-supplied policy, and leaving a non-zero value in place would
	// let a composition root switch the dispatch tail off for every block a node
	// ever produces, with nothing downstream to notice.
	req.Dispatch.AttemptIndex = uint32(attempt.index)
	c, err := b.prepare(ctx, req)
	if err != nil {
		return nil, err
	}
	c.limits.applyAttempt(attempt)
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if err = c.limits.addProof(c.outQueue.RootCell()); err != nil {
		return nil, fmt.Errorf("initial outbound queue proof: %w", err)
	}
	if err = c.limits.addProof(c.dispatchQueue.RootCell()); err != nil {
		return nil, fmt.Errorf("initial dispatch queue proof: %w", err)
	}
	timer.stop()

	// The canonical phase order: cleanup of already-delivered own-queue
	// entries, dispatch queue, inbound internal messages, then externals and
	// newly-generated messages.
	timer = c.req.assembly.start(CollationStageCleanupOutQueue)
	if err = c.cleanupOutQueue(); err != nil {
		return nil, err
	}
	timer.stop()

	timer = c.req.assembly.start(CollationStageExecuteInternalMessages)
	if err = c.processDispatchQueue(); err != nil {
		return nil, err
	}
	if err = c.processInternals(); err != nil {
		return nil, err
	}
	timer.stop()

	return c, nil
}

func (c *collation) finishShard() (*Candidate, error) {
	timer := c.req.assembly.start(CollationStageProcessedInfo)
	defer func() {
		timer.stop()
	}()
	if err := c.updateProcessedInfo(); err != nil {
		return nil, err
	}
	timer.stop()
	timer = c.req.assembly.start(CollationStageClaimedLocalCleanup)
	if err := c.cleanupClaimedLocalDequeues(); err != nil {
		return nil, err
	}
	timer.stop()

	timer = c.req.assembly.start(CollationStageFinalizeAccounts)
	if err := c.finishAccounts(); err != nil {
		return nil, err
	}
	timer.stop()

	return c.finish()
}

func validateCollationRequest(req *collationRequest) error {
	if req.randSeed == ([32]byte{}) {
		return fmt.Errorf("%w: random seed is zero", ErrInvalidInput)
	}
	if len(req.externals) > 0 && req.maxExternalAttempts <= 0 {
		return fmt.Errorf("%w: external attempt limit must be positive", ErrInvalidInput)
	}
	if err := validateExternalInputs(req.externals); err != nil {
		return err
	}
	if err := validateInternalInputs(req.internalMessages()); err != nil {
		return err
	}
	if len(req.neighbors) > 0 && req.neighborShardEndLT == nil {
		// Neighbor records are unusable without the shard end lt binding: the
		// coverage check cannot decide messages generated outside a record's shard.
		return fmt.Errorf("%w: neighbor processed records require a shard end lt resolver", ErrInvalidInput)
	}
	if err := validateNeighbors(req.neighbors); err != nil {
		return err
	}
	if err := validateStorageStats(req.storageStats); err != nil {
		return err
	}
	if req.dispatch.DeferringEnabled && req.dispatch.DeferMessagesAfter == 0 {
		return fmt.Errorf("%w: defer-messages-after must be positive when message deferring is enabled", ErrInvalidInput)
	}
	if req.dispatch.Phase3AdaptivePerInitiator && req.dispatch.Phase3MaxPerInitiator != 0 {
		return fmt.Errorf("%w: adaptive and explicit phase-3 per-initiator dispatch limits are mutually exclusive", ErrInvalidInput)
	}

	return nil
}

func (b *Builder) prepare(ctx context.Context, req ShardRequest) (*collation, error) {
	usage, err := openPredecessorReadSet(req, b.readSetHint())
	if err != nil {
		return nil, err
	}
	storageCells, storageProofCells := b.storageHints()
	fullCollated := req.Masterchain.Config.capabilities&capFullCollatedData != 0
	var collatedProofEstimate *proofSizeEstimator
	if fullCollated {
		// The estimator and the callback are one unit: every insert into the
		// read record has to report to this estimator, because the collated
		// proof is selected from what the estimator was told. An estimator
		// opened without the callback would silently lose those hashes from the
		// proof, which peers report back as a pruned branch while the block
		// still verifies locally. collated_proof_selection_test.go pins it.
		collatedProofEstimate = newProofSizeEstimator(b.readSetHint())
		usage.SetRecordCallback(collatedProofEstimate.addLoadedCell)
	}
	predecessor, err := preparePredecessor(req, usage)
	if err != nil {
		return nil, err
	}
	oldRoot := predecessor.oldRoot
	oldState := predecessor.state
	oldStats := predecessor.stats
	queueInfo := predecessor.queueInfo
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	header, err := b.makeHeader(req, &predecessor)
	if err != nil {
		return nil, err
	}
	if err = validateBlockTime(header, &oldState, req.Masterchain.GenUtime, req.Masterchain.Config.globalVersion); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err = validatePredecessorMasterchainReference(req.Previous.ID, predecessor.firstMasterRef, header.MasterRef); err != nil {
		return nil, err
	}
	if predecessor.secondMasterRef != nil {
		if err = validatePredecessorMasterchainReference(req.Previous2.ID, predecessor.secondMasterRef, header.MasterRef); err != nil {
			return nil, err
		}
	}
	if err = bindShardTopologySession(req, &predecessor.topology); err != nil {
		return nil, err
	}
	if req.Masterchain.Config.capabilities&capFullCollatedData != 0 {
		if err = verifyFullCollatedNeighborSet(req.Masterchain, header.Shard, req.Neighbors); err != nil {
			return nil, err
		}
	}
	if req.Masterchain.Groups.LastKeyBlockSeqno > req.Masterchain.ID.SeqNo {
		return nil, fmt.Errorf("%w: previous key block is ahead of the masterchain context", ErrInvalidInput)
	}
	header.GenValidatorListHashShort = predecessor.topology.session.ValidatorSetHash
	header.GenCatchainSeqno = predecessor.topology.session.CatchainSeqno
	header.PrevKeyBlockSeqno = req.Masterchain.Groups.LastKeyBlockSeqno

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var dispatchQueue *tlb.DispatchQueueAugDict
	if queueInfo.Extra == nil {
		dispatchQueue, err = tlb.NewDispatchQueueAugDict()
		if err != nil {
			return nil, fmt.Errorf("create dispatch queue: %w", err)
		}
	} else {
		dispatchQueue = &tlb.DispatchQueueAugDict{
			AugmentedDictionary: queueInfo.Extra.DispatchQueue.Copy(),
		}
	}

	owner := msgpool.ShardIdent{
		Workchain: header.Shard.WorkchainID,
		Shard:     uint64(header.Shard.GetShardID()),
	}
	processedRecords, err := tlb.LoadProcessedUptoRecords(queueInfo.ProcInfo, owner.Shard)
	if err != nil {
		return nil, fmt.Errorf("%w: decode processed info: %v", ErrInvalidInput, err)
	}
	processedMinMC := minRecordedMCSeqno(processedRecords)

	queueSize := predecessor.queueSize

	limits, err := blockLimitsAtTime(
		req.Masterchain.Config.basechain.limits,
		oldState.GenUTime,
		header.GenUtime,
	)
	if err != nil {
		return nil, err
	}
	hardLTDelta := limits.ltDelta[3]
	blockCtx, err := req.Masterchain.Config.execution.NewBlockContext(tvm.BlockOptions{
		Now:        header.GenUtime,
		BlockLT:    int64(header.StartLt),
		RandSeed:   req.RandSeed[:],
		PrevBlocks: req.Masterchain.PrevBlocks,
		GlobalID:   oldState.GlobalID,
		Libraries:  req.Masterchain.Libraries,
	})
	if err != nil {
		return nil, fmt.Errorf("prepare transaction context: %w", err)
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}

	accounts := &tlb.ShardAccountsAugDict{AugmentedDictionary: oldState.Accounts.ShardAccounts.Copy()}
	outQueue := &tlb.OutMsgQueueAugDict{AugmentedDictionary: queueInfo.OutQueue.Copy()}
	inMessages, err := tlb.NewInMsgDescrAugDict(req.Masterchain.Config.globalVersion)
	if err != nil {
		return nil, err
	}
	outMessages, err := tlb.NewOutMsgDescrAugDict(req.Masterchain.Config.globalVersion)
	if err != nil {
		return nil, err
	}
	accountBlocks, err := tlb.NewShardAccountBlocksAugDict()
	if err != nil {
		return nil, err
	}

	maxLT := header.StartLt + 1

	c := &collation{
		ctx:                 ctx,
		builder:             b,
		req:                 shardCollationRequest(req),
		header:              header,
		shard:               owner,
		topology:            predecessor.topology,
		config:              req.Masterchain.Config,
		usage:               usage,
		shardEndLT:          newShardEndLTResolver(req.NeighborShardEndLT),
		oldRoot:             oldRoot,
		oldState:            oldState,
		oldStats:            oldStats,
		accountSources:      predecessor.accountSources,
		dispatchSources:     predecessor.dispatchSources,
		dispatchSourceCount: predecessor.dispatchSourceCount,
		blockCtx:            blockCtx,
		limits:              newBlockLimitStatus(limits, header.StartLt, usage, storageCells, storageProofCells),
		hardLTDelta:         hardLTDelta,
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
		dispatchQueue:         dispatchQueue,
		lanes:                 make(map[[32]byte]*accountLane),
		senderGenerated:       make(map[[32]byte]uint32),
		lastDispatchEmitted:   make(map[[32]byte]uint64),
		unprocessedDeferred:   make(map[[32]byte]uint32),
		maxLT:                 maxLT,
		externals:             make([]msgpool.ExternalFeedback, 0, len(req.Externals)),
		fullCollated:          fullCollated,
		collatedProofEstimate: collatedProofEstimate,
	}
	c.prepareAccountPathRecorder()
	if fullCollated {
		if err = c.prepareFullCollatedProofs(); err != nil {
			return nil, err
		}
		fixedRoots := []*cell.Cell{consensusExtraRoot(req.Header.GenUtimeMS)}
		for i := 0; i < c.previousCount(); i++ {
			previous := c.previousAt(i)
			if previous.ID.SeqNo == 0 {
				if !bytes.Equal(previous.ID.RootHash, previous.State.WithoutTrace().Hash()) {
					return nil, fmt.Errorf("%w: previous zerostate root differs from its block id", ErrInvalidInput)
				}
				continue
			}
			c.previousBlockProofs[i], err = c.buildPreviousBlockStateProof(previous)
			if err != nil {
				return nil, err
			}
			fixedRoots = append(fixedRoots, c.previousBlockProofs[i])
		}
		fixedRoots = append(fixedRoots, c.fullCollatedProofs...)
		fixedSize, err := collatedBOCSize(fixedRoots)
		if err != nil {
			return nil, fmt.Errorf("estimate fixed collated data: %w", err)
		}
		c.collatedFixedEstimate = fixedSize
		c.updateCollatedEstimate()
	}

	return c, nil
}

func validateExternalInputs(inputs []ExternalInput) error {
	seen := make(map[[32]byte]struct{}, len(inputs))
	for i := range inputs {
		input := &inputs[i]
		if input.message == nil {
			return fmt.Errorf("%w: external %d has no prepared message", ErrInvalidInput, i)
		}
		if input.Ref.Generation == 0 {
			return fmt.Errorf("%w: external %d has no selection generation", ErrInvalidInput, i)
		}

		hash := input.message.Cell().HashKey()
		if input.Ref.Hash != hash {
			return fmt.Errorf("%w: external %d reference does not match its message", ErrInvalidInput, i)
		}
		if _, exists := seen[hash]; exists {
			return fmt.Errorf("%w: duplicate external %x", ErrInvalidInput, hash)
		}
		seen[hash] = struct{}{}
	}

	return nil
}

func validateStorageStats(stats AccountStorageStats) error {
	for hash, root := range stats {
		if root == nil || root.HashKey() != hash {
			return fmt.Errorf("%w: account storage stat %x does not match its content hash", ErrInvalidInput, hash)
		}
	}

	return nil
}

func (b *Builder) makeHeader(req ShardRequest, predecessor *preparedPredecessor) (tlb.BlockHeader, error) {
	topology := &predecessor.topology
	if topology.target.WorkchainID != 0 {
		return tlb.BlockHeader{}, fmt.Errorf("%w: only basechain blocks are supported", ErrUnsupported)
	}
	if err := verifyBasechainWorkchain(req.Masterchain.Config, req.Header.GenUtime); err != nil {
		return tlb.BlockHeader{}, err
	}

	previous, err := blockReference(req.Previous.ID, predecessor.firstGenLT)
	if err != nil {
		return tlb.BlockHeader{}, fmt.Errorf("%w: previous block id: %v", ErrInvalidInput, err)
	}
	var previous2 *tlb.ExtBlkRef
	if req.Previous2 != nil {
		second, secondErr := blockReference(req.Previous2.ID, predecessor.secondGenLT)
		if secondErr != nil {
			return tlb.BlockHeader{}, fmt.Errorf("%w: second previous block id: %v", ErrInvalidInput, secondErr)
		}
		previous2 = &second
	}

	masterchain, err := blockReference(req.Masterchain.ID, req.Masterchain.EndLT)
	if err != nil {
		return tlb.BlockHeader{}, fmt.Errorf("%w: masterchain block id: %v", ErrInvalidInput, err)
	}
	if req.Masterchain.ID.Workchain != address.MasterchainID || req.Masterchain.ID.Shard != math.MinInt64 {
		return tlb.BlockHeader{}, fmt.Errorf("%w: reference block is not a masterchain block", ErrInvalidInput)
	}

	var header tlb.BlockHeader
	header.NotMaster = true
	header.AfterMerge = topology.kind == topologyAfterMerge
	header.BeforeSplit = req.BeforeSplit
	header.AfterSplit = topology.kind == topologyAfterSplit
	header.SeqNo = topology.seqno
	if req.Masterchain.VertSeqno < topology.previousVertSeqno {
		return tlb.BlockHeader{}, fmt.Errorf("%w: masterchain vertical seqno precedes the previous state", ErrInvalidInput)
	}
	header.VertSeqNo = req.Masterchain.VertSeqno
	header.Shard = topology.target
	header.GenUtime = req.Header.GenUtime
	if req.Header.GenUtimeMS/1000 != uint64(header.GenUtime) {
		return tlb.BlockHeader{}, fmt.Errorf("%w: millisecond generation time is outside the block second", ErrInvalidInput)
	}
	header.StartLt, err = nextBlockStartLT(topology.previousGenLT, req.Masterchain.EndLT)
	if err != nil {
		return tlb.BlockHeader{}, err
	}
	header.MasterRef = &masterchain
	header.PrevRef = tlb.BlkPrevInfo{Prev1: previous, Prev2: previous2}
	if req.Masterchain.Config.capabilities&capReportVersion != 0 {
		header.Flags = 1
		software := b.software
		header.GenSoftware = &software
	}
	return header, nil
}

func blockReference(id ton.BlockIDExt, endLT uint64) (tlb.ExtBlkRef, error) {
	if len(id.RootHash) != 32 || len(id.FileHash) != 32 {
		return tlb.ExtBlkRef{}, errors.New("root and file hashes must be 256 bits")
	}
	return tlb.ExtBlkRef{
		EndLt:    endLT,
		SeqNo:    id.SeqNo,
		RootHash: id.RootHash,
		FileHash: id.FileHash,
	}, nil
}

// requireGenUtimeMonotonic is the single copy of the gen_utime monotonicity
// rule: from global version 13 the candidate may repeat its bound's second,
// below it the second must strictly advance. Shard and masterchain collation
// and validation all gate on it, and a version bump applied to only some of
// them would produce a collator emitting blocks its own validator rejects, so
// the comparison lives here once. subject names the bound in the rejection.
//
// The bound itself stays at each call site: shard uses max(predecessor,
// masterchain reference) while masterchain uses the predecessor alone, which
// mirrors the C++ split between Collator::init_utime and ValidateQuery.
func requireGenUtimeMonotonic(globalVersion, genUtime, minGenUtime uint32, subject string) error {
	if globalVersion >= 13 {
		if genUtime < minGenUtime {
			return fmt.Errorf("%w: generation time precedes %s", ErrInvalidInput, subject)
		}
		return nil
	}
	if genUtime <= minGenUtime {
		return fmt.Errorf("%w: generation time does not follow %s", ErrInvalidInput, subject)
	}

	return nil
}

func validateBlockTime(
	header tlb.BlockHeader,
	state *tlb.ShardStateUnsplit,
	referenceMasterchainGenUtime uint32,
	globalVersion uint32,
) error {
	return requireGenUtimeMonotonic(
		globalVersion,
		header.GenUtime,
		max(state.GenUTime, referenceMasterchainGenUtime),
		"the previous and masterchain states",
	)
}

func nextBlockStartLT(previousLT, masterchainLT uint64) (uint64, error) {
	startLT := max(previousLT, masterchainLT)
	// An aligned bound still has to advance: candidate validation requires
	// start_lt to be strictly greater than every predecessor/reference bound.
	increment := logicalTimeAlignment - startLT%logicalTimeAlignment
	if startLT > math.MaxUint64-increment {
		return 0, fmt.Errorf("%w: start lt overflow", ErrInvalidInput)
	}
	startLT += increment
	if startLT > math.MaxInt64 {
		return 0, fmt.Errorf("%w: start lt exceeds signed TVM range", ErrInvalidInput)
	}
	return startLT, nil
}

// validatePredecessorMasterchainReference verifies monotonic masterchain
// references across a shard transition. A shard zerostate has no predecessor
// masterchain view by definition; its first block establishes that reference.
func validatePredecessorMasterchainReference(
	previousBlock ton.BlockIDExt,
	previous, next *tlb.ExtBlkRef,
) error {
	if previous == nil {
		if previousBlock.SeqNo == 0 {
			return nil
		}
		return fmt.Errorf("%w: previous basechain state has no masterchain reference", ErrInvalidInput)
	}
	if next.SeqNo < previous.SeqNo || next.SeqNo > previous.SeqNo && next.EndLt <= previous.EndLt {
		return fmt.Errorf("%w: masterchain reference precedes the previous state reference", ErrInvalidInput)
	}
	if next.SeqNo == previous.SeqNo && (next.EndLt != previous.EndLt ||
		!bytes.Equal(next.RootHash, previous.RootHash) || !bytes.Equal(next.FileHash, previous.FileHash)) {
		return fmt.Errorf("%w: masterchain reference conflicts with the previous state reference", ErrInvalidInput)
	}
	return nil
}

func existingQueueSize(info *tlb.OutMsgQueueInfo, supplied *uint64) (uint64, error) {
	if info.Extra != nil && info.Extra.OutQueueSize != nil {
		stored := *info.Extra.OutQueueSize
		if supplied != nil && *supplied != stored {
			return 0, fmt.Errorf("%w: previous outbound queue size differs from state", ErrInvalidInput)
		}
		return stored, nil
	}
	if info.OutQueue.IsEmpty() {
		if supplied != nil && *supplied != 0 {
			return 0, fmt.Errorf("%w: previous outbound queue is empty but its supplied size is non-zero", ErrInvalidInput)
		}
		return 0, nil
	}
	if supplied == nil {
		return 0, fmt.Errorf("%w: previous outbound queue size is required", ErrInvalidInput)
	}
	if *supplied > maxOutMsgQueueSize {
		return 0, fmt.Errorf("%w: previous outbound queue size exceeds 48 bits", ErrInvalidInput)
	}
	return *supplied, nil
}

func parseExact(dst any, root *cell.Cell) error {
	if root == nil {
		return errors.New("cell is absent")
	}
	var s cell.Slice
	if err := root.BeginParseInto(&s); err != nil {
		return err
	}
	if err := tlb.LoadFromCell(dst, &s); err != nil {
		return err
	}
	if s.BitsLeft() != 0 || s.RefsNum() != 0 {
		return fmt.Errorf("trailing data: %d bits, %d refs", s.BitsLeft(), s.RefsNum())
	}
	return nil
}

func parseProofExact(dst any, root *cell.Cell) error {
	if root == nil {
		return errors.New("cell is absent")
	}
	var s cell.Slice
	if err := root.BeginParseInto(&s); err != nil {
		return err
	}
	if err := tlb.LoadFromCellAsProof(dst, &s); err != nil {
		return err
	}
	if s.BitsLeft() != 0 || s.RefsNum() != 0 {
		return fmt.Errorf("trailing data: %d bits, %d refs", s.BitsLeft(), s.RefsNum())
	}
	return nil
}

func (c *collation) finish() (*Candidate, error) {
	timer := c.req.assembly.start(CollationStageFlushBatches)
	defer func() {
		timer.stop()
	}()

	if err := c.ctx.Err(); err != nil {
		return nil, err
	}
	// Everything past this point reads the descriptor dictionaries rather than
	// writing them, so the batches have to land first.
	if err := c.flushDescriptors(); err != nil {
		return nil, err
	}
	if err := c.flushQueueDeletes(); err != nil {
		return nil, err
	}
	if err := c.limits.addProof(c.outQueue.RootCell()); err != nil {
		return nil, fmt.Errorf("final outbound queue proof: %w", err)
	}
	if err := c.limits.addProof(c.dispatchQueue.RootCell()); err != nil {
		return nil, fmt.Errorf("final dispatch queue proof: %w", err)
	}

	if c.maxLT-c.header.StartLt > c.hardLTDelta {
		return nil, fmt.Errorf("%w: block exceeds the logical time limit", ErrInvalidInput)
	}
	timer.stop()
	timer = c.req.assembly.start(CollationStageBuildStateUpdate)

	parts, err := c.buildStateAndBlockParts()
	if err != nil {
		return nil, err
	}
	// The periodic samples above only ever saw the dictionary roots as they stood
	// at a sample point; the finished state is what the block actually carries.
	// Collator closes the same gap at the same place — create_shard_state adds the
	// proof right after generating the Merkle update (collator.cpp:5819) — and
	// without it every estimate this collation reports, including the one the
	// size-limit rebuild aims with and the load class that feeds split/merge
	// history, is short by whatever changed since the last sample.
	//
	// The read set prunes the walk at every cell the collation already touched, so
	// what this adds is the new state's own spine, not a second pass over the
	// predecessor.
	c.limits.admissionEstimate = c.limits.estimatedBytes()
	if err = c.limits.addProof(parts.state); err != nil {
		return nil, fmt.Errorf("final state proof: %w", err)
	}
	timer.stop()
	timer = c.req.assembly.start(CollationStageSerializeBlock)
	// The block root is built before the validation closure rather than after it,
	// so the branch that serializes it can start while the closure runs. The move
	// is inert with respect to the record: tlb.ToCell writes, it never parses, so
	// it takes no traced read and the closure still sees exactly the set it saw
	// when it ran second. The stage order in METRICS.md follows this order.
	var blockRoot *cell.Cell
	if parts.extraRoot == nil {
		blockRoot, err = tlb.ToCell(&tlb.Block{
			GlobalID:    c.oldState.GlobalID,
			BlockInfo:   parts.header,
			ValueFlow:   parts.valueFlow,
			StateUpdate: parts.stateUpdate,
			Extra:       parts.extra,
		})
	} else {
		blockRoot, err = serializeBlock(c.oldState.GlobalID, parts.header, parts.valueFlow, parts.stateUpdate, parts.extraRoot)
	}
	if err != nil {
		return nil, fmt.Errorf("serialize block: %w", err)
	}
	// The published state must be the target the update names. Everything else
	// verifyPredecessor used to restate about our own block is either impossible
	// by construction here or re-checked where the value is next used; this one
	// clause is the only failure that would be silent, and it is the worst kind:
	// a successor tree nobody else can reach, which forks this node from itself
	// rather than getting the block rejected. Two cached-hash reads and a 32-byte
	// compare.
	target, err := parts.stateUpdate.PeekRef(1)
	if err != nil {
		return nil, fmt.Errorf("load produced state update target: %w", err)
	}
	if target.HashKeyAt(0) != parts.state.HashKeyAt(0) {
		return nil, fmt.Errorf("produced state differs from the update it was built with")
	}
	if err = c.ctx.Err(); err != nil {
		return nil, err
	}
	// c.limits must stay off every branch, so the hint is taken before the fork
	// and the collated size is recorded after the join, before classify() reads
	// it.
	cellHint := int(c.limits.storage.TotalStat().Cells + candidateBOCHeadroom)
	timer.stop()

	// The block BOC moves to its own branch and everything that follows on this
	// goroutine — the validation closure, then the collated serialization —
	// overlaps it. The branch performs no traced load at all, because BOC
	// serialization never parses a cell, and cell hashes are precomputed at
	// finalization exactly so overlapping read walks are safe.
	//
	// The branch outlives the two returns inside the closure section below, so
	// the joins are deferred and their order is load-bearing: wg.Wait is
	// registered first and signalInert second, which by LIFO closes the gate and
	// only then waits. Registered the other way round a failed closure would park
	// the branch on <-inert for ever.
	var (
		boc      []byte
		bocErr   error
		fileHash [sha256.Size]byte
		bocSpan  time.Duration
		wg       sync.WaitGroup
	)
	// The block size limit is measured on the branch and consulted by the
	// collated serialization, where the broadcast payload is started against a
	// block that has to fit. Buffered and sent exactly once: the reader may
	// return before it reads, and this send must never block the branch the
	// verdict is about.
	blockFits := make(chan bool, 1)
	// inert is closed once this collation has stopped recording, which is the
	// only thing the branch has to wait for before anything may be handed to a
	// successor. permit says whether there is anything to hand over: it is set on
	// the success path alone, so a build that failed after the fork hands off
	// nothing even though the gate opens.
	inert := make(chan struct{})
	signalInert := sync.OnceFunc(func() { close(inert) })
	var permit atomic.Bool
	wg.Add(1)
	defer wg.Wait()
	defer signalInert()
	go func() {
		defer wg.Done()
		bocStart := time.Now()
		boc, bocErr = c.serializeBlockBOC(blockRoot, cellHint)
		// Published the instant it is known, ahead of the hash below, because the
		// reader is racing towards the point where it decides whether to start a
		// payload nothing can afterwards stop.
		blockFits <- bocErr == nil
		// Hashed on the branch, not after the join: the block bytes are final the
		// moment serializeBlockBOC returns and the digest is a pure function of
		// them, so this millisecond of sha256 runs alongside work that is still
		// going instead of being added to the serial tail.
		if bocErr == nil {
			fileHash = sha256.Sum256(boc)
		}
		bocSpan = time.Since(bocStart)
		// The fence, and the only thing the handoff waits for. Past it this
		// collation records nothing, so the state and the roots below can be read
		// by another collation without either of them touching the other's
		// record. permit is what distinguishes "the fence opened" from "the fence
		// opened because the build failed" — a deferred close opens it on every
		// return, and only the success path sets the flag.
		<-inert
		if bocErr == nil && permit.Load() {
			c.handOffSuccessor(fileHash, blockRoot, &parts)
		}
	}()

	timer = c.req.assembly.start(CollationStageValidationClosure)
	// The validation-closure shims run here, after the state update exists, and
	// the position is the whole point. The update's source graph descends only
	// through cells the read set recorded, so a read taken before this line ends
	// up inside the block: it widens the update's old side with cells the
	// reference collator never puts there, costing block bytes and breaking
	// bit-parity with it. Taken after, the same read still reaches the collated
	// proof — that one is selected from collatedProofEstimate, a separate hash
	// set the record callback keeps filling — and reaches nothing else.
	//
	// This is the split Collator has: prepare_proofs is called from
	// create_collated_data (collator.cpp:6297), after create_shard_state has
	// generated the update at :5806 and after create_block.
	if err = c.traceValidationClosure(); err != nil {
		return nil, err
	}
	if err = c.ctx.Err(); err != nil {
		return nil, err
	}
	// The fence. Reading is over — the closure was the last thing that could
	// record — so the recorder is detached and every lane tracer standing in
	// front of it goes inert with it. The table itself stays: the collated proof
	// below is selected out of it, and resolving those cells again would mean
	// reading them from disk on the hot path. Seal, which does drop the table,
	// still happens where it always did, after the proof has been built.
	c.usage.Detach()
	for _, lane := range c.lanes {
		lane.tracer.seal()
	}
	permit.Store(true)
	signalInert()
	timer.stop()

	timer = c.req.assembly.start(CollationStageSerializeCandidate)
	collatedData, collatedRoots, prepared, collatedErr := c.serializeCollatedData(
		parts.stateUpdate, blockRoot, parts.header.SeqNo, cellHint, blockFits,
	)
	wg.Wait()
	if bocErr != nil {
		return nil, bocErr
	}
	if collatedErr != nil {
		return nil, collatedErr
	}
	c.req.assembly.substage(CollationStageSerializeCandidate, "block_boc", bocSpan)
	timer.stop()
	timer = c.req.assembly.start(CollationStageFinalizeCandidate)

	c.limits.collatedData = uint64(len(collatedData))
	collatedFileHash := sha256.Sum256(collatedData)
	rootHash := blockRoot.HashKey()

	// The broadcast payload has been compressing since the collated goroutine
	// had its roots; these two hashes are the last thing it is missing. They are
	// what the serializer checks the payload against, so declaring them here —
	// before the candidate below exists, and therefore before anything can reach
	// the capsule through it — is what makes that check possible at all.
	prepared.DeclareDigests(fileHash, collatedFileHash)

	// Recorded only for a build that produced a candidate, so a run abandoned
	// part-way cannot shrink the next recorder to the size it stopped at. These
	// are integers describing the scratch structures this build filled — nothing
	// that holds a cell crosses from one collation to the next.
	storageCells, storageProofCells := c.limits.storage.CellCounts()
	c.builder.observeBuildSizes(
		c.usage.Size(),
		int(storageCells),
		int(storageProofCells),
		c.updateMemoCells,
	)

	// The record has produced everything it exists for — the state update, the
	// collated proofs and the size estimates — and the collation is over. Closing
	// it here matters because the block this build is about to hand back embeds
	// cells that were read out of the predecessor and still carry this recorder's
	// trace: a handful in the inbound descriptors, envelopes reimported from our
	// own out-queue. Retaining that block would otherwise retain the whole record
	// — the predecessor tree and a six-figure table — for as long as the candidate
	// lives, and every later descent through one of those cells would both record
	// into a set nobody reads again and hand out a wrapped copy instead of the
	// shared cell. Nothing here or after reads the record: BOC serialization never
	// parses a cell, so the broadcast payload still being compressed in the
	// background cannot reach it.
	c.usage.Seal()
	// The lanes' tracers stand in front of the record on every cell a lane
	// read, and a block embeds such cells. Sealed together with the record,
	// for the same reason and with the same effect.
	for _, lane := range c.lanes {
		lane.tracer.seal()
	}

	c.stats.EndLT = c.maxLT
	c.stats.OutQueueSize = c.queueSize
	c.stats.Transactions = uint32(c.limits.transactions)
	c.stats.GasUsed = c.limits.gas
	c.stats.Load = max(c.peakLoad, c.limits.classify())
	candidate := &Candidate{
		ID: ton.BlockIDExt{
			Workchain: parts.header.Shard.WorkchainID,
			Shard:     int64(parts.header.Shard.GetShardID()),
			SeqNo:     parts.header.SeqNo,
			RootHash:  rootHash[:],
			FileHash:  fileHash[:],
		},
		CreatedBy:        c.req.createdBy,
		BlockBOC:         boc,
		CollatedData:     collatedData,
		CollatedFileHash: collatedFileHash,
		State:            parts.state,
		StateUpdate:      parts.stateUpdate,
		StorageStats:     c.storageStats,
		Externals:        c.externals,
		Stats:            c.stats,
		prepared:         prepared,
		// ID.FileHash and CollatedFileHash above are fileHash and
		// collatedFileHash, taken from these very buffers a few statements ago.
		// signArtifact used to re-derive both to compare them against
		// themselves; C++'s window-producer accepts the collator's hashes as
		// they come out of collator.cpp:6445-6450 and hashes nothing.
		digested: true,
		// The commit that follows re-derived all of this from BlockBOC: a full
		// eager parse of the block, a second apply of the update over the
		// predecessor, and a re-validation of a predecessor this very build read.
		// The reference does strictly less at the same point — ChainState::apply
		// deserializes lazily and applies, and checks nothing about the parent.
	}
	// finish() creates the private derivation capsule, but the complete Builder
	// entry point seals it only after wrappers have written their final Stats.
	candidate.built = newBuiltCandidate(
		candidate.ID,
		blockRoot,
		collatedRoots,
		parts.state,
		parts.stateUpdate,
		parts.header.StartLt,
		parts.header.GenUtime,
		c.req.header.GenUtimeMS,
		c.builtPredecessors()...,
	)

	return candidate, nil
}

// builtPredecessors names the predecessor states this collation's Merkle update
// was built over, in the order openPredecessorReadSet combined them.
func (c *collation) builtPredecessors() []*PreviousBlock {
	previous := make([]*PreviousBlock, 0, 2)
	for i := range c.previousCount() {
		previous = append(previous, c.previousAt(i))
	}

	return previous
}

func (c *collation) serializeBlockBOC(blockRoot *cell.Cell, cellHint int) ([]byte, error) {
	// Match reference std_boc_serialize(root, 31) byte-for-byte. Its root is
	// not marked for a top hash, despite bit 4 being present in the mode.
	boc, err := blockRoot.ToBOCWithOptionsErr(cell.BOCSerializeOptions{
		WithCRC32C:     true,
		WithIndex:      true,
		WithCacheBits:  true,
		WithIntHashes:  true,
		CellsCountHint: cellHint,
	})
	if err != nil {
		return nil, fmt.Errorf("serialize block BOC: %w", err)
	}
	if err = c.ctx.Err(); err != nil {
		return nil, err
	}
	if uint64(len(boc)) > uint64(c.config.maxBlockBytes) {
		return nil, sizeLimitError{
			what:     "block BOC",
			produced: uint64(len(boc)),
			limit:    uint64(c.config.maxBlockBytes),
			estimate: c.limits.rebuildEstimate(),
		}
	}

	return boc, nil
}

// serializeCollatedData runs on a second goroutine during finish. It must stay
// read-only over c.limits: the collated size is recorded by the caller after
// the join so that classify() still observes it. blockCells is the caller's
// sizing for the block BOC, taken off c.limits before the fork for that reason.
//
// The broadcast payload is started here rather than returned to be started by
// the caller, because the roots it is a BOC of are final here and nowhere
// earlier: handing them back instead would delay it by this call's own
// serialization, and rebuilding them from the returned bytes would parse back
// what this call just wrote.
//
// The roots themselves are returned for that same reason, one consumer later:
// this node validates the candidate it produced, and the alternative there is
// parsing back the bytes written below.
//
// blockFits is the other branch's verdict on the block size limit, readable
// once. A nil capsule is returned when it says no; the caller returns that
// branch's error before anything reaches the capsule.
func (c *collation) serializeCollatedData(
	stateUpdate, blockRoot *cell.Cell,
	seqNo uint32,
	blockCells int,
	blockFits <-chan bool,
) ([]byte, []*cell.Cell, *simplex.PreparedCandidate, error) {
	collatedRoots, err := c.buildCollatedRoots(consensusExtraRoot(c.req.header.GenUtimeMS), stateUpdate)
	if err != nil {
		return nil, nil, nil, err
	}
	collatedCells := max(1, c.usage.Size()+len(collatedRoots))
	startPayload := func() *simplex.PreparedCandidate {
		return simplex.PrepareCandidateAsync(
			seqNo,
			blockRoot,
			collatedRoots,
			simplex.PayloadCellHintFromCounts(blockCells, collatedCells),
		)
	}

	// The broadcast payload is a third serialization of these same roots and
	// nothing before the broadcast reads it, so it starts at the first instant
	// both root sets are final rather than after the two component BOCs have
	// been written. From here it overlaps the collated serialization below, the
	// tail of the block serialization on the other goroutine, and everything the
	// caller does afterwards: signing, the candidate marker fsync, the state
	// commit and the scheduled broadcast wait. Started after the join it
	// overlapped only that last group, and delivery paid the rest.
	//
	// A third concurrent walk over these trees rests on the same argument the
	// two already running do: BOC serialization never parses a cell, so it
	// records nothing into the read set — which c.usage.Seal is about to empty
	// under it — and mutates nothing the others read.
	//
	// Neither BOC exists yet, so the hint cannot be the two counts read back out
	// of their headers. It is instead the sum of the two counts those
	// serializations are presized with, over the same two cell sets: the same
	// construction, with each half an estimate rather than a finished count. The
	// hint sizes scratch only and no byte of the payload depends on it.
	//
	// The two file hashes the payload is checked against do not exist yet
	// either; the caller declares them on this capsule as soon as it has hashed
	// both BOCs, which is before the candidate carrying it exists.
	//
	// Nothing can stop the payload once it runs — it takes no context, and the
	// candidate that is the only way to reach it is never built when this
	// collation fails — so it is started only if nothing already known says
	// this candidate is lost. Three things can say so, and all three are cheap
	// here:
	//
	//   - the deadline, read here as well as below;
	//   - the block size limit, measured on the sibling goroutine against the
	//     finished block BOC. Its verdict is taken without blocking: it
	//     normally lands about a millisecond before the roots above are
	//     finished, and waiting for the times it does not would put a block
	//     serialization in front of the collated one these two branches were
	//     forked to overlap;
	//   - the collated size limit, which cannot be measured yet because its
	//     bytes are written below. The collation's own estimate answers for it
	//     — the same figure admission stopped on, which stands above the bytes
	//     a collation actually produces, so an estimate already over the limit
	//     is a collation the check below cannot pass.
	//
	// Only that last one is an approximation, and it errs by starting the
	// payload late rather than by starting one that is thrown away. Retries are
	// what the whole gate is for: a size failure rebuilds the block up to four
	// more times under escalating concessions, on the largest blocks and against
	// the tightest
	// deadline, which is the worst moment to hand a core to a full candidate
	// compression for a block that no longer exists.
	var (
		prepared     *simplex.PreparedCandidate
		blockVerdict bool
		verdictTaken bool
	)
	select {
	case blockVerdict = <-blockFits:
		verdictTaken = true
	default:
	}
	if verdictTaken && blockVerdict && c.ctx.Err() == nil &&
		c.limits.collatedData <= uint64(c.config.maxCollatedBytes) {
		prepared = startPayload()
	}

	collatedData, err := cell.ToBOCWithOptionsErr(collatedRoots, cell.BOCSerializeOptions{
		WithCRC32C:     true,
		CellsCountHint: collatedCells,
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("serialize collated data: %w", err)
	}
	if err = c.ctx.Err(); err != nil {
		return nil, nil, nil, err
	}
	if uint64(len(collatedData)) > uint64(c.config.maxCollatedBytes) {
		return nil, nil, nil, sizeLimitError{
			what:     "collated data",
			produced: uint64(len(collatedData)),
			limit:    uint64(c.config.maxCollatedBytes),
			estimate: c.limits.rebuildEstimate(),
		}
	}

	// Both limits are settled now, so a payload held back above starts here,
	// still ahead of the two file hashes, the candidate assembly, the signature
	// and the commit. The verdict is waited for this time: there is nothing
	// left on this branch for it to delay.
	if prepared == nil {
		if !verdictTaken {
			blockVerdict = <-blockFits
		}
		if blockVerdict {
			prepared = startPayload()
		}
	}

	return collatedData, collatedRoots, prepared, nil
}

// buildCollatedRoots runs on the collated-data goroutine, so it must stay
// read-only over c.usage, c.limits, c.lanes and c.collatedProofEstimate — the
// untraced c.oldRoot read below is what keeps the read set from recording new
// reads while the block branch serializes concurrently.
func (c *collation) buildCollatedRoots(consensusExtra, stateUpdate *cell.Cell) ([]*cell.Cell, error) {
	roots := []*cell.Cell{consensusExtra}
	if c.master != nil {
		var err error
		roots, err = c.masterCollatedPrefix(consensusExtra)
		if err != nil {
			return nil, err
		}
	}
	if c.config.capabilities&capFullCollatedData == 0 {
		return roots, nil
	}
	if err := c.ctx.Err(); err != nil {
		return nil, err
	}

	if c.master == nil {
		for i := 0; i < c.previousCount(); i++ {
			previous := c.previousAt(i)
			if previous.ID.SeqNo == 0 {
				continue
			}
			proof := c.previousBlockProofs[i]
			if proof == nil {
				var err error
				proof, err = c.buildPreviousBlockStateProof(previous)
				if err != nil {
					return nil, err
				}
			}
			roots = append(roots, proof)
		}

		// The reference full-collated path selects cells by the global hashes
		// observed by ProofStorageStat, not by the paths they were read through. A split
		// or merge can rebuild an equal dictionary subtree under a synthetic path;
		// the validator still needs the original predecessor occurrence of every
		// hash read through that view.
		loaded := c.collatedProofEstimate.loadedHashes()
		updateSource, err := merkleUpdateSourceShape(stateUpdate)
		if err != nil {
			return nil, fmt.Errorf("collect state update source shape: %w", err)
		}
		// The read set is handed in as the resolver because it holds the loaded
		// form of everything this walk keeps that the collation already touched;
		// without it a lazy predecessor is paged in a second time to build a proof
		// over cells that are already in memory. The selection is still the hash
		// predicate above, so the proof bytes do not depend on it.
		// The read set's size is handed over as the memo capacity. It is the only
		// figure available on this goroutine that bounds the walk — the proof
		// keeps the cells the record saw, plus the boundaries under them — and on
		// a mainnet block it is a third over, where reaching the same table by
		// doubling from sixteen entries allocates nearly three times what the
		// table ends up holding and discards the rest. Capacity only: the hash
		// predicate above is what decides the bytes.
		previousStateProof, err := c.oldRoot.WithoutTrace().CreateHashUsageProofResolvedSized(func(hash cell.Hash) bool {
			return loaded.loaded(hash) || updateSource.emitted(hash)
		}, c.usage.RecordedCell, c.usage.Size())
		if err != nil {
			return nil, fmt.Errorf("build previous state proof: %w", err)
		}
		if c.topology.kind == topologyAfterMerge {
			body := previousStateProof.MustPeekRef(0)
			for i := 0; i < 2; i++ {
				proof, proofErr := cell.CreateMerkleProof(body.MustPeekRef(i))
				if proofErr != nil {
					return nil, fmt.Errorf("build merge predecessor state proof %d: %w", i, proofErr)
				}
				roots = append(roots, proof)
			}
		} else {
			roots = append(roots, previousStateProof)
		}

	}

	roots = append(roots, c.fullCollatedProofs...)

	var storageProofs map[cell.Hash]*cell.Cell
	var storageBuilders map[cell.Hash]*cell.MerkleProofBuilder
	for _, lane := range c.lanes {
		if !lane.touched || lane.initialStorageStat == nil {
			continue
		}
		// The emitted proof has to cover every dictionary node that any
		// transaction of this account reads, not only the first one's: the
		// verifier binds one proof per account and then replays the whole
		// sequence against it, so a branch a later transaction descends into
		// would meet a pruned boundary and the executor would refuse the
		// dictionary outright. The builder stays live for the whole collation;
		// its record callback accounts for proof growth without constructing an
		// intermediate proof, and this one CreateProof yields the union of every
		// read the account made.
		//
		// It also has to cover every OTHER account bound to the same dictionary,
		// for the same reason one step out: the collated data carries one proof
		// per dictionary hash, not per account, and each of those accounts
		// replays its own walk against it. That union is not assembled here —
		// it is already true of the builder, because sharedAccountStorageProof
		// hands every such account the same recorder. All this loop must do is
		// emit each distinct builder once; keying the emitted map by dictionary
		// hash while creating the proof per lane is exactly the defect this
		// guards against, since the last lane's assignment would win and every
		// other account's branches would reach the validator pruned.
		//
		// A missing builder is an internal invariant break, not a case to fall
		// back on: a lane gets its storage stat and its builder in the same
		// branch, and full collated data is exactly the mode that sets both. The
		// substitute this used to make — a plain Merkle proof of the whole
		// dictionary — is not the same object, since nothing prunes it, so it
		// would silently emit a BOC far larger than the size estimate admitted
		// against.
		if lane.storageProof == nil {
			return nil, fmt.Errorf("collated account storage proof: account %x has a storage stat without a proof builder", lane.key)
		}
		// An untouched builder produces an all-pruned proof. Shipping that proof
		// is not merely useless: the validator binds it as the account storage
		// dictionary and then cannot serve the first lookup. The reference omits
		// the proof when the transaction made no storage-stat updates. Any real
		// dictionary access starts at the root, so its presence in the read set is
		// the equivalent signal here.
		if !accountStorageProofUsed(lane.storageProof) {
			continue
		}
		hash := lane.initialStorageStat.HashKey()
		if shared, seen := storageBuilders[hash]; seen {
			// Two lanes on one dictionary that do not share a recorder cannot be
			// reconciled at this point — only one proof can be shipped for the
			// hash — so this is raised rather than papered over.
			if shared != lane.storageProof {
				return nil, fmt.Errorf(
					"collated account storage proof: account %x does not share the traced builder of storage dict %x",
					lane.key,
					hash,
				)
			}
			continue
		}
		proof, err := lane.storageProof.CreateProof()
		if err != nil {
			return nil, fmt.Errorf("build account storage stat proof %x: %w", hash, err)
		}
		proof = wrapAccountStorageProof(proof)
		if storageProofs == nil {
			storageProofs = make(map[cell.Hash]*cell.Cell)
			storageBuilders = make(map[cell.Hash]*cell.MerkleProofBuilder)
		}
		storageProofs[hash] = proof
		storageBuilders[hash] = lane.storageProof
	}
	hashes := make([]cell.Hash, 0, len(storageProofs))
	for hash := range storageProofs {
		hashes = append(hashes, hash)
	}
	sort.Slice(hashes, func(i, j int) bool {
		return bytes.Compare(hashes[i][:], hashes[j][:]) < 0
	})
	for _, hash := range hashes {
		roots = append(roots, storageProofs[hash])
	}

	verified, err := verifyCollatedRoots(roots, c.header.GenUtime)
	if err != nil {
		return nil, fmt.Errorf("verify constructed full collated data: %w", err)
	}
	if c.master == nil {
		if err = verifyCollatedPredecessors(&verified, c.req.previous, c.req.previous2); err != nil {
			return nil, fmt.Errorf("verify constructed predecessor proofs: %w", err)
		}
	}
	if err = verifyCollatedNeighborStates(&verified, c.req.neighbors); err != nil {
		return nil, fmt.Errorf("verify constructed neighbor proofs: %w", err)
	}

	return roots, nil
}

func (c *collation) prepareFullCollatedProofs() error {
	if c.req.fullCollatedProofs == nil {
		return nil
	}
	proofs, err := c.req.fullCollatedProofs.BuildFullCollatedProofs(c.ctx, FullCollatedProofRequest{
		Previous:  c.req.previous,
		Previous2: c.req.previous2,
		Neighbors: c.req.neighbors,
		Internals: c.req.internalMessages(),
		QueueScan: c.fullCollatedQueueScan(),
	})
	if err != nil {
		return fmt.Errorf("build full collated neighbor proofs: %w", err)
	}
	if err = c.ctx.Err(); err != nil {
		return err
	}
	proofs, err = canonicalCollatedProofs(proofs)
	if err != nil {
		return err
	}
	c.fullCollatedProofs = proofs

	return nil
}

func (c *collation) fullCollatedQueueScan() *FullCollatedQueueScan {
	messages := c.req.internalMessages()
	if len(messages) != 0 {
		last := messages[len(messages)-1]

		return &FullCollatedQueueScan{
			Target: c.shard,
			LT:     last.EnqueuedLT,
			Hash:   last.Root.HashKey(),
		}
	}
	if c.req.internalsIncomplete() {
		return nil
	}
	_, referenceEndLT := c.processedReference()
	if referenceEndLT == 0 {
		return nil
	}

	return &FullCollatedQueueScan{
		Target: c.shard,
		LT:     referenceEndLT - 1,
		Hash:   processedInfinityHash,
	}
}

// trackAccountStorageProof charges growth of the account's shared storage-stat
// proof after every committed transaction. Reads feed the estimator directly
// from ReadSet's first-record callback, so this path neither walks the proof nor
// serializes it. The estimate prices every loaded cell with the BOC format's
// maximum reference width, every unread child as the boundary proof generation
// emits (a full terminal cell or a pruned branch), and the fixed wrapper/header
// overhead; it is therefore no smaller than the standalone wrapped proof BOC
// this function used to build.
//
// Accounts with the same initial dictionary share both recorder and estimator.
// charged is their common high-watermark, so a later account or transaction pays
// only growth of the one proof buildCollatedRoots will emit, never another copy.
func (c *collation) trackAccountStorageProof(lane *accountLane) {
	if !c.fullCollated || lane.initialStorageStat == nil {
		return
	}
	if !accountStorageProofUsed(lane.storageProof) {
		return
	}
	hash := lane.initialStorageStat.HashKey()
	shared := c.accountStorageProofs[hash]
	estimate := shared.estimatedBOCSize()
	if estimate <= shared.charged {
		return
	}
	charge := estimate - shared.charged
	shared.charged = estimate
	if math.MaxUint64-c.collatedFixedEstimate < charge {
		c.collatedFixedEstimate = math.MaxUint64
	} else {
		c.collatedFixedEstimate += charge
	}
	c.updateCollatedEstimate()
}

func accountStorageProofUsed(builder *cell.MerkleProofBuilder) bool {
	_, used := builder.ReadSet().Contains(builder.OriginalRoot().HashKey())
	return used
}

func wrapAccountStorageProof(proof *cell.Cell) *cell.Cell {
	return cell.BeginCell().
		MustStoreUInt(0x37c1e3fc, 32). // account_storage_dict_proof#37c1e3fc
		MustStoreRef(proof).
		EndCell()
}

func (c *collation) buildPreviousBlockStateProof(previous *PreviousBlock) (*cell.Cell, error) {
	blockRoot := previous.Block
	if blockRoot == nil {
		return nil, fmt.Errorf("%w: full collated data requires the previous block root", ErrInvalidInput)
	}
	if !bytes.Equal(blockRoot.Hash(), previous.ID.RootHash) {
		return nil, fmt.Errorf("%w: previous block root differs from its block id", ErrInvalidInput)
	}

	var block tlb.Block
	if err := parseExact(&block, blockRoot.WithoutTrace()); err != nil {
		return nil, fmt.Errorf("%w: decode previous block: %v", ErrInvalidInput, err)
	}
	if block.BlockInfo.SeqNo != previous.ID.SeqNo ||
		block.BlockInfo.Shard.WorkchainID != previous.ID.Workchain ||
		int64(block.BlockInfo.Shard.GetShardID()) != previous.ID.Shard {
		return nil, fmt.Errorf("%w: previous block header differs from its block id", ErrInvalidInput)
	}
	if err := cell.ValidateMerkleUpdate(block.StateUpdate); err != nil {
		return nil, fmt.Errorf("%w: invalid previous block state update: %v", ErrInvalidInput, err)
	}
	if block.StateUpdate.MustPeekRef(1).HashKeyAt(0) != previous.State.WithoutTrace().HashKeyAt(0) {
		return nil, fmt.Errorf("%w: previous block state update does not produce the supplied state", ErrInvalidInput)
	}

	usage := cell.NewReadSet(blockRoot.WithoutTrace())
	loader, err := usage.Root().BeginParse()
	if err != nil {
		return nil, fmt.Errorf("build previous block proof: %w", err)
	}
	if _, err = loader.LoadUInt(64); err != nil {
		return nil, fmt.Errorf("build previous block proof: %w", err)
	}
	if _, err = loader.LoadRefCell(); err != nil {
		return nil, fmt.Errorf("build previous block info proof: %w", err)
	}
	if _, err = loader.LoadRefCell(); err != nil {
		return nil, fmt.Errorf("build previous value flow proof: %w", err)
	}
	stateUpdate, err := loader.LoadRefCell()
	if err != nil {
		return nil, fmt.Errorf("build previous state update proof: %w", err)
	}
	if _, err = loader.LoadRefCell(); err != nil {
		return nil, fmt.Errorf("build previous block extra proof: %w", err)
	}
	if loader.BitsLeft() != 0 || loader.RefsNum() != 0 {
		return nil, fmt.Errorf("%w: previous block has trailing data", ErrInvalidInput)
	}
	if _, err = stateUpdate.BeginParse(); err != nil {
		return nil, fmt.Errorf("load previous state update for proof: %w", err)
	}

	proof, err := usage.Proof()
	if err != nil {
		return nil, fmt.Errorf("build previous block state proof: %w", err)
	}
	return proof, nil
}

func (c *collation) previousCount() int {
	if c.req.previous2 != nil {
		return 2
	}
	return 1
}

func (c *collation) previousAt(index int) *PreviousBlock {
	if index == 0 {
		return &c.req.previous
	}
	return c.req.previous2
}

func (c *collation) updatePeakLoad() {
	c.updateCollatedEstimate()
	c.peakLoad = max(c.peakLoad, c.limits.classify())
}

func (c *collation) updateCollatedEstimate() {
	if !c.fullCollated || c.limits == nil {
		return
	}
	proofBytes := c.collatedProofEstimate.size()
	if math.MaxUint64-c.collatedFixedEstimate < proofBytes {
		c.limits.collatedData = math.MaxUint64
		return
	}
	c.limits.collatedData = c.collatedFixedEstimate + proofBytes
}

func consensusExtraRoot(genUtimeMS uint64) *cell.Cell {
	return cell.BeginCell().
		MustStoreUInt(consensusExtraDataTag, 32).
		MustStoreUInt(0, 32).
		MustStoreUInt(genUtimeMS, 64).
		EndCell()
}

func isMissingKey(err error) bool {
	return errors.Is(err, cell.ErrNoSuchKeyInDict)
}

// consumedExternals is the set a successor must not be offered: the messages
// this block took out of the pool's reach. Only the three outcomes that end a
// message's life in this window count.
//
// ExternalSkippedLimit is deliberately absent. A message skipped for a limit was
// never executed and its generation is untouched, so it is exactly the kind of
// message the next slot should pick up — excluding it would strand it for the
// rest of the window for no reason.
func consumedExternals(feedback []msgpool.ExternalFeedback) [][32]byte {
	if len(feedback) == 0 {
		return nil
	}
	consumed := make([][32]byte, 0, len(feedback))
	for i := range feedback {
		switch feedback[i].Outcome {
		case msgpool.ExternalIncluded, msgpool.ExternalInvalid, msgpool.ExternalNotAccepted:
			consumed = append(consumed, feedback[i].Ref.Hash)
		}
	}
	if len(consumed) == 0 {
		return nil
	}

	return consumed
}

// handOffSuccessor gives the next slot everything it needs to start building on
// this block, at the earliest instant it can be given: the record has just gone
// inert and nothing has been serialized yet. The whole of the tail below —
// the collated proof, both BOCs, the signature, the commit, the marker and the
// emission — then runs beside the successor instead of in front of it.
//
// It runs on the block-BOC branch rather than on the main goroutine because the
// main goroutine is already inside the collated serialization by the time the
// fence opens, and a handoff that waited for that would give away most of what
// it is for.
//
// Nothing here is speculative about the block: every value is final. What is
// speculative is that this block will be the one committed for its slot — a
// size-limit retry rebuilds it and moves its root — and that is settled by the
// producer, which adopts an offer only against the predecessor it went on to
// commit.
func (c *collation) handOffSuccessor(fileHash [32]byte, blockRoot *cell.Cell, parts *blockParts) {
	port := c.req.successor
	if port == nil || port.offer == nil {
		return
	}
	rootHash := blockRoot.HashKey()

	// The queue lineage the successor inherits, by the same rule
	// recordCandidateLocked applies when it installs this candidate: a block
	// whose predecessor was itself a candidate keeps that candidate's base,
	// while the first block of a chain becomes the base.
	queueBase := port.queueBase
	if port.candidateTip == nil {
		queueBase = port.previous
	}

	id := ton.BlockIDExt{
		Workchain: parts.header.Shard.WorkchainID,
		Shard:     int64(parts.header.Shard.GetShardID()),
		SeqNo:     parts.header.SeqNo,
		RootHash:  rootHash[:],
		FileHash:  fileHash[:],
	}
	port.noteOffered(rootHash)
	port.offer(SuccessorOffer{
		adopt: port.noteAdopted,
		successorPayload: successorPayload{
			ID:           id,
			Root:         blockRoot,
			State:        parts.state,
			StateUpdate:  parts.stateUpdate,
			StartLT:      parts.header.StartLt,
			GenUTime:     parts.header.GenUtime,
			OutQueueSize: c.queueSize,
			StorageStats: c.storageStats,
			Externals:    c.externals,
		},
		QueueBase:    clonePreviousBlocks(queueBase),
		CandidateTip: cloneHashPointer(port.candidateTip),
		Previous:     clonePreviousBlocks(port.previous),
		Policy: CandidateState{
			Block:       cloneBlockID(id),
			NextSeqno:   parts.header.SeqNo + 1,
			BeforeSplit: port.policy.BeforeSplit,
		},
		Exclude:         consumedExternals(c.externals),
		predecessorSlot: port.slot,
		handoffAt:       time.Now(),
	})
}
