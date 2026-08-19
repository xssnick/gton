package collator

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"slices"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

const dispatchFirstPhaseLimit = uint64(1 << 30)

type dispatchPhaseLimits struct {
	maxTotal        uint64
	maxPerInitiator uint64
}

type dispatchInitiator struct {
	workchain   int32
	accountID   [32]byte
	initiatorLT uint64
}

// processDispatchQueue drains deferred messages in phases. Each phase
// scans a private view while removals are applied to the real queue: phase 0
// takes one message from every account, then the two policy-bounded phases may
// take more. Rebuilding the scan view between phases is what lets an account
// participate once in phase 0 and again under later limits.
func (c *collation) processDispatchQueue() error {
	// The internal request owns these slice headers, but their backing arrays
	// still belong to the caller. Canonicalization copies before sorting and is
	// cached here for every dispatch phase and the later generated-message pass.
	c.req.dispatch.PriorityList = canonicalDispatchAccounts(c.req.dispatch.PriorityList)
	c.req.dispatch.Whitelist = canonicalDispatchAccounts(c.req.dispatch.Whitelist)

	hardQueueLimit := c.config.deferOutQueueSizeLimit
	deferQueueLimit := max(c.req.dispatch.DeferOutQueueSizeLimit, hardQueueLimit)
	if c.queueSize > deferQueueLimit && c.oldQueueSize > hardQueueLimit {
		return nil
	}

	c.haveUnprocessedDispatchQueue = true
	phases := [3]dispatchPhaseLimits{
		{maxTotal: dispatchFirstPhaseLimit, maxPerInitiator: dispatchFirstPhaseLimit},
		{
			maxTotal:        uint64(c.req.dispatch.Phase2MaxTotal),
			maxPerInitiator: uint64(c.req.dispatch.Phase2MaxPerInitiator),
		},
		{
			maxTotal:        uint64(c.req.dispatch.Phase3MaxTotal),
			maxPerInitiator: c.phase3DispatchPerInitiatorLimit(),
		},
	}

	for phase, limits := range phases {
		if limits.maxTotal == 0 || limits.maxPerInitiator == 0 {
			continue
		}
		if phase > 0 && c.req.dispatch.AttemptIndex >= 1 {
			break
		}

		current := &tlb.DispatchQueueAugDict{
			AugmentedDictionary: c.dispatchQueue.Copy(),
		}
		priorities := slices.Clone(c.req.dispatch.PriorityList)
		priorityIndex := 0
		perInitiator := make(map[dispatchInitiator]uint64)
		var total uint64

		for !current.IsEmpty() {
			c.updateCollatedEstimate()
			if !c.limits.fits(LoadNormal) {
				c.blockFull = true
				c.updatePeakLoad()
				return c.registerDispatchOp(true)
			}
			// collator.cpp:4424-4429, which returns register_dispatch_queue_op(true)
			// on the timeout exactly as it does on the block-full one.
			if c.internalMsgExpired() {
				c.blockFull = true
				c.stats.InternalMsgTimeouts++
				c.updatePeakLoad()
				return c.registerDispatchOp(true)
			}
			if err := c.ctx.Err(); err != nil {
				return err
			}

			source, accountQueue, err := c.nextDispatchAccount(current, &priorities, &priorityIndex)
			if err != nil {
				return err
			}
			lt, err := minimumDispatchLT(accountQueue)
			if err != nil {
				return fmt.Errorf("%w: dispatch account %x: %v", ErrInvalidInput, source.AccountID, err)
			}
			metadata, err := c.processDeferredMessage(source, lt)
			if err != nil {
				return err
			}

			removeAccount := phase == 0 ||
				(phase == 1 &&
					uint64(c.senderGenerated[source.AccountID]) >= uint64(c.req.dispatch.DeferMessagesAfter) &&
					!dispatchAccountListed(c.req.dispatch.Whitelist, source))
			if err = updateDispatchScanQueue(current, source.AccountID, accountQueue, lt, removeAccount); err != nil {
				return fmt.Errorf("%w: update dispatch scan account %x: %v", ErrInvalidInput, source.AccountID, err)
			}

			if metadata != nil {
				initiator, initiatorErr := dispatchInitiatorFromMetadata(metadata)
				if initiatorErr != nil {
					return initiatorErr
				}
				perInitiator[initiator]++
				if perInitiator[initiator] >= limits.maxPerInitiator {
					// Phase 1 may already have removed the account because its sender
					// threshold was reached, and this second removal is then a no-op.
					if !removeAccount {
						if err = current.Delete(dispatchAccountKey(source.AccountID)); err != nil {
							return fmt.Errorf("remove capped dispatch account %x: %w", source.AccountID, err)
						}
					}
				}
			}

			total++
			if total >= limits.maxTotal {
				c.peakLoad = max(c.peakLoad, LoadSoft)
				break
			}
		}

		if phase == 0 {
			c.haveUnprocessedDispatchQueue = false
		}
		if err := c.registerDispatchOp(true); err != nil {
			return err
		}
	}

	return nil
}

func (c *collation) phase3DispatchPerInitiatorLimit() uint64 {
	if !c.req.dispatch.Phase3AdaptivePerInitiator {
		return uint64(c.req.dispatch.Phase3MaxPerInitiator)
	}

	switch {
	case c.queueSize <= 256:
		return 10
	case c.queueSize <= 512:
		return 2
	case c.queueSize <= 1500:
		return 1
	default:
		return 0
	}
}

func (c *collation) nextDispatchAccount(
	queue *tlb.DispatchQueueAugDict,
	priorities *[]DispatchAccount,
	priorityIndex *int,
) (DispatchAccount, *tlb.AccountDispatchQueue, error) {
	for len(*priorities) != 0 {
		if *priorityIndex == len(*priorities) {
			*priorityIndex = 0
		}
		candidate := (*priorities)[*priorityIndex]
		if !c.dispatchAccountInShard(candidate) {
			*priorities = append((*priorities)[:*priorityIndex], (*priorities)[*priorityIndex+1:]...)
			continue
		}

		accountQueue, err := loadAccountDispatchQueue(queue, candidate.AccountID)
		if isMissingKey(err) {
			*priorities = append((*priorities)[:*priorityIndex], (*priorities)[*priorityIndex+1:]...)
			continue
		}
		if err != nil {
			return DispatchAccount{}, nil, fmt.Errorf("%w: load priority dispatch account %x: %v", ErrInvalidInput, candidate.AccountID, err)
		}

		*priorityIndex++
		return candidate, accountQueue, nil
	}

	account, err := minimumDispatchAccount(queue)
	if err != nil {
		return DispatchAccount{}, nil, err
	}
	account.Workchain = c.shard.Workchain
	accountQueue, err := loadAccountDispatchQueue(queue, account.AccountID)
	if err != nil {
		return DispatchAccount{}, nil, fmt.Errorf("%w: load minimum dispatch account %x: %v", ErrInvalidInput, account.AccountID, err)
	}
	return account, accountQueue, nil
}

func (c *collation) dispatchAccountInShard(account DispatchAccount) bool {
	prefix := binary.BigEndian.Uint64(account.AccountID[:8])
	return c.shard.Contains(account.Workchain, prefix)
}

func minimumDispatchAccount(queue *tlb.DispatchQueueAugDict) (DispatchAccount, error) {
	if queue == nil || queue.IsEmpty() {
		return DispatchAccount{}, fmt.Errorf("%w: dispatch queue has no minimum account", ErrInvalidInput)
	}

	current := queue.AugmentedDictionary.Copy()
	rootExtra, err := current.LoadRootExtra()
	if err != nil {
		return DispatchAccount{}, fmt.Errorf("%w: load dispatch queue root augmentation: %v", ErrInvalidInput, err)
	}
	minimumLT, err := dispatchMinimumLT(rootExtra)
	if err != nil {
		return DispatchAccount{}, fmt.Errorf("%w: dispatch queue root augmentation: %v", ErrInvalidInput, err)
	}

	accountKey := cell.BeginCell()
	for {
		common, commonErr := current.GetCommonPrefix()
		if commonErr != nil {
			return DispatchAccount{}, fmt.Errorf("%w: load dispatch queue common prefix: %v", ErrInvalidInput, commonErr)
		}
		remaining := current.GetKeySize()
		if common.BitsSize() > remaining || common.RefsNum() != 0 {
			return DispatchAccount{}, fmt.Errorf("%w: invalid dispatch queue common prefix", ErrInvalidInput)
		}
		if err = accountKey.StoreBuilder(common.ToBuilder()); err != nil {
			return DispatchAccount{}, fmt.Errorf("%w: append dispatch account prefix: %v", ErrInvalidInput, err)
		}
		if common.BitsSize() == remaining {
			if accountKey.BitsUsed() != 256 {
				return DispatchAccount{}, fmt.Errorf("%w: dispatch account key has %d bits", ErrInvalidInput, accountKey.BitsUsed())
			}
			key := accountKey.EndCell().MustBeginParse()
			keyBytes, loadErr := key.LoadSlice(256)
			if loadErr != nil || key.BitsLeft() != 0 || key.RefsNum() != 0 {
				return DispatchAccount{}, fmt.Errorf("%w: invalid dispatch account key", ErrInvalidInput)
			}
			var selected DispatchAccount
			copy(selected.AccountID[:], keyBytes)
			return selected, nil
		}

		// The fork augmentation is min(left, right). Prefer the left branch
		// when both sides contain the same minimum, which gives the canonical
		// lexicographically smallest account.
		leftPrefix := common.ToBuilder().MustStoreUInt(0, 1).EndCell()
		left := current.Copy()
		ok, cutErr := left.CutPrefixSubdict(leftPrefix, true)
		if cutErr != nil {
			return DispatchAccount{}, fmt.Errorf("%w: cut left dispatch queue branch: %v", ErrInvalidInput, cutErr)
		}
		if !ok || left.IsEmpty() {
			return DispatchAccount{}, fmt.Errorf("%w: left dispatch queue branch is absent", ErrInvalidInput)
		}
		leftExtra, loadErr := left.LoadRootExtra()
		if loadErr != nil {
			return DispatchAccount{}, fmt.Errorf("%w: load left dispatch queue augmentation: %v", ErrInvalidInput, loadErr)
		}
		leftMinimum, loadErr := dispatchMinimumLT(leftExtra)
		if loadErr != nil {
			return DispatchAccount{}, fmt.Errorf("%w: left dispatch queue augmentation: %v", ErrInvalidInput, loadErr)
		}

		branch := uint64(0)
		if leftMinimum == minimumLT {
			current = left
		} else {
			branch = 1
			rightPrefix := common.ToBuilder().MustStoreUInt(1, 1).EndCell()
			ok, cutErr = current.CutPrefixSubdict(rightPrefix, true)
			if cutErr != nil {
				return DispatchAccount{}, fmt.Errorf("%w: cut right dispatch queue branch: %v", ErrInvalidInput, cutErr)
			}
			if !ok || current.IsEmpty() {
				return DispatchAccount{}, fmt.Errorf("%w: right dispatch queue branch is absent", ErrInvalidInput)
			}
			rightExtra, rightErr := current.LoadRootExtra()
			if rightErr != nil {
				return DispatchAccount{}, fmt.Errorf("%w: load right dispatch queue augmentation: %v", ErrInvalidInput, rightErr)
			}
			rightMinimum, rightErr := dispatchMinimumLT(rightExtra)
			if rightErr != nil {
				return DispatchAccount{}, fmt.Errorf("%w: right dispatch queue augmentation: %v", ErrInvalidInput, rightErr)
			}
			if rightMinimum != minimumLT {
				return DispatchAccount{}, fmt.Errorf("%w: right dispatch queue branch does not contain the root minimum", ErrInvalidInput)
			}
		}
		if err = accountKey.StoreUInt(branch, 1); err != nil {
			return DispatchAccount{}, fmt.Errorf("%w: append dispatch account branch: %v", ErrInvalidInput, err)
		}
	}
}

func dispatchMinimumLT(extra *cell.Slice) (uint64, error) {
	if extra == nil {
		return 0, fmt.Errorf("augmentation is absent")
	}
	lt, err := extra.LoadUInt(64)
	if err != nil || extra.BitsLeft() != 0 || extra.RefsNum() != 0 {
		return 0, fmt.Errorf("expected exactly one uint64")
	}
	return lt, nil
}

func loadAccountDispatchQueue(queue *tlb.DispatchQueueAugDict, accountID [32]byte) (*tlb.AccountDispatchQueue, error) {
	value, err := queue.LoadValue(dispatchAccountKey(accountID))
	if err != nil {
		return nil, err
	}

	var accountQueue tlb.AccountDispatchQueue
	if err = loadExactSlice(&accountQueue, value); err != nil {
		return nil, err
	}
	return &accountQueue, nil
}

func (c *collation) loadPredecessorDispatchValue(accountID [32]byte) (*cell.Slice, error) {
	prefix := binary.BigEndian.Uint64(accountID[:8])
	for i := 0; i < c.dispatchSourceCount; i++ {
		source := c.dispatchSources[i]
		shard := msgpool.ShardIdent{
			Workchain: source.shard.WorkchainID,
			Shard:     uint64(source.shard.GetShardID()),
		}
		if !shard.Contains(c.shard.Workchain, prefix) {
			continue
		}
		if source.queue == nil {
			return nil, cell.ErrNoSuchKeyInDict
		}
		return source.queue.LoadValue(dispatchAccountKey(accountID))
	}

	return nil, fmt.Errorf("%w: dispatch account %x is outside predecessor shards", ErrInvalidInput, accountID)
}

func (c *collation) loadPredecessorDispatchAccount(accountID [32]byte) (*tlb.AccountDispatchQueue, error) {
	value, err := c.loadPredecessorDispatchValue(accountID)
	if err != nil {
		return nil, err
	}

	var account tlb.AccountDispatchQueue
	if err = loadExactSlice(&account, value); err != nil {
		return nil, err
	}
	return &account, nil
}

func minimumDispatchLT(queue *tlb.AccountDispatchQueue) (uint64, error) {
	key, _, err := queue.Messages.LoadMin()
	if err != nil {
		return 0, err
	}
	loader := key.MustBeginParse()
	lt, err := loader.LoadUInt(64)
	if err != nil || loader.BitsLeft() != 0 || loader.RefsNum() != 0 {
		return 0, fmt.Errorf("invalid dispatch message key")
	}
	return lt, nil
}

func updateDispatchScanQueue(
	queue *tlb.DispatchQueueAugDict,
	accountID [32]byte,
	accountQueue *tlb.AccountDispatchQueue,
	lt uint64,
	removeAccount bool,
) error {
	if removeAccount {
		return queue.Delete(dispatchAccountKey(accountID))
	}

	if _, err := accountQueue.Messages.LoadValueAndDelete(dispatchLTKey(lt)); err != nil {
		return err
	}
	accountQueue.Count--
	return storeAccountDispatchQueue(queue.AugmentedDictionary, accountID, accountQueue)
}

func (c *collation) processDeferredMessage(source DispatchAccount, lt uint64) (*tlb.MsgMetadata, error) {
	enqueued, err := c.removeDispatchEntry(source.AccountID, lt)
	if err != nil {
		return nil, err
	}
	if err = c.registerDispatchOp(false); err != nil {
		return nil, err
	}
	c.senderGenerated[source.AccountID]++

	if enqueued.EnqueuedLT != lt {
		return nil, fmt.Errorf("%w: dispatch message %x:%d has enqueued lt %d", ErrInvalidInput, source.AccountID, lt, enqueued.EnqueuedLT)
	}
	if enqueued.Msg.Level() != 0 {
		return nil, fmt.Errorf("%w: dispatch message %x:%d envelope has non-zero level", ErrInvalidInput, source.AccountID, lt)
	}

	var envelope tlb.MsgEnvelope
	if err = parseExact(&envelope, enqueued.Msg); err != nil {
		return nil, fmt.Errorf("%w: decode dispatch envelope %x:%d: %v", ErrInvalidInput, source.AccountID, lt, err)
	}
	if envelope.EmittedLT != nil {
		return nil, fmt.Errorf("%w: dispatch envelope %x:%d already has emitted lt", ErrInvalidInput, source.AccountID, lt)
	}
	if envelope.CurAddr.Type != tlb.IntermediateAddressRegular || envelope.CurAddr.UseDestBits != 0 ||
		envelope.NextAddr.Type != tlb.IntermediateAddressRegular || envelope.NextAddr.UseDestBits != 0 {
		return nil, fmt.Errorf("%w: dispatch envelope %x:%d has non-zero routing addresses", ErrInvalidInput, source.AccountID, lt)
	}

	prepared, err := tvm.PrepareMessage(envelope.Msg)
	if err != nil {
		return nil, fmt.Errorf("%w: prepare dispatch message %x:%d: %v", ErrInvalidInput, source.AccountID, lt, err)
	}
	message := prepared.Message()
	if message.MsgType != tlb.MsgTypeInternal {
		return nil, fmt.Errorf("%w: dispatch message %x:%d is not internal", ErrInvalidInput, source.AccountID, lt)
	}
	internal := message.AsInternal()
	if internal.CreatedLT != lt {
		return nil, fmt.Errorf("%w: dispatch message %x:%d has created lt %d", ErrInvalidInput, source.AccountID, lt, internal.CreatedLT)
	}
	if envelope.FwdFeeRemaining.Nano().Cmp(internal.FwdFee.Nano()) > 0 {
		return nil, fmt.Errorf("%w: dispatch message %x:%d remaining fee exceeds forward fee", ErrInvalidInput, source.AccountID, lt)
	}

	sourcePrefix, err := accountPrefixFromAddress(internal.SrcAddr)
	if err != nil {
		return nil, fmt.Errorf("%w: dispatch message %x:%d source: %v", ErrInvalidInput, source.AccountID, lt, err)
	}
	if _, err = accountPrefixFromAddress(internal.DstAddr); err != nil {
		return nil, fmt.Errorf("%w: dispatch message %x:%d destination: %v", ErrInvalidInput, source.AccountID, lt, err)
	}
	messageSourceID, err := accountIDFromAddress(internal.SrcAddr)
	if err != nil || sourcePrefix.Workchain != source.Workchain || messageSourceID != source.AccountID || !c.shard.ContainsPrefix(sourcePrefix) {
		return nil, fmt.Errorf("%w: dispatch message %x:%d source differs from its queue account", ErrInvalidInput, source.AccountID, lt)
	}

	emittedBase := max(c.header.StartLt, c.lastDispatchEmitted[source.AccountID])
	if emittedBase == math.MaxUint64 {
		return nil, fmt.Errorf("%w: dispatch emitted lt overflow for %x", ErrInvalidInput, source.AccountID)
	}
	emittedLT := emittedBase + 1
	if lane := c.lanes[source.AccountID]; lane != nil {
		accountLT := lane.current.State().LastTransactionLT
		if accountLT == math.MaxUint64 {
			return nil, fmt.Errorf("%w: dispatch account lt overflow for %x", ErrInvalidInput, source.AccountID)
		}
		emittedLT = max(emittedLT, accountLT+1)
	}
	if emittedLT == math.MaxUint64 {
		return nil, fmt.Errorf("%w: dispatch block end lt overflow for %x", ErrInvalidInput, source.AccountID)
	}
	c.lastDispatchEmitted[source.AccountID] = emittedLT
	c.maxLT = max(c.maxLT, emittedLT+1)

	envelope.EmittedLT = &emittedLT
	emittedEnvelope, err := envelope.ToCell()
	if err != nil {
		return nil, fmt.Errorf("serialize emitted dispatch envelope %x:%d: %w", source.AccountID, lt, err)
	}
	if err = c.limits.storage.AddCell(emittedEnvelope); err != nil {
		return nil, err
	}

	hash := envelope.Msg.HashKey()
	c.new.push(newMessage{
		lt:               emittedLT,
		hash:             hash,
		root:             envelope.Msg,
		parsed:           message,
		metadata:         envelope.Metadata,
		dispatchEnvelope: emittedEnvelope,
	})
	c.limits.extraOutMsgs++
	c.unprocessedDeferred[source.AccountID]++
	c.stats.DispatchedMessages++

	return envelope.Metadata, nil
}

func (c *collation) removeDispatchEntry(accountID [32]byte, lt uint64) (*tlb.EnqueuedMsg, error) {
	if err := c.traceDispatchMutation(accountID); err != nil {
		return nil, err
	}
	oldAccount := c.oldDispatchAccounts[accountID]
	if oldAccount == nil {
		return nil, fmt.Errorf("%w: predecessor dispatch account %x is absent", ErrInvalidInput, accountID)
	}
	oldValue, err := oldAccount.Messages.LoadValue(dispatchLTKey(lt))
	if err != nil {
		return nil, fmt.Errorf("%w: load predecessor dispatch message %x:%d: %v", ErrInvalidInput, accountID, lt, err)
	}

	accountQueue, err := loadAccountDispatchQueue(c.dispatchQueue, accountID)
	if err != nil {
		return nil, fmt.Errorf("%w: load dispatch account %x: %v", ErrInvalidInput, accountID, err)
	}
	_, err = accountQueue.Messages.LoadValueAndDelete(dispatchLTKey(lt))
	if err != nil {
		return nil, fmt.Errorf("%w: remove dispatch message %x:%d: %v", ErrInvalidInput, accountID, lt, err)
	}
	var enqueued tlb.EnqueuedMsg
	if err = loadExactSlice(&enqueued, oldValue); err != nil {
		return nil, fmt.Errorf("%w: decode dispatch message %x:%d: %v", ErrInvalidInput, accountID, lt, err)
	}

	accountQueue.Count--
	if err = storeAccountDispatchQueue(c.dispatchQueue.AugmentedDictionary, accountID, accountQueue); err != nil {
		return nil, fmt.Errorf("%w: store dispatch account %x: %v", ErrInvalidInput, accountID, err)
	}
	return &enqueued, nil
}

func (c *collation) traceDispatchMutation(accountID [32]byte) error {
	if _, traced := c.dispatchChanged[accountID]; traced {
		return nil
	}

	accountQueue, err := c.loadPredecessorDispatchAccount(accountID)
	if err == nil {
		// DispatchQueue diff validation checks that removals and additions form a
		// boundary by reading the predecessor maximum once for this account.
		if _, _, err = accountQueue.Messages.LoadMax(); err != nil {
			return fmt.Errorf("%w: load predecessor dispatch account %x maximum: %v", ErrInvalidInput, accountID, err)
		}
	} else if !isMissingKey(err) {
		return fmt.Errorf("%w: load predecessor dispatch account %x: %v", ErrInvalidInput, accountID, err)
	}

	if c.dispatchChanged == nil {
		c.dispatchChanged = make(map[[32]byte]struct{})
	}
	if c.oldDispatchAccounts == nil {
		c.oldDispatchAccounts = make(map[[32]byte]*tlb.AccountDispatchQueue)
	}
	c.oldDispatchAccounts[accountID] = accountQueue
	c.dispatchChanged[accountID] = struct{}{}
	return nil
}

func storeAccountDispatchQueue(
	queue *cell.AugmentedDictionary,
	accountID [32]byte,
	accountQueue *tlb.AccountDispatchQueue,
) error {
	if accountQueue.Count == 0 {
		if !accountQueue.Messages.IsEmpty() {
			return fmt.Errorf("account dispatch count reached zero with messages remaining")
		}
		return queue.Delete(dispatchAccountKey(accountID))
	}

	value, err := accountQueue.ToCell()
	if err != nil {
		return err
	}
	return queue.Set(dispatchAccountKey(accountID), value)
}

func (c *collation) registerDispatchOp(force bool) error {
	c.dispatchOps++
	if force || c.dispatchOps&63 == 0 {
		return c.limits.addProof(c.dispatchQueue.RootCell())
	}
	return nil
}

func dispatchInitiatorFromMetadata(metadata *tlb.MsgMetadata) (dispatchInitiator, error) {
	accountID, err := accountIDFromAddress(metadata.Initiator)
	if err != nil {
		return dispatchInitiator{}, fmt.Errorf("%w: dispatch metadata initiator: %v", ErrInvalidInput, err)
	}
	return dispatchInitiator{
		workchain:   metadata.Initiator.Workchain(),
		accountID:   accountID,
		initiatorLT: metadata.InitiatorLT,
	}, nil
}

func canonicalDispatchAccounts(accounts []DispatchAccount) []DispatchAccount {
	ordered := slices.Clone(accounts)
	slices.SortFunc(ordered, compareDispatchAccounts)
	return slices.Compact(ordered)
}

func dispatchAccountListed(accounts []DispatchAccount, target DispatchAccount) bool {
	_, found := slices.BinarySearchFunc(accounts, target, compareDispatchAccounts)
	return found
}

func compareDispatchAccounts(left, right DispatchAccount) int {
	if left.Workchain < right.Workchain {
		return -1
	}
	if left.Workchain > right.Workchain {
		return 1
	}
	return bytes.Compare(left.AccountID[:], right.AccountID[:])
}

func dispatchAccountKey(accountID [32]byte) *cell.Cell {
	return cell.BeginCell().MustStoreSlice(accountID[:], 256).EndCell()
}

func dispatchLTKey(lt uint64) *cell.Cell {
	return cell.BeginCell().MustStoreUInt(lt, 64).EndCell()
}

func (c *collation) isMasterSpecialAccount(accountID [32]byte) (bool, error) {
	if c.shard.Workchain != address.MasterchainID {
		return false, nil
	}
	set, err := c.masterSpecialAccountIDs()
	if err != nil {
		return false, err
	}
	_, special := set[accountID]

	return special, nil
}

func (c *collation) shouldDeferGenerated(
	message *newMessage,
	sourcePrefix msgpool.AccountPrefix,
	sourceID [32]byte,
) (bool, error) {
	source := DispatchAccount{Workchain: sourcePrefix.Workchain, AccountID: sourceID}
	policy := &c.req.dispatch
	if c.config.capabilities&capDeferMessages != 0 && policy.DeferringEnabled && message.index != 0 {
		special, err := c.isMasterSpecialAccount(sourceID)
		if err != nil {
			return false, err
		}
		if !special && !dispatchAccountListed(policy.Whitelist, source) {
			c.senderGenerated[sourceID]++
			deferQueueLimit := max(policy.DeferOutQueueSizeLimit, c.config.deferOutQueueSizeLimit)
			if c.senderGenerated[sourceID] >= policy.DeferMessagesAfter || c.queueSize > deferQueueLimit {
				return true, nil
			}
		}
	}

	if c.unprocessedDeferred[sourceID] != 0 {
		return true, nil
	}
	_, err := c.dispatchQueue.LoadValue(dispatchAccountKey(sourceID))
	if err == nil {
		return true, nil
	}
	if !isMissingKey(err) {
		return false, fmt.Errorf("%w: lookup dispatch account %x: %v", ErrInvalidInput, sourceID, err)
	}
	return false, nil
}

func (c *collation) deferGenerated(message *newMessage, sourceID [32]byte) error {
	internal := message.parsed.AsInternal()
	envelope, err := (tlb.MsgEnvelope{
		CurAddr:         tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular},
		NextAddr:        tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular},
		FwdFeeRemaining: internal.FwdFee,
		Msg:             message.root,
		Metadata:        message.metadata,
	}).ToCell()
	if err != nil {
		return fmt.Errorf("serialize deferred message envelope %x: %w", message.hash, err)
	}
	out, err := descriptor(0b10100, 5, envelope, message.transaction) // msg_export_new_defer$10100
	if err != nil {
		return err
	}
	if err = c.insert(c.outMessages.AugmentedDictionary, &c.outDescr, message.root, out); err != nil {
		return err
	}

	enqueued, err := (tlb.EnqueuedMsg{EnqueuedLT: message.lt, Msg: envelope}).ToCell()
	if err != nil {
		return err
	}
	if err = c.traceDispatchMutation(sourceID); err != nil {
		return err
	}
	accountQueue, err := loadAccountDispatchQueue(c.dispatchQueue, sourceID)
	if isMissingKey(err) {
		accountQueue = &tlb.AccountDispatchQueue{Messages: cell.NewDict(64)}
	} else if err != nil {
		return fmt.Errorf("%w: load dispatch account %x: %v", ErrInvalidInput, sourceID, err)
	}
	if accountQueue.Count == maxOutMsgQueueSize {
		return fmt.Errorf("%w: dispatch account %x count overflow", ErrInvalidInput, sourceID)
	}
	inserted, err := accountQueue.Messages.SetWithMode(dispatchLTKey(message.lt), enqueued, cell.DictSetModeAdd)
	if err != nil {
		return fmt.Errorf("defer generated message %x: %w", message.hash, err)
	}
	if !inserted {
		return fmt.Errorf("%w: duplicate dispatch message lt %x:%d", ErrInvalidInput, sourceID, message.lt)
	}
	accountQueue.Count++
	if err = storeAccountDispatchQueue(c.dispatchQueue.AugmentedDictionary, sourceID, accountQueue); err != nil {
		return fmt.Errorf("store dispatch account %x: %w", sourceID, err)
	}

	c.stats.DeferredMessages++
	return c.registerDispatchOp(false)
}
