package collator

import (
	"fmt"
	"math/big"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"

	sharddomain "github.com/xssnick/gton/service/shard"
	"github.com/xssnick/gton/service/validator/msgpool"
)

const shardStateSplitTag = uint64(0x5f327da5)

type preparedPredecessor struct {
	topology            shardTopology
	oldRoot             *cell.Cell
	accountSources      [2]predecessorAccountSource
	dispatchSources     [2]predecessorDispatchSource
	dispatchSourceCount int
	state               tlb.ShardStateUnsplit
	stats               tlb.ShardStateStats
	queueInfo           tlb.OutMsgQueueInfo
	queueSize           uint64
	firstGenLT          uint64
	secondGenLT         uint64
	firstMasterRef      *tlb.ExtBlkRef
	secondMasterRef     *tlb.ExtBlkRef
}

// predecessorAccountSource preserves one dictionary in the shape in which it
// is committed by the predecessor root. Split and merge preparation rebuilds
// the effective dictionary, whose paths cannot be used to mark the original
// state proof.
type predecessorAccountSource struct {
	shard    tlb.ShardIdent
	accounts *tlb.ShardAccountsAugDict
}

// predecessorDispatchSource keeps the dictionary path committed by a real
// predecessor state. Split and merge preparation rebuild effective roots;
// values loaded through those roots must not become the proof provenance.
type predecessorDispatchSource struct {
	shard tlb.ShardIdent
	queue *tlb.DispatchQueueAugDict
}

// openPredecessorReadSet opens the recorder over the tree the collation reads
// its predecessor through: one previous state, or the mechanical split cell that
// presents both merge parents under a single root. The read set must be opened
// over exactly this cell, because the state update is built from its source and
// a merge update is rooted at the split cell rather than at either parent.
func openPredecessorReadSet(req ShardRequest, cellHint int) (*cell.ReadSet, error) {
	if req.Previous.State == nil {
		return nil, fmt.Errorf("%w: first predecessor state is absent", ErrInvalidInput)
	}
	if req.Previous2 == nil {
		return cell.NewReadSetSized(req.Previous.State, cellHint), nil
	}
	if req.Previous2.State == nil {
		return nil, fmt.Errorf("%w: second predecessor state is absent", ErrInvalidInput)
	}
	splitRoot, err := mechanicalSplitState(req.Previous.State, req.Previous2.State)
	if err != nil {
		return nil, err
	}
	return cell.NewReadSetSized(splitRoot, cellHint), nil
}

func preparePredecessor(req ShardRequest, read *cell.ReadSet) (preparedPredecessor, error) {
	if req.Previous.State == nil {
		return preparedPredecessor{}, fmt.Errorf("%w: first predecessor state is absent", ErrInvalidInput)
	}

	oldRoot := read.Root()
	if req.Previous2 == nil {
		var state tlb.ShardStateUnsplit
		if err := parseExact(&state, oldRoot); err != nil {
			return preparedPredecessor{}, fmt.Errorf("%w: decode predecessor state: %v", ErrInvalidInput, err)
		}
		topology, err := resolveShardTopologyState(req, &state, nil)
		if err != nil {
			return preparedPredecessor{}, err
		}

		prepared, err := prepareSinglePredecessor(req, topology, oldRoot, state)
		if err != nil {
			return preparedPredecessor{}, err
		}
		return prepared, nil
	}

	if req.Previous2.State == nil {
		return preparedPredecessor{}, fmt.Errorf("%w: second predecessor state is absent", ErrInvalidInput)
	}
	var states tlb.ShardStateSplit
	if err := parseExact(&states, oldRoot); err != nil {
		return preparedPredecessor{}, fmt.Errorf("%w: decode merge predecessor states: %v", ErrInvalidInput, err)
	}
	topology, err := resolveShardTopologyState(req, &states.Left, &states.Right)
	if err != nil {
		return preparedPredecessor{}, err
	}

	return mergePredecessors(req, topology, oldRoot, states.Left, states.Right)
}

func prepareSinglePredecessor(
	req ShardRequest,
	topology shardTopology,
	oldRoot *cell.Cell,
	state tlb.ShardStateUnsplit,
) (preparedPredecessor, error) {
	stats, queueInfo, queueSize, err := parsePredecessorParts(&req.Previous, &state)
	if err != nil {
		return preparedPredecessor{}, err
	}

	prepared := preparedPredecessor{
		topology: topology,
		oldRoot:  oldRoot,
		accountSources: [2]predecessorAccountSource{{
			shard:    state.ShardIdent,
			accounts: state.Accounts.ShardAccounts,
		}},
		dispatchSources: [2]predecessorDispatchSource{{
			shard: state.ShardIdent,
			queue: predecessorDispatchQueue(&queueInfo),
		}},
		dispatchSourceCount: 1,
		state:               state,
		stats:               stats,
		queueInfo:           queueInfo,
		queueSize:           queueSize,
		firstGenLT:          state.GenLT,
		firstMasterRef:      stats.MasterRef,
	}
	if topology.kind == topologyAfterSplit {
		if err = splitPredecessor(&prepared); err != nil {
			return preparedPredecessor{}, err
		}
	}

	return prepared, nil
}

func parsePredecessorParts(
	previous *PreviousBlock,
	state *tlb.ShardStateUnsplit,
) (tlb.ShardStateStats, tlb.OutMsgQueueInfo, uint64, error) {
	if state.McStateExtra != nil {
		return tlb.ShardStateStats{}, tlb.OutMsgQueueInfo{}, 0,
			fmt.Errorf("%w: shardchain predecessor contains masterchain state extra", ErrInvalidInput)
	}
	// C++ ShardState::unpack_state validates the HashmapAugE wrapper and its
	// root extra before it reads the shard prefix or root balance. Preserve that
	// root read in the predecessor usage proof; descendants are opened only by
	// the later prefix, split, or changed-account operations.
	if err := state.Accounts.ShardAccounts.Validate(); err != nil {
		return tlb.ShardStateStats{}, tlb.OutMsgQueueInfo{}, 0,
			fmt.Errorf("%w: invalid predecessor account dictionary: %v", ErrInvalidInput, err)
	}
	if err := verifyAccountsShardPrefix(state.ShardIdent, state.Accounts.ShardAccounts); err != nil {
		return tlb.ShardStateStats{}, tlb.OutMsgQueueInfo{}, 0, err
	}

	var stats tlb.ShardStateStats
	if err := parseExact(&stats, state.Stats); err != nil {
		return tlb.ShardStateStats{}, tlb.OutMsgQueueInfo{}, 0,
			fmt.Errorf("%w: decode shard state statistics: %v", ErrInvalidInput, err)
	}
	if !stats.Libraries.IsEmpty() {
		return tlb.ShardStateStats{}, tlb.OutMsgQueueInfo{}, 0,
			fmt.Errorf("%w: shardchain state has public libraries", ErrInvalidInput)
	}
	if !stats.TotalValidatorFees.ExtraCurrencies.IsEmpty() {
		return tlb.ShardStateStats{}, tlb.OutMsgQueueInfo{}, 0,
			fmt.Errorf("%w: validator fees contain extra currencies", ErrInvalidInput)
	}
	accountsBalance, err := loadAccountsBalance(state.Accounts.ShardAccounts)
	if err != nil {
		return tlb.ShardStateStats{}, tlb.OutMsgQueueInfo{}, 0,
			fmt.Errorf("%w: decode predecessor account balance: %v", ErrInvalidInput, err)
	}
	if !accountsBalance.Equals(stats.TotalBalance) {
		return tlb.ShardStateStats{}, tlb.OutMsgQueueInfo{}, 0,
			fmt.Errorf("%w: predecessor account balance differs from state statistics", ErrInvalidInput)
	}

	var queueInfo tlb.OutMsgQueueInfo
	if err := parseExact(&queueInfo, state.OutMsgQueueInfo); err != nil {
		return tlb.ShardStateStats{}, tlb.OutMsgQueueInfo{}, 0,
			fmt.Errorf("%w: decode outbound queue: %v", ErrInvalidInput, err)
	}
	// C++ constructs both augmented dictionaries while unpacking the state.
	// Their validate() opens only the trie root and checks its stored extra;
	// ordinary queue operations may otherwise leave an unchanged root pruned.
	if err := queueInfo.OutQueue.Validate(); err != nil {
		return tlb.ShardStateStats{}, tlb.OutMsgQueueInfo{}, 0,
			fmt.Errorf("%w: invalid predecessor outbound queue root: %v", ErrInvalidInput, err)
	}
	if queueInfo.Extra != nil {
		if err := queueInfo.Extra.DispatchQueue.Validate(); err != nil {
			return tlb.ShardStateStats{}, tlb.OutMsgQueueInfo{}, 0,
				fmt.Errorf("%w: invalid predecessor dispatch queue root: %v", ErrInvalidInput, err)
		}
	}
	queueSize, err := existingQueueSize(&queueInfo, previous.OutQueueSize)
	if err != nil {
		return tlb.ShardStateStats{}, tlb.OutMsgQueueInfo{}, 0, err
	}

	return stats, queueInfo, queueSize, nil
}

func splitPredecessor(prepared *preparedPredecessor) error {
	state := &prepared.state
	target := prepared.topology.target
	prefix := shardPrefixCell(target)

	accounts := &tlb.ShardAccountsAugDict{AugmentedDictionary: state.Accounts.ShardAccounts.Copy()}
	if ok, err := accounts.CutPrefixSubdict(prefix, false); err != nil || !ok {
		return fmt.Errorf("%w: split account dictionary", ErrInvalidInput)
	}
	if ok, err := accounts.HasCommonPrefix(prefix); err != nil || !ok {
		return fmt.Errorf("%w: split account dictionary has the wrong prefix", ErrInvalidInput)
	}

	outQueue := &tlb.OutMsgQueueAugDict{AugmentedDictionary: prepared.queueInfo.OutQueue.Copy()}
	queueSize, err := filterOutQueue(outQueue, state.ShardIdent, target)
	if err != nil {
		return err
	}
	processed, err := splitProcessed(prepared.queueInfo.ProcInfo, state.ShardIdent, target)
	if err != nil {
		return err
	}

	dispatch, err := copiedDispatchQueue(&prepared.queueInfo)
	if err != nil {
		return err
	}
	if ok, cutErr := dispatch.CutPrefixSubdict(prefix, false); cutErr != nil || !ok {
		return fmt.Errorf("%w: split dispatch queue", ErrInvalidInput)
	}
	if ok, prefixErr := dispatch.HasCommonPrefix(prefix); prefixErr != nil || !ok {
		return fmt.Errorf("%w: split dispatch queue has the wrong prefix", ErrInvalidInput)
	}

	prepared.stats.TotalBalance, err = loadAccountsBalance(accounts)
	if err != nil {
		return err
	}
	prepared.stats.TotalValidatorFees, err = splitValidatorFees(
		prepared.stats.TotalValidatorFees,
		uint64(target.GetShardID()) > uint64(state.ShardIdent.GetShardID()),
	)
	if err != nil {
		return err
	}
	prepared.stats.OverloadHistory = 0
	prepared.stats.UnderloadHistory = 0

	prepared.queueInfo = tlb.OutMsgQueueInfo{
		OutQueue: outQueue,
		ProcInfo: processed,
		Extra: &tlb.OutMsgQueueExtra{
			DispatchQueue: dispatch,
			OutQueueSize:  &queueSize,
		},
	}
	prepared.queueSize = queueSize
	state.ShardIdent = target
	state.BeforeSplit = false
	state.Accounts.ShardAccounts = accounts
	state.Stats, err = prepared.stats.ToCell()
	if err != nil {
		return fmt.Errorf("serialize split state statistics: %w", err)
	}
	state.OutMsgQueueInfo, err = prepared.queueInfo.ToCell()
	if err != nil {
		return fmt.Errorf("serialize split outbound queue: %w", err)
	}

	return nil
}

func mergePredecessors(
	req ShardRequest,
	topology shardTopology,
	oldRoot *cell.Cell,
	left tlb.ShardStateUnsplit,
	right tlb.ShardStateUnsplit,
) (preparedPredecessor, error) {
	leftStats, leftQueue, leftSize, err := parsePredecessorParts(&req.Previous, &left)
	if err != nil {
		return preparedPredecessor{}, err
	}
	rightStats, rightQueue, rightSize, err := parsePredecessorParts(req.Previous2, &right)
	if err != nil {
		return preparedPredecessor{}, err
	}
	// The complete queue is scanned after a merge. That work stays bounded only
	// while both children satisfy the protocol merge threshold.
	if leftSize > mergeMaxQueueSize || rightSize > mergeMaxQueueSize {
		return preparedPredecessor{}, fmt.Errorf("%w: merge predecessor outbound queue exceeds the merge limit", ErrInvalidInput)
	}

	accounts := &tlb.ShardAccountsAugDict{AugmentedDictionary: left.Accounts.ShardAccounts.Copy()}
	if ok, combineErr := accounts.CombineWith(right.Accounts.ShardAccounts.AugmentedDictionary); combineErr != nil || !ok {
		return preparedPredecessor{}, fmt.Errorf("%w: merge account dictionaries", ErrInvalidInput)
	}
	prefix := shardPrefixCell(topology.target)
	if ok, prefixErr := accounts.HasCommonPrefix(prefix); prefixErr != nil || !ok {
		return preparedPredecessor{}, fmt.Errorf("%w: merged account dictionary has the wrong prefix", ErrInvalidInput)
	}

	totalBalance, err := leftStats.TotalBalance.Add(rightStats.TotalBalance)
	if err != nil {
		return preparedPredecessor{}, fmt.Errorf("%w: merge predecessor balances: %v", ErrInvalidInput, err)
	}
	accountBalance, err := loadAccountsBalance(accounts)
	if err != nil {
		return preparedPredecessor{}, err
	}
	if !accountBalance.Equals(totalBalance) {
		return preparedPredecessor{}, fmt.Errorf("%w: merged account balance differs from predecessor totals", ErrInvalidInput)
	}
	validatorFees, err := leftStats.TotalValidatorFees.Add(rightStats.TotalValidatorFees)
	if err != nil {
		return preparedPredecessor{}, fmt.Errorf("%w: merge validator fees: %v", ErrInvalidInput, err)
	}

	outQueue := &tlb.OutMsgQueueAugDict{AugmentedDictionary: leftQueue.OutQueue.Copy()}
	if ok, combineErr := outQueue.CombineWith(rightQueue.OutQueue.AugmentedDictionary); combineErr != nil || !ok {
		return preparedPredecessor{}, fmt.Errorf("%w: merge outbound queues", ErrInvalidInput)
	}
	processed, err := mergeProcessed(leftQueue.ProcInfo, left.ShardIdent, rightQueue.ProcInfo, right.ShardIdent)
	if err != nil {
		return preparedPredecessor{}, err
	}
	leftDispatch, err := copiedDispatchQueue(&leftQueue)
	if err != nil {
		return preparedPredecessor{}, err
	}
	rightDispatch, err := copiedDispatchQueue(&rightQueue)
	if err != nil {
		return preparedPredecessor{}, err
	}
	if ok, combineErr := leftDispatch.CombineWith(rightDispatch.AugmentedDictionary); combineErr != nil || !ok {
		return preparedPredecessor{}, fmt.Errorf("%w: merge dispatch queues", ErrInvalidInput)
	}

	queueSize := leftSize + rightSize
	if queueSize > maxOutMsgQueueSize || queueSize < leftSize {
		return preparedPredecessor{}, fmt.Errorf("%w: merged outbound queue size overflow", ErrInvalidInput)
	}
	var storedQueueSize *uint64
	if queueStoresSize(&leftQueue) && queueStoresSize(&rightQueue) {
		storedQueueSize = &queueSize
	}
	var queueExtra *tlb.OutMsgQueueExtra
	if !leftDispatch.IsEmpty() || storedQueueSize != nil {
		queueExtra = &tlb.OutMsgQueueExtra{DispatchQueue: leftDispatch, OutQueueSize: storedQueueSize}
	}
	queueInfo := tlb.OutMsgQueueInfo{OutQueue: outQueue, ProcInfo: processed, Extra: queueExtra}

	stats := leftStats
	stats.OverloadHistory = 0
	stats.UnderloadHistory = 0
	stats.TotalBalance = totalBalance
	stats.TotalValidatorFees = validatorFees
	stats.MasterRef = newerMasterReference(leftStats.MasterRef, rightStats.MasterRef)
	statsCell, err := stats.ToCell()
	if err != nil {
		return preparedPredecessor{}, fmt.Errorf("serialize merged state statistics: %w", err)
	}
	queueCell, err := queueInfo.ToCell()
	if err != nil {
		return preparedPredecessor{}, fmt.Errorf("serialize merged outbound queue: %w", err)
	}

	state := left
	state.ShardIdent = topology.target
	state.Seqno = topology.seqno - 1
	state.VertSeqno = topology.previousVertSeqno
	state.GenUTime = topology.previousGenUtime
	state.GenLT = topology.previousGenLT
	state.MinRefMCSeqno = min(left.MinRefMCSeqno, right.MinRefMCSeqno)
	state.OutMsgQueueInfo = queueCell
	state.BeforeSplit = false
	state.Accounts.ShardAccounts = accounts
	state.Stats = statsCell
	state.McStateExtra = nil

	return preparedPredecessor{
		topology: topology,
		oldRoot:  oldRoot,
		accountSources: [2]predecessorAccountSource{
			{shard: left.ShardIdent, accounts: left.Accounts.ShardAccounts},
			{shard: right.ShardIdent, accounts: right.Accounts.ShardAccounts},
		},
		dispatchSources: [2]predecessorDispatchSource{
			{shard: left.ShardIdent, queue: predecessorDispatchQueue(&leftQueue)},
			{shard: right.ShardIdent, queue: predecessorDispatchQueue(&rightQueue)},
		},
		dispatchSourceCount: 2,
		state:               state,
		stats:               stats,
		queueInfo:           queueInfo,
		queueSize:           queueSize,
		firstGenLT:          left.GenLT,
		secondGenLT:         right.GenLT,
		firstMasterRef:      leftStats.MasterRef,
		secondMasterRef:     rightStats.MasterRef,
	}, nil
}

func predecessorDispatchQueue(info *tlb.OutMsgQueueInfo) *tlb.DispatchQueueAugDict {
	if info.Extra == nil {
		return nil
	}
	return info.Extra.DispatchQueue
}

// MergedPredecessorStates is the split_state wrapper a merge candidate's Merkle
// update is applied to: shard_state_split with the two children as references,
// in order.
//
// It is exported because the consensus runtime builds the very same cell for
// the same purpose — it is the root of a ChainState with two tips — and the two
// have to be the same bytes or the two sides disagree about which tree a merge
// candidate is a successor of. Sharing the constructor is what makes that
// agreement structural instead of a comment in two places.
func MergedPredecessorStates(left, right *cell.Cell) (*cell.Cell, error) {
	builder := cell.BeginCell().MustStoreUInt(shardStateSplitTag, 32)
	if err := builder.StoreRef(left); err != nil {
		return nil, fmt.Errorf("%w: store left merge state: %v", ErrInvalidInput, err)
	}
	if err := builder.StoreRef(right); err != nil {
		return nil, fmt.Errorf("%w: store right merge state: %v", ErrInvalidInput, err)
	}
	return builder.EndCell(), nil
}

func mechanicalSplitState(left, right *cell.Cell) (*cell.Cell, error) {
	return MergedPredecessorStates(left, right)
}

func shardPrefixCell(shard tlb.ShardIdent) *cell.Cell {
	if shard.PrefixBits == 0 {
		return cell.BeginCell().EndCell()
	}
	prefix := shard.ShardPrefix >> (64 - uint(shard.PrefixBits))
	return cell.BeginCell().MustStoreUInt(prefix, uint(shard.PrefixBits)).EndCell()
}

func verifyAccountsShardPrefix(shard tlb.ShardIdent, accounts *tlb.ShardAccountsAugDict) error {
	// The two absences are different states of the world and only one of them
	// can come out of a decode: every ShardAccounts parse either fails or leaves
	// the dictionary set, including for an empty shard and for a state pruned
	// down to its root. A nil field therefore means the state was assembled
	// rather than decoded, which is worth saying out loud instead of folding
	// into one message.
	switch {
	case accounts == nil:
		return fmt.Errorf("%w: shard accounts field was never decoded", ErrInvalidInput)
	case accounts.AugmentedDictionary == nil:
		return fmt.Errorf("%w: shard accounts dictionary is absent", ErrInvalidInput)
	}
	ok, err := accounts.HasCommonPrefix(shardPrefixCell(shard))
	if err != nil {
		return fmt.Errorf("%w: check shard accounts prefix: %v", ErrInvalidInput, err)
	}
	if !ok {
		return fmt.Errorf("%w: account lies outside the state shard", ErrInvalidInput)
	}

	return nil
}

func filterOutQueue(queue *tlb.OutMsgQueueAugDict, owner, target tlb.ShardIdent) (uint64, error) {
	var size uint64
	_, err := queue.Filter(func(value, _ *cell.Slice, key *cell.Cell) (cell.DictFilterAction, error) {
		current, err := queuedCurrentPrefix(value, key)
		if err != nil {
			return cell.DictFilterKeep, err
		}
		if current.Workchain != owner.WorkchainID ||
			!sharddomain.Contains(int64(owner.GetShardID()), int64(current.Prefix)) {
			return cell.DictFilterKeep,
				fmt.Errorf("%w: outbound message current address is outside predecessor shard", ErrInvalidInput)
		}
		if current.Workchain == target.WorkchainID &&
			sharddomain.Contains(int64(target.GetShardID()), int64(current.Prefix)) {
			size++
			return cell.DictFilterKeep, nil
		}
		return cell.DictFilterRemove, nil
	})
	if err != nil {
		return 0, fmt.Errorf("%w: split outbound queue: %v", ErrInvalidInput, err)
	}
	return size, nil
}

func queuedCurrentPrefix(value *cell.Slice, key *cell.Cell) (msgpool.AccountPrefix, error) {
	var keyLoader cell.Slice
	err := key.BeginParseInto(&keyLoader)
	if err != nil {
		return msgpool.AccountPrefix{}, err
	}
	var queueKey msgpool.QueueKey
	if err = keyLoader.LoadSliceInto(queueKey[:], 352); err != nil {
		return msgpool.AccountPrefix{}, err
	}

	// Filter hands out the live value slice; the decode consumes it, so the
	// copy is the ownership boundary rather than a defensive clone.
	valueCopy := *value
	entry, err := parseQueueEntry(&valueCopy, queueKey)
	if err != nil {
		return msgpool.AccountPrefix{}, err
	}

	return msgpool.AccountPrefix{Workchain: entry.descr.CurWorkchain, Prefix: entry.descr.CurPrefix}, nil
}

func splitProcessed(dict *cell.Dictionary, owner, target tlb.ShardIdent) (*cell.Dictionary, error) {
	records, err := tlb.LoadProcessedUptoRecords(dict, uint64(owner.GetShardID()))
	if err != nil {
		return nil, fmt.Errorf("%w: decode processed info for split: %v", ErrInvalidInput, err)
	}
	result, err := tlb.ProcessedUptoDict(splitProcessedRecords(records, int64(target.GetShardID())))
	if err != nil {
		return nil, fmt.Errorf("serialize split processed info: %w", err)
	}
	return result, nil
}

// splitProcessedRecords narrows a ProcessedUpto collection to one child shard
// the way a processed-upto collection is split. The input is left untouched:
// callers pass records they do not own.
func splitProcessedRecords(records []tlb.ProcessedUptoRecord, target int64) []tlb.ProcessedUptoRecord {
	kept := make([]tlb.ProcessedUptoRecord, 0, len(records))
	for i := range records {
		recordID := int64(records[i].ShardPrefix)
		if !sharddomain.Intersects(recordID, target) {
			continue
		}
		record := records[i]
		if sharddomain.Contains(recordID, target) {
			record.ShardPrefix = uint64(target)
		}
		kept = append(kept, record)
	}
	return tlb.CompactifyProcessedUpto(kept)
}

func mergeProcessed(
	left *cell.Dictionary,
	leftOwner tlb.ShardIdent,
	right *cell.Dictionary,
	rightOwner tlb.ShardIdent,
) (*cell.Dictionary, error) {
	leftRecords, err := tlb.LoadProcessedUptoRecords(left, uint64(leftOwner.GetShardID()))
	if err != nil {
		return nil, fmt.Errorf("%w: decode left processed info for merge: %v", ErrInvalidInput, err)
	}
	rightRecords, err := tlb.LoadProcessedUptoRecords(right, uint64(rightOwner.GetShardID()))
	if err != nil {
		return nil, fmt.Errorf("%w: decode right processed info for merge: %v", ErrInvalidInput, err)
	}
	records := make([]tlb.ProcessedUptoRecord, 0, len(leftRecords)+len(rightRecords))
	records = append(records, leftRecords...)
	records = append(records, rightRecords...)
	records = tlb.CompactifyProcessedUpto(records)
	result, err := tlb.ProcessedUptoDict(records)
	if err != nil {
		return nil, fmt.Errorf("serialize merged processed info: %w", err)
	}
	return result, nil
}

func copiedDispatchQueue(info *tlb.OutMsgQueueInfo) (*tlb.DispatchQueueAugDict, error) {
	if info.Extra == nil {
		return tlb.NewDispatchQueueAugDict()
	}
	return &tlb.DispatchQueueAugDict{
		AugmentedDictionary: info.Extra.DispatchQueue.Copy(),
	}, nil
}

func splitValidatorFees(fees tlb.CurrencyCollection, right bool) (tlb.CurrencyCollection, error) {
	if !fees.ExtraCurrencies.IsEmpty() {
		return tlb.CurrencyCollection{}, fmt.Errorf("%w: validator fees contain extra currencies", ErrInvalidInput)
	}
	nano := fees.Coins.Nano()
	if right {
		nano.Add(nano, big.NewInt(1))
	}
	nano.Rsh(nano, 1)
	return tlb.CurrencyCollection{Coins: tlb.FromNanoTON(nano)}, nil
}

func queueStoresSize(info *tlb.OutMsgQueueInfo) bool {
	return info.Extra != nil && info.Extra.OutQueueSize != nil
}

func newerMasterReference(left, right *tlb.ExtBlkRef) *tlb.ExtBlkRef {
	if left == nil || right != nil && right.SeqNo > left.SeqNo {
		return right
	}
	return left
}
