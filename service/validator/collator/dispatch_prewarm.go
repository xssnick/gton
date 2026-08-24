package collator

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/msgpool"
)

const (
	dispatchPrewarmFrontLimit  = 512
	maxPendingDispatchPrewarms = 64
)

const (
	dispatchPrewarmSourceState dispatchPrewarmSourceKind = iota
	dispatchPrewarmSourcePredecessors
)

type dispatchPrewarmSourceKind uint8

type dispatchPrewarmOwner struct {
	session    [32]byte
	generation uint64
}

type dispatchPrewarmSourceKey struct {
	owner  dispatchPrewarmOwner
	kind   dispatchPrewarmSourceKind
	count  uint8
	seqnos [2]uint32
	roots  [2]cell.Hash
}

type dispatchPrewarmSource struct {
	owner            dispatchPrewarmOwner
	kind             dispatchPrewarmSourceKind
	ctx              context.Context
	deadline         time.Time
	shard            msgpool.ShardIdent
	seqno            uint32
	key              dispatchPrewarmSourceKey
	state            *cell.Cell
	predecessors     [2]ton.BlockIDExt
	predecessorCount uint8
}

// dispatchPrewarmScheduler bounds lazy DispatchQueue discovery itself. Cell
// record warming has its own worker pool, but selecting 512 envelopes may also
// fault dictionary paths from storage; one scanner prevents state publication
// and concurrent sessions from turning that discovery into an I/O fan-out.
// Pending work is latest-wins per shard.
type dispatchPrewarmScheduler struct {
	mu           sync.Mutex
	running      bool
	hasActive    bool
	active       dispatchPrewarmSource
	activeCancel context.CancelFunc
	pending      []dispatchPrewarmSource
}

func (a *LocalAcquisition) enqueueDispatchStatePrewarm(
	ctx context.Context,
	owner dispatchPrewarmOwner,
	id ton.BlockIDExt,
	state *cell.Cell,
) bool {
	if a.accountPrewarmer == nil {
		return false
	}

	task := dispatchPrewarmSource{
		owner: owner,
		kind:  dispatchPrewarmSourceState,
		ctx:   context.WithoutCancel(ctx),
		shard: blockShardIdent(id),
		seqno: id.SeqNo,
		key: dispatchPrewarmSourceKey{
			owner:  owner,
			kind:   dispatchPrewarmSourceState,
			count:  1,
			seqnos: [2]uint32{id.SeqNo},
			roots:  [2]cell.Hash{state.HashKeyAt(0)},
		},
		state: state,
	}
	return a.enqueueDispatchPrewarm(task)
}

func (a *LocalAcquisition) enqueueRegisteredDispatchPrewarm(
	ctx context.Context,
	owner dispatchPrewarmOwner,
	target groups.ShardID,
	registered []groups.ShardDescription,
) bool {
	if a.accountPrewarmer == nil || !validDispatchPrewarmPredecessors(target, registered) {
		return false
	}

	task := dispatchPrewarmSource{
		owner:            owner,
		kind:             dispatchPrewarmSourcePredecessors,
		ctx:              context.WithoutCancel(ctx),
		shard:            targetShardIdent(target),
		predecessorCount: uint8(len(registered)),
		key: dispatchPrewarmSourceKey{
			owner: owner,
			kind:  dispatchPrewarmSourcePredecessors,
			count: uint8(len(registered)),
		},
	}
	for i := range registered {
		root, err := blockRootKey(registered[i].Block)
		if err != nil {
			return false
		}

		task.predecessors[i] = cloneBlockID(registered[i].Block)
		task.key.seqnos[i] = registered[i].Block.SeqNo
		task.key.roots[i] = cell.Hash(root)
		task.seqno = max(task.seqno, registered[i].Block.SeqNo)
	}

	return a.enqueueDispatchPrewarm(task)
}

func validDispatchPrewarmPredecessors(
	target groups.ShardID,
	registered []groups.ShardDescription,
) bool {
	_, err := resolveRegisteredShardTopology(&groups.Session{Shard: target, Registered: registered})
	return err == nil
}

func (a *LocalAcquisition) enqueueDispatchPrewarm(task dispatchPrewarmSource) bool {
	now := time.Now()
	task.deadline = now.Add(registeredTopWarmTimeout)
	scheduler := &a.dispatchPrewarm
	scheduler.mu.Lock()
	if scheduler.hasActive && !scheduler.active.deadline.After(now) {
		scheduler.activeCancel()
		scheduler.hasActive = false
		scheduler.active = dispatchPrewarmSource{}
		scheduler.activeCancel = nil
	}

	kept := scheduler.pending[:0]
	for i := range scheduler.pending {
		if scheduler.pending[i].deadline.After(now) {
			kept = append(kept, scheduler.pending[i])
		}
	}
	for i := len(kept); i < len(scheduler.pending); i++ {
		scheduler.pending[i] = dispatchPrewarmSource{}
	}
	scheduler.pending = kept

	for i := range scheduler.pending {
		current := scheduler.pending[i]
		if current.shard != task.shard {
			continue
		}
		if task.seqno < current.seqno || task.seqno == current.seqno && task.key == current.key {
			scheduler.mu.Unlock()
			return false
		}
		scheduler.pending[i] = task
		if scheduler.hasActive && scheduler.active.shard == task.shard && task.seqno >= scheduler.active.seqno {
			scheduler.activeCancel()
		}
		scheduler.mu.Unlock()
		return true
	}

	if scheduler.hasActive && scheduler.active.shard == task.shard {
		if task.seqno < scheduler.active.seqno ||
			task.seqno == scheduler.active.seqno && task.key == scheduler.active.key {
			scheduler.mu.Unlock()
			return false
		}
	}
	if len(scheduler.pending) == maxPendingDispatchPrewarms {
		scheduler.mu.Unlock()
		return false
	}

	scheduler.pending = append(scheduler.pending, task)
	if scheduler.hasActive && scheduler.active.shard == task.shard {
		scheduler.activeCancel()
	}
	start := !scheduler.running
	if start {
		scheduler.running = true
	}
	scheduler.mu.Unlock()

	if start {
		go a.runDispatchPrewarms()
	}
	return true
}

func (a *LocalAcquisition) runDispatchPrewarms() {
	scheduler := &a.dispatchPrewarm
	for {
		scheduler.mu.Lock()
		if len(scheduler.pending) == 0 {
			scheduler.running = false
			scheduler.hasActive = false
			scheduler.active = dispatchPrewarmSource{}
			scheduler.activeCancel = nil
			scheduler.mu.Unlock()
			return
		}

		task := scheduler.pending[0]
		scheduler.pending[0] = dispatchPrewarmSource{}
		scheduler.pending = scheduler.pending[1:]
		ctx, cancel := context.WithDeadline(task.ctx, task.deadline)
		scheduler.active = task
		scheduler.hasActive = true
		scheduler.activeCancel = cancel
		scheduler.mu.Unlock()

		a.runDispatchPrewarm(ctx, task)
		cancel()

		scheduler.mu.Lock()
		scheduler.hasActive = false
		scheduler.active = dispatchPrewarmSource{}
		scheduler.activeCancel = nil
		scheduler.mu.Unlock()
	}
}

func (a *LocalAcquisition) runDispatchPrewarm(ctx context.Context, task dispatchPrewarmSource) {
	if ctx.Err() != nil {
		return
	}

	var err error
	switch task.kind {
	case dispatchPrewarmSourceState:
		err = a.prewarmDispatchFront(ctx, task.shard, task.state)
	case dispatchPrewarmSourcePredecessors:
		var queue *tlb.DispatchQueueAugDict
		queue, err = a.loadEffectiveDispatchPrewarmQueue(ctx, task)
		if err == nil {
			err = a.prewarmDispatchQueue(ctx, task.shard, queue)
		}
	default:
		err = fmt.Errorf("%w: unknown dispatch prewarm source", ErrInvalidInput)
	}
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		a.log.Debug().Err(err).
			Int32("workchain", task.shard.Workchain).
			Uint64("shard", task.shard.Shard).
			Uint32("seqno", task.seqno).
			Msg("dispatch front prewarm skipped")
	}
}

func (a *LocalAcquisition) cancelSessionDispatchPrewarms(owner dispatchPrewarmOwner) {
	scheduler := &a.dispatchPrewarm
	scheduler.mu.Lock()

	kept := scheduler.pending[:0]
	for i := range scheduler.pending {
		if scheduler.pending[i].owner != owner {
			kept = append(kept, scheduler.pending[i])
		}
	}
	for i := len(kept); i < len(scheduler.pending); i++ {
		scheduler.pending[i] = dispatchPrewarmSource{}
	}
	scheduler.pending = kept
	if scheduler.hasActive && scheduler.active.owner == owner {
		scheduler.activeCancel()
		scheduler.hasActive = false
		scheduler.active = dispatchPrewarmSource{}
		scheduler.activeCancel = nil
	}
	scheduler.mu.Unlock()
}

func (s *localAcquisitionSession) dispatchPrewarmOwner() dispatchPrewarmOwner {
	return dispatchPrewarmOwner{session: s.session.ID, generation: s.dispatchPrewarmGeneration}
}

func (a *LocalAcquisition) loadEffectiveDispatchPrewarmQueue(
	ctx context.Context,
	task dispatchPrewarmSource,
) (*tlb.DispatchQueueAugDict, error) {
	var sources [2]*localBlockSource
	for i := range int(task.predecessorCount) {
		source, err := a.blockSource(ctx, task.predecessors[i], acquisitionReadImmediate)
		if err != nil {
			return nil, err
		}
		sources[i] = source
	}

	target := groups.ShardID{Workchain: task.shard.Workchain, Shard: int64(task.shard.Shard)}
	return effectiveDispatchPrewarmQueue(target, sources[0], sources[1])
}

func effectiveDispatchPrewarmQueue(
	target groups.ShardID,
	first *localBlockSource,
	second *localBlockSource,
) (*tlb.DispatchQueueAugDict, error) {
	request := ShardRequest{Shard: target, Previous: first.previous}
	var secondState *tlb.ShardStateUnsplit
	if second != nil {
		secondPrevious := second.previous
		request.Previous2 = &secondPrevious
		secondState = &second.state
	}
	topology, err := resolveShardTopologyState(request, &first.state, secondState)
	if err != nil {
		return nil, err
	}

	queue, err := dispatchPrewarmQueueFromState(&first.state)
	if err != nil {
		return nil, err
	}
	switch topology.kind {
	case topologyLinear:
		return queue, nil
	case topologyAfterSplit:
		ok, cutErr := queue.CutPrefixSubdict(shardPrefixCell(topology.target), false)
		if cutErr != nil || !ok {
			return nil, fmt.Errorf("%w: split dispatch prewarm queue", ErrInvalidInput)
		}
		return queue, nil
	case topologyAfterMerge:
		right, rightErr := dispatchPrewarmQueueFromState(&second.state)
		if rightErr != nil {
			return nil, rightErr
		}
		ok, combineErr := queue.CombineWith(right.AugmentedDictionary)
		if combineErr != nil || !ok {
			return nil, fmt.Errorf("%w: merge dispatch prewarm queues", ErrInvalidInput)
		}
		return queue, nil
	default:
		return nil, fmt.Errorf("%w: unknown dispatch prewarm topology", ErrInvalidInput)
	}
}

func dispatchPrewarmQueueFromState(state *tlb.ShardStateUnsplit) (*tlb.DispatchQueueAugDict, error) {
	var queueInfo tlb.OutMsgQueueInfo
	if err := parseExact(&queueInfo, state.OutMsgQueueInfo.WithoutTrace()); err != nil {
		return nil, fmt.Errorf("%w: decode dispatch prewarm outbound queue: %v", ErrInvalidInput, err)
	}

	return copiedDispatchQueue(&queueInfo)
}

func (a *LocalAcquisition) prewarmDispatchFront(
	ctx context.Context,
	shard msgpool.ShardIdent,
	state *cell.Cell,
) error {
	queueInfo, err := msgpool.StateOutMsgQueueInfo(state.WithoutTrace())
	if err != nil {
		return err
	}
	if queueInfo.Extra == nil || queueInfo.Extra.DispatchQueue.IsEmpty() {
		return nil
	}
	return a.prewarmDispatchQueue(ctx, shard, queueInfo.Extra.DispatchQueue)
}

func (a *LocalAcquisition) prewarmDispatchQueue(
	ctx context.Context,
	shard msgpool.ShardIdent,
	source *tlb.DispatchQueueAugDict,
) error {
	queue := &tlb.DispatchQueueAugDict{
		AugmentedDictionary: source.Copy(),
	}
	priorities := canonicalDispatchAccounts(a.dispatch.PriorityList)
	priorityIndex := 0
	messages := make([]*msgpool.InternalMessage, 0, dispatchPrewarmFrontLimit)
	var err error
	for !queue.IsEmpty() && len(messages) < dispatchPrewarmFrontLimit {
		if err = ctx.Err(); err != nil {
			return err
		}

		source, accountQueue, selectErr := nextDispatchAccount(queue, &priorities, &priorityIndex, shard)
		if selectErr != nil {
			return selectErr
		}
		lt, selectErr := minimumDispatchLT(accountQueue)
		if selectErr != nil {
			return fmt.Errorf("dispatch prewarm account %x minimum: %w", source.AccountID, selectErr)
		}
		value, selectErr := accountQueue.Messages.LoadValue(dispatchLTKey(lt))
		if selectErr != nil {
			return fmt.Errorf("dispatch prewarm account %x message %d: %w", source.AccountID, lt, selectErr)
		}
		var enqueued tlb.EnqueuedMsg
		if selectErr = loadExactSlice(&enqueued, value); selectErr != nil {
			return fmt.Errorf("dispatch prewarm account %x message %d: %w", source.AccountID, lt, selectErr)
		}
		if enqueued.EnqueuedLT != lt {
			return fmt.Errorf("dispatch prewarm account %x message key %d differs from enqueued lt %d",
				source.AccountID, lt, enqueued.EnqueuedLT)
		}
		message, selectErr := msgpool.InternalMessageFromEnvelope(enqueued.Msg.WithoutTrace())
		if selectErr != nil {
			return fmt.Errorf("dispatch prewarm account %x message %d: %w", source.AccountID, lt, selectErr)
		}
		if message.Envelope.EmittedLT != nil || message.QueueLT != lt {
			return fmt.Errorf("dispatch prewarm account %x message %d is not an unemitted deferred message",
				source.AccountID, lt)
		}
		messages = append(messages, message)

		if selectErr = queue.Delete(dispatchAccountKey(source.AccountID)); selectErr != nil {
			return fmt.Errorf("dispatch prewarm remove account %x: %w", source.AccountID, selectErr)
		}
	}

	if err = ctx.Err(); err != nil {
		return err
	}
	seenAccounts := make(map[prewarmAccountKey]struct{}, len(messages))
	for _, message := range messages {
		if err = ctx.Err(); err != nil {
			return err
		}
		if !message.DestinationPrewarmable ||
			!shard.Contains(message.DestinationWorkchain, binary.BigEndian.Uint64(message.DestinationAccount[:8])) {
			continue
		}
		key := prewarmAccountKey{
			workchain: message.DestinationWorkchain,
			account:   message.DestinationAccount,
		}
		if _, exists := seenAccounts[key]; exists {
			continue
		}
		seenAccounts[key] = struct{}{}
		a.accountPrewarmer.EnqueueAccount(key.workchain, key.account)
	}

	seenRoots := make(map[cell.Hash]struct{}, len(messages))
	for _, message := range messages {
		if err = ctx.Err(); err != nil {
			return err
		}
		root := cell.Hash(message.EnvHash)
		if _, exists := seenRoots[root]; exists {
			continue
		}
		seenRoots[root] = struct{}{}
		a.accountPrewarmer.EnqueueRoot(root)
	}

	return nil
}
