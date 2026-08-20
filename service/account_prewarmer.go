package service

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	DefaultAccountPrewarmerWorkers   = 32
	DefaultAccountPrewarmerQueueSize = 512
	accountPrewarmerQueuedPerWorker  = 16
)

var ErrAccountPrewarmerClosed = errors.New("account prewarmer is closed")

// CellRecordWarmer loads the encoded cell-record tree without populating the
// decoded-cell cache.
type CellRecordWarmer interface {
	WarmCellRecords(context.Context, cell.Hash) error
}

// AccountRootResolver resolves the current state-cell root for an account.
type AccountRootResolver interface {
	AccountRoot(context.Context, int32, [32]byte) (cell.Hash, error)
}

type AccountPrewarmerOptions struct {
	Workers   int
	QueueSize int
}

type accountPrewarmTaskKind uint8

const (
	accountPrewarmRoot accountPrewarmTaskKind = iota
	accountPrewarmAddress
)

type accountPrewarmTask struct {
	kind      accountPrewarmTaskKind
	root      cell.Hash
	workchain int32
	account   [32]byte
}

type accountPrewarmTaskState uint8

const (
	accountPrewarmTaskQueued accountPrewarmTaskState = iota + 1
	accountPrewarmTaskRunning
	accountPrewarmTaskImmediate
)

// AccountPrewarmer owns a bounded, best-effort worker pool for account-state
// cell-record prewarming. Enqueue methods never wait for queue capacity.
type AccountPrewarmer struct {
	log            zerolog.Logger
	warmer         CellRecordWarmer
	resolver       AccountRootResolver
	workers        int
	tasks          chan accountPrewarmTask
	immediateSlots chan struct{}

	mu           sync.Mutex
	started      bool
	closed       bool
	runCtx       context.Context
	cancel       context.CancelFunc
	taskStates   map[accountPrewarmTask]accountPrewarmTaskState
	warmingRoots map[cell.Hash]struct{}
	wg           sync.WaitGroup
	closeOnce    sync.Once
}

func NewAccountPrewarmer(
	logger zerolog.Logger,
	warmer CellRecordWarmer,
	resolver AccountRootResolver,
	opts AccountPrewarmerOptions,
) (*AccountPrewarmer, error) {
	if warmer == nil {
		return nil, fmt.Errorf("account prewarmer cell-record warmer is required")
	}
	if resolver == nil {
		return nil, fmt.Errorf("account prewarmer root resolver is required")
	}

	workers := opts.Workers
	if workers == 0 {
		workers = DefaultAccountPrewarmerWorkers
	} else if workers < 0 {
		return nil, fmt.Errorf("account prewarmer workers must be positive")
	}

	queueSize := opts.QueueSize
	if queueSize == 0 {
		queueSize = workers * accountPrewarmerQueuedPerWorker
	} else if queueSize < 0 {
		return nil, fmt.Errorf("account prewarmer queue size must be positive")
	}

	return &AccountPrewarmer{
		log:            logger,
		warmer:         warmer,
		resolver:       resolver,
		workers:        workers,
		tasks:          make(chan accountPrewarmTask, queueSize),
		immediateSlots: make(chan struct{}, workers),
		taskStates:     make(map[accountPrewarmTask]accountPrewarmTaskState, queueSize+workers),
		warmingRoots:   make(map[cell.Hash]struct{}, workers),
	}, nil
}

// PrewarmCapacity is the useful canonical look-ahead for one collation. It
// includes work that can start immediately and the bounded backlog that keeps
// those workers busy while transaction execution advances through the block.
func (p *AccountPrewarmer) PrewarmCapacity() int {
	return p.workers + cap(p.tasks)
}

// Start starts the worker pool once. Repeated calls while it is running are
// harmless; a closed prewarmer cannot be restarted.
func (p *AccountPrewarmer) Start(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return ErrAccountPrewarmerClosed
	}
	if p.started {
		return nil
	}

	p.runCtx, p.cancel = context.WithCancel(ctx)
	p.started = true
	p.wg.Add(p.workers)
	for worker := 0; worker < p.workers; worker++ {
		go p.runWorker(p.runCtx)
	}
	return nil
}

// Close cancels active work, stops all workers, and rejects future enqueues.
func (p *AccountPrewarmer) Close() {
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		if p.cancel != nil {
			p.cancel()
		}
		p.mu.Unlock()

		p.wg.Wait()

		p.mu.Lock()
		clear(p.taskStates)
		clear(p.warmingRoots)
		p.mu.Unlock()
	})
}

// EnqueueRoot submits an already-resolved account root without waiting for a
// worker or queue capacity. It returns false when the task is already pending,
// is currently running, or cannot be queued.
func (p *AccountPrewarmer) EnqueueRoot(root cell.Hash) bool {
	task := accountPrewarmTask{kind: accountPrewarmRoot, root: root}

	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.warmingRoots[root]; ok {
		return false
	}
	return p.enqueueLocked(task)
}

// EnqueueAccount submits an account whose current root will be resolved by a
// worker. It never performs resolution on the caller's goroutine.
func (p *AccountPrewarmer) EnqueueAccount(workchain int32, account [32]byte) bool {
	task := accountPrewarmTask{
		kind:      accountPrewarmAddress,
		workchain: workchain,
		account:   account,
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	return p.enqueueLocked(task)
}

// PrewarmAccountNow starts one account warm immediately, bypassing the bounded
// hint queue. Collation uses it as an early hint for generated destinations
// that may execute in the current block, and deduplication still limits one
// in-flight goroutine per destination account.
func (p *AccountPrewarmer) PrewarmAccountNow(workchain int32, account [32]byte) bool {
	task := accountPrewarmTask{
		kind:      accountPrewarmAddress,
		workchain: workchain,
		account:   account,
	}

	p.mu.Lock()
	if !p.acceptingLocked() {
		p.mu.Unlock()
		return false
	}
	state, exists := p.taskStates[task]
	if exists && state != accountPrewarmTaskQueued {
		p.mu.Unlock()
		return false
	}
	select {
	case p.immediateSlots <- struct{}{}:
	default:
		p.mu.Unlock()
		return false
	}
	p.taskStates[task] = accountPrewarmTaskImmediate
	ctx := p.runCtx
	p.wg.Add(1)
	p.mu.Unlock()

	go func() {
		defer p.wg.Done()
		defer func() { <-p.immediateSlots }()
		p.processTask(ctx, task)
	}()
	return true
}

func (p *AccountPrewarmer) enqueueLocked(task accountPrewarmTask) bool {
	if !p.acceptingLocked() {
		return false
	}
	if _, exists := p.taskStates[task]; exists {
		return false
	}

	p.taskStates[task] = accountPrewarmTaskQueued
	select {
	case p.tasks <- task:
		return true
	default:
		delete(p.taskStates, task)
		return false
	}
}

func (p *AccountPrewarmer) acceptingLocked() bool {
	return p.started && !p.closed && p.runCtx.Err() == nil
}

func (p *AccountPrewarmer) runWorker(ctx context.Context) {
	defer p.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case task := <-p.tasks:
			if !p.beginQueuedTask(ctx, task) {
				continue
			}
			p.processTask(ctx, task)
		}
	}
}

func (p *AccountPrewarmer) beginQueuedTask(ctx context.Context, task accountPrewarmTask) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.taskStates[task] != accountPrewarmTaskQueued {
		return false
	}
	if ctx.Err() != nil {
		delete(p.taskStates, task)
		return false
	}

	p.taskStates[task] = accountPrewarmTaskRunning
	return true
}

func (p *AccountPrewarmer) processTask(ctx context.Context, task accountPrewarmTask) {
	defer p.completeTask(task)

	root := task.root
	if task.kind == accountPrewarmAddress {
		var err error
		root, err = p.resolver.AccountRoot(ctx, task.workchain, task.account)
		if err != nil {
			p.logTaskError(task, cell.Hash{}, "resolve account root for prewarm", err)
			return
		}
	}

	if !p.claimRoot(task, root) {
		return
	}
	defer p.releaseRoot(root)

	if err := p.warmer.WarmCellRecords(ctx, root); err != nil {
		p.logTaskError(task, root, "prewarm account cell records", err)
	}
}

func (p *AccountPrewarmer) claimRoot(task accountPrewarmTask, root cell.Hash) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, ok := p.warmingRoots[root]; ok {
		return false
	}
	if task.kind == accountPrewarmAddress {
		exact := accountPrewarmTask{kind: accountPrewarmRoot, root: root}
		if state, exists := p.taskStates[exact]; exists {
			if state != accountPrewarmTaskQueued {
				return false
			}
			// The address task has already resolved the same root. Supersede the
			// queued exact-root copy; its channel value will be ignored by
			// beginQueuedTask when a worker eventually receives it.
			delete(p.taskStates, exact)
		}
	}

	p.warmingRoots[root] = struct{}{}
	return true
}

func (p *AccountPrewarmer) releaseRoot(root cell.Hash) {
	p.mu.Lock()
	delete(p.warmingRoots, root)
	p.mu.Unlock()
}

func (p *AccountPrewarmer) completeTask(task accountPrewarmTask) {
	p.mu.Lock()
	delete(p.taskStates, task)
	p.mu.Unlock()
}

func (p *AccountPrewarmer) logTaskError(task accountPrewarmTask, root cell.Hash, message string, err error) {
	if errors.Is(err, storage.ErrNotFound) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}

	event := p.log.Debug().Err(err)
	if root != (cell.Hash{}) {
		event.Hex("root_hash", root[:])
	}
	if task.kind == accountPrewarmAddress {
		event.Int32("workchain", task.workchain).Hex("account", task.account[:])
	}
	event.Msg(message)
}
