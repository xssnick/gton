package collator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/validator/groups"
)

const (
	// neighborWarmWorkers bounds every neighbour warm-up. The reads are short —
	// a state-tree root, a block root and a header prewarm each — so the point
	// of the bound is to keep a wide shard configuration from putting one
	// goroutine and one storage read per shard in flight at once, not to pace
	// the individual read.
	neighborWarmWorkers = 8
	// registeredTopWarmTimeout stops a warm that outlived the view it was
	// started for. It is generous on purpose: the warm holds nothing back, and
	// a read still running when the next masterchain block arrives has simply
	// lost its race and should end rather than accumulate.
	registeredTopWarmTimeout = 30 * time.Second
)

// LocalStateStore is the exact read surface required by local collation. The
// node live store implements it directly; acquisition never reads a different
// database or falls back to a newer chain position.
type LocalStateStore interface {
	BlockState(context.Context, ton.BlockIDExt) (*storage.BlockState, error)
	LoadStateCellTree(context.Context, ton.BlockIDExt, []byte) (*cell.Cell, error)
	BlockRoot(context.Context, ton.BlockIDExt) (*cell.Cell, error)
}

// LocalGroupSource publishes current and retained canonical validator-group
// snapshots. A speculative view must name its previous snapshot explicitly;
// an unavailable historical view returns ErrAcquisitionNotReady instead of a
// mixed snapshot.
type LocalGroupSource interface {
	Snapshot() (*groups.Snapshot, error)
	Project(*groups.Snapshot, groups.ApplyInput) (*groups.Snapshot, error)
}

type localPreparedConfig struct {
	execution *tvm.PreparedBlockchainConfig
	config    *Config
	groups    *groups.Config
}

// maxLocalPreparedConfigs bounds the prepared-config cache. The live working
// set is one config, briefly two while a key block installs the next one, so
// this is slack rather than a tuning knob. An evicted entry is only ever
// re-prepared; callers hold the pointers they were handed, so eviction can
// never invalidate a config already in use.
const maxLocalPreparedConfigs = 4

type localConfigCache struct {
	mu      sync.Mutex
	entries map[cell.Hash]localPreparedConfig
	// order is insertion order, evicted from the front. At this cap the shift
	// is cheaper than tracking generations, and it keeps one backing array.
	order []cell.Hash
}

type localMasterView struct {
	previous PreviousBlock
	state    tlb.ShardStateUnsplit
	extra    tlb.McStateExtra
	info     tlb.McStateExtraBlockInfo
	stats    tlb.ShardStateStats
	registry *ShardRegistry
	context  MasterchainContext
}

// PublishMasterchainView installs the exact resident block/state pair supplied
// by the node's applied-block hook. The hook runs before those artifacts enter
// the ordinary live store, so controller reconciliation and masterchain
// collation must use this coherent view instead of waiting on a storage side
// effect that cannot happen until the hook returns.
func (a *LocalAcquisition) PublishMasterchainView(
	ctx context.Context,
	snapshot *groups.Snapshot,
	blockRoot *cell.Cell,
	stateRoot *cell.Cell,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if snapshot == nil || !snapshot.Ready {
		return fmt.Errorf("%w: validator group snapshot is not ready", ErrAcquisitionNotReady)
	}
	id := snapshot.MasterchainBlock
	if id.Workchain != masterchainWorkchainID || id.Shard != math.MinInt64 {
		return fmt.Errorf("%w: context block is not the masterchain root", ErrInvalidInput)
	}
	if stateRoot == nil || blockRoot == nil && id.SeqNo != 0 {
		return fmt.Errorf("%w: resident masterchain block or state is absent", ErrInvalidInput)
	}

	a.master.mu.RLock()
	resident := a.master.view
	a.master.mu.RUnlock()
	if residentMasterchainViewIsCurrent(resident, id, snapshot) {
		return nil
	}

	// Construction runs outside the lock, mirroring localConfigCache.prepare:
	// every concurrent projectedMasterView takes the read lock on the entry path
	// of ValidateCandidate, and the whole decode — state extra, prepared config,
	// shard registry, masterchain history and libraries — would otherwise stall
	// them once per masterchain block. A racing duplicate build is the accepted
	// cost, exactly as it is for a prepared config.
	queueSize, state, err := residentMasterchainPredecessor(id, blockRoot, stateRoot)
	if err != nil {
		return err
	}
	previous := PreviousBlock{
		ID:           cloneBlockID(id),
		Block:        blockRoot,
		State:        stateRoot,
		OutQueueSize: &queueSize,
	}
	view, err := a.masterView(previous, state, snapshot)
	if err != nil {
		return err
	}
	if view.context.Config.execution.Root().HashKey() != snapshot.ConfigRootHash {
		return fmt.Errorf("%w: group snapshot config differs from resident masterchain state", ErrInvalidInput)
	}

	a.master.mu.Lock()
	defer a.master.mu.Unlock()
	if residentMasterchainViewIsCurrent(a.master.view, id, snapshot) {
		return nil
	}
	// Two publishers exist — the node's applied-block hook and controller
	// reconciliation — and each is internally serialized but not against the
	// other. Building outside the lock widens the window between reading the
	// incumbent and installing, so the install must refuse to move the resident
	// view backwards: a view that lost that race would otherwise also retire
	// block-cache entries the newer view still references.
	if a.master.view != nil && a.master.view.context.ID.SeqNo > id.SeqNo {
		return nil
	}
	a.master.view = view
	// Registered neighbor tops only move when the masterchain view moves, so
	// installing a view is the exact bound on cached block lifetime.
	a.blocks.advance()
	a.warmRegisteredTops(view)

	return nil
}

// warmRegisteredTops reads the newly registered shard tops into the block cache
// off the collation path.
//
// Those tops are exactly the neighbour states the next slot asks for, and the
// masterchain has no time to read them in: a shard build starts a full
// TargetRate before its slot, a masterchain build starts at the slot itself, so
// every one of these reads lands inside the masterchain's own budget. Installing
// a view is the earliest moment their exact block ids are known, and it happens
// once per masterchain block rather than once per slot.
//
// It is bounded by construction: the cache is content-addressed, so a top that
// did not move is a hit and costs nothing, and only the shards that actually
// advanced are read. Failures are dropped — this decides nothing, and the slot
// performs the same read authoritatively.
func (a *LocalAcquisition) warmRegisteredTops(view *localMasterView) {
	if a.store == nil || view == nil || view.registry == nil {
		return
	}

	tops := make([]ton.BlockIDExt, 0, len(view.registry.leaves))
	for _, leaf := range view.registry.leaves {
		if leaf.top.Block.Workchain == masterchainWorkchainID {
			continue
		}
		if _, err := a.blocks.lookup(leaf.top.Block); err == nil {
			continue
		}
		tops = append(tops, cloneBlockID(leaf.top.Block))
	}
	if len(tops) == 0 {
		return
	}

	// Detached from the publishing hook: it runs on the node's applied-block
	// path, which must not wait for reads that only exist to be early. The
	// context is the warm's own, because the hook's ends with the publish.
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), registeredTopWarmTimeout)
		defer cancel()

		queue := make(chan ton.BlockIDExt, len(tops))
		for _, id := range tops {
			queue <- id
		}
		close(queue)

		var wg sync.WaitGroup
		for range min(len(tops), neighborWarmWorkers) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for id := range queue {
					if ctx.Err() != nil {
						return
					}
					_, _ = a.blockSource(ctx, id)
				}
			}()
		}
		wg.Wait()
	}()
}

// residentMasterchainViewIsCurrent reports whether the installed view already is
// the exact one a publish would produce. Both the block id and the snapshot
// pointer must match: the same masterchain block can be republished with a newer
// group projection, and the view binds one snapshot.
func residentMasterchainViewIsCurrent(
	resident *localMasterView,
	id ton.BlockIDExt,
	snapshot *groups.Snapshot,
) bool {
	return resident != nil && id.Equals(&resident.context.ID) && resident.context.Groups == snapshot
}

func residentMasterchainPredecessor(
	id ton.BlockIDExt,
	blockRoot *cell.Cell,
	stateRoot *cell.Cell,
) (uint64, tlb.ShardStateUnsplit, error) {
	previous := PreviousBlock{ID: id, Block: blockRoot, State: stateRoot}
	state, err := verifyPredecessor("resident masterchain", &previous)
	if err != nil {
		return 0, tlb.ShardStateUnsplit{}, err
	}
	queueSize, err := exactOutQueueSize(state.OutMsgQueueInfo)
	if err != nil {
		return 0, tlb.ShardStateUnsplit{}, err
	}

	return queueSize, state, nil
}

func (a *LocalAcquisition) masterView(
	previous PreviousBlock,
	state tlb.ShardStateUnsplit,
	snapshot *groups.Snapshot,
) (*localMasterView, error) {
	if state.McStateExtra == nil {
		return nil, fmt.Errorf("%w: masterchain state extra is absent", ErrInvalidInput)
	}
	var extra tlb.McStateExtra
	if err := parseExact(&extra, state.McStateExtra); err != nil {
		return nil, fmt.Errorf("%w: decode masterchain state extra: %v", ErrInvalidInput, err)
	}
	if extra.ConfigParams.Config.Params == nil || extra.ConfigParams.Config.Params.AsCell() == nil {
		return nil, fmt.Errorf("%w: masterchain config dictionary is absent", ErrInvalidInput)
	}
	prepared, err := a.configs.prepare(extra.ConfigParams.Config.Params.AsCell())
	if err != nil {
		return nil, err
	}

	info, err := parseMasterStateInfo(extra.Info)
	if err != nil {
		return nil, fmt.Errorf("%w: decode masterchain state info: %v", ErrInvalidInput, err)
	}
	if info.PrevBlocks == nil || info.PrevBlocks.AugmentedDictionary == nil {
		return nil, fmt.Errorf("%w: masterchain history is absent", ErrInvalidInput)
	}
	var stats tlb.ShardStateStats
	if err = parseExact(&stats, state.Stats); err != nil {
		return nil, fmt.Errorf("%w: decode masterchain state statistics: %v", ErrInvalidInput, err)
	}
	registry, err := ParseShardRegistry(extra.ShardHashes)
	if err != nil {
		return nil, err
	}
	prevBlocks, err := masterPrevBlocksTuple(previous.ID, &info, prepared.config.globalVersion)
	if err != nil {
		return nil, err
	}
	libraries, err := masterExecutionLibraries(stats.Libraries)
	if err != nil {
		return nil, err
	}

	return &localMasterView{
		previous: previous,
		state:    state,
		extra:    extra,
		info:     info,
		stats:    stats,
		registry: registry,
		context: MasterchainContext{
			ID:              cloneBlockID(previous.ID),
			EndLT:           state.GenLT,
			GenUtime:        state.GenUTime,
			VertSeqno:       state.VertSeqno,
			Config:          prepared.config,
			OutMsgQueueInfo: state.OutMsgQueueInfo,
			Groups:          snapshot,
			PrevBlocks:      prevBlocks,
			Libraries:       libraries,
		},
	}, nil
}

func (c *localConfigCache) prepare(root *cell.Cell) (localPreparedConfig, error) {
	hash := root.HashKey()
	c.mu.Lock()
	prepared, exists := c.entries[hash]
	c.mu.Unlock()
	if exists {
		return prepared, nil
	}

	execution, err := tvm.PrepareBlockchainConfig(root)
	if err != nil {
		return localPreparedConfig{}, fmt.Errorf("%w: prepare blockchain config: %v", ErrInvalidInput, err)
	}
	config, err := PrepareConfig(execution)
	if err != nil {
		return localPreparedConfig{}, err
	}
	groupConfig, err := groups.ParseConfig(root)
	if err != nil {
		return localPreparedConfig{}, fmt.Errorf("%w: prepare validator groups config: %v", ErrInvalidInput, err)
	}
	prepared = localPreparedConfig{execution: execution, config: config, groups: groupConfig}

	return c.store(hash, prepared), nil
}

// store publishes one prepared config and evicts the oldest once the cache is
// over its cap. The incumbent wins a race so that every caller of one config
// root shares one prepared instance.
func (c *localConfigCache) store(hash cell.Hash, prepared localPreparedConfig) localPreparedConfig {
	c.mu.Lock()
	defer c.mu.Unlock()

	if current, ok := c.entries[hash]; ok {
		return current
	}
	c.entries[hash] = prepared
	c.order = append(c.order, hash)
	for len(c.order) > maxLocalPreparedConfigs {
		delete(c.entries, c.order[0])
		c.order = append(c.order[:0], c.order[1:]...)
	}

	return prepared
}

// loadPrevious serves every block read from the content-addressed block cache:
// the same neighbor, predecessor and historical masterchain blocks are read
// again on every slot and every candidate validation, and the read plus its
// verifyPredecessor binding is a pure function of the block id.
func (a *LocalAcquisition) loadPrevious(
	ctx context.Context,
	id ton.BlockIDExt,
) (PreviousBlock, tlb.ShardStateUnsplit, error) {
	source, err := a.blockSource(ctx, id)
	if err != nil {
		return PreviousBlock{}, tlb.ShardStateUnsplit{}, err
	}

	return source.previous, source.state, nil
}

// blockSource returns the cached verified read of one block, loading it on a
// miss. Concurrent misses for the same block share one read: unlike a prepared
// config, which is CPU over cells already in hand, this read is storage I/O, and
// the collation, the validation and the neighbour warm-up routinely want the
// same neighbour at the same moment.
func (a *LocalAcquisition) blockSource(ctx context.Context, id ton.BlockIDExt) (*localBlockSource, error) {
	source, err := a.blocks.lookup(id)
	if err == nil {
		return source, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return nil, err
	}

	return a.blocks.loadOnce(ctx, id, func() (*localBlockSource, error) {
		previous, state, readErr := a.readPrevious(ctx, id)
		if readErr != nil {
			return nil, readErr
		}

		return &localBlockSource{previous: previous, state: state}, nil
	})
}

func (a *LocalAcquisition) readPrevious(
	ctx context.Context,
	id ton.BlockIDExt,
) (PreviousBlock, tlb.ShardStateUnsplit, error) {
	stored, err := a.store.BlockState(ctx, id)
	if err != nil {
		return PreviousBlock{}, tlb.ShardStateUnsplit{}, acquisitionReadError("block state", id, err)
	}
	if !id.Equals(&stored.Block) {
		return PreviousBlock{}, tlb.ShardStateUnsplit{}, fmt.Errorf("%w: stored state belongs to another block", ErrInvalidInput)
	}
	root := stored.Cell
	if root == nil {
		root, err = a.store.LoadStateCellTree(ctx, id, stored.StateRootHash)
		if err != nil {
			return PreviousBlock{}, tlb.ShardStateUnsplit{}, acquisitionReadError("state cells", id, err)
		}
	}
	if len(stored.StateRootHash) == 32 && !bytes.Equal(root.Hash(), stored.StateRootHash) {
		return PreviousBlock{}, tlb.ShardStateUnsplit{}, fmt.Errorf("%w: stored state root hash differs from its metadata", ErrInvalidInput)
	}

	var blockRoot *cell.Cell
	if id.SeqNo != 0 {
		blockRoot, err = a.store.BlockRoot(ctx, id)
		if err != nil {
			return PreviousBlock{}, tlb.ShardStateUnsplit{}, acquisitionReadError("block root", id, err)
		}
		// liveview parses archived block BOCs lazily. A Block retains its state
		// update as a direct reference, so it must be materialized before TLB
		// decoding: otherwise the reference is still a pruned lazy boundary and
		// Merkle-update validation sees the boundary's type instead of the real
		// update cell. One edge loads only the block header's four direct refs;
		// deeper state branches remain storage-backed lazy cells.
		blockRoot, err = blockRoot.PrewarmRecursive(1)
		if err != nil {
			return PreviousBlock{}, tlb.ShardStateUnsplit{}, fmt.Errorf("load block root references: %w", err)
		}
	}

	var state tlb.ShardStateUnsplit
	if err = parseExact(&state, root); err != nil {
		return PreviousBlock{}, tlb.ShardStateUnsplit{}, fmt.Errorf("%w: decode state for block %d: %v", ErrInvalidInput, id.SeqNo, err)
	}
	queueSize, err := exactOutQueueSize(state.OutMsgQueueInfo)
	if err != nil {
		return PreviousBlock{}, tlb.ShardStateUnsplit{}, err
	}
	previous := PreviousBlock{
		ID:           cloneBlockID(id),
		Block:        blockRoot,
		State:        root,
		OutQueueSize: &queueSize,
	}
	verified, err := verifyPredecessor("acquired", &previous)
	if err != nil {
		return PreviousBlock{}, tlb.ShardStateUnsplit{}, err
	}

	return previous, verified, nil
}

func exactOutQueueSize(root *cell.Cell) (uint64, error) {
	var queue tlb.OutMsgQueueInfo
	if err := parseExact(&queue, root); err != nil {
		return 0, fmt.Errorf("%w: decode outbound queue: %v", ErrInvalidInput, err)
	}
	if queue.Extra != nil && queue.Extra.OutQueueSize != nil {
		return *queue.Extra.OutQueueSize, nil
	}
	count, err := queue.OutQueue.Count()
	if err != nil {
		return 0, fmt.Errorf("%w: count outbound queue: %v", ErrInvalidInput, err)
	}
	if count < 0 || uint64(count) > maxOutMsgQueueSize {
		return 0, fmt.Errorf("%w: outbound queue size exceeds 48 bits", ErrInvalidInput)
	}

	return uint64(count), nil
}

func acquisitionReadError(kind string, id ton.BlockIDExt, err error) error {
	if errors.Is(err, storage.ErrNotFound) {
		return fmt.Errorf("%w: exact %s for block %d is unavailable", ErrAcquisitionNotReady, kind, id.SeqNo)
	}

	return fmt.Errorf("read %s for block %d: %w", kind, id.SeqNo, err)
}

func (v *localMasterView) blockAt(seqno uint32) (ton.BlockIDExt, error) {
	if seqno == v.context.ID.SeqNo {
		return cloneBlockID(v.context.ID), nil
	}
	if seqno > v.context.ID.SeqNo {
		return ton.BlockIDExt{}, fmt.Errorf("%w: masterchain block %d is ahead of context %d",
			ErrInvalidInput, seqno, v.context.ID.SeqNo)
	}

	return oldMasterBlockID(v.info.PrevBlocks, seqno)
}

func (v *localMasterView) requireAncestor(ancestor ton.BlockIDExt) error {
	if ancestor.Workchain != masterchainWorkchainID || ancestor.Shard != math.MinInt64 ||
		ancestor.SeqNo > v.context.ID.SeqNo {
		return fmt.Errorf("%w: invalid minimum masterchain block", ErrInvalidInput)
	}
	resolved, err := v.blockAt(ancestor.SeqNo)
	if err != nil {
		return err
	}
	if !ancestor.Equals(&resolved) {
		return fmt.Errorf("%w: minimum masterchain block is not an ancestor of the context", ErrInvalidInput)
	}

	return nil
}

func (v *localMasterView) requirePredecessorMasterReference(
	previous ton.BlockIDExt,
	reference *tlb.ExtBlkRef,
) error {
	if reference == nil {
		if previous.SeqNo == 0 {
			return nil
		}
		return fmt.Errorf("%w: shard predecessor has no masterchain reference", ErrInvalidInput)
	}
	resolved, err := v.blockAt(reference.SeqNo)
	if err != nil {
		return err
	}
	if !bytes.Equal(resolved.RootHash, reference.RootHash) || !bytes.Equal(resolved.FileHash, reference.FileHash) {
		return fmt.Errorf("%w: shard predecessor masterchain reference is not in the selected context", ErrInvalidInput)
	}

	return nil
}

func (a *LocalAcquisition) masterViewForPredecessor(
	ctx context.Context,
	base *localMasterView,
	previous PreviousBlock,
	asOf time.Time,
) (*localMasterView, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if previous.ID.Equals(&base.context.ID) {
		return base, nil
	}
	if previous.ID.Workchain != masterchainWorkchainID || previous.ID.Shard != math.MinInt64 {
		return nil, fmt.Errorf("%w: speculative masterchain predecessor has invalid shard", ErrInvalidInput)
	}

	var state tlb.ShardStateUnsplit
	if err := parseExact(&state, previous.State); err != nil {
		return nil, fmt.Errorf("%w: decode speculative masterchain state: %v", ErrInvalidInput, err)
	}
	if state.Seqno != previous.ID.SeqNo || state.ShardIdent.WorkchainID != masterchainWorkchainID ||
		int64(state.ShardIdent.GetShardID()) != math.MinInt64 {
		return nil, fmt.Errorf("%w: speculative masterchain state differs from its block id", ErrInvalidInput)
	}
	projected, err := a.groups.Project(base.context.Groups, groups.ApplyInput{
		Block: cloneBlockID(previous.ID),
		Root:  previous.State,
		AsOf:  asOf,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: project speculative validator groups: %v", ErrAcquisitionNotReady, err)
	}

	return a.masterView(previous, state, projected)
}
