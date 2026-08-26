package collator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/rs/zerolog"
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

// LocalStateStore is the exact read surface required by local acquisition. The
// node live store implements it directly; acquisition never reads a different
// database, falls back to a newer chain position, or starts a download.
type LocalStateStore interface {
	BlockState(context.Context, ton.BlockIDExt) (*storage.BlockState, error)
	LoadStateCellTree(context.Context, ton.BlockIDExt, []byte) (*cell.Cell, error)
	BlockRoot(context.Context, ton.BlockIDExt) (*cell.Cell, error)
	WaitBlockArtifacts(context.Context, ton.BlockIDExt) error
}

// LocalGroupSource publishes current and retained canonical validator-group
// snapshots. A speculative view must name its previous snapshot explicitly;
// an unavailable historical view returns ErrAcquisitionNotReady instead of a
// mixed snapshot.
type LocalGroupSource interface {
	Snapshot() (*groups.Snapshot, error)
	Project(*groups.Snapshot, groups.ApplyInput) (*groups.Snapshot, error)
	WaitProject(context.Context, *groups.Snapshot, groups.ApplyInput) (*groups.Snapshot, error)
}

type acquisitionInputWait struct {
	duration time.Duration
}

type acquisitionReadMode struct {
	waits *acquisitionInputWait
}

var acquisitionReadImmediate acquisitionReadMode

func (m acquisitionReadMode) waitsForInputs() bool {
	return m.waits != nil
}

func (m acquisitionReadMode) waitFor(wait func() error) error {
	started := time.Now()
	err := wait()
	m.waits.duration += time.Since(started)

	return err
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

// maxLocalMasterViews bounds the non-resident masterchain views kept alongside
// the resident one. The live working set is the resident view plus the one or
// two immediately older ones that lagging session updates and older candidate
// MasterRefs name. Each entry pins one masterchain state tree together with its
// decoded extra, registry and libraries, so this cap is about resident memory
// rather than freshness — an evicted view is only ever rebuilt.
const maxLocalMasterViews = 4

// maxLocalMasterViewLag drops cached views that fell this far behind the
// resident one. It is kept at localBlockSourceGenerations because a validation
// running on a view older than that would re-read its blocks from storage and
// refresh their generation, which can evict fresher block-cache entries.
const maxLocalMasterViewLag = localBlockSourceGenerations

type localConfigCache struct {
	// log carries the one event this cache can emit: a configuration whose parse
	// footprint could not be captured. prepare runs once per configuration root,
	// so it fires once per epoch rather than once per block.
	log     zerolog.Logger
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
	// The view being replaced is exactly the one lagging sessions and candidate
	// masterchain references are about to ask for, so demoting it into the entry
	// map is the highest-value insert there is. The map is written only here, in
	// the same critical section that already refused to move the resident view
	// backwards, so a publisher that lost the race can never publish into it.
	a.master.demoteLocked(a.master.view)
	a.master.view = view
	a.master.sweepLocked(id.SeqNo)
	// Registered neighbor tops only move when the masterchain view moves, so
	// installing a view is the exact bound on cached block lifetime.
	a.blocks.advance()
	a.warmRegisteredTops(view)

	return nil
}

// lookup returns the immutable view of id and whether it is the resident one.
// A hit is always valid: the cache is keyed by block root hash and a block root
// hash can never name two different states, but the full id is still compared so
// a collision with a different seqno or file hash reads as a miss.
//
// The resident flag is what callers use to decide whether the group binding still
// has to be revalidated: PublishMasterchainView bound the resident view to its
// snapshot when it installed it, while a demoted entry can outlive the tracker's
// history of its block.
func (c *localMasterViewCache) lookup(id ton.BlockIDExt) (*localMasterView, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.view != nil && id.Equals(&c.view.context.ID) {
		return c.view, true
	}
	key, err := blockRootKey(id)
	if err != nil {
		return nil, false
	}
	if entry := c.entries[key]; entry != nil && entry.context.ID.Equals(&id) {
		return entry, false
	}

	return nil, false
}

// store publishes one built view. Unlike the resident install it has no side
// effects: it never advances the block cache and never warms registered tops,
// because those are correct exactly once per masterchain block and a cached
// candidate view is not one.
func (c *localMasterViewCache) store(view *localMasterView) {
	if view == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.view != nil && view.context.ID.Equals(&c.view.context.ID) {
		return
	}
	c.demoteLocked(view)
	if c.view != nil {
		c.sweepLocked(c.view.context.ID.SeqNo)
	}
}

func (c *localMasterViewCache) demoteLocked(view *localMasterView) {
	if view == nil {
		return
	}
	key, err := blockRootKey(view.context.ID)
	if err != nil {
		return
	}
	if _, exists := c.entries[key]; exists {
		return
	}
	if c.entries == nil {
		c.entries = make(map[[32]byte]*localMasterView, maxLocalMasterViews)
	}
	c.entries[key] = view
	c.order = append(c.order, key)
	for len(c.order) > maxLocalMasterViews {
		delete(c.entries, c.order[0])
		c.order = append(c.order[:0], c.order[1:]...)
	}
}

// sweepLocked drops views that fell too far behind the resident one. Insertion
// order alone would keep an ancient view alive while the chain moved on.
func (c *localMasterViewCache) sweepLocked(resident uint32) {
	kept := c.order[:0]
	for _, key := range c.order {
		entry := c.entries[key]
		if entry == nil {
			continue
		}
		if entry.context.ID.SeqNo+maxLocalMasterViewLag <= resident {
			delete(c.entries, key)
			continue
		}
		kept = append(kept, key)
	}
	c.order = kept
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
					_, _ = a.blockSource(ctx, id, acquisitionReadImmediate)
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
	if root.IsVirtualized() {
		parsed, err := parseMasterConfigEpoch(root)
		if err != nil {
			return localPreparedConfig{}, err
		}

		// A collated masterchain proof carries only the configuration paths the
		// candidate touched. Its root hash authenticates those paths, but the
		// physical tree is not an epoch-wide execution context: a later contract
		// may request another CONFIGPARAM and hit a pruned boundary. Keep it local
		// to this validation; only a resident configuration may populate the cache.
		return localPreparedConfig{execution: parsed.execution, config: parsed.config, groups: parsed.groups}, nil
	}

	// The capture runs here, before the entry is published, so no other goroutine
	// can observe a Config whose footprint is still being filled in. It is also
	// the only place it can run: parseMasterConfigEpoch is what the capture
	// records, and master collation's own fresh branch calls that inside a live
	// collation.
	//
	// It runs first because it is what materializes the configuration, and the
	// parse below then reads cells that are already in memory. Reversed — the
	// order this had when it was written — the parse pages the configuration in
	// and the capture pages it in again: measured on mainnet, 5992 loads against
	// the 2962 one pass takes. See TestPrepareConfigMaterializesBeforeParsing.
	//
	// This is reached inline from candidate validation, on the first block of a
	// configuration epoch, so what it costs there is what a validator pays inside
	// a consensus slot. What remains after the reorder is measured and small: one
	// pass reads 2928 cells against the 2791 the parses need on their own, and
	// the whole of prepare costs 3.16ms of CPU against 0.92ms for a parse with no
	// capture at all. It is left here rather than moved off the path because the
	// alternative is worse in both directions — publishing without a footprint
	// makes master collation re-parse 0.92ms of configuration on every block, and
	// filling one in afterwards races the readers this runs before.
	resident, footprint := captureConfigFootprint(root)
	if resident == nil {
		resident = root
	}
	parsed, err := parseMasterConfigEpoch(resident)
	if err != nil {
		return localPreparedConfig{}, err
	}
	parsed.config.footprint = footprint
	if footprint == nil {
		c.log.Warn().Hex("config", hash[:]).
			Msg("configuration footprint was not captured, masterchain collation will re-parse the config")
	}
	prepared = localPreparedConfig{execution: parsed.execution, config: parsed.config, groups: parsed.groups}

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
	mode acquisitionReadMode,
) (PreviousBlock, tlb.ShardStateUnsplit, error) {
	source, err := a.blockSource(ctx, id, mode)
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
func (a *LocalAcquisition) blockSource(
	ctx context.Context,
	id ton.BlockIDExt,
	mode acquisitionReadMode,
) (*localBlockSource, error) {
	for {
		source, err := a.blocks.lookup(id)
		if err == nil {
			return source, nil
		}
		if !errors.Is(err, ErrNotFound) {
			return nil, err
		}

		source, err = a.blocks.loadOnce(ctx, id, func() (*localBlockSource, error) {
			previous, state, readErr := a.readPrevious(ctx, id)
			if readErr != nil {
				return nil, readErr
			}

			return &localBlockSource{previous: previous, state: state}, nil
		})
		if err == nil || !mode.waitsForInputs() || !errors.Is(err, ErrAcquisitionNotReady) {
			return source, err
		}
		if err = mode.waitFor(func() error {
			return a.store.WaitBlockArtifacts(ctx, id)
		}); err != nil {
			return nil, acquisitionReadError("block artifacts", id, err)
		}
	}
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
	if len(stored.StateRootHash) == 32 && !equalCellHashBytes(root, stored.StateRootHash) {
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
