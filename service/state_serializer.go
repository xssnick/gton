package service

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/xssnick/gton/internal/logutil"
	state2 "github.com/xssnick/gton/service/state"
	"github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	stateSerializationProgressInterval = 5 * time.Second
)

var (
	errStateSerializationRunning    = errors.New("state serialization is already running")
	errStateSerializationNotRunning = errors.New("state serialization is not running")
	errStateSerializationCanceled   = errors.New("state serialization canceled")
	errStateSerializationDir        = errors.New("persistent state serialization dir is not configured")
)

type stateSerializer struct {
	log                   zerolog.Logger
	store                 storage.Storage
	dir                   string
	disableAutomatic      bool
	largeBOCBatchSize     int
	stateSerializeOnePass bool

	mu        sync.Mutex
	activeRun *stateSerializationRun

	attemptMu         sync.Mutex
	automaticAttempts map[uint32]int
}

type stateSerializationRun struct {
	master     ton.BlockIDExt
	scope      PersistentStateSerializationScope
	cancel     context.CancelCauseFunc
	done       chan struct{}
	err        error
	cleanupErr error
}

type stateSerializationTarget struct {
	block      ton.BlockIDExt
	state      *storage.BlockState
	kind       string
	splitDepth uint32
}

type PersistentStateSerializationScope uint8

const (
	PersistentStateSerializationAll PersistentStateSerializationScope = iota
	PersistentStateSerializationBasechain
)

type serializedStateFile struct {
	path     string
	size     int64
	fileHash []byte
}

func newStateSerializer(logger zerolog.Logger, store storage.Storage, dir string, disableAutomatic bool, largeBOCBatchSize int, stateSerializeOnePass bool) *stateSerializer {
	if largeBOCBatchSize <= 0 {
		largeBOCBatchSize = defaultPersistentStateLargeBOCBatchSize
	}
	return &stateSerializer{
		log:                   logger.With().Str("component", "state_serializer").Logger(),
		store:                 store,
		dir:                   dir,
		disableAutomatic:      disableAutomatic,
		largeBOCBatchSize:     largeBOCBatchSize,
		stateSerializeOnePass: stateSerializeOnePass,
		automaticAttempts:     make(map[uint32]int),
	}
}

func (s *Service) StartPersistentStateSerialization(ctx context.Context, masterSeqno uint32, scope PersistentStateSerializationScope) error {
	if !scope.valid() {
		return fmt.Errorf("unknown persistent state serialization scope: %d", scope)
	}
	lease, err := s.beginExclusiveServiceTask(ctx, exclusiveServiceTaskStateSerialization)
	if err != nil {
		return err
	}

	master, err := s.durableMasterchainBlock(ctx, masterSeqno, "persistent state serialization")
	if err != nil {
		lease.release()
		return err
	}
	if err = s.ensurePersistentStateSerializationDiskSpace(ctx, master, statFSSyncDiskSpace); err != nil {
		lease.release()
		return err
	}
	if err = s.stateSerializer.start(ctx, master, scope, s.runAsync, s.afterPersistentStateSerialized, lease.release); err != nil {
		lease.release()
		return err
	}
	return nil
}

func (s *Service) CancelPersistentStateSerialization(ctx context.Context) error {
	s.stateSerializer.mu.Lock()
	run := s.stateSerializer.activeRun
	if run == nil {
		s.stateSerializer.mu.Unlock()
		return errStateSerializationNotRunning
	}
	done := run.done
	run.cancel(errStateSerializationCanceled)
	s.stateSerializer.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return run.cleanupErr
	}
}

func (s PersistentStateSerializationScope) valid() bool {
	return s == PersistentStateSerializationAll || s == PersistentStateSerializationBasechain
}

func (s PersistentStateSerializationScope) String() string {
	switch s {
	case PersistentStateSerializationAll:
		return "all"
	case PersistentStateSerializationBasechain:
		return "basechain"
	default:
		return fmt.Sprintf("unknown_%d", s)
	}
}

func (s *Service) durableMasterchainBlock(ctx context.Context, masterSeqno uint32, operation string) (ton.BlockIDExt, error) {
	if err := s.waitCurrentStatePersist(ctx); err != nil {
		return ton.BlockIDExt{}, fmt.Errorf("wait durable current state: %w", err)
	}

	current, err := s.storage.CurrentState(ctx)
	if err != nil {
		return ton.BlockIDExt{}, fmt.Errorf("load durable current state: %w", err)
	}

	durable := current.Masterchain.Block
	if durable.Workchain != -1 || durable.Shard != topShard {
		return ton.BlockIDExt{}, fmt.Errorf("durable current state masterchain block is invalid: %s", storage.FormatBlockRef(durable))
	}
	if masterSeqno > durable.SeqNo {
		return ton.BlockIDExt{}, fmt.Errorf("%s target is newer than durable current masterchain block: requested_seqno=%d durable=%s", operation, masterSeqno, storage.FormatBlockRef(durable))
	}

	if masterSeqno == durable.SeqNo {
		return durable, nil
	}

	master, err := s.storage.LookupBlockBySeqNo(ctx, storage.BlockSeqRef{Workchain: -1, Shard: topShard, SeqNo: masterSeqno})
	if err != nil {
		return ton.BlockIDExt{}, fmt.Errorf("lookup durable masterchain block #%d: %w", masterSeqno, err)
	}
	return master, nil
}

func (s *stateSerializer) start(ctx context.Context, master ton.BlockIDExt, scope PersistentStateSerializationScope, runAsync func(func()), after func(context.Context, ton.BlockIDExt, PersistentStateSerializationScope), done func()) error {
	if s.dir == "" {
		return errStateSerializationDir
	}
	if master.Workchain != -1 || master.Shard != topShard {
		return fmt.Errorf("persistent state serialization target is not masterchain: %s", storage.FormatBlockRef(master))
	}

	runCtx, run, err := s.acquire(ctx, master, scope)
	if err != nil {
		return err
	}

	runAsync(func() {
		if err := s.finishRun(runCtx, run, s.run(runCtx, master, scope)); err != nil {
			if done != nil {
				done()
			}
			event := s.log.Error()
			msg := "persistent state serialization failed"
			if errors.Is(err, errStateSerializationCanceled) {
				event = s.log.Warn()
				msg = "persistent state serialization canceled"
			}
			event.
				Err(err).
				Str("masterchain", storage.FormatBlockRef(master)).
				Str("scope", scope.String()).
				Msg(msg)
			return
		}

		s.log.Info().
			Str("masterchain", storage.FormatBlockRef(master)).
			Str("scope", scope.String()).
			Msg("persistent state serialization finished")
		if done != nil {
			done()
		}
		if after != nil {
			after(runCtx, master, scope)
		}
	})
	return nil
}

func (s *stateSerializer) runExclusive(ctx context.Context, master ton.BlockIDExt, scope PersistentStateSerializationScope) error {
	if s.dir == "" {
		return errStateSerializationDir
	}
	runCtx, run, err := s.acquire(ctx, master, scope)
	if err != nil {
		return err
	}

	return s.finishRun(runCtx, run, s.run(runCtx, master, scope))
}

func (s *stateSerializer) acquire(ctx context.Context, master ton.BlockIDExt, scope PersistentStateSerializationScope) (context.Context, *stateSerializationRun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.activeRun != nil {
		return nil, nil, errStateSerializationRunning
	}

	runCtx, cancel := context.WithCancelCause(ctx)
	run := &stateSerializationRun{
		master: master,
		scope:  scope,
		cancel: cancel,
		done:   make(chan struct{}),
	}
	s.activeRun = run
	return runCtx, run, nil
}

func (s *stateSerializer) finishRun(ctx context.Context, run *stateSerializationRun, err error) error {
	finishedErr := err
	if errors.Is(context.Cause(ctx), errStateSerializationCanceled) {
		run.cleanupErr = s.cleanupSerializedState(context.Background(), run.master, run.scope)
		if clearErr := s.clearActiveSerializationMarker(context.Background(), run.master); clearErr != nil {
			run.cleanupErr = errors.Join(run.cleanupErr, clearErr)
		}
		if run.cleanupErr != nil {
			finishedErr = fmt.Errorf("%w: %w", errStateSerializationCanceled, run.cleanupErr)
		} else {
			finishedErr = errStateSerializationCanceled
		}
	}
	run.err = finishedErr
	s.release(run)
	return finishedErr
}

func (s *stateSerializer) release(run *stateSerializationRun) {
	s.mu.Lock()
	if s.activeRun == run {
		s.activeRun = nil
	}
	s.mu.Unlock()
	close(run.done)
}

func (s *stateSerializer) cleanupSerializedState(ctx context.Context, master ton.BlockIDExt, scope PersistentStateSerializationScope) error {
	targets, err := s.loadTargets(ctx, master, scope)
	if err != nil {
		return err
	}

	var cleanupErr error
	for _, target := range targets {
		loader := newLargeBOCStateLoader(ctx, s.store, target.state.CellGeneration)
		root, err := s.lazyStateRoot(ctx, target, loader.Load)
		if err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("load cleanup state root %s: %w", storage.FormatBlockRef(target.block), err))
			continue
		}

		parts, err := state2.SplitPersistentState(target.block, root, target.splitDepth)
		if err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("split cleanup state %s: %w", storage.FormatBlockRef(target.block), err))
			continue
		}

		for _, part := range parts {
			err = s.store.DeletePersistentStateFile(ctx, target.block, master, part.EffectiveShard)
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			if err != nil {
				cleanupErr = errors.Join(cleanupErr, fmt.Errorf("delete serialized state %s effective_shard=%016x: %w", storage.FormatBlockRef(target.block), uint64(part.EffectiveShard), err))
				continue
			}

			s.log.Info().
				Str("block", storage.FormatBlockRef(target.block)).
				Str("masterchain", storage.FormatBlockRef(master)).
				Str("kind", target.kind).
				Str("part", part.Kind.String()).
				Int64("effective_shard", part.EffectiveShard).
				Msg("persistent state part removed after serialization cancel")
		}
	}
	return cleanupErr
}

func (s *stateSerializer) clearActiveSerializationMarker(ctx context.Context, master ton.BlockIDExt) error {
	active, err := s.store.ActivePersistentStateSerialization(ctx)
	if errors.Is(err, storage.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if !active.Block.Equals(&master) {
		return nil
	}
	return clearActivePersistentStateSerialization(ctx, s.store, master)
}

func (s *stateSerializer) run(ctx context.Context, master ton.BlockIDExt, scope PersistentStateSerializationScope) error {
	targets, err := s.loadTargets(ctx, master, scope)
	if err != nil {
		return err
	}

	s.log.Info().
		Str("masterchain", storage.FormatBlockRef(master)).
		Str("scope", scope.String()).
		Int("states", len(targets)).
		Msg("persistent state serialization started")

	for i, target := range targets {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		s.log.Info().
			Str("block", storage.FormatBlockRef(target.block)).
			Str("masterchain", storage.FormatBlockRef(master)).
			Str("kind", target.kind).
			Int("index", i+1).
			Int("total", len(targets)).
			Uint32("split_depth", target.splitDepth).
			Msg("preparing persistent state serialization")

		loader := newLargeBOCStateLoader(ctx, s.store, target.state.CellGeneration)
		root, err := s.lazyStateRoot(ctx, target, loader.Load)
		if err != nil {
			return fmt.Errorf("load lazy %s state root %s: %w", target.kind, storage.FormatBlockRef(target.block), err)
		}

		parts, err := state2.SplitPersistentState(target.block, root, target.splitDepth)
		if err != nil {
			return fmt.Errorf("split %s state %s: %w", target.kind, storage.FormatBlockRef(target.block), err)
		}
		onePassLargeBOC := s.useOnePassLargeBOCForStateSerialization(target, parts)

		for partIndex, part := range parts {
			file, err := s.existingSerializedState(ctx, master, target, part)
			if err == nil {
				s.log.Info().
					Str("block", storage.FormatBlockRef(target.block)).
					Str("masterchain", storage.FormatBlockRef(master)).
					Str("kind", target.kind).
					Str("part", part.Kind.String()).
					Int64("effective_shard", part.EffectiveShard).
					Int("index", i+1).
					Int("total", len(targets)).
					Int("part_index", partIndex+1).
					Int("parts", len(parts)).
					Bool("one_pass_large_boc", onePassLargeBOC).
					Int64("size", file.size).
					Str("path", file.path).
					Msg("persistent state part already serialized")
				continue
			}
			if !errors.Is(err, storage.ErrNotFound) {
				return fmt.Errorf("check existing %s state %s part %s: %w", target.kind, storage.FormatBlockRef(target.block), part.Kind, err)
			}
			s.log.Info().
				Str("block", storage.FormatBlockRef(target.block)).
				Str("masterchain", storage.FormatBlockRef(master)).
				Str("kind", target.kind).
				Str("part", part.Kind.String()).
				Int64("effective_shard", part.EffectiveShard).
				Int("index", i+1).
				Int("total", len(targets)).
				Int("part_index", partIndex+1).
				Int("parts", len(parts)).
				Bool("one_pass_large_boc", onePassLargeBOC).
				Msg("serializing persistent state part")

			file, err = s.serializeStatePart(ctx, master, target, part, onePassLargeBOC)
			if err != nil {
				return fmt.Errorf("serialize %s state %s part %s: %w", target.kind, storage.FormatBlockRef(target.block), part.Kind, err)
			}
			if err = s.store.SavePersistentStateFile(&storage.PersistentStateFile{
				Block:            target.block,
				MasterchainBlock: master,
				EffectiveShard:   part.EffectiveShard,
				Ref: &storage.ArtifactRef{
					Path:   file.path,
					Offset: 0,
					Size:   file.size,
				},
				FileHash:      file.fileHash,
				StateRootHash: target.state.StateRootHash,
			}); err != nil {
				return fmt.Errorf("register persistent state file: %w", err)
			}

			s.log.Info().
				Str("block", storage.FormatBlockRef(target.block)).
				Str("masterchain", storage.FormatBlockRef(master)).
				Str("kind", target.kind).
				Str("part", part.Kind.String()).
				Int64("effective_shard", part.EffectiveShard).
				Int64("size", file.size).
				Str("path", file.path).
				Msg("persistent state part serialized and registered")
		}
	}
	return nil
}

func (s *stateSerializer) existingSerializedState(ctx context.Context, master ton.BlockIDExt, target stateSerializationTarget, part state2.PersistentStatePart) (serializedStateFile, error) {
	finalPath, err := s.serializedStatePath(master, target.block, part.EffectiveShard)
	if err != nil {
		return serializedStateFile{}, err
	}

	size, err := s.store.PersistentStateSize(ctx, target.block, master, part.EffectiveShard)
	if errors.Is(err, storage.ErrNotFound) {
		return serializedStateFile{}, storage.ErrNotFound
	}
	if err != nil {
		return serializedStateFile{}, err
	}
	return serializedStateFile{
		path: finalPath,
		size: size,
	}, nil
}

func (s *stateSerializer) loadTargets(ctx context.Context, master ton.BlockIDExt, scope PersistentStateSerializationScope) ([]stateSerializationTarget, error) {
	masterState, err := s.store.BlockState(ctx, master)
	if err != nil {
		return nil, fmt.Errorf("load masterchain state %s: %w", storage.FormatBlockRef(master), err)
	}

	shards, err := state2.ShardBlocksFromMasterState(masterState)
	if err != nil {
		return nil, fmt.Errorf("load shard blocks from masterchain state %s: %w", storage.FormatBlockRef(master), err)
	}
	sort.Slice(shards, func(i, j int) bool {
		if shards[i].Workchain != shards[j].Workchain {
			return shards[i].Workchain < shards[j].Workchain
		}
		if shards[i].Shard != shards[j].Shard {
			return shards[i].Shard < shards[j].Shard
		}
		return shards[i].SeqNo < shards[j].SeqNo
	})

	targets := make([]stateSerializationTarget, 0, 1+len(shards))
	if scope == PersistentStateSerializationAll {
		targets = append(targets, stateSerializationTarget{
			block: master,
			state: masterState,
			kind:  "masterchain",
		})
	}
	splitDepthBlocks := shards
	if scope == PersistentStateSerializationBasechain {
		splitDepthBlocks = make([]ton.BlockIDExt, 0, len(shards))
		for _, shard := range shards {
			if shard.Workchain == 0 {
				splitDepthBlocks = append(splitDepthBlocks, shard)
			}
		}
	}
	splitDepths, err := state2.PersistentStateSplitDepths(masterState, splitDepthBlocks)
	if err != nil {
		return nil, fmt.Errorf("load persistent state split depths for %s: %w", storage.FormatBlockRef(master), err)
	}

	for _, shard := range shards {
		if scope == PersistentStateSerializationBasechain && shard.Workchain != 0 {
			continue
		}

		state, err := s.store.BlockState(ctx, shard)
		if err != nil {
			return nil, fmt.Errorf("load shard state %s: %w", storage.FormatBlockRef(shard), err)
		}
		kind := "shardchain"
		if shard.Workchain == 0 {
			kind = "basechain"
		}
		targets = append(targets, stateSerializationTarget{
			block:      shard,
			state:      state,
			kind:       kind,
			splitDepth: splitDepths[shard.Workchain],
		})
	}
	return targets, nil
}

func useOnePassLargeBOCForStateSerialization(target stateSerializationTarget, parts []state2.PersistentStatePart) bool {
	if target.block.Workchain != 0 {
		return false
	}

	splitAccounts := 0
	for _, part := range parts {
		if part.Kind == state2.PersistentStatePartSplitAccount {
			splitAccounts++
		}
	}
	return splitAccounts >= persistentStateOnePassSplitAccountThreshold
}

func (s *stateSerializer) useOnePassLargeBOCForStateSerialization(target stateSerializationTarget, parts []state2.PersistentStatePart) bool {
	return s.stateSerializeOnePass || useOnePassLargeBOCForStateSerialization(target, parts)
}

func (s *stateSerializer) serializeStatePart(ctx context.Context, master ton.BlockIDExt, target stateSerializationTarget, part state2.PersistentStatePart, onePassLargeBOC bool) (serializedStateFile, error) {
	var err error
	if part.Kind != state2.PersistentStatePartUnsplit {
		records, err := splitStateCellRecords(ctx, part.Root)
		if err != nil {
			return serializedStateFile{}, err
		}
		if err = s.saveCellsForTarget(ctx, target, records); err != nil {
			return serializedStateFile{}, fmt.Errorf("save split state synthetic roots: %w", err)
		}
	}

	loader := newLargeBOCStateLoader(ctx, s.store, target.state.CellGeneration)

	if err = os.MkdirAll(s.dir, 0o755); err != nil {
		return serializedStateFile{}, err
	}

	finalPath, err := s.serializedStatePath(master, target.block, part.EffectiveShard)
	if err != nil {
		return serializedStateFile{}, err
	}
	tmp, err := os.CreateTemp(s.dir, "."+filepath.Base(finalPath)+".*.tmp")
	if err != nil {
		return serializedStateFile{}, err
	}

	tmpPath := tmp.Name()
	keepTmp := false
	defer func() {
		if !keepTmp {
			_ = os.Remove(tmpPath)
		}
	}()

	cellsCountHint := s.previousPersistentStateCellsCountHint(ctx, master, target, part)
	if cellsCountHint > 0 {
		s.log.Info().
			Str("block", storage.FormatBlockRef(target.block)).
			Str("kind", stateSerializationLogKind(target, part)).
			Int64("effective_shard", part.EffectiveShard).
			Uint64("cells_count_hint", cellsCountHint).
			Msg("presizing persistent state serialization from previous state cells count")
	}

	rootHash := part.Root.HashKey()
	progressStop := startStateSerializationProgress(ctx, s.log, stateSerializationLogBlock(target, part), stateSerializationLogKind(target, part), loader)
	hash := sha256.New()
	writer := io.MultiWriter(contextWriter{ctx: ctx, w: tmp}, hash)

	releaseCompactions := s.store.ThrottleCellCompactions()
	if onePassLargeBOC {
		err = cell.ToLargeBOCOnePass(writer, []cell.Hash{rootHash}, persistentStateBOCOptions(), loader, cellsCountHint, s.largeBOCBatchSize)
	} else {
		err = cell.ToLargeBOC(writer, []cell.Hash{rootHash}, persistentStateBOCOptions(), loader, cellsCountHint, s.largeBOCBatchSize)
	}
	releaseCompactions()
	progressStop()
	if err != nil {
		_ = tmp.Close()
		return serializedStateFile{}, err
	}
	if err = tmp.Sync(); err != nil {
		_ = tmp.Close()
		return serializedStateFile{}, err
	}
	if err = tmp.Close(); err != nil {
		return serializedStateFile{}, err
	}

	stat, err := os.Stat(tmpPath)
	if err != nil {
		return serializedStateFile{}, err
	}
	if err = os.Rename(tmpPath, finalPath); err != nil {
		return serializedStateFile{}, err
	}
	keepTmp = true
	if err = syncDir(filepath.Dir(finalPath)); err != nil {
		return serializedStateFile{}, err
	}

	return serializedStateFile{
		path:     finalPath,
		size:     stat.Size(),
		fileHash: hash.Sum(nil),
	}, nil
}

func (s *stateSerializer) lazyStateRoot(ctx context.Context, target stateSerializationTarget, loader cell.LazyCellLoader) (*cell.Cell, error) {
	if len(target.state.StateRootHash) != 32 {
		return nil, fmt.Errorf("state root hash size mismatch: %d", len(target.state.StateRootHash))
	}

	record, err := s.cellRecordForTarget(ctx, target, target.state.StateRootHash)
	if err != nil {
		return nil, err
	}
	return storage.LazyCellRecord(record, loader)
}

func (s *stateSerializer) saveCellsForTarget(ctx context.Context, target stateSerializationTarget, records []*storage.CellRecord) error {
	if target.state.CellGeneration != 0 {
		return s.store.SaveCellsInGeneration(ctx, target.state.CellGeneration, records)
	}
	return s.store.SaveCells(records)
}

func (s *stateSerializer) cellRecordForTarget(ctx context.Context, target stateSerializationTarget, hash []byte) (*storage.CellRecord, error) {
	if target.state.CellGeneration != 0 {
		return s.store.CellRecordInGeneration(ctx, target.state.CellGeneration, hash)
	}
	return s.store.CellRecord(ctx, hash)
}

func splitStateCellRecords(ctx context.Context, root *cell.Cell) ([]*storage.CellRecord, error) {
	records := make([]*storage.CellRecord, 0, 8)
	seen := make(map[cell.Hash]struct{})

	var visit func(*cell.Cell) error
	visit = func(cl *cell.Cell) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		meta := cl.GetMetadata()
		if _, ok := seen[meta.Hash]; ok {
			return nil
		}
		seen[meta.Hash] = struct{}{}

		record, err := storage.CellRecordFromCell(cl)
		if err != nil {
			return fmt.Errorf("build split synthetic cell record %x: %w", meta.Hash[:], err)
		}
		records = append(records, record)

		for i, ref := range meta.Refs {
			if ref.Lazy {
				continue
			}
			child, err := cl.PeekRef(i)
			if err != nil {
				return fmt.Errorf("load split synthetic cell ref %d for %x: %w", i, meta.Hash[:], err)
			}
			if err = visit(child); err != nil {
				return err
			}
		}
		return nil
	}

	if err := visit(root); err != nil {
		return nil, err
	}
	return records, nil
}

func (s *stateSerializer) serializedStatePath(master ton.BlockIDExt, block ton.BlockIDExt, effectiveShard int64) (string, error) {
	name, err := storage.PersistentStateFileName(block, master, effectiveShard)
	if err != nil {
		return "", err
	}
	return filepath.Join(s.dir, name), nil
}

func persistentStateBOCOptions() cell.BOCSerializeOptions {
	return cell.BOCSerializeOptions{
		WithCRC32C:    true,
		WithIndex:     true,
		WithCacheBits: true,
		WithTopHash:   true,
		WithIntHashes: true,
	}
}

const (
	defaultPersistentStateLargeBOCBatchSize     = 512 << 10
	persistentStateOnePassSplitAccountThreshold = 4
)

func formatStateSerializationETA(done, total uint64, elapsed time.Duration) string {
	if done == 0 || done >= total || elapsed <= 0 {
		return "unknown"
	}

	rate := float64(done) / elapsed.Seconds()
	if rate <= 0 {
		return "unknown"
	}

	eta := time.Duration(float64(total-done) / rate * float64(time.Second)).Round(time.Second)
	return eta.String()
}

func formatStateSerializationElapsed(elapsed time.Duration) string {
	if elapsed <= 0 {
		return "0s"
	}
	if elapsed < time.Second {
		return elapsed.Round(time.Millisecond).String()
	}
	return elapsed.Round(time.Second).String()
}

func stateSerializationLogKind(target stateSerializationTarget, part state2.PersistentStatePart) string {
	if part.Kind == state2.PersistentStatePartUnsplit {
		return target.kind
	}
	return target.kind + "_" + part.Kind.String()
}

func stateSerializationLogBlock(target stateSerializationTarget, part state2.PersistentStatePart) ton.BlockIDExt {
	block := target.block
	if part.Kind != state2.PersistentStatePartUnsplit {
		block.Shard = part.EffectiveShard
	}
	return block
}

const (
	largeBOCStatePhaseMeta uint32 = iota
	largeBOCStatePhasePayload
	largeBOCStatePhaseCells
)

type largeBOCStateLoader struct {
	ctx        context.Context
	store      storage.Storage
	generation uint64

	phase              atomic.Uint32
	dataPhaseStarted   atomic.Int64
	metaLoaded         atomic.Uint64
	cellLoaded         atomic.Uint64
	metaBatches        atomic.Uint64
	dataBatches        atomic.Uint64
	lastMetaBatchCells atomic.Uint64
	lastDataBatchCells atomic.Uint64
	lastMetaBatchNanos atomic.Int64
	lastDataBatchNanos atomic.Int64
}

func newLargeBOCStateLoader(ctx context.Context, store storage.Storage, generation uint64) *largeBOCStateLoader {
	return &largeBOCStateLoader{
		ctx:        ctx,
		store:      store,
		generation: generation,
	}
}

func (l *largeBOCStateLoader) Load(hash cell.Hash) (*cell.Cell, error) {
	select {
	case <-l.ctx.Done():
		return nil, l.ctx.Err()
	default:
	}

	var record *storage.CellRecord
	var err error
	if l.generation != 0 {
		record, err = l.store.CellRecordInGeneration(l.ctx, l.generation, hash[:])
	} else {
		record, err = l.store.CellRecord(l.ctx, hash[:])
	}
	if err != nil {
		return nil, err
	}
	return storage.LazyCellRecord(record, l.Load)
}

func (l *largeBOCStateLoader) LoadMeta(hashes []cell.Hash, dst []cell.LargeBOCMetaRecord) ([]cell.LargeBOCMetaRecord, error) {
	select {
	case <-l.ctx.Done():
		return dst, l.ctx.Err()
	default:
	}

	base := len(dst)
	var records []cell.LargeBOCMetaRecord
	var err error
	started := time.Now()
	if l.generation != 0 {
		records, err = l.store.LargeBOCLoadMetaInGeneration(l.ctx, l.generation, hashes, dst)
	} else {
		records, err = l.store.LargeBOCLoadMeta(l.ctx, hashes, dst)
	}
	if err != nil {
		return dst, err
	}
	if loaded := len(records) - base; loaded > 0 {
		l.metaLoaded.Add(uint64(loaded))
		l.metaBatches.Add(1)
		l.lastMetaBatchCells.Store(uint64(loaded))
		l.lastMetaBatchNanos.Store(time.Since(started).Nanoseconds())
	}
	return records, nil
}

func (l *largeBOCStateLoader) LoadPayload(hashes []cell.Hash, dst []cell.LargeBOCPayloadRecord) ([]cell.LargeBOCPayloadRecord, error) {
	select {
	case <-l.ctx.Done():
		return dst, l.ctx.Err()
	default:
	}

	if l.phase.CompareAndSwap(largeBOCStatePhaseMeta, largeBOCStatePhasePayload) {
		l.dataPhaseStarted.Store(time.Now().UnixNano())
	}

	base := len(dst)
	var records []cell.LargeBOCPayloadRecord
	var err error
	started := time.Now()
	if l.generation != 0 {
		records, err = l.store.LargeBOCLoadPayloadInGeneration(l.ctx, l.generation, hashes, dst)
	} else {
		records, err = l.store.LargeBOCLoadPayload(l.ctx, hashes, dst)
	}
	if err != nil {
		return dst, err
	}
	if loaded := len(records) - base; loaded > 0 {
		l.cellLoaded.Add(uint64(loaded))
		l.dataBatches.Add(1)
		l.lastDataBatchCells.Store(uint64(loaded))
		l.lastDataBatchNanos.Store(time.Since(started).Nanoseconds())
	}
	return records, nil
}

func (l *largeBOCStateLoader) LoadCells(hashes []cell.Hash, dst []cell.LargeBOCRecord) ([]cell.LargeBOCRecord, error) {
	select {
	case <-l.ctx.Done():
		return dst, l.ctx.Err()
	default:
	}

	if l.phase.CompareAndSwap(largeBOCStatePhaseMeta, largeBOCStatePhaseCells) {
		l.dataPhaseStarted.Store(time.Now().UnixNano())
	}

	base := len(dst)
	var records []cell.LargeBOCRecord
	var err error
	started := time.Now()
	if l.generation != 0 {
		records, err = l.store.LargeBOCLoadCellsInGeneration(l.ctx, l.generation, hashes, dst)
	} else {
		records, err = l.store.LargeBOCLoadCells(l.ctx, hashes, dst)
	}
	if err != nil {
		return dst, err
	}
	if loaded := len(records) - base; loaded > 0 {
		l.metaLoaded.Add(uint64(loaded))
		l.cellLoaded.Add(uint64(loaded))
		l.dataBatches.Add(1)
		l.lastDataBatchCells.Store(uint64(loaded))
		l.lastDataBatchNanos.Store(time.Since(started).Nanoseconds())
	}
	return records, nil
}

func startStateSerializationProgress(ctx context.Context, log zerolog.Logger, block ton.BlockIDExt, kind string, loader *largeBOCStateLoader) func() {
	done := make(chan struct{})
	started := time.Now()

	go func() {
		ticker := time.NewTicker(stateSerializationProgressInterval)
		defer ticker.Stop()

		lastPhase := loader.phase.Load()
		phaseStarted := started
		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case now := <-ticker.C:
				phase := loader.phase.Load()
				metaLoaded := loader.metaLoaded.Load()
				cellLoaded := loader.cellLoaded.Load()
				if phase != lastPhase {
					lastPhase = phase
					phaseStarted = now
					if phase == largeBOCStatePhasePayload || phase == largeBOCStatePhaseCells {
						if started := loader.dataPhaseStarted.Load(); started > 0 {
							phaseStarted = time.Unix(0, started)
						}
					}
				}

				current := metaLoaded
				phaseName := "load_meta"
				loadBatches := loader.metaBatches.Load()
				lastLoadBatchCells := loader.lastMetaBatchCells.Load()
				lastLoadBatchElapsed := time.Duration(loader.lastMetaBatchNanos.Load())
				switch phase {
				case largeBOCStatePhasePayload:
					current = cellLoaded
					phaseName = "load_payload"
					loadBatches = loader.dataBatches.Load()
					lastLoadBatchCells = loader.lastDataBatchCells.Load()
					lastLoadBatchElapsed = time.Duration(loader.lastDataBatchNanos.Load())
				case largeBOCStatePhaseCells:
					current = cellLoaded
					phaseName = "load_cells"
					loadBatches = loader.dataBatches.Load()
					lastLoadBatchCells = loader.lastDataBatchCells.Load()
					lastLoadBatchElapsed = time.Duration(loader.lastDataBatchNanos.Load())
				}

				elapsed := now.Sub(started)
				phaseElapsed := now.Sub(phaseStarted)
				eta := "unknown"
				event := log.Info().
					Str("block", storage.FormatBlockRef(block)).
					Str("kind", kind).
					Str("phase", phaseName).
					Uint64("loaded_cells", current).
					Uint64("metadata_cells", metaLoaded).
					Uint64("payload_cells", cellLoaded).
					Uint64("load_batches", loadBatches).
					Uint64("last_load_batch_cells", lastLoadBatchCells).
					Str("last_load_batch_elapsed", formatStateSerializationElapsed(lastLoadBatchElapsed)).
					Str("last_load_batch_speed", logutil.FormatCellRate(lastLoadBatchCells, lastLoadBatchElapsed)).
					Str("avg_speed", logutil.FormatCellRate(current, now.Sub(phaseStarted))).
					Str("elapsed", formatStateSerializationElapsed(elapsed)).
					Str("phase_elapsed", formatStateSerializationElapsed(phaseElapsed))

				if phase == largeBOCStatePhasePayload {
					total := metaLoaded
					if total > 0 {
						event = event.
							Uint64("total_cells", total).
							Str("progress", formatCatchUpProgressPercent(float64(cellLoaded)*100/float64(total)))

						eta = formatStateSerializationETA(cellLoaded, total, phaseElapsed)
					}
				}
				event = event.Str("eta", eta)
				event.Msg("persistent state serialization progress")
			}
		}
	}()

	return func() {
		close(done)
	}
}

type contextWriter struct {
	ctx context.Context
	w   io.Writer
}

func (w contextWriter) Write(p []byte) (int, error) {
	select {
	case <-w.ctx.Done():
		return 0, w.ctx.Err()
	default:
	}
	return w.w.Write(p)
}

func syncDir(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	// Windows does not support flushing directory handles. Opening the
	// directory above still validates that it exists and is accessible.
	if runtime.GOOS == "windows" {
		return nil
	}

	if err = file.Sync(); err != nil && !errors.Is(err, syscall.EINVAL) {
		return err
	}
	return nil
}
