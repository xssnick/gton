package service

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"time"

	state2 "github.com/xssnick/gton/service/state"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

const (
	stateSerializationIdlePollDelay = 10 * time.Second
	stateSerializationRetryDelay    = 16 * time.Second
	stateSerializationMaxAttempts   = 128
	stateSerializationMaxJitter     = 6 * time.Hour
)

var errStateSerializationDelayed = errors.New("persistent state serialization is delayed")

type stateSerializationDelayError struct {
	delay time.Duration
}

func (e stateSerializationDelayError) Error() string {
	return fmt.Sprintf("%v: %s", errStateSerializationDelayed, e.delay)
}

func (e stateSerializationDelayError) Unwrap() error {
	return errStateSerializationDelayed
}

type persistentStateSchedulerStore interface {
	PersistentStateSerializerState(ctx context.Context) (*storage.PersistentStateSerializerState, error)
	SavePersistentStateSerializerState(ctx context.Context, state *storage.PersistentStateSerializerState) error
	ActivePersistentStateSerialization(ctx context.Context) (*storage.PersistentStateSerializerActive, error)
	SaveActivePersistentStateSerialization(ctx context.Context, active *storage.PersistentStateSerializerActive) error
	DeleteActivePersistentStateSerialization(ctx context.Context) error
	SavePersistentStateDescription(ctx context.Context, desc *storage.PersistentStateDescription) error
}

type masterBlockMetaForStateSerialization struct {
	block ton.BlockIDExt
	meta  *storage.BlockMeta
}

func (s *Service) processPersistentStateSerialization(ctx context.Context) error {
	scheduler, err := s.stateSerializer.schedulerStore()
	if err != nil {
		return err
	}
	if err = s.waitCurrentStatePersist(ctx); err != nil {
		return err
	}

	current, err := s.storage.CurrentState(ctx)
	if errors.Is(err, storage.ErrNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("load durable current state: %w", err)
	}
	if current.Masterchain.Block.Workchain != -1 || current.Masterchain.Block.Shard != topShard {
		return fmt.Errorf("durable current state masterchain block is invalid: %s", storage.FormatBlockRef(current.Masterchain.Block))
	}

	cursor, err := scheduler.PersistentStateSerializerState(ctx)
	if errors.Is(err, storage.ErrNotFound) {
		return s.initPersistentStateSerializer(ctx, scheduler, current.Masterchain.Block)
	}
	if err != nil {
		return fmt.Errorf("load persistent state serializer cursor: %w", err)
	}

	active, err := scheduler.ActivePersistentStateSerialization(ctx)
	if errors.Is(err, storage.ErrNotFound) {
		active = nil
	} else if err != nil {
		return fmt.Errorf("load active persistent state serialization: %w", err)
	}
	retryInterrupted := active != nil && active.Block.SeqNo <= current.Masterchain.Block.SeqNo
	if active != nil && active.Block.SeqNo <= cursor.LastBlock.SeqNo {
		if err = clearActivePersistentStateSerialization(ctx, scheduler, active.Block); err != nil {
			return err
		}
		active = nil
		retryInterrupted = false
	}

	if cursor.LastBlock.SeqNo >= current.Masterchain.Block.SeqNo {
		return nil
	}

	blocks, latestKey, latestKeyUTime, err := s.loadStateSerializationMasterBlocks(ctx, cursor.LastBlock.SeqNo+1, current.Masterchain.Block.SeqNo)
	if err != nil {
		return err
	}

	event := s.stateSerializer.log.Debug().
		Str("from", storage.FormatBlockRef(cursor.LastBlock)).
		Str("to", storage.FormatBlockRef(current.Masterchain.Block)).
		Int("blocks", len(blocks))
	if latestKeyUTime != 0 {
		event = event.
			Str("latest_key_block", storage.FormatBlockRef(latestKey)).
			Uint32("latest_key_utime", latestKeyUTime)
	}
	event.Msg("checking durable masterchain blocks for persistent state serialization")

	for _, item := range blocks {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if !item.meta.Has(storage.BlockMetaIsKeyBlock) {
			if activePersistentStateSerializationMatches(active, item.block) {
				if err = clearActivePersistentStateSerialization(ctx, scheduler, item.block); err != nil {
					return err
				}
			}
			if err = savePersistentStateSerializerCursor(ctx, scheduler, cursor, item.block, nil, 0); err != nil {
				return err
			}
			continue
		}

		if err = s.processPersistentStateKeyBlock(ctx, scheduler, cursor, item.block, item.meta, latestKeyUTime, active, retryInterrupted); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) initPersistentStateSerializer(ctx context.Context, scheduler persistentStateSchedulerStore, current ton.BlockIDExt) error {
	latestKey, latestKeyUTime, err := s.latestKnownKeyBlockAtOrBefore(ctx, current)
	if err != nil {
		return fmt.Errorf("initialize persistent state serializer cursor: %w", err)
	}

	cursor := &storage.PersistentStateSerializerState{
		LastBlock:             current,
		LastWrittenBlock:      latestKey,
		LastWrittenBlockUTime: latestKeyUTime,
	}
	if err = scheduler.SavePersistentStateSerializerState(ctx, cursor); err != nil {
		return fmt.Errorf("save initial persistent state serializer cursor: %w", err)
	}

	s.stateSerializer.log.Info().
		Str("last_block", storage.FormatBlockRef(cursor.LastBlock)).
		Str("last_written", storage.FormatBlockRef(cursor.LastWrittenBlock)).
		Uint32("last_written_utime", cursor.LastWrittenBlockUTime).
		Msg("initialized persistent state serializer cursor")
	return nil
}

func (s *Service) processPersistentStateKeyBlock(
	ctx context.Context,
	scheduler persistentStateSchedulerStore,
	cursor *storage.PersistentStateSerializerState,
	block ton.BlockIDExt,
	meta *storage.BlockMeta,
	latestKeyUTime uint32,
	active *storage.PersistentStateSerializerActive,
	retryInterrupted bool,
) error {
	if !state2.IsPersistentState(meta.GenUTime, cursor.LastWrittenBlockUTime) {
		s.stateSerializer.log.Debug().
			Str("block", storage.FormatBlockRef(block)).
			Uint32("utime", meta.GenUTime).
			Uint32("last_persistent_utime", cursor.LastWrittenBlockUTime).
			Msg("key block is not a persistent state boundary")
		if activePersistentStateSerializationMatches(active, block) {
			if err := clearActivePersistentStateSerialization(ctx, scheduler, block); err != nil {
				return err
			}
		}
		return savePersistentStateSerializerCursor(ctx, scheduler, cursor, block, nil, 0)
	}

	ttl := state2.PersistentStateTTL(meta.GenUTime)
	if ttl <= uint64(time.Now().Unix()) {
		s.stateSerializer.log.Info().
			Str("block", storage.FormatBlockRef(block)).
			Uint32("utime", meta.GenUTime).
			Uint64("ttl", ttl).
			Msg("persistent state key block ttl is already expired")
		if activePersistentStateSerializationMatches(active, block) {
			if err := clearActivePersistentStateSerialization(ctx, scheduler, block); err != nil {
				return err
			}
		}
		return savePersistentStateSerializerCursor(ctx, scheduler, cursor, block, nil, 0)
	}

	err := s.tryPersistentStateKeyBlock(ctx, scheduler, cursor, block, meta, latestKeyUTime, ttl, active, retryInterrupted)
	if err == nil {
		s.stateSerializer.resetAutomaticAttempts(block.SeqNo)
		return nil
	}
	if errors.Is(err, errServiceMaintenanceRescan) {
		s.stateSerializer.resetAutomaticAttempts(block.SeqNo)
		return err
	}
	if errors.Is(err, errStateSerializationCanceled) {
		s.stateSerializer.resetAutomaticAttempts(block.SeqNo)
		return err
	}
	if errors.Is(err, errStateSerializationDelayed) {
		return err
	}
	if errors.Is(err, errStateSerializationRunning) || errors.Is(err, errCellGenerationMigrationRunning) || errors.Is(err, errPersistentStateGCActive) || errors.Is(err, errArchiveTTLGCActive) || errors.Is(err, errStateSerializationLowDiskSpace) {
		return err
	}

	attempt, giveUp := s.stateSerializer.recordAutomaticFailure(block.SeqNo)
	if !giveUp {
		return fmt.Errorf("persistent state key block %s attempt %d/%d: %w", storage.FormatBlockRef(block), attempt, stateSerializationMaxAttempts, err)
	}

	s.stateSerializer.log.Error().
		Err(err).
		Str("block", storage.FormatBlockRef(block)).
		Int("attempts", attempt).
		Msg("persistent state key block serialization reached max attempts, advancing cursor")
	s.stateSerializer.resetAutomaticAttempts(block.SeqNo)
	if activePersistentStateSerializationMatches(active, block) {
		if clearErr := clearActivePersistentStateSerialization(ctx, scheduler, block); clearErr != nil {
			return clearErr
		}
	}
	return savePersistentStateSerializerCursor(ctx, scheduler, cursor, block, nil, 0)
}

func (s *Service) tryPersistentStateKeyBlock(
	ctx context.Context,
	scheduler persistentStateSchedulerStore,
	cursor *storage.PersistentStateSerializerState,
	block ton.BlockIDExt,
	meta *storage.BlockMeta,
	latestKeyUTime uint32,
	ttl uint64,
	active *storage.PersistentStateSerializerActive,
	retryInterrupted bool,
) error {
	if s.stateSerializer.disableAutomatic {
		if err := s.storePersistentStateDescription(ctx, scheduler, block, meta.GenUTime, ttl); err != nil {
			return err
		}
		s.stateSerializer.log.Info().
			Str("block", storage.FormatBlockRef(block)).
			Msg("skipping persistent state serialization because it is disabled in config")
		if activePersistentStateSerializationMatches(active, block) {
			if err := clearActivePersistentStateSerialization(ctx, scheduler, block); err != nil {
				return err
			}
		}
		return savePersistentStateSerializerCursor(ctx, scheduler, cursor, block, &block, meta.GenUTime)
	}
	if s.haveNewerPersistentState(meta.GenUTime, latestKeyUTime) {
		if err := s.storePersistentStateDescription(ctx, scheduler, block, meta.GenUTime, ttl); err != nil {
			return err
		}
		s.stateSerializer.log.Info().
			Str("block", storage.FormatBlockRef(block)).
			Uint32("utime", meta.GenUTime).
			Uint32("latest_key_utime", latestKeyUTime).
			Msg("skipping persistent state serialization because newer persistent key block is known")
		if activePersistentStateSerializationMatches(active, block) {
			if err := clearActivePersistentStateSerialization(ctx, scheduler, block); err != nil {
				return err
			}
		}
		return savePersistentStateSerializerCursor(ctx, scheduler, cursor, block, &block, meta.GenUTime)
	}

	delay := time.Duration(0)
	if !retryInterrupted && s.stateSerializer.randomDelay != nil {
		delay = s.stateSerializer.randomDelay()
	}
	if delay > 0 {
		if err := saveActivePersistentStateSerialization(ctx, scheduler, block); err != nil {
			return err
		}
		s.stateSerializer.log.Warn().
			Str("block", storage.FormatBlockRef(block)).
			Dur("delay", delay).
			Msg("delaying persistent state serialization")
		return stateSerializationDelayError{delay: delay}
	}

	lease, err := s.beginExclusiveServiceTask(ctx, exclusiveServiceTaskStateSerialization)
	if err != nil {
		return err
	}
	if err = s.ensurePersistentStateSerializationDiskSpace(ctx, block); err != nil {
		lease.release()
		return err
	}
	if err = s.storePersistentStateDescription(ctx, scheduler, block, meta.GenUTime, ttl); err != nil {
		lease.release()
		return err
	}
	if err = saveActivePersistentStateSerialization(ctx, scheduler, block); err != nil {
		lease.release()
		return err
	}
	err = s.stateSerializer.runExclusive(ctx, block, PersistentStateSerializationAll)
	if err != nil {
		lease.release()
		return err
	}
	if err = clearActivePersistentStateSerialization(ctx, scheduler, block); err != nil {
		lease.release()
		return err
	}
	if err = savePersistentStateSerializerCursor(ctx, scheduler, cursor, block, &block, meta.GenUTime); err != nil {
		lease.release()
		return err
	}
	lease.release()
	s.afterPersistentStateSerialized(ctx, block, PersistentStateSerializationAll)
	return errServiceMaintenanceRescan
}

func activePersistentStateSerializationMatches(active *storage.PersistentStateSerializerActive, block ton.BlockIDExt) bool {
	return active != nil && active.Block.Equals(&block)
}

func saveActivePersistentStateSerialization(ctx context.Context, scheduler persistentStateSchedulerStore, block ton.BlockIDExt) error {
	active := &storage.PersistentStateSerializerActive{
		Block:         block,
		StartedAtUnix: uint64(time.Now().Unix()),
	}
	if err := scheduler.SaveActivePersistentStateSerialization(ctx, active); err != nil {
		return fmt.Errorf("save active persistent state serialization %s: %w", storage.FormatBlockRef(block), err)
	}
	return nil
}

func clearActivePersistentStateSerialization(ctx context.Context, scheduler persistentStateSchedulerStore, block ton.BlockIDExt) error {
	if err := scheduler.DeleteActivePersistentStateSerialization(ctx); err != nil {
		return fmt.Errorf("clear active persistent state serialization %s: %w", storage.FormatBlockRef(block), err)
	}
	return nil
}

func (s *Service) storePersistentStateDescription(ctx context.Context, scheduler persistentStateSchedulerStore, block ton.BlockIDExt, startTime uint32, endTime uint64) error {
	masterState, err := s.storage.BlockState(ctx, block)
	if err != nil {
		return fmt.Errorf("load masterchain state for persistent state description %s: %w", storage.FormatBlockRef(block), err)
	}

	desc, err := buildPersistentStateDescription(masterState, startTime, endTime)
	if err != nil {
		return err
	}
	if err = scheduler.SavePersistentStateDescription(ctx, desc); err != nil {
		return fmt.Errorf("save persistent state description %s: %w", storage.FormatBlockRef(block), err)
	}

	s.stateSerializer.log.Info().
		Str("masterchain", storage.FormatBlockRef(desc.MasterchainBlock)).
		Uint32("start_time", desc.StartTime).
		Uint64("end_time", desc.EndTime).
		Int("shards", len(desc.ShardBlocks)).
		Msg("persistent state description stored")
	return nil
}

func buildPersistentStateDescription(masterState *storage.BlockState, startTime uint32, endTime uint64) (*storage.PersistentStateDescription, error) {
	shards, err := state2.ShardBlocksFromMasterState(masterState)
	if err != nil {
		return nil, err
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

	desc := &storage.PersistentStateDescription{
		MasterchainBlock: masterState.Block,
		StartTime:        startTime,
		EndTime:          endTime,
		ShardBlocks:      make([]storage.PersistentStateDescriptionShard, 0, len(shards)),
	}
	splitDepths, err := state2.PersistentStateSplitDepths(masterState, shards)
	if err != nil {
		return nil, fmt.Errorf("load persistent state split depths for %s: %w", storage.FormatBlockRef(masterState.Block), err)
	}

	for _, shard := range shards {
		desc.ShardBlocks = append(desc.ShardBlocks, storage.PersistentStateDescriptionShard{
			Block:      shard,
			SplitDepth: splitDepths[shard.Workchain],
		})
	}
	return desc, nil
}

func savePersistentStateSerializerCursor(
	ctx context.Context,
	scheduler persistentStateSchedulerStore,
	cursor *storage.PersistentStateSerializerState,
	lastBlock ton.BlockIDExt,
	lastWritten *ton.BlockIDExt,
	lastWrittenUTime uint32,
) error {
	cursor.LastBlock = lastBlock
	if lastWritten != nil {
		cursor.LastWrittenBlock = *lastWritten
		cursor.LastWrittenBlockUTime = lastWrittenUTime
	}
	if err := scheduler.SavePersistentStateSerializerState(ctx, cursor); err != nil {
		return fmt.Errorf("save persistent state serializer cursor: %w", err)
	}
	return nil
}

func (s *Service) latestKnownKeyBlockAtOrBefore(ctx context.Context, block ton.BlockIDExt) (ton.BlockIDExt, uint32, error) {
	if block.Workchain != -1 || block.Shard != topShard {
		return ton.BlockIDExt{}, 0, fmt.Errorf("block is not masterchain: %s", storage.FormatBlockRef(block))
	}

	for seqno := block.SeqNo; ; seqno-- {
		select {
		case <-ctx.Done():
			return ton.BlockIDExt{}, 0, ctx.Err()
		default:
		}

		current := block
		if seqno != block.SeqNo {
			var err error
			current, err = s.storage.LookupBlockBySeqNo(ctx, storage.BlockHistoryKey{Workchain: -1, Shard: topShard}, seqno)
			if err != nil {
				return ton.BlockIDExt{}, 0, err
			}
		}

		meta, err := s.storage.BlockMeta(ctx, current)
		if err != nil {
			return ton.BlockIDExt{}, 0, err
		}
		if meta.Has(storage.BlockMetaIsKeyBlock) {
			if meta.GenUTime == 0 {
				return ton.BlockIDExt{}, 0, fmt.Errorf("key block %s has no gen utime", storage.FormatBlockRef(current))
			}
			return current, meta.GenUTime, nil
		}

		if seqno == 0 {
			break
		}
	}
	return ton.BlockIDExt{}, 0, storage.ErrNotFound
}

func (s *Service) loadStateSerializationMasterBlocks(ctx context.Context, fromSeqno uint32, toSeqno uint32) ([]masterBlockMetaForStateSerialization, ton.BlockIDExt, uint32, error) {
	blocks := make([]masterBlockMetaForStateSerialization, 0, int(toSeqno-fromSeqno)+1)
	var latestKey ton.BlockIDExt
	var latestKeyUTime uint32

	for seqno := fromSeqno; seqno <= toSeqno; seqno++ {
		select {
		case <-ctx.Done():
			return nil, ton.BlockIDExt{}, 0, ctx.Err()
		default:
		}

		block, err := s.storage.LookupBlockBySeqNo(ctx, storage.BlockHistoryKey{Workchain: -1, Shard: topShard}, seqno)
		if err != nil {
			return nil, ton.BlockIDExt{}, 0, fmt.Errorf("lookup masterchain block #%d: %w", seqno, err)
		}
		meta, err := s.storage.BlockMeta(ctx, block)
		if err != nil {
			return nil, ton.BlockIDExt{}, 0, fmt.Errorf("load masterchain block meta %s: %w", storage.FormatBlockRef(block), err)
		}
		if meta.Has(storage.BlockMetaIsKeyBlock) {
			if meta.GenUTime == 0 {
				return nil, ton.BlockIDExt{}, 0, fmt.Errorf("key block %s has no gen utime", storage.FormatBlockRef(block))
			}
			latestKey = block
			latestKeyUTime = meta.GenUTime
		}

		blocks = append(blocks, masterBlockMetaForStateSerialization{
			block: block,
			meta:  meta,
		})
	}
	return blocks, latestKey, latestKeyUTime, nil
}

func (s *Service) haveNewerPersistentState(currentUTime uint32, latestKeyUTime uint32) bool {
	return currentUTime/(1<<17) < latestKeyUTime/(1<<17)
}

func (s *stateSerializer) schedulerStore() (persistentStateSchedulerStore, error) {
	store, ok := s.store.(persistentStateSchedulerStore)
	if !ok {
		return nil, fmt.Errorf("storage %T does not support persistent state serializer metadata", s.store)
	}
	return store, nil
}

func (s *stateSerializer) recordAutomaticFailure(seqno uint32) (int, bool) {
	s.attemptMu.Lock()
	defer s.attemptMu.Unlock()

	attempt := s.automaticAttempts[seqno] + 1
	s.automaticAttempts[seqno] = attempt
	return attempt, attempt >= stateSerializationMaxAttempts
}

func (s *stateSerializer) resetAutomaticAttempts(seqno uint32) {
	s.attemptMu.Lock()
	delete(s.automaticAttempts, seqno)
	s.attemptMu.Unlock()
}

func randomStateSerializationDelay() time.Duration {
	seconds := int64(stateSerializationMaxJitter / time.Second)
	if seconds <= 0 {
		return 0
	}

	value, err := rand.Int(rand.Reader, big.NewInt(seconds+1))
	if err != nil {
		return time.Duration(time.Now().UnixNano()%int64(stateSerializationMaxJitter)) * time.Nanosecond
	}
	return time.Duration(value.Int64()) * time.Second
}
