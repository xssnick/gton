package state

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

func (s *Syncer) SyncZeroStateCurrent(ctx context.Context) (*storage.CurrentState, error) {
	zeroBlock, err := s.source.ZeroStateBlock(ctx)
	if err != nil {
		return nil, err
	}
	if err = validateTrustedKeyBlockAnchor(zeroBlock); err != nil {
		return nil, err
	}
	if zeroBlock.SeqNo != 0 {
		return nil, fmt.Errorf("configured zero state is not zerostate: %s", storage.FormatBlockRef(zeroBlock))
	}

	s.log.Info().
		Str("masterchain", storage.FormatBlockRef(zeroBlock)).
		Msg("importing masterchain zero state")
	masterState, err := s.importZeroState(ctx, zeroBlock, zeroBlock)
	if err != nil {
		return nil, fmt.Errorf("import masterchain zero state %s: %w", storage.FormatBlockRef(zeroBlock), err)
	}

	shardBlocks, err := ShardBlocksFromMasterState(masterState)
	if err != nil {
		return nil, fmt.Errorf("load shard zero states from masterchain zero state %s: %w", storage.FormatBlockRef(zeroBlock), err)
	}

	current := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: 0,
		Masterchain:      storage.BlockStateWithoutCells(masterState),
		Shards:           map[storage.ShardKey]storage.BlockState{},
	}
	states := []*storage.BlockState{masterState}

	for _, shardBlock := range shardBlocks {
		if shardBlock.SeqNo != 0 {
			return nil, fmt.Errorf("masterchain zero state references non-zero shard state %s", storage.FormatBlockRef(shardBlock))
		}

		s.log.Info().
			Str("shard", storage.FormatBlockRef(shardBlock)).
			Str("masterchain", storage.FormatBlockRef(zeroBlock)).
			Msg("importing shard zero state")
		shardState, err := s.importZeroState(ctx, shardBlock, zeroBlock)
		if err != nil {
			return nil, fmt.Errorf("import shard zero state %s: %w", storage.FormatBlockRef(shardBlock), err)
		}

		current.Shards[storage.ShardKeyFromBlock(shardBlock)] = storage.BlockStateWithoutCells(shardState)
		states = append(states, shardState)
	}

	if err = s.storage.SaveStateCheckpoint(ctx, states, current); err != nil {
		return nil, fmt.Errorf("persist zero state current: %w", err)
	}
	if err = s.storage.ClearStateSyncProgress(ctx); err != nil {
		return nil, fmt.Errorf("clear pending state sync progress: %w", err)
	}

	s.log.Info().
		Str("masterchain", storage.FormatBlockRef(zeroBlock)).
		Int("shards", len(current.Shards)).
		Msg("zero state current persisted")
	return current, nil
}

func (s *Syncer) ImportZeroState(ctx context.Context, block ton.BlockIDExt, master ton.BlockIDExt) (*storage.BlockState, error) {
	return s.importZeroState(ctx, block, master)
}

func (s *Syncer) importZeroState(ctx context.Context, block ton.BlockIDExt, master ton.BlockIDExt) (*storage.BlockState, error) {
	if block.SeqNo != 0 {
		return nil, fmt.Errorf("state is not zerostate: %s", storage.FormatBlockRef(block))
	}
	if len(block.RootHash) != 32 || len(block.FileHash) != 32 {
		return nil, fmt.Errorf("zerostate has invalid hashes: %s root_hash_len=%d file_hash_len=%d", storage.FormatBlockRef(block), len(block.RootHash), len(block.FileHash))
	}

	state, err := s.storage.BlockState(ctx, block)
	if err == nil {
		setShardMasterchainRef(state, master)
		s.log.Info().
			Str("block", storage.FormatBlockRef(block)).
			Msg("using stored zero state")
		return state, nil
	}
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return nil, fmt.Errorf("load zero state %s: %w", storage.FormatBlockRef(block), err)
	}

	downloaded, err := s.source.ZeroState(ctx, block)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cleanupErr := downloaded.Cleanup(); cleanupErr != nil {
			s.log.Warn().
				Err(cleanupErr).
				Str("block", storage.FormatBlockRef(block)).
				Msg("failed to cleanup temporary zero state")
		}
	}()

	downloadedBlock := downloaded.Block()
	if !downloadedBlock.Equals(&block) {
		return nil, fmt.Errorf("zero state artifact is for %s, expected %s", storage.FormatBlockRef(downloadedBlock), storage.FormatBlockRef(block))
	}

	state, err = s.importer.ImportAndPersist(ctx, downloaded, s.storage, master)
	if err != nil {
		return nil, err
	}
	return state, nil
}
