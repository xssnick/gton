package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type statusBlockMetrics struct {
	Utime           int64
	Transactions    uint32
	HasTransactions bool
}

func (s *Service) blockStatusMetrics(ctx context.Context, state *storage.BlockState) statusBlockMetrics {
	// Status collection is best-effort: a live-cache miss or malformed cached
	// block must not make the whole status snapshot unavailable.
	metrics, _, _ := s.liveBlockStatusMetrics(ctx, state.Block)
	if state.Parsed != nil && state.Parsed.GenUTime != 0 {
		metrics.Utime = int64(state.Parsed.GenUTime)
	} else if metrics.Utime == 0 {
		metrics.Utime = blockUtimeFromMeta(ctx, s.storage, &state.Block)
	}
	return metrics
}

func (s *Service) liveBlockStatusMetrics(ctx context.Context, block ton.BlockIDExt) (statusBlockMetrics, *storage.BlockMeta, error) {
	meta, err := s.liveBlockMeta(ctx, block)
	if err != nil {
		return statusBlockMetrics{}, nil, err
	}

	metrics := statusBlockMetrics{
		Utime: int64(meta.GenUTime),
	}
	data, err := s.liveBlockData(ctx, block)
	if err == nil {
		txCount, err := storage.BlockTransactionCountFromBlockData(block, data)
		if err == nil {
			metrics.Transactions = txCount
			metrics.HasTransactions = true
		}
	}
	return metrics, meta, nil
}

func (s *Service) liveBlockMeta(ctx context.Context, block ton.BlockIDExt) (*storage.BlockMeta, error) {
	cache := s.liveBlockCache
	if cache == nil {
		return nil, storage.ErrNotFound
	}

	full, err := cache.BlockFull(ctx, block)
	if err == nil && full.Meta != nil {
		meta := full.Meta.Clone()
		meta.ID = block
		return meta, nil
	}
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return nil, err
	}

	data, err := cache.BlockData(ctx, block)
	if err != nil {
		return nil, err
	}
	meta, err := storage.BuildBlockMetaFromBlockData(block, data)
	if err != nil {
		return nil, err
	}
	return meta, nil
}

func (s *Service) liveBlockData(ctx context.Context, block ton.BlockIDExt) ([]byte, error) {
	cache := s.liveBlockCache
	if cache == nil {
		return nil, storage.ErrNotFound
	}

	return cache.BlockData(ctx, block)
}

func (s *Service) masterShardBlocks(ctx context.Context, master ton.BlockIDExt) ([]ton.BlockIDExt, error) {
	data, err := s.storage.BlockData(ctx, master)
	if err != nil {
		return nil, err
	}

	root, err := cell.FromBOC(data)
	if err != nil {
		return nil, fmt.Errorf("parse master block boc: %w", err)
	}
	parsed, err := storage.ParseVerifiedBlockCell(master, root)
	if err != nil {
		return nil, err
	}
	if parsed.Extra == nil || parsed.Extra.Custom == nil || parsed.Extra.Custom.ShardHashes == nil {
		return nil, fmt.Errorf("master block %s has no shard hashes", storage.FormatBlockRef(master))
	}

	shards, err := ton.LoadShardsFromHashes(parsed.Extra.Custom.ShardHashes, false)
	if err != nil {
		return nil, err
	}

	blocks := make([]ton.BlockIDExt, 0, len(shards))
	for _, shard := range shards {
		if shard != nil {
			blocks = append(blocks, *shard)
		}
	}
	return blocks, nil
}
