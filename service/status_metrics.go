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
	if state == nil {
		return statusBlockMetrics{}
	}

	metrics, _, err := s.storedBlockStatusMetrics(ctx, state.Block)
	if state.Parsed != nil && state.Parsed.GenUTime != 0 {
		metrics.Utime = int64(state.Parsed.GenUTime)
	}
	if err == nil {
		return metrics
	}

	return metrics
}

func (s *Service) recentTPSSnapshot(ctx context.Context, current *storage.CurrentState, window int) StatusTPSSnapshot {
	if current == nil || s.storage == nil || window <= 0 {
		return StatusTPSSnapshot{}
	}

	latest := current.Masterchain.Block
	if latest.Workchain != -1 {
		return StatusTPSSnapshot{}
	}

	firstSeqno := uint32(0)
	if latest.SeqNo >= uint32(window-1) {
		firstSeqno = latest.SeqNo - uint32(window-1)
	}

	complete := true
	masters := make([]ton.BlockIDExt, 0, window)
	for seqno := firstSeqno; ; seqno++ {
		master, err := s.storage.LookupBlockBySeqNo(ctx, storage.BlockHistoryKey{Workchain: -1, Shard: topShard}, seqno)
		if err != nil {
			complete = false
		} else {
			masters = append(masters, master)
		}
		if seqno == latest.SeqNo {
			break
		}
	}

	snapshot := StatusTPSSnapshot{WindowMasters: len(masters)}
	if len(masters) < 2 {
		return snapshot
	}

	firstMetrics, _, err := s.storedBlockStatusMetrics(ctx, masters[0])
	if err != nil {
		return snapshot
	}
	lastMetrics, _, err := s.storedBlockStatusMetrics(ctx, masters[len(masters)-1])
	if err != nil || firstMetrics.Utime == 0 || lastMetrics.Utime <= firstMetrics.Utime {
		return snapshot
	}

	shardTops := map[string]ton.BlockIDExt{}
	for _, master := range masters {
		metrics, _, err := s.storedBlockStatusMetrics(ctx, master)
		if err != nil || !metrics.HasTransactions {
			complete = false
		} else {
			snapshot.Transactions += uint64(metrics.Transactions)
		}

		shards, err := s.masterShardBlocks(ctx, master)
		if err != nil {
			complete = false
			continue
		}
		for _, shard := range shards {
			if shard.Workchain != 0 {
				continue
			}
			shardTops[statusBlockKey(shard)] = shard
		}
	}

	seenShards := map[string]struct{}{}
	for _, shard := range shardTops {
		s.collectShardTPS(ctx, shard, firstMetrics.Utime, lastMetrics.Utime, seenShards, &snapshot.Transactions, &complete)
	}

	snapshot.DurationSeconds = lastMetrics.Utime - firstMetrics.Utime
	snapshot.TPS = float64(snapshot.Transactions) / float64(snapshot.DurationSeconds)
	snapshot.Complete = complete
	return snapshot
}

func (s *Service) collectShardTPS(ctx context.Context, start ton.BlockIDExt, firstUtime int64, lastUtime int64, seen map[string]struct{}, total *uint64, complete *bool) {
	stack := []ton.BlockIDExt{start}
	for len(stack) > 0 {
		block := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if block.Workchain != 0 {
			continue
		}

		key := statusBlockKey(block)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}

		metrics, meta, err := s.storedBlockStatusMetrics(ctx, block)
		if err != nil || metrics.Utime == 0 {
			*complete = false
			continue
		}
		if metrics.Utime >= firstUtime && metrics.Utime <= lastUtime {
			if metrics.HasTransactions {
				*total += uint64(metrics.Transactions)
			} else {
				*complete = false
			}
		}
		if metrics.Utime < firstUtime {
			continue
		}

		for _, prev := range meta.PrevRefs {
			if prev.Workchain == 0 {
				stack = append(stack, prev)
			}
		}
	}
}

func (s *Service) storedBlockStatusMetrics(ctx context.Context, block ton.BlockIDExt) (statusBlockMetrics, *storage.BlockMeta, error) {
	meta, err := s.storedBlockMeta(ctx, block)
	if err != nil {
		return statusBlockMetrics{}, nil, err
	}

	metrics := statusBlockMetrics{
		Utime: int64(meta.GenUTime),
	}
	txCount, err := s.blockTransactionCountFromData(ctx, block)
	if err == nil {
		metrics.Transactions = txCount
		metrics.HasTransactions = true
	}
	return metrics, meta, nil
}

func (s *Service) storedBlockMeta(ctx context.Context, block ton.BlockIDExt) (*storage.BlockMeta, error) {
	if s.storage == nil {
		return nil, storage.ErrNotFound
	}

	meta, err := s.storage.BlockMeta(ctx, block)
	if err == nil && meta != nil {
		return meta, nil
	}
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return nil, err
	}

	data, err := s.storage.BlockData(ctx, block)
	if err != nil {
		return nil, err
	}
	meta, err = storage.BuildBlockMetaFromBlockData(block, data)
	if err != nil {
		return nil, err
	}
	return meta, nil
}

func (s *Service) blockTransactionCountFromData(ctx context.Context, block ton.BlockIDExt) (uint32, error) {
	if s.storage == nil {
		return 0, storage.ErrNotFound
	}

	data, err := s.storage.BlockData(ctx, block)
	if err != nil {
		return 0, err
	}

	txCount, err := storage.BlockTransactionCountFromBlockData(block, data)
	if err != nil {
		return 0, err
	}
	return txCount, nil
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

func statusBlockKey(block ton.BlockIDExt) string {
	return fmt.Sprintf("%d:%016x:%d:%x:%x", block.Workchain, uint64(block.Shard), block.SeqNo, block.RootHash, block.FileHash)
}
