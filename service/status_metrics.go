package service

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type statusBlockMetrics struct {
	Utime           int64
	Transactions    uint32
	HasTransactions bool
}

// Snapshot combines tracked chain progress with a point-in-time network snapshot.
func (t *StatusTracker) Snapshot(ctx context.Context, network p2p.StatusSnapshot) StatusSnapshot {
	snapshot := StatusSnapshot{
		StatusSnapshot: network,
		RecentTPS:      t.recentTPS.statusSnapshot(),
	}

	current, err := t.currentStateSnapshot(ctx)
	if err == nil {
		snapshot.LocalStateLoaded = true
		master := current.Masterchain.Block
		masterMetrics := t.blockStatusMetrics(ctx, &current.Masterchain)
		snapshot.LocalMasterchain = &master
		snapshot.LocalMasterchainUtime = masterMetrics.Utime
		snapshot.LocalMasterchainTx = masterMetrics.Transactions
		snapshot.LocalMasterchainHasTx = masterMetrics.HasTransactions
		for _, shard := range current.Shards {
			if shard.Block.Workchain != 0 {
				continue
			}

			block := shard.Block
			metrics := t.blockStatusMetrics(ctx, &shard)
			snapshot.LocalBasechainShards = append(snapshot.LocalBasechainShards, ShardStatusSnapshot{
				Block:           block,
				Utime:           metrics.Utime,
				Transactions:    metrics.Transactions,
				HasTransactions: metrics.HasTransactions,
			})
			if block.Shard == topShard {
				snapshot.LocalBasechain = &block
				snapshot.LocalBasechainUtime = metrics.Utime
				snapshot.LocalBasechainTx = metrics.Transactions
				snapshot.LocalBasechainHasTx = metrics.HasTransactions
			}
		}
		t.populateLatestBasechain(ctx, &snapshot, current)
		sort.Slice(snapshot.LocalBasechainShards, func(i, j int) bool {
			left := snapshot.LocalBasechainShards[i].Block
			right := snapshot.LocalBasechainShards[j].Block
			if left.Workchain != right.Workchain {
				return left.Workchain < right.Workchain
			}

			return uint64(left.Shard) < uint64(right.Shard)
		})
	} else if !errors.Is(err, storage.ErrNotFound) {
		snapshot.LocalStateError = err.Error()
	}

	appliedMaster := t.appliedMasterchainSnapshot()
	if current != nil && (appliedMaster == nil || appliedMaster.Block.SeqNo < current.Masterchain.Block.SeqNo) {
		appliedMaster = storage.CloneBlockState(&current.Masterchain)
	}
	if appliedMaster != nil {
		master := appliedMaster.Block
		metrics := t.blockStatusMetrics(ctx, appliedMaster)
		snapshot.AppliedMasterchain = &master
		snapshot.AppliedMasterchainUtime = metrics.Utime
	}

	return snapshot
}

func (t *StatusTracker) blockStatusMetrics(ctx context.Context, state *storage.BlockState) statusBlockMetrics {
	// Status collection is best-effort: a live-cache miss or malformed cached
	// block must not make the whole status snapshot unavailable.
	metrics, _, _ := t.liveBlockStatusMetrics(ctx, state.Block)
	if state.Parsed != nil && state.Parsed.GenUTime != 0 {
		metrics.Utime = int64(state.Parsed.GenUTime)
	} else if metrics.Utime == 0 {
		metrics.Utime = blockUtimeFromMeta(ctx, t.storage, &state.Block)
	}

	return metrics
}

func (t *StatusTracker) liveBlockStatusMetrics(ctx context.Context, block ton.BlockIDExt) (statusBlockMetrics, *storage.BlockMeta, error) {
	meta, err := t.liveBlockMeta(ctx, block)
	if err != nil {
		return statusBlockMetrics{}, nil, err
	}

	metrics := statusBlockMetrics{
		Utime: int64(meta.GenUTime),
	}
	data, err := t.liveBlockData(ctx, block)
	if err == nil {
		txCount, err := storage.BlockTransactionCountFromBlockData(block, data)
		if err == nil {
			metrics.Transactions = txCount
			metrics.HasTransactions = true
		}
	}

	return metrics, meta, nil
}

func (t *StatusTracker) liveBlockMeta(ctx context.Context, block ton.BlockIDExt) (*storage.BlockMeta, error) {
	cache := t.liveBlockCache
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

func (t *StatusTracker) liveBlockData(ctx context.Context, block ton.BlockIDExt) ([]byte, error) {
	cache := t.liveBlockCache
	if cache == nil {
		return nil, storage.ErrNotFound
	}

	return cache.BlockData(ctx, block)
}

func (t *StatusTracker) masterShardBlocks(ctx context.Context, master ton.BlockIDExt) ([]ton.BlockIDExt, error) {
	data, err := t.storage.BlockData(ctx, master)
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

func (t *StatusTracker) populateLatestBasechain(ctx context.Context, snapshot *StatusSnapshot, current *storage.CurrentState) {
	latestMaster := current.Masterchain.Block
	if snapshot.LatestMasterchain != nil && snapshot.LatestMasterchain.SeqNo > latestMaster.SeqNo {
		latestMaster = *snapshot.LatestMasterchain
	}

	shards, err := t.masterShardBlocks(ctx, latestMaster)
	if err == nil {
		setStatusLatestBasechain(snapshot, shards)
		return
	}
	if !latestMaster.Equals(&current.Masterchain.Block) {
		t.log.Debug().
			Err(err).
			Str("latest_masterchain", storage.FormatBlockRef(latestMaster)).
			Str("current_masterchain", storage.FormatBlockRef(current.Masterchain.Block)).
			Msg("skip latest basechain status because latest shard blocks are unavailable")
		return
	}

	currentShards := make([]ton.BlockIDExt, 0, len(current.Shards))
	for _, shard := range current.Shards {
		currentShards = append(currentShards, shard.Block)
	}
	setStatusLatestBasechain(snapshot, currentShards)
}

func setStatusLatestBasechain(snapshot *StatusSnapshot, shards []ton.BlockIDExt) {
	latestByShard := make(map[storage.ShardKey]ton.BlockIDExt, len(snapshot.LatestBasechainShards)+len(shards))
	for _, shard := range snapshot.LatestBasechainShards {
		if shard.Workchain == 0 {
			latestByShard[storage.ShardKeyFromBlock(shard)] = shard
		}
	}
	if len(latestByShard) == 0 && snapshot.LatestBasechain != nil && snapshot.LatestBasechain.Workchain == 0 {
		latestByShard[storage.ShardKeyFromBlock(*snapshot.LatestBasechain)] = *snapshot.LatestBasechain
	}
	for _, shard := range shards {
		if shard.Workchain != 0 {
			continue
		}

		key := storage.ShardKeyFromBlock(shard)
		if current, ok := latestByShard[key]; !ok || shard.SeqNo > current.SeqNo {
			latestByShard[key] = shard
		}
	}
	if len(latestByShard) == 0 {
		return
	}

	baseShards := make([]ton.BlockIDExt, 0, len(latestByShard))
	var latest ton.BlockIDExt
	hasLatest := false
	for _, shard := range latestByShard {
		baseShards = append(baseShards, shard)
		if !hasLatest || shard.SeqNo > latest.SeqNo {
			latest = shard
			hasLatest = true
		}
	}

	sort.Slice(baseShards, func(i, j int) bool {
		left := storage.ShardKeyFromBlock(baseShards[i])
		right := storage.ShardKeyFromBlock(baseShards[j])
		if left.Workchain != right.Workchain {
			return left.Workchain < right.Workchain
		}

		return uint64(left.Shard) < uint64(right.Shard)
	})
	snapshot.LatestBasechain = &latest
	snapshot.LatestBasechainShards = baseShards
}
