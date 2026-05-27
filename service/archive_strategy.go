package service

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"math/bits"
	"sync"

	"github.com/xssnick/gton/service/archive"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
)

const (
	shardNextBlockCatchUpMaxRemaining = 100_000
	archiveToNextLagSeconds           = 200
	nextToArchiveLagSeconds           = 600
	maxArchiveMonitorSplitDepth       = 12
)

func masterchainBlockLagSeconds(blockUTime int64, nowUnix int64) (int64, bool) {
	if blockUTime == 0 {
		return 0, false
	}
	return nowUnix - blockUTime, true
}

func shouldSwitchNextToArchiveByLag(lagSeconds int64) bool {
	return lagSeconds > nextToArchiveLagSeconds
}

func shouldSwitchArchiveToNextByLag(lagSeconds int64) bool {
	return lagSeconds < archiveToNextLagSeconds
}

func remainingLagSeconds(lagSeconds int64) int64 {
	remaining := lagSeconds - archiveToNextLagSeconds
	if remaining < 0 {
		return 0
	}
	return remaining
}

func archiveCatchUpTargetByLag(current *storage.CurrentState, lagSeconds int64) (ton.BlockIDExt, bool) {
	if !shouldSwitchNextToArchiveByLag(lagSeconds) {
		return ton.BlockIDExt{}, false
	}

	if current.Masterchain.Block.SeqNo == ^uint32(0) {
		return ton.BlockIDExt{}, false
	}

	target := current.Masterchain.Block
	target.SeqNo = ^uint32(0)
	return target, true
}

func (r *archiveCatchUpRunner) downloadAndImportShardArchives(ctx context.Context, queue *archiveImportQueue, masterchainSeqno uint32, shards []archive.ShardID, splitDepth uint32, priority archiveImportPriority) ([]*archiveImportResult, error) {
	if len(shards) == 0 {
		return nil, nil
	}

	preloadCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	type archivePreloadResult struct {
		idx      int
		imported *archiveImportResult
		err      error
	}

	type archivePreloadJob struct {
		idx  int
		done <-chan archiveImportQueueResult
	}

	limit := archiveShardArchiveImportInFlight
	if limit < 1 {
		limit = 1
	}
	if limit > len(shards) {
		limit = len(shards)
	}

	results := make(chan archivePreloadResult, limit)
	var wg sync.WaitGroup
	submitted := 0
	inFlight := 0
	submit := func(idx int) error {
		shard := shards[idx]
		done, err := queue.submitArchive(preloadCtx, masterchainSeqno, shard, splitDepth, priority)
		if err != nil {
			return fmt.Errorf("preload shard archive #%d %s: %w", masterchainSeqno, shard.String(), err)
		}

		job := archivePreloadJob{idx: idx, done: done}
		wg.Add(1)
		go func() {
			defer wg.Done()
			var res archivePreloadResult
			select {
			case result := <-job.done:
				res = archivePreloadResult{idx: job.idx, imported: result.imported, err: result.err}
			case <-preloadCtx.Done():
				res = archivePreloadResult{idx: job.idx, err: preloadCtx.Err()}
			}
			results <- res
		}()
		submitted++
		inFlight++
		return nil
	}

	imports := make([]*archiveImportResult, len(shards))
	var firstErr error
	completed := 0
	for submitted < len(shards) && inFlight < limit {
		if err := submit(submitted); err != nil {
			cancel()
			return nil, err
		}
	}

	for completed < submitted {
		res := <-results
		completed++
		inFlight--

		if res.err != nil {
			cancel()
			if firstErr == nil || errors.Is(firstErr, context.Canceled) {
				firstErr = fmt.Errorf("preload shard archive #%d %s: %w", masterchainSeqno, shards[res.idx].String(), res.err)
			}
		} else {
			imports[res.idx] = res.imported
		}

		for firstErr == nil && submitted < len(shards) && inFlight < limit {
			if err := submit(submitted); err != nil {
				cancel()
				firstErr = err
				break
			}
		}
	}
	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}
	if completed != len(shards) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("preload shard archive #%d: incomplete import results got=%d want=%d", masterchainSeqno, completed, len(shards))
	}
	return imports, nil
}

func mergeImportStats(total, next *archive.ImportStats, includeSeqRange bool) {
	if total.ArchiveID == 0 && next.ArchiveID != 0 {
		total.ArchiveID = next.ArchiveID
	}
	if total.Peer == "" || total.Peer == "local-master-catchup" && next.Peer != "" {
		total.Peer = next.Peer
	}

	total.Bytes += next.Bytes
	total.Entries += next.Entries
	total.IgnoredEntries += next.IgnoredEntries
	total.Blocks += next.Blocks
	total.Proofs += next.Proofs
	total.ProofLinks += next.ProofLinks
	total.FullBlocks += next.FullBlocks
	total.Links += next.Links
	total.DownloadElapsed += next.DownloadElapsed
	total.ImportElapsed += next.ImportElapsed
	total.ProcessingElapsed += next.ProcessingElapsed
	total.BlockPrepareElapsed += next.BlockPrepareElapsed
	total.StateUpdateCells += next.StateUpdateCells
	total.StateUpdateCellBytes += next.StateUpdateCellBytes
	total.StateUpdateCellPrepare += next.StateUpdateCellPrepare

	if includeSeqRange {
		if total.FirstSeqno == 0 || next.FirstSeqno != 0 && next.FirstSeqno < total.FirstSeqno {
			total.FirstSeqno = next.FirstSeqno
		}
		if next.LastSeqno > total.LastSeqno {
			total.LastSeqno = next.LastSeqno
		}
	}
}

func archivePrefixHasChangedShard(prefix archive.ShardID, nextBlocks []ton.BlockIDExt, prevBlocks map[storage.ShardKey]ton.BlockIDExt) bool {
	return archivePrefixHasChangedShardMatching(prefix, nextBlocks, prevBlocks, nil)
}

func archivePrefixHasChangedShardMatching(prefix archive.ShardID, nextBlocks []ton.BlockIDExt, prevBlocks map[storage.ShardKey]ton.BlockIDExt, needBlock func(ton.BlockIDExt) bool) bool {
	for _, next := range nextBlocks {
		if next.Workchain != prefix.Workchain || !archiveShardIntersects(prefix.Shard, next.Shard) {
			continue
		}

		prev, ok := prevBlocks[storage.ShardKeyFromBlock(next)]
		if ok && prev.Equals(&next) {
			continue
		}
		if needBlock == nil || needBlock(next) {
			return true
		}
	}
	return false
}

func archiveShardIntersects(left int64, right int64) bool {
	leftDepth := archiveShardPrefixLength(left)
	rightDepth := archiveShardPrefixLength(right)
	depth := leftDepth
	if rightDepth < depth {
		depth = rightDepth
	}
	if depth <= 0 {
		return true
	}

	mask := ^uint64(0) << (64 - depth)
	return uint64(left)&mask == uint64(right)&mask
}

func archiveShardPrefixLength(shard int64) int {
	value := uint64(shard)
	if value == 0 {
		return 0
	}
	return 63 - bits.TrailingZeros64(value)
}

func monitorMinSplitDepth(state *storage.BlockState, workchain int32) (uint32, error) {
	if state.Parsed == nil || state.Parsed.McStateExtra == nil {
		return 0, fmt.Errorf("masterchain state %s is missing mc_state_extra", storage.FormatBlockRef(state.Block))
	}

	var extra tlb.McStateExtra
	loader, err := state.Parsed.McStateExtra.BeginParse()
	if err != nil {
		return 0, fmt.Errorf("parse mc_state_extra for %s: %w", storage.FormatBlockRef(state.Block), err)
	}
	if err := tlb.LoadFromCell(&extra, loader); err != nil {
		return 0, fmt.Errorf("parse mc_state_extra for %s: %w", storage.FormatBlockRef(state.Block), err)
	}
	if extra.ConfigParams.Config.Params == nil {
		return 0, fmt.Errorf("masterchain state %s has no config params", storage.FormatBlockRef(state.Block))
	}

	cfg := tlb.BlockchainConfig{Root: extra.ConfigParams.Config.Params.AsCell()}
	workchains, err := cfg.GetWorkchains()
	if err != nil {
		return 0, err
	}
	if workchains.Workchains == nil {
		return 0, fmt.Errorf("workchains config is empty")
	}

	value, err := workchains.Workchains.LoadValueByIntKey(big.NewInt(int64(workchain)))
	if err != nil {
		return 0, err
	}
	if value.BitsLeft() < 48 && value.RefsNum() > 0 {
		value, err = value.LoadRef()
		if err != nil {
			return 0, err
		}
	}

	magic, err := value.LoadUInt(8)
	if err != nil {
		return 0, err
	}
	if magic != 0xa6 && magic != 0xa7 {
		return 0, fmt.Errorf("unsupported workchain descriptor magic 0x%x", magic)
	}
	if _, err = value.LoadUInt(32); err != nil {
		return 0, err
	}
	depth, err := value.LoadUInt(8)
	if err != nil {
		return 0, err
	}
	return uint32(depth), nil
}
