package service

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"

	"github.com/xssnick/gton/service/archive"
	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	archiveToNextLagSeconds     = 200
	nextToArchiveLagSeconds     = 600
	maxArchiveMonitorSplitDepth = 12
	archiveImportPeerRetries    = 2
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

func (r *archiveCatchUpRunner) downloadAndImportShardArchives(ctx context.Context, queue *archiveImportQueue, masterchainSeqno uint32, plans []archiveShardImportPlan, splitDepth uint32, priority archiveImportPriority) ([]*archiveImportResult, error) {
	if len(plans) == 0 {
		return nil, nil
	}

	preloadCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	type archivePreloadResult struct {
		idx       int
		imported  *archiveImportResult
		peer      string
		archiveID int64
		err       error
	}

	type archivePreloadJob struct {
		idx  int
		done <-chan archiveImportQueueResult
	}

	limit := archiveShardArchiveImportInFlight
	if limit < 1 {
		limit = 1
	}
	if limit > len(plans) {
		limit = len(plans)
	}

	results := make(chan archivePreloadResult, limit)
	var wg sync.WaitGroup
	inFlight := 0
	submit := func(idx int) error {
		shard := plans[idx].shard
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
				res = archivePreloadResult{idx: job.idx, imported: result.imported, peer: result.peer, archiveID: result.archiveID, err: result.err}
			case <-preloadCtx.Done():
				res = archivePreloadResult{idx: job.idx, err: preloadCtx.Err()}
			}
			results <- res
		}()
		inFlight++
		return nil
	}

	imports := make([]*archiveImportResult, len(plans))
	retries := make([]int, len(plans))
	var firstErr error
	next := 0
	completed := 0
	submitMore := func() {
		for firstErr == nil && next < len(plans) && inFlight < limit {
			if err := submit(next); err != nil {
				cancel()
				firstErr = err
				return
			}
			next++
		}
	}
	submitMore()

	for completed < len(plans) && firstErr == nil {
		res := <-results
		inFlight--
		plan := plans[res.idx]

		if res.err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				cancel()
				firstErr = ctxErr
				break
			}
			if r.rejectArchiveImportPeer(plan.shard, res.peer, res.archiveID, p2p.ArchivePeerRejectImportFailed, res.err) {
				retries[res.idx]++
				if retries[res.idx] <= archiveImportPeerRetries {
					if err := submit(res.idx); err != nil {
						cancel()
						firstErr = err
					}
					continue
				}
			} else if ctx.Err() == nil && retries[res.idx] < archiveImportPeerRetries {
				retries[res.idx]++
				if err := submit(res.idx); err != nil {
					cancel()
					firstErr = err
				}
				continue
			}
			cancel()
			if firstErr == nil {
				firstErr = fmt.Errorf("preload shard archive #%d %s: %w", masterchainSeqno, plan.shard.String(), res.err)
			}
			break
		}

		if err := validateArchiveImportCoversPlan(res.imported, plan); err != nil {
			if r.rejectArchiveImportPeer(plan.shard, res.peer, res.archiveID, p2p.ArchivePeerRejectImportIncomplete, err) {
				retries[res.idx]++
				if retries[res.idx] <= archiveImportPeerRetries {
					if err := submit(res.idx); err != nil {
						cancel()
						firstErr = err
					}
					continue
				}
			} else if ctx.Err() == nil && retries[res.idx] < archiveImportPeerRetries {
				retries[res.idx]++
				if err := submit(res.idx); err != nil {
					cancel()
					firstErr = err
				}
				continue
			}
			cancel()
			firstErr = fmt.Errorf("preload shard archive #%d %s: %w", masterchainSeqno, plan.shard.String(), err)
			break
		}

		imports[res.idx] = res.imported
		completed++
		submitMore()
	}
	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}
	if completed != len(plans) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("preload shard archive #%d: incomplete import results got=%d want=%d", masterchainSeqno, completed, len(plans))
	}
	return imports, nil
}

func validateArchiveImportCoversPlan(imported *archiveImportResult, plan archiveShardImportPlan) error {
	if imported == nil {
		return fmt.Errorf("archive shard %s import returned empty result", plan.shard.String())
	}

	for _, block := range plan.needed {
		if _, ok := imported.blocks[storage.BlockKey(block)]; !ok {
			return fmt.Errorf("archive shard %s does not contain planned shard block %s", plan.shard.String(), storage.FormatBlockRef(block))
		}
	}
	return nil
}

func (r *archiveCatchUpRunner) rejectArchiveImportPeer(shard archive.ShardID, peer string, archiveID int64, reason string, err error) bool {
	if r.archiveSession == nil || peer == "" {
		return false
	}

	rejected := r.archiveSession.RejectArchivePeer(shard, peer, reason)
	if rejected && r.service != nil {
		r.service.log.Debug().
			Err(err).
			Str("peer", peer).
			Int64("archive_id", archiveID).
			Int32("workchain", shard.Workchain).
			Str("shard", fmt.Sprintf("%016x", uint64(shard.Shard))).
			Str("reason", reason).
			Msg("rejected archive import peer")
	}
	return rejected
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
		if errors.Is(err, tlb.ErrBlockchainConfigParamAbsent) {
			return 0, nil
		}
		return 0, err
	}
	if workchains.Workchains == nil {
		return 0, nil
	}

	value, err := workchains.Workchains.LoadValueByIntKey(big.NewInt(int64(workchain)))
	if err != nil {
		if errors.Is(err, cell.ErrNoSuchKeyInDict) {
			return 0, nil
		}
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
