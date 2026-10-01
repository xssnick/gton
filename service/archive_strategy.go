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
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	archiveToNextLagSeconds     = 200
	nextToArchiveLagSeconds     = 600
	maxArchiveMonitorSplitDepth = 12
	archiveImportPeerRetries    = 2
)

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

func (r *archiveCatchUpRun) downloadAndImportShardArchives(ctx context.Context, queue *archiveImportQueue, masterchainSeqno uint32, plans []archiveShardImportPlan, splitDepth uint32, priority archiveImportPriority) ([]*archiveImportResult, error) {
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
	submit := func(idx int) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Keep completed shard imports across pipeline retries, just like
			// master imports. Otherwise an unavailable shard discards all of
			// its successful siblings and every window starts downloading again.
			loaded, err := r.loadArchiveImport(preloadCtx, queue, masterchainSeqno, plans[idx].shard, splitDepth, priority, nil)
			res := archivePreloadResult{idx: idx, imported: loaded.imported, err: err}
			if err == nil {
				res.peer = loaded.imported.stats.Peer
				res.archiveID = loaded.imported.stats.ArchiveID
			} else {
				var peerErr *archiveImportPeerError
				if errors.As(err, &peerErr) {
					res.peer = peerErr.peer
					res.archiveID = peerErr.archiveID
				}
			}
			results <- res
		}()
		inFlight++
	}

	imports := make([]*archiveImportResult, len(plans))
	retries := make([]int, len(plans))
	var firstErr error
	next := 0
	completed := 0
	submitMore := func() {
		for firstErr == nil && next < len(plans) && inFlight < limit {
			submit(next)
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
					submit(res.idx)
					continue
				}
			} else if ctx.Err() == nil && retries[res.idx] < archiveImportPeerRetries {
				retries[res.idx]++
				submit(res.idx)
				continue
			}
			// A transient failure must not repeatedly cancel slower successful
			// siblings before they reach the cache. Stop submitting new work,
			// but join the already bounded in-flight imports before restarting.
			if !isArchiveCatchUpRetryError(res.err) {
				cancel()
			}
			firstErr = fmt.Errorf("preload shard archive #%d %s: %w", masterchainSeqno, plan.shard.String(), res.err)
			break
		}

		if err := validateArchiveImportCoversPlan(res.imported, plan); err != nil {
			r.importCache.drop(res.imported.cacheKey)
			if r.rejectArchiveImportPeer(plan.shard, res.peer, res.archiveID, p2p.ArchivePeerRejectImportIncomplete, err) {
				retries[res.idx]++
				if retries[res.idx] <= archiveImportPeerRetries {
					submit(res.idx)
					continue
				}
			} else if ctx.Err() == nil && retries[res.idx] < archiveImportPeerRetries {
				retries[res.idx]++
				submit(res.idx)
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
	for _, block := range plan.needed {
		if _, ok := imported.blocks[storage.BlockKey(block)]; !ok {
			return fmt.Errorf("archive shard %s does not contain planned shard block %s", plan.shard.String(), storage.FormatBlockRef(block))
		}
	}
	return nil
}

func (r *archiveCatchUpRun) rejectArchiveImportPeer(shard archive.ShardID, peer string, archiveID int64, reason string, err error) bool {
	if peer == "" {
		return false
	}

	rejected := r.archiveSession.RejectArchivePeer(shard, peer, reason)
	if rejected && r.archive != nil {
		r.archive.log.Debug().
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
