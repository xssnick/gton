package service

import (
	"context"
	"flexserver/service/archive"
	"flexserver/service/p2p"
	state2 "flexserver/service/state"
	"flexserver/service/storage"
	"fmt"
	"os"
	"time"

	"github.com/xssnick/tonutils-go/ton"
)

const (
	archiveCatchUpProgressInterval = 5 * time.Second
)

type archiveImportResult struct {
	stats  *archive.ImportStats
	blocks map[string]p2p.DownloadedBlock
}

type shardClientArchiveWindow struct {
	startSeqno    uint32
	masterStats   *archive.ImportStats
	totalStats    *archive.ImportStats
	masterStates  map[uint32]*storage.BlockState
	masterBlocks  map[uint32]p2p.DownloadedBlock
	archiveBlocks map[string]p2p.DownloadedBlock
	shardArchives int
	splitDepth    uint32
}

func (s *Service) catchUpShardClientFromArchives(ctx context.Context, current *storage.CurrentState, target ton.BlockIDExt) (*storage.CurrentState, error) {
	current = storage.CloneCurrentState(current)
	if current.ShardClientSeqno == 0 {
		current.ShardClientSeqno = current.Masterchain.Block.SeqNo
	}
	if current.Masterchain.Block.SeqNo != current.ShardClientSeqno {
		return nil, fmt.Errorf("current masterchain seqno %d differs from shard client seqno %d", current.Masterchain.Block.SeqNo, current.ShardClientSeqno)
	}

	started := time.Now()
	startSeqno := current.ShardClientSeqno
	lastProgress := started

	s.log.Info().
		Str("from", storage.FormatBlockRef(current.Masterchain.Block)).
		Str("target", storage.FormatBlockRef(target)).
		Uint32("shard_client_seqno", current.ShardClientSeqno).
		Uint32("total_masterchain_blocks", target.SeqNo-current.ShardClientSeqno).
		Msg("starting archive shard-client catch-up")

	for current.ShardClientSeqno < target.SeqNo {
		before := current.ShardClientSeqno
		window, err := s.importShardClientArchiveWindow(ctx, current, target)
		if err != nil {
			return nil, err
		}
		if len(window.masterStates) == 0 {
			return nil, fmt.Errorf("archive window #%d did not provide next masterchain blocks", window.startSeqno)
		}

		next, err := s.applyShardClientArchiveWindow(ctx, current, window)
		if err != nil {
			return nil, err
		}
		if next.ShardClientSeqno <= before {
			return nil, fmt.Errorf("archive window #%d did not advance shard client seqno %d", window.startSeqno, before)
		}
		current = next

		if time.Since(lastProgress) >= archiveCatchUpProgressInterval || current.ShardClientSeqno >= target.SeqNo {
			done := current.ShardClientSeqno - startSeqno
			total := target.SeqNo - startSeqno
			s.log.Info().
				Str("current", storage.FormatBlockRef(current.Masterchain.Block)).
				Str("target", storage.FormatBlockRef(target)).
				Uint32("processed_masterchain_blocks", done).
				Uint32("total_masterchain_blocks", total).
				Uint32("remaining", target.SeqNo-current.ShardClientSeqno).
				Str("progress", formatCatchUpProgress(done, total)).
				Str("speed", formatBlockRate(done, time.Since(started))).
				Str("eta", formatCatchUpETA(done, total, time.Since(started))).
				Msg("archive shard-client catch-up progress")
			lastProgress = time.Now()
		}
	}

	s.log.Info().
		Str("masterchain", storage.FormatBlockRef(current.Masterchain.Block)).
		Uint32("shard_client_seqno", current.ShardClientSeqno).
		Int("shards", len(current.Shards)).
		Msg("archive shard-client catch-up completed")
	return current, nil
}

func (s *Service) importShardClientArchiveWindow(ctx context.Context, current *storage.CurrentState, target ton.BlockIDExt) (*shardClientArchiveWindow, error) {
	startSeqno := current.ShardClientSeqno + 1
	startMaster, err := s.loadBlockStateForApply(ctx, current.Masterchain)
	if err != nil {
		return nil, fmt.Errorf("load archive start masterchain state %s: %w", storage.FormatBlockRef(current.Masterchain.Block), err)
	}

	masterArchive, err := s.node.DownloadArchive(ctx, startSeqno, archive.ShardID{Workchain: -1, Shard: topShard}, "")
	if err != nil {
		return nil, fmt.Errorf("download master archive #%d: %w", startSeqno, err)
	}
	masterImport, err := s.importArchiveBlocks(ctx, masterArchive)
	if err != nil {
		return nil, fmt.Errorf("import master archive #%d: %w", startSeqno, err)
	}

	window := &shardClientArchiveWindow{
		startSeqno:    startSeqno,
		masterStats:   masterImport.stats,
		totalStats:    cloneImportStats(masterImport.stats),
		masterStates:  map[uint32]*storage.BlockState{},
		masterBlocks:  map[uint32]p2p.DownloadedBlock{},
		archiveBlocks: masterImport.blocks,
	}
	for _, block := range masterImport.blocks {
		if block.ID.Workchain == -1 && block.ID.Shard == topShard {
			window.masterBlocks[block.ID.SeqNo] = block
		}
	}

	lastMaster, err := s.applyArchiveMasterBlocks(startMaster, target, window)
	if err != nil {
		return nil, err
	}
	if len(window.masterStates) == 0 {
		return window, nil
	}

	if !masterImport.stats.ContainsShardBlocks {
		shards, splitDepth, err := s.archiveShardPrefixesForWindow(startMaster, lastMaster)
		if err != nil {
			return nil, err
		}
		window.splitDepth = splitDepth
		window.shardArchives = len(shards)

		files, err := s.downloadShardArchives(ctx, startSeqno, shards)
		if err != nil {
			return nil, fmt.Errorf("download shard archives #%d: %w", startSeqno, err)
		}
		for _, file := range files {
			imported, err := s.importArchiveBlocks(ctx, file)
			if err != nil {
				for _, cleanup := range files {
					if cleanup != nil {
						_ = os.Remove(cleanup.Path)
					}
				}
				return nil, fmt.Errorf("import shard archive #%d %s: %w", startSeqno, file.Shard.String(), err)
			}
			mergeImportStats(window.totalStats, imported.stats, true)
			for key, block := range imported.blocks {
				window.archiveBlocks[key] = block
			}
		}
	}
	window.totalStats.ShardArchives = window.shardArchives

	s.log.Debug().
		Uint32("archive_masterchain_seqno", startSeqno).
		Int64("archive_id", window.masterStats.ArchiveID).
		Str("peer", window.masterStats.Peer).
		Int("master_blocks", len(window.masterStates)).
		Int("archive_blocks", len(window.archiveBlocks)).
		Int("shard_archives", window.shardArchives).
		Uint32("monitor_split_depth", window.splitDepth).
		Int64("bytes", window.totalStats.Bytes).
		Dur("download_elapsed", window.totalStats.DownloadElapsed).
		Dur("import_elapsed", window.totalStats.ImportElapsed).
		Uint32("first_seqno", window.totalStats.FirstSeqno).
		Uint32("last_seqno", window.totalStats.LastSeqno).
		Uint32("masterchain_first_seqno", window.totalStats.MasterchainFirstSeqno).
		Uint32("masterchain_last_seqno", window.totalStats.MasterchainLastSeqno).
		Msg("archive shard-client window imported")
	return window, nil
}

func (s *Service) applyArchiveMasterBlocks(start *storage.BlockState, target ton.BlockIDExt, window *shardClientArchiveWindow) (*storage.BlockState, error) {
	master := start
	for seqno := start.Block.SeqNo + 1; seqno != 0 && seqno <= target.SeqNo; seqno++ {
		downloaded, ok := window.masterBlocks[seqno]
		if !ok {
			if seqno == start.Block.SeqNo+1 {
				return nil, fmt.Errorf("archive window #%d has no next masterchain block after %s", window.startSeqno, storage.FormatBlockRef(start.Block))
			}
			break
		}
		if downloaded.Meta != nil && seqno != window.startSeqno && downloaded.Meta.Has(storage.BlockMetaIsKeyBlock) {
			break
		}

		if downloaded.Meta == nil || len(downloaded.Meta.PrevRefs) != 1 || !downloaded.Meta.PrevRefs[0].Equals(&master.Block) {
			return nil, fmt.Errorf("archive master block %s does not follow %s", downloaded.BlockRef(), storage.FormatBlockRef(master.Block))
		}

		next, err := ApplyBlock(master, downloaded)
		if err != nil {
			return nil, fmt.Errorf("apply archive master block %s: %w", downloaded.BlockRef(), err)
		}
		master = next
		window.masterStates[seqno] = master
	}
	return master, nil
}

func (s *Service) applyShardClientArchiveWindow(ctx context.Context, current *storage.CurrentState, window *shardClientArchiveWindow) (*storage.CurrentState, error) {
	applied := map[string]*storage.BlockState{}
	next := storage.CloneCurrentState(current)

	seqno := current.ShardClientSeqno + 1
	for ; ; seqno++ {
		masterState := window.masterStates[seqno]
		if masterState == nil {
			break
		}

		targets, err := state2.ShardBlocksFromMasterState(masterState)
		if err != nil {
			return nil, fmt.Errorf("load shard blocks from archive master state %s: %w", storage.FormatBlockRef(masterState.Block), err)
		}

		prevShards := next.Shards
		next = &storage.CurrentState{
			SyncedAt:         time.Now(),
			ShardClientSeqno: seqno,
			Masterchain:      blockStateWithoutCells(masterState),
			Shards:           make(map[storage.ShardKey]storage.BlockState, len(targets)),
		}

		for _, target := range targets {
			shardState, err := s.applyArchiveShardBlock(ctx, target, masterState.Block, prevShards, applied, window.archiveBlocks)
			if err != nil {
				return nil, fmt.Errorf("apply archive shard block %s at masterchain seqno %d: %w", storage.FormatBlockRef(target), seqno, err)
			}
			next.Shards[storage.ShardKeyFromBlock(target)] = blockStateWithoutCells(shardState)
		}
	}
	if next.ShardClientSeqno == current.ShardClientSeqno {
		return next, nil
	}

	if err := s.persistArchiveCurrentState(ctx, next, applied, window.masterStates[next.ShardClientSeqno]); err != nil {
		return nil, err
	}
	return next, nil
}

func (s *Service) applyArchiveShardBlock(ctx context.Context, block ton.BlockIDExt, master ton.BlockIDExt, current map[storage.ShardKey]storage.BlockState, applied map[string]*storage.BlockState, blocks map[string]p2p.DownloadedBlock) (*storage.BlockState, error) {
	if state, ok, err := s.archiveBlockState(ctx, block, current, applied); ok || err != nil {
		return state, err
	}
	if block.SeqNo == 0 {
		return nil, fmt.Errorf("zerostate %s is missing", storage.FormatBlockRef(block))
	}

	downloaded, ok := blocks[storage.BlockKey(block)]
	if !ok {
		return nil, fmt.Errorf("no archive data/proof for shard block %s", storage.FormatBlockRef(block))
	}
	if downloaded.Meta == nil {
		return nil, fmt.Errorf("archive shard block %s is missing metadata", downloaded.BlockRef())
	}
	if downloaded.Meta.MasterchainRef != nil && downloaded.Meta.MasterchainRef.SeqNo > master.SeqNo {
		return nil, fmt.Errorf("archive shard block %s references future masterchain block %s", downloaded.BlockRef(), storage.FormatBlockRef(*downloaded.Meta.MasterchainRef))
	}

	prevRefs := downloaded.Meta.PrevRefs
	if len(prevRefs) == 0 {
		return nil, fmt.Errorf("archive shard block %s has no previous refs", downloaded.BlockRef())
	}

	previous := make([]*storage.BlockState, len(prevRefs))
	if len(prevRefs) == 1 && prevRefs[0].Workchain == block.Workchain && prevRefs[0].Shard == block.Shard {
		prev, err := s.applyArchiveShardBlock(ctx, prevRefs[0], master, current, applied, blocks)
		if err != nil {
			return nil, err
		}
		previous[0] = prev
	} else {
		for i, prevRef := range prevRefs {
			prev, ok, err := s.archiveBlockState(ctx, prevRef, current, applied)
			if err != nil {
				return nil, err
			}
			if !ok {
				return nil, fmt.Errorf("previous shard state %s is not applied", storage.FormatBlockRef(prevRef))
			}
			previous[i] = prev
		}
	}

	next, err := ApplyBlockWithPreviousStates(previous, downloaded)
	if err != nil {
		return nil, err
	}
	applied[storage.BlockKey(next.Block)] = next
	return next, nil
}

func (s *Service) archiveBlockState(ctx context.Context, block ton.BlockIDExt, current map[storage.ShardKey]storage.BlockState, applied map[string]*storage.BlockState) (*storage.BlockState, bool, error) {
	if state := applied[storage.BlockKey(block)]; state != nil {
		return state, true, nil
	}

	if currentState, ok := current[storage.ShardKeyFromBlock(block)]; ok && currentState.Block.Equals(&block) {
		state, err := s.loadBlockStateForApply(ctx, currentState)
		if err != nil {
			return nil, false, fmt.Errorf("load current shard state %s: %w", storage.FormatBlockRef(block), err)
		}
		return state, true, nil
	}

	return nil, false, nil
}

func (s *Service) persistArchiveCurrentState(ctx context.Context, current *storage.CurrentState, applied map[string]*storage.BlockState, master *storage.BlockState) error {
	if master == nil {
		return fmt.Errorf("archive current state has no master state for seqno %d", current.ShardClientSeqno)
	}

	for _, key := range storage.SortedShardKeys(current.Shards) {
		shard := current.Shards[key]
		state := applied[storage.BlockKey(shard.Block)]
		if state == nil {
			var err error
			state, err = s.storage.BlockState(ctx, shard.Block)
			if err != nil {
				return fmt.Errorf("load archive current shard state %s: %w", storage.FormatBlockRef(shard.Block), err)
			}
		}
		if err := s.storage.SaveBlockState(ctx, state); err != nil {
			return fmt.Errorf("persist archive current shard state %s: %w", storage.FormatBlockRef(state.Block), err)
		}
	}

	if err := s.storage.SaveBlockStateAndCurrentState(ctx, master, current); err != nil {
		return fmt.Errorf("persist archive current state %s: %w", storage.FormatBlockRef(master.Block), err)
	}
	return nil
}

func (s *Service) importArchiveBlocks(ctx context.Context, downloaded *archive.Downloaded) (*archiveImportResult, error) {
	if downloaded == nil {
		return nil, archive.ErrNotAvailable
	}
	defer func() { _ = os.Remove(downloaded.Path) }()

	blocks := map[string]p2p.DownloadedBlock{}
	stats, err := archive.ImportFile(ctx, downloaded, archive.ImportSink{
		Writer: s.storage,
		FullBlock: func(full *storage.ServedBlockFull) error {
			if _, exists := blocks[storage.BlockKey(full.ID)]; exists {
				return nil
			}

			block, err := prepareBlockDataForApply("archive block", full.ID, full.Block)
			if err != nil {
				return err
			}
			block.ProofBOC = full.Proof
			block.IsLink = full.IsLink
			full.Meta = block.Meta
			blocks[storage.BlockKey(full.ID)] = block
			return nil
		},
	})
	if err != nil {
		return nil, err
	}
	return &archiveImportResult{stats: stats, blocks: blocks}, nil
}

func (s *Service) archiveShardPrefixesForWindow(start *storage.BlockState, end *storage.BlockState) ([]archive.ShardID, uint32, error) {
	splitDepth, err := monitorMinSplitDepth(start, 0)
	if err != nil {
		return nil, 0, fmt.Errorf("load monitor split depth for %s: %w", storage.FormatBlockRef(start.Block), err)
	}
	if splitDepth > maxArchiveMonitorSplitDepth {
		return nil, 0, fmt.Errorf("monitor split depth %d exceeds supported archive prefix fanout %d", splitDepth, maxArchiveMonitorSplitDepth)
	}

	startBlocks, err := state2.ShardBlocksFromMasterState(start)
	if err != nil {
		return nil, 0, fmt.Errorf("load start shard blocks from %s: %w", storage.FormatBlockRef(start.Block), err)
	}
	endBlocks, err := state2.ShardBlocksFromMasterState(end)
	if err != nil {
		return nil, 0, fmt.Errorf("load end shard blocks from %s: %w", storage.FormatBlockRef(end.Block), err)
	}

	startByShard := make(map[storage.ShardKey]ton.BlockIDExt, len(startBlocks))
	for _, block := range startBlocks {
		startByShard[storage.ShardKeyFromBlock(block)] = block
	}

	count := 1 << splitDepth
	shards := make([]archive.ShardID, 0, count)
	for i := 0; i < count; i++ {
		shard := uint64(i*2+1) << (64 - splitDepth - 1)
		prefix := archive.ShardID{
			Workchain: 0,
			Shard:     int64(shard),
		}
		if archivePrefixHasChangedShard(prefix, endBlocks, startByShard) {
			shards = append(shards, prefix)
		}
	}
	return shards, splitDepth, nil
}

func cloneImportStats(stats *archive.ImportStats) *archive.ImportStats {
	if stats == nil {
		return &archive.ImportStats{}
	}
	cloned := *stats
	cloned.MasterchainShardBlocks = append([]ton.BlockIDExt(nil), stats.MasterchainShardBlocks...)
	return &cloned
}
