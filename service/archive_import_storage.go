package service

import (
	"context"
	"fmt"
	"sort"

	"github.com/xssnick/gton/service/archive"
	state2 "github.com/xssnick/gton/service/state"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

func (s *Service) importArchiveBlocks(ctx context.Context, downloaded *archive.Downloaded, splitDepth uint32) (*archiveImportResult, error) {
	imported, err := loadDownloadedArchive(ctx, downloaded)
	if err != nil {
		return nil, err
	}
	result, err := s.prepareImportedArchiveBlocks(imported, splitDepth)
	if err != nil {
		return nil, err
	}
	s.observeImportedArchiveBlocksReceived(ctx, imported, result)
	return result, nil
}

func loadDownloadedArchive(ctx context.Context, downloaded *archive.Downloaded) (*archive.Imported, error) {
	if downloaded == nil {
		return nil, archive.ErrNotAvailable
	}
	if len(downloaded.Data) == 0 {
		return nil, fmt.Errorf("import archive: empty downloaded archive data")
	}

	imported, err := archive.ImportBytes(ctx, downloaded, downloaded.Data)
	if err != nil {
		return nil, err
	}
	return imported, nil
}

func (s *Service) observeImportedArchiveBlocksReceived(ctx context.Context, imported *archive.Imported, result *archiveImportResult) {
	if s.blockReceivedHooks == nil || imported == nil || result == nil {
		return
	}

	seenBlocks := map[storage.BlockRootHash]struct{}{}
	for _, full := range imported.FullBlocks {
		key := storage.BlockKey(full.ID)
		if _, exists := seenBlocks[key]; exists {
			continue
		}
		seenBlocks[key] = struct{}{}

		block, ok := result.blocks[key]
		if ok {
			s.observePreparedBlockReceived(ctx, block, true)
		}
	}
}

func (s *Service) prepareImportedArchiveBlocks(imported *archive.Imported, splitDepth uint32) (*archiveImportResult, error) {
	if imported == nil {
		return nil, fmt.Errorf("import archive: empty imported data")
	}
	if imported.Stats == nil {
		return nil, fmt.Errorf("import archive: empty stats")
	}

	blocks := map[storage.BlockRootHash]PreparedBlock{}

	seenBlocks := map[storage.BlockRootHash]struct{}{}
	for _, full := range imported.FullBlocks {
		key := storage.BlockKey(full.ID)
		if _, exists := seenBlocks[key]; exists {
			continue
		}
		seenBlocks[key] = struct{}{}
		prepared := imported.PreparedBlocks[key]
		if prepared.Meta == nil {
			return nil, fmt.Errorf("archive block %s was not prepared by archive import", storage.FormatBlockRef(full.ID))
		}
		if prepared.State == nil {
			return nil, fmt.Errorf("archive block %s state was not prepared by archive import", storage.FormatBlockRef(full.ID))
		}
		if prepared.StateUpdate == nil {
			return nil, fmt.Errorf("archive block %s state update was not prepared by archive import", storage.FormatBlockRef(full.ID))
		}
		blocks[key] = PreparedBlock{
			ID:                        full.ID,
			BlockBOC:                  full.Block,
			ProofBOC:                  full.Proof,
			BlockRoot:                 prepared.Block,
			Meta:                      prepared.Meta.Clone(),
			StateUpdate:               prepared.StateUpdate,
			StateUpdateToCells:        prepared.StateUpdateToCells,
			StateUpdateToCellsElapsed: prepared.StateUpdateToCellsElapsed,
			PrepareElapsed:            prepared.StateUpdateToCellsElapsed,
			IsLink:                    storage.ServedBlockProofIsLink(full.ID, full.IsLink),
		}
	}

	return &archiveImportResult{stats: imported.Stats, blocks: blocks, splitDepth: splitDepth}, nil
}

type archiveShardPrefixInputs struct {
	splitDepth  uint32
	startBlocks []ton.BlockIDExt
	stateBlocks [][]ton.BlockIDExt
}

type archiveShardImportPlan struct {
	shard      archive.ShardID
	splitDepth uint32
	needed     []ton.BlockIDExt
}

func (s *Service) missingArchiveShardImportPlansForWindow(start *storage.BlockState, states map[uint32]*storage.BlockState, blocks map[storage.BlockRootHash]PreparedBlock) ([]archiveShardImportPlan, uint32, error) {
	inputs, err := s.archiveShardPrefixInputsForWindow(start, states)
	if err != nil {
		return nil, 0, err
	}
	return archiveShardImportPlansMissingFromBlockStates(inputs.splitDepth, inputs.startBlocks, inputs.stateBlocks, blocks), inputs.splitDepth, nil
}

func (s *Service) archiveShardPrefixInputsForWindow(start *storage.BlockState, states map[uint32]*storage.BlockState) (archiveShardPrefixInputs, error) {
	splitDepth, err := monitorMinSplitDepth(start, 0)
	if err != nil {
		return archiveShardPrefixInputs{}, fmt.Errorf("load monitor split depth for %s: %w", storage.FormatBlockRef(start.Block), err)
	}
	if splitDepth > maxArchiveMonitorSplitDepth {
		return archiveShardPrefixInputs{}, fmt.Errorf("monitor split depth %d exceeds supported archive prefix fanout %d", splitDepth, maxArchiveMonitorSplitDepth)
	}

	startBlocks, err := state2.ShardBlocksFromMasterState(start)
	if err != nil {
		return archiveShardPrefixInputs{}, fmt.Errorf("load start shard blocks from %s: %w", storage.FormatBlockRef(start.Block), err)
	}

	seqnos := make([]int, 0, len(states))
	for seqno := range states {
		seqnos = append(seqnos, int(seqno))
	}
	sort.Ints(seqnos)

	stateBlocks := make([][]ton.BlockIDExt, 0, len(seqnos))
	for _, seqno := range seqnos {
		state := states[uint32(seqno)]
		blocks, err := state2.ShardBlocksFromMasterState(state)
		if err != nil {
			return archiveShardPrefixInputs{}, fmt.Errorf("load shard blocks from %s: %w", storage.FormatBlockRef(state.Block), err)
		}
		stateBlocks = append(stateBlocks, blocks)
	}

	return archiveShardPrefixInputs{
		splitDepth:  splitDepth,
		startBlocks: startBlocks,
		stateBlocks: stateBlocks,
	}, nil
}

func archiveShardImportPlansMissingFromBlockStates(splitDepth uint32, startBlocks []ton.BlockIDExt, stateBlocks [][]ton.BlockIDExt, blocks map[storage.BlockRootHash]PreparedBlock) []archiveShardImportPlan {
	return archiveShardImportPlansForBlockStatesMatching(splitDepth, startBlocks, stateBlocks, func(block ton.BlockIDExt) bool {
		downloaded, ok := blocks[storage.BlockKey(block)]
		return !ok || !downloaded.ID.Equals(&block)
	})
}

func archiveShardImportPlansForBlockStatesMatching(splitDepth uint32, startBlocks []ton.BlockIDExt, stateBlocks [][]ton.BlockIDExt, needBlock func(ton.BlockIDExt) bool) []archiveShardImportPlan {
	startByShard := make(map[storage.ShardKey]ton.BlockIDExt, len(startBlocks))
	for _, block := range startBlocks {
		startByShard[storage.ShardKeyFromBlock(block)] = block
	}

	count := 1 << splitDepth
	plansByPrefix := make([]archiveShardImportPlan, count)
	seenByPrefix := make([]map[storage.BlockRootHash]struct{}, count)
	for _, blocks := range stateBlocks {
		for _, next := range blocks {
			// Zerostates are downloaded through state overlay, not shard archive packages.
			if next.SeqNo == 0 {
				continue
			}
			if next.Workchain != 0 {
				continue
			}

			prev, ok := startByShard[storage.ShardKeyFromBlock(next)]
			if ok && prev.Equals(&next) {
				continue
			}
			if needBlock != nil && !needBlock(next) {
				continue
			}

			key := storage.BlockKey(next)
			start, end := archiveShardPrefixIndexRange(splitDepth, next.Shard)
			for idx := start; idx < end; idx++ {
				seen := seenByPrefix[idx]
				if seen == nil {
					seen = map[storage.BlockRootHash]struct{}{}
					seenByPrefix[idx] = seen
					plansByPrefix[idx] = archiveShardImportPlan{
						shard:      archiveShardIDForPrefixIndex(splitDepth, idx),
						splitDepth: splitDepth,
					}
				}
				if _, ok = seen[key]; ok {
					continue
				}
				seen[key] = struct{}{}
				plansByPrefix[idx].needed = append(plansByPrefix[idx].needed, next)
			}
		}
	}

	plans := make([]archiveShardImportPlan, 0, count)
	for _, plan := range plansByPrefix {
		if len(plan.needed) > 0 {
			plans = append(plans, plan)
		}
	}
	return plans
}

func archiveShardIDForPrefixIndex(splitDepth uint32, idx int) archive.ShardID {
	shard := uint64(idx*2+1) << (64 - splitDepth - 1)
	return archive.ShardID{
		Workchain: 0,
		Shard:     int64(shard),
	}
}

func archiveShardPrefixIndexRange(splitDepth uint32, shard int64) (int, int) {
	count := 1 << splitDepth
	depth := storage.ShardPrefixLength(shard)
	if depth <= 0 {
		return 0, count
	}
	if uint32(depth) >= splitDepth {
		idx := int(uint64(shard) >> (64 - splitDepth))
		return idx, idx + 1
	}

	prefix := uint64(shard) >> (64 - uint(depth))
	span := 1 << (splitDepth - uint32(depth))
	start := int(prefix) * span
	return start, start + span
}
