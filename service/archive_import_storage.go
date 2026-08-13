package service

import (
	"context"
	"fmt"
	"sort"

	"github.com/xssnick/gton/service/archive"
	sharddomain "github.com/xssnick/gton/service/shard"
	state2 "github.com/xssnick/gton/service/state"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

func (a *ArchiveRunner) importArchiveBlocks(ctx context.Context, importer *archive.Importer, downloaded *archive.Downloaded, splitDepth uint32) (*archiveImportResult, error) {
	if len(downloaded.Data) == 0 {
		return nil, fmt.Errorf("import archive: empty downloaded archive data")
	}

	imported, err := importer.ImportBytes(ctx, downloaded, downloaded.Data)
	if err != nil {
		return nil, err
	}
	result, err := a.prepareImportedArchiveBlocks(imported, splitDepth)
	if err != nil {
		return nil, err
	}
	a.observeImportedArchiveBlocksReceived(ctx, imported, result)
	return result, nil
}

func (a *ArchiveRunner) observeImportedArchiveBlocksReceived(ctx context.Context, imported *archive.Imported, result *archiveImportResult) {
	if a.blockReceivedObserver == nil {
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
			a.blockReceivedObserver.run(ctx, BlockReceivedEvent{
				IsSigned:  true,
				BlockBOC:  block.BlockBOC,
				ProofBOC:  block.ProofBOC,
				BlockRoot: block.BlockRoot,
				Meta:      blockReceivedPreparedMeta(block),
			})
		}
	}
}

func (a *ArchiveRunner) prepareImportedArchiveBlocks(imported *archive.Imported, splitDepth uint32) (*archiveImportResult, error) {
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
	// stateTargets indexes stateBlocks by master seqno so the apply runner can
	// reuse the planner's parse instead of re-reading every master state. The
	// slices are shared between both consumers and must never be mutated.
	stateTargets map[uint32][]ton.BlockIDExt
}

type archiveShardImportPlan struct {
	shard      archive.ShardID
	splitDepth uint32
	needed     []ton.BlockIDExt
}

func (a *ArchiveRunner) missingArchiveShardImportPlansForWindow(start *storage.BlockState, states map[uint32]*storage.BlockState, blocks map[storage.BlockRootHash]PreparedBlock) ([]archiveShardImportPlan, archiveShardPrefixInputs, error) {
	inputs, err := a.archiveShardPrefixInputsForWindow(start, states)
	if err != nil {
		return nil, archiveShardPrefixInputs{}, err
	}
	plans, err := archiveShardImportPlansMissingFromBlockStates(inputs.splitDepth, inputs.startBlocks, inputs.stateBlocks, blocks)
	return plans, inputs, err
}

func (a *ArchiveRunner) archiveShardPrefixInputsForWindow(start *storage.BlockState, states map[uint32]*storage.BlockState) (archiveShardPrefixInputs, error) {
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
	stateTargets := make(map[uint32][]ton.BlockIDExt, len(seqnos))
	for _, seqno := range seqnos {
		state := states[uint32(seqno)]
		blocks, err := state2.ShardBlocksFromMasterState(state)
		if err != nil {
			return archiveShardPrefixInputs{}, fmt.Errorf("load shard blocks from %s: %w", storage.FormatBlockRef(state.Block), err)
		}
		stateBlocks = append(stateBlocks, blocks)
		stateTargets[uint32(seqno)] = blocks
	}

	return archiveShardPrefixInputs{
		splitDepth:   splitDepth,
		startBlocks:  startBlocks,
		stateBlocks:  stateBlocks,
		stateTargets: stateTargets,
	}, nil
}

func archiveShardImportPlansMissingFromBlockStates(splitDepth uint32, startBlocks []ton.BlockIDExt, stateBlocks [][]ton.BlockIDExt, blocks map[storage.BlockRootHash]PreparedBlock) ([]archiveShardImportPlan, error) {
	return archiveShardImportPlansForBlockStatesMatching(splitDepth, startBlocks, stateBlocks, func(block ton.BlockIDExt) bool {
		downloaded, ok := blocks[storage.BlockKey(block)]
		return !ok || !downloaded.ID.Equals(&block)
	})
}

func archiveShardImportPlansForBlockStatesMatching(splitDepth uint32, startBlocks []ton.BlockIDExt, stateBlocks [][]ton.BlockIDExt, needBlock func(ton.BlockIDExt) bool) ([]archiveShardImportPlan, error) {
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
			start, end, err := archiveShardPrefixIndexRange(splitDepth, next.Shard)
			if err != nil {
				return nil, fmt.Errorf("archive shard prefix for %s: %w", storage.FormatBlockRef(next), err)
			}
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
	return plans, nil
}

func archiveShardIDForPrefixIndex(splitDepth uint32, idx int) archive.ShardID {
	shard := uint64(idx*2+1) << (64 - splitDepth - 1)
	return archive.ShardID{
		Workchain: 0,
		Shard:     int64(shard),
	}
}

func archiveShardPrefixIndexRange(splitDepth uint32, shard int64) (int, int, error) {
	count := 1 << splitDepth
	depth, err := sharddomain.PrefixLength(shard)
	if err != nil {
		return 0, 0, err
	}
	if depth == 0 {
		return 0, count, nil
	}
	if depth >= splitDepth {
		idx := int(uint64(shard) >> (64 - splitDepth))
		return idx, idx + 1, nil
	}

	prefix := uint64(shard) >> (64 - depth)
	span := 1 << (splitDepth - depth)
	start := int(prefix) * span
	return start, start + span, nil
}
