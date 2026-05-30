package service

import (
	"context"
	"fmt"
	"os"
	"sort"

	"github.com/xssnick/gton/service/archive"
	state2 "github.com/xssnick/gton/service/state"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

func (s *Service) importArchiveBlocks(ctx context.Context, downloaded *archive.Downloaded, splitDepth uint32) (*archiveImportResult, error) {
	if downloaded == nil {
		return nil, archive.ErrNotAvailable
	}

	cleanupPath := downloaded.Path
	cleanupDownloaded := cleanupPath != ""
	defer func() {
		if cleanupDownloaded {
			_ = os.Remove(cleanupPath)
		}
	}()

	imported := downloaded.Imported
	if downloaded.Path != "" {
		stored, err := s.storage.SaveArchiveFile(
			int32(downloaded.MasterchainSeqno),
			downloaded.Shard.Workchain,
			downloaded.Shard.Shard,
			downloaded.ArchiveID,
			downloaded.Path,
		)
		if err != nil {
			return nil, fmt.Errorf("store downloaded archive pack: %w", err)
		}
		cleanupDownloaded = false
		if stored.ReusedExisting {
			imported, err = importArchiveFile(ctx, downloaded, stored.Path)
			if err != nil {
				return nil, err
			}
		} else if imported == nil {
			imported, err = importArchiveFile(ctx, downloaded, stored.Path)
			if err != nil {
				return nil, err
			}
		} else {
			imported.SetArtifactPath(stored.Path)
		}
	} else if imported == nil {
		var err error
		imported, err = importArchiveFile(ctx, downloaded, downloaded.Path)
		if err != nil {
			return nil, err
		}
	}

	return s.prepareImportedArchiveBlocks(imported, splitDepth)
}

func importArchiveFile(ctx context.Context, downloaded *archive.Downloaded, path string) (*archive.Imported, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open archive pack %s: %w", path, err)
	}
	defer func() { _ = file.Close() }()

	archiveRef := *downloaded
	archiveRef.Path = path
	archiveRef.Imported = nil
	imported, err := archive.ImportStream(ctx, &archiveRef, file)
	if err != nil {
		return nil, err
	}
	return imported, nil
}

func (s *Service) prepareImportedArchiveBlocks(imported *archive.Imported, splitDepth uint32) (*archiveImportResult, error) {
	if imported == nil {
		return nil, fmt.Errorf("import archive: empty imported data")
	}
	if imported.Stats == nil {
		return nil, fmt.Errorf("import archive %s: empty stats", imported.ArtifactPath)
	}

	blocks := map[storage.BlockRootHash]PreparedBlock{}
	stored := storage.ServedArchiveImport{
		FullBlocks: make([]*storage.ServedBlockFull, 0, len(imported.FullBlocks)),
		Links:      append([]storage.ServedBlockLink(nil), imported.Links...),
	}

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
			Kind:                      "archive block",
			BlockBOC:                  full.Block,
			ProofBOC:                  full.Proof,
			BlockRoot:                 prepared.Block,
			Meta:                      prepared.Meta.Clone(),
			StateUpdate:               prepared.StateUpdate,
			StateUpdateToCells:        prepared.StateUpdateToCells,
			StateUpdateToCellsElapsed: prepared.StateUpdateToCellsElapsed,
			PrepareElapsed:            prepared.StateUpdateToCellsElapsed,
			IsLink:                    full.IsLink,
		}
		stored.FullBlocks = append(stored.FullBlocks, &storage.ServedBlockFull{
			ID:                     full.ID,
			BlockRef:               full.BlockRef.Clone(),
			ProofRef:               full.ProofRef.Clone(),
			Meta:                   prepared.Meta.Clone(),
			IsLink:                 full.IsLink,
			ArchiveShardSplitDepth: splitDepth,
		})
	}

	return &archiveImportResult{stats: imported.Stats, blocks: blocks, stored: stored, splitDepth: splitDepth}, nil
}

type archiveShardPrefixInputs struct {
	splitDepth  uint32
	startBlocks []ton.BlockIDExt
	stateBlocks [][]ton.BlockIDExt
}

func (s *Service) archiveShardPrefixesForWindow(start *storage.BlockState, states map[uint32]*storage.BlockState) ([]archive.ShardID, uint32, error) {
	inputs, err := s.archiveShardPrefixInputsForWindow(start, states)
	if err != nil {
		return nil, 0, err
	}
	return archiveShardPrefixesForBlockStates(inputs.splitDepth, inputs.startBlocks, inputs.stateBlocks), inputs.splitDepth, nil
}

func (s *Service) missingArchiveShardPrefixesForWindow(start *storage.BlockState, states map[uint32]*storage.BlockState, blocks map[storage.BlockRootHash]PreparedBlock) ([]archive.ShardID, uint32, error) {
	inputs, err := s.archiveShardPrefixInputsForWindow(start, states)
	if err != nil {
		return nil, 0, err
	}
	return archiveShardPrefixesMissingFromBlockStates(inputs.splitDepth, inputs.startBlocks, inputs.stateBlocks, blocks), inputs.splitDepth, nil
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

func archiveShardPrefixesForBlockStates(splitDepth uint32, startBlocks []ton.BlockIDExt, stateBlocks [][]ton.BlockIDExt) []archive.ShardID {
	return archiveShardPrefixesForBlockStatesMatching(splitDepth, startBlocks, stateBlocks, nil)
}

func archiveShardPrefixesMissingFromBlockStates(splitDepth uint32, startBlocks []ton.BlockIDExt, stateBlocks [][]ton.BlockIDExt, blocks map[storage.BlockRootHash]PreparedBlock) []archive.ShardID {
	return archiveShardPrefixesForBlockStatesMatching(splitDepth, startBlocks, stateBlocks, func(block ton.BlockIDExt) bool {
		_, ok := blocks[storage.BlockKey(block)]
		return !ok
	})
}

func archiveShardPrefixesForBlockStatesMatching(splitDepth uint32, startBlocks []ton.BlockIDExt, stateBlocks [][]ton.BlockIDExt, needBlock func(ton.BlockIDExt) bool) []archive.ShardID {
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
		for _, blocks := range stateBlocks {
			if archivePrefixHasChangedShardMatching(prefix, blocks, startByShard, needBlock) {
				shards = append(shards, prefix)
				break
			}
		}
	}
	return shards
}
