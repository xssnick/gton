package p2p

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"flexserver/service/storage"

	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	stateDownloadTempSuffix       = ".part"
	stateArtifactProgressInterval = 5 * time.Second
)

type zeroStateSnapshotArtifact struct {
	block ton.BlockIDExt
	data  []byte
}

func (a *zeroStateSnapshotArtifact) Block() ton.BlockIDExt {
	return a.block
}

func (a *zeroStateSnapshotArtifact) Decode(ctx context.Context) (*storage.BlockState, error) {
	return a.decode(ctx, nil)
}

func (a *zeroStateSnapshotArtifact) ImportCells(ctx context.Context, cells storage.StateCellTreeImporter) (*storage.BlockState, error) {
	return a.decode(ctx, cells)
}

func (a *zeroStateSnapshotArtifact) decode(ctx context.Context, cells storage.StateCellTreeImporter) (*storage.BlockState, error) {
	state, err := storage.ParseStateBOC(&a.block, a.data, a.block.RootHash, a.block.FileHash)
	if err != nil {
		return nil, err
	}
	state.DownloadedAt = time.Now()
	return importBlockStateCells(ctx, cells, state, nil)
}

func (a *zeroStateSnapshotArtifact) Cleanup() error {
	a.data = nil
	return nil
}

type stagedStateFile struct {
	effectiveShard int64
	peerAddr       string
	path           string
	size           int64
	cells          uint64
	fileHash       []byte
	prefix         []byte
	lazyRoot       *cell.Cell
	state          *storage.BlockState
	keep           bool
	persisted      bool
}

func (f *stagedStateFile) cleanup() error {
	if f == nil || f.path == "" {
		return nil
	}
	if f.keep {
		return nil
	}
	err := os.Remove(f.path)
	if errors.Is(err, os.ErrNotExist) {
		f.path = ""
		return nil
	}
	if err == nil {
		f.path = ""
	}
	return err
}

func (f *stagedStateFile) open() (*os.File, error) {
	if f == nil || f.path == "" {
		return nil, fmt.Errorf("staged state file path is empty")
	}
	return os.Open(f.path)
}

func (f *stagedStateFile) decodeRootCells() (*cell.Cell, []cell.Cell, error) {
	file, err := f.open()
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = file.Close() }()

	roots, parsedCells, err := cell.FromBOCMultiRootReader(file, cell.BOCParseOptions{})
	if err != nil {
		return nil, nil, err
	}
	root, err := singleStateBOCRoot(roots)
	if err != nil {
		return nil, nil, err
	}
	return root, parsedCells, nil
}

func (f *stagedStateFile) rootCells() (*cell.Cell, []cell.Cell, error) {
	if f != nil && f.lazyRoot != nil {
		return f.lazyRoot, nil, nil
	}
	return f.decodeRootCells()
}

func singleStateBOCRoot(roots []*cell.Cell) (*cell.Cell, error) {
	if len(roots) != 1 {
		return nil, fmt.Errorf("boc should contain exactly one root, got %d", len(roots))
	}
	return roots[0], nil
}

func (n *Node) acquireStateCellImportSlot(ctx context.Context, block ton.BlockIDExt, staged *stagedStateFile) (func(), error) {
	if n.stateCellImportSlot == nil {
		return func() {}, nil
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case n.stateCellImportSlot <- struct{}{}:
		return func() { <-n.stateCellImportSlot }, nil
	default:
		n.log.Info().
			Str("block", formatPersistentStateBlockRef(block, staged.effectiveShard)).
			Str("size", formatByteSize(staged.size)).
			Uint64("cells", staged.cells).
			Msg("staged state snapshot is ready, waiting for cell import slot")

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case n.stateCellImportSlot <- struct{}{}:
			return func() { <-n.stateCellImportSlot }, nil
		}
	}
}

type stagedStateHashKind uint8

const (
	stagedStateRootHash stagedStateHashKind = iota
	stagedStateCellHash
)

func (n *Node) decodeAndImportStagedStateCellTree(ctx context.Context, block ton.BlockIDExt, staged *stagedStateFile, wantRootHash []byte) (*cell.Cell, error) {
	return n.decodeAndImportStagedStateCellTreeAs(ctx, block, blockWithEffectiveShard(block, staged.effectiveShard), staged, wantRootHash, stagedStateRootHash)
}

func (n *Node) decodeAndImportStagedStateCellTreeAs(ctx context.Context, logBlock ton.BlockIDExt, storageBlock ton.BlockIDExt, staged *stagedStateFile, wantRootHash []byte, hashKind stagedStateHashKind) (*cell.Cell, error) {
	release, err := n.acquireStateCellImportSlot(ctx, logBlock, staged)
	if err != nil {
		return nil, err
	}
	defer release()

	root, parsedCells, err := staged.decodeRootCells()
	if err != nil {
		return nil, fmt.Errorf("%w: parse staged state boc: %w", errStateSnapshotInvalid, err)
	}
	if len(wantRootHash) > 0 {
		rootHash := stagedStateHash(root, hashKind)
		if !bytes.Equal(rootHash[:], wantRootHash) {
			root = nil
			parsedCells = nil
			runtime.GC()
			return nil, fmt.Errorf("%w: staged state root hash mismatch", errStateSnapshotInvalid)
		}
	}

	lazyRoot, err := n.importStagedStateCellTree(ctx, storageBlock, staged, root, parsedCells)
	root = nil
	parsedCells = nil
	runtime.GC()
	if err != nil {
		return nil, err
	}
	return lazyRoot, nil
}

func stagedStateHash(root *cell.Cell, kind stagedStateHashKind) cell.Hash {
	if kind == stagedStateCellHash {
		return root.HashKey()
	}
	return root.HashKey(0)
}

func (n *Node) tryImportReusableStagedStateFile(ctx context.Context, block ton.BlockIDExt, master ton.BlockIDExt, effectiveShard int64, wantRootHash []byte) (*stagedStateFile, *cell.Cell, error) {
	return n.tryImportReusableStagedStateFileAs(ctx, block, master, blockWithEffectiveShard(block, effectiveShard), effectiveShard, wantRootHash, stagedStateRootHash)
}

func (n *Node) tryImportReusableStagedStateFileAs(ctx context.Context, block ton.BlockIDExt, master ton.BlockIDExt, storageBlock ton.BlockIDExt, effectiveShard int64, wantRootHash []byte, hashKind stagedStateHashKind) (*stagedStateFile, *cell.Cell, error) {
	paths, err := n.reusableStagedStateFiles(block, master, effectiveShard)
	if err != nil {
		return nil, nil, err
	}

	for _, path := range paths {
		if err = ctx.Err(); err != nil {
			return nil, nil, err
		}

		staged, err := n.loadReusableStagedStateFile(path, effectiveShard)
		if err != nil {
			if errors.Is(err, errStateSnapshotInvalid) {
				n.removeInvalidReusableStateFile(block, effectiveShard, path, err)
				continue
			}
			return nil, nil, err
		}

		n.log.Info().
			Str("block", formatPersistentStateBlockRef(block, effectiveShard)).
			Str("size", formatByteSize(staged.size)).
			Uint64("cells", staged.cells).
			Str("path", staged.path).
			Msg("importing reusable staged state snapshot cells")

		stagedPath := staged.path
		lazyRoot, err := n.decodeAndImportStagedStateCellTreeAs(ctx, block, storageBlock, staged, wantRootHash, hashKind)
		if err == nil {
			n.log.Info().
				Str("block", formatPersistentStateBlockRef(block, effectiveShard)).
				Uint64("cells", staged.cells).
				Str("path", stagedPath).
				Msg("reusable staged state snapshot switched to lazy celldb root")
			return staged, lazyRoot, nil
		}
		if errors.Is(err, context.Canceled) {
			return nil, nil, err
		}
		if errors.Is(err, errStateSnapshotInvalid) {
			staged.keep = false
			if cleanupErr := staged.cleanup(); cleanupErr != nil {
				n.log.Warn().
					Err(cleanupErr).
					Str("block", formatPersistentStateBlockRef(block, effectiveShard)).
					Str("path", staged.path).
					Msg("failed to remove invalid staged state snapshot file")
			}
			n.log.Warn().
				Err(err).
				Str("block", formatPersistentStateBlockRef(block, effectiveShard)).
				Str("path", path).
				Msg("removed invalid staged state snapshot file")
			continue
		}
		return nil, nil, err
	}

	return nil, nil, storage.ErrNotFound
}

func (n *Node) importStagedStateCellTree(ctx context.Context, storageBlock ton.BlockIDExt, staged *stagedStateFile, root *cell.Cell, parsedCells []cell.Cell) (*cell.Cell, error) {
	if n.storage == nil || root == nil {
		return root, nil
	}

	lazyRoot, err := n.storage.ImportStateCellTree(ctx, storageBlock, root, parsedCells, staged.cells)
	if err != nil {
		return nil, err
	}
	staged.lazyRoot = lazyRoot
	return lazyRoot, nil
}

func (n *Node) cacheImportedStagedBlockState(block ton.BlockIDExt, staged *stagedStateFile, root *cell.Cell, stateRootHash []byte) error {
	if root == nil {
		return nil
	}

	state, err := storage.ParseStateCell(&block, root, nil, stateRootHash, nil)
	if err != nil {
		return fmt.Errorf("%w: %w", errStateSnapshotInvalid, err)
	}
	state.CellsCount = staged.cells
	state.StateFileHash = staged.fileHash
	state.DownloadedAt = time.Now()

	staged.state = storage.CloneBlockState(state)

	n.log.Info().
		Str("peer", staged.peerAddr).
		Str("block", formatPersistentStateBlockRef(block, staged.effectiveShard)).
		Uint64("cells", state.CellsCount).
		Str("state_root_hash", hex.EncodeToString(state.StateRootHash)).
		Str("state_file_hash", hex.EncodeToString(state.StateFileHash)).
		Msg("imported state snapshot metadata prepared")
	return nil
}

func (n *Node) savePersistentStateFile(block ton.BlockIDExt, master ton.BlockIDExt, staged *stagedStateFile, stateRootHash []byte) error {
	if staged == nil || staged.path == "" || staged.persisted {
		return nil
	}

	writer, _ := n.peerStorage.(storage.PeerServingStorageWriter)
	if writer == nil {
		return nil
	}

	if err := writer.SavePersistentStateFile(&storage.PersistentStateFile{
		Block:            block,
		MasterchainBlock: master,
		EffectiveShard:   staged.effectiveShard,
		Ref: &storage.ArtifactRef{
			Path:   staged.path,
			Offset: 0,
			Size:   staged.size,
		},
		FileHash:      bytes.Clone(staged.fileHash),
		StateRootHash: bytes.Clone(stateRootHash),
		CellsCount:    staged.cells,
	}); err != nil {
		return err
	}

	staged.keep = true
	staged.persisted = true
	return nil
}

type persistentStateSnapshotArtifact struct {
	node          *Node
	block         ton.BlockIDExt
	master        ton.BlockIDExt
	stateRootHash []byte
	staged        *stagedStateFile
}

func (a *persistentStateSnapshotArtifact) Block() ton.BlockIDExt {
	return a.block
}

func (a *persistentStateSnapshotArtifact) Decode(ctx context.Context) (*storage.BlockState, error) {
	return a.decode(ctx, nil)
}

func (a *persistentStateSnapshotArtifact) ImportCells(ctx context.Context, cells storage.StateCellTreeImporter) (*storage.BlockState, error) {
	return a.decode(ctx, cells)
}

func (a *persistentStateSnapshotArtifact) decode(ctx context.Context, cells storage.StateCellTreeImporter) (*storage.BlockState, error) {
	a.node.log.Info().
		Str("peer", a.staged.peerAddr).
		Str("block", formatPersistentStateBlockRef(a.block, a.staged.effectiveShard)).
		Str("size", formatByteSize(a.staged.size)).
		Str("path", a.staged.path).
		Msg("decoding state snapshot from temp file")

	if a.staged.state != nil {
		if err := a.node.savePersistentStateFile(a.block, a.master, a.staged, a.stateRootHash); err != nil {
			return nil, fmt.Errorf("store persistent state file: %w", err)
		}
		return storage.CloneBlockState(a.staged.state), nil
	}

	root, parsedCells, err := a.staged.rootCells()
	if err != nil {
		a.node.log.Error().
			Err(err).
			Str("peer", a.staged.peerAddr).
			Str("block", formatPersistentStateBlockRef(a.block, a.staged.effectiveShard)).
			Int64("size", a.staged.size).
			Str("path", a.staged.path).
			Str("prefix", firstBytesHex(a.staged.prefix, 16)).
			Msg("failed to parse staged state snapshot BOC")
		return nil, fmt.Errorf("%w: %w", errStateSnapshotInvalid, err)
	}

	state, err := storage.ParseStateCell(&a.block, root, nil, a.stateRootHash, nil)
	if err != nil {
		a.node.log.Debug().
			Err(err).
			Str("peer", a.staged.peerAddr).
			Str("block", formatPersistentStateBlockRef(a.block, a.staged.effectiveShard)).
			Int64("size", a.staged.size).
			Str("state_file_hash", hex.EncodeToString(a.staged.fileHash)).
			Str("prefix", firstBytesHex(a.staged.prefix, 16)).
			Msg("failed to parse downloaded state snapshot")
		return nil, fmt.Errorf("%w: %w", errStateSnapshotInvalid, err)
	}
	state.CellsCount = a.staged.cells
	state.StateFileHash = a.staged.fileHash
	state.DownloadedAt = time.Now()

	a.node.log.Info().
		Str("peer", a.staged.peerAddr).
		Str("block", formatPersistentStateBlockRef(a.block, a.staged.effectiveShard)).
		Str("size", formatByteSize(a.staged.size)).
		Uint64("cells", state.CellsCount).
		Str("state_root_hash", hex.EncodeToString(state.StateRootHash)).
		Str("state_file_hash", hex.EncodeToString(a.staged.fileHash)).
		Msg("state snapshot parsed")
	state, err = importBlockStateCells(ctx, cells, state, parsedCells)
	if err != nil {
		return nil, err
	}
	if err = a.node.savePersistentStateFile(a.block, a.master, a.staged, a.stateRootHash); err != nil {
		return nil, fmt.Errorf("store persistent state file: %w", err)
	}
	return state, nil
}

func (a *persistentStateSnapshotArtifact) Cleanup() error {
	return a.staged.cleanup()
}

type splitPersistentStatePartArtifact struct {
	part   splitStatePart
	staged *stagedStateFile
}

type splitPersistentStateSnapshotArtifact struct {
	node          *Node
	block         ton.BlockIDExt
	master        ton.BlockIDExt
	stateRootHash []byte
	header        *downloadedSplitStateHeader
	parts         []splitPersistentStatePartArtifact
}

func (a *splitPersistentStateSnapshotArtifact) Block() ton.BlockIDExt {
	return a.block
}

func (a *splitPersistentStateSnapshotArtifact) Decode(ctx context.Context) (*storage.BlockState, error) {
	return a.decode(ctx, nil)
}

func (a *splitPersistentStateSnapshotArtifact) ImportCells(ctx context.Context, cells storage.StateCellTreeImporter) (*storage.BlockState, error) {
	return a.decode(ctx, cells)
}

func (a *splitPersistentStateSnapshotArtifact) decode(ctx context.Context, cells storage.StateCellTreeImporter) (*storage.BlockState, error) {
	totalCells := a.header.cells
	accounts, err := cell.NewAugDict(256, shardAccountsAugmentation{})
	if err != nil {
		return nil, err
	}
	if a.header != nil && a.header.staged != nil {
		if err = a.node.savePersistentStateFile(a.block, a.master, a.header.staged, nil); err != nil {
			return nil, fmt.Errorf("store split persistent state header file: %w", err)
		}
	}

	for i, part := range a.parts {
		a.node.log.Info().
			Str("peer", part.staged.peerAddr).
			Str("block", formatPersistentStateBlockRef(a.block, int64(part.part.effectiveShard))).
			Str("masterchain", formatBlockRef(a.master)).
			Int("part", i+1).
			Int("parts", len(a.parts)).
			Str("size", formatByteSize(part.staged.size)).
			Str("path", part.staged.path).
			Msg("decoding split persistent state part from temp file")

		root, parsedCells, err := part.staged.rootCells()
		if err != nil {
			return nil, fmt.Errorf("%w: parse split state part %d boc: %w", errStateSnapshotInvalid, i+1, err)
		}

		partRootHash := root.HashKey()
		if !bytes.Equal(partRootHash[:], part.part.rootHash) {
			err = fmt.Errorf("split state part %d root hash mismatch", i+1)
			a.node.log.Error().
				Err(err).
				Str("peer", part.staged.peerAddr).
				Str("block", formatPersistentStateBlockRef(a.block, int64(part.part.effectiveShard))).
				Int("part", i+1).
				Int64("size", part.staged.size).
				Str("state_file_hash", hex.EncodeToString(part.staged.fileHash)).
				Msg("split persistent state part hash mismatch")
			return nil, fmt.Errorf("%w: %w", errStateSnapshotInvalid, err)
		}

		if cells != nil && !root.IsLazy() {
			partBlock := splitStatePartStorageBlock(a.block, part.part)
			lazyRoot, err := cells.ImportStateCellTree(ctx, partBlock, root, parsedCells, part.staged.cells)
			if err != nil {
				return nil, fmt.Errorf("import split state part %d cells: %w", i+1, err)
			}
			root = lazyRoot
			parsedCells = nil
			runtime.GC()

			a.node.log.Info().
				Str("peer", part.staged.peerAddr).
				Str("block", formatPersistentStateBlockRef(a.block, int64(part.part.effectiveShard))).
				Int("part", i+1).
				Int("parts", len(a.parts)).
				Uint64("cells", part.staged.cells).
				Msg("split persistent state part switched to lazy celldb root")
		}

		stopProgress := a.startSplitStatePartProgress(ctx, part, i, "load_accounts")
		partAccounts, err := loadSplitStatePartAccounts(root)
		stopProgress()
		if err != nil {
			return nil, fmt.Errorf("%w: parse split state part %d accounts: %w", errStateSnapshotInvalid, i+1, err)
		}

		stopProgress = a.startSplitStatePartProgress(ctx, part, i, "merge_accounts")
		merged, err := accounts.CombineWith(partAccounts)
		stopProgress()
		if err != nil {
			return nil, fmt.Errorf("%w: merge split state part %d accounts: %w", errStateSnapshotInvalid, i+1, err)
		}
		if !merged {
			return nil, fmt.Errorf("%w: duplicate account in split state part %d", errStateSnapshotInvalid, i+1)
		}
		if err = a.node.savePersistentStateFile(a.block, a.master, part.staged, nil); err != nil {
			return nil, fmt.Errorf("store split persistent state part %d file: %w", i+1, err)
		}

		totalCells += part.staged.cells
		a.node.log.Info().
			Str("peer", part.staged.peerAddr).
			Str("block", formatPersistentStateBlockRef(a.block, int64(part.part.effectiveShard))).
			Int("part", i+1).
			Int("parts", len(a.parts)).
			Str("size", formatByteSize(part.staged.size)).
			Uint64("cells", part.staged.cells).
			Str("state_file_hash", hex.EncodeToString(part.staged.fileHash)).
			Msg("split persistent state part parsed")
	}

	a.node.log.Info().
		Str("block", formatBlockRef(a.block)).
		Str("masterchain", formatBlockRef(a.master)).
		Int("parts", len(a.parts)).
		Uint64("cells", totalCells).
		Msg("merging split persistent state parts")

	stateRoot, err := mergeSplitStateAccounts(a.header.state, accounts)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errStateSnapshotInvalid, err)
	}
	stateRootHash := stateRoot.HashKey(0)
	if !bytes.Equal(stateRootHash[:], a.stateRootHash) {
		return nil, fmt.Errorf("%w: split state root hash mismatch", errStateSnapshotInvalid)
	}

	if cells != nil {
		lazyRoot, err := cells.ImportStateCellTree(ctx, a.block, stateRoot, nil, totalCells)
		if err != nil {
			return nil, fmt.Errorf("import merged split state cells: %w", err)
		}
		stateRoot = lazyRoot
		runtime.GC()

		a.node.log.Info().
			Str("block", formatBlockRef(a.block)).
			Str("masterchain", formatBlockRef(a.master)).
			Uint64("cells", totalCells).
			Msg("merged split state switched to lazy celldb root")
	}

	state, err := storage.ParseStateCell(&a.block, stateRoot, nil, a.stateRootHash, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errStateSnapshotInvalid, err)
	}
	state.CellsCount = totalCells
	state.DownloadedAt = time.Now()

	a.node.log.Info().
		Str("block", formatBlockRef(a.block)).
		Uint64("cells", state.CellsCount).
		Str("state_root_hash", hex.EncodeToString(state.StateRootHash)).
		Msg("split state snapshot parsed")
	return state, nil
}

func importBlockStateCells(ctx context.Context, cells storage.StateCellTreeImporter, state *storage.BlockState, parsedCells []cell.Cell) (*storage.BlockState, error) {
	if cells == nil || state == nil || state.Cell == nil {
		return state, nil
	}
	if state.Cell.IsLazy() {
		return state, nil
	}

	lazyRoot, err := cells.ImportStateCellTree(ctx, state.Block, state.Cell, parsedCells, state.CellsCount)
	if err != nil {
		return nil, err
	}
	state.Cell = nil
	state.Parsed = nil
	parsedCells = nil
	runtime.GC()

	lazyState, err := storage.ParseStateCell(&state.Block, lazyRoot, nil, state.StateRootHash, nil)
	if err != nil {
		return nil, err
	}
	lazyState.StateFileHash = state.StateFileHash
	lazyState.CellsCount = state.CellsCount
	lazyState.DownloadedAt = state.DownloadedAt
	return lazyState, nil
}

func (a *splitPersistentStateSnapshotArtifact) startSplitStatePartProgress(ctx context.Context, part splitPersistentStatePartArtifact, index int, phase string) func() {
	done := make(chan struct{})
	started := time.Now()

	go func() {
		ticker := time.NewTicker(stateArtifactProgressInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				a.node.log.Info().
					Str("peer", part.staged.peerAddr).
					Str("block", formatPersistentStateBlockRef(a.block, int64(part.part.effectiveShard))).
					Str("masterchain", formatBlockRef(a.master)).
					Int("part", index+1).
					Int("parts", len(a.parts)).
					Str("phase", phase).
					Str("size", formatByteSize(part.staged.size)).
					Uint64("cells", part.staged.cells).
					Dur("elapsed", time.Since(started)).
					Msg("split persistent state part decode progress")
			case <-done:
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	return func() {
		close(done)
	}
}

func (a *splitPersistentStateSnapshotArtifact) Cleanup() error {
	var errs []error
	if a.header != nil && a.header.staged != nil {
		if err := a.header.staged.cleanup(); err != nil {
			errs = append(errs, err)
		}
	}
	for _, part := range a.parts {
		if err := part.staged.cleanup(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func persistentStateFileName(block ton.BlockIDExt, master ton.BlockIDExt, effectiveShard int64) (string, error) {
	hash, err := persistentStateFileKeyHash(block, master, effectiveShard)
	if err != nil {
		return "", err
	}
	hashHex := hex.EncodeToString(hash)

	switch {
	case effectiveShard == 0:
		return fmt.Sprintf("state_%d_%d_%x_%s", master.SeqNo, block.Workchain, uint64(block.Shard), hashHex), nil
	case effectiveShard == block.Shard:
		return fmt.Sprintf("statesplit_%d_%d_%016x_%s", master.SeqNo, block.Workchain, uint64(block.Shard), hashHex), nil
	default:
		return fmt.Sprintf("stateaccount_%d_%d_%016x_%016x_%s", master.SeqNo, block.Workchain, uint64(block.Shard), uint64(effectiveShard), hashHex), nil
	}
}

func persistentStateFileKeyHash(block ton.BlockIDExt, master ton.BlockIDExt, effectiveShard int64) ([]byte, error) {
	switch {
	case effectiveShard == 0:
		return tl.Hash(DbFileDBKeyPersistentStateFile{
			BlockID:            block,
			MasterchainBlockID: master,
		})
	case effectiveShard == block.Shard:
		return tl.Hash(DbFileDBKeySplitPersistentStateFile{
			BlockID:            block,
			MasterchainBlockID: master,
		})
	default:
		return tl.Hash(DbFileDBKeySplitAccountStateFile{
			BlockID:            block,
			MasterchainBlockID: master,
			EffectiveShard:     effectiveShard,
		})
	}
}

func (n *Node) persistentStateFilePath(block ton.BlockIDExt, master ton.BlockIDExt, effectiveShard int64) (string, error) {
	name, err := persistentStateFileName(block, master, effectiveShard)
	if err != nil {
		return "", err
	}
	return filepath.Join(n.stateDownloadDir, name), nil
}

func (n *Node) reusableStagedStateFiles(block ton.BlockIDExt, master ton.BlockIDExt, effectiveShard int64) ([]string, error) {
	if strings.TrimSpace(n.stateDownloadDir) == "" {
		return nil, nil
	}
	path, err := n.persistentStateFilePath(block, master, effectiveShard)
	if err != nil {
		return nil, err
	}
	if _, err = os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	return []string{path}, nil
}

func (n *Node) loadReusableStagedStateFile(path string, effectiveShard int64) (*stagedStateFile, error) {
	stat, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !stat.Mode().IsRegular() {
		return nil, fmt.Errorf("staged state path is not a regular file")
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()

	prefix := make([]byte, persistentStateReadPrefixLimit)
	nn, err := io.ReadFull(file, prefix)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, err
	}
	prefix = prefix[:nn]

	cellsCount, err := storage.BOCCellsCount(prefix)
	if err != nil {
		return nil, fmt.Errorf("%w: parse staged state boc header: %w", errStateSnapshotInvalid, err)
	}

	if _, err = file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	hash := sha256.New()
	if _, err = io.Copy(hash, file); err != nil {
		return nil, err
	}

	return &stagedStateFile{
		effectiveShard: effectiveShard,
		peerAddr:       "disk",
		path:           path,
		size:           stat.Size(),
		cells:          cellsCount,
		fileHash:       hash.Sum(nil),
		prefix:         prefix,
		keep:           true,
	}, nil
}

func (n *Node) removeInvalidReusableStateFile(block ton.BlockIDExt, effectiveShard int64, path string, cause error) {
	if removeErr := os.Remove(path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
		n.log.Warn().
			Err(removeErr).
			Str("block", formatPersistentStateBlockRef(block, effectiveShard)).
			Str("path", path).
			Msg("failed to remove invalid staged state snapshot file")
	}

	n.log.Warn().
		Err(cause).
		Str("block", formatPersistentStateBlockRef(block, effectiveShard)).
		Str("path", path).
		Msg("removed invalid staged state snapshot file")
}

func (n *Node) createStateDownloadFile(block ton.BlockIDExt, master ton.BlockIDExt, effectiveShard int64) (*os.File, string, string, error) {
	dir := n.stateDownloadDir
	if dir == "" {
		return nil, "", "", fmt.Errorf("state download dir is empty")
	}

	finalPath, err := n.persistentStateFilePath(block, master, effectiveShard)
	if err != nil {
		return nil, "", "", err
	}
	finalPath, err = filepath.Abs(finalPath)
	if err != nil {
		return nil, "", "", err
	}
	if err = os.MkdirAll(dir, 0o755); err != nil {
		return nil, "", "", err
	}
	name := filepath.Base(finalPath)
	file, err := os.CreateTemp(dir, name+stateDownloadTempSuffix+"-")
	if err != nil {
		return nil, "", "", err
	}
	path, err := filepath.Abs(file.Name())
	if err != nil {
		_ = file.Close()
		_ = os.Remove(file.Name())
		return nil, "", "", err
	}
	if !strings.Contains(filepath.Base(path), stateDownloadTempSuffix) {
		_ = file.Close()
		_ = os.Remove(file.Name())
		return nil, "", "", fmt.Errorf("state download temp file has unexpected name: %s", path)
	}
	return file, path, finalPath, nil
}

func (d persistentStateSnapshotDownloader) stagePersistentStateFile(
	ctx context.Context,
	candidate persistentStateCandidate,
	seed *persistentStateChunkSeed,
	chunkLimiter chan struct{},
) (*stagedStateFile, error) {
	n := d.node

	reader, err := d.newPersistentStateChunkReader(ctx, candidate.peer, candidate.id, candidate.size, candidate.workers, chunkLimiter, seed)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	file, path, finalPath, err := n.createStateDownloadFile(d.block, d.master, candidate.id.EffectiveShard)
	if err != nil {
		return nil, err
	}
	success := false
	closed := false
	defer func() {
		if !closed {
			_ = file.Close()
		}
		if !success {
			_ = os.Remove(path)
		}
	}()

	n.log.Info().
		Str("peer", candidate.peer.addr).
		Str("block", formatPersistentStateBlockRef(d.block, candidate.id.EffectiveShard)).
		Str("masterchain", formatBlockRef(d.master)).
		Str("size", formatByteSize(candidate.size)).
		Str("path", path).
		Msg("streaming state snapshot into temp file")

	written, err := io.Copy(file, reader)
	if err != nil {
		if downloadErr := reader.DownloadErr(); downloadErr != nil {
			return nil, downloadErr
		}
		return nil, err
	}
	if written != candidate.size {
		return nil, fmt.Errorf("persistent state size mismatch: wrote=%d want=%d", written, candidate.size)
	}
	if err = file.Close(); err != nil {
		return nil, err
	}
	closed = true
	if _, err = os.Stat(finalPath); err == nil {
		_ = os.Remove(path)
		return n.loadReusableStagedStateFile(finalPath, candidate.id.EffectiveShard)
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if err = os.Rename(path, finalPath); err != nil {
		return nil, err
	}
	path = finalPath

	prefix := reader.Prefix()
	cellsCount, err := storage.BOCCellsCount(prefix)
	if err != nil {
		return nil, fmt.Errorf("%w: parse state boc header: %w", errStateSnapshotInvalid, err)
	}

	success = true
	n.log.Info().
		Str("peer", candidate.peer.addr).
		Str("block", formatPersistentStateBlockRef(d.block, candidate.id.EffectiveShard)).
		Str("size", formatByteSize(candidate.size)).
		Str("path", path).
		Msg("state snapshot stored to states file")

	return &stagedStateFile{
		effectiveShard: candidate.id.EffectiveShard,
		peerAddr:       candidate.peer.addr,
		path:           path,
		size:           candidate.size,
		cells:          cellsCount,
		fileHash:       reader.FileHash(),
		prefix:         prefix,
	}, nil
}

func removeIncompleteStateDownloadFiles(dir string) error {
	if strings.TrimSpace(dir) == "" {
		return nil
	}
	matches, err := filepath.Glob(filepath.Join(dir, "*"+stateDownloadTempSuffix+"*"))
	if err != nil {
		return err
	}

	var errs []error
	for _, path := range matches {
		if err = os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
