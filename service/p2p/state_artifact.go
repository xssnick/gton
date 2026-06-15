package p2p

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	tnstate "github.com/xssnick/gton/service/state"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	stateFileTempSuffix           = ".part"
	stateArtifactProgressInterval = 5 * time.Second
)

type zeroStateSnapshotArtifact struct {
	block  ton.BlockIDExt
	data   []byte
	writer storage.PeerServingStorageWriter
	stored bool
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
	state, err = importBlockStateCells(ctx, cells, state)
	if err != nil {
		return nil, err
	}
	if err = a.save(); err != nil {
		return nil, err
	}
	return state, nil
}

func (a *zeroStateSnapshotArtifact) save() error {
	if a.writer == nil || a.stored {
		return nil
	}
	if err := a.writer.SaveZeroState(a.block, a.data, nil); err != nil {
		return fmt.Errorf("store zero state: %w", err)
	}
	a.stored = true
	return nil
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
	fileHash       []byte
	prefix         []byte
	lazyRoot       *cell.Cell
	state          *storage.BlockState
	keep           bool
	persisted      bool
}

type PersistentStateFileLoader func(effectiveShard int64) (*storage.PersistentStateFile, error)

func NewPersistentStateSnapshotArtifactFromFile(node *Node, file *storage.PersistentStateFile) (storage.DownloadedState, error) {
	staged, err := stagedStateFileFromPersistentStateFile(file)
	if err != nil {
		return nil, err
	}
	stateRootHash := file.StateRootHash
	if len(stateRootHash) == 0 {
		return nil, fmt.Errorf("persistent state file root hash is empty")
	}
	return &persistentStateSnapshotArtifact{
		node:          node,
		block:         file.Block,
		master:        file.MasterchainBlock,
		stateRootHash: bytes.Clone(stateRootHash),
		staged:        staged,
	}, nil
}

func NewSplitPersistentStateSnapshotArtifactFromStoredFiles(
	ctx context.Context,
	node *Node,
	block ton.BlockIDExt,
	master ton.BlockIDExt,
	splitDepth uint32,
	stateRootHash []byte,
	loadFile PersistentStateFileLoader,
) (storage.DownloadedState, error) {
	if len(stateRootHash) == 0 {
		return nil, fmt.Errorf("persistent state root hash is empty")
	}

	headerFile, err := loadFile(block.Shard)
	if err != nil {
		return nil, fmt.Errorf("load split persistent state header file: %w", err)
	}
	headerStaged, err := stagedStateFileFromPersistentStateFile(headerFile)
	if err != nil {
		return nil, err
	}

	downloader := persistentStateSnapshotDownloader{
		node:          node,
		block:         block,
		master:        master,
		stateRootHash: bytes.Clone(stateRootHash),
	}
	header, err := downloader.parseStagedSplitHeader(splitDepth, headerStaged)
	runtime.GC()
	if err != nil {
		return nil, err
	}

	parts := make([]splitPersistentStatePartArtifact, 0, len(header.parts))
	for _, part := range header.parts {
		if err = ctx.Err(); err != nil {
			return nil, err
		}

		file, err := loadFile(int64(part.effectiveShard))
		if err != nil {
			return nil, fmt.Errorf("load split persistent state part %016x file: %w", part.effectiveShard, err)
		}
		staged, err := stagedStateFileFromPersistentStateFile(file)
		if err != nil {
			return nil, err
		}
		parts = append(parts, splitPersistentStatePartArtifact{
			part:   part,
			staged: staged,
		})
	}

	return &splitPersistentStateSnapshotArtifact{
		node:          node,
		block:         block,
		master:        master,
		stateRootHash: bytes.Clone(stateRootHash),
		header:        header,
		parts:         parts,
	}, nil
}

func stagedStateFileFromPersistentStateFile(file *storage.PersistentStateFile) (*stagedStateFile, error) {
	if file.Ref.Path == "" {
		return nil, fmt.Errorf("persistent state file path is empty")
	}
	if file.Ref.Offset != 0 {
		return nil, fmt.Errorf("persistent state file offset %d is not supported for local import", file.Ref.Offset)
	}
	if file.Ref.Size <= 0 {
		return nil, fmt.Errorf("persistent state file size is invalid")
	}
	return &stagedStateFile{
		effectiveShard: file.EffectiveShard,
		peerAddr:       "local",
		path:           file.Ref.Path,
		size:           file.Ref.Size,
		fileHash:       bytes.Clone(file.FileHash),
		keep:           true,
		persisted:      true,
	}, nil
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

func (f *stagedStateFile) decodeRootCells(options cell.BOCParseOptions) (*cell.Cell, error) {
	file, err := f.open()
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()

	roots, _, err := cell.FromBOCMultiRootReader(file, options)
	if err != nil {
		return nil, err
	}
	root, err := singleStateBOCRoot(roots)
	if err != nil {
		return nil, err
	}
	return root, nil
}

func (f *stagedStateFile) openBOCView(options cell.BOCViewOptions) (*cell.BOCView, *os.File, error) {
	file, err := f.open()
	if err != nil {
		return nil, nil, err
	}

	stat, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, nil, err
	}

	view, err := cell.OpenBOCView(file, stat.Size(), options)
	if err != nil {
		_ = file.Close()
		return nil, nil, err
	}
	return view, file, nil
}

func (f *stagedStateFile) rootCells(options cell.BOCParseOptions) (*cell.Cell, error) {
	if f != nil && f.lazyRoot != nil {
		return f.lazyRoot, nil
	}
	return f.decodeRootCells(options)
}

func stateBOCParseOptions() cell.BOCParseOptions {
	return cell.BOCParseOptions{
		DisableLazyCache: true,
	}
}

func stateBOCViewOptions(cells storage.StateCellTreeImporter) cell.BOCViewOptions {
	trustedHashes := false
	if cells != nil {
		trustedHashes = cells.TrustImportedStateCellHashes()
	}
	return cell.BOCViewOptions{
		TrustedHashes: trustedHashes,
		RequireIndex:  true,
		ValidateCRC:   true,
	}
}

func singleStateBOCRoot(roots []*cell.Cell) (*cell.Cell, error) {
	if len(roots) != 1 {
		return nil, fmt.Errorf("boc should contain exactly one root, got %d", len(roots))
	}
	return roots[0], nil
}

func singleStateBOCViewRootMeta(view *cell.BOCView) (cell.BOCCellMeta, error) {
	roots := view.Roots()
	if len(roots) != 1 {
		return cell.BOCCellMeta{}, fmt.Errorf("boc should contain exactly one root, got %d", len(roots))
	}
	meta, err := view.CellMeta(roots[0])
	if err != nil {
		return cell.BOCCellMeta{}, err
	}
	return meta, nil
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
			Msg("staged state snapshot is ready, waiting for cell import slot")

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case n.stateCellImportSlot <- struct{}{}:
			return func() { <-n.stateCellImportSlot }, nil
		}
	}
}

func (n *Node) acquireStateSplitPartDecodeSlot(ctx context.Context, block ton.BlockIDExt, staged *stagedStateFile) (func(), error) {
	if n.stateSplitPartDecodeSlot == nil {
		return func() {}, nil
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case n.stateSplitPartDecodeSlot <- struct{}{}:
		return func() { <-n.stateSplitPartDecodeSlot }, nil
	default:
		n.log.Info().
			Str("block", formatPersistentStateBlockRef(block, staged.effectiveShard)).
			Str("size", formatByteSize(staged.size)).
			Msg("staged split state part is ready, waiting for decode slot")

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case n.stateSplitPartDecodeSlot <- struct{}{}:
			return func() { <-n.stateSplitPartDecodeSlot }, nil
		}
	}
}

type stagedStateHashKind uint8

const (
	stagedStateRootHash stagedStateHashKind = iota
	stagedStateConcreteHash
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

	view, file, err := staged.openBOCView(stateBOCViewOptions(n.storage))
	if err != nil {
		return nil, fmt.Errorf("%w: parse staged state boc: %w", errStateSnapshotInvalid, err)
	}
	defer func() { _ = file.Close() }()

	rootMeta, err := singleStateBOCViewRootMeta(view)
	if err != nil {
		return nil, fmt.Errorf("%w: parse staged state boc: %w", errStateSnapshotInvalid, err)
	}
	if hashKind == stagedStateRootHash && rootMeta.Hash != rootMeta.HashAtLevel(0) {
		return nil, fmt.Errorf("%w: staged state root is not level-0 for %s", errStateSnapshotInvalid, formatBlockRef(logBlock))
	}
	if len(wantRootHash) > 0 {
		rootHash := stagedStateMetaHash(rootMeta, hashKind)
		if !bytes.Equal(rootHash[:], wantRootHash) {
			return nil, fmt.Errorf("%w: staged state root hash mismatch: got=%s want=%s", errStateSnapshotInvalid, hex.EncodeToString(rootHash[:]), hex.EncodeToString(wantRootHash))
		}
	}

	lazyRoot, err := n.importStagedStateBOCView(ctx, storageBlock, staged, view)
	runtime.GC()
	if err != nil {
		return nil, err
	}
	return lazyRoot, nil
}

func (n *Node) decodeAndImportSplitPartStagedStateCellTree(ctx context.Context, logBlock ton.BlockIDExt, storageBlock ton.BlockIDExt, staged *stagedStateFile, wantRootHash []byte) (*cell.Cell, error) {
	releaseDecode, err := n.acquireStateSplitPartDecodeSlot(ctx, logBlock, staged)
	if err != nil {
		return nil, err
	}
	decodeReleased := false
	defer func() {
		if !decodeReleased {
			releaseDecode()
		}
	}()

	view, file, err := staged.openBOCView(stateBOCViewOptions(n.storage))
	if err != nil {
		return nil, fmt.Errorf("%w: parse staged state boc: %w", errStateSnapshotInvalid, err)
	}
	defer func() { _ = file.Close() }()

	rootMeta, err := singleStateBOCViewRootMeta(view)
	if err != nil {
		return nil, fmt.Errorf("%w: parse staged state boc: %w", errStateSnapshotInvalid, err)
	}
	if len(wantRootHash) > 0 {
		rootHash := stagedStateMetaHash(rootMeta, stagedStateConcreteHash)
		if !bytes.Equal(rootHash[:], wantRootHash) {
			return nil, fmt.Errorf("%w: staged state root hash mismatch: got=%s want=%s", errStateSnapshotInvalid, hex.EncodeToString(rootHash[:]), hex.EncodeToString(wantRootHash))
		}
	}

	releaseImport, err := n.acquireStateCellImportSlot(ctx, logBlock, staged)
	if err != nil {
		return nil, err
	}
	releaseDecode()
	decodeReleased = true
	defer releaseImport()

	lazyRoot, err := n.importStagedStateBOCView(ctx, storageBlock, staged, view)
	runtime.GC()
	if err != nil {
		return nil, err
	}
	return lazyRoot, nil
}

func stagedStateMetaHash(meta cell.BOCCellMeta, kind stagedStateHashKind) cell.Hash {
	if kind == stagedStateConcreteHash {
		return meta.Hash
	}
	return meta.HashAtLevel(0)
}

func (n *Node) tryImportReusableStagedStateFile(ctx context.Context, block ton.BlockIDExt, master ton.BlockIDExt, effectiveShard int64, wantRootHash []byte) (*stagedStateFile, *cell.Cell, error) {
	return n.tryImportReusableStagedStateFileAs(ctx, block, master, blockWithEffectiveShard(block, effectiveShard), effectiveShard, wantRootHash, stagedStateRootHash)
}

func (n *Node) tryImportReusableStagedStateFileAs(ctx context.Context, block ton.BlockIDExt, master ton.BlockIDExt, storageBlock ton.BlockIDExt, effectiveShard int64, wantRootHash []byte, hashKind stagedStateHashKind) (*stagedStateFile, *cell.Cell, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}

	staged, err := n.loadReusablePersistentStateFile(ctx, block, master, effectiveShard)
	if err != nil {
		return nil, nil, err
	}

	n.log.Info().
		Str("block", formatPersistentStateBlockRef(block, effectiveShard)).
		Str("size", formatByteSize(staged.size)).
		Str("path", staged.path).
		Msg("importing reusable staged state snapshot cells")

	stagedPath := staged.path
	lazyRoot, err := n.decodeAndImportStagedStateCellTreeAs(ctx, block, storageBlock, staged, wantRootHash, hashKind)
	if err == nil {
		n.log.Info().
			Str("block", formatPersistentStateBlockRef(block, effectiveShard)).
			Str("path", stagedPath).
			Msg("reusable staged state snapshot switched to lazy celldb root")
		return staged, lazyRoot, nil
	}
	if errors.Is(err, context.Canceled) {
		return nil, nil, err
	}
	return nil, nil, err
}

func (n *Node) importStagedStateBOCView(ctx context.Context, storageBlock ton.BlockIDExt, staged *stagedStateFile, view *cell.BOCView) (*cell.Cell, error) {
	lazyRoot, err := n.storage.ImportStateBOCView(ctx, storageBlock, view)
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
	state.StateFileHash = staged.fileHash

	staged.state = storage.CloneBlockState(state)

	n.log.Info().
		Str("peer", staged.peerAddr).
		Str("block", formatPersistentStateBlockRef(block, staged.effectiveShard)).
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

	if cells != nil {
		return a.decodeAndImportBOCView(ctx, cells)
	}

	root, err := a.staged.rootCells(stateBOCParseOptions())
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
	state.StateFileHash = a.staged.fileHash

	a.node.log.Info().
		Str("peer", a.staged.peerAddr).
		Str("block", formatPersistentStateBlockRef(a.block, a.staged.effectiveShard)).
		Str("size", formatByteSize(a.staged.size)).
		Str("state_root_hash", hex.EncodeToString(state.StateRootHash)).
		Str("state_file_hash", hex.EncodeToString(a.staged.fileHash)).
		Msg("state snapshot parsed")
	state, err = importBlockStateCells(ctx, cells, state)
	if err != nil {
		return nil, err
	}
	if err = a.node.savePersistentStateFile(a.block, a.master, a.staged, a.stateRootHash); err != nil {
		return nil, fmt.Errorf("store persistent state file: %w", err)
	}
	return state, nil
}

func (a *persistentStateSnapshotArtifact) decodeAndImportBOCView(ctx context.Context, cells storage.StateCellTreeImporter) (*storage.BlockState, error) {
	view, file, err := a.staged.openBOCView(stateBOCViewOptions(cells))
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
	defer func() { _ = file.Close() }()

	rootMeta, err := singleStateBOCViewRootMeta(view)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errStateSnapshotInvalid, err)
	}
	rootHash := rootMeta.HashAtLevel(0)
	if rootMeta.Hash != rootHash {
		return nil, fmt.Errorf("%w: state snapshot root is not level-0 for %s", errStateSnapshotInvalid, formatBlockRef(a.block))
	}
	if len(a.stateRootHash) > 0 && !bytes.Equal(rootHash[:], a.stateRootHash) {
		a.node.log.Debug().
			Str("peer", a.staged.peerAddr).
			Str("block", formatPersistentStateBlockRef(a.block, a.staged.effectiveShard)).
			Int64("size", a.staged.size).
			Str("state_file_hash", hex.EncodeToString(a.staged.fileHash)).
			Str("prefix", firstBytesHex(a.staged.prefix, 16)).
			Msg("downloaded state snapshot root hash mismatch")
		return nil, fmt.Errorf("%w: state root hash mismatch for %s", errStateSnapshotInvalid, formatBlockRef(a.block))
	}

	state := &storage.BlockState{
		Block:         a.block,
		StateRootHash: bytes.Clone(rootHash[:]),
		StateFileHash: a.staged.fileHash,
	}

	a.node.log.Info().
		Str("peer", a.staged.peerAddr).
		Str("block", formatPersistentStateBlockRef(a.block, a.staged.effectiveShard)).
		Str("size", formatByteSize(a.staged.size)).
		Str("state_root_hash", hex.EncodeToString(state.StateRootHash)).
		Str("state_file_hash", hex.EncodeToString(a.staged.fileHash)).
		Msg("state snapshot parsed")

	lazyRoot, err := cells.ImportStateBOCView(ctx, a.block, view)
	if err != nil {
		return nil, err
	}
	a.staged.lazyRoot = lazyRoot
	runtime.GC()

	lazyState, err := storage.ParseStateCell(&a.block, lazyRoot, nil, state.StateRootHash, nil)
	if err != nil {
		return nil, err
	}
	lazyState.StateFileHash = state.StateFileHash
	if err = a.node.savePersistentStateFile(a.block, a.master, a.staged, a.stateRootHash); err != nil {
		return nil, fmt.Errorf("store persistent state file: %w", err)
	}
	return lazyState, nil
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
	accounts, err := tnstate.NewShardAccountsAugDict()
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

		root, err := a.splitStatePartRoot(ctx, cells, part, i)
		if err != nil {
			return nil, err
		}

		stopProgress := a.startSplitStatePartProgress(ctx, part, i, "load_accounts")
		partAccounts, err := tnstate.LoadShardAccountsRoot(root)
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

		a.node.log.Info().
			Str("peer", part.staged.peerAddr).
			Str("block", formatPersistentStateBlockRef(a.block, int64(part.part.effectiveShard))).
			Int("part", i+1).
			Int("parts", len(a.parts)).
			Str("size", formatByteSize(part.staged.size)).
			Str("state_file_hash", hex.EncodeToString(part.staged.fileHash)).
			Msg("split persistent state part parsed")
	}

	a.node.log.Info().
		Str("block", formatBlockRef(a.block)).
		Str("masterchain", formatBlockRef(a.master)).
		Int("parts", len(a.parts)).
		Msg("merging split persistent state parts")

	stateRoot, err := tnstate.MergeSplitStateAccounts(a.header.state, accounts)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errStateSnapshotInvalid, err)
	}
	if stateRoot.IsVirtualized() {
		return nil, fmt.Errorf("%w: merged split state root is virtualized", errStateSnapshotInvalid)
	}
	stateRootHash := stateRoot.HashKey(0)
	if !bytes.Equal(stateRootHash[:], a.stateRootHash) {
		return nil, fmt.Errorf("%w: split state root hash mismatch", errStateSnapshotInvalid)
	}

	if cells != nil {
		lazyRoot, err := cells.ImportStateCellTree(ctx, a.block, stateRoot, 0)
		if err != nil {
			return nil, fmt.Errorf("import merged split state cells: %w", err)
		}
		stateRoot = lazyRoot
		runtime.GC()

		a.node.log.Info().
			Str("block", formatBlockRef(a.block)).
			Str("masterchain", formatBlockRef(a.master)).
			Msg("merged split state switched to lazy celldb root")
	}

	state, err := storage.ParseStateCell(&a.block, stateRoot, nil, a.stateRootHash, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errStateSnapshotInvalid, err)
	}

	a.node.log.Info().
		Str("block", formatBlockRef(a.block)).
		Str("state_root_hash", hex.EncodeToString(state.StateRootHash)).
		Msg("split state snapshot parsed")
	return state, nil
}

func (a *splitPersistentStateSnapshotArtifact) splitStatePartRoot(ctx context.Context, cells storage.StateCellTreeImporter, part splitPersistentStatePartArtifact, index int) (*cell.Cell, error) {
	if cells != nil && !cells.ReuseImportedSplitStatePartCells() {
		return a.importSplitStatePartBOCView(ctx, cells, part, index)
	}

	if cells != nil && part.staged.lazyRoot != nil {
		return part.staged.lazyRoot, nil
	}

	if cells != nil {
		lazyRoot, err := a.node.loadImportedSplitStatePartRoot(ctx, a.block, part.part)
		if err == nil {
			part.staged.lazyRoot = lazyRoot
			a.node.log.Info().
				Str("block", formatPersistentStateBlockRef(a.block, int64(part.part.effectiveShard))).
				Int("part", index+1).
				Int("parts", len(a.parts)).
				Msg("using imported split persistent state part cells")
			return lazyRoot, nil
		}
		if !errors.Is(err, storage.ErrNotFound) {
			return nil, fmt.Errorf("load imported split state part %d cells: %w", index+1, err)
		}

		return a.importSplitStatePartBOCView(ctx, cells, part, index)
	}

	root, err := part.staged.rootCells(stateBOCParseOptions())
	if err != nil {
		return nil, fmt.Errorf("%w: parse split state part %d boc: %w", errStateSnapshotInvalid, index+1, err)
	}

	partRootHash := root.HashKey()
	if !bytes.Equal(partRootHash[:], part.part.rootHash) {
		err = fmt.Errorf("split state part %d root hash mismatch", index+1)
		a.node.log.Error().
			Err(err).
			Str("peer", part.staged.peerAddr).
			Str("block", formatPersistentStateBlockRef(a.block, int64(part.part.effectiveShard))).
			Int("part", index+1).
			Int64("size", part.staged.size).
			Str("state_file_hash", hex.EncodeToString(part.staged.fileHash)).
			Msg("split persistent state part hash mismatch")
		return nil, fmt.Errorf("%w: %w", errStateSnapshotInvalid, err)
	}
	return root, nil
}

func (a *splitPersistentStateSnapshotArtifact) importSplitStatePartBOCView(ctx context.Context, cells storage.StateCellTreeImporter, part splitPersistentStatePartArtifact, index int) (*cell.Cell, error) {
	view, file, err := part.staged.openBOCView(stateBOCViewOptions(cells))
	if err != nil {
		return nil, fmt.Errorf("%w: parse split state part %d boc: %w", errStateSnapshotInvalid, index+1, err)
	}
	defer func() { _ = file.Close() }()

	rootMeta, err := singleStateBOCViewRootMeta(view)
	if err != nil {
		return nil, fmt.Errorf("%w: parse split state part %d boc: %w", errStateSnapshotInvalid, index+1, err)
	}
	partRootHash := rootMeta.Hash
	if !bytes.Equal(partRootHash[:], part.part.rootHash) {
		err = fmt.Errorf("split state part %d root hash mismatch", index+1)
		a.node.log.Error().
			Err(err).
			Str("peer", part.staged.peerAddr).
			Str("block", formatPersistentStateBlockRef(a.block, int64(part.part.effectiveShard))).
			Int("part", index+1).
			Int64("size", part.staged.size).
			Str("state_file_hash", hex.EncodeToString(part.staged.fileHash)).
			Msg("split persistent state part hash mismatch")
		return nil, fmt.Errorf("%w: %w", errStateSnapshotInvalid, err)
	}

	partBlock := splitStatePartStorageBlock(a.block, part.part)
	lazyRoot, err := cells.ImportStateBOCView(ctx, partBlock, view)
	if err != nil {
		return nil, fmt.Errorf("import split state part %d cells: %w", index+1, err)
	}
	part.staged.lazyRoot = lazyRoot
	runtime.GC()

	a.node.log.Info().
		Str("peer", part.staged.peerAddr).
		Str("block", formatPersistentStateBlockRef(a.block, int64(part.part.effectiveShard))).
		Int("part", index+1).
		Int("parts", len(a.parts)).
		Msg("split persistent state part switched to lazy celldb root")
	return lazyRoot, nil
}

func (n *Node) loadImportedSplitStatePartRoot(ctx context.Context, block ton.BlockIDExt, part splitStatePart) (*cell.Cell, error) {
	if n.storage == nil {
		return nil, storage.ErrNotFound
	}
	if len(part.rootHash) != 32 {
		return nil, fmt.Errorf("split state part root hash size mismatch: %d", len(part.rootHash))
	}

	record, err := n.storage.CellRecord(ctx, part.rootHash)
	if err != nil {
		return nil, err
	}
	root, err := storage.LazyCellRecord(record, n.storage.LazyCellLoader())
	if err != nil {
		return nil, fmt.Errorf("create imported split state part lazy root: %w", err)
	}

	rootHash := root.HashKey()
	if !bytes.Equal(rootHash[:], part.rootHash) {
		return nil, fmt.Errorf("imported split state part cell hash mismatch: got=%x want=%x", rootHash[:], part.rootHash)
	}
	return root, nil
}

func importBlockStateCells(ctx context.Context, cells storage.StateCellTreeImporter, state *storage.BlockState) (*storage.BlockState, error) {
	if cells == nil || state == nil || state.Cell == nil {
		return state, nil
	}
	if state.Cell.IsLazy() {
		return state, nil
	}

	lazyRoot, err := cells.ImportStateCellTree(ctx, state.Block, state.Cell, 0)
	if err != nil {
		return nil, err
	}
	state.Cell = nil
	state.Parsed = nil
	runtime.GC()

	lazyState, err := storage.ParseStateCell(&state.Block, lazyRoot, nil, state.StateRootHash, nil)
	if err != nil {
		return nil, err
	}
	lazyState.StateFileHash = state.StateFileHash
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

func (n *Node) persistentStateFilePath(block ton.BlockIDExt, master ton.BlockIDExt, effectiveShard int64) (string, error) {
	name, err := storage.PersistentStateFileName(block, master, effectiveShard)
	if err != nil {
		return "", err
	}
	return filepath.Join(n.stateFilesDir, name), nil
}

func (n *Node) loadReusablePersistentStateFile(ctx context.Context, block ton.BlockIDExt, master ton.BlockIDExt, effectiveShard int64) (*stagedStateFile, error) {
	reader := n.reusablePersistentStateFileReader()
	if reader == nil {
		return nil, storage.ErrNotFound
	}

	file, err := reader.PersistentStateFile(ctx, block, master, effectiveShard)
	if err != nil {
		return nil, err
	}

	staged, err := stagedStateFileFromPersistentStateFile(file)
	if err != nil {
		return nil, err
	}

	stat, err := os.Stat(staged.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, storage.ErrNotFound
		}
		return nil, err
	}
	if !stat.Mode().IsRegular() {
		return nil, fmt.Errorf("%w: staged state path is not a regular file", errStateSnapshotInvalid)
	}
	if stat.Size() != staged.size {
		return nil, fmt.Errorf("%w: staged state file size mismatch: got=%d want=%d", errStateSnapshotInvalid, stat.Size(), staged.size)
	}
	return staged, nil
}

func (n *Node) reusablePersistentStateFileReader() storage.PersistentStateFileStorage {
	var source any = n.storage
	if n.peerStorage != nil {
		source = n.peerStorage
	}
	reader, _ := source.(storage.PersistentStateFileStorage)
	return reader
}

func (n *Node) createStateFile(block ton.BlockIDExt, master ton.BlockIDExt, effectiveShard int64) (*os.File, string, string, error) {
	dir := n.stateFilesDir
	if dir == "" {
		return nil, "", "", fmt.Errorf("state files dir is empty")
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
	file, err := os.CreateTemp(dir, name+stateFileTempSuffix+"-")
	if err != nil {
		return nil, "", "", err
	}
	path, err := filepath.Abs(file.Name())
	if err != nil {
		_ = file.Close()
		_ = os.Remove(file.Name())
		return nil, "", "", err
	}
	if !strings.Contains(filepath.Base(path), stateFileTempSuffix) {
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

	file, path, finalPath, err := n.createStateFile(d.block, d.master, candidate.id.EffectiveShard)
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
	replacedExisting := false
	if _, err = os.Stat(finalPath); err == nil {
		replacedExisting = true
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if err = os.Rename(path, finalPath); err != nil {
		return nil, err
	}
	path = finalPath

	success = true
	n.log.Info().
		Str("peer", candidate.peer.addr).
		Str("block", formatPersistentStateBlockRef(d.block, candidate.id.EffectiveShard)).
		Str("size", formatByteSize(candidate.size)).
		Str("path", path).
		Bool("replaced_existing", replacedExisting).
		Msg("state snapshot stored to states file")

	return &stagedStateFile{
		effectiveShard: candidate.id.EffectiveShard,
		peerAddr:       candidate.peer.addr,
		path:           path,
		size:           candidate.size,
		fileHash:       reader.FileHash(),
		prefix:         reader.Prefix(),
	}, nil
}

func removeIncompleteStateFiles(dir string) error {
	if strings.TrimSpace(dir) == "" {
		return nil
	}
	matches, err := filepath.Glob(filepath.Join(dir, "*"+stateFileTempSuffix+"*"))
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
