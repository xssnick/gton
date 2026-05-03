package p2p

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	tnstore "flexserver/service/storage"
	"flexserver/service/storage/pebblestore"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestFormatByteRate(t *testing.T) {
	tests := []struct {
		name    string
		bytes   int64
		elapsed time.Duration
		want    string
	}{
		{name: "zero bytes", bytes: 0, elapsed: time.Second, want: "0 B/s"},
		{name: "bytes", bytes: 512, elapsed: time.Second, want: "512 B/s"},
		{name: "kibibytes", bytes: 1536, elapsed: time.Second, want: "1.50 KB/s"},
		{name: "mebibytes", bytes: 5 << 20, elapsed: 2 * time.Second, want: "2.50 MB/s"},
		{name: "gibibytes", bytes: 3 << 30, elapsed: time.Second, want: "3.00 GB/s"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := formatByteRate(tt.bytes, tt.elapsed); got != tt.want {
				t.Fatalf("formatByteRate() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestCacheImportedStagedBlockStateDoesNotPersistMetadata(t *testing.T) {
	block := ton.BlockIDExt{
		Workchain: 0,
		Shard:     int64(0x0800000000000000),
		SeqNo:     63272132,
	}
	root := mustTestShardStateCell(t, block)
	rootHash := root.HashKey(0)
	cellHash := root.HashKey()
	raw := root.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false})
	fileHash := sha256.Sum256(raw)

	store := newTestPebbleStore(t)
	node := &Node{
		log:     zerolog.Nop(),
		storage: store,
	}
	staged := &stagedStateFile{
		effectiveShard: 0,
		peerAddr:       "127.0.0.1:30303",
		cells:          1,
		fileHash:       fileHash[:],
	}

	if err := node.cacheImportedStagedBlockState(block, staged, root, rootHash[:]); err != nil {
		t.Fatalf("cache imported staged block state: %v", err)
	}
	if staged.state == nil {
		t.Fatal("expected staged state to be cached")
	}

	if !bytes.Equal(staged.state.StateRootHash, rootHash[:]) {
		t.Fatalf("unexpected state root hash %x want %x", staged.state.StateRootHash, rootHash[:])
	}
	if !bytes.Equal(staged.state.StateCellHash, cellHash[:]) {
		t.Fatalf("unexpected state cell hash %x want %x", staged.state.StateCellHash, cellHash[:])
	}
	if staged.state.CellsCount != staged.cells {
		t.Fatalf("unexpected cells count %d want %d", staged.state.CellsCount, staged.cells)
	}
	if _, err := store.BlockState(context.Background(), block); !errors.Is(err, tnstore.ErrNotFound) {
		t.Fatalf("staged metadata should not be persisted by p2p, got %v", err)
	}
}

func TestPrepareStateDownloadDirPreservesCompletedFiles(t *testing.T) {
	dir := t.TempDir()
	completedPath := filepath.Join(dir, "state.boc")
	incompletePath := filepath.Join(dir, "state.boc"+stateDownloadTempSuffix)

	if err := os.WriteFile(completedPath, []byte("complete"), 0o644); err != nil {
		t.Fatalf("write completed file: %v", err)
	}
	if err := os.WriteFile(incompletePath, []byte("partial"), 0o644); err != nil {
		t.Fatalf("write incomplete file: %v", err)
	}

	got, owned, err := prepareStateDownloadDir(dir)
	if err != nil {
		t.Fatalf("prepare state download dir: %v", err)
	}
	if got != dir {
		t.Fatalf("unexpected dir %q want %q", got, dir)
	}
	if owned {
		t.Fatal("configured state download dir should not be owned")
	}
	if _, err = os.Stat(completedPath); err != nil {
		t.Fatalf("completed state file should be preserved: %v", err)
	}
	if _, err = os.Stat(incompletePath); !os.IsNotExist(err) {
		t.Fatalf("incomplete state file should be removed, got %v", err)
	}
}

func TestTryImportReusableStagedStateFile(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	block := ton.BlockIDExt{
		Workchain: 0,
		Shard:     int64(0x0800000000000000),
		SeqNo:     63272132,
	}
	root := mustTestShardStateCell(t, block)
	rootHash := root.HashKey(0)
	path := filepath.Join(dir, "wc0-shard0800000000000000-seqno63272132-eff0000000000000000-reuse.boc")
	if err := os.WriteFile(path, root.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false}), 0o644); err != nil {
		t.Fatalf("write reusable state file: %v", err)
	}

	store := newTestPebbleStore(t)
	node := &Node{
		log:              zerolog.Nop(),
		storage:          store,
		stateDownloadDir: dir,
	}
	staged, lazyRoot, err := node.tryImportReusableStagedStateFile(ctx, block, 0, rootHash[:])
	if err != nil {
		t.Fatalf("import reusable staged state file: %v", err)
	}
	if staged == nil || staged.peerAddr != "disk" {
		t.Fatalf("unexpected staged file %+v", staged)
	}
	if lazyRoot == nil || lazyRoot.HashKey(0) != rootHash {
		t.Fatal("unexpected imported root")
	}
	if _, _, err = store.LoadStateCellTree(ctx, block, rootHash[:]); err != nil {
		t.Fatalf("expected reusable state to be imported: %v", err)
	}
	if _, err = os.Stat(path); err != nil {
		t.Fatalf("reusable staged file should stay until artifact cleanup: %v", err)
	}
	if err = staged.cleanup(); err != nil {
		t.Fatalf("cleanup reusable staged file: %v", err)
	}
	if _, err = os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("cleanup should remove reusable staged file, got %v", err)
	}
}

func TestTryLoadReusableSplitPersistentStateHeader(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	block := ton.BlockIDExt{
		Workchain: 0,
		Shard:     int64(-1 << 63),
		SeqNo:     63272132,
	}
	root := mustTestShardStateCellWithAccounts(t, block, 0, 1)
	rootHash := root.HashKey(0)
	skeleton := cell.CreateProofSkeleton()
	skeleton.SetRecursive()
	proof, err := root.CreateProof(skeleton)
	if err != nil {
		t.Fatalf("create split header proof: %v", err)
	}

	path := filepath.Join(dir, "wc0-shard8000000000000000-seqno63272132-eff8000000000000000-reuse.boc")
	if err = os.WriteFile(path, proof.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false}), 0o644); err != nil {
		t.Fatalf("write reusable split header file: %v", err)
	}

	store := newTestPebbleStore(t)
	node := &Node{
		log:              zerolog.Nop(),
		storage:          store,
		stateDownloadDir: dir,
	}
	header, err := persistentStateSnapshotDownloader{
		node:          node,
		block:         block,
		master:        ton.BlockIDExt{Workchain: -1, Shard: int64(-1 << 63), SeqNo: block.SeqNo},
		stateRootHash: rootHash[:],
	}.tryLoadReusableSplitHeader(ctx, 4)
	if err != nil {
		t.Fatalf("load reusable split persistent state header: %v", err)
	}
	if header == nil || header.staged == nil || header.staged.path != path {
		t.Fatalf("unexpected header %+v", header)
	}
	if len(header.parts) == 0 {
		t.Fatal("expected split header parts")
	}
	if _, _, err = store.LoadStateCellTree(ctx, splitStateHeaderStorageBlock(block), nil); err != nil {
		t.Fatalf("expected split header cells to be imported: %v", err)
	}
	if _, err = os.Stat(path); err != nil {
		t.Fatalf("reusable split header file should stay until artifact cleanup: %v", err)
	}
	if err = header.staged.cleanup(); err != nil {
		t.Fatalf("cleanup reusable split header file: %v", err)
	}
}

func TestLoadImportedSplitPersistentStatePart(t *testing.T) {
	ctx := context.Background()
	block := ton.BlockIDExt{
		Workchain: 0,
		Shard:     int64(-1 << 63),
		SeqNo:     63272132,
	}
	partRoot := cell.BeginCell().MustStoreUInt(1, 1).EndCell()
	partRootHash := partRoot.HashKey()
	part := splitStatePart{
		effectiveShard: 0x0800000000000000,
		rootHash:       partRootHash[:],
	}

	store := newTestPebbleStore(t)
	if _, err := store.ImportStateCellTree(ctx, splitStatePartStorageBlock(block, part), partRoot, nil, 7); err != nil {
		t.Fatalf("import split part cell tree: %v", err)
	}

	node := &Node{
		log:     zerolog.Nop(),
		storage: store,
	}
	imported, err := persistentStateSnapshotDownloader{
		node:   node,
		block:  block,
		master: ton.BlockIDExt{Workchain: -1, Shard: int64(-1 << 63), SeqNo: block.SeqNo},
	}.loadImportedSplitPart(ctx, 1, 4, part)
	if err != nil {
		t.Fatalf("load imported split persistent state part: %v", err)
	}
	if imported.staged == nil || imported.staged.lazyRoot == nil {
		t.Fatal("expected staged lazy root")
	}
	if imported.staged.cells != 7 {
		t.Fatalf("unexpected cells count %d want 7", imported.staged.cells)
	}
	if imported.staged.effectiveShard != int64(part.effectiveShard) {
		t.Fatalf("unexpected effective shard %016x", uint64(imported.staged.effectiveShard))
	}
	if imported.staged.lazyRoot.HashKey() != partRootHash {
		t.Fatalf("unexpected lazy root hash")
	}
}

func TestSplitPersistentStatePartStorageDoesNotCollideWithFullState(t *testing.T) {
	ctx := context.Background()
	block := ton.BlockIDExt{
		Workchain: 0,
		Shard:     int64(-1 << 63),
		SeqNo:     63272132,
		RootHash:  bytes.Repeat([]byte{0x11}, 32),
		FileHash:  bytes.Repeat([]byte{0x22}, 32),
	}
	partRoot := cell.BeginCell().MustStoreUInt(1, 1).EndCell()
	partRootHash := partRoot.HashKey()
	part := splitStatePart{
		effectiveShard: uint64(block.Shard),
		rootHash:       partRootHash[:],
	}
	fullRoot := cell.BeginCell().MustStoreUInt(2, 2).EndCell()
	fullRootHash := fullRoot.HashKey(0)

	store := newTestPebbleStore(t)
	if _, err := store.ImportStateCellTree(ctx, splitStatePartStorageBlock(block, part), partRoot, nil, 1); err != nil {
		t.Fatalf("import split part cell tree: %v", err)
	}
	if _, err := store.ImportStateCellTree(ctx, block, fullRoot, nil, 1); err != nil {
		t.Fatalf("import full state cell tree: %v", err)
	}

	loadedPart, _, err := store.LoadStateCellTree(ctx, splitStatePartStorageBlock(block, part), partRootHash[:])
	if err != nil {
		t.Fatalf("load split part cell tree: %v", err)
	}
	if loadedPart.HashKey() != partRootHash {
		t.Fatalf("unexpected split part root hash")
	}

	loadedFull, _, err := store.LoadStateCellTree(ctx, block, fullRootHash[:])
	if err != nil {
		t.Fatalf("load full state cell tree: %v", err)
	}
	if loadedFull.HashKey(0) != fullRootHash {
		t.Fatalf("unexpected full root hash")
	}
}

func TestSplitPersistentStateMergeFromPebbleUsesPartRoots(t *testing.T) {
	ctx := context.Background()
	block := ton.BlockIDExt{
		Workchain: 0,
		Shard:     int64(-1 << 63),
		SeqNo:     63272132,
	}
	master := ton.BlockIDExt{Workchain: -1, Shard: int64(-1 << 63), SeqNo: block.SeqNo}
	splitDepth := uint32(4)
	fullRoot := mustTestShardStateCellWithAccountIDs(
		t,
		block,
		big.NewInt(0),
		new(big.Int).Lsh(big.NewInt(15), 252),
	)
	fullRootHash := fullRoot.HashKey(0)
	fullCellHash := fullRoot.HashKey()

	var fullState tlb.ShardStateUnsplit
	if err := tlb.LoadFromCell(&fullState, fullRoot.BeginParse()); err != nil {
		t.Fatalf("parse full state: %v", err)
	}

	store, err := pebblestore.Open(pebblestore.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open pebble store: %v", err)
	}
	defer func() { _ = store.Close() }()

	node := &Node{
		log:     zerolog.Nop(),
		storage: store,
	}
	header := &downloadedSplitStateHeader{
		state: &fullState,
		parts: mustSplitStatePartsFromFullState(t, block, &fullState, splitDepth),
	}
	if len(header.parts) != 2 {
		t.Fatalf("unexpected split parts count %d want 2", len(header.parts))
	}

	partArtifacts := make([]splitPersistentStatePartArtifact, 0, len(header.parts))
	for i, part := range header.parts {
		partRoot := mustSplitStatePartRoot(t, &fullState, part, splitDepth)
		if partRoot.GetType() == cell.MerkleProofCellType {
			t.Fatalf("part %d root should be account dict wrapper, got merkle proof", i+1)
		}
		partHash := partRoot.HashKey()
		if !bytes.Equal(partHash[:], part.rootHash) {
			t.Fatalf("part %d storage hash mismatch: got=%x want=%x", i+1, partHash, part.rootHash)
		}

		partBlock := splitStatePartStorageBlock(block, part)
		lazyPartRoot, err := store.ImportStateCellTree(ctx, partBlock, partRoot, nil, 0)
		if err != nil {
			t.Fatalf("import split part %d cells: %v", i+1, err)
		}
		if lazyPartRoot.GetType() == cell.MerkleProofCellType {
			t.Fatalf("lazy part %d root should not be merkle proof", i+1)
		}
		if lazyPartRoot.HashKey() != partHash {
			t.Fatalf("lazy part %d hash mismatch", i+1)
		}

		imported, err := persistentStateSnapshotDownloader{
			node:   node,
			block:  block,
			master: master,
		}.loadImportedSplitPart(ctx, i, len(header.parts), part)
		if err != nil {
			t.Fatalf("load imported split part %d: %v", i+1, err)
		}
		if imported.staged.lazyRoot == nil {
			t.Fatalf("imported split part %d has no lazy root", i+1)
		}
		if imported.staged.lazyRoot.GetType() == cell.MerkleProofCellType {
			t.Fatalf("imported split part %d should be part root, not proof header", i+1)
		}

		partArtifacts = append(partArtifacts, splitPersistentStatePartArtifact{
			part:   part,
			staged: imported.staged,
		})
	}

	artifact := &splitPersistentStateSnapshotArtifact{
		node:          node,
		block:         block,
		master:        master,
		stateRootHash: fullRootHash[:],
		header:        header,
		parts:         partArtifacts,
	}
	state, err := artifact.ImportCells(ctx, store)
	if err != nil {
		t.Fatalf("decode merged split state from pebble parts: %v", err)
	}
	if state.Cell.GetType() == cell.MerkleProofCellType {
		t.Fatal("merged state root should not be proof header")
	}
	if state.Cell.IsVirtualized() {
		t.Fatal("merged state root should be materialized, not virtualized")
	}
	if !bytes.Equal(state.StateRootHash, fullRootHash[:]) {
		t.Fatalf("merged state root hash mismatch: got=%x want=%x", state.StateRootHash, fullRootHash)
	}
	if !bytes.Equal(state.StateCellHash, fullCellHash[:]) {
		t.Fatalf("merged state cell hash mismatch: got=%x want=%x", state.StateCellHash, fullCellHash)
	}

	loadedRoot, _, err := store.LoadStateCellTree(ctx, block, fullRootHash[:])
	if err != nil {
		t.Fatalf("load merged state from pebble: %v", err)
	}
	if loadedRoot.GetType() == cell.MerkleProofCellType {
		t.Fatal("loaded merged state should not be proof header")
	}
	if loadedRoot.IsVirtualized() {
		t.Fatal("loaded merged state should not be virtualized")
	}
	if loadedRoot.HashKey() != fullCellHash {
		t.Fatalf("loaded merged state cell hash mismatch")
	}

	parsed, err := tnstore.ParseStateCell(&block, loadedRoot, nil, fullRootHash[:], nil)
	if err != nil {
		t.Fatalf("parse loaded merged state: %v", err)
	}
	if parsed.Parsed.Seqno != block.SeqNo {
		t.Fatalf("unexpected parsed seqno %d", parsed.Parsed.Seqno)
	}
}

func TestMergeSplitStateDoesNotExpandLazySplitPartAccounts(t *testing.T) {
	ctx := context.Background()
	block := ton.BlockIDExt{
		Workchain: 0,
		Shard:     int64(-1 << 63),
		SeqNo:     63272132,
	}
	splitDepth := uint32(4)
	accountIDs := make([]*big.Int, 0, 32)
	for i := int64(0); i < 32; i++ {
		accountIDs = append(accountIDs, big.NewInt(i))
	}

	fullRoot := mustTestShardStateCellWithAccountIDs(t, block, accountIDs...)
	fullRootHash := fullRoot.HashKey(0)

	var fullState tlb.ShardStateUnsplit
	if err := tlb.LoadFromCell(&fullState, fullRoot.BeginParse()); err != nil {
		t.Fatalf("parse full state: %v", err)
	}

	parts := mustSplitStatePartsFromFullState(t, block, &fullState, splitDepth)
	if len(parts) != 1 {
		t.Fatalf("unexpected split parts count %d want 1", len(parts))
	}

	store := newTestPebbleStore(t)
	partRoot := mustSplitStatePartRoot(t, &fullState, parts[0], splitDepth)
	if _, err := store.ImportStateCellTree(ctx, splitStatePartStorageBlock(block, parts[0]), partRoot, nil, 0); err != nil {
		t.Fatalf("import split part cells: %v", err)
	}

	loader := &countingLazyCellLoader{base: store.LazyCellLoader()}
	record, err := store.CellRecord(ctx, partRoot.Hash())
	if err != nil {
		t.Fatalf("load split part cell record: %v", err)
	}
	lazyPartRoot, err := tnstore.LazyCellRecord(record, loader.LoadCell)
	if err != nil {
		t.Fatalf("create counted lazy split part: %v", err)
	}

	merged, err := mergeSplitState(&fullState, []*cell.Cell{lazyPartRoot})
	if err != nil {
		t.Fatalf("merge split state: %v", err)
	}
	if merged.HashKey(0) != fullRootHash {
		t.Fatalf("merged state root hash mismatch")
	}
	if loader.calls > 8 {
		t.Fatalf("merge expanded lazy account tree: lazy loads=%d", loader.calls)
	}
}

func TestLoadImportedSplitPersistentStateHeader(t *testing.T) {
	ctx := context.Background()
	block := ton.BlockIDExt{
		Workchain: 0,
		Shard:     int64(-1 << 63),
		SeqNo:     63272132,
	}
	root := mustTestShardStateCellWithAccounts(t, block, 0, 1)
	rootHash := root.HashKey(0)
	skeleton := cell.CreateProofSkeleton()
	skeleton.SetRecursive()
	proof, err := root.CreateProof(skeleton)
	if err != nil {
		t.Fatalf("create split header proof: %v", err)
	}

	store := newTestPebbleStore(t)
	if _, err = store.ImportStateCellTree(ctx, splitStateHeaderStorageBlock(block), proof, nil, 11); err != nil {
		t.Fatalf("import split header cell tree: %v", err)
	}

	node := &Node{
		log:     zerolog.Nop(),
		storage: store,
	}
	header, err := persistentStateSnapshotDownloader{
		node:          node,
		block:         block,
		master:        ton.BlockIDExt{Workchain: -1, Shard: int64(-1 << 63), SeqNo: block.SeqNo},
		stateRootHash: rootHash[:],
	}.loadImportedSplitHeader(ctx, 4)
	if err != nil {
		t.Fatalf("load imported split persistent state header: %v", err)
	}
	if header.cells != 11 {
		t.Fatalf("unexpected header cells %d want 11", header.cells)
	}
	if len(header.parts) == 0 {
		t.Fatal("expected split header parts")
	}
	if header.state == nil || header.state.Seqno != block.SeqNo {
		t.Fatal("unexpected imported split header state")
	}
}

type countingLazyCellLoader struct {
	base  cell.LazyCellLoader
	calls int
}

func (l *countingLazyCellLoader) LoadCell(hash cell.Hash) (*cell.Cell, error) {
	l.calls++
	return l.base(hash)
}

func mustTestShardStateCell(t *testing.T, block ton.BlockIDExt) *cell.Cell {
	t.Helper()

	accounts, err := cell.NewAugDict(256, shardAccountsAugmentation{})
	if err != nil {
		t.Fatalf("create accounts dict: %v", err)
	}

	state := tlb.ShardStateUnsplit{
		GlobalID: -239,
		ShardIdent: tlb.ShardIdent{
			PrefixBits:  4,
			WorkchainID: block.Workchain,
			ShardPrefix: 0,
		},
		Seqno:           block.SeqNo,
		OutMsgQueueInfo: cell.BeginCell().EndCell(),
		Stats:           cell.BeginCell().EndCell(),
	}
	state.Accounts.ShardAccounts = &tlb.ShardAccountsAugDict{AugmentedDictionary: accounts}

	root, err := tlb.ToCell(&state)
	if err != nil {
		t.Fatalf("build shard state cell: %v", err)
	}
	return root
}

func mustTestShardStateCellWithAccounts(t *testing.T, block ton.BlockIDExt, keys ...uint64) *cell.Cell {
	t.Helper()

	accountIDs := make([]*big.Int, 0, len(keys))
	for _, key := range keys {
		accountIDs = append(accountIDs, new(big.Int).SetUint64(key))
	}
	return mustTestShardStateCellWithAccountIDs(t, block, accountIDs...)
}

func mustTestShardStateCellWithAccountIDs(t *testing.T, block ton.BlockIDExt, accountIDs ...*big.Int) *cell.Cell {
	t.Helper()

	accounts, err := cell.NewAugDict(256, shardAccountsAugmentation{})
	if err != nil {
		t.Fatalf("create accounts dict: %v", err)
	}

	for i, accountID := range accountIDs {
		account, err := tlb.ToCell(&tlb.ShardAccount{
			Account:       cell.BeginCell().MustStoreBoolBit(false).EndCell(),
			LastTransHash: make([]byte, 32),
			LastTransLT:   uint64(i + 1),
		})
		if err != nil {
			t.Fatalf("build shard account: %v", err)
		}
		if err = accounts.Set(cell.BeginCell().MustStoreBigInt(accountID, 256).EndCell(), account); err != nil {
			t.Fatalf("set shard account: %v", err)
		}
	}

	state := tlb.ShardStateUnsplit{
		GlobalID: -239,
		ShardIdent: tlb.ShardIdent{
			PrefixBits:  0,
			WorkchainID: block.Workchain,
			ShardPrefix: 0,
		},
		Seqno:           block.SeqNo,
		OutMsgQueueInfo: cell.BeginCell().EndCell(),
		Stats:           cell.BeginCell().EndCell(),
	}
	state.Accounts.ShardAccounts = &tlb.ShardAccountsAugDict{AugmentedDictionary: accounts}

	root, err := tlb.ToCell(&state)
	if err != nil {
		t.Fatalf("build shard state cell: %v", err)
	}
	return root
}

func mustSplitStatePartRoot(t *testing.T, header *tlb.ShardStateUnsplit, part splitStatePart, splitDepth uint32) *cell.Cell {
	t.Helper()

	prefix := cell.BeginCell().MustStoreUInt(part.effectiveShard>>(64-splitDepth), uint(splitDepth)).EndCell()
	partRoot, err := header.Accounts.ShardAccounts.ExtractPrefixSubdictRoot(prefix, false)
	if err != nil {
		t.Fatalf("extract split part root: %v", err)
	}
	if partRoot == nil {
		t.Fatal("split part root is empty")
	}

	wrapped, err := wrapShardAccountsRoot(partRoot)
	if err != nil {
		t.Fatalf("wrap split part root: %v", err)
	}
	return wrapped
}

func mustSplitStatePartsFromFullState(t *testing.T, block ton.BlockIDExt, header *tlb.ShardStateUnsplit, splitDepth uint32) []splitStatePart {
	t.Helper()

	shardPrefixLen := shardPrefixLength(block.Shard)
	if splitDepth <= uint32(shardPrefixLen) || splitDepth > 63 {
		t.Fatalf("invalid split depth %d for shard prefix length %d", splitDepth, shardPrefixLen)
	}

	partsCount := 1 << (splitDepth - uint32(shardPrefixLen))
	effectiveShard := uint64(block.Shard) ^ (uint64(1) << (63 - shardPrefixLen)) ^ (uint64(1) << (63 - splitDepth))
	increment := uint64(1) << (64 - splitDepth)

	parts := make([]splitStatePart, 0, partsCount)
	for i := 0; i < partsCount; i++ {
		prefix := cell.BeginCell().MustStoreUInt(effectiveShard>>(64-splitDepth), uint(splitDepth)).EndCell()
		partRoot, err := header.Accounts.ShardAccounts.ExtractPrefixSubdictRoot(prefix, false)
		if err != nil {
			t.Fatalf("extract split part %d root: %v", i+1, err)
		}
		if partRoot != nil {
			wrapped, err := wrapShardAccountsRoot(partRoot)
			if err != nil {
				t.Fatalf("wrap split part %d root: %v", i+1, err)
			}
			rootHash := wrapped.HashKey()
			parts = append(parts, splitStatePart{
				effectiveShard: effectiveShard,
				rootHash:       rootHash[:],
			})
		}
		effectiveShard += increment
	}
	if len(parts) == 0 {
		t.Fatal("expected non-empty split parts")
	}
	return parts
}
