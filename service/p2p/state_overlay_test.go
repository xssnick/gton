package p2p

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	tnstate "github.com/xssnick/gton/service/state"
	tnstore "github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/storage/pebblestore"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func saveTestBlockState(ctx context.Context, store *pebblestore.Store, state *tnstore.BlockState) error {
	entries := []tnstore.StateCheckpointBlock{testStateCheckpointEntry(state)}
	if state != nil && state.Block.Workchain != -1 && state.Block.SeqNo != 0 && state.MasterchainRef == nil {
		entries = append([]tnstore.StateCheckpointBlock{testStateCheckpointEntry(testDummyMasterState(state.Block.SeqNo))}, entries...)
	}
	_, err := store.SaveStateCheckpointEntries(ctx, entries, tnstore.StateCellRecords{}, nil)
	return err
}

func testStateCheckpointEntry(state *tnstore.BlockState) tnstore.StateCheckpointBlock {
	entry := tnstore.StateCheckpointBlock{State: state}
	if state != nil && state.Block.SeqNo != 0 {
		entry.Artifact = testStateCheckpointArtifact(state)
	}
	return entry
}

func testStateCheckpointArtifact(state *tnstore.BlockState) *tnstore.ServedBlockFull {
	block := state.Block
	meta := &tnstore.BlockMeta{ID: block, GenUTime: block.SeqNo}
	if block.Workchain != -1 {
		if state.MasterchainRef != nil {
			meta.MasterchainRefSeqno = state.MasterchainRef.SeqNo
		} else {
			meta.MasterchainRefSeqno = block.SeqNo
		}
	}
	return &tnstore.ServedBlockFull{
		ID:    block,
		Block: []byte{0x01},
		Proof: []byte{0x02},
		Meta:  meta, MessageEntries: []tnstore.MessageTransactionIndexEntry{},
	}
}

func testDummyMasterState(seqno uint32) *tnstore.BlockState {
	return &tnstore.BlockState{
		Block: ton.BlockIDExt{
			Workchain: -1,
			Shard:     topShard,
			SeqNo:     seqno,
			RootHash:  testDummyHash(0xf1, seqno),
			FileHash:  testDummyHash(0xf2, seqno),
		},
		StateRootHash: testDummyHash(0xf3, seqno),
	}
}

func testDummyHash(prefix byte, seqno uint32) []byte {
	hash := bytes.Repeat([]byte{prefix}, 32)
	binary.BigEndian.PutUint32(hash[len(hash)-4:], seqno)
	return hash
}

func testFullBlockID(workchain int32, shard int64, seqno uint32, rootPrefix byte, filePrefix byte) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: workchain,
		Shard:     shard,
		SeqNo:     seqno,
		RootHash:  testDummyHash(rootPrefix, seqno),
		FileHash:  testDummyHash(filePrefix, seqno),
	}
}

func TestZeroStateArchiveCandidatesSkipsTriedPeers(t *testing.T) {
	peerA := testZeroStatePeer("a")
	peerB := testZeroStatePeer("b")
	sub := &overlaySubscription{
		peers: map[PeerID]*overlayPeer{
			peerA.id: peerA,
			peerB.id: peerB,
		},
	}
	pool := testArchivePool(sub)
	shard := archiveShardFromBlock(testBlockID(-1, topShard, 0))

	got := zeroStateArchiveCandidates(pool, nil, shard, map[PeerID]struct{}{
		peerA.id: {},
	})
	if len(got) != 1 {
		t.Fatalf("unexpected candidate count %d", len(got))
	}
	if got[0].id != peerB.id {
		t.Fatalf("unexpected candidate %s, want %s", got[0].addr, peerB.addr)
	}
}

func TestZeroStateDeadlineGraceKeepsPeerCandidate(t *testing.T) {
	peer := testZeroStatePeer("slow")
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := &overlaySubscription{
		log:  discardLogger(),
		node: node,
		peers: map[PeerID]*overlayPeer{
			peer.id: peer,
		},
	}
	pool := testArchivePool(sub)
	session := node.BeginArchiveSession()
	defer session.Close()
	shard := archiveShardFromBlock(testBlockID(-1, topShard, 0))

	pool.markAvailable(shard, peer)
	session.noteArchivePeerAvailable(peer)
	tried := map[PeerID]struct{}{}
	if !session.noteZeroStatePeerError(context.Background(), pool, shard, peer, context.DeadlineExceeded) {
		t.Fatal("deadline grace should keep zero-state peer retryable")
	}

	got := zeroStateArchiveCandidates(pool, session, shard, tried)
	if len(got) == 0 || got[0].id != peer.id {
		t.Fatalf("deadline-graced peer was not retried: %#v", got)
	}
	failures, pinned := session.archivePeerDeadlineFailures(peer)
	if !pinned || failures != 1 {
		t.Fatalf("deadline failures = %d pinned=%v, want 1 and pinned", failures, pinned)
	}
	if pool.coolingDown(shard, peer) {
		t.Fatal("deadline grace should not cool down zero-state peer")
	}
}

func TestZeroStateNotAvailableKeepsBorrowedLivePeer(t *testing.T) {
	peer := testZeroStatePeer("live")
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := &overlaySubscription{
		log:  discardLogger(),
		node: node,
		peers: map[PeerID]*overlayPeer{
			peer.id: peer,
		},
	}
	pool := testArchivePool(sub)
	session := node.BeginArchiveSession()
	defer session.Close()
	shard := archiveShardFromBlock(testBlockID(-1, topShard, 0))

	session.rejectArchivePeer(context.Background(), pool, shard, peer, archivePeerRejectStateNotAvailable)

	if _, ok := sub.peers[peer.id]; !ok {
		t.Fatal("borrowed live peer was removed from live pool")
	}
	if !pool.hasPeer(peer.id) {
		t.Fatal("borrowed live peer was removed from archive pool")
	}
}

func TestZeroStateNotAvailableRotatesArchiveOnlyPeer(t *testing.T) {
	peer := testZeroStatePeer("archive-only")
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := &overlaySubscription{
		log:  discardLogger(),
		node: node,
	}
	pool := testArchivePool(sub)
	pool.addArchiveOnlyPeer(peer)
	session := node.BeginArchiveSession()
	defer session.Close()
	shard := archiveShardFromBlock(testBlockID(-1, topShard, 0))

	session.rejectArchivePeer(context.Background(), pool, shard, peer, archivePeerRejectStateNotAvailable)
	if !pool.hasPeer(peer.id) {
		t.Fatal("archive-only peer rotated after a single zero-state not-available")
	}

	session.rejectArchivePeer(context.Background(), pool, shard, peer, archivePeerRejectStateNotAvailable)
	if pool.hasPeer(peer.id) {
		t.Fatal("archive-only peer survived repeated zero-state not-available rotation")
	}
}

func TestPersistentStateProbeAcquiresDownloadLease(t *testing.T) {
	data := bytes.Repeat([]byte{0x42}, 64)
	rldpClient := &testArchiveRLDP{
		adnl:        newTestOverlayADNL(),
		asyncResult: data,
		asyncDelay:  150 * time.Millisecond,
	}
	peer := &overlayPeer{
		id:          testPeerID("probe-peer"),
		addr:        "probe-peer",
		rldpOverlay: overlay.CreateExtendedRLDP(rldpClient).CreateOverlay([]byte{0x01}),
		announced:   &overlay.Node{Version: int32(time.Now().Unix())},
		alive:       true,
	}
	node := &Node{
		log:     discardLogger(),
		peerUse: map[PeerID]peerUse{},
	}
	sub := &overlaySubscription{
		log:  discardLogger(),
		node: node,
		spec: overlaySpec{ShortID: []byte{0x01}},
	}
	block := testBlockID(-1, topShard, 42)
	downloader := persistentStateSnapshotDownloader{
		node:   node,
		sub:    sub,
		block:  block,
		master: block,
	}
	candidate := persistentStateCandidate{
		peer: peer,
		id: PersistentStateIDV2{
			Block:            block,
			MasterchainBlock: block,
			EffectiveShard:   topShard,
		},
		size:       int64(len(data)),
		workers:    1,
		chunkCount: 1,
	}

	done := make(chan error, 1)
	go func() {
		probes, errs := downloader.probePersistentStateCandidates(context.Background(), []persistentStateCandidate{candidate}, 1, nil)
		if len(probes) != 1 {
			done <- errors.Join(errs...)
			return
		}
		done <- nil
	}()

	deadline := time.After(time.Second)
	for node.downloadPeerLeaseCount(peer) == 0 {
		select {
		case err := <-done:
			t.Fatalf("probe finished before download lease was observed: %v", err)
		case <-deadline:
			t.Fatal("probe did not acquire download lease")
		case <-time.After(10 * time.Millisecond):
		}
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("probe failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("probe did not finish")
	}
	if leases := node.downloadPeerLeaseCount(peer); leases != 0 {
		t.Fatalf("probe leaked download lease: %d", leases)
	}
}

func testZeroStatePeer(label string) *overlayPeer {
	id := testPeerID(label)
	return &overlayPeer{
		id:            id,
		addr:          label,
		fixedMember:   true,
		overlay:       &overlay.ADNLOverlayWrapper{},
		alive:         true,
		lastReceiveAt: time.Now(),
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
	if _, err := store.BlockState(context.Background(), block); !errors.Is(err, tnstore.ErrNotFound) {
		t.Fatalf("staged metadata should not be persisted by p2p, got %v", err)
	}
}

func TestPrepareStateFilesDirPreservesCompletedFiles(t *testing.T) {
	dir := t.TempDir()
	completedPath := filepath.Join(dir, "state.boc")
	incompletePath := filepath.Join(dir, "state.boc"+stateFileTempSuffix)

	if err := os.WriteFile(completedPath, []byte("complete"), 0o644); err != nil {
		t.Fatalf("write completed file: %v", err)
	}
	if err := os.WriteFile(incompletePath, []byte("partial"), 0o644); err != nil {
		t.Fatalf("write incomplete file: %v", err)
	}

	got, err := prepareStateFilesDir(dir)
	if err != nil {
		t.Fatalf("prepare state files dir: %v", err)
	}
	if got != dir {
		t.Fatalf("unexpected dir %q want %q", got, dir)
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
	store := newTestPebbleStore(t)
	dir := store.StateFilesDir()
	block := testFullBlockID(0, int64(0x0800000000000000), 63272132, 0x11, 0x22)
	master := testFullBlockID(-1, int64(-1<<63), block.SeqNo, 0x33, 0x44)
	root := mustTestShardStateCell(t, block)
	rootHash := root.HashKey(0)
	name, err := tnstore.PersistentStateFileName(block, master, 0)
	if err != nil {
		t.Fatalf("persistent state file name: %v", err)
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, root.ToBOCWithOptions(cell.BOCSerializeOptions{
		WithIndex:     true,
		WithCRC32C:    true,
		WithTopHash:   true,
		WithIntHashes: true,
	}), 0o644); err != nil {
		t.Fatalf("write reusable state file: %v", err)
	}

	if err = store.SavePersistentStateFile(&tnstore.PersistentStateFile{
		Block:            block,
		MasterchainBlock: master,
		EffectiveShard:   0,
		Ref: &tnstore.ArtifactRef{
			Path: path,
			Size: int64(len(root.ToBOCWithOptions(cell.BOCSerializeOptions{
				WithIndex:     true,
				WithCRC32C:    true,
				WithTopHash:   true,
				WithIntHashes: true,
			}))),
		},
		FileHash:      bytes.Repeat([]byte{0x55}, 32),
		StateRootHash: rootHash[:],
	}); err != nil {
		t.Fatalf("save reusable state file metadata: %v", err)
	}
	node := &Node{
		log:           zerolog.Nop(),
		storage:       store,
		stateFilesDir: dir,
	}
	staged, lazyRoot, err := node.tryImportReusableStagedStateFile(ctx, block, master, 0, rootHash[:])
	if err != nil {
		t.Fatalf("import reusable staged state file: %v", err)
	}
	if staged == nil || staged.peerAddr != "local" {
		t.Fatalf("unexpected staged file %+v", staged)
	}
	if lazyRoot == nil || lazyRoot.HashKey(0) != rootHash {
		t.Fatal("unexpected imported root")
	}
	if _, err = store.LoadStateCellTree(ctx, block, rootHash[:]); !errors.Is(err, tnstore.ErrNotFound) {
		t.Fatalf("reusable state should stay uncommitted until metadata save, got %v", err)
	}
	if _, err = os.Stat(path); err != nil {
		t.Fatalf("reusable state file should stay on disk: %v", err)
	}
	if err = staged.cleanup(); err != nil {
		t.Fatalf("cleanup reusable state file: %v", err)
	}
	if _, err = os.Stat(path); err != nil {
		t.Fatalf("cleanup should keep reusable state file, got %v", err)
	}
}

func TestTryImportReusableStagedStateFileUsesPeerStorage(t *testing.T) {
	ctx := context.Background()
	store := newTestPebbleStore(t)
	dir := store.StateFilesDir()
	block := testFullBlockID(0, int64(0x0800000000000000), 63272133, 0x12, 0x23)
	master := testFullBlockID(-1, int64(-1<<63), block.SeqNo, 0x34, 0x45)
	root := mustTestShardStateCell(t, block)
	rootHash := root.HashKey(0)
	data := root.ToBOCWithOptions(cell.BOCSerializeOptions{
		WithIndex:     true,
		WithCRC32C:    true,
		WithTopHash:   true,
		WithIntHashes: true,
	})
	name, err := tnstore.PersistentStateFileName(block, master, 0)
	if err != nil {
		t.Fatalf("persistent state file name: %v", err)
	}
	path := filepath.Join(dir, name)
	if err = os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write reusable state file: %v", err)
	}
	if err = store.SavePersistentStateFile(&tnstore.PersistentStateFile{
		Block:            block,
		MasterchainBlock: master,
		EffectiveShard:   0,
		Ref: &tnstore.ArtifactRef{
			Path: path,
			Size: int64(len(data)),
		},
		FileHash:      bytes.Repeat([]byte{0x56}, 32),
		StateRootHash: rootHash[:],
	}); err != nil {
		t.Fatalf("save reusable state file metadata: %v", err)
	}

	node := &Node{
		log:           zerolog.Nop(),
		storage:       persistentStateFileMissingStore{Storage: store},
		peerStorage:   store,
		stateFilesDir: dir,
	}
	staged, lazyRoot, err := node.tryImportReusableStagedStateFile(ctx, block, master, 0, rootHash[:])
	if err != nil {
		t.Fatalf("import reusable staged state file from peer storage: %v", err)
	}
	if staged == nil || staged.peerAddr != "local" {
		t.Fatalf("unexpected staged file %+v", staged)
	}
	if lazyRoot == nil || lazyRoot.HashKey(0) != rootHash {
		t.Fatal("unexpected imported root")
	}
}

type persistentStateFileMissingStore struct {
	tnstore.Storage
}

func (s persistentStateFileMissingStore) PersistentStateFile(context.Context, ton.BlockIDExt, ton.BlockIDExt, int64) (*tnstore.PersistentStateFile, error) {
	return nil, tnstore.ErrNotFound
}

func TestTryLoadReusableSplitPersistentStateHeader(t *testing.T) {
	ctx := context.Background()
	store := newTestPebbleStore(t)
	dir := store.StateFilesDir()
	block := testFullBlockID(0, int64(-1<<63), 63272132, 0x11, 0x22)
	root := mustTestShardStateCellWithAccounts(t, block, 0, 1)
	rootHash := root.HashKey(0)
	proof := testFullMerkleProof(t, root)

	master := testFullBlockID(-1, int64(-1<<63), block.SeqNo, 0x33, 0x44)
	name, err := tnstore.PersistentStateFileName(block, master, block.Shard)
	if err != nil {
		t.Fatalf("split header file name: %v", err)
	}
	path := filepath.Join(dir, name)
	if err = os.WriteFile(path, proof.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false}), 0o644); err != nil {
		t.Fatalf("write reusable split header file: %v", err)
	}

	if err = store.SavePersistentStateFile(&tnstore.PersistentStateFile{
		Block:            block,
		MasterchainBlock: master,
		EffectiveShard:   block.Shard,
		Ref: &tnstore.ArtifactRef{
			Path: path,
			Size: int64(len(proof.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false}))),
		},
		FileHash:      bytes.Repeat([]byte{0x55}, 32),
		StateRootHash: rootHash[:],
	}); err != nil {
		t.Fatalf("save reusable split header file metadata: %v", err)
	}
	node := &Node{
		log:           zerolog.Nop(),
		storage:       store,
		stateFilesDir: dir,
	}
	header, err := persistentStateSnapshotDownloader{
		node:          node,
		block:         block,
		master:        master,
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
	if _, err = os.Stat(path); err != nil {
		t.Fatalf("reusable split header file should stay until artifact cleanup: %v", err)
	}
	if err = header.staged.cleanup(); err != nil {
		t.Fatalf("cleanup reusable split header file: %v", err)
	}
	if _, err = os.Stat(path); err != nil {
		t.Fatalf("cleanup should keep reusable split header file, got %v", err)
	}
}

func TestSaveSplitPersistentStateHeaderStoresStateRootHash(t *testing.T) {
	ctx := context.Background()
	store := newTestPebbleStore(t)
	dir := store.StateFilesDir()
	block := testFullBlockID(0, int64(-1<<63), 63272132, 0x11, 0x22)
	root := mustTestShardStateCellWithAccounts(t, block, 0, 1)
	rootHash := root.HashKey(0)
	proof := testFullMerkleProof(t, root)
	data := proof.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false})

	master := testFullBlockID(-1, int64(-1<<63), block.SeqNo, 0x33, 0x44)
	name, err := tnstore.PersistentStateFileName(block, master, block.Shard)
	if err != nil {
		t.Fatalf("split header file name: %v", err)
	}
	path := filepath.Join(dir, name)
	if err = os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write split header file: %v", err)
	}

	node := &Node{
		log:           zerolog.Nop(),
		peerStorage:   store,
		stateFilesDir: dir,
	}
	staged := &stagedStateFile{
		effectiveShard: block.Shard,
		peerAddr:       "127.0.0.1:30303",
		path:           path,
		size:           int64(len(data)),
		fileHash:       bytes.Repeat([]byte{0x55}, 32),
	}
	if err = node.savePersistentStateFile(block, master, staged, rootHash[:]); err != nil {
		t.Fatalf("save split persistent state header file: %v", err)
	}

	file, err := store.PersistentStateFile(ctx, block, master, block.Shard)
	if err != nil {
		t.Fatalf("load saved split header metadata: %v", err)
	}
	if !bytes.Equal(file.StateRootHash, rootHash[:]) {
		t.Fatalf("state root hash = %x, want %x", file.StateRootHash, rootHash)
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
	if _, err := store.ImportStateCellTree(ctx, splitStatePartStorageBlock(block, part), partRoot, 1); err != nil {
		t.Fatalf("import split part cell tree: %v", err)
	}
	if _, err := store.ImportStateCellTree(ctx, block, fullRoot, 1); err != nil {
		t.Fatalf("import full state cell tree: %v", err)
	}
	partLevelHash := partRoot.HashKey(0)
	if err := saveTestBlockState(ctx, store, &tnstore.BlockState{
		Block:          splitStatePartStorageBlock(block, part),
		StateRootHash:  partLevelHash[:],
		CellGeneration: 1,
	}); err != nil {
		t.Fatalf("save split part metadata: %v", err)
	}
	if err := saveTestBlockState(ctx, store, &tnstore.BlockState{
		Block:          block,
		StateRootHash:  fullRootHash[:],
		CellGeneration: 1,
	}); err != nil {
		t.Fatalf("save full state metadata: %v", err)
	}

	loadedPart, err := store.LoadStateCellTree(ctx, splitStatePartStorageBlock(block, part), partLevelHash[:])
	if err != nil {
		t.Fatalf("load split part cell tree: %v", err)
	}
	if loadedPart.HashKey() != partRootHash {
		t.Fatalf("unexpected split part root hash")
	}

	loadedFull, err := store.LoadStateCellTree(ctx, block, fullRootHash[:])
	if err != nil {
		t.Fatalf("load full state cell tree: %v", err)
	}
	if loadedFull.HashKey(0) != fullRootHash {
		t.Fatalf("unexpected full root hash")
	}
}

func TestStageSplitPartUsesImportedCellsProgress(t *testing.T) {
	ctx := context.Background()
	block := ton.BlockIDExt{
		Workchain: 0,
		Shard:     int64(-1 << 63),
		SeqNo:     63272132,
		RootHash:  bytes.Repeat([]byte{0x11}, 32),
		FileHash:  bytes.Repeat([]byte{0x22}, 32),
	}
	master := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     63272130,
		RootHash:  bytes.Repeat([]byte{0x33}, 32),
		FileHash:  bytes.Repeat([]byte{0x44}, 32),
	}
	partRoot := cell.BeginCell().MustStoreUInt(1, 1).EndCell()
	partRootHash := partRoot.HashKey()
	part := splitStatePart{
		effectiveShard: uint64(block.Shard),
		rootHash:       partRootHash[:],
	}
	boc := partRoot.ToBOCWithOptions(cell.BOCSerializeOptions{
		WithCRC32C:    true,
		WithIndex:     true,
		WithCacheBits: true,
		WithTopHash:   true,
		WithIntHashes: true,
	})
	store := newTestPebbleStore(t)
	dir := store.StateFilesDir()
	name, err := tnstore.PersistentStateFileName(block, master, int64(part.effectiveShard))
	if err != nil {
		t.Fatalf("persistent state file name: %v", err)
	}
	path := filepath.Join(dir, name)
	if err = os.WriteFile(path, boc, 0o644); err != nil {
		t.Fatalf("write reusable split part file: %v", err)
	}

	if err = store.SavePersistentStateFile(&tnstore.PersistentStateFile{
		Block:            block,
		MasterchainBlock: master,
		EffectiveShard:   int64(part.effectiveShard),
		Ref: &tnstore.ArtifactRef{
			Path: path,
			Size: int64(len(boc)),
		},
		FileHash:      bytes.Repeat([]byte{0x55}, 32),
		StateRootHash: part.rootHash,
	}); err != nil {
		t.Fatalf("save reusable split part file metadata: %v", err)
	}
	if _, err := store.ImportStateCellTree(ctx, splitStatePartStorageBlock(block, part), partRoot, 1); err != nil {
		t.Fatalf("import split part cells: %v", err)
	}

	node := &Node{
		log:           zerolog.Nop(),
		storage:       store,
		stateFilesDir: dir,
	}
	downloader := persistentStateSnapshotDownloader{
		node:          node,
		block:         block,
		master:        master,
		stateRootHash: bytes.Repeat([]byte{0x66}, 32),
	}
	res := downloader.stageSplitPart(ctx, 0, 1, part, make(chan struct{}, 1))
	if res.err != nil {
		t.Fatalf("stage split part: %v", res.err)
	}
	if res.part == nil || res.part.staged == nil || res.part.staged.lazyRoot == nil {
		t.Fatalf("expected staged lazy root from imported cells")
	}
	if res.part.staged.lazyRoot.HashKey() != partRootHash {
		t.Fatalf("unexpected imported split part hash")
	}
	if res.part.staged.peerAddr != "local" {
		t.Fatalf("unexpected staged peer %q", res.part.staged.peerAddr)
	}
}

func TestImportSplitPartSavesReusableFileAndCells(t *testing.T) {
	ctx := context.Background()
	block := ton.BlockIDExt{
		Workchain: 0,
		Shard:     int64(-1 << 63),
		SeqNo:     63272132,
		RootHash:  bytes.Repeat([]byte{0x11}, 32),
		FileHash:  bytes.Repeat([]byte{0x22}, 32),
	}
	master := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     63272130,
		RootHash:  bytes.Repeat([]byte{0x33}, 32),
		FileHash:  bytes.Repeat([]byte{0x44}, 32),
	}
	partRoot := cell.BeginCell().MustStoreUInt(1, 1).EndCell()
	partRootHash := partRoot.HashKey()
	part := splitStatePart{
		effectiveShard: uint64(block.Shard),
		rootHash:       partRootHash[:],
	}
	boc := partRoot.ToBOCWithOptions(cell.BOCSerializeOptions{
		WithCRC32C:    true,
		WithIndex:     true,
		WithCacheBits: true,
		WithTopHash:   true,
		WithIntHashes: true,
	})
	store := newTestPebbleStore(t)
	name, err := tnstore.PersistentStateFileName(block, master, int64(part.effectiveShard))
	if err != nil {
		t.Fatalf("split part file name: %v", err)
	}
	path := filepath.Join(store.StateFilesDir(), name)
	if err := os.WriteFile(path, boc, 0o644); err != nil {
		t.Fatalf("write split part boc: %v", err)
	}

	node := &Node{
		log:           zerolog.Nop(),
		storage:       store,
		peerStorage:   store,
		stateFilesDir: store.StateFilesDir(),
	}
	downloader := persistentStateSnapshotDownloader{
		node:          node,
		block:         block,
		master:        master,
		stateRootHash: bytes.Repeat([]byte{0x66}, 32),
	}
	staged := &stagedStateFile{
		effectiveShard: int64(part.effectiveShard),
		peerAddr:       "127.0.0.1:30303",
		path:           path,
		size:           int64(len(boc)),
		fileHash:       bytes.Repeat([]byte{0x55}, 32),
	}
	if err := downloader.importSplitPart(ctx, 0, 1, part, &downloadedSplitStatePart{staged: staged}); err != nil {
		t.Fatalf("import split part: %v", err)
	}

	loaded, err := node.loadImportedSplitStatePartRoot(ctx, block, part)
	if err != nil {
		t.Fatalf("load imported split part cells: %v", err)
	}
	if loaded.HashKey() != partRootHash {
		t.Fatalf("unexpected imported split part hash")
	}
	if _, err = store.BlockState(ctx, splitStatePartStorageBlock(block, part)); !errors.Is(err, tnstore.ErrNotFound) {
		t.Fatalf("split part import should not save block state metadata, got %v", err)
	}
	if _, err = store.PersistentStateFile(ctx, block, master, int64(part.effectiveShard)); err != nil {
		t.Fatalf("load saved split part file metadata: %v", err)
	}
}

func TestSplitStatePartRootImportsWhenImporterDisablesPartReuse(t *testing.T) {
	ctx := context.Background()
	block := testFullBlockID(0, int64(-1<<63), 63272132, 0x11, 0x22)
	master := testFullBlockID(-1, int64(-1<<63), block.SeqNo, 0x33, 0x44)
	partRoot := cell.BeginCell().MustStoreUInt(1, 1).EndCell()
	partRootHash := partRoot.HashKey()
	part := splitStatePart{
		effectiveShard: uint64(block.Shard),
		rootHash:       partRootHash[:],
	}
	boc := partRoot.ToBOCWithOptions(cell.BOCSerializeOptions{
		WithCRC32C:    true,
		WithIndex:     true,
		WithCacheBits: true,
		WithTopHash:   true,
		WithIntHashes: true,
	})
	path := filepath.Join(t.TempDir(), "part.boc")
	if err := os.WriteFile(path, boc, 0o644); err != nil {
		t.Fatalf("write split part boc: %v", err)
	}

	oldStore := newTestPebbleStore(t)
	if _, err := oldStore.ImportStateCellTree(ctx, splitStatePartStorageBlock(block, part), partRoot, 1); err != nil {
		t.Fatalf("import old split part cells: %v", err)
	}

	importer := &splitPartReuseDisabledImporter{Store: newTestPebbleStore(t)}
	artifact := &splitPersistentStateSnapshotArtifact{
		node:   &Node{log: zerolog.Nop(), storage: oldStore},
		block:  block,
		master: master,
	}
	root, err := artifact.splitStatePartRoot(ctx, importer, splitPersistentStatePartArtifact{
		part: part,
		staged: &stagedStateFile{
			effectiveShard: int64(part.effectiveShard),
			peerAddr:       "local",
			path:           path,
			size:           int64(len(boc)),
		},
	}, 0)
	if err != nil {
		t.Fatalf("load split part root: %v", err)
	}
	if root.HashKey() != partRootHash {
		t.Fatalf("unexpected split part root hash")
	}
	if importer.bocImports != 1 {
		t.Fatalf("split part imports = %d, want 1", importer.bocImports)
	}
}

func TestSplitPersistentStateMergeFromPebbleUsesPartRoots(t *testing.T) {
	ctx := context.Background()
	block := testFullBlockID(0, int64(-1<<63), 63272132, 0x11, 0x22)
	master := testFullBlockID(-1, int64(-1<<63), block.SeqNo, 0x33, 0x44)
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
	if err := tlb.LoadFromCell(&fullState, fullRoot.MustBeginParse()); err != nil {
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
		lazyPartRoot, err := store.ImportStateCellTree(ctx, partBlock, partRoot, 0)
		if err != nil {
			t.Fatalf("import split part %d cells: %v", i+1, err)
		}
		if lazyPartRoot.GetType() == cell.MerkleProofCellType {
			t.Fatalf("lazy part %d root should not be merkle proof", i+1)
		}
		if lazyPartRoot.HashKey() != partHash {
			t.Fatalf("lazy part %d hash mismatch", i+1)
		}

		partArtifacts = append(partArtifacts, splitPersistentStatePartArtifact{
			part: part,
			staged: &stagedStateFile{
				effectiveShard: int64(part.effectiveShard),
				peerAddr:       "celldb",
				lazyRoot:       lazyPartRoot,
			},
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
	if err = saveTestBlockState(ctx, store, state); err != nil {
		t.Fatalf("commit merged state metadata: %v", err)
	}

	loadedRoot, err := store.LoadStateCellTree(ctx, block, fullRootHash[:])
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

type splitPartReuseDisabledImporter struct {
	*pebblestore.Store
	bocImports int
}

func (i *splitPartReuseDisabledImporter) ImportStateBOCView(ctx context.Context, block ton.BlockIDExt, view *cell.BOCView) (*cell.Cell, error) {
	i.bocImports++
	return i.Store.ImportStateBOCView(ctx, block, view)
}

func (i *splitPartReuseDisabledImporter) ReuseImportedSplitStatePartCells() bool {
	return false
}

func TestSplitStatePartsMatchesSerializedProofHeaderPartHashes(t *testing.T) {
	block := ton.BlockIDExt{
		Workchain: 0,
		Shard:     int64(-1 << 63),
		SeqNo:     63272132,
	}
	splitDepth := uint32(4)
	fullRoot := mustTestShardStateCellWithAccountIDs(
		t,
		block,
		big.NewInt(0),
		new(big.Int).Lsh(big.NewInt(3), 252),
		new(big.Int).Lsh(big.NewInt(9), 252),
		new(big.Int).Lsh(big.NewInt(15), 252),
	)
	fullRootHash := fullRoot.HashKey(0)

	headerProof, expected := testSplitPersistentStateHeaderProof(t, block, fullRoot, splitDepth)

	_, headerParts, err := splitStateParts(block, headerProof, splitDepth, fullRootHash[:])
	if err != nil {
		t.Fatalf("parse split state header proof: %v", err)
	}
	if len(headerParts) != len(expected) {
		t.Fatalf("unexpected header parts count %d want %d", len(headerParts), len(expected))
	}
	for _, part := range headerParts {
		want, ok := expected[part.effectiveShard]
		if !ok {
			t.Fatalf("unexpected split part shard %016x", part.effectiveShard)
		}
		if !bytes.Equal(part.rootHash, want[:]) {
			t.Fatalf("split part %016x hash mismatch: got=%x want=%x", part.effectiveShard, part.rootHash, want)
		}
	}
}

func testSplitPersistentStateHeaderProof(t *testing.T, block ton.BlockIDExt, fullRoot *cell.Cell, splitDepth uint32) (*cell.Cell, map[uint64]cell.Hash) {
	t.Helper()

	proofBuilder := cell.NewMerkleProofBuilder(fullRoot)
	proofRoot := proofBuilder.Root()

	var header tlb.ShardStateUnsplit
	if err := tlb.LoadFromCell(&header, proofRoot.MustBeginParse()); err != nil {
		t.Fatalf("parse proof root: %v", err)
	}

	parts := mustSplitStatePartsFromFullState(t, block, &header, splitDepth)
	expected := make(map[uint64]cell.Hash, len(parts))
	for _, part := range parts {
		partRoot := mustSplitStatePartRoot(t, &header, part, splitDepth)
		expected[part.effectiveShard] = partRoot.HashKey()
	}

	testMarkSplitHeaderStateExceptAccounts(t, proofRoot)
	proof, err := proofBuilder.CreateProof()
	if err != nil {
		t.Fatalf("create split header proof: %v", err)
	}
	return proof, expected
}

func testMarkSplitHeaderStateExceptAccounts(t *testing.T, root *cell.Cell) {
	t.Helper()

	refs := int(root.RefsNum())
	if refs < 3 {
		t.Fatalf("shard state root refs=%d, want at least 3", refs)
	}
	for i := 0; i < refs; i++ {
		if i == 1 {
			continue
		}
		ref, err := root.PeekRef(i)
		if err != nil {
			t.Fatalf("peek shard state ref %d: %v", i, err)
		}
		testMarkFullProofSubtree(t, ref)
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
	if err := tlb.LoadFromCell(&fullState, fullRoot.MustBeginParse()); err != nil {
		t.Fatalf("parse full state: %v", err)
	}

	parts := mustSplitStatePartsFromFullState(t, block, &fullState, splitDepth)
	if len(parts) != 1 {
		t.Fatalf("unexpected split parts count %d want 1", len(parts))
	}

	store := newTestPebbleStore(t)
	partRoot := mustSplitStatePartRoot(t, &fullState, parts[0], splitDepth)
	if _, err := store.ImportStateCellTree(ctx, splitStatePartStorageBlock(block, parts[0]), partRoot, 0); err != nil {
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

	merged, err := tnstate.MergeSplitState(&fullState, []*cell.Cell{lazyPartRoot})
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

func testFullMerkleProof(t *testing.T, root *cell.Cell) *cell.Cell {
	t.Helper()

	proofBuilder := cell.NewMerkleProofBuilder(root)
	testMarkFullProofSubtree(t, proofBuilder.Root())
	proof, err := proofBuilder.CreateProof()
	if err != nil {
		t.Fatalf("create full proof: %v", err)
	}
	return proof
}

func testMarkFullProofSubtree(t *testing.T, root *cell.Cell) {
	t.Helper()

	loader, err := root.BeginParse()
	if err != nil {
		t.Fatalf("begin proof subtree: %v", err)
	}
	for i := 0; i < loader.RefsNum(); i++ {
		ref, err := loader.PeekRefCellAt(i)
		if err != nil {
			t.Fatalf("proof subtree ref %d: %v", i, err)
		}
		testMarkFullProofSubtree(t, ref)
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

	accounts, err := tnstate.NewShardAccountsAugDict()
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

	accounts, err := tnstate.NewShardAccountsAugDict()
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

	wrapped, err := tnstate.WrapShardAccountsRoot(partRoot)
	if err != nil {
		t.Fatalf("wrap split part root: %v", err)
	}
	return wrapped
}

func mustSplitStatePartsFromFullState(t *testing.T, block ton.BlockIDExt, header *tlb.ShardStateUnsplit, splitDepth uint32) []splitStatePart {
	t.Helper()

	shardPrefixLen := tnstate.ShardPrefixLength(block.Shard)
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
			wrapped, err := tnstate.WrapShardAccountsRoot(partRoot)
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
