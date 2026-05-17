package state

import (
	"context"
	"errors"
	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	topShard   = int64(-1 << 63)
	leftShard  = int64(1 << 62)
	rightShard = int64(-1 << 62)
)

func TestSyncerSyncCurrentStoresSnapshot(t *testing.T) {
	master := testStateBlock(-1, topShard, 100)
	base := testStateBlock(0, leftShard, 200)
	aux := testStateBlock(0, rightShard, 201)

	source := &fakeSource{
		master: master,
		states: map[string]*storage.BlockState{
			storage.BlockKey(master): testMasterState(t, master, base, aux),
			storage.BlockKey(base):   {Block: base, Cell: cell.BeginCell().EndCell()},
			storage.BlockKey(aux):    {Block: aux, Cell: cell.BeginCell().EndCell()},
		},
	}

	store := newTestStateStore()
	savePendingMasterProgress(t, store, master, base, aux)
	syncer := NewSyncer(source, store, SyncerOptions{})

	snapshot, err := syncer.SyncCurrent(context.Background())
	if err != nil {
		t.Fatalf("sync current: %v", err)
	}

	if snapshot.Masterchain.Block.SeqNo != master.SeqNo {
		t.Fatalf("unexpected masterchain seqno %d", snapshot.Masterchain.Block.SeqNo)
	}
	if len(snapshot.Shards) != 2 {
		t.Fatalf("unexpected shard count %d", len(snapshot.Shards))
	}
	if snapshot.Masterchain.Cell != nil || snapshot.Masterchain.Parsed != nil {
		t.Fatal("snapshot retained masterchain cell data")
	}
	for _, shard := range snapshot.Shards {
		if shard.Cell != nil || shard.Parsed != nil {
			t.Fatal("snapshot retained shard cell data")
		}
	}

	storedCurrent, err := store.CurrentState(context.Background())
	if err != nil {
		t.Fatalf("load current state: %v", err)
	}
	if storedCurrent.Masterchain.Block.SeqNo != master.SeqNo {
		t.Fatalf("stored unexpected masterchain seqno %d", storedCurrent.Masterchain.Block.SeqNo)
	}

	storedBase, err := store.BlockState(context.Background(), base)
	if err != nil {
		t.Fatalf("load base shard state: %v", err)
	}
	if storedBase.Block.SeqNo != base.SeqNo {
		t.Fatalf("stored unexpected base shard seqno %d", storedBase.Block.SeqNo)
	}

	if got := source.downloadCount[storage.BlockKey(master)]; got != 0 {
		t.Fatalf("expected pending master state not to be downloaded, got %d", got)
	}
}

func TestSyncerReturnsShardDownloadError(t *testing.T) {
	master := testStateBlock(-1, topShard, 10)
	shard := testStateBlock(0, topShard, 11)

	source := &fakeSource{
		master: master,
		states: map[string]*storage.BlockState{
			storage.BlockKey(master): testMasterState(t, master, shard),
		},
		errByBlock: map[string]error{
			storage.BlockKey(shard): context.DeadlineExceeded,
		},
	}

	store := newTestStateStore()
	savePendingMasterProgress(t, store, master, shard)
	syncer := NewSyncer(source, store, SyncerOptions{})

	if _, err := syncer.SyncCurrent(context.Background()); err == nil {
		t.Fatal("expected sync error")
	}
}

func TestSyncerRetriesShardStateWithoutRepeatingMasterStage(t *testing.T) {
	master := testStateBlock(-1, topShard, 10)
	shard := testStateBlock(0, topShard, 11)

	source := &fakeSource{
		master: master,
		states: map[string]*storage.BlockState{
			storage.BlockKey(master): testMasterState(t, master, shard),
			storage.BlockKey(shard):  {Block: shard},
		},
		errSequenceByBlock: map[string][]error{
			storage.BlockKey(shard): {p2p.ErrStateNotAvailable},
		},
	}

	store := newTestStateStore()
	savePendingMasterProgress(t, store, master, shard)
	syncer := NewSyncer(source, store, SyncerOptions{})
	snapshot, err := syncer.SyncCurrent(context.Background())
	if err != nil {
		t.Fatalf("sync current: %v", err)
	}
	if !snapshot.Masterchain.Block.Equals(&master) {
		t.Fatalf("unexpected masterchain block %s", storage.FormatBlockRef(snapshot.Masterchain.Block))
	}
	if len(snapshot.Shards) != 1 {
		t.Fatalf("unexpected shard count %d", len(snapshot.Shards))
	}
	if got := source.downloadCount[storage.BlockKey(master)]; got != 0 {
		t.Fatalf("expected pending master state not to be downloaded, got %d", got)
	}
	if got := source.zeroStateCalls; got != 0 {
		t.Fatalf("expected zero state lookup to be skipped, got %d", got)
	}
}

func TestSyncerUsesStoredShardStateWithoutCurrentCheckpoint(t *testing.T) {
	master := testStateBlock(-1, topShard, 10)
	shard := testStateBlock(0, topShard, 11)

	source := &fakeSource{
		master: master,
		errByBlock: map[string]error{
			storage.BlockKey(shard): errors.New("shard should not be downloaded"),
		},
	}

	store := newTestStateStore()
	if err := store.SaveBlockState(context.Background(), testMasterState(t, master, shard)); err != nil {
		t.Fatalf("save stored master: %v", err)
	}
	if err := store.SaveBlockState(context.Background(), &storage.BlockState{Block: shard}); err != nil {
		t.Fatalf("save stored shard: %v", err)
	}
	if err := store.SaveStateSyncProgress(context.Background(), &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: master.SeqNo,
		Masterchain:      storage.BlockState{Block: master},
	}); err != nil {
		t.Fatalf("save pending progress: %v", err)
	}

	syncer := NewSyncer(source, store, SyncerOptions{})
	snapshot, err := syncer.SyncCurrent(context.Background())
	if err != nil {
		t.Fatalf("sync current: %v", err)
	}
	if _, ok := snapshot.Shards[storage.ShardKeyFromBlock(shard)]; !ok {
		t.Fatalf("stored shard %s is missing from current snapshot", storage.FormatBlockRef(shard))
	}
	if got := source.downloadCount[storage.BlockKey(shard)]; got != 0 {
		t.Fatalf("expected stored shard not to be downloaded, got %d", got)
	}
}

func TestSyncerResumesPendingStateSyncWithoutSelectingNewMaster(t *testing.T) {
	pendingMaster := testStateBlock(-1, topShard, 10)
	newerMaster := testStateBlock(-1, topShard, 20)
	doneShard := testStateBlock(0, leftShard, 11)
	missingShard := testStateBlock(0, rightShard, 12)

	source := &fakeSource{
		master: newerMaster,
		states: map[string]*storage.BlockState{
			storage.BlockKey(missingShard): {Block: missingShard},
		},
		errByBlock: map[string]error{
			storage.BlockKey(doneShard): errors.New("completed shard should not be downloaded"),
		},
	}

	store := newTestStateStore()
	if err := store.SaveBlockState(context.Background(), testMasterState(t, pendingMaster, doneShard, missingShard)); err != nil {
		t.Fatalf("save pending master: %v", err)
	}
	if err := store.SaveBlockState(context.Background(), &storage.BlockState{Block: doneShard}); err != nil {
		t.Fatalf("save completed shard: %v", err)
	}
	progress := &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: pendingMaster.SeqNo,
		Masterchain:      storage.BlockState{Block: pendingMaster},
		Shards: map[storage.ShardKey]storage.BlockState{
			storage.ShardKeyFromBlock(doneShard): {Block: doneShard},
		},
	}
	if err := store.SaveStateSyncProgress(context.Background(), progress); err != nil {
		t.Fatalf("save pending progress: %v", err)
	}

	syncer := NewSyncer(source, store, SyncerOptions{})
	snapshot, err := syncer.SyncCurrent(context.Background())
	if err != nil {
		t.Fatalf("sync current: %v", err)
	}
	if !snapshot.Masterchain.Block.Equals(&pendingMaster) {
		t.Fatalf("expected pending master %s, got %s", storage.FormatBlockRef(pendingMaster), storage.FormatBlockRef(snapshot.Masterchain.Block))
	}
	if _, ok := snapshot.Shards[storage.ShardKeyFromBlock(doneShard)]; !ok {
		t.Fatalf("completed shard %s is missing from current snapshot", storage.FormatBlockRef(doneShard))
	}
	if _, ok := snapshot.Shards[storage.ShardKeyFromBlock(missingShard)]; !ok {
		t.Fatalf("downloaded shard %s is missing from current snapshot", storage.FormatBlockRef(missingShard))
	}
	if got := source.zeroStateCalls; got != 0 {
		t.Fatalf("expected zero state lookup to be skipped, got %d", got)
	}
	if got := source.downloadCount[storage.BlockKey(doneShard)]; got != 0 {
		t.Fatalf("expected completed shard not to be downloaded, got %d", got)
	}
	if got := source.downloadCount[storage.BlockKey(missingShard)]; got != 1 {
		t.Fatalf("expected missing shard to be downloaded once, got %d", got)
	}
	if _, err = store.StateSyncProgress(context.Background()); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected pending progress to be cleared, got %v", err)
	}
}

func TestSyncerSerializesShardStateDecodeAndPersist(t *testing.T) {
	master := testStateBlock(-1, topShard, 10)
	shardA := testStateBlock(0, leftShard, 11)
	shardB := testStateBlock(0, rightShard, 12)

	var currentDecodes atomic.Int32
	var maxDecodes atomic.Int32

	source := &fakeSource{
		master: master,
		states: map[string]*storage.BlockState{
			storage.BlockKey(master): testMasterState(t, master, shardA, shardB),
			storage.BlockKey(shardA): {Block: shardA},
			storage.BlockKey(shardB): {Block: shardB},
		},
		downloadedFactory: func(block ton.BlockIDExt) storage.DownloadedState {
			return &trackingDownloadedState{
				block:          block,
				currentDecodes: &currentDecodes,
				maxDecodes:     &maxDecodes,
				delay:          20 * time.Millisecond,
			}
		},
	}

	store := newTestStateStore()
	savePendingMasterProgress(t, store, master, shardA, shardB)
	syncer := NewSyncer(source, store, SyncerOptions{})
	if _, err := syncer.SyncCurrent(context.Background()); err != nil {
		t.Fatalf("sync current: %v", err)
	}
	if got := maxDecodes.Load(); got != 1 {
		t.Fatalf("expected serialized decode, got max concurrent decodes %d", got)
	}
}

func TestSyncerWalksKeyBlocksFromZeroStateToTailWithoutLatestLookup(t *testing.T) {
	zero := testStateBlock(-1, topShard, 0)
	key20 := testStateBlock(-1, topShard, 20)
	key30 := testStateBlock(-1, topShard, 30)
	key40 := testStateBlock(-1, topShard, 40)

	source := &fakeSource{
		master: zero,
		keyBlockBatches: map[uint32]p2p.KeyBlockBatch{
			0:  {Blocks: []ton.BlockIDExt{key20, key30}},
			30: {Blocks: []ton.BlockIDExt{key40}, Incomplete: true},
			40: {Incomplete: true},
		},
	}

	syncer := NewSyncer(source, newTestStateStore(), SyncerOptions{})
	blocks, err := syncer.keyBlockIDs(context.Background(), zero)
	if err != nil {
		t.Fatalf("walk key blocks: %v", err)
	}

	if got, want := len(blocks), 3; got != want {
		t.Fatalf("unexpected key block count %d, want %d", got, want)
	}
	if !blocks[0].Equals(&key20) || !blocks[1].Equals(&key30) || !blocks[2].Equals(&key40) {
		t.Fatalf("unexpected key block chain %#v", blocks)
	}
	if got := source.nextKeyBlockCalls; got != 3 {
		t.Fatalf("unexpected next key block calls %d", got)
	}
}

func TestSyncerUsesConfiguredZeroStateWhenInitBlockIsEmpty(t *testing.T) {
	emptyInit := ton.BlockIDExt{}
	zero := testStateBlock(-1, topShard, 0)
	source := &fakeSource{
		initBlock: &emptyInit,
		zeroBlock: &zero,
		zeroState: testMasterState(t, zero),
	}
	syncer := NewSyncer(source, newTestStateStore(), SyncerOptions{})

	trusted, err := syncer.trustedKeyBlockAnchor(context.Background())
	if err != nil {
		t.Fatalf("trusted key block anchor: %v", err)
	}
	if !trusted.block.Equals(&zero) {
		t.Fatalf("trusted block = %s, want %s", storage.FormatBlockRef(trusted.block), storage.FormatBlockRef(zero))
	}
	if got := source.zeroStateCalls; got != 1 {
		t.Fatalf("zero state block calls = %d, want 1", got)
	}
}

func TestSyncerRejectsNonMasterchainKeyBlockAnchor(t *testing.T) {
	source := &fakeSource{}
	syncer := NewSyncer(source, newTestStateStore(), SyncerOptions{})

	_, err := syncer.keyBlockIDs(context.Background(), testStateBlock(0, 0, 0))
	if err == nil {
		t.Fatal("expected non-masterchain key block anchor error")
	}
	if got := source.nextKeyBlockCalls; got != 0 {
		t.Fatalf("unexpected next key block calls %d", got)
	}
}

func savePendingMasterProgress(t *testing.T, store *testStateStore, master ton.BlockIDExt, shards ...ton.BlockIDExt) {
	t.Helper()

	if err := store.SaveBlockState(context.Background(), testMasterState(t, master, shards...)); err != nil {
		t.Fatalf("save pending master: %v", err)
	}
	if err := store.SaveStateSyncProgress(context.Background(), &storage.CurrentState{
		SyncedAt:         time.Now(),
		ShardClientSeqno: master.SeqNo,
		Masterchain:      storage.BlockState{Block: master},
	}); err != nil {
		t.Fatalf("save pending progress: %v", err)
	}
}

func testMasterState(t *testing.T, master ton.BlockIDExt, shards ...ton.BlockIDExt) *storage.BlockState {
	t.Helper()

	state := &storage.BlockState{
		Block: master,
		Cell:  cell.BeginCell().EndCell(),
		Parsed: &tlb.ShardStateUnsplit{
			McStateExtra: testMcStateExtra(t, shards...),
		},
	}
	if len(shards) > 0 {
		got, err := ShardBlocksFromMasterState(state)
		if err != nil {
			t.Fatalf("validate test master state shards: %v", err)
		}
		if len(got) != len(shards) {
			t.Fatalf("test master state shard count = %d, want %d", len(got), len(shards))
		}
		want := make(map[string]ton.BlockIDExt, len(shards))
		for _, shard := range shards {
			want[storage.BlockKey(shard)] = shard
		}
		for _, shard := range got {
			if _, ok := want[storage.BlockKey(shard)]; !ok {
				t.Fatalf("test master state returned unexpected shard %s", storage.FormatBlockRef(shard))
			}
		}
	}
	return state
}

func testMcStateExtra(t *testing.T, shards ...ton.BlockIDExt) *cell.Cell {
	t.Helper()

	shardHashes := cell.NewDict(32)
	byWorkchain := map[int32][]ton.BlockIDExt{}
	for _, shard := range shards {
		byWorkchain[shard.Workchain] = append(byWorkchain[shard.Workchain], shard)
	}
	for workchain, workchainShards := range byWorkchain {
		key := cell.BeginCell().MustStoreInt(int64(workchain), 32).EndCell()
		value := cell.BeginCell().MustStoreRef(testShardBinTree(t, workchainShards)).EndCell()
		if err := shardHashes.Set(key, value); err != nil {
			t.Fatalf("store shard hashes workchain %d: %v", workchain, err)
		}
	}
	if len(shards) > 0 && shardHashes.AsCell() == nil {
		t.Fatal("test shard hashes dictionary is empty")
	}

	configParams := cell.NewDict(32)
	if err := configParams.Set(cell.BeginCell().MustStoreUInt(0, 32).EndCell(), cell.BeginCell().EndCell()); err != nil {
		t.Fatalf("store dummy config param: %v", err)
	}

	return cell.BeginCell().
		MustStoreUInt(0xcc26, 16).
		MustStoreDict(shardHashes).
		MustStoreSlice(make([]byte, 32), 256).
		MustStoreRef(configParams.AsCell()).
		MustStoreRef(cell.BeginCell().EndCell()).
		MustStoreCoins(0).
		MustStoreDict(nil).
		EndCell()
}

func testShardBinTree(t *testing.T, shards []ton.BlockIDExt) *cell.Cell {
	t.Helper()

	if len(shards) == 0 {
		t.Fatal("empty shard bin tree")
	}
	if len(shards) == 1 {
		return cell.BeginCell().
			MustStoreUInt(0, 1).
			MustStoreBuilder(testShardDescCell(t, shards[0]).ToBuilder()).
			EndCell()
	}

	mid := len(shards) / 2
	return cell.BeginCell().
		MustStoreUInt(1, 1).
		MustStoreRef(testShardBinTree(t, shards[:mid])).
		MustStoreRef(testShardBinTree(t, shards[mid:])).
		EndCell()
}

func testShardDescCell(t *testing.T, shard ton.BlockIDExt) *cell.Cell {
	t.Helper()

	desc := tlb.ShardDesc{
		SeqNo:              shard.SeqNo,
		RootHash:           testBlockHash(shard.RootHash),
		FileHash:           testBlockHash(shard.FileHash),
		NextValidatorShard: shard.Shard,
		SplitMergeAt:       tlb.FutureSplitMergeNone{},
	}
	c, err := tlb.ToCell(desc)
	if err != nil {
		t.Fatalf("build shard desc for %s: %v", storage.FormatBlockRef(shard), err)
	}
	return c
}

func testBlockHash(hash []byte) []byte {
	if len(hash) == 32 {
		return append([]byte(nil), hash...)
	}
	return make([]byte, 32)
}

func testStateBlock(workchain int32, shard int64, seqno uint32) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: workchain,
		Shard:     shard,
		SeqNo:     seqno,
		RootHash:  make([]byte, 32),
		FileHash:  make([]byte, 32),
	}
}

type fakeSource struct {
	mu                 sync.Mutex
	master             ton.BlockIDExt
	initBlock          *ton.BlockIDExt
	zeroBlock          *ton.BlockIDExt
	zeroState          *storage.BlockState
	states             map[string]*storage.BlockState
	keyBlockBatches    map[uint32]p2p.KeyBlockBatch
	downloadedFactory  func(block ton.BlockIDExt) storage.DownloadedState
	errByBlock         map[string]error
	errSequenceByBlock map[string][]error
	downloadCount      map[string]int
	nextKeyBlockCalls  int
	zeroStateCalls     int
}

func (f *fakeSource) InitBlock(context.Context) (ton.BlockIDExt, error) {
	if f.initBlock != nil {
		return *f.initBlock, nil
	}
	return f.master, nil
}

func (f *fakeSource) ZeroStateBlock(context.Context) (ton.BlockIDExt, error) {
	f.zeroStateCalls++
	if f.zeroBlock != nil {
		return *f.zeroBlock, nil
	}
	return f.master, nil
}

func (f *fakeSource) ZeroState(_ context.Context, block ton.BlockIDExt) (storage.DownloadedState, error) {
	if f.zeroState != nil && f.zeroState.Block.Equals(&block) {
		return newImmediateDownloadedState(f.zeroState), nil
	}
	return nil, errors.New("unexpected zero state download")
}

func (f *fakeSource) NextKeyBlocks(_ context.Context, from ton.BlockIDExt, _ int32) (p2p.KeyBlockBatch, error) {
	f.nextKeyBlockCalls++
	if f.keyBlockBatches == nil {
		return p2p.KeyBlockBatch{}, nil
	}
	return f.keyBlockBatches[from.SeqNo], nil
}

func (f *fakeSource) InitBlockProof(context.Context, ton.BlockIDExt) (p2p.ProofDownload, error) {
	return p2p.ProofDownload{}, errors.New("unexpected init block proof download")
}

func (f *fakeSource) MasterchainProof(context.Context, ton.BlockIDExt, bool) ([]byte, error) {
	return nil, errors.New("unexpected masterchain proof download")
}

func (f *fakeSource) DownloadState(_ context.Context, block ton.BlockIDExt, _ ton.BlockIDExt, _ uint32) (storage.DownloadedState, error) {
	key := storage.BlockKey(block)

	f.mu.Lock()
	defer f.mu.Unlock()

	if sequence := f.errSequenceByBlock[key]; len(sequence) > 0 {
		err := sequence[0]
		f.errSequenceByBlock[key] = sequence[1:]
		if err != nil {
			return nil, err
		}
	}
	if err := f.errByBlock[key]; err != nil {
		return nil, err
	}
	if f.downloadCount == nil {
		f.downloadCount = map[string]int{}
	}
	f.downloadCount[key]++

	state := f.states[key]
	if state == nil {
		return nil, context.Canceled
	}
	if f.downloadedFactory != nil {
		return f.downloadedFactory(block), nil
	}
	return newImmediateDownloadedState(storage.CloneBlockState(state)), nil
}

type trackingDownloadedState struct {
	block          ton.BlockIDExt
	currentDecodes *atomic.Int32
	maxDecodes     *atomic.Int32
	delay          time.Duration
}

func (d *trackingDownloadedState) Block() ton.BlockIDExt {
	return d.block
}

func (d *trackingDownloadedState) Decode(ctx context.Context) (*storage.BlockState, error) {
	current := d.currentDecodes.Add(1)
	defer d.currentDecodes.Add(-1)

	for {
		max := d.maxDecodes.Load()
		if current <= max || d.maxDecodes.CompareAndSwap(max, current) {
			break
		}
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(d.delay):
	}

	return &storage.BlockState{
		Block: d.block,
		Cell:  cell.BeginCell().EndCell(),
	}, nil
}

func (d *trackingDownloadedState) ImportCells(ctx context.Context, _ storage.StateCellTreeImporter) (*storage.BlockState, error) {
	return d.Decode(ctx)
}

func (d *trackingDownloadedState) Cleanup() error {
	return nil
}

type immediateDownloadedState struct {
	state *storage.BlockState
}

func newImmediateDownloadedState(state *storage.BlockState) storage.DownloadedState {
	return &immediateDownloadedState{state: storage.CloneBlockState(state)}
}

func (d *immediateDownloadedState) Block() ton.BlockIDExt {
	if d.state == nil {
		return ton.BlockIDExt{}
	}
	return d.state.Block
}

func (d *immediateDownloadedState) Decode(context.Context) (*storage.BlockState, error) {
	return storage.CloneBlockState(d.state), nil
}

func (d *immediateDownloadedState) ImportCells(ctx context.Context, _ storage.StateCellTreeImporter) (*storage.BlockState, error) {
	return d.Decode(ctx)
}

func (d *immediateDownloadedState) Cleanup() error {
	d.state = nil
	return nil
}
