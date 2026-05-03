package state

import (
	"context"
	"errors"
	"flexserver/service/p2p"
	"flexserver/service/storage"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const topShard = int64(-1 << 63)

func TestSyncerSyncCurrentStoresSnapshot(t *testing.T) {
	master := ton.BlockIDExt{Workchain: -1, Shard: topShard, SeqNo: 100}
	base := ton.BlockIDExt{Workchain: 0, Shard: topShard, SeqNo: 200}
	aux := ton.BlockIDExt{Workchain: 0, Shard: int64(0x4000000000000000), SeqNo: 201}

	now := time.Now()
	source := &fakeSource{
		master: master,
		shards: []ton.BlockIDExt{base, aux},
		states: map[string]*storage.BlockState{
			storage.BlockKey(master): {Block: master, Cell: cell.BeginCell().EndCell(), DownloadedAt: now},
			storage.BlockKey(base):   {Block: base, Cell: cell.BeginCell().EndCell(), DownloadedAt: now},
			storage.BlockKey(aux):    {Block: aux, Cell: cell.BeginCell().EndCell(), DownloadedAt: now},
		},
	}

	store := newTestStateStore()
	syncer := NewSyncer(source, store)

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

	if got := source.downloadCount[storage.BlockKey(master)]; got != 1 {
		t.Fatalf("expected master state to be downloaded once, got %d", got)
	}
	if got := source.latestCalls; got != 0 {
		t.Fatalf("expected latest masterchain lookup to be skipped, got %d", got)
	}
}

func TestSyncerReturnsShardDownloadError(t *testing.T) {
	master := ton.BlockIDExt{Workchain: -1, Shard: topShard, SeqNo: 10}
	shard := ton.BlockIDExt{Workchain: 0, Shard: topShard, SeqNo: 11}

	source := &fakeSource{
		master: master,
		shards: []ton.BlockIDExt{shard},
		states: map[string]*storage.BlockState{
			storage.BlockKey(master): {Block: master},
		},
		errByBlock: map[string]error{
			storage.BlockKey(shard): context.DeadlineExceeded,
		},
	}

	syncer := NewSyncer(source, newTestStateStore())

	if _, err := syncer.SyncCurrent(context.Background()); err == nil {
		t.Fatal("expected sync error")
	}
}

func TestSyncerRetriesSelectedStateWithoutReselectingKeyBlock(t *testing.T) {
	master := ton.BlockIDExt{Workchain: -1, Shard: topShard, SeqNo: 10}
	source := &fakeSource{
		master: master,
		states: map[string]*storage.BlockState{
			storage.BlockKey(master): {Block: master},
		},
		errSequenceByBlock: map[string][]error{
			storage.BlockKey(master): {p2p.ErrStateNotAvailable},
		},
	}

	syncer := NewSyncer(source, newTestStateStore())

	snapshot, err := syncer.SyncCurrent(context.Background())
	if err != nil {
		t.Fatalf("sync current: %v", err)
	}
	if snapshot.Masterchain.Block.SeqNo != master.SeqNo {
		t.Fatalf("unexpected masterchain seqno %d", snapshot.Masterchain.Block.SeqNo)
	}
	if source.trustedInitCalls != 1 {
		t.Fatalf("expected one trusted init lookup, got %d", source.trustedInitCalls)
	}
}

func TestSyncerRetriesShardStateWithoutRepeatingMasterStage(t *testing.T) {
	master := ton.BlockIDExt{Workchain: -1, Shard: topShard, SeqNo: 10}
	shard := ton.BlockIDExt{Workchain: 0, Shard: topShard, SeqNo: 11}

	source := &fakeSource{
		master: master,
		shards: []ton.BlockIDExt{shard},
		states: map[string]*storage.BlockState{
			storage.BlockKey(master): {Block: master},
			storage.BlockKey(shard):  {Block: shard},
		},
		errSequenceByBlock: map[string][]error{
			storage.BlockKey(shard): {p2p.ErrStateNotAvailable},
		},
	}

	syncer := NewSyncer(source, newTestStateStore())
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
	if got := source.downloadCount[storage.BlockKey(master)]; got != 1 {
		t.Fatalf("expected master state to be downloaded once, got %d", got)
	}
	if got := source.trustedInitCalls; got != 1 {
		t.Fatalf("expected one trusted init lookup, got %d", got)
	}
}

func TestSyncerUsesStoredShardStateWithoutCurrentCheckpoint(t *testing.T) {
	master := ton.BlockIDExt{Workchain: -1, Shard: topShard, SeqNo: 10}
	shard := ton.BlockIDExt{Workchain: 0, Shard: topShard, SeqNo: 11}

	source := &fakeSource{
		master: master,
		shards: []ton.BlockIDExt{shard},
		errByBlock: map[string]error{
			storage.BlockKey(shard): errors.New("shard should not be downloaded"),
		},
	}

	store := newTestStateStore()
	if err := store.SaveBlockState(context.Background(), &storage.BlockState{Block: master}); err != nil {
		t.Fatalf("save stored master: %v", err)
	}
	if err := store.SaveBlockState(context.Background(), &storage.BlockState{Block: shard}); err != nil {
		t.Fatalf("save stored shard: %v", err)
	}

	syncer := NewSyncer(source, store)
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
	pendingMaster := ton.BlockIDExt{Workchain: -1, Shard: topShard, SeqNo: 10}
	newerMaster := ton.BlockIDExt{Workchain: -1, Shard: topShard, SeqNo: 20}
	doneShard := ton.BlockIDExt{Workchain: 0, Shard: topShard, SeqNo: 11}
	missingShard := ton.BlockIDExt{Workchain: 0, Shard: int64(0x4000000000000000), SeqNo: 12}

	source := &fakeSource{
		master: newerMaster,
		shards: []ton.BlockIDExt{doneShard, missingShard},
		states: map[string]*storage.BlockState{
			storage.BlockKey(missingShard): {Block: missingShard},
		},
		errByBlock: map[string]error{
			storage.BlockKey(doneShard): errors.New("completed shard should not be downloaded"),
		},
	}

	store := newTestStateStore()
	if err := store.SaveBlockState(context.Background(), &storage.BlockState{Block: pendingMaster}); err != nil {
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

	syncer := NewSyncer(source, store)
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
	if got := source.latestCalls; got != 0 {
		t.Fatalf("expected latest masterchain lookup to be skipped, got %d", got)
	}
	if got := source.trustedInitCalls; got != 0 {
		t.Fatalf("expected trusted init lookup to be skipped, got %d", got)
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

func TestSyncerIgnoresIncompleteStoredMasterchainState(t *testing.T) {
	storedMaster := ton.BlockIDExt{Workchain: -1, Shard: topShard, SeqNo: 10}
	newerMaster := ton.BlockIDExt{Workchain: -1, Shard: topShard, SeqNo: 20}
	shard := ton.BlockIDExt{Workchain: 0, Shard: topShard, SeqNo: 21}

	source := &fakeSource{
		master: newerMaster,
		shards: []ton.BlockIDExt{shard},
		states: map[string]*storage.BlockState{
			storage.BlockKey(newerMaster): {Block: newerMaster},
			storage.BlockKey(shard):       {Block: shard},
		},
	}

	store := newTestStateStore()
	if err := store.SaveBlockState(context.Background(), &storage.BlockState{Block: storedMaster}); err != nil {
		t.Fatalf("save stored master: %v", err)
	}

	syncer := NewSyncer(source, store)
	snapshot, err := syncer.SyncCurrent(context.Background())
	if err != nil {
		t.Fatalf("sync current: %v", err)
	}
	if !snapshot.Masterchain.Block.Equals(&newerMaster) {
		t.Fatalf("expected selected master %s, got %s", storage.FormatBlockRef(newerMaster), storage.FormatBlockRef(snapshot.Masterchain.Block))
	}
	if got := source.downloadCount[storage.BlockKey(newerMaster)]; got != 1 {
		t.Fatalf("expected newer master to be downloaded once, got %d", got)
	}
	if got := source.trustedInitCalls; got != 1 {
		t.Fatalf("expected trusted init lookup, got %d", got)
	}
}

func TestSyncerSerializesShardStateDecodeAndPersist(t *testing.T) {
	master := ton.BlockIDExt{Workchain: -1, Shard: topShard, SeqNo: 10}
	shardA := ton.BlockIDExt{Workchain: 0, Shard: topShard, SeqNo: 11}
	shardB := ton.BlockIDExt{Workchain: 0, Shard: int64(0x4000000000000000), SeqNo: 12}

	var currentDecodes atomic.Int32
	var maxDecodes atomic.Int32

	source := &fakeSource{
		master: master,
		shards: []ton.BlockIDExt{shardA, shardB},
		states: map[string]*storage.BlockState{
			storage.BlockKey(master): {Block: master},
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

	syncer := NewSyncer(source, newTestStateStore())
	if _, err := syncer.SyncCurrent(context.Background()); err != nil {
		t.Fatalf("sync current: %v", err)
	}
	if got := maxDecodes.Load(); got != 1 {
		t.Fatalf("expected serialized decode, got max concurrent decodes %d", got)
	}
}

func TestSyncerWalksKeyBlocksToTailWithoutLatestLookup(t *testing.T) {
	init := ton.BlockIDExt{Workchain: -1, Shard: topShard, SeqNo: 10}
	key20 := ton.BlockIDExt{Workchain: -1, Shard: topShard, SeqNo: 20}
	key30 := ton.BlockIDExt{Workchain: -1, Shard: topShard, SeqNo: 30}
	key40 := ton.BlockIDExt{Workchain: -1, Shard: topShard, SeqNo: 40}

	source := &fakeSource{
		master: init,
		keyBlockBatches: map[uint32]p2p.KeyBlockBatch{
			10: {Blocks: []ton.BlockIDExt{key20, key30}},
			30: {Blocks: []ton.BlockIDExt{key40}, Incomplete: true},
			40: {Incomplete: true},
		},
	}

	syncer := NewSyncer(source, newTestStateStore())
	blocks, err := syncer.keyBlockIDs(context.Background(), init)
	if err != nil {
		t.Fatalf("walk key blocks: %v", err)
	}

	if got, want := len(blocks), 4; got != want {
		t.Fatalf("unexpected key block count %d, want %d", got, want)
	}
	if !blocks[0].Equals(&init) || !blocks[1].Equals(&key20) || !blocks[2].Equals(&key30) || !blocks[3].Equals(&key40) {
		t.Fatalf("unexpected key block chain %#v", blocks)
	}
	if got := source.nextKeyBlockCalls; got != 3 {
		t.Fatalf("unexpected next key block calls %d", got)
	}
	if got := source.latestCalls; got != 0 {
		t.Fatalf("expected latest masterchain lookup to be skipped, got %d", got)
	}
}

type fakeSource struct {
	mu                 sync.Mutex
	master             ton.BlockIDExt
	shards             []ton.BlockIDExt
	states             map[string]*storage.BlockState
	keyBlockBatches    map[uint32]p2p.KeyBlockBatch
	downloadedFactory  func(block ton.BlockIDExt) storage.DownloadedState
	errByBlock         map[string]error
	errSequenceByBlock map[string][]error
	downloadCount      map[string]int
	nextKeyBlockCalls  int
	latestCalls        int
	trustedInitCalls   int
}

func (f *fakeSource) LatestMasterchainBlock(context.Context) (ton.BlockIDExt, error) {
	f.latestCalls++
	return f.master, nil
}

func (f *fakeSource) TrustedInitBlock(context.Context) (ton.BlockIDExt, error) {
	f.trustedInitCalls++
	return f.master, nil
}

func (f *fakeSource) NextKeyBlocks(_ context.Context, from ton.BlockIDExt, _ int32) (p2p.KeyBlockBatch, error) {
	f.nextKeyBlockCalls++
	if f.keyBlockBatches == nil {
		return p2p.KeyBlockBatch{}, nil
	}
	return f.keyBlockBatches[from.SeqNo], nil
}

func (f *fakeSource) TrustedInitProof(context.Context, ton.BlockIDExt) ([]byte, error) {
	return nil, errors.New("unexpected trusted init proof download")
}

func (f *fakeSource) MasterchainProof(context.Context, ton.BlockIDExt, bool) ([]byte, error) {
	return nil, errors.New("unexpected masterchain proof download")
}

func (f *fakeSource) ShardBlocks(context.Context, ton.BlockIDExt) ([]ton.BlockIDExt, error) {
	return append([]ton.BlockIDExt(nil), f.shards...), nil
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
