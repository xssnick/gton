package liveview

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// acceptedStateShard is the shard the applied current state is behind on. Its
// applied top is seqno 40, so anything above that is a block only the local
// consensus knows about.
const acceptedStateAppliedSeqno = 40

func acceptedStateShardID() int64 { return int64(1) << 62 }

// acceptedStateStore builds a store whose applied current state stops at
// acceptedStateAppliedSeqno. That is the measured field condition: the shard
// client has not reached the block yet, so backingSeqnoLookupAllowedLocked
// refuses even to ASK the backing for anything above the applied top — no commit
// can make the block readable, only a publication can.
func acceptedStateStore(t *testing.T, opts ...Options) (*Store, *countingBacking) {
	t.Helper()

	backing := &countingBacking{}
	live := New(backing, opts...)
	advanceAppliedShardTop(t, live, acceptedStateAppliedSeqno, 900, 0x60)

	return live, backing
}

// advanceAppliedShardTop publishes an applied current state the way the sync
// pipeline publishes one: the block markers first, flushed, and then the current
// snapshot — which is the only order in which the store actually adopts it.
func advanceAppliedShardTop(t *testing.T, live *Store, shardSeqno, masterSeqno uint32, fill byte) {
	t.Helper()

	master := storage.BlockState{Block: testLiveBlockID(-1, masterchainShard, masterSeqno, fill+1)}
	masterRoot := cell.BeginCell().MustStoreUInt(uint64(fill)+1, 16).EndCell()
	master.StateRootHash = masterRoot.Hash()
	master.Cell = masterRoot

	shard := storage.BlockState{Block: testLiveBlockID(0, acceptedStateShardID(), shardSeqno, fill)}
	shardRoot := cell.BeginCell().MustStoreUInt(uint64(fill), 16).EndCell()
	shard.StateRootHash = shardRoot.Hash()
	shard.Cell = shardRoot

	for _, state := range []storage.BlockState{master, shard} {
		meta, err := storage.BuildBlockMetaFromState(state)
		if err != nil {
			t.Fatalf("build the applied block marker: %v", err)
		}
		if err = live.PublishLiveBlockArtifacts(storage.LiveBlockArtifacts{
			Block:           state.Block,
			Meta:            meta,
			State:           &state,
			ArtifactFlushed: true,
			StateFlushed:    true,
		}); err != nil {
			t.Fatalf("publish the applied block marker: %v", err)
		}
	}
	live.SetLiveCurrentState(&storage.CurrentState{
		Masterchain: master,
		Shards: map[storage.ShardKey]storage.BlockState{
			{Workchain: 0, Shard: acceptedStateShardID()}: shard,
		},
	})
	if _, err := live.CurrentState(context.Background()); err != nil {
		t.Fatalf("the applied current state was not adopted: %v", err)
	}
}

// countingBacking answers nothing and counts being asked. Zero calls is part of
// the point: the block under test is ahead of the applied shard top, so a read
// that reached the backing would be a read that could never succeed.
type countingBacking struct {
	noopBacking
	calls int
}

func (b *countingBacking) BlockState(context.Context, ton.BlockIDExt) (*storage.BlockState, error) {
	b.calls++
	return nil, storage.ErrNotFound
}

func (b *countingBacking) LoadStateCellTree(context.Context, ton.BlockIDExt, []byte) (*cell.Cell, error) {
	b.calls++
	return nil, storage.ErrNotFound
}

type acceptedBlockFixture struct {
	block     ton.BlockIDExt
	root      *cell.Cell
	blockData []byte
	state     *cell.Cell
	proof     []byte
}

func newAcceptedBlockFixture(t *testing.T, seqno uint32, fill byte) acceptedBlockFixture {
	t.Helper()

	root := cell.BeginCell().MustStoreUInt(uint64(fill), 16).MustStoreUInt(uint64(seqno), 32).EndCell()
	data, err := root.ToBOCWithOptionsErr(cell.BOCSerializeOptions{WithCRC32C: true})
	if err != nil {
		t.Fatalf("serialize the fixture block: %v", err)
	}
	state := cell.BeginCell().
		MustStoreUInt(uint64(fill)+0x100, 32).
		MustStoreRef(cell.BeginCell().MustStoreUInt(uint64(seqno), 32).EndCell()).
		EndCell()

	return acceptedBlockFixture{
		block: ton.BlockIDExt{
			Workchain: 0,
			Shard:     acceptedStateShardID(),
			SeqNo:     seqno,
			RootHash:  root.Hash(),
			FileHash:  bytes.Repeat([]byte{fill}, 32),
		},
		root:      root,
		blockData: data,
		state:     state,
		proof:     append([]byte{fill}, 0xaa, 0xbb),
	}
}

func (f acceptedBlockFixture) artifacts() storage.LiveBlockArtifacts {
	return storage.LiveBlockArtifacts{
		Block:     f.block,
		Root:      f.root,
		BlockData: f.blockData,
		Meta:      &storage.BlockMeta{ID: f.block, GenUTime: 1_720_000_000, StartLT: 100, EndLT: 200},
		State: &storage.BlockState{
			Block:         f.block,
			StateRootHash: f.state.Hash(),
			Cell:          f.state,
		},
		Proofs: []storage.LiveBlockProofArtifact{{
			Kind: storage.ServedProofBlockLink,
			Data: f.proof,
		}},
	}
}

// THE READ RULE, on the one case the live view could not answer before. The
// validator and the collator read state only through this store; for a block only
// this node has finalized, the store had nothing and the backing was not even
// consulted. All four reads a predecessor needs must now be served, and the state
// must come back as the very cell the producer computed.
func TestAcceptedStatePublicationServesEveryPredecessorRead(t *testing.T) {
	live, backing := acceptedStateStore(t)
	fixture := newAcceptedBlockFixture(t, acceptedStateAppliedSeqno+7, 0x70)

	// The control first: nothing answers before the publication, and the backing
	// is not even asked, because the block is ahead of the applied shard top.
	if _, err := live.BlockState(context.Background(), fixture.block); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("state of an unpublished block = %v, want not found", err)
	}
	if backing.calls != 0 {
		t.Fatalf("backing was consulted %d times for a block ahead of the applied shard top", backing.calls)
	}

	if err := live.PublishAcceptedBlockState(fixture.artifacts()); err != nil {
		t.Fatalf("publish the accepted block state: %v", err)
	}

	state, err := live.BlockState(context.Background(), fixture.block)
	if err != nil {
		t.Fatalf("state metadata after the publication: %v", err)
	}
	// Identity, not equality: chain_state.go compares tip states BY POINTER for
	// the live-successor carry-back, and a second materialization of one parent
	// silently costs a full re-apply per candidate.
	if state.Cell != fixture.state {
		t.Error("the served state is a different materialization of the published cell")
	}
	tree, err := live.LoadStateCellTree(context.Background(), fixture.block, state.StateRootHash)
	if err != nil {
		t.Fatalf("state cells after the publication: %v", err)
	}
	if tree != fixture.state {
		t.Error("the served state tree is a different materialization of the published cell")
	}
	root, err := live.BlockRoot(context.Background(), fixture.block)
	if err != nil {
		t.Fatalf("block root after the publication: %v", err)
	}
	if !bytes.Equal(root.Hash(), fixture.block.RootHash) {
		t.Error("the served block root is not this block's")
	}
	// The BOC is the read the store's own readiness check does not make, and the
	// one a chain tip cannot do without.
	data, err := live.BlockData(context.Background(), fixture.block)
	if err != nil {
		t.Fatalf("block data after the publication: %v", err)
	}
	if !bytes.Equal(data, fixture.blockData) {
		t.Error("the served block data is not what was published")
	}
	// And the proof link, which is what the next block's shard top description
	// reads.
	proof, err := live.BlockProof(context.Background(), storage.ServedProofBlockLink, fixture.block)
	if err != nil {
		t.Fatalf("proof link after the publication: %v", err)
	}
	if !bytes.Equal(proof, fixture.proof) {
		t.Error("the served proof link is not what was published")
	}
	if backing.calls != 0 {
		t.Fatalf("backing was consulted %d times, want never", backing.calls)
	}
}

// The publication has to end the exact-block wait the collator parks on. That
// wait is edge-triggered, so this also pins that publishing raises the signal.
func TestAcceptedStatePublicationReleasesTheArtifactsWait(t *testing.T) {
	live, _ := acceptedStateStore(t)
	fixture := newAcceptedBlockFixture(t, acceptedStateAppliedSeqno+3, 0x74)

	signal := live.BlockArtifactsSignal()
	waited := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		waited <- live.WaitBlockArtifacts(ctx, fixture.block)
	}()

	if err := live.PublishAcceptedBlockState(fixture.artifacts()); err != nil {
		t.Fatalf("publish the accepted block state: %v", err)
	}
	select {
	case <-signal:
	default:
		t.Fatal("the publication raised no artifacts signal, so an edge-triggered wait would hang")
	}
	select {
	case err := <-waited:
		if err != nil {
			t.Fatalf("wait for the published block: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the wait did not end when the block was published")
	}
}

// LIFETIME, half one: the publication survives while nothing has committed it —
// including the ordinary cache trim, which would otherwise be free to drop it
// once the store flush marks it evictable.
func TestAcceptedStateSurvivesTheOrdinaryCacheTrim(t *testing.T) {
	live, _ := acceptedStateStore(t, Options{
		MasterBlockCache: 4,
		// One evictable shard block. Everything else has to be protected or gone.
		ShardBlockCache: 1,
	})
	fixture := newAcceptedBlockFixture(t, acceptedStateAppliedSeqno+11, 0x78)

	if err := live.PublishAcceptedBlockState(fixture.artifacts()); err != nil {
		t.Fatalf("publish the accepted block state: %v", err)
	}
	// The flush is what makes an ordinary live block evictable. The accepted entry
	// must still be there afterwards: the pipeline has committed the block, but the
	// current state has not reached it, so this is still the state a lineage read
	// will ask for.
	live.MarkLiveBlockFlushed(fixture.block)
	live.MarkLiveBlockStatesFlushed([]ton.BlockIDExt{fixture.block})
	for seqno := uint32(0); seqno < 8; seqno++ {
		other := newAcceptedBlockFixture(t, acceptedStateAppliedSeqno+20+seqno, byte(0x80+seqno))
		artifacts := other.artifacts()
		artifacts.ArtifactFlushed = true
		artifacts.StateFlushed = true
		artifacts.AvailabilityOnly = true
		if err := live.PublishLiveBlockArtifacts(artifacts); err != nil {
			t.Fatalf("publish an ordinary block: %v", err)
		}
	}

	if _, err := live.BlockState(context.Background(), fixture.block); err != nil {
		t.Fatalf("the accepted state was evicted by the ordinary trim: %v", err)
	}
}

// LIFETIME, half two: it is released when the applied current state reaches the
// block, which is the moment the sync pipeline has published its own copy — and
// the live block goes with it, because an uncommitted block nothing else holds
// would otherwise leak.
func TestAcceptedStateIsReleasedWhenTheCurrentStateCatchesUp(t *testing.T) {
	live, _ := acceptedStateStore(t)
	fixture := newAcceptedBlockFixture(t, acceptedStateAppliedSeqno+2, 0x90)

	if err := live.PublishAcceptedBlockState(fixture.artifacts()); err != nil {
		t.Fatalf("publish the accepted block state: %v", err)
	}
	if blocks := live.AcceptedStateBlocks(); len(blocks) != 1 {
		t.Fatalf("accepted publications = %d, want the one just published", len(blocks))
	}

	advanceAppliedShardTop(t, live, fixture.block.SeqNo+1, 901, 0x91)

	if blocks := live.AcceptedStateBlocks(); len(blocks) != 0 {
		t.Fatalf("accepted publications after the current state passed them = %d, want none", len(blocks))
	}
	live.mu.RLock()
	leaked := live.blocks[storage.BlockKey(fixture.block)]
	live.mu.RUnlock()
	if leaked != nil {
		t.Fatal("the released publication left its uncommitted live block behind")
	}
}

// LIFETIME, half three: the set is BOUNDED. An accepted publication is not
// evictable by the ordinary limit, so without its own bound the set would be an
// unbounded pin — exactly the shape a standstill would grow without limit.
func TestAcceptedStatePublicationsAreBounded(t *testing.T) {
	const bound = 3
	live, _ := acceptedStateStore(t, Options{
		MasterBlockCache:   4,
		ShardBlockCache:    64,
		AcceptedStateCache: bound,
	})

	published := make([]acceptedBlockFixture, 0, bound+2)
	for i := uint32(0); i < bound+2; i++ {
		fixture := newAcceptedBlockFixture(t, acceptedStateAppliedSeqno+1+i, byte(0xa0+i))
		if err := live.PublishAcceptedBlockState(fixture.artifacts()); err != nil {
			t.Fatalf("publish accepted block %d: %v", i, err)
		}
		published = append(published, fixture)
	}

	blocks := live.AcceptedStateBlocks()
	if len(blocks) != bound {
		t.Fatalf("accepted publications = %d, want the bound %d", len(blocks), bound)
	}
	// Oldest first out, and the oldest ones are gone completely rather than left
	// pinned with nobody tracking them.
	for _, fixture := range published[:2] {
		if _, err := live.BlockState(context.Background(), fixture.block); !errors.Is(err, storage.ErrNotFound) {
			t.Errorf("block %d was trimmed from the bound but its state is still resident", fixture.block.SeqNo)
		}
	}
	for _, fixture := range published[2:] {
		if _, err := live.BlockState(context.Background(), fixture.block); err != nil {
			t.Errorf("block %d is inside the bound but was released: %v", fixture.block.SeqNo, err)
		}
	}
	// Zero and negative are the default, not "no bound": there is deliberately no
	// way to ask for an unbounded set of uncommitted states.
	if size := acceptedStateCacheSize(0); size != DefaultAcceptedStateCache {
		t.Errorf("zero accepted-state cache = %d, want the default", size)
	}
	if size := acceptedStateCacheSize(-1); size != DefaultAcceptedStateCache {
		t.Errorf("negative accepted-state cache = %d, want the default", size)
	}
}

// ingestArtifacts is what the sync coordinator hands the store when the block
// this node just accepted comes back around as an internal broadcast:
// publishInternalNonfinalShardBlock (service/live_blocks.go:76) passes the block
// root, the block BOC, the parsed meta and the block's STATE UPDATE — never a
// state — so the store rebuilds the state cell from the update. That rebuild is
// the second materialization the accepted publication exists to avoid, and
// building the artifacts in exactly that shape is what keeps the test from being
// vacuous against production.
func (f acceptedBlockFixture) ingestArtifacts(
	t *testing.T,
	parent *cell.Cell,
	prev ton.BlockIDExt,
) storage.LiveBlockArtifacts {
	t.Helper()

	// A real Merkle update from the applied parent to this block's state: the
	// store's own MayApplyMerkleUpdate check against the predecessor has to pass,
	// or the publication would be refused for a reason that has nothing to do with
	// what is under test.
	reads := cell.NewReadSet(parent)
	slice, err := reads.Root().BeginParse()
	if err != nil {
		t.Fatalf("read the parent state: %v", err)
	}
	if _, err = slice.LoadUInt(16); err != nil {
		t.Fatalf("read the parent state root: %v", err)
	}
	update, err := reads.CreateMerkleUpdate(f.state)
	if err != nil {
		t.Fatalf("build the ingest state update: %v", err)
	}
	stateHash := f.state.Hash()

	return storage.LiveBlockArtifacts{
		Block:     f.block,
		Root:      f.root,
		BlockData: f.blockData,
		Meta: &storage.BlockMeta{
			ID:            f.block,
			GenUTime:      1_720_000_000,
			StartLT:       100,
			EndLT:         200,
			StateRootHash: stateHash,
			PrevRefs:      []ton.BlockIDExt{prev},
		},
		StateUpdate: update,
		// The one deviation from the ingest, and it is the same one every test in
		// this package makes: the fixture's block and state cells are stand-ins
		// rather than a real block and a real ShardState, so the view build — which
		// parses both — is skipped. Nothing under test here is in the view: the
		// state rebuild, the cell records and the publish are all upstream of it.
		AvailabilityOnly: true,
	}
}

// appliedShardTopState is the state cell advanceAppliedShardTop published for the
// applied shard top, which is the parent an ingest update has to apply to.
func appliedShardTopState(fill byte) *cell.Cell {
	return cell.BeginCell().MustStoreUInt(uint64(fill), 16).EndCell()
}

// MAJOR 1. The load-bearing claim of the whole change is that the state is
// published BY REFERENCE so there is exactly ONE materialization of a block this
// node finalized. In the wired node the accepted publication is not the last word:
// the block this node just accepted is submitted locally, comes back through the
// ingest as an internal non-final block, and that path REBUILDS the state cell
// from cell records behind a lazy loader. The non-final store is on for every
// validator and collator node (cmd/node/node/startup.go passes
// cfg.Validator.Enabled || cfg.Collator.Enabled as retainNonfinalShardStates), so
// this republication is not a corner case — it is what happens to every block.
//
// This drives that republication in its production shape and pins that the
// accepted publication wins: every door onto the state — BlockState,
// LoadStateCellTree, BlockFragments — must still hand back the very cell the
// producer computed, because chain_state.go compares tip states BY POINTER and a
// second materialization silently costs a full re-apply per candidate.
//
// The control at the end is what makes the rest evidence: the same artifacts,
// published into a store that never saw the accepted publication, DO rebuild a
// different cell. Without that, a fixture that quietly failed to rebuild anything
// would pass this test forever.
func TestAcceptedStateSurvivesTheIngestRepublication(t *testing.T) {
	options := Options{MasterBlockCache: 4, ShardBlockCache: 64, NonFinalEnabled: true}
	live, _ := acceptedStateStore(t, options)
	fixture := newAcceptedBlockFixture(t, acceptedStateAppliedSeqno+1, 0xd0)
	prev := testLiveBlockID(0, acceptedStateShardID(), acceptedStateAppliedSeqno, 0x60)
	ingest := fixture.ingestArtifacts(t, appliedShardTopState(0x60), prev)

	if err := live.PublishAcceptedBlockState(fixture.artifacts()); err != nil {
		t.Fatalf("publish the accepted block state: %v", err)
	}
	if err := live.PublishNonfinalBlockArtifacts(ingest, storage.LiveBlockNonfinalSigned); err != nil {
		t.Fatalf("republish the accepted block through the ingest: %v", err)
	}

	state, err := live.BlockState(context.Background(), fixture.block)
	if err != nil {
		t.Fatalf("state metadata after the ingest republication: %v", err)
	}
	if state.Cell != fixture.state {
		t.Error("the ingest republication replaced the published state with a second materialization")
	}
	tree, err := live.LoadStateCellTree(context.Background(), fixture.block, state.StateRootHash)
	if err != nil {
		t.Fatalf("state cells after the ingest republication: %v", err)
	}
	if tree != fixture.state {
		t.Error("the served state tree is a second materialization of the published cell")
	}
	// The third door onto a state, checked from inside because building a real view
	// needs a real block and a real ShardState. The non-final path builds its view
	// over its own rebuilt tree, so an installed view would hand a reader the
	// second materialization even with the state map intact; none must be there.
	live.mu.RLock()
	installed := live.blocks[storage.BlockKey(fixture.block)]
	live.mu.RUnlock()
	if installed == nil || !installed.acceptedOwner {
		t.Fatal("the ingest republication took ownership of the accepted live block")
	}
	if installed.fragments != nil && installed.fragments.StateRoot() != fixture.state {
		t.Error("the installed block view is built over a second materialization of the published state")
	}
	// The publication also carries what the non-final path deliberately strips.
	if data, err := live.BlockData(context.Background(), fixture.block); err != nil || !bytes.Equal(data, fixture.blockData) {
		t.Errorf("block data after the ingest republication: %v", err)
	}
	if blocks := live.AcceptedStateBlocks(); len(blocks) != 1 {
		t.Fatalf("accepted publications after the republication = %d, want the one published", len(blocks))
	}
	// The one thing the ingest path contributes that acceptance does not: the
	// liteserver's pending-shard-blocks listing.
	signed, _ := live.NonfinalPendingShardBlocks(nil)
	if len(signed) != 1 || !blockIDEqual(signed[0], fixture.block) {
		t.Errorf("pending non-final signed blocks = %v, want the republished block", signed)
	}

	// THE CONTROL: the same ingest artifacts, with no accepted publication in
	// front of them, really do rebuild the state.
	control, _ := acceptedStateStore(t, options)
	if err = control.PublishNonfinalBlockArtifacts(ingest, storage.LiveBlockNonfinalSigned); err != nil {
		t.Fatalf("publish the ingest artifacts without an accepted publication: %v", err)
	}
	rebuilt, err := control.BlockState(context.Background(), fixture.block)
	if err != nil {
		t.Fatalf("state after the ingest publication alone: %v", err)
	}
	if rebuilt.Cell == fixture.state {
		t.Fatal("the ingest fixture does not rebuild the state cell, so it cannot demonstrate the overwrite")
	}
	if !bytes.Equal(rebuilt.StateRootHash, state.StateRootHash) {
		t.Fatal("the rebuilt state is not the same state, so the fixture is wrong rather than the store")
	}
}

// MAJOR 2, ownership. The accepted bookkeeping may only ever drop what it
// published itself.
//
// The shape is the routine one, not a corner case: one masterchain block carries
// a shard top several seqnos ahead, so the applied current state moves from below
// the accepted block to above it WITHOUT ever naming it. coveredByCurrentStateLocked
// is then true while currentRefersToBlockLocked is false. If the sync pipeline has
// published its own copy of that block in between — which is what applying it does
// — the live entry is the pipeline's, and it is unflushed until the next checkpoint
// (the flush delay that was the 13 s median of the measured standstills). Dropping
// it there destroyed a publication the accepted bookkeeping never made, and left
// the store unable to answer for the block at all.
func TestAcceptedStateReleaseLeavesAnotherProducersBlockAlone(t *testing.T) {
	live, _ := acceptedStateStore(t, Options{MasterBlockCache: 4, ShardBlockCache: 64})
	fixture := newAcceptedBlockFixture(t, acceptedStateAppliedSeqno+2, 0xd8)

	if err := live.PublishAcceptedBlockState(fixture.artifacts()); err != nil {
		t.Fatalf("publish the accepted block state: %v", err)
	}
	// The sync pipeline applies the block and publishes its own copy — state,
	// block root, meta — and it is NOT flushed yet.
	pipeline := fixture.artifacts()
	pipelineState := cell.BeginCell().
		MustStoreUInt(uint64(0xd8)+0x100, 32).
		MustStoreRef(cell.BeginCell().MustStoreUInt(uint64(fixture.block.SeqNo), 32).EndCell()).
		EndCell()
	pipeline.State = &storage.BlockState{
		Block:         fixture.block,
		StateRootHash: pipelineState.Hash(),
		Cell:          pipelineState,
	}
	// Stand-in cells again, so the view build is skipped; see ingestArtifacts.
	pipeline.AvailabilityOnly = true
	if !bytes.Equal(pipelineState.Hash(), fixture.state.Hash()) {
		t.Fatal("the pipeline fixture is not the same state, so it does not model the same block")
	}
	if err := live.PublishLiveBlockArtifacts(pipeline); err != nil {
		t.Fatalf("publish the pipeline copy of the block: %v", err)
	}

	// The applied current state jumps PAST the block without naming it.
	advanceAppliedShardTop(t, live, fixture.block.SeqNo+3, 902, 0xd9)

	if blocks := live.AcceptedStateBlocks(); len(blocks) != 0 {
		t.Fatalf("accepted publications after the current state passed them = %d, want none", len(blocks))
	}
	// The hole this closes: both reads answered a moment earlier, and the store
	// cannot answer for the block until its checkpoint flush.
	state, err := live.BlockState(context.Background(), fixture.block)
	if err != nil {
		t.Fatalf("the accepted release destroyed the pipeline's own publication: %v", err)
	}
	if _, err = live.LoadStateCellTree(context.Background(), fixture.block, state.StateRootHash); err != nil {
		t.Fatalf("the accepted release destroyed the pipeline's own state cells: %v", err)
	}
	if _, err = live.BlockRoot(context.Background(), fixture.block); err != nil {
		t.Fatalf("the accepted release destroyed the pipeline's own block root: %v", err)
	}
}

// And the other half of ownership, which is what keeps the release from leaking:
// where the accepted publication IS the only producer, releasing it still takes
// the live block with it, because an uncommitted block nothing else holds is not
// evictable by the ordinary limit and nothing else would ever release it.
func TestAcceptedStateReleaseStillTakesItsOwnBlock(t *testing.T) {
	live, _ := acceptedStateStore(t, Options{MasterBlockCache: 4, ShardBlockCache: 64})
	fixture := newAcceptedBlockFixture(t, acceptedStateAppliedSeqno+2, 0xdc)

	if err := live.PublishAcceptedBlockState(fixture.artifacts()); err != nil {
		t.Fatalf("publish the accepted block state: %v", err)
	}
	advanceAppliedShardTop(t, live, fixture.block.SeqNo+3, 903, 0xdd)

	live.mu.RLock()
	leaked := live.blocks[storage.BlockKey(fixture.block)]
	live.mu.RUnlock()
	if leaked != nil {
		t.Fatal("the released publication left its own uncommitted live block behind")
	}
}

// The existing non-final publish path is NOT a usable vehicle for a validator's
// own state, and this is the measured reason a separate entry point exists: it
// rebuilds the state cell from cell records, which destroys the pointer identity
// chain_state.go compares on, and it deliberately strips the block data a chain
// tip read needs. Both are pinned here so nobody re-routes the publication
// through it.
func TestNonfinalPublicationCannotServeAValidatorState(t *testing.T) {
	live, _ := acceptedStateStore(t, Options{
		MasterBlockCache: 4,
		ShardBlockCache:  64,
		NonFinalEnabled:  true,
	})
	fixture := newAcceptedBlockFixture(t, acceptedStateAppliedSeqno+5, 0xb0)
	artifacts := fixture.artifacts()
	artifacts.Meta.PrevRefs = []ton.BlockIDExt{
		testLiveBlockID(0, acceptedStateShardID(), acceptedStateAppliedSeqno, 0x60),
	}
	artifacts.AvailabilityOnly = true

	if err := live.PublishNonfinalBlockArtifacts(artifacts, storage.LiveBlockNonfinalSigned); err != nil {
		t.Fatalf("publish through the non-final path: %v", err)
	}

	state, err := live.BlockState(context.Background(), fixture.block)
	if err != nil {
		t.Fatalf("non-final state read: %v", err)
	}
	if state.Cell == fixture.state {
		t.Fatal("the non-final path now preserves the cell, so this test no longer explains the separate entry point")
	}
	if _, err = live.BlockData(context.Background(), fixture.block); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("non-final block data = %v, want the stripped not-found this cannot serve", err)
	}
}

// Only a full, ordinary state root may be published. The structural half is what
// can be checked locally: a proof root over a level-0 tree is ITSELF level 0, so a
// level check alone would pass it, and a pruned branch carries a level. Narrowness
// itself is unrepresentable at the producer, which is the real guarantee.
func TestAcceptedStatePublicationRefusesWhatIsNotAFullOrdinaryState(t *testing.T) {
	live, _ := acceptedStateStore(t)
	fixture := newAcceptedBlockFixture(t, acceptedStateAppliedSeqno+9, 0xc0)

	proof, err := cell.CreateMerkleProof(fixture.state)
	if err != nil {
		t.Fatalf("build a proof root: %v", err)
	}
	// The shape worth naming: a proof over a level-0 tree is ITSELF level 0, so a
	// level check alone lets it through. The special-cell half is what catches it.
	if proof.Level() != 0 || !proof.IsSpecial() {
		t.Fatalf("proof fixture is level %d special=%v, so it does not exercise the check", proof.Level(), proof.IsSpecial())
	}
	cases := map[string]func(storage.LiveBlockArtifacts) storage.LiveBlockArtifacts{
		"a proof root": func(a storage.LiveBlockArtifacts) storage.LiveBlockArtifacts {
			a.State = &storage.BlockState{Block: a.Block, StateRootHash: proof.Hash(), Cell: proof}
			return a
		},
		"no state cells": func(a storage.LiveBlockArtifacts) storage.LiveBlockArtifacts {
			a.State = &storage.BlockState{Block: a.Block, StateRootHash: fixture.state.Hash()}
			return a
		},
		"a state for another block": func(a storage.LiveBlockArtifacts) storage.LiveBlockArtifacts {
			other := newAcceptedBlockFixture(t, a.Block.SeqNo+1, 0xc4)
			a.State = &storage.BlockState{Block: other.block, StateRootHash: fixture.state.Hash(), Cell: fixture.state}
			return a
		},
		"no block": func(a storage.LiveBlockArtifacts) storage.LiveBlockArtifacts {
			a.Root = nil
			a.BlockData = nil
			return a
		},
		"the masterchain": func(a storage.LiveBlockArtifacts) storage.LiveBlockArtifacts {
			a.Block.Workchain = -1
			a.Block.Shard = masterchainShard
			a.State.Block = a.Block
			return a
		},
		"a zerostate": func(a storage.LiveBlockArtifacts) storage.LiveBlockArtifacts {
			a.Block.SeqNo = 0
			a.State.Block = a.Block
			return a
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			if err := live.PublishAcceptedBlockState(mutate(fixture.artifacts())); err == nil {
				t.Fatal("published something that is not a full accepted shard state")
			}
			if blocks := live.AcceptedStateBlocks(); len(blocks) != 0 {
				t.Fatalf("a refused publication left %d entries behind", len(blocks))
			}
		})
	}
}
