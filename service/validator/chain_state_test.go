package validator

import (
	"bytes"
	"crypto/sha256"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/gton/service/shard"
	"github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// testTipBlock is the stand-in block root of a fixture chain tip. newChainState
// requires an ordinary tip to carry a parsed root that hashes to ID.RootHash,
// so a fixture block id has to be derived from a cell instead of an arbitrary
// byte pattern. The marker keeps the mapping injective, so ids that used to be
// distinct still are.
func testTipBlock(marker uint64) *cell.Cell {
	return cell.BeginCell().MustStoreUInt(marker, 64).EndCell()
}

// testTipRootHash builds a fixture block id root hash and remembers the root it
// belongs to, so the backend serving that id can hand the cell back.
func testTipRootHash(marker uint64) []byte {
	return registerTestTipBlock(testTipBlock(marker)).Hash()
}

// testTipFileHash is the other half of a fixture block id: the digest of the
// serialization of that same block, which is what a file hash is. Fixture ids
// have to derive it rather than pick a pattern, because a chain tip's bytes and
// its parsed root are reconciled through exactly this digest — a tip whose
// BlockBOC is not the block its id names is the defect the reconciliation
// exists to catch, and fixtures that carry one only prove the check works.
func testTipFileHash(marker uint64) []byte {
	digest := sha256.Sum256(testTipBlock(marker).ToBOC())

	return digest[:]
}

// testTipBOCFor is what a test backend hands to ChainTip.BlockBOC: the exact
// bytes the block registered for this id was serialized from.
func testTipBOCFor(id ton.BlockIDExt) []byte {
	record, ok := testTipBlocks.Load(string(id.RootHash))
	if !ok || id.SeqNo == 0 {
		return nil
	}

	return record.(testTipBlockRecord).boc
}

// testTipBlocks recovers the root a fixture id was built from, and the exact
// bytes that root's id names. Both halves are stored together because a chain
// tip carries both and they have to be the same block; a fixture that serves
// one without the other is the defect newChainState now refuses. Test backends
// read it from resolver goroutines while the test body still registers roots,
// so it has to be concurrency-safe.
var testTipBlocks sync.Map

type testTipBlockRecord struct {
	root *cell.Cell
	boc  []byte
}

// registerTestTipBlock makes a root built by a fixture findable by its hash,
// including roots that are real block cells rather than markers. The bytes are
// the plain serialization, which is what a fixture id built by testTipFileHash
// names.
func registerTestTipBlock(root *cell.Cell) *cell.Cell {
	return registerTestTipBlockBOC(root, root.ToBOC())
}

// registerTestTipBlockBOC is for a fixture that serializes its own block with
// its own options: the file hash in its id is the digest of those bytes, so
// those are the bytes a backend must serve for it.
func registerTestTipBlockBOC(root *cell.Cell, boc []byte) *cell.Cell {
	testTipBlocks.Store(string(root.Hash()), testTipBlockRecord{root: root, boc: boc})

	return root
}

// testTipBlockFor is what a test backend hands to ChainTip.Block. A zerostate
// tip must carry none, exactly as the production loader leaves it.
func testTipBlockFor(id ton.BlockIDExt) *cell.Cell {
	if id.SeqNo == 0 {
		return nil
	}
	record, ok := testTipBlocks.Load(string(id.RootHash))
	if !ok {
		return nil
	}

	return record.(testTipBlockRecord).root
}

// testStateUpdate builds the transition from parent to a successor derived from
// it. Both sides are complete trees, so the update prunes nothing: a fixture
// that only needs "a transition this parent can be advanced by" gets one
// without having to model what a block changed. The tests that care about
// pruning — the ones about materialization — build their own.
// It panics rather than reporting through testing.TB because fixture backends
// call it from methods that have none, and the only way it can fail is a
// fixture parent that is not a level-0 cell.
func testStateUpdate(parent *cell.Cell, marker uint64) (*cell.Cell, *cell.Cell) {
	successor := cell.BeginCell().MustStoreUInt(marker, 64).MustStoreRef(parent).EndCell()
	update, err := cell.CreateMerkleUpdate(parent, successor)
	if err != nil {
		panic("build a state update from the fixture parent: " + err.Error())
	}

	return update, successor
}

// testValidatedSuccessor is what candidate validation now hands a fixture
// backend: a transition, never a state. The update is built against this exact
// parent root, so a fixture that chains candidates gets a genuine transition
// per parent instead of one that only fits the first.
func testValidatedSuccessor(state *ChainState, artifact *CandidateArtifact) CandidateSuccessor {
	update, successor := testStateUpdate(state.root, uint64(artifact.Candidate.Block.SeqNo)+1)
	// The capsule the verifier hands back in production; without it the
	// announced apply below would take the plain path and the fixture would
	// stop standing for the code it mirrors.
	prepared, err := cell.PrepareMerkleUpdatePlanned(update)
	if err != nil {
		panic(err)
	}

	return CandidateSuccessor{
		BlockRoot:   testTipBlockFor(artifact.Candidate.Block),
		StateUpdate: update,
		Prepared:    prepared,
		StateHash:   successor.HashKeyAt(0),
	}
}

// testCandidateValidation is the fixture form of the backend's success path.
func testCandidateValidation(state *ChainState, artifact *CandidateArtifact) (CandidateValidation, error) {
	next, err := state.validatedCandidateState(artifact, testValidatedSuccessor(state, artifact), nil)
	if err != nil {
		return CandidateValidation{}, err
	}

	return CandidateValidation{State: next}, nil
}

// testValidatedCandidateState is the same, for fixtures that only want the
// successor and treat a failure to build it as a broken fixture.
func testValidatedCandidateState(
	tb testing.TB,
	state *ChainState,
	artifact *CandidateArtifact,
) *ChainState {
	tb.Helper()

	next, err := state.validatedCandidateState(artifact, testValidatedSuccessor(state, artifact), nil)
	if err != nil {
		tb.Fatalf("apply a validated candidate to the fixture parent: %v", err)
	}

	return next
}

func chainStateBlock(shardID int64, seqno uint32, marker uint64) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: 0,
		Shard:     shardID,
		SeqNo:     seqno,
		RootHash:  testTipRootHash(marker),
		FileHash:  testTipFileHash(marker),
	}
}

func chainStateData(blocks ...ton.BlockIDExt) ChainStateData {
	tips := make([]ChainTip, len(blocks))
	for i := range blocks {
		tips[i] = ChainTip{
			ID:    blocks[i],
			Block: testTipBlockFor(blocks[i]),
			State: cell.BeginCell().MustStoreUInt(uint64(i), 1).EndCell(),
		}
		if blocks[i].SeqNo != 0 {
			tips[i].BlockBOC = testTipBOCFor(blocks[i])
		}
	}

	return ChainStateData{Tips: tips}
}

// Every producer of a tip already holds the parsed block root, so a loaded tip
// has to hand it over instead of leaving each candidate validation to decode
// BlockBOC again. The guard lives here rather than on ChainTip so that fixtures
// which never build a ChainState through this constructor keep working.
func TestChainStateTipCarriesItsParsedBlock(t *testing.T) {
	block := chainStateBlock(shard.Root, 10, 0x10)
	zero := chainStateBlock(shard.Root, 0, 0x20)
	request := ChainStateRequest{
		Shard:  groups.ShardID{Workchain: 0, Shard: shard.Root},
		Blocks: []ton.BlockIDExt{block},
	}
	if _, err := newChainState(request, chainStateData(block)); err != nil {
		t.Fatalf("tip carrying its parsed block rejected: %v", err)
	}

	missing := chainStateData(block)
	missing.Tips[0].Block = nil
	if _, err := newChainState(request, missing); err == nil {
		t.Fatal("tip without a parsed block was accepted")
	}

	foreign := chainStateData(block)
	foreign.Tips[0].Block = testTipBlock(0x11)
	if _, err := newChainState(request, foreign); err == nil {
		t.Fatal("tip whose parsed block belongs to another id was accepted")
	}

	zeroRequest := ChainStateRequest{Shard: request.Shard, Blocks: []ton.BlockIDExt{zero}}
	zeroData := chainStateData(zero)
	if _, err := newChainState(zeroRequest, zeroData); err != nil {
		t.Fatalf("zerostate tip rejected: %v", err)
	}
	zeroData.Tips[0].Block = testTipBlock(0x20)
	if _, err := newChainState(zeroRequest, zeroData); err == nil {
		t.Fatal("zerostate tip carrying a block was accepted")
	}

	// The documented case: a Merkle proof handed over as a tip state. This is
	// the shape a caller is most likely to have lying around, and it is the one
	// a level check alone does NOT catch — a proof cell's level mask is its
	// body's shifted right by one, so a proof over a level-0 tree is itself
	// level 0.
	body := cell.BeginCell().MustStoreUInt(0xa1, 8).MustStoreRef(cell.BeginCell().EndCell()).EndCell()
	proof, err := cell.CreateMerkleProof(body)
	if err != nil {
		t.Fatal(err)
	}
	if proof.Level() != 0 || !proof.IsSpecial() {
		t.Fatalf("fixture proof is level %d special=%v, want the level-0 special case", proof.Level(), proof.IsSpecial())
	}
	proofTip := chainStateData(block)
	proofTip.Tips[0].State = proof
	if _, err = newChainState(request, proofTip); err == nil {
		t.Fatal("a Merkle proof was accepted as a tip state")
	}

	// And a level-bearing state, which is not a source ApplyMerkleUpdate can
	// walk either. Note what neither case proves: a proof virtualized into an
	// ordinary tree is level 0 and not special, so passing here says nothing
	// about the tip state being a full live tree.
	pruned, err := cell.CreatePrunedBranch(
		cell.BeginCell().MustStoreRef(cell.BeginCell().EndCell()).EndCell(),
		1,
		0,
	)
	if err != nil {
		t.Fatal(err)
	}
	levelled := chainStateData(block)
	levelled.Tips[0].State = cell.BeginCell().MustStoreRef(pruned).EndCell()
	if _, err = newChainState(request, levelled); err == nil {
		t.Fatal("tip state above level 0 was accepted")
	}

	// The two block halves have to be the same block: a tip whose bytes are not
	// the block its id names is refused, even when the parsed root is right.
	forgedBOC := chainStateData(block)
	forgedBOC.Tips[0].BlockBOC = append([]byte{0x00}, forgedBOC.Tips[0].BlockBOC...)
	if _, err = newChainState(request, forgedBOC); err == nil {
		t.Fatal("a tip carrying block data from another block was accepted")
	}
}

func TestChainStateRequiresDirectSplitParent(t *testing.T) {
	left, err := shard.Child(shard.Root, true)
	if err != nil {
		t.Fatal(err)
	}
	leftLeft, err := shard.Child(left, true)
	if err != nil {
		t.Fatal(err)
	}
	parent := chainStateBlock(shard.Root, 10, 0x10)

	directRequest := ChainStateRequest{
		Shard:  groups.ShardID{Workchain: 0, Shard: left},
		Blocks: []ton.BlockIDExt{parent},
	}
	state, err := newChainState(directRequest, chainStateData(parent))
	if err != nil {
		t.Fatalf("direct parent rejected: %v", err)
	}
	if _, err = state.NormalBlock(); err == nil {
		t.Fatal("before-split topology was exposed as a normal tip")
	}

	deepRequest := directRequest
	deepRequest.Shard.Shard = leftLeft
	if _, err = newChainState(deepRequest, chainStateData(parent)); err == nil {
		t.Fatal("arbitrary ancestor was accepted as a split predecessor")
	}
}

func TestChainStateMergeShapeMatchesReference(t *testing.T) {
	left, err := shard.Child(shard.Root, true)
	if err != nil {
		t.Fatal(err)
	}
	right, err := shard.Child(shard.Root, false)
	if err != nil {
		t.Fatal(err)
	}
	leftBlock := chainStateBlock(left, 11, 0x21)
	rightBlock := chainStateBlock(right, 12, 0x31)
	request := ChainStateRequest{
		Shard:  groups.ShardID{Workchain: 0, Shard: shard.Root},
		Blocks: []ton.BlockIDExt{leftBlock, rightBlock},
	}
	data := chainStateData(leftBlock, rightBlock)
	merged, err := newChainState(request, data)
	if err != nil {
		t.Fatalf("ordered merge children rejected: %v", err)
	}

	// Both sides apply a merge candidate's update to this root — the collator to
	// the one it builds over the predecessor list, this runtime to the one it
	// keeps as the state root — so a shape that differed by a bit would make one
	// of the two applies fail against a parent the other accepted. The
	// constructor is shared; this is what catches a future hand-rolled copy of
	// it here.
	reference, err := collator.MergedPredecessorStates(data.Tips[0].State, data.Tips[1].State)
	if err != nil {
		t.Fatal(err)
	}
	if merged.root.HashKeyAt(0) != reference.HashKeyAt(0) {
		t.Fatal("the merge root differs from the one the collator applies updates to")
	}
	if !bytes.Equal(merged.root.ToBOC(), reference.ToBOC()) {
		t.Fatal("the merge root is not byte-identical to the collator's")
	}

	reversed := ChainStateRequest{Shard: request.Shard, Blocks: []ton.BlockIDExt{rightBlock, leftBlock}}
	if _, err = newChainState(reversed, chainStateData(rightBlock, leftBlock)); err == nil {
		t.Fatal("reversed merge children were accepted")
	}

	zeroLeft := chainStateBlock(left, 0, 0x41)
	zeroRequest := ChainStateRequest{Shard: request.Shard, Blocks: []ton.BlockIDExt{zeroLeft, rightBlock}}
	zeroData := chainStateData(zeroLeft, rightBlock)
	if _, err = newChainState(zeroRequest, zeroData); err == nil {
		t.Fatal("zero merge predecessor was accepted")
	}
}

func TestCandidateGenerationTimeUsesConsensusExtraData(t *testing.T) {
	want := time.UnixMilli(1_765_432_109_876)
	extra := cell.BeginCell().
		MustStoreUInt(consensusExtraDataTag, 32).
		MustStoreUInt(0, 32).
		MustStoreUInt(uint64(want.UnixMilli()), 64).
		EndCell()
	boc, err := cell.ToBOCWithOptionsErr([]*cell.Cell{extra}, cell.BOCSerializeOptions{WithCRC32C: true})
	if err != nil {
		t.Fatal(err)
	}
	got, err := candidateGenUtime(boc)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Equal(want) {
		t.Fatalf("candidate generation time = %v, want %v", got, want)
	}
}
