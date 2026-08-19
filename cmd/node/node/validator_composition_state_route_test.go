package node

import (
	"bytes"
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	"github.com/xssnick/gton/service/hooks"
	"github.com/xssnick/gton/service/liveview"
	"github.com/xssnick/gton/service/storage"
	core "github.com/xssnick/gton/service/validator/collator"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// localSessionNode.LoadStateCellTree is the one seam through which both
// collation and validation reach state. These pin that the seam is a
// pass-through, that the collator sits on it rather than beside it, and — the
// property everything else serves — that the two consumers cannot end up with
// different materializations of one parent.

type routedStateStore struct {
	hooks.Store
	serviceCalls int
	root         *cell.Cell
}

func (s *routedStateStore) LoadStateCellTree(context.Context, ton.BlockIDExt, []byte) (*cell.Cell, error) {
	s.serviceCalls++
	return s.root, nil
}

func TestLocalSessionNodeReadsStateThroughTheStore(t *testing.T) {
	root := cell.BeginCell().MustStoreUInt(0x5a, 8).EndCell()
	store := &routedStateStore{root: root}
	node := localSessionNode{store: store}

	loaded, err := node.LoadStateCellTree(context.Background(), ton.BlockIDExt{}, nil)
	if err != nil {
		t.Fatalf("load state cell tree: %v", err)
	}
	// Identity, not equality: the seam must hand back what the store returned,
	// untouched. A transform here — a copy, a rebuild, a rewrap — is invisible
	// to a hash check and fatal to the pointer comparison below.
	if loaded != root {
		t.Fatal("the seam did not return the store's own materialization")
	}
	if store.serviceCalls != 1 {
		t.Fatalf("store calls = %d, want 1", store.serviceCalls)
	}
}

// The collator used to be handed node.Store directly while the validator went
// through localSessionNode. They agreed only because the seam happened to be a
// pass-through; nothing structural said they had to. This pins that the collator
// surface is built ON the seam, so whatever the seam does, both consumers get.
func TestCollatorStateStoreSharesTheValidatorSeam(t *testing.T) {
	root := cell.BeginCell().MustStoreUInt(0x1c, 8).EndCell()
	store := &routedStateStore{root: root}
	artifacts := &countingArtifactStore{}
	collatorStore := localCollatorStateStore{
		localSessionNode: localSessionNode{store: store},
		artifacts:        artifacts,
	}

	loaded, err := collatorStore.LoadStateCellTree(context.Background(), ton.BlockIDExt{}, nil)
	if err != nil {
		t.Fatalf("load state cell tree: %v", err)
	}
	if loaded != root {
		t.Fatal("loaded the wrong cell")
	}
	if store.serviceCalls != 1 {
		t.Fatalf("collator state reads = %d, want 1", store.serviceCalls)
	}

	// BlockRoot and WaitBlockArtifacts are the two reads hooks.Store does not
	// carry. They are straight pass-throughs to the artifact store: blocks are
	// whole BOCs and never enter the decoded cell cache, so there is nothing to
	// decide for them.
	if _, err = collatorStore.BlockRoot(context.Background(), ton.BlockIDExt{}); err != nil {
		t.Fatalf("block root: %v", err)
	}
	if artifacts.blockRoots != 1 {
		t.Fatalf("block root calls = %d, want 1 straight pass-through", artifacts.blockRoots)
	}
	if err = collatorStore.WaitBlockArtifacts(context.Background(), ton.BlockIDExt{}); err != nil {
		t.Fatalf("wait block artifacts: %v", err)
	}
	if artifacts.waits != 1 {
		t.Fatalf("wait calls = %d, want 1", artifacts.waits)
	}
}

type countingArtifactStore struct {
	core.LocalStateStore
	blockRoots int
	waits      int
}

func (s *countingArtifactStore) BlockRoot(context.Context, ton.BlockIDExt) (*cell.Cell, error) {
	s.blockRoots++
	return cell.BeginCell().MustStoreUInt(0xb0, 8).EndCell(), nil
}

func (s *countingArtifactStore) WaitBlockArtifacts(context.Context, ton.BlockIDExt) error {
	s.waits++
	return nil
}

// stableStateStore models a store backed by a decoded cell cache: repeated reads
// of one state return the object the cache already holds, which is where pointer
// identity comes from in the running node.
//
// It also counts, so a divergence can be attributed. Two consumers reading one
// cached state is two calls returning one object; two consumers reading two
// materializations is the failure this exists to catch.
type stableStateStore struct {
	hooks.Store
	root  *cell.Cell
	reads int
}

func newStableStateStore() *stableStateStore {
	return &stableStateStore{root: cell.BeginCell().MustStoreUInt(0x51, 8).EndCell()}
}

func (s *stableStateStore) LoadStateCellTree(context.Context, ton.BlockIDExt, []byte) (*cell.Cell, error) {
	s.reads++
	return s.root, nil
}

// THE COUPLING, pinned directly. ChainState.validatedCandidateState carries a
// verified candidate forward with successor.Live.Over(root, tipStates...), which
// takes the tip states BY POINTER. If the collator and the validator ever
// receive different materializations of one parent, Over refuses, every
// candidate silently costs a full re-apply instead, and nothing fails: no error,
// no metric, no test. This is that test.
//
// It does NOT assert how the state is reached — that is deliberately free to
// change. What is not free is the two consumers reaching it differently, whether
// by taking different paths or by passing through a seam that does not return
// the same object twice.
func TestCollatorAndValidatorShareOneParentMaterialization(t *testing.T) {
	store := newStableStateStore()
	localNode := localSessionNode{store: store}
	// Exactly how validator_composition.go builds the two surfaces.
	validatorSurface := localNode
	collatorSurface := localCollatorStateStore{
		localSessionNode: localNode,
		artifacts:        &countingArtifactStore{},
	}

	block := ton.BlockIDExt{Workchain: 0, Shard: int64(-1 << 63), SeqNo: 41}
	validatorRoot, err := validatorSurface.LoadStateCellTree(context.Background(), block, nil)
	if err != nil {
		t.Fatalf("validator state read: %v", err)
	}
	collatorRoot, err := collatorSurface.LoadStateCellTree(context.Background(), block, nil)
	if err != nil {
		t.Fatalf("collator state read: %v", err)
	}

	// Sanity: the divergence this guards against is invisible to a hash check,
	// so if the hashes disagree the fixture is broken, not the code.
	if validatorRoot.HashKey() != collatorRoot.HashKey() {
		t.Fatalf("fixture is broken, the two surfaces carry different bits: validator=%x collator=%x",
			validatorRoot.Hash()[:4], collatorRoot.Hash()[:4])
	}
	// THE assertion.
	if validatorRoot != collatorRoot {
		t.Errorf("the collator and the validator received DIFFERENT materializations of one parent; "+
			"chain_state.go compares tip states by pointer, so every candidate would silently "+
			"cost a full re-apply (store reads=%d)", store.reads)
	}
	// And they came from the store's one cached object, not from anything the
	// seam produced along the way.
	if validatorRoot != store.root {
		t.Error("the surfaces returned something other than the store's materialization")
	}

	// The seam has to be the same object for both, not two copies of one config,
	// or a later change to one surface silently diverges the other.
	if collatorSurface.localSessionNode.store != validatorSurface.store {
		t.Fatal("the collator and the validator are reading through different stores")
	}
}

// THE ORIGINAL FINDING, pinned where a unit test cannot reach. The two tests
// above prove that localCollatorStateStore rides the validator's seam; neither
// proves that the factories BUILD the collator that way. That gap is exactly the
// bug that was found once already — the collator's LocalStateStore bound
// straight to node.Store, agreeing with the validator only by luck — and it is
// invisible to behaviour, because today the seam is a pass-through and both
// paths land on the same object anyway. It only becomes a silent full re-apply
// per candidate once anything is added to the seam.
//
// So it is pinned structurally: whatever local variable each factory passes as
// LocalAcquisitionOptions.Store must be one it built from a
// localCollatorStateStore literal. The check is name-agnostic — rename the
// variable freely — and fails if a factory goes back to handing the collator a
// bare store.
func TestCollatorFactoriesBuildTheCollatorOnTheValidatorSeam(t *testing.T) {
	fileSet := token.NewFileSet()
	parsed, err := parser.ParseFile(fileSet, "validator_composition.go", nil, 0)
	if err != nil {
		t.Fatalf("parse validator_composition.go: %v", err)
	}

	for _, factory := range []string{"newLocalValidatorFactory", "newStandaloneCollatorFactory"} {
		decl := findFuncDecl(t, parsed, factory)
		storeArg := acquisitionStoreArgument(t, decl, factory)
		if !isBuiltFromCollatorStateStore(decl, storeArg) {
			t.Errorf("%s passes %q as the collator's LocalStateStore, but never builds it from a "+
				"localCollatorStateStore literal; the collator would read state off the validator's "+
				"seam and chain_state.go compares parents by pointer", factory, storeArg)
		}
	}
}

// AND THE OTHER ROUTE THE AUDIT NAMED, which neither the graph test in
// service/validator nor the compile-time block beside it can see: a store handed to
// the consensus packages through options or composition. That block asserts only
// that *liveview.Store SATISFIES the boundary; a *pebblestore.Store satisfies the
// same method set, so it could be passed in with both of those tests still green,
// and every consensus read would then go to the committed store and hand back a
// fresh materialization of every parent.
//
// The assertion has to be about the store's IDENTITY, and this is where the store
// is chosen. compositionStore is the look-alike: it satisfies everything the seam
// asks for and is not the live view.
func TestConsensusCompositionRefusesAStoreThatIsNotTheLiveView(t *testing.T) {
	lookalike := &compositionStore{}
	// The premise: it really does satisfy the seam, so nothing else would refuse it.
	if _, ok := hooks.Store(lookalike).(core.LocalStateStore); !ok {
		t.Fatal("the look-alike does not satisfy the collator's read surface, so it is not the shape under test")
	}

	for _, role := range []string{"validator", "collator"} {
		if _, err := consensusLiveView(role, lookalike); err == nil {
			t.Errorf("%s composition accepted a store that is not the live view", role)
		}
	}
	// And the live view itself is accepted, or the check would just be a wall.
	live := liveview.New(nothingBacked{})
	accepted, err := consensusLiveView("validator", live)
	if err != nil {
		t.Fatalf("the composition refused the live view itself: %v", err)
	}
	if accepted != live {
		t.Fatal("the composition returned another store than the one it was given")
	}
}

func findFuncDecl(t *testing.T, file *ast.File, name string) *ast.FuncDecl {
	t.Helper()

	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if ok && fn.Name.Name == name {
			return fn
		}
	}
	t.Fatalf("%s not found in validator_composition.go", name)
	return nil
}

// acquisitionStoreArgument returns the identifier passed as the Store field of
// the core.LocalAcquisitionOptions literal inside fn.
func acquisitionStoreArgument(t *testing.T, fn *ast.FuncDecl, factory string) string {
	t.Helper()

	var name string
	ast.Inspect(fn, func(node ast.Node) bool {
		literal, ok := node.(*ast.CompositeLit)
		if !ok {
			return true
		}
		selector, ok := literal.Type.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != "LocalAcquisitionOptions" {
			return true
		}
		for _, element := range literal.Elts {
			field, ok := element.(*ast.KeyValueExpr)
			if !ok {
				continue
			}
			key, ok := field.Key.(*ast.Ident)
			if !ok || key.Name != "Store" {
				continue
			}
			if value, ok := field.Value.(*ast.Ident); ok {
				name = value.Name
			}
		}
		return true
	})
	if name == "" {
		t.Fatalf("%s: no identifier passed as LocalAcquisitionOptions.Store", factory)
	}
	return name
}

// isBuiltFromCollatorStateStore reports whether name is assigned a
// localCollatorStateStore composite literal anywhere in fn.
func isBuiltFromCollatorStateStore(fn *ast.FuncDecl, name string) bool {
	built := false
	ast.Inspect(fn, func(node ast.Node) bool {
		assign, ok := node.(*ast.AssignStmt)
		if !ok {
			return true
		}
		for i, target := range assign.Lhs {
			ident, ok := target.(*ast.Ident)
			if !ok || ident.Name != name || i >= len(assign.Rhs) {
				continue
			}
			literal, ok := assign.Rhs[i].(*ast.CompositeLit)
			if !ok {
				continue
			}
			if typeName, ok := literal.Type.(*ast.Ident); ok && typeName.Name == "localCollatorStateStore" {
				built = true
			}
		}
		return true
	})
	return built
}

// nothingBacked is a node store with nothing in it. It stands for the measured
// condition: the block is one only local consensus knows about, so no commit is
// coming and nothing below the live view can ever answer.
type nothingBacked struct{}

func (nothingBacked) BlockData(context.Context, ton.BlockIDExt) ([]byte, error) {
	return nil, storage.ErrNotFound
}

func (nothingBacked) BlockProof(context.Context, storage.ServedProofKind, ton.BlockIDExt) ([]byte, error) {
	return nil, storage.ErrNotFound
}

func (nothingBacked) ZeroState(context.Context, ton.BlockIDExt) ([]byte, error) {
	return nil, storage.ErrNotFound
}

func (nothingBacked) CurrentState(context.Context) (*storage.CurrentState, error) {
	return nil, storage.ErrNotFound
}

func (nothingBacked) BlockState(context.Context, ton.BlockIDExt) (*storage.BlockState, error) {
	return nil, storage.ErrNotFound
}

func (nothingBacked) LoadStateCellTree(context.Context, ton.BlockIDExt, []byte) (*cell.Cell, error) {
	return nil, storage.ErrNotFound
}

func (nothingBacked) BlockMeta(context.Context, ton.BlockIDExt) (*storage.BlockMeta, error) {
	return nil, storage.ErrNotFound
}

func (nothingBacked) LookupBlockBySeqNo(context.Context, storage.BlockSeqRef) (ton.BlockIDExt, error) {
	return ton.BlockIDExt{}, storage.ErrNotFound
}

func (nothingBacked) LookupBlockByLT(context.Context, storage.BlockHistoryKey, uint64) (ton.BlockIDExt, error) {
	return ton.BlockIDExt{}, storage.ErrNotFound
}

func (nothingBacked) LookupBlockByAccountLT(context.Context, int32, []byte, uint64) (ton.BlockIDExt, error) {
	return ton.BlockIDExt{}, storage.ErrNotFound
}

func (nothingBacked) LookupBlockByUnixTime(context.Context, storage.BlockHistoryKey, uint32) (ton.BlockIDExt, error) {
	return ton.BlockIDExt{}, storage.ErrNotFound
}

func (nothingBacked) LazyCellLoader() cell.LazyCellLoader { return nil }

// THE COUPLING AGAIN, now across the publication. The tests above pin that both
// consumers read one parent through one seam; this pins that the seam's WRITE
// side does not break it — the state the validator publishes at acceptance is the
// state both of them read back, as the same object.
//
// It runs on the real types: the production localSessionNode and
// localCollatorStateStore over a real live view whose backing has nothing in it,
// which is exactly the case the publication exists for. If the publication ever
// copies or rebuilds the state, the pointer comparison in
// ChainState.validatedCandidateState stops matching and every candidate silently
// costs a full re-apply — no error, no metric, no other test.
func TestPublishedAcceptedStateIsTheSameMaterializationForBothConsumers(t *testing.T) {
	// The non-final cache is ON, because that is how a validator or collator node is
	// wired: startup.go passes cfg.Validator.Enabled || cfg.Collator.Enabled as
	// retainNonfinalShardStates. With it on, the block this node accepts comes back
	// through the ingest as an internal non-final block and the store rebuilds its
	// state from cell records — so a fixture with the cache OFF is vacuous against
	// production, which is what this test used to be.
	live := liveview.New(nothingBacked{}, liveview.Options{NonFinalEnabled: true})
	localNode := localSessionNode{store: live}
	validatorSurface := localNode
	collatorSurface := localCollatorStateStore{
		localSessionNode: localNode,
		// Exactly the capability assertion newLocalValidatorFactory makes.
		artifacts: core.LocalStateStore(live),
	}

	blockRoot := cell.BeginCell().MustStoreUInt(0xb10c, 32).EndCell()
	blockData, err := blockRoot.ToBOCWithOptionsErr(cell.BOCSerializeOptions{WithCRC32C: true})
	if err != nil {
		t.Fatalf("serialize the block: %v", err)
	}
	computed := cell.BeginCell().
		MustStoreUInt(0x57a7e, 32).
		MustStoreRef(cell.BeginCell().MustStoreUInt(0xa1, 8).EndCell()).
		EndCell()
	block := ton.BlockIDExt{
		Workchain: 0,
		Shard:     int64(-1 << 63),
		SeqNo:     371,
		RootHash:  blockRoot.Hash(),
		FileHash:  bytes.Repeat([]byte{0x3b}, 32),
	}

	// The predecessor the ingest republication's Merkle update applies to, published
	// the way the apply pipeline publishes a committed block.
	parentState := cell.BeginCell().MustStoreUInt(0x9a1e, 16).EndCell()
	parent := ton.BlockIDExt{
		Workchain: block.Workchain,
		Shard:     block.Shard,
		SeqNo:     block.SeqNo - 1,
		RootHash:  parentState.Hash(),
		FileHash:  bytes.Repeat([]byte{0x3a}, 32),
	}
	if err = live.PublishLiveBlockArtifacts(storage.LiveBlockArtifacts{
		Block: parent,
		Meta:  &storage.BlockMeta{ID: parent, GenUTime: 1_719_999_999},
		State: &storage.BlockState{
			Block:         parent,
			StateRootHash: parentState.Hash(),
			Cell:          parentState,
		},
		AvailabilityOnly: true,
	}); err != nil {
		t.Fatalf("publish the predecessor: %v", err)
	}

	// Nothing answers before the publication, for either consumer.
	if _, err = validatorSurface.BlockState(context.Background(), block); err == nil {
		t.Fatal("the validator surface answered for a block nothing has published")
	}
	if _, err = collatorSurface.LoadStateCellTree(context.Background(), block, nil); err == nil {
		t.Fatal("the collator surface answered for a block nothing has published")
	}

	if err = validatorSurface.PublishAcceptedBlockState(storage.LiveBlockArtifacts{
		Block:     block,
		Root:      blockRoot,
		BlockData: blockData,
		Meta:      &storage.BlockMeta{ID: block, GenUTime: 1_720_000_000, StartLT: 10, EndLT: 20},
		State: &storage.BlockState{
			Block:         block,
			StateRootHash: computed.Hash(),
			Cell:          computed,
		},
	}); err != nil {
		t.Fatalf("publish the accepted state through the seam: %v", err)
	}

	// THE INGEST, in its production shape. SubmitBlockLocally hands the block to the
	// sync pipeline, which reaches liveview through publishInternalNonfinalShardBlock
	// (service/live_blocks.go:76) with the block root, the block BOC, the parsed meta
	// and the STATE UPDATE — never a state. Left to itself that path rebuilds the
	// state cell from records and installs it, which is a second materialization of
	// this very block, and the assertions below would then be reading it.
	reads := cell.NewReadSet(parentState)
	slice, err := reads.Root().BeginParse()
	if err != nil {
		t.Fatalf("read the predecessor state: %v", err)
	}
	if _, err = slice.LoadUInt(16); err != nil {
		t.Fatalf("read the predecessor state root: %v", err)
	}
	update, err := reads.CreateMerkleUpdate(computed)
	if err != nil {
		t.Fatalf("build the ingest state update: %v", err)
	}
	if err = live.PublishNonfinalBlockArtifacts(storage.LiveBlockArtifacts{
		Block:     block,
		Root:      blockRoot,
		BlockData: blockData,
		Meta: &storage.BlockMeta{
			ID:            block,
			GenUTime:      1_720_000_000,
			StartLT:       10,
			EndLT:         20,
			StateRootHash: computed.Hash(),
			PrevRefs:      []ton.BlockIDExt{parent},
		},
		StateUpdate: update,
		// The fixture's cells are stand-ins rather than a real block and ShardState,
		// so the view build is skipped. Nothing under test is in the view.
		AvailabilityOnly: true,
	}, storage.LiveBlockNonfinalSigned); err != nil {
		t.Fatalf("republish the accepted block through the ingest: %v", err)
	}

	validatorState, err := validatorSurface.BlockState(context.Background(), block)
	if err != nil {
		t.Fatalf("validator state read: %v", err)
	}
	collatorState, err := collatorSurface.BlockState(context.Background(), block)
	if err != nil {
		t.Fatalf("collator state read: %v", err)
	}
	collatorRoot, err := collatorSurface.LoadStateCellTree(context.Background(), block, collatorState.StateRootHash)
	if err != nil {
		t.Fatalf("collator state cells: %v", err)
	}

	// Sanity: the divergence this guards against is invisible to a hash check, so
	// disagreeing hashes mean a broken fixture rather than a broken seam.
	if validatorState.Cell.HashKey() != collatorRoot.HashKey() {
		t.Fatalf("fixture is broken, the two surfaces carry different bits")
	}
	// THE assertions.
	if validatorState.Cell != computed {
		t.Error("the validator reads back a different materialization than it published")
	}
	if collatorRoot != computed {
		t.Error("the collator reads back a different materialization than the validator published")
	}
	if validatorState.Cell != collatorRoot {
		t.Error("the collator and the validator received DIFFERENT materializations of one published parent")
	}
	// And the reads that only the publication can serve for this block.
	if _, err = validatorSurface.BlockData(context.Background(), block); err != nil {
		t.Errorf("block data after the publication: %v", err)
	}
	if _, err = collatorSurface.BlockRoot(context.Background(), block); err != nil {
		t.Errorf("block root after the publication: %v", err)
	}
	if err = collatorSurface.WaitBlockArtifacts(context.Background(), block); err != nil {
		t.Errorf("exact-input wait after the publication: %v", err)
	}
}
