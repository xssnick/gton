package liveview

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

// THE RULE: every entry point that installs a block state raises the artifacts
// edge before it releases the lock.
//
// It exists because the reader on the other side is edge-triggered. LocalSessionBackend
// .loadChainTip takes the signal, reads, and blocks only if the read failed, so a
// publication path that makes a state readable WITHOUT raising the signal is not a
// slow path — it is a blind wait until the 30 s backstop fires, which is precisely
// the shape of the silent standstill this whole change came from. Since the signal
// carries no information about which block arrived, raising it too often costs a
// reader one extra read; not raising it costs it the backstop.
//
// SetLiveCurrentStateSnapshot is the one that was missing: it installs the state of
// every block the adopted snapshot names, through publishPendingCurrentLocked ->
// rememberCurrentBlockStatesLocked, and raised nothing. A per-call-site test would
// not have found it, which is why this one is structural: it walks the package's own
// call graph, so a new installer added tomorrow is caught by the same rule.
func TestEveryBlockStateInstallerRaisesTheArtifactsEdge(t *testing.T) {
	graph := packageCallGraph(t)
	installing := graph.stateInstallers()

	// The walk is deliberately narrow: it follows a call into an installer only
	// while it stays inside the critical section, which by this package's naming
	// convention is a callee whose name ends in Locked. That is what makes the
	// answer mean "this lock hold installs a state" rather than "this function
	// eventually causes one somewhere" — SetLiveCurrentStateSnapshot ends by calling
	// promoteNonfinalWaiting, which takes the lock again for OTHER blocks and
	// signals there, and a transitive walk would have accepted that as coverage for
	// the states this call installed.
	for _, want := range []string{
		"publishLiveBlockArtifacts",
		"publishNonfinalBlockArtifacts",
		"SetLiveCurrentStateSnapshot",
		"MarkLiveCurrentStateFlushed",
	} {
		if _, ok := installing[want]; !ok {
			t.Fatalf("the walk does not see %s installing a state, so it proves nothing", want)
		}
	}
	if _, ok := installing["cachedBlockState"]; ok {
		t.Fatal("the walk reports a pure read as an installer, so its answers are meaningless")
	}
	if _, ok := installing["promoteNonfinalWaiting"]; ok {
		t.Fatal("the walk crossed a second lock acquisition, so it cannot tell coverage from coincidence")
	}

	for name := range installing {
		if strings.HasSuffix(name, "Locked") {
			// Not a lock holder: its own caller is where the section is.
			continue
		}
		if graph.signalsWithin(name, installing) {
			continue
		}
		t.Errorf(
			"%s installs a block state without raising signalBlockArtifactsLocked in the same "+
				"critical section: an edge-triggered reader (LocalSessionBackend.loadChainTip) "+
				"would wait blind for its 30 s backstop instead of for this publication",
			name,
		)
	}
}

// And the behavioural half of the same rule for the installer that was missing it:
// the snapshot alone, with no block publication of its own, must raise the edge.
func TestCurrentStateSnapshotRaisesTheArtifactsEdge(t *testing.T) {
	live, _ := acceptedStateStore(t)

	master := storage.BlockState{Block: testLiveBlockID(-1, masterchainShard, 910, 0xe1)}
	masterRoot := cell.BeginCell().MustStoreUInt(0xe1, 16).EndCell()
	master.StateRootHash = masterRoot.Hash()
	master.Cell = masterRoot
	shard := storage.BlockState{Block: testLiveBlockID(0, acceptedStateShardID(), acceptedStateAppliedSeqno+4, 0xe0)}
	shardRoot := cell.BeginCell().MustStoreUInt(0xe0, 16).EndCell()
	shard.StateRootHash = shardRoot.Hash()
	shard.Cell = shardRoot

	// Block markers only — no state — so the snapshot below is the ONE thing that
	// makes these states readable. That is the shape the missing edge mattered in.
	for _, state := range []storage.BlockState{master, shard} {
		if err := live.PublishLiveBlockArtifacts(storage.LiveBlockArtifacts{
			Block:           state.Block,
			Meta:            &storage.BlockMeta{ID: state.Block, GenUTime: 1_720_000_100},
			ArtifactFlushed: true,
		}); err != nil {
			t.Fatalf("publish the block marker: %v", err)
		}
	}
	if _, err := live.BlockState(context.Background(), shard.Block); err == nil {
		t.Fatal("the marker already made the state readable, so the snapshot is not what installs it")
	}

	signal := live.BlockArtifactsSignal()
	live.SetLiveCurrentState(&storage.CurrentState{
		Masterchain: master,
		Shards: map[storage.ShardKey]storage.BlockState{
			{Workchain: 0, Shard: acceptedStateShardID()}: shard,
		},
	})

	select {
	case <-signal:
	default:
		t.Fatal("the current-state snapshot raised no artifacts edge, so an edge-triggered reader waits blind")
	}
	// The states really did become readable through this call and nothing else, which
	// is what makes the missing edge a hole rather than a cosmetic omission.
	if _, err := live.BlockState(context.Background(), shard.Block); err != nil {
		t.Fatalf("the snapshot did not install the shard state it names: %v", err)
	}
}

// storeCallGraph is the intra-package call graph of *Store methods and package
// functions, by name. Names are unique enough inside one package for this, and a
// name-based graph is what keeps the walk hermetic — nothing outside the package is
// followed.
type storeCallGraph struct {
	calls map[string]map[string]struct{}
}

// stateInstallers is every function that writes a block state into s.states within
// its caller's critical section: the two writers themselves, and then upwards
// through Locked callees only, which is where one lock hold ends in this package.
func (g *storeCallGraph) stateInstallers() map[string]struct{} {
	installing := map[string]struct{}{
		"putBlockLocked":           {},
		"rememberBlockStateLocked": {},
	}
	for {
		grown := false
		for from, callees := range g.calls {
			if _, known := installing[from]; known {
				continue
			}
			for callee := range callees {
				if _, ok := installing[callee]; !ok || !strings.HasSuffix(callee, "Locked") {
					continue
				}
				installing[from] = struct{}{}
				grown = true
				break
			}
		}
		if !grown {
			return installing
		}
	}
}

// signalsWithin reports that this lock holder raises the artifacts edge itself, or
// that it is a wrapper around one that does. The wrapper case is real and narrow:
// SetLiveCurrentState hands to SetLiveCurrentStateSnapshot, and PublishLiveBlockArtifacts
// to publishLiveBlockArtifacts, without holding the lock across either.
func (g *storeCallGraph) signalsWithin(name string, installing map[string]struct{}) bool {
	const signal = "signalBlockArtifactsLocked"
	if _, direct := g.calls[name][signal]; direct {
		return true
	}
	for callee := range g.calls[name] {
		if strings.HasSuffix(callee, "Locked") {
			continue
		}
		if _, installs := installing[callee]; !installs {
			continue
		}
		if g.signalsWithin(callee, installing) {
			return true
		}
	}

	return false
}

func packageCallGraph(t *testing.T) *storeCallGraph {
	t.Helper()

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read the package directory: %v", err)
	}
	graph := &storeCallGraph{calls: map[string]map[string]struct{}{}}
	fileSet := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		parsed, parseErr := parser.ParseFile(fileSet, name, nil, 0)
		if parseErr != nil {
			t.Fatalf("parse %s: %v", name, parseErr)
		}
		for _, decl := range parsed.Decls {
			function, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			from := function.Name.Name
			if graph.calls[from] == nil {
				graph.calls[from] = map[string]struct{}{}
			}
			ast.Inspect(function.Body, func(node ast.Node) bool {
				call, isCall := node.(*ast.CallExpr)
				if !isCall {
					return true
				}
				switch fun := call.Fun.(type) {
				case *ast.Ident:
					graph.calls[from][fun.Name] = struct{}{}
				case *ast.SelectorExpr:
					graph.calls[from][fun.Sel.Name] = struct{}{}
				}
				return true
			})
		}
	}
	return graph
}
