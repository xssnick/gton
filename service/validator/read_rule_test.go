package validator

import (
	"context"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/xssnick/gton/service/liveview"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/validator/collator"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// THE RULE, pinned structurally rather than one call site at a time.
//
// Every read of the node's block metadata and cells from consensus goes through
// the live view. Where the live view cannot answer, the fix is to extend it — see
// liveview.Store.PublishAcceptedBlockState — and never to fall through to the
// store and never to wait for a commit. Those stores are recoverable by catch-up
// after a restart, so blocking a hot path on their persistence buys nothing and
// costs finality: the measured cost was 28.1% of an 18,047 s run.
//
// A per-call-site test cannot pin that. What can is the package graph: if neither
// consensus package can even reach the store package, no helper, no capability
// assertion and no store handed in through options can route around the live view.

const nodeStorePackage = "github.com/xssnick/gton/service/storage/pebblestore"

const gtonModule = "github.com/xssnick/gton"

// TestConsensusCannotReachTheNodeStoreExceptThroughTheLiveView walks the real
// import graph of the two consensus packages, transitively, and fails if either
// can reach the node's meta DB and celldb implementation at all.
//
// It is deliberately about REACHABILITY and not about imports written in these
// two directories: the defect it exists to prevent is a read that reaches pebble
// through something else — a helper package, an interface satisfied by the raw
// store, a store passed in through options. If the package is not in the graph,
// none of those is possible.
func TestConsensusCannotReachTheNodeStoreExceptThroughTheLiveView(t *testing.T) {
	root := moduleRoot(t)
	for _, pkg := range []string{
		gtonModule + "/service/validator",
		gtonModule + "/service/validator/collator",
	} {
		t.Run(pkg, func(t *testing.T) {
			if chain := importChainTo(t, root, pkg, nodeStorePackage); chain != nil {
				t.Fatalf(
					"%s can reach the node meta DB and celldb: %s\n"+
						"every such read must go through the live view, and where the live view cannot "+
						"answer the fix is to publish into it, not to read the store and not to wait for a commit",
					pkg,
					strings.Join(chain, " -> "),
				)
			}
		})
	}

	// The counter-check that makes the result meaningful: the walk DOES find the
	// package when it is reachable. The node root package is where the store is
	// opened, so it is the one place the answer must be "yes". Without this a
	// broken walk would report "clean" forever.
	if chain := importChainTo(t, root, gtonModule, nodeStorePackage); chain == nil {
		t.Fatal("the import walk found no path from the node root to the node store, so it proves nothing")
	}
}

// And the other half of the rule, at compile time: everything the consensus
// boundary asks a node for is served by the live view ITSELF. That is what makes
// routing every read through it possible in the first place, and it is what
// breaks if a read is ever added to the boundary that only the raw store can
// answer — the compiler says so here rather than the composition silently taking
// a different store.
type consensusNodeBoundary interface {
	BlockData(context.Context, ton.BlockIDExt) ([]byte, error)
	BlockProof(context.Context, storage.ServedProofKind, ton.BlockIDExt) ([]byte, error)
	BlockState(context.Context, ton.BlockIDExt) (*storage.BlockState, error)
	LoadStateCellTree(context.Context, ton.BlockIDExt, []byte) (*cell.Cell, error)
	LookupBlockBySeqNo(context.Context, storage.BlockSeqRef) (ton.BlockIDExt, error)
	BlockRoot(context.Context, ton.BlockIDExt) (*cell.Cell, error)
	WaitBlockArtifacts(context.Context, ton.BlockIDExt) error
	CurrentState(context.Context) (*storage.CurrentState, error)
	BlockMeta(context.Context, ton.BlockIDExt) (*storage.BlockMeta, error)
	// The extension: what the live view could not answer for a block only this
	// node has finalized, and the edge that replaces polling for it.
	PublishAcceptedBlockState(storage.LiveBlockArtifacts) error
	BlockArtifactsSignal() <-chan struct{}
}

var (
	_ consensusNodeBoundary = (*liveview.Store)(nil)
	// Both consumers' own boundaries are subsets of it, so neither can ask for
	// something the live view cannot serve.
	_ interface {
		LoadStateCellTree(context.Context, ton.BlockIDExt, []byte) (*cell.Cell, error)
		BlockRoot(context.Context, ton.BlockIDExt) (*cell.Cell, error)
		WaitBlockArtifacts(context.Context, ton.BlockIDExt) error
	} = (collator.LocalStateStore)(nil)
	_ interface {
		BlockState(context.Context, ton.BlockIDExt) (*storage.BlockState, error)
		LoadStateCellTree(context.Context, ton.BlockIDExt, []byte) (*cell.Cell, error)
		PublishAcceptedBlockState(storage.LiveBlockArtifacts) error
		BlockArtifactsSignal() <-chan struct{}
	} = (LocalSessionNode)(nil)
)

func moduleRoot(t *testing.T) string {
	t.Helper()

	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve the module root: %v", err)
	}
	if _, err = os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("the module root does not carry go.mod: %v", err)
	}

	return root
}

// importChainTo returns the shortest import chain from pkg to target, or nil when
// target is unreachable. Only packages of this module are followed; everything
// else is a leaf, which is what keeps the walk hermetic.
func importChainTo(t *testing.T, root, pkg, target string) []string {
	t.Helper()

	type step struct {
		pkg   string
		chain []string
	}
	visited := map[string]struct{}{pkg: {}}
	queue := []step{{pkg: pkg, chain: []string{pkg}}}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		for _, imported := range packageImports(t, root, current.pkg) {
			if imported == target {
				return append(current.chain, imported)
			}
			if !strings.HasPrefix(imported, gtonModule+"/") {
				continue
			}
			if _, seen := visited[imported]; seen {
				continue
			}
			visited[imported] = struct{}{}
			queue = append(queue, step{
				pkg:   imported,
				chain: append(append([]string(nil), current.chain...), imported),
			})
		}
	}

	return nil
}

// packageImports parses the non-test Go files of one package of this module and
// returns their import paths, sorted so the walk is deterministic.
func packageImports(t *testing.T, root, pkg string) []string {
	t.Helper()

	dir := root
	if trimmed := strings.TrimPrefix(pkg, gtonModule); trimmed != pkg {
		dir = filepath.Join(root, filepath.FromSlash(strings.TrimPrefix(trimmed, "/")))
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read package %s: %v", pkg, err)
	}

	fileSet := token.NewFileSet()
	unique := map[string]struct{}{}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		parsed, parseErr := parser.ParseFile(fileSet, filepath.Join(dir, name), nil, parser.ImportsOnly)
		if parseErr != nil {
			t.Fatalf("parse %s/%s: %v", pkg, name, parseErr)
		}
		for _, spec := range parsed.Imports {
			path, unquoteErr := strconv.Unquote(spec.Path.Value)
			if unquoteErr != nil {
				t.Fatalf("unquote import %s in %s/%s: %v", spec.Path.Value, pkg, name, unquoteErr)
			}
			unique[path] = struct{}{}
		}
	}

	imports := make([]string, 0, len(unique))
	for path := range unique {
		imports = append(imports, path)
	}
	sort.Strings(imports)

	return imports
}
