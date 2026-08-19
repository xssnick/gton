package collator

import (
	"go/ast"
	"go/parser"
	"go/token"
	"slices"
	"testing"
)

// The phase benchmarks drive the pipeline themselves so that production
// collation and validation stay free of instrumentation. That leaves the phase
// order written down twice, and a phase added to the real pipeline but not to
// the benchmark would silently drop out of every measurement. These guards read
// the production functions and fail when the two disagree, naming the file to
// fix.

func TestCollationPhaseOrderMatchesBuildShard(t *testing.T) {
	want := []string{
		"prepare",
		"cleanupOutQueue",
		"processDispatchQueue",
		"processInternals",
		"processExternals",
		"processNewMessages",
		"updateProcessedInfo",
		"finishAccounts",
		"finish",
	}
	// Live external waiting splits the attempt at the exact C++ phase boundary:
	// pre-external preparation can run early, while finalization runs only after
	// the ready stream closes. Flatten those production helpers before comparing
	// them to the benchmark's deliberately linear mirror.
	got := append(methodCallOrder(t, "build.go", "prepareShardPhases", "b", "c"), "processExternals", "processNewMessages")
	got = append(got, methodCallOrder(t, "build.go", "finishShard", "c")...)
	if !slices.Equal(got, want) {
		t.Fatalf("shard attempt runs %v; collateWithPhases in bench_phases_test.go mirrors %v", got, want)
	}
	if len(collationPhases) != len(want) {
		t.Fatalf("collationPhases has %d entries for %d pipeline steps", len(collationPhases), len(want))
	}
}

func TestSemanticPhaseOrderMatchesVerifier(t *testing.T) {
	if got, want := functionCallOrder(t, "semantic_verify.go", "VerifyCandidateTransition"),
		[]string{"newSemanticReplay"}; !slices.Equal(got, want) {
		t.Fatalf("VerifyCandidateTransition calls %v; phaseSemantics mirrors %v", got, want)
	}
	want := []string{
		"precheckAccountUpdates",
		"prepareQueueValidation",
		"verifyAccounts",
		"verifyAfterReplay",
		"verifyMasterSemantics",
	}
	got := methodCallOrder(t, "semantic_verify.go", "VerifyCandidateTransition", "replay", "queues")
	if !slices.Equal(got, want) {
		t.Fatalf("VerifyCandidateTransition runs %v; phaseSemantics in bench_phases_test.go mirrors %v", got, want)
	}
}

// methodCallOrder returns, in source order, the names of every method called on
// one of the named receiver identifiers inside fn. Calls on a nested selector
// (c.limits.addProof) have a receiver that is not a bare identifier and are
// skipped.
func methodCallOrder(tb testing.TB, file, fn string, receivers ...string) []string {
	tb.Helper()

	var names []string
	forEachCall(tb, file, fn, func(call *ast.CallExpr) {
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return
		}
		receiver, ok := selector.X.(*ast.Ident)
		if !ok || !slices.Contains(receivers, receiver.Name) {
			return
		}
		names = append(names, selector.Sel.Name)
	})
	return names
}

// functionCallOrder returns, in source order, the names of every package-level
// function called by bare identifier inside fn.
func functionCallOrder(tb testing.TB, file, fn string) []string {
	tb.Helper()

	var names []string
	forEachCall(tb, file, fn, func(call *ast.CallExpr) {
		if ident, ok := call.Fun.(*ast.Ident); ok {
			names = append(names, ident.Name)
		}
	})
	return names
}

func forEachCall(tb testing.TB, file, fn string, visit func(*ast.CallExpr)) {
	tb.Helper()

	// Anchored to the package directory, not the process cwd; see
	// packageSourceFile.
	parsed, err := parser.ParseFile(token.NewFileSet(), packageSourceFile(tb, file), nil, 0)
	if err != nil {
		tb.Fatalf("parse %s: %v", file, err)
	}
	for _, decl := range parsed.Decls {
		declared, ok := decl.(*ast.FuncDecl)
		if !ok || declared.Name.Name != fn || declared.Body == nil {
			continue
		}
		ast.Inspect(declared.Body, func(node ast.Node) bool {
			if call, ok := node.(*ast.CallExpr); ok {
				visit(call)
			}
			return true
		})
		return
	}
	tb.Fatalf("%s declares no function %s", file, fn)
}
