package collator

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"runtime"
	"testing"

	"github.com/xssnick/tonutils-go/address"
)

// The external phase is entered once per ready batch — eighteen times in a field
// block — so a worker pool tied to that call is a goroutine set and a join
// barrier per batch, for waves of about five messages. The pool must outlive the
// batch and belong to the collation, which is what its own type comment claims
// and what the internal waves were rebuilt to do.
func TestExternalWavePoolIsReusedAcrossBatches(t *testing.T) {
	c := &collation{}
	c.externalWaves.start(c, 4)
	first := c.externalWaves.queue
	if first == nil {
		t.Fatal("the first start did not create a worker pool")
	}

	// A second batch. start() is idempotent by design, so the batch after it
	// finds the same workers rather than paying for a new set.
	c.externalWaves.start(c, 4)
	if c.externalWaves.queue != first {
		t.Fatal("a second batch replaced the worker pool instead of reusing it")
	}

	c.stopWaves()
	if c.externalWaves.queue != nil {
		t.Fatal("the collation did not release the pool")
	}
	// Idempotent teardown: the deferred stop of a build that never ran a wave
	// must not panic on a closed or absent queue.
	c.stopWaves()
}

// Where the pool is released is the whole property, and it is invisible to a
// behavioural test: a stop inside the batch function still produces correct
// blocks, just one pool per batch. This pins the call sites instead.
func TestExternalWavePoolIsReleasedByTheCollationNotTheBatch(t *testing.T) {
	fileSet := token.NewFileSet()
	callers := map[string][]string{}
	for _, name := range []string{"externals_waves.go", "build.go", "ready_externals.go", "master.go"} {
		parsed, err := parser.ParseFile(fileSet, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, declaration := range parsed.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok {
				continue
			}
			ast.Inspect(function, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				switch selector.Sel.Name {
				case "stopWaves":
					callers["stopWaves"] = append(callers["stopWaves"], function.Name.Name)
				case "stop":
					if inner, ok := selector.X.(*ast.SelectorExpr); ok && inner.Sel.Name == "externalWaves" {
						callers["externalWaves.stop"] = append(callers["externalWaves.stop"], function.Name.Name)
					}
				}

				return true
			})
		}
	}
	for _, caller := range callers["externalWaves.stop"] {
		if caller != "stopWaves" {
			t.Fatalf("externalWaves.stop is called from %s; only the collation-level release may stop it", caller)
		}
	}
	// Every entry point that runs an external phase must release it, or the
	// pool outlives the collation that owns it: on each chain, the
	// deterministic attempt and the one that follows a ready-external stream.
	// The masterchain pair was missing once, and the leak it left — a worker
	// set parked forever per master block, each pinning the whole collation —
	// was invisible to every behavioural test.
	want := map[string]bool{
		"buildShardAttemptPaced":  false,
		"buildShardReadyAttempt":  false,
		"buildMasterAttempt":      false,
		"buildMasterReadyAttempt": false,
	}
	for _, caller := range callers["stopWaves"] {
		if _, ok := want[caller]; ok {
			want[caller] = true
		}
	}
	for name, found := range want {
		if !found {
			t.Fatalf("%s does not release the external wave pool", name)
		}
	}
}

// The masterchain entry points were the ones that leaked, so the property is
// also pinned behaviourally on that path: a sequence of master builds must not
// leave wave workers behind.
func TestMasterBuildLeavesNoWaveWorkers(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)
	// One external is enough to enter the wave machinery; an empty batch is
	// short-circuited before a pool is started and would make this vacuous. It
	// need not be accepted — a rejected external still runs the phase.
	fixture.request.Externals = []ExternalInput{
		prewarmExternalInput(t, address.NewAddress(0, 0xff, fixture.configAddress)),
	}
	builder := testBuilder()

	if _, err := builder.BuildMaster(context.Background(), fixture.request); err != nil {
		t.Fatal(err)
	}
	before := runtime.NumGoroutine()
	for range 3 {
		if _, err := builder.BuildMaster(context.Background(), fixture.request); err != nil {
			t.Fatal(err)
		}
	}
	// Workers exit on queue close before stop() returns, so no settle is
	// needed; a small tolerance covers unrelated runtime goroutines.
	if after := runtime.NumGoroutine(); after > before+2 {
		buf := make([]byte, 1<<20)
		t.Fatalf("goroutines grew %d -> %d across three master builds\n%s", before, after, buf[:runtime.Stack(buf, true)])
	}
}
