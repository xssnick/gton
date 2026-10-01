package collator

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

// Where the hints are issued is the whole property, and it is invisible to a
// behavioural test: issuing them under the session mutex produces exactly the
// same warming, just with a few hundred takes of the prewarmer's global mutex
// standing in front of AdvanceConsensusBase — which is how a leader window opens
// — and in front of the commit that sits between a block's signature and its
// broadcast. This pins the placement instead.
//
// Defers are LIFO, so "issued after the unlock" is written as "deferred before
// the unlock", and getting that backwards is silent: the hints still go out, on
// the wrong side of the lock.
func TestPrewarmHintsAreDeferredAboveTheSessionUnlock(t *testing.T) {
	fileSet := token.NewFileSet()
	parsed, err := parser.ParseFile(fileSet, "local_acquisition_build.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Every entry point that takes a session and may name a hint. Kept as a
	// closed list rather than derived, so a new one has to be considered here.
	want := map[string]bool{
		"AcquireShard":     false,
		"AcquireMaster":    false,
		"CommitCandidate":  false,
		"RestoreCandidate": false,
	}
	for _, declaration := range parsed.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Body == nil {
			continue
		}
		if _, wanted := want[function.Name.Name]; !wanted {
			continue
		}
		issueAt, unlockAt := -1, -1
		ast.Inspect(function, func(node ast.Node) bool {
			deferred, ok := node.(*ast.DeferStmt)
			if !ok {
				return true
			}
			selector, ok := deferred.Call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			switch selector.Sel.Name {
			case "issuePrewarmHints":
				if issueAt < 0 {
					issueAt = int(deferred.Pos())
				}
			case "Unlock":
				if unlockAt < 0 {
					unlockAt = int(deferred.Pos())
				}
			}

			return true
		})
		if unlockAt < 0 {
			t.Fatalf("%s no longer defers a session unlock; this guard is stale", function.Name.Name)
		}
		if issueAt < 0 {
			t.Fatalf("%s locks a session but never issues its prewarm hints", function.Name.Name)
		}
		if issueAt > unlockAt {
			t.Fatalf("%s defers issuePrewarmHints after the unlock, so it runs before it "+
				"and the hints go out with the session mutex held", function.Name.Name)
		}
		want[function.Name.Name] = true
	}
	for name, found := range want {
		if !found {
			t.Fatalf("%s was not found in local_acquisition_build.go", name)
		}
	}
}

// The two buckets are kept apart on purpose. A destination the block is about to
// execute is worth a worker now; one that merely entered the branch is
// background work. Merging them would let a queue delta seen earlier in the same
// acquisition swallow the urgent hint for the same account.
func TestUrgentAndQueuedHintsDedupeSeparately(t *testing.T) {
	warmer := &recordedAccountPrewarmer{}
	acquisition := &LocalAcquisition{accountPrewarmer: warmer, accountPrewarmCapacity: 8}
	account := [32]byte{0x51}

	var hints prewarmHints
	acquisition.collectPooledInternals(&hints, []*msgpool.InternalMessage{
		{DestinationAccount: account, DestinationPrewarmable: true, EnvHash: cell.Hash{0xb1}},
		{DestinationAccount: account, DestinationPrewarmable: true, EnvHash: cell.Hash{0xb1}},
	})
	acquisition.collectCurrentInternals(&hints, &msgpool.Cut{Messages: []*msgpool.InternalMessage{
		{DestinationAccount: account, DestinationPrewarmable: true},
		{DestinationAccount: account, DestinationPrewarmable: true},
	}})

	if len(hints.queued) != 1 || len(hints.urgent) != 1 || len(hints.roots) != 1 {
		t.Fatalf("hints = %d queued, %d urgent, %d roots; want 1, 1, 1",
			len(hints.queued), len(hints.urgent), len(hints.roots))
	}
	acquisition.issuePrewarmHints(&hints)
	if warmer.immediate != 1 {
		t.Fatalf("immediate warms = %d, want the one destination the block is about to execute", warmer.immediate)
	}
}
