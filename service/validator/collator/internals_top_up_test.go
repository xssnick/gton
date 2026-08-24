package collator

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
	"time"

	"github.com/xssnick/gton/service/validator/msgpool"
)

// buildAtQueueDepth builds the mainnet workload with the predecessor's outbound
// queue reported at the given depth. Only the depth differs between calls, so
// everything else the stats show is the collation reacting to it.
func buildAtQueueDepth(t *testing.T, depth uint64) Stats {
	t.Helper()

	req, _ := benchMainnetRequestRepeated(t, benchMainnetFiller, 12)
	natural := uint64(0)
	if req.Previous.OutQueueSize != nil {
		natural = *req.Previous.OutQueueSize
	}
	size := depth
	if size < natural {
		// Below the trie's real content the first dequeue would underflow; the
		// natural depth is this fixture's floor.
		size = natural
	}
	req.Previous.OutQueueSize = &size

	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatalf("depth %d: %v", depth, err)
	}

	return candidate.Stats
}

// TestTopUpRunsAfterTheExternalPhase is the structural gate for the mistake
// this replaced, and it has to be structural: the top-up fires only once the
// brake is closed, which is the same condition that refuses externals, so a
// build where it ran first would produce identical numbers on any fixture where
// the brake is open — and starve the externals on every one where it is not.
// Order is the invariant, so order is what is checked.
func TestTopUpRunsAfterTheExternalPhase(t *testing.T) {
	for _, tc := range []struct{ file, function, externals string }{
		{file: "build.go", function: "buildShardAttemptPaced", externals: "processExternals"},
		{file: "ready_externals.go", function: "buildShardReadyAttempt", externals: "processExternalBatch"},
	} {
		t.Run(tc.function, func(t *testing.T) {
			parsed, err := parser.ParseFile(token.NewFileSet(), packageSourceFile(t, tc.file), nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			var body *ast.BlockStmt
			for _, declaration := range parsed.Decls {
				declared, ok := declaration.(*ast.FuncDecl)
				if ok && declared.Name.Name == tc.function {
					body = declared.Body
				}
			}
			if body == nil {
				t.Fatalf("%s does not declare %s any more; the guard has lost its subject", tc.file, tc.function)
			}
			first := func(name string) token.Pos {
				var at token.Pos
				ast.Inspect(body, func(node ast.Node) bool {
					call, ok := node.(*ast.CallExpr)
					if !ok {
						return true
					}
					if fn, ok := call.Fun.(*ast.SelectorExpr); ok && fn.Sel.Name == name && !at.IsValid() {
						at = call.Pos()
					}
					return true
				})
				return at
			}
			externals, topUp := first(tc.externals), first("topUpInternals")
			if !externals.IsValid() || !topUp.IsValid() {
				t.Fatalf("%s no longer calls both %s and topUpInternals", tc.function, tc.externals)
			}
			if topUp < externals {
				t.Fatalf("%s tops up internals before %s: the reserve is spent before the externals it belongs to"+
					" have had their chance", tc.function, tc.externals)
			}
		})
	}
}

// TestTopUpNeverPreEmptsExternalMessages is the behavioural half. Giving internals the reserved half of the budget UP FRONT, on a
// guess about whether externals would want it, silently starved them: the
// predecessor's queue can be past the brake while this block's cleanup drops
// the live one back under it, and then the externals the brake would have
// admitted find the budget already spent. The top-up runs after the external
// phase for exactly this reason, and a block whose brake reopens must admit
// what a shallow one admits.
func TestTopUpNeverPreEmptsExternalMessages(t *testing.T) {
	shallow := buildAtQueueDepth(t, 0)
	reopened := buildAtQueueDepth(t, skipExternalsQueueSize+100)

	if shallow.ExternalIncluded == 0 {
		t.Fatal("the shallow build admitted no externals, so it cannot show the reopened one losing any")
	}
	if reopened.ExternalIncluded != shallow.ExternalIncluded {
		t.Fatalf("a block whose brake reopened admitted %d externals, the shallow one %d: the drain pre-empted them",
			reopened.ExternalIncluded, shallow.ExternalIncluded)
	}
	if reopened.InternalsImported != shallow.InternalsImported {
		t.Fatalf("a block whose brake reopened imported %d internals, the shallow one %d: it spent the reserve"+
			" while externals could still use it", reopened.InternalsImported, shallow.InternalsImported)
	}
}

// TestBackloggedBlockTopsUpFromTheReserve is the other side: once the queue is
// deep enough that the brake refuses externals outright, the half of the budget
// they cannot use goes to the drain rather than being shipped empty.
func TestBackloggedBlockTopsUpFromTheReserve(t *testing.T) {
	shallow := buildAtQueueDepth(t, 0)
	backlogged := buildAtQueueDepth(t, skipExternalsQueueSize+3000)

	if backlogged.ExternalIncluded != 0 || backlogged.ExternalSkippedLimit == 0 {
		t.Fatalf("the backlogged build was not behind the brake at all: included %d, skipped %d",
			backlogged.ExternalIncluded, backlogged.ExternalSkippedLimit)
	}
	if backlogged.InternalsImported <= shallow.InternalsImported {
		t.Fatalf("backlogged build imported %d internals, shallow %d: the reserve went unused",
			backlogged.InternalsImported, shallow.InternalsImported)
	}
}

// TestMediumMarkClearsOnlyALimitLatch pins the one latch a raised mark may not
// clear. blockFull means "stop adding work", and raising the mark makes the
// limit that set it false again — but the internal-message timeout is a
// statement about the age of the traffic this block is importing, and no mark
// makes that false. Clearing it would put a shard that is already behind on
// time back to work.
func TestMediumMarkClearsOnlyALimitLatch(t *testing.T) {
	// A status with room to spare: the escalation only clears the latch when
	// the raised mark still fits, and this test is about which latch, not about
	// where the marks sit.
	roomy := blockLimits{
		bytes:        limitThresholds{1, 1 << 30, 1 << 31, 1 << 32},
		gas:          limitThresholds{1, 1 << 30, 1 << 31, 1 << 32},
		ltDelta:      limitThresholds{1, 1 << 30, 1 << 31, 1 << 32},
		collatedData: limitThresholds{1, 1 << 30, 1 << 31, 1 << 32},
	}
	newStatus := func() *blockLimitStatus {
		return newBlockLimitStatus(roomy, 0, nil, 0, 0)
	}

	byLimit := &collation{limits: newStatus(), blockFull: true}
	byLimit.escalateToMediumMark()
	if byLimit.fullMark() != LoadSoft {
		t.Fatalf("mark = %v, want LoadSoft after escalation", byLimit.fullMark())
	}
	if byLimit.blockFull {
		t.Fatal("a limit latch survived the raised mark, so the block will enqueue what it could deliver")
	}

	byTimeout := &collation{limits: newStatus(), blockFull: true, blockFullTimeout: true}
	byTimeout.escalateToMediumMark()
	if !byTimeout.blockFull {
		t.Fatal("the internal-message timeout latch was cleared by a raised mark")
	}
}

func TestFullMarkIsTheSoftBoundaryUntilEscalation(t *testing.T) {
	c := &collation{}
	if got := c.fullMark(); got != LoadNormal {
		t.Fatalf("mark = %v before escalation, want LoadNormal", got)
	}
	c.mediumMark = true
	if got := c.fullMark(); got != LoadSoft {
		t.Fatalf("mark = %v after escalation, want LoadSoft", got)
	}
}

// TestTopUpIsConfinedToTheFirstAttempt keeps the top-up out of the size retry.
// A retry exists to make an oversized block smaller and replays what the first
// attempt admitted; topping it up again would grow the block the retry was
// called to shrink, and the retry would either loop or ship the same overflow.
// Checked structurally because the retry's own tests build blocks that never
// reach the brake, so none of them would notice.
func TestTopUpIsConfinedToTheFirstAttempt(t *testing.T) {
	parsed, err := parser.ParseFile(token.NewFileSet(), packageSourceFile(t, "build.go"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	guarded := false
	ast.Inspect(parsed, func(node ast.Node) bool {
		branch, ok := node.(*ast.IfStmt)
		if !ok {
			return true
		}
		condition, ok := branch.Cond.(*ast.BinaryExpr)
		if !ok || condition.Op != token.EQL {
			return true
		}
		selector, ok := condition.X.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != "index" {
			return true
		}
		ast.Inspect(branch.Body, func(inner ast.Node) bool {
			call, ok := inner.(*ast.CallExpr)
			if !ok {
				return true
			}
			if fn, ok := call.Fun.(*ast.SelectorExpr); ok && fn.Sel.Name == "topUpInternals" {
				guarded = true
			}
			return true
		})
		return true
	})
	if !guarded {
		t.Fatal("build.go calls topUpInternals outside an attempt.index == 0 guard, so a size retry would grow the block")
	}
}

// TestFirstSlotTopsUpDespiteExpiredExternalDeadlines is the gate for the slot
// this whole mechanism exists for and used to miss entirely. A window's first
// slot cannot be built before the window opens, and both of its external
// deadlines are slotStartTime(slot) — the instant it opened — so by the time
// collation runs they are in the past. Bounding the top-up by them made it
// return immediately on exactly that slot: the one that carries no externals,
// stops at the soft mark, and has the backed-up queue behind it.
//
// Both runs below have spent external deadlines and an open internal budget,
// and differ only in whether the queue is behind the brake. That is the only
// thing the top-up keys on, so the difference between them IS the top-up —
// which a run that also varied the internal budget would not have been, since
// the main import answers to that budget too.
func TestFirstSlotTopsUpDespiteExpiredExternalDeadlines(t *testing.T) {
	run := func(depth uint64) Stats {
		t.Helper()
		req, _ := benchMainnetRequestRepeated(t, benchMainnetFiller, 12)
		size := depth
		if natural := req.Previous.OutQueueSize; natural != nil && size < *natural {
			size = *natural
		}
		req.Previous.OutQueueSize = &size
		req.InternalMsgUntil = time.Now().Add(2 * time.Second)
		// The first slot's shape: the window opened a moment ago, so the
		// external wait and the external processing budget are both spent.
		expired := time.Now().Add(-50 * time.Millisecond)

		pool := msgpool.New(msgpool.Config{})
		t.Cleanup(pool.Close)
		stream, err := pool.OpenExternalStream(targetShardIdent(req.Shard), 8)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = stream.Close() })

		candidate, _, err := testBuilder().buildShardWithReadyExternals(
			t.Context(), req, stream, expired, expired, 8, time.Time{}, time.Time{},
		)
		if err != nil {
			t.Fatal(err)
		}
		return candidate.Stats
	}

	shallow := run(0)
	backlogged := run(skipExternalsQueueSize + 3000)

	if shallow.ExternalIncluded != 0 || backlogged.ExternalIncluded != 0 {
		t.Fatalf("a first-slot build admitted externals (%d, %d); the shape is wrong",
			shallow.ExternalIncluded, backlogged.ExternalIncluded)
	}
	if backlogged.InternalsImported <= shallow.InternalsImported {
		t.Fatalf("behind the brake the first slot imported %d internals, in front of it %d:"+
			" the top-up never ran, because the spent external deadlines still bound it",
			backlogged.InternalsImported, shallow.InternalsImported)
	}
}
