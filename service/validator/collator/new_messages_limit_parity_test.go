package collator

import (
	"bytes"
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

// pendingInShardMessage pushes one generated in-shard internal message onto the
// heap the way registerOutputs does, so processNewMessages sees exactly the item
// a real transaction would have left behind.
//
// The source and destination are both in-shard, which is what makes the message
// a candidate for immediate delivery: processNewMessages enqueues anything whose
// destination is outside the shard regardless of the limit state, so an
// out-of-shard message could not tell the two branches apart.
func pendingInShardMessage(
	tb testing.TB,
	c *collation,
	src, dst *address.Address,
	lt uint64,
) {
	tb.Helper()

	root, err := tlb.ToCell(&tlb.InternalMessage{
		IHRDisabled: true,
		SrcAddr:     src,
		DstAddr:     dst,
		Amount:      tlb.FromNanoTONU(1_000_000_000),
		CreatedLT:   lt,
		CreatedAt:   c.header.GenUtime,
		FwdFee:      tlb.FromNanoTONU(100_000),
		IHRFee:      tlb.FromNanoTONU(0),
		StateInit:   nil,
		Body:        cell.BeginCell().EndCell(),
	})
	if err != nil {
		tb.Fatal(err)
	}
	var parsed tlb.Message
	if err = parseExact(&parsed, root); err != nil {
		tb.Fatal(err)
	}
	c.new.push(newMessage{
		lt:          lt,
		hash:        root.HashKey(),
		root:        root,
		parsed:      &parsed,
		transaction: cell.BeginCell().EndCell(),
	})
	c.limits.extraOutMsgs++
}

// TestNewMessagesRereadsTheNormalLimitPerHeapItem pins the port of
// collator.cpp:4847-4850
//
//	while (!new_msgs.empty()) {
//	  if (!block_limit_status_->fits(block::ParamLimits::cl_normal)) { block_full_ = true; }
//
// which the reference evaluates at the top of EVERY heap item, before
// extra_out_msgs-- and before the enqueue_only latch at :4858-4863.
//
// Before this port processNewMessages read only the c.blockFull FIELD, and that
// field is written in three places — deliverImmediate (execute.go, the port of
// collator.cpp:3727-3731), processInternals (imports.go) and
// processDispatchQueue (dispatch.go). None of them covers work that grows the
// estimate WITHOUT an immediate delivery, and enqueue() is exactly that: it
// charges cells through insert()/limits.storage.AddCell and re-proofs the queue
// root every 64 operations. So a drain whose earlier items were enqueued could
// cross the normal class and keep executing later in-shard items in the same
// block, where the reference would have enqueued them.
//
// The real-world shape is a heap holding an out-of-shard message followed by an
// in-shard one: the first is enqueued, pushing the estimate over normal, and the
// second is then executed by us and enqueued by the reference. This test forces
// the same limit state directly with a byte ceiling rather than by tuning a
// fixture into it, because the ceiling makes the branch the only variable — the
// number of enqueues it takes to cross a real threshold is a property of the
// size estimator, not of the rule under test.
//
// The consequence is consensus-visible in the differential sense: it changes the
// descriptor constructors (msg_export_new$001 instead of the
// msg_import_imm$011/msg_export_imm$010 pair), the transaction set, the outbound
// queue and the block's end lt. It is NOT a rejection risk — validate-query
// enforces only the HARD classes (validate-query.cpp:2456 lt_delta.hard,
// :6107-6117 gas.hard) and never cl_normal — so a reference validator would have
// accepted the pre-fix block. See TestGoldenFixturesNeverCrossNormalMidDrain for
// why the pinned golden hashes are unmoved by it.
func TestNewMessagesRereadsTheNormalLimitPerHeapItem(t *testing.T) {
	sender := address.NewAddress(0, 0, bytes.Repeat([]byte{0xc1}, 32))
	receiver := address.NewAddress(0, 0, bytes.Repeat([]byte{0xc2}, 32))

	req := emptyCandidateRequest(t)
	req.Internals = &msgpool.Cut{}
	req.Previous.State = stateWithAccounts(t, req.Previous.State, activeContracts(t, req.Header.GenUtime,
		activeContract{address: sender, code: externalAcceptCode(t), balance: 100_000_000_000},
		activeContract{address: receiver, code: externalAcceptCode(t), balance: 100_000_000_000},
	))

	c, err := testBuilder().prepare(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	pendingInShardMessage(t, c, sender, receiver, requestStartLT(t, req)+1)

	// The limit state a real drain reaches part-way through: over the normal
	// class, under hard, and c.blockFull still false because nothing has been
	// delivered yet. Asserted rather than assumed — a fixture that arrived here
	// already blockFull would make the whole test vacuous, since the enqueue_only
	// latch would fire for a different reason.
	entry := c.limits.estimatedBytes()
	c.limits.limits.bytes = limitThresholds{entry, entry, entry + 1, entry + 2}
	if c.blockFull {
		t.Fatal("fixture is already blockFull before the drain: the rule under test is unreachable")
	}
	if c.limits.fits(LoadNormal) {
		t.Fatal("fixture does not exceed the normal class: the rule under test is unreachable")
	}
	if !c.limits.fits(LoadHard) {
		t.Fatal("fixture exceeds the hard class: the drain would stop for a different reason")
	}

	if err = c.processNewMessages(false); err != nil {
		t.Fatalf("drain failed: %v", err)
	}

	if c.stats.ImmediateDelivered != 0 {
		t.Fatalf("the drain delivered %d generated messages in-block while over the normal limit class; "+
			"the reference (collator.cpp:4847-4850) enqueues them", c.stats.ImmediateDelivered)
	}
	if c.stats.EnqueuedMessages != 1 {
		t.Fatalf("EnqueuedMessages = %d, want 1: the message went neither to the block nor to the queue",
			c.stats.EnqueuedMessages)
	}
	if !c.blockFull {
		t.Fatal("blockFull was not raised by the loop-top recheck")
	}
}

// TestGoldenFixturesNeverCrossNormalMidDrain is the honest gate on the pinned
// byte-identity constants. The per-item recheck above MUST change produced bytes
// on any block whose heap drain crosses the normal class part-way through, so
// leaving the golden hashes untouched is only defensible if the recorded
// fixtures provably never do that.
//
// They do not, and for two different reasons, which is why both arms are here:
//
//	repeat=1 — 76 heap items, every one delivered in-block, and the estimate
//	  never reaches the normal class at all, so the recheck never fires.
//	repeat=3 — 114 heap items, every one enqueued, because processInternals had
//	  already set blockFull before the drain started. The recheck can only
//	  observe a state that was already reached.
//
// The direct measurement is that the four pinned arms of
// payload_hint_mainnet_test.go and the two of full_collated_golden_test.go still
// pass unchanged, verified with the clause both present and absent. This test
// pins the REASON, so a future fixture change that starts crossing the class
// mid-drain fails here — loudly, and with the golden hashes still describing the
// old semantics — instead of silently re-baselining them.
func TestGoldenFixturesNeverCrossNormalMidDrain(t *testing.T) {
	for _, repeat := range []int{1, 3} {
		req := benchMainnetCollatedRequest(t, repeat)
		candidate, err := testBuilder().BuildShard(context.Background(), req)
		if err != nil {
			t.Fatalf("repeat=%d: %v", repeat, err)
		}
		// capFullCollatedData must be in effect: with the capability clear the
		// collated data degenerates to the 33-byte marker and the size estimate
		// that drives the whole rule is never installed.
		if len(candidate.CollatedData) <= benchMainnetCollatedMarkerBytes {
			t.Fatalf("repeat=%d: collated data is %d bytes, i.e. the bare marker: capFullCollatedData "+
				"is not in effect and the limit estimate under test does not exist",
				repeat, len(candidate.CollatedData))
		}
		stats := candidate.Stats
		delivered, enqueued := stats.ImmediateDelivered, stats.EnqueuedMessages
		if delivered+enqueued == 0 {
			t.Fatalf("repeat=%d: the fixture drains no generated messages at all, so it cannot "+
				"speak to the drain rule: %+v", repeat, stats)
		}
		// The two ways the class cannot be crossed mid-drain: nothing was
		// enqueued (so no enqueue grew the estimate under a live delivery
		// branch), or nothing was delivered (so the latch was set before the
		// first item). A fixture doing both would need the crossing point
		// established, and this assertion is what forces that.
		if delivered != 0 && enqueued != 0 {
			t.Fatalf("repeat=%d: the fixture now both delivers (%d) and enqueues (%d) generated "+
				"messages, so it may cross the normal class mid-drain. The pinned golden hashes no "+
				"longer provably describe the same semantics as the per-item recheck: re-derive them "+
				"deliberately rather than updating the constants", repeat, delivered, enqueued)
		}
	}
}

// TestInternalMsgUntilDerivation pins Collator::internal_msg_timeout_ from
// collator.cpp:85-97 against its sibling queue_cleanup_timeout_. The two differ
// in exactly one place — the no-external-wait fraction, half against a quarter
// (collator.cpp:94-95) — and agree everywhere else, so the table asserts both to
// make an accidental divergence in the shared branch visible.
func TestInternalMsgUntilDerivation(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	soft := now.Add(time.Second)

	for _, tc := range []struct {
		name              string
		soft              time.Time
		externalWaitUntil time.Time
		wantInternal      time.Time
		wantCleanup       time.Time
	}{
		{
			name: "inert without a soft deadline", soft: time.Time{},
			externalWaitUntil: now.Add(time.Hour),
			wantInternal:      time.Time{}, wantCleanup: time.Time{},
		},
		{
			name: "external wait before soft", soft: soft,
			externalWaitUntil: now.Add(100 * time.Millisecond),
			wantInternal:      soft, wantCleanup: soft,
		},
		{
			name: "external wait after soft", soft: soft,
			externalWaitUntil: soft.Add(time.Second),
			wantInternal:      soft.Add(time.Second), wantCleanup: soft.Add(time.Second),
		},
		{
			// The only asymmetry: half of what is left for the internal phases,
			// a quarter for cleanup.
			name: "half of the remaining budget", soft: soft,
			externalWaitUntil: time.Time{},
			wantInternal:      now.Add(500 * time.Millisecond), wantCleanup: now.Add(250 * time.Millisecond),
		},
		{
			name: "already past the soft deadline", soft: now.Add(-time.Second),
			externalWaitUntil: time.Time{},
			wantInternal:      now, wantCleanup: now,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := internalMsgUntil(now, tc.soft, tc.externalWaitUntil); !got.Equal(tc.wantInternal) {
				t.Fatalf("internalMsgUntil = %v, want %v", got, tc.wantInternal)
			}
			if got := queueCleanupUntil(now, tc.soft, tc.externalWaitUntil); !got.Equal(tc.wantCleanup) {
				t.Fatalf("queueCleanupUntil = %v, want %v", got, tc.wantCleanup)
			}
		})
	}
}

// TestInternalMsgTimeoutTruncatesTheDrain pins the port of
// collator.cpp:4851-4854: with the soft boundary already behind us the reference
// stops admitting new-message work, sets block_full_ and enqueues the rest,
// rather than running to completion.
//
// The distinction that matters is liveness, not bytes. Before this port the
// three internal phases had NO time budget at all — grep for time in imports.go,
// dispatch.go and execute.go found only the external batch deadline — so a slot
// that ran long was aborted wholesale by ctx cancellation and published nothing.
// The reference truncates and publishes. The budget is wall-clock, so it cannot
// be pinned by a golden hash; it is gated by an already-past instant instead,
// which is the only injection point the collation has.
func TestInternalMsgTimeoutTruncatesTheDrain(t *testing.T) {
	sender := address.NewAddress(0, 0, bytes.Repeat([]byte{0xd1}, 32))
	receiver := address.NewAddress(0, 0, bytes.Repeat([]byte{0xd2}, 32))

	req := emptyCandidateRequest(t)
	req.Internals = &msgpool.Cut{}
	req.Previous.State = stateWithAccounts(t, req.Previous.State, activeContracts(t, req.Header.GenUtime,
		activeContract{address: sender, code: externalAcceptCode(t), balance: 100_000_000_000},
		activeContract{address: receiver, code: externalAcceptCode(t), balance: 100_000_000_000},
	))
	// An instant already in the past. internalMsgUntil derives this from
	// BuildSoftDeadline on the live path; a deterministic entry point leaves it
	// zero, which is why every other test in this package is unaffected.
	req.InternalMsgUntil = time.Now().Add(-time.Second)

	c, err := testBuilder().prepare(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	pendingInShardMessage(t, c, sender, receiver, requestStartLT(t, req)+1)
	if c.blockFull {
		t.Fatal("fixture is already blockFull: the timeout would not be the cause")
	}
	if !c.limits.fits(LoadNormal) {
		t.Fatal("fixture is already over the normal class: the timeout would not be the cause")
	}

	if err = c.processNewMessages(false); err != nil {
		t.Fatalf("drain failed: %v", err)
	}

	if c.stats.ImmediateDelivered != 0 || c.stats.EnqueuedMessages != 1 {
		t.Fatalf("past the soft boundary the drain must enqueue, not deliver: delivered=%d enqueued=%d",
			c.stats.ImmediateDelivered, c.stats.EnqueuedMessages)
	}
	if c.stats.InternalMsgTimeouts == 0 {
		t.Fatal("InternalMsgTimeouts was not raised: a block truncated by the wall clock would be " +
			"indistinguishable from one truncated by a limit axis")
	}
	if !c.blockFull {
		t.Fatal("blockFull was not raised by the soft-timeout check")
	}
}

// TestInternalMsgTimeoutTruncatesInboundInternals pins the port of
// collator.cpp:4141-4146, the inbound-internal half of the same budget. This is
// the site that matters most in the field: the block a slot publishes late is
// almost always late because it is importing queued internals, and before the
// port there was no way to stop importing short of ctx cancellation, which
// discards the whole candidate.
func TestInternalMsgTimeoutTruncatesInboundInternals(t *testing.T) {
	req := emptyCandidateRequest(t)
	startLT := requestStartLT(t, req)

	source := address.NewAddress(0, 0, bytes.Repeat([]byte{0xe1}, 32))
	dest := address.NewAddress(0, 0, bytes.Repeat([]byte{0xe2}, 32))
	req.Previous.State = stateWithAccounts(t, req.Previous.State, activeContracts(t, req.Header.GenUtime,
		activeContract{address: dest, code: externalAcceptCode(t), balance: 10_000_000_000},
	))

	fee := tlb.FromNanoTONU(100_000)
	msg, enqueued := queuedInternal(t, source, dest, startLT-10, req.Header.GenUtime-1,
		fee, fee, routingAddressBits, msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll})
	req.Previous.State = stateWithQueueMessage(t, req.Previous.State, msg.Key, enqueued)
	queueSize := uint64(1)
	req.Previous.OutQueueSize = &queueSize
	req.Internals = &msgpool.Cut{Messages: []*msgpool.InternalMessage{msg}}

	// Baseline arm first: without the budget the message IS imported. Without
	// this the assertion below could pass on a fixture that never had an
	// importable message in it.
	base, err := testBuilder().prepare(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if err = base.processInternals(); err != nil {
		t.Fatalf("baseline import failed: %v", err)
	}
	if base.stats.InternalsImported != 1 {
		t.Fatalf("baseline imported %d internals, want 1: the fixture has nothing to truncate",
			base.stats.InternalsImported)
	}

	req.InternalMsgUntil = time.Now().Add(-time.Second)
	c, err := testBuilder().prepare(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.processInternals(); err != nil {
		t.Fatalf("truncated import failed: %v", err)
	}
	if c.stats.InternalsImported != 0 {
		t.Fatalf("imported %d internals past the soft boundary, want 0", c.stats.InternalsImported)
	}
	if !c.blockFull {
		t.Fatal("blockFull was not raised, so the remaining generated messages would still be delivered " +
			"in-block instead of enqueued (collator.cpp:4142)")
	}
	if c.stats.InternalMsgTimeouts != 1 {
		t.Fatalf("InternalMsgTimeouts = %d, want 1", c.stats.InternalMsgTimeouts)
	}
}

// TestInternalMsgTimeoutIsCheckedByEveryInternalPhase pins the three CALL SITES.
// The behavioural tests above drive processNewMessages and processInternals
// directly, so deleting the check from processDispatchQueue — the third site,
// collator.cpp:4424-4429 — would leave the whole suite green while the
// dispatch-queue pass ran unbounded.
//
// A dispatch fixture that reaches the boundary needs a populated DispatchQueue
// and a policy that admits a second phase, which is a large fixture for one
// branch; the structural gate buys the same protection at the cost of failing
// when the code is restructured rather than when it is broken. It is deliberately
// keyed on the receiver-qualified method, not a bare name.
func TestInternalMsgTimeoutIsCheckedByEveryInternalPhase(t *testing.T) {
	const check = "internalMsgExpired"

	// Bound to the compiled method, so a rename fails here at build time instead
	// of leaving the AST rule matching nothing.
	var expired func(*collation) bool = (*collation).internalMsgExpired
	if expired == nil {
		t.Fatal("internalMsgExpired is not a method of *collation")
	}

	want := map[string]string{
		"processInternals":     "imports.go",
		"processDispatchQueue": "dispatch.go",
		"processNewMessages":   "execute.go",
		"deliverImmediate":     "execute.go",
	}
	found := map[string]int{}
	for function, file := range want {
		parsed, err := parser.ParseFile(token.NewFileSet(), packageSourceFile(t, file), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}
		for _, declaration := range parsed.Decls {
			declared, ok := declaration.(*ast.FuncDecl)
			if !ok || declared.Name.Name != function || declared.Body == nil || declared.Recv == nil {
				continue
			}
			ast.Inspect(declared.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if ok && selector.Sel.Name == check {
					found[function]++
				}
				return true
			})
		}
	}
	for function, file := range want {
		if found[function] == 0 {
			t.Fatalf("%s (%s) does not call %s: that phase runs with no time budget, which is the "+
				"unported internal_msg_timeout_ this file exists to close", function, file, check)
		}
	}
}

// TestInternalMsgTimeoutIsInertWithoutADeadline is the other half of the
// convention every deterministic entry point in this package depends on: a zero
// InternalMsgUntil must leave the budget completely inert, or BuildShard, the
// restore path and the whole test surface would stop reproducing their
// pre-budget candidates.
func TestInternalMsgTimeoutIsInertWithoutADeadline(t *testing.T) {
	req := emptyCandidateRequest(t)
	req.Internals = &msgpool.Cut{}
	c, err := testBuilder().prepare(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if !req.InternalMsgUntil.IsZero() {
		t.Fatal("emptyCandidateRequest must leave InternalMsgUntil zero")
	}
	if c.internalMsgExpired() {
		t.Fatal("a zero InternalMsgUntil must never report expiry")
	}
}

// TestProcessedBoundHasExactlyTwoCallSites pins the invariant the reference
// encodes structurally: update_last_proc_int_msg is called from exactly two
// places in collator.cpp — :3956 on the inbound queue import and :3668 in
// process_one_new_message, the latter only after the `enqueue || defer` exit at
// :3649-3663 and only when `!is_special`.
//
// Our two are importInternal (imports.go) and deliverImmediate (execute.go).
// Nothing else in this package would notice a third: adding an advance to
// enqueue() would compile, pass every existing test, and silently raise the lt
// floor for later transactions on the strength of work the block never performed
// — a divergence in the same family as the one this file's other tests close.
// See the comment on advanceProcessedBound for why the enqueue-side twin is
// parity rather than an omission.
func TestProcessedBoundHasExactlyTwoCallSites(t *testing.T) {
	const advance = "advanceProcessedBound"

	var bound func(*collation, string, uint64, [32]byte) error = (*collation).advanceProcessedBound
	if bound == nil {
		t.Fatal("advanceProcessedBound is not a method of *collation")
	}

	sites := map[string]int{}
	fileSet := token.NewFileSet()
	entries, err := os.ReadDir(filepath.Dir(packageSourceFile(t, "execute.go")))
	if err != nil {
		t.Fatalf("read the package directory: %v", err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		parsed, parseErr := parser.ParseFile(fileSet, packageSourceFile(t, name), nil, 0)
		if parseErr != nil {
			t.Fatalf("parse %s: %v", name, parseErr)
		}
		for _, declaration := range parsed.Decls {
			declared, ok := declaration.(*ast.FuncDecl)
			if !ok || declared.Body == nil {
				continue
			}
			ast.Inspect(declared.Body, func(node ast.Node) bool {
				call, isCall := node.(*ast.CallExpr)
				if !isCall {
					return true
				}
				selector, isSelector := call.Fun.(*ast.SelectorExpr)
				if isSelector && selector.Sel.Name == advance {
					sites[declared.Name.Name]++
				}
				return true
			})
		}
	}

	want := map[string]int{"importInternal": 1, "deliverImmediate": 1}
	if len(sites) != len(want) {
		t.Fatalf("%s is called from %v, want exactly %v. A third feed for the processed bound is a "+
			"consensus-visible change: it moves the floor that every later transaction's lt is "+
			"computed from. If the reference grew one, cite it (update_last_proc_int_msg in "+
			"collator.cpp) and update this list deliberately", advance, sites, want)
	}
	for function, count := range want {
		if sites[function] != count {
			t.Fatalf("%s calls %s %d times, want %d", function, advance, sites[function], count)
		}
	}
}
