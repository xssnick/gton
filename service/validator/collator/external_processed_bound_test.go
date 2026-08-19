package collator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"math/big"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
	funcsop "github.com/xssnick/tonutils-go/tvm/op/funcs"
	stackop "github.com/xssnick/tonutils-go/tvm/op/stack"

	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/msgpool"
)

// inShardMessage builds an internal message to an in-shard destination, so a
// contract that sends it generates a message this block may deliver itself.
func inShardMessage(tb testing.TB, dst *address.Address, amount uint64) *cell.Cell {
	tb.Helper()

	msg, err := tlb.ToCell(&tlb.InternalMessage{
		IHRDisabled: true,
		SrcAddr:     address.NewAddressNone(),
		DstAddr:     dst,
		Amount:      tlb.FromNanoTONU(amount),
		Body:        cell.BeginCell().EndCell(),
	})
	if err != nil {
		tb.Fatal(err)
	}
	return msg
}

// externalSendManyCode accepts an external message and SENDRAWMSGs every
// message given, in order, so the transaction emits len(messages) internal
// messages with consecutive created_lt.
func externalSendManyCode(tb testing.TB, messages ...*cell.Cell) *cell.Cell {
	tb.Helper()

	code := externalAcceptCode(tb).ToBuilder()
	for _, msg := range messages {
		if err := code.StoreBuilder(stackop.PUSHREF(msg).Serialize()); err != nil {
			tb.Fatal(err)
		}
		if err := code.StoreBuilder(stackop.PUSHINT(big.NewInt(0)).Serialize()); err != nil {
			tb.Fatal(err)
		}
		if err := code.StoreBuilder(funcsop.SENDRAWMSG().Serialize()); err != nil {
			tb.Fatal(err)
		}
	}
	return code.EndCell()
}

// transactionLTOf reports the logical time of the single transaction the block
// holds for one account, and fails if the account has none or more than one.
func transactionLTOf(tb testing.TB, candidate *Candidate, account *address.Address) uint64 {
	tb.Helper()

	var block tlb.Block
	if err := parseExact(&block, candidateBlock(tb, candidate)); err != nil {
		tb.Fatal(err)
	}
	transactions, err := block.ListTransactions()
	if err != nil {
		tb.Fatalf("list transactions: %v", err)
	}
	var found []uint64
	for _, transaction := range transactions {
		if bytes.Equal(transaction.AccountAddr, account.Data()) {
			found = append(found, transaction.LT)
		}
	}
	if len(found) != 1 {
		tb.Fatalf("account %s has %d transactions in the block, want exactly one", account, len(found))
	}
	return found[0]
}

// boundFixtureRequest is the shared base for both arms: an empty candidate
// request with capFullCollatedData ON and the collated-proof wiring the
// capability requires.
//
// The capability is the whole point and not decoration. With it clear —
// which is what emptyCandidateRequest and testBuilder() give you by default —
// CollatedData degenerates to a 33-byte marker, the proof-size estimator is
// never installed, and traceValidationClosure records into nothing. Both arms
// below assert lt ordering on a candidate that a proof-backed validator will
// actually open, so the configuration has to be the one the field nodes run.
//
// The predecessor is bound as a zerostate (bindZerostatePredecessor) because the
// synthetic fixture has no predecessor BLOCK to carry the state update the
// capability normally binds Previous.ID.RootHash through. That takes the other
// branch of the same binding and leaves everything under test untouched.
func boundFixtureRequest(t *testing.T) ShardRequest {
	t.Helper()

	req := emptyCandidateRequest(t)
	req.Masterchain.Config.capabilities |= capFullCollatedData
	// A complete, empty cut: internalsIncomplete() is false, so
	// processNewMessages is allowed to deliver immediately rather than enqueue.
	// Without this the fixture silently lacks the shape under test, because
	// emptyCandidateRequest leaves Internals nil.
	req.Internals = &msgpool.Cut{}

	return req
}

// finishBoundFixture applies the collated-proof wiring after the caller has
// finished rewriting the predecessor state. It must run last: both helpers hash
// the state as it then stands.
func finishBoundFixture(t *testing.T, req *ShardRequest) {
	t.Helper()

	attachFullCollatedTestNeighbors(t, req)
	startLT := requestStartLT(t, *req)
	req.NeighborShardEndLT = func(uint32, int32, uint64) uint64 { return startLT }
	bindZerostatePredecessor(t, req)
}

// assertBoundCandidateIsProofBacked closes the two traps that have voided
// results in this package before: a 33-byte collated marker standing in for a
// proof, and acceptingCandidateTransitionVerifier standing in for the rule.
//
// The verifier is NewSemanticVerifier on the live TVM. testCandidateTransitionVerifier
// IS acceptingCandidateTransitionVerifier — it answers yes to everything, which
// would make the lt assertions decorative and the reference clause unreachable.
func assertBoundCandidateIsProofBacked(t *testing.T, req ShardRequest, candidate *Candidate) {
	t.Helper()

	if len(candidate.CollatedData) <= benchMainnetCollatedMarkerBytes {
		t.Fatalf("collated data is %d bytes, i.e. the bare %d-byte marker: capFullCollatedData is not "+
			"in effect and this candidate carries no proof for a validator to open",
			len(candidate.CollatedData), benchMainnetCollatedMarkerBytes)
	}

	resident := loadPreviousShardState(t, req)
	neighbors := append([]Neighbor(nil), req.Neighbors...)
	for i := range neighbors {
		if neighbors[i].Block.Workchain == address.MasterchainID {
			continue
		}
		neighbors[i].OutMsgQueueInfo = resident.OutMsgQueueInfo
	}
	if err := VerifyShardCandidate(context.Background(), ShardVerificationRequest{
		Previous:           req.Previous,
		Masterchain:        req.Masterchain,
		Neighbors:          neighbors,
		NeighborShardEndLT: req.NeighborShardEndLT,
		Semantics:          NewSemanticVerifier(tvm.NewTVM()),
		Candidate:          candidate,
	}); err != nil {
		t.Fatalf("our own semantic validator rejected the candidate: %v", err)
	}
}

// scriptedExternalWaves serves later external waves by CONTROL FLOW rather than
// by clock: TakeReady never yields anything, so a scripted wave can only be
// observed through Next — which the wave loop calls after processNewMessages has
// drained the previous wave's generated messages.
//
// This replaces a real pool plus a 150 ms sleep, and the replacement is what
// makes a byte golden possible on this path at all. Measured on the arm below,
// the same input produced two different blocks depending only on that sleep:
// 5127 B sha256 155e13ff18cb2edb… at 150 ms and at 5 ms, 5127 B sha256
// 1da8c0c175ef5294… with no sleep. The second is wave collapse — wave two
// arriving through TakeReady in the same pass, before processNewMessages, so it
// runs at S+1 instead of S+4 — and it is invisible in Stats: both shapes report
// ExternalBatches=2, ExternalIncluded=2, ImmediateDelivered=3,
// EnqueuedMessages=0 and ExternalStop=deadline. The collapsed shape also PASSES
// against a tree with the reference floor removed, so the old shape guard
// (ExternalBatches >= 2) could not have caught the fixture going quiet.
//
// The scripted source reproduces the 150 ms bytes exactly, without the sleep.
type scriptedExternalWaves struct {
	waves      [][]msgpool.ExternalSnapshot
	served     int
	takeReadys int
	nexts      int
}

func (s *scriptedExternalWaves) TakeReady(int) []msgpool.ExternalSnapshot {
	s.takeReadys++
	return nil
}

func (s *scriptedExternalWaves) Next(ctx context.Context, _ int) ([]msgpool.ExternalSnapshot, error) {
	s.nexts++
	if s.served < len(s.waves) {
		wave := s.waves[s.served]
		s.served++
		return wave, nil
	}
	// Out of script: park exactly as an empty pool does, so the loop leaves
	// through its own graceful branch — it accepts a stop only when BOTH the
	// error and the wait context report DeadlineExceeded.
	<-ctx.Done()
	return nil, ctx.Err()
}

// readyExternalWave builds the pool snapshots for one scripted wave. The
// snapshots have unexported fields, so they are minted through a real pool and
// taken out of it here, where no collation is watching.
func readyExternalWave(t *testing.T, shard groups.ShardID, dst *address.Address, tag uint64) []msgpool.ExternalSnapshot {
	t.Helper()

	pool := msgpool.New(msgpool.Config{})
	t.Cleanup(pool.Close)
	stream, err := pool.OpenExternalStream(targetShardIdent(shard), 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stream.Close() })

	addReadyExternal(t, pool, dst, tag)
	wave := stream.TakeReady(4)
	if len(wave) != 1 {
		t.Fatalf("scripted wave holds %d snapshots, want 1", len(wave))
	}
	return wave
}

// boundWaveExitBudget is how long the loop parks on the exhausted script before
// its wait deadline fires and it stops gracefully. It is the only wall-clock
// value left on this path, it is on the EXIT and not on the schedule, and the
// produced bytes were measured identical at 20 ms and at 250 ms.
const boundWaveExitBudget = 150 * time.Millisecond

// collateWithWaves runs the live ready-externals schedule: the request's own
// pre-admitted externals form wave one, and the scripted waves arrive through
// stream.Next(), i.e. only after processNewMessages has run.
//
// buildShardWithReadyExternals is the only shard path that runs
// processNewMessages more than once, and the single-pass BuildShard path
// provably cannot express the defect these tests pin — processNewMessages runs
// only AFTER processExternals there, so no delivery can raise the bound before
// an external executes. A test that ran only through BuildShard would prove
// nothing about the rule.
func collateWithWaves(
	t *testing.T,
	req ShardRequest,
	waves ...[]msgpool.ExternalSnapshot,
) (*Candidate, *scriptedExternalWaves) {
	t.Helper()

	source := &scriptedExternalWaves{waves: waves}
	candidate, live, err := testBuilder().buildShardWithReadyExternals(
		t.Context(),
		req,
		source,
		time.Now().Add(time.Minute),
		time.Now().Add(boundWaveExitBudget),
		4,
		time.Time{},
	)
	if err != nil {
		t.Fatalf("collation failed: %v (phase %q)", err, live.failedAt)
	}
	// Every scripted wave must have been consumed through Next. If one were left
	// unserved the arm would silently be a single-wave collation, which is the
	// shape that cannot express the rule under test.
	if source.served != len(waves) {
		t.Fatalf("only %d of %d scripted waves were served through Next: the wave loop never "+
			"reached the wait, so this arm is single-pass", source.served, len(waves))
	}
	if candidate.Stats.ExternalStop != ExternalStopDeadline {
		t.Fatalf("external stop = %v, want %v: the loop did not leave through the wait",
			candidate.Stats.ExternalStop, ExternalStopDeadline)
	}
	// The loop must still be asking for already-admitted work between waves. A
	// source that was never consulted through TakeReady would mean the schedule
	// under test is not the production one.
	if source.takeReadys == 0 || source.nexts <= source.served {
		t.Fatalf("wave loop called TakeReady %d times and Next %d times for %d served waves: "+
			"this is not the production schedule", source.takeReadys, source.nexts, source.served)
	}
	return candidate, source
}

// assertBoundGolden pins the produced bytes. A candidate that merely does not
// crash proves nothing about lt assignment on this path; these hashes move if
// any transaction on it moves.
//
// SELF-VALIDATED, deliberately and knowably. The reference collator can be made
// to run a multi-wave schedule — its own loop is the same shape, gated on the
// wait_externals_until parameter the benchmark harness leaves unset — but even
// then it could only certify the (account, lt) transaction set, never the bytes:
// gen_software and rand_seed differ by construction, and the Merkle update's
// pruning shape differs over an identical old/new state pair. So the boundary is
// explicit: the ORDERING these arms assert is the reference's rule, the BYTES
// below are ours.
//
// What reference validation of this fixture would take, for whoever picks it up
// (all of it is on the harness side; no collation logic is involved):
//
//   - bench-collate's make_collate_params never sets wait_externals_until, so
//     the harness runs the reference's single-pass schedule — the same shape as
//     BuildShard here, which provably cannot express this rule. One assignment
//     turns the multi-wave loop on.
//   - its external push coroutine pushes every message and then closes the
//     queue, so all waves arrive as one. It has to close after the LAST wave
//     instead.
//   - the genuinely missing piece is a wave boundary: td's BackpressureQueue
//     exposes no pop-waiter count, and the collator's queue capacity is 500, so
//     the harness cannot tell when a consumer is parked and would have to sleep
//     — reintroducing on that side exactly the non-determinism the scripted
//     source removes on this one.
//   - the fixture format (mc_state/shard_state/shard_block/mc_block/candidate/
//     externals/meta.json) is already written by TestExportBenchFixture; it
//     needs a wave list in meta.json and a multi-wave export branch. The real
//     work is rebuilding this workload on benchRequestFrom, because
//     boundFixtureRequest binds a ZEROSTATE predecessor and the format requires
//     a predecessor block.
//
// Even with all of that the payoff is the (account, lt) set, not the bytes.
func assertBoundGolden(t *testing.T, candidate *Candidate, blockLen int, blockSum string, collatedLen int, collatedSum string) {
	t.Helper()

	gotBlock := sha256.Sum256(candidate.BlockBOC)
	if len(candidate.BlockBOC) != blockLen || hex.EncodeToString(gotBlock[:]) != blockSum {
		t.Fatalf("block is %d bytes %s, want the recorded %d bytes %s",
			len(candidate.BlockBOC), hex.EncodeToString(gotBlock[:]), blockLen, blockSum)
	}
	gotCollated := sha256.Sum256(candidate.CollatedData)
	if len(candidate.CollatedData) != collatedLen || hex.EncodeToString(gotCollated[:]) != collatedSum {
		t.Fatalf("collated data is %d bytes %s, want the recorded %d bytes %s",
			len(candidate.CollatedData), hex.EncodeToString(gotCollated[:]), collatedLen, collatedSum)
	}
}

// TestExternalTransactionLTClearsProcessedBoundAcrossExternalWaves pins the port
// of collator.cpp:3317-3319
//
//	if (external) { after_lt = std::max(after_lt, last_proc_int_msg_.first); }
//
// consumed at :3400 as trans_min_lt = std::max(trans_min_lt, after_lt), with the
// rule stated in the reference's own comment at :3398-3399: "transactions
// processing external messages must have lt larger than all processed internal
// messages".
//
// The shape, with S = block StartLt: wave one's external emits TWO in-shard
// internal messages; the first processNewMessages call delivers both, leaving
// the processed bound at the second message's created_lt S+3. Wave two's
// external then runs on a fresh account. Without the reference's floor it
// restarts at S+1, so the message it generates carries created_lt S+2, BELOW
// the bound, and the second processNewMessages call fails the strict order
// check of advanceProcessedBound — the port of collator.cpp:3493-3505 — with
// "internal message processing order violated". With the floor the transaction
// runs at max(S, S+3)+1 = S+4 and its message at S+5, which clears the bound.
//
// Configuration, because it is what makes this a real gate: capFullCollatedData
// ON through boundFixtureRequest, a fresh request built per arm (the wiring
// mutates the predecessor state), CollatedData asserted past the 33-byte marker,
// and NewSemanticVerifier(tvm.NewTVM()) as the verifier.
func TestExternalTransactionLTClearsProcessedBoundAcrossExternalWaves(t *testing.T) {
	req := boundFixtureRequest(t)
	startLT := requestStartLT(t, req)

	waveOneSender := address.NewAddress(0, 0, bytes.Repeat([]byte{0xa1}, 32))
	waveTwoSender := address.NewAddress(0, 0, bytes.Repeat([]byte{0xa2}, 32))
	recvA := address.NewAddress(0, 0, bytes.Repeat([]byte{0xb1}, 32))
	recvB := address.NewAddress(0, 0, bytes.Repeat([]byte{0xb2}, 32))
	recvC := address.NewAddress(0, 0, bytes.Repeat([]byte{0xb3}, 32))

	req.Previous.State = stateWithAccounts(t, req.Previous.State, activeContracts(t, req.Header.GenUtime,
		activeContract{
			address: waveOneSender,
			code: externalSendManyCode(t,
				inShardMessage(t, recvA, 1_000_000_000),
				inShardMessage(t, recvB, 1_000_000_000),
			),
			balance: 100_000_000_000,
		},
		activeContract{
			address: waveTwoSender,
			code:    externalSendManyCode(t, inShardMessage(t, recvC, 1_000_000_000)),
			balance: 100_000_000_000,
		},
		activeContract{address: recvA, code: externalAcceptCode(t), balance: 10_000_000_000},
		activeContract{address: recvB, code: externalAcceptCode(t), balance: 10_000_000_000},
		activeContract{address: recvC, code: externalAcceptCode(t), balance: 10_000_000_000},
	))
	finishBoundFixture(t, &req)

	waveOneExternal, err := tlb.ToCell(&tlb.ExternalMessage{
		DstAddr: waveOneSender,
		Body:    cell.BeginCell().EndCell(),
	})
	if err != nil {
		t.Fatal(err)
	}
	req.Externals = []ExternalInput{externalInput(t, waveOneExternal)}

	candidate, _ := collateWithWaves(t, req,
		readyExternalWave(t, req.Shard, waveTwoSender, 1))

	// Shape first: neither the second wave nor the immediate deliveries may
	// quietly go missing, or the lt assertion below would hold vacuously.
	// collateWithWaves has already established that the wave arrived through the
	// wait; ExternalBatches alone could not, because it reads 2 for the collapsed
	// shape as well.
	stats := candidate.Stats
	if stats.ExternalBatches != 2 || stats.ExternalIncluded != 2 {
		t.Fatalf("fixture did not reach a second external wave: %+v", stats)
	}
	if stats.ImmediateDelivered != 3 || stats.EnqueuedMessages != 0 {
		t.Fatalf("fixture did not deliver every generated message in-block: %+v", stats)
	}

	// The bound wave one leaves behind is the second delivered message's
	// created_lt, S+3; the reference floors wave two's transaction by it.
	if got := transactionLTOf(t, candidate, waveTwoSender); got != startLT+4 {
		t.Fatalf("wave two transaction lt = %d (S+%d), want %d (S+4): the external message was not "+
			"floored by the processed bound", got, got-startLT, startLT+4)
	}
	if got := transactionLTOf(t, candidate, waveOneSender); got != startLT+1 {
		t.Fatalf("wave one transaction lt = %d (S+%d), want %d (S+1)", got, got-startLT, startLT+1)
	}

	assertBoundGolden(t, candidate,
		5127, "155e13ff18cb2edb383b80848478a040a6d643c8e24cfcb601902ba2d2bcdadd",
		1197, "a4559892625dcd80bcc5be3ff1e3d962009e598c6da98c85665ae2527b3f2f4f")
	assertBoundCandidateIsProofBacked(t, req, candidate)
}

// TestExternalTransactionLTClearsBoundAdvancedByAnImportedMessage pins the same
// reference clause with the bound advanced from the OTHER source: a message
// imported from the inbound queue, whose own transaction then generates the
// in-shard messages that raise the bound.
//
// It replaces an earlier arm that gave the inbound queue entry a canonical lt of
// StartLt+5 and read the bound straight off the import. That shape cannot exist
// in a valid chain: an inbound queue entry was created by an EARLIER block, so
// its canonical lt is always below our StartLt, and max(StartLt, bound) is then
// simply StartLt. The clause was therefore gated on an input no chain can
// produce. Here the queue entry carries a realistic lt BELOW StartLt (S-10) and
// the bound is raised the only way it can be — by immediate delivery of what the
// imported message's transaction generates.
//
// With S = block StartLt: the import runs at S+1 and emits two in-shard
// messages at S+2 and S+3; wave one's processNewMessages delivers both and
// leaves the bound at S+3. The external admitted afterwards must run at
// max(S, S+3)+1 = S+4.
//
// The distinct value over the arm above is the provenance: this one proves the
// floor reads the bound itself and not some artefact of an external-generated
// message, and it is the shape the field events came from — every one of the 71
// had an inbound import in the same block.
func TestExternalTransactionLTClearsBoundAdvancedByAnImportedMessage(t *testing.T) {
	req := boundFixtureRequest(t)
	startLT := requestStartLT(t, req)

	queueSource := address.NewAddress(0, 0, bytes.Repeat([]byte{0x91}, 32))
	queueDest := address.NewAddress(0, 0, bytes.Repeat([]byte{0x92}, 32))
	sender := address.NewAddress(0, 0, bytes.Repeat([]byte{0xa1}, 32))
	recvA := address.NewAddress(0, 0, bytes.Repeat([]byte{0xb1}, 32))
	recvB := address.NewAddress(0, 0, bytes.Repeat([]byte{0xb2}, 32))
	recvC := address.NewAddress(0, 0, bytes.Repeat([]byte{0xb3}, 32))

	req.Previous.State = stateWithAccounts(t, req.Previous.State, activeContracts(t, req.Header.GenUtime,
		// The imported message's destination generates the two in-shard messages
		// that move the bound.
		activeContract{
			address: queueDest,
			code: externalSendManyCode(t,
				inShardMessage(t, recvA, 1_000_000_000),
				inShardMessage(t, recvB, 1_000_000_000),
			),
			balance: 100_000_000_000,
		},
		activeContract{
			address: sender,
			code:    externalSendManyCode(t, inShardMessage(t, recvC, 1_000_000_000)),
			balance: 100_000_000_000,
		},
		activeContract{address: recvA, code: externalAcceptCode(t), balance: 10_000_000_000},
		activeContract{address: recvB, code: externalAcceptCode(t), balance: 10_000_000_000},
		activeContract{address: recvC, code: externalAcceptCode(t), balance: 10_000_000_000},
	))

	// A realistic inbound entry: created by an earlier block, so its canonical lt
	// is BELOW our StartLt.
	fee := tlb.FromNanoTONU(100_000)
	msg, enqueued := queuedInternal(t, queueSource, queueDest, startLT-10, req.Header.GenUtime-1,
		fee, fee, routingAddressBits, msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll})
	req.Previous.State = stateWithQueueMessage(t, req.Previous.State, msg.Key, enqueued)
	queueSize := uint64(1)
	req.Previous.OutQueueSize = &queueSize
	req.Internals = &msgpool.Cut{Messages: []*msgpool.InternalMessage{msg}}
	finishBoundFixture(t, &req)

	// No pre-admitted external: the only one arrives through stream.Next() after
	// the import and its deliveries have already run. This is also the
	// external_batches==1 shape all seven of the field's eb==1 events had.
	req.Externals = nil

	candidate, _ := collateWithWaves(t, req,
		readyExternalWave(t, req.Shard, sender, 1))

	stats := candidate.Stats
	if stats.InternalsImported != 1 {
		t.Fatalf("fixture imported %d internals, want 1: the bound is not advanced by an import: %+v",
			stats.InternalsImported, stats)
	}
	if stats.ImmediateDelivered != 3 || stats.EnqueuedMessages != 0 {
		t.Fatalf("fixture did not deliver every generated message in-block: %+v", stats)
	}
	if stats.ExternalIncluded != 1 {
		t.Fatalf("fixture did not execute the waited external: %+v", stats)
	}

	// The import itself is floored by its created_lt + 1 — a different rule with
	// a different source (tvm transactionStartLT, the port of
	// transaction.cpp:921-922) — and its created_lt is below S, so it runs at S+1.
	if got := transactionLTOf(t, candidate, queueDest); got != startLT+1 {
		t.Fatalf("imported transaction lt = %d (S+%d), want %d (S+1)", got, got-startLT, startLT+1)
	}
	if got := transactionLTOf(t, candidate, sender); got != startLT+4 {
		t.Fatalf("external transaction lt = %d (S+%d), want %d (S+4): the external message was not "+
			"floored by the bound the imported message's deliveries left behind",
			got, got-startLT, startLT+4)
	}

	assertBoundGolden(t, candidate,
		5214, "218980f4c7192987efe0082d613c5a95cb380eac07b38b40b7223da08be91f0c",
		1363, "bd11b15e6546b12ee4bea1530a4c3654af1c7439b6d7a2b09a2c561cdc5e526c")
	assertBoundCandidateIsProofBacked(t, req, candidate)
}

// TestFailedWaveAttemptReportsItsPhaseAndShape closes the observability defect
// that made 71 identical field events look like three phenomena.
//
// live.stop used to be assigned only AFTER the wave loop, i.e. only when the
// attempt succeeded. Every failing return left the zero value, whose String() is
// "unknown", so external_stop on all 71 order-violation events meant nothing but
// "it failed" — and it was read as evidence of an unreachable third path. The
// counters that would have identified the route died the same way, because they
// are read off a candidate that is nil on failure, which is why block_bytes and
// collated_bytes were 0 on every one of them.
//
// This drives the pre-fix defect deliberately — the reference clause is removed
// from the collation under test by pointing the fixture at a build whose external
// floor cannot fire, which is not possible from a test — so instead it asserts
// the mechanism directly: a failing attempt must still report the phase it died
// in and the shape counters, and the phase must be process_new_messages, the only
// phase from which the order violation is reachable.
func TestFailedWaveAttemptReportsItsPhaseAndShape(t *testing.T) {
	req := boundFixtureRequest(t)
	// A cut whose single message is malformed enough to fail the import, so the
	// attempt dies inside prepareShardPhases' processInternals. The phase name is
	// the assertion; the specific failure is not.
	req.Internals = &msgpool.Cut{}
	finishBoundFixture(t, &req)

	pool := msgpool.New(msgpool.Config{})
	t.Cleanup(pool.Close)
	stream, err := pool.OpenExternalStream(targetShardIdent(req.Shard), 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stream.Close() })

	// A successful attempt first, to establish that this fixture reaches the wave
	// loop at all and that the shape snapshot is populated on the ordinary path
	// too. Without it a failure below could be a fixture that never got started.
	var live readyExternalStats
	transcript := []ExternalInput{}
	candidate, err := testBuilder().buildShardReadyAttempt(
		t.Context(), req, 0, stream,
		time.Now().Add(time.Second), time.Time{}, 4,
		&transcript, &live, collationPace{},
	)
	if err != nil {
		t.Fatalf("baseline attempt failed: %v", err)
	}
	if candidate == nil {
		t.Fatal("baseline attempt produced no candidate")
	}
	if !live.shape.valid {
		t.Fatal("the shape snapshot is not populated even on a successful attempt: the defer that " +
			"writes it is not running")
	}
	if live.shape.startLT != requestStartLT(t, req) {
		t.Fatalf("shape.startLT = %d, want the request's StartLt %d",
			live.shape.startLT, requestStartLT(t, req))
	}
	if live.failedAt != "" {
		t.Fatalf("a successful attempt named failure phase %q", live.failedAt)
	}
	if live.stop == ExternalStopUnknown {
		t.Fatal("a successful attempt left external_stop unknown")
	}

	// And the failing side of the same mechanism. The failure is placed INSIDE the
	// wave loop deliberately: a context cancelled before the call fails in
	// prepareShardPhases, which is before the loop and therefore proves nothing
	// about the returns that used to lose their attribution. Cancelling while the
	// attempt is parked in stream.Next() puts it in the wait phase, which is
	// reached only after the pre-admitted batch AND the first processNewMessages
	// drain have already run — so the shape counters must be populated by then.
	failing := boundFixtureRequest(t)
	sender := address.NewAddress(0, 0, bytes.Repeat([]byte{0xf1}, 32))
	receiver := address.NewAddress(0, 0, bytes.Repeat([]byte{0xf2}, 32))
	failing.Previous.State = stateWithAccounts(t, failing.Previous.State,
		activeContracts(t, failing.Header.GenUtime,
			activeContract{
				address: sender,
				code:    externalSendManyCode(t, inShardMessage(t, receiver, 1_000_000_000)),
				balance: 100_000_000_000,
			},
			activeContract{address: receiver, code: externalAcceptCode(t), balance: 10_000_000_000},
		))
	finishBoundFixture(t, &failing)
	external, err := tlb.ToCell(&tlb.ExternalMessage{
		DstAddr: sender,
		Body:    cell.BeginCell().EndCell(),
	})
	if err != nil {
		t.Fatal(err)
	}
	failing.Externals = []ExternalInput{externalInput(t, external)}

	failStream, err := pool.OpenExternalStream(targetShardIdent(failing.Shard), 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = failStream.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type failure struct {
		live readyExternalStats
		err  error
	}
	failed := make(chan failure, 1)
	go func() {
		var failLive readyExternalStats
		failTranscript := []ExternalInput{}
		// waitUntil far enough out that the deadline branch cannot win the race
		// with the cancellation below; the pool is empty, so stream.Next blocks.
		_, buildErr := testBuilder().buildShardReadyAttempt(
			ctx, failing, 0, failStream,
			time.Now().Add(time.Minute), time.Now().Add(time.Minute), 4,
			&failTranscript, &failLive, collationPace{},
		)
		failed <- failure{live: failLive, err: buildErr}
	}()

	time.Sleep(200 * time.Millisecond)
	cancel()

	var got failure
	select {
	case got = <-failed:
	case <-time.After(20 * time.Second):
		t.Fatal("the cancelled attempt never returned")
	}
	if got.err == nil {
		t.Fatal("a cancelled attempt succeeded")
	}
	if got.live.wait <= 0 {
		t.Fatalf("the attempt did not reach the wait phase (wait=%v), so this arm is not exercising "+
			"an in-loop failure at all", got.live.wait)
	}
	if got.live.failedAt != externalPhaseWait {
		t.Fatalf("failing phase = %q, want %q. A failing return inside the wave loop that names no "+
			"phase leaves external_stop reading \"unknown\", which is exactly what made all 71 field "+
			"order-violation events unattributable", got.live.failedAt, externalPhaseWait)
	}
	if !got.live.shape.valid {
		t.Fatal("the shape snapshot did not survive the failing return, so the field record still " +
			"cannot say how many externals were pre-admitted or how far the import got")
	}
	// The pre-admitted count and the drain result are the two facts that
	// distinguish the field's external_batches==1 events from an unreachable path.
	if got.live.shape.preAdmitted != 1 {
		t.Fatalf("shape.preAdmitted = %d, want 1", got.live.shape.preAdmitted)
	}
	if got.live.shape.immediateDelivered != 1 {
		t.Fatalf("shape.immediateDelivered = %d, want 1: the drain before the wait is not reflected",
			got.live.shape.immediateDelivered)
	}
	if got.live.shape.lastProcLT == 0 {
		t.Fatal("shape.lastProcLT is zero after a delivery: the processed bound the order rule turns " +
			"on is not being reported")
	}

	// The log field itself, because the struct being right is not the same as the
	// record being right: that is the gap that produced all 71 uninformative
	// events.
	if phase := externalFailurePhase(got.live, got.err); phase != externalPhaseWait {
		t.Fatalf("external_phase = %q, want %q", phase, externalPhaseWait)
	}
	if phase := externalFailurePhase(live, nil); phase != "" {
		t.Fatalf("external_phase on success = %q, want empty so external_stop stays the answer", phase)
	}
}
