package collator

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math/big"
	"sync"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
	execop "github.com/xssnick/tonutils-go/tvm/op/exec"
	funcsop "github.com/xssnick/tonutils-go/tvm/op/funcs"
	mathop "github.com/xssnick/tonutils-go/tvm/op/math"
	stackop "github.com/xssnick/tonutils-go/tvm/op/stack"

	"github.com/xssnick/gton/service/validator/msgpool"
)

// This file builds the block shapes the collation and validation benchmarks
// run on. Everything here is test-only: the benchmarks reach the individual
// phases through the unexported pipeline instead of through an instrumentation
// hook, so the production path carries no timing code at all.
//
// A workload is deliberately not a microbenchmark fixture. It carries the
// mainnet configuration, an account dictionary deep enough for its lookups to
// cost what they cost in production, a populated outbound queue that the
// cleanup phase has to scan, inbound internals that arrive as pooled own-queue
// entries, and externals that run real TVM code and emit real messages.

// benchProfile describes one block shape.
type benchProfile struct {
	name string
	// accounts is the total number of contracts in the predecessor state. It
	// sets the depth of the account dictionary every transaction walks and the
	// size of the tree the state update has to diff.
	accounts int
	// externals and internals are the inbound work offered to the collator.
	// Each one lands on its own sender account, which forwards one internal
	// message to its own receiver account — so the block contains two
	// transactions and one immediate delivery per inbound message, the shape of
	// an ordinary transfer.
	externals int
	internals int
	// queued is the number of masterchain-bound entries already sitting in the
	// outbound queue. All of them are covered by the masterchain frontier, so
	// the cleanup phase scans the whole queue and dequeues every entry.
	queued int
	// deferred is the number of accounts carrying one message in the predecessor
	// DispatchQueue. The first dispatch pass takes one message per account, so
	// each becomes a delivered message and a transaction.
	deferred int
	// fullCollated turns on capFullCollatedData, which makes collation carry a
	// proof-size estimator on every cell load and build the collated proof roots
	// at the end. Without it the collated data is an empty BOC and that whole
	// path is unmeasured. It implies mintPredecessor.
	fullCollated bool
	// mintPredecessor gives the request a real predecessor block rather than a
	// bare state. Collation does not need one; the fixture export does, because
	// the reference collator asks its manager for the previous block data.
	mintPredecessor bool
}

// benchProfiles are the shapes the benchmarks run by default. Each inbound
// message costs two transactions — the sender and the receiver it delivers to —
// and each deferred message one, so a profile yields 2*(externals+internals) +
// deferred of them: idle 0, light 225, mid 400, typical 560, heavy 1000.
//
// "idle" isolates the per-block fixed cost (prepare, processed info, account
// finish, serialization) from the per-message cost of the rest. "mid" and
// "heavy" share the same 100k accounts so that only the transaction count
// differs between them. "heavy" is sized to sit just under the mainnet block
// limits — at ~1.3 MB it is past the 1 MiB soft byte limit and short of the
// 2 MiB hard one — and buildBenchWorkload rejects any profile they would
// truncate.
var benchProfiles = []benchProfile{
	{name: "idle", accounts: 10_000},
	{name: "light", accounts: 20_000, externals: 50, internals: 50, queued: 100, deferred: 25},
	{name: "mid", accounts: 100_000, externals: 90, internals: 90, queued: 40, deferred: 40},
	{name: "typical", accounts: 50_000, externals: 125, internals: 125, queued: 200, deferred: 60},
	{name: "heavy", accounts: 100_000, externals: 225, internals: 225, queued: 100, deferred: 100},
	{name: "heavy-collated", accounts: 100_000, externals: 225, internals: 225, queued: 100, deferred: 100,
		fullCollated: true},
}

// benchWorkload is one prepared block: the collation request, the candidate the
// builder produces from it, and the verification request that validates that
// candidate.
type benchWorkload struct {
	profile      benchProfile
	request      ShardRequest
	candidate    *Candidate
	verification ShardVerificationRequest
}

// TestBenchWorkloadShape keeps the benchmarks measuring a block. Every profile
// knob feeds exactly one collation phase, and a workload that quietly stopped
// producing transactions, deliveries or dequeues would still benchmark — at a
// number that means nothing. This is the cheap profile; the rest are built by
// the benchmarks themselves.
func TestBenchWorkloadShape(t *testing.T) {
	profile := benchProfiles[1]
	workload := benchWorkloadFor(t, profile)
	stats := workload.candidate.Stats
	inbound := profile.externals + profile.internals

	// Each external and each imported internal runs a sender, which forwards one
	// message that is delivered in the same block; each deferred message is
	// dispatched and delivered. The predecessor queue is drained completely: the
	// internals by import, the rest by cleanup.
	if want := uint32(2*inbound + profile.deferred); stats.Transactions != want {
		t.Errorf("transactions = %d, want %d", stats.Transactions, want)
	}
	if want := uint32(inbound + profile.deferred); stats.ImmediateDelivered != want {
		t.Errorf("immediate deliveries = %d, want %d", stats.ImmediateDelivered, want)
	}
	if stats.EnqueuedMessages != 0 {
		t.Errorf("enqueued %d messages, want an entirely delivered block", stats.EnqueuedMessages)
	}
	if size := *workload.request.Previous.OutQueueSize; size != uint64(profile.internals+profile.queued) {
		t.Errorf("predecessor queue holds %d entries, want %d", size, profile.internals+profile.queued)
	}

	// Trivial contract code would leave the TVM invisible in the phase
	// breakdown. Mainnet transfers cost single-digit thousands of gas.
	perTx := stats.GasUsed / uint64(stats.Transactions)
	if perTx < 2_000 || perTx > 25_000 {
		t.Errorf("gas per transaction = %d, want a realistic transfer cost", perTx)
	}
}

// TestBuildCandidateSerializesOverflowingBlock covers the one shape the
// profiles deliberately stay under: the block hits its limits mid-flight, so
// the generated messages that no longer fit are enqueued instead of delivered.
// Serializing that state used to fail — the state update pruned a queue node
// the source proof could not resolve, and BuildShard returned "unknown pruned
// branch" — so the overflow path is held down here even though no benchmark
// measures it.
func TestBuildCandidateSerializesOverflowingBlock(t *testing.T) {
	req := benchRequest(t, benchProfile{accounts: 2_000, externals: 300, internals: 300})
	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatalf("build overflowing candidate: %v", err)
	}
	if candidate.Stats.Load <= LoadNormal {
		t.Fatalf("block load class is %v, want a block the limits stopped", candidate.Stats.Load)
	}
	if candidate.Stats.EnqueuedMessages == 0 {
		t.Fatal("overflowing block enqueued nothing, so it does not cover the queued-generated path")
	}

	verification := shardVerificationRequest(req, candidate)
	verification.NeighborShardEndLT = req.NeighborShardEndLT
	verification.Semantics = NewSemanticVerifier(tvm.NewTVM())
	if err = VerifyShardCandidate(context.Background(), verification); err != nil {
		t.Fatalf("verify overflowing candidate: %v", err)
	}
}

var (
	benchWorkloadMu    sync.Mutex
	benchWorkloadCache = map[string]*benchWorkload{}
)

// benchWorkloadFor builds a profile once per process. Construction dominates the
// benchmark otherwise: a 100k-account dictionary is more expensive to fill than
// the block that walks it. Neither BuildShard nor VerifyShardCandidate mutates
// its request, so one instance is safe to reuse across iterations.
func benchWorkloadFor(tb testing.TB, profile benchProfile) *benchWorkload {
	tb.Helper()

	benchWorkloadMu.Lock()
	defer benchWorkloadMu.Unlock()
	if cached, ok := benchWorkloadCache[profile.name]; ok {
		return cached
	}
	built := buildBenchWorkload(tb, profile)
	benchWorkloadCache[profile.name] = built
	return built
}

func buildBenchWorkload(tb testing.TB, profile benchProfile) *benchWorkload {
	tb.Helper()

	req := benchRequest(tb, profile)
	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		tb.Fatalf("build %s workload candidate: %v", profile.name, err)
	}
	verification := ShardVerificationRequest{
		Previous:           req.Previous,
		Masterchain:        req.Masterchain,
		Neighbors:          req.Neighbors,
		NeighborShardEndLT: req.NeighborShardEndLT,
		Semantics:          NewSemanticVerifier(tvm.NewTVM()),
		Candidate:          candidate,
	}
	if err = VerifyShardCandidate(context.Background(), verification); err != nil {
		tb.Fatalf("verify %s workload candidate: %v", profile.name, err)
	}
	// A profile that the block limits cut short would still benchmark, just not
	// the block it claims to. Sizing is only ever wrong here by accident, so it
	// is a build failure rather than a note in the output.
	stats := candidate.Stats
	if int(stats.ExternalIncluded) != profile.externals ||
		int(stats.InternalsImported) != profile.internals ||
		int(stats.DispatchedMessages) != profile.deferred ||
		stats.OutQueueSize != 0 {
		tb.Fatalf("%s profile does not fit the block limits: included %d/%d externals, "+
			"imported %d/%d internals, dispatched %d/%d deferred, left %d messages queued "+
			"(load class %v) — resize the profile",
			profile.name, stats.ExternalIncluded, profile.externals,
			stats.InternalsImported, profile.internals,
			stats.DispatchedMessages, profile.deferred, stats.OutQueueSize, stats.Load)
	}

	return &benchWorkload{
		profile:      profile,
		request:      req,
		candidate:    candidate,
		verification: verification,
	}
}

// benchRequest assembles the collation request for a profile.
func benchRequest(tb testing.TB, profile benchProfile) ShardRequest {
	return benchRequestFrom(tb, profile, emptyCandidateRequest(tb))
}

// benchRequestFrom fills a caller-supplied base request, so the fixture export
// can settle the masterchain block identity before anything derives from it.
func benchRequestFrom(tb testing.TB, profile benchProfile, req ShardRequest) ShardRequest {
	tb.Helper()

	startLT := requestStartLT(tb, req)
	senders := profile.externals + profile.internals

	// Every account that actually transacts gets its own code cell carrying its
	// own outgoing message, so the generated messages spread across the
	// destination dictionary instead of collapsing onto one key. Receivers only
	// accept, which bounds the block at one delivery hop.
	accounts, err := tlb.NewShardAccountsAugDict()
	if err != nil {
		tb.Fatal(err)
	}
	idleCode := externalAcceptCode(tb)
	receiverCode := benchWorkCode(tb, benchReceiverLoops, nil)
	senderAddrs := make([]*address.Address, senders)
	for i := range senderAddrs {
		senderAddrs[i] = benchAddress("sender", i)
		receiver := benchAddress("receiver", i)
		outbound, err := tlb.ToCell(&tlb.InternalMessage{
			IHRDisabled: true,
			SrcAddr:     address.NewAddressNone(),
			DstAddr:     receiver,
			Amount:      tlb.FromNanoTONU(100_000_000),
			Body:        cell.BeginCell().EndCell(),
		})
		if err != nil {
			tb.Fatal(err)
		}
		benchSetAccount(tb, accounts, activeContract{
			address: senderAddrs[i],
			code:    benchWorkCode(tb, benchSenderLoops, outbound),
			balance: 100_000_000_000,
		}, req.Header.GenUtime)
		benchSetAccount(tb, accounts, activeContract{
			address: receiver,
			code:    receiverCode,
			balance: 10_000_000_000,
		}, req.Header.GenUtime)
	}
	for i := range profile.deferred {
		benchSetAccount(tb, accounts, activeContract{
			address: benchAddress("deferred-target", i),
			code:    receiverCode,
			balance: 10_000_000_000,
		}, req.Header.GenUtime)
	}
	for i := 2*senders + profile.deferred; i < profile.accounts; i++ {
		benchSetAccount(tb, accounts, activeContract{
			address: benchAddress("idle", i),
			code:    idleCode,
			balance: 1_000_000_000,
		}, req.Header.GenUtime)
	}

	// The inbound internals are own-queue entries: the shard sent them to itself
	// in an earlier block and the pool now offers them back. The cleanup entries
	// are masterchain-bound and covered by the frontier below.
	fee := tlb.FromNanoTONU(100_000)
	entries := make([]benchQueueEntry, 0, profile.internals+profile.queued)
	internals := make([]*msgpool.InternalMessage, profile.internals)
	source := benchAddress("queue-source", 0)
	for i := range internals {
		lt := startLT - uint64(profile.internals+profile.queued) + uint64(i)
		msg, enqueued := queuedInternal(tb, source, senderAddrs[profile.externals+i], lt,
			req.Header.GenUtime-1, fee, fee, 96, msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll})
		internals[i] = msg
		entries = append(entries, benchQueueEntry{key: msg.Key, value: enqueued})
	}
	cleanupLT := startLT - uint64(profile.queued)
	for i := range profile.queued {
		lt := cleanupLT + uint64(i)
		msg, enqueued := queuedInternal(tb, benchAddress("cleanup-source", i),
			address.NewAddress(0, 0xff, benchAddress("cleanup-target", i).Data()), lt,
			req.Header.GenUtime-1, fee, fee, 0, msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll})
		entries = append(entries, benchQueueEntry{key: msg.Key, value: enqueued})
	}
	req.Previous.State = benchPreviousState(tb, req.Previous.State, accounts, entries,
		benchDispatchQueue(tb, profile.deferred, req.Header.GenUtime-1, startLT-1))
	queueSize := uint64(len(entries))
	req.Previous.OutQueueSize = &queueSize

	if profile.fullCollated || profile.mintPredecessor {
		// Full collated data has to prove the predecessor, so the request needs
		// a real block whose state update produces exactly this state — a hand
		// assembled state cannot supply one. One idle collation over the seeded
		// state mints it: with no neighbours it runs no cleanup, and with no cut
		// it imports nothing, so the accounts, the queue and the dispatch queue
		// all pass through untouched into the block it produces.
		req = benchMintPredecessorBlock(tb, req)
		startLT = requestStartLT(tb, req)
		if profile.fullCollated {
			// Enabled only now: the minting collation has no neighbours, and the
			// capability demands a neighbour set matching the registered shards.
			// The predecessor and account-storage proofs are built by collation
			// itself, so no external proof provider is needed.
			req.Masterchain.Config.capabilities |= capFullCollatedData
		}
	}

	// The own shard is a neighbor with the predecessor's frontier, which is
	// empty — so cleanup drops that part at once and the internals survive to be
	// imported. The masterchain frontier covers every cleanup entry, and has to
	// be published through the masterchain queue the validator authenticates.
	masterFrontier := []tlb.ProcessedUptoRecord{{
		ShardPrefix: msgpool.ShardAll,
		MCSeqno:     req.Masterchain.ID.SeqNo,
		LastMsgLT:   startLT,
		LastMsgHash: processedInfinityHash,
	}}
	req.Masterchain.OutMsgQueueInfo = benchQueueInfo(tb, masterFrontier)
	previousState := loadPreviousShardState(tb, req)
	req.Neighbors = []Neighbor{
		{
			Block: req.Previous.ID,
			Shard: msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll},
			// The end lt of the own shard is its predecessor's, which full
			// collated data checks against the state it proves.
			EndLT:           previousState.GenLT,
			OutMsgQueueInfo: previousState.OutMsgQueueInfo,
		},
		{
			Block:     req.Masterchain.ID,
			Shard:     msgpool.ShardIdent{Workchain: address.MasterchainID, Shard: msgpool.ShardAll},
			EndLT:     req.Masterchain.EndLT,
			Processed: masterFrontier,
		},
	}
	req.NeighborShardEndLT = func(uint32, int32, uint64) uint64 { return startLT }

	req.Externals = make([]ExternalInput, profile.externals)
	for i := range req.Externals {
		message, err := tlb.ToCell(&tlb.ExternalMessage{
			DstAddr: senderAddrs[i],
			Body:    cell.BeginCell().MustStoreUInt(uint64(i), 32).EndCell(),
		})
		if err != nil {
			tb.Fatal(err)
		}
		req.Externals[i] = externalInput(tb, message)
	}
	req.MaxExternalAttempts = max(profile.externals, 1)
	// A complete cut lets generated messages be delivered in the same block,
	// which is the normal case and the one that costs the most.
	req.Internals = &msgpool.Cut{Messages: internals}

	return req
}

// benchComputeLoops sizes the compute phase of a benchmark contract. Trivial
// accept-and-send code runs in a few hundred gas, which would leave the TVM
// nearly invisible next to the dictionary work; a real transfer costs a few
// thousand. Each loop is eight arithmetic steps.
const (
	benchSenderLoops   = 40
	benchReceiverLoops = 15
)

// benchWorkCode is a contract that accepts the message, spends a realistic
// amount of gas, and optionally forwards one internal message.
func benchWorkCode(tb testing.TB, loops int, message *cell.Cell) *cell.Cell {
	tb.Helper()

	body := cell.BeginCell()
	for range 8 {
		if err := body.StoreBuilder(mathop.ADDCONST(1).Serialize()); err != nil {
			tb.Fatal(err)
		}
	}
	code := externalAcceptCode(tb).ToBuilder()
	steps := []*cell.Builder{
		stackop.PUSHINT(big.NewInt(0)).Serialize(),            // accumulator
		stackop.PUSHINT(big.NewInt(int64(loops))).Serialize(), // repeat count
		stackop.PUSHCONT(body.EndCell()).Serialize(),
		execop.REPEAT().Serialize(),
		stackop.DROP().Serialize(),
	}
	if message != nil {
		steps = append(steps,
			stackop.PUSHREF(message).Serialize(),
			stackop.PUSHINT(big.NewInt(0)).Serialize(),
			funcsop.SENDRAWMSG().Serialize(),
		)
	}
	for _, step := range steps {
		if err := code.StoreBuilder(step); err != nil {
			tb.Fatal(err)
		}
	}
	return code.EndCell()
}

// benchMintPredecessorBlock collates one empty block over the seeded state and
// returns the request rebased onto it, so Previous carries a block root bound to
// its state. The seeded content survives because that collation is given nothing
// to do with it.
func benchMintPredecessorBlock(tb testing.TB, req ShardRequest) ShardRequest {
	tb.Helper()

	idle := req
	idle.Neighbors = nil
	idle.NeighborShardEndLT = nil
	idle.Externals = nil
	idle.Internals = nil
	candidate, err := testBuilder().BuildShard(context.Background(), idle)
	if err != nil {
		tb.Fatalf("mint predecessor block: %v", err)
	}
	if candidate.Stats.Transactions != 0 || candidate.Stats.OutQueueSize != *req.Previous.OutQueueSize {
		tb.Fatalf("predecessor collation was not idle: %d transactions, queue %d of %d",
			candidate.Stats.Transactions, candidate.Stats.OutQueueSize, *req.Previous.OutQueueSize)
	}

	blockRoot, err := cell.FromBOC(candidate.BlockBOC)
	if err != nil {
		tb.Fatal(err)
	}
	queueSize := candidate.Stats.OutQueueSize
	req.Previous = PreviousBlock{
		ID:           candidate.ID,
		Block:        blockRoot,
		State:        candidate.State,
		OutQueueSize: &queueSize,
	}
	req.Header.GenUtime++
	req.Header.GenUtimeMS = uint64(req.Header.GenUtime) * 1_000
	// The group snapshot registers the shard's head block, which is now the one
	// just minted; the capability checks the neighbour set against it.
	for i := range req.Masterchain.Groups.Active {
		session := &req.Masterchain.Groups.Active[i]
		for j := range session.Registered {
			if session.Registered[j].Shard == req.Shard {
				session.Registered[j].Block = *candidate.ID.Copy()
			}
		}
	}
	return req
}

// benchAddress derives a well-spread basechain address so the account
// dictionary branches the way a real one does.
func benchAddress(role string, index int) *address.Address {
	digest := sha256.Sum256(fmt.Appendf(nil, "gton-collator-bench/%s/%d", role, index))
	return address.NewAddress(0, 0, digest[:])
}

func benchSetAccount(
	tb testing.TB,
	accounts *tlb.ShardAccountsAugDict,
	contract activeContract,
	lastPaid uint32,
) {
	tb.Helper()

	key := cell.BeginCell().MustStoreSlice(contract.address.Data(), 256).EndCell()
	if err := accounts.Set(key, activeShardAccount(tb, contract, lastPaid)); err != nil {
		tb.Fatal(err)
	}
}

type benchQueueEntry struct {
	key   msgpool.QueueKey
	value *cell.Cell
}

// benchDispatchQueue builds a predecessor DispatchQueue holding one deferred
// message per account, the shape the mandatory first dispatch pass drains.
func benchDispatchQueue(tb testing.TB, accounts int, createdAt uint32, lt uint64) *tlb.DispatchQueueAugDict {
	tb.Helper()

	if accounts == 0 {
		return nil
	}
	dictionary, err := cell.NewAugDict(256, tlb.AugDispatchQueue{})
	if err != nil {
		tb.Fatal(err)
	}
	for i := range accounts {
		source := benchAddress("deferred-source", i)
		message, err := tlb.ToCell(&tlb.InternalMessage{
			IHRDisabled: true,
			SrcAddr:     source,
			DstAddr:     benchAddress("deferred-target", i),
			Amount:      tlb.FromNanoTONU(100_000_000),
			FwdFee:      tlb.FromNanoTONU(1_000),
			CreatedLT:   lt,
			CreatedAt:   createdAt,
			Body:        cell.BeginCell().EndCell(),
		})
		if err != nil {
			tb.Fatal(err)
		}
		envelope, err := (tlb.MsgEnvelope{
			CurAddr:         tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular},
			NextAddr:        tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular},
			FwdFeeRemaining: tlb.FromNanoTONU(1_000),
			Msg:             message,
		}).ToCell()
		if err != nil {
			tb.Fatal(err)
		}
		enqueued, err := (tlb.EnqueuedMsg{EnqueuedLT: lt, Msg: envelope}).ToCell()
		if err != nil {
			tb.Fatal(err)
		}
		messages := cell.NewDict(64)
		if err = messages.Set(cell.BeginCell().MustStoreUInt(lt, 64).EndCell(), enqueued); err != nil {
			tb.Fatal(err)
		}
		accountQueue, err := (tlb.AccountDispatchQueue{Messages: messages, Count: 1}).ToCell()
		if err != nil {
			tb.Fatal(err)
		}
		key := cell.BeginCell().MustStoreSlice(source.Data(), 256).EndCell()
		if err = dictionary.Set(key, accountQueue); err != nil {
			tb.Fatal(err)
		}
	}
	return &tlb.DispatchQueueAugDict{AugmentedDictionary: dictionary}
}

// benchQueueInfo builds an empty outbound queue carrying one processed
// frontier, the shape a neighbor's authenticated queue field has.
func benchQueueInfo(tb testing.TB, frontier []tlb.ProcessedUptoRecord) *cell.Cell {
	tb.Helper()

	outQueue, err := tlb.NewOutMsgQueueAugDict()
	if err != nil {
		tb.Fatal(err)
	}
	procInfo, err := tlb.ProcessedUptoDict(frontier)
	if err != nil {
		tb.Fatal(err)
	}
	root, err := (tlb.OutMsgQueueInfo{OutQueue: outQueue, ProcInfo: procInfo}).ToCell()
	if err != nil {
		tb.Fatal(err)
	}
	return root
}

// benchPreviousState installs the accounts and the whole outbound queue in one
// pass. The per-message helpers reparse and reserialize the state for every
// insert, which is quadratic at these sizes.
func benchPreviousState(
	tb testing.TB,
	root *cell.Cell,
	accounts *tlb.ShardAccountsAugDict,
	entries []benchQueueEntry,
	dispatch *tlb.DispatchQueueAugDict,
) *cell.Cell {
	tb.Helper()

	var state tlb.ShardStateUnsplit
	if err := parseExact(&state, root); err != nil {
		tb.Fatal(err)
	}
	var queue tlb.OutMsgQueueInfo
	if err := parseExact(&queue, state.OutMsgQueueInfo); err != nil {
		tb.Fatal(err)
	}
	for _, entry := range entries {
		key := cell.BeginCell().MustStoreSlice(entry.key[:], 352).EndCell()
		inserted, err := queue.OutQueue.SetWithMode(key, entry.value, cell.DictSetModeAdd)
		if err != nil {
			tb.Fatal(err)
		}
		if !inserted {
			tb.Fatalf("duplicate outbound queue key %x", entry.key)
		}
	}
	if dispatch != nil {
		queue.Extra = &tlb.OutMsgQueueExtra{DispatchQueue: dispatch}
	}
	queueInfo, err := queue.ToCell()
	if err != nil {
		tb.Fatal(err)
	}
	state.OutMsgQueueInfo = queueInfo

	var stats tlb.ShardStateStats
	if err = parseExact(&stats, state.Stats); err != nil {
		tb.Fatal(err)
	}
	balance, err := loadAccountsBalance(accounts)
	if err != nil {
		tb.Fatal(err)
	}
	stats.TotalBalance = balance
	if state.Stats, err = stats.ToCell(); err != nil {
		tb.Fatal(err)
	}
	state.Accounts.ShardAccounts = accounts

	next, err := tlb.ToCell(&state)
	if err != nil {
		tb.Fatal(err)
	}
	return next
}
