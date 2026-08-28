package collator

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

// Real mainnet traffic as a collation workload. The synthetic profiles in
// bench_workload_test.go run one hand-written contract that burns gas in a
// loop: a stable yardstick, but not the opcode mix a validator actually meets.
// This fixture is a real basechain block — the accounts are the real contracts
// with the code and data they held in the block before, and the work offered to
// the collator is the messages those transactions consumed.
//
// It is not a replay. Our collator assigns its own logical times and its own
// order, so the block it builds is not the mainnet one, and it is not meant to
// be: what carries over is which code runs and against what data, which is
// exactly the part a synthetic contract cannot imitate. Both implementations
// read the same exported fixture, so the comparison stands regardless.
const (
	benchMainnetFixturePath = "testdata/tvm_replay_fat_block_66519406.json"
	// benchMainnetProfileName selects this workload in the fixture export,
	// where the synthetic profiles are chosen by name.
	benchMainnetProfileName = "mainnet"
	// benchMainnetHeavyProfileName is the same traffic offered several times
	// over, to export a fixture the size of a full block.
	benchMainnetHeavyProfileName = "mainnet-heavy"
)

type benchMainnetFixture struct {
	Transactions     int    `json:"transactions"`
	BlockBOCBase64   string `json:"block_boc_base64"`
	PreviousAccounts []struct {
		Account               string `json:"account"`
		ShardAccountBOCBase64 string `json:"shard_account_boc_base64"`
	} `json:"previous_accounts"`
}

// benchMainnetShape reports what the fixture actually offered, because the
// interesting failure mode here is silent: a workload that looks real but whose
// messages all bounce off frozen or expired accounts measures nothing.
type benchMainnetShape struct {
	accounts  int
	created   int
	externals int
	internals int
	now       uint32
}

// runPackageTests runs the package and, when the run was asked to repeat
// itself, says out loud what the repeat does not repeat.
//
// The fixtures below — the mainnet JSON, the account dictionaries, the
// assembled workloads, and the synthetic ones in bench_workload_test.go — are
// memoized per process, and -count repeats the test list inside one process.
// Go offers no per-iteration hook to reset them on (m.Run is entered once), and
// making them per-test would give the race suite its ~420 s back, so the
// limitation is reported instead of removed: a second iteration re-runs every
// test but reuses the fixtures the first one built, which is a weaker repeat
// than -count usually implies. Repeating the fixture construction as well means
// separate processes: `go test -count=1` run twice, or -shuffle to reorder
// within one.
func runPackageTests(m *testing.M) int {
	code := m.Run()
	if count := flag.Lookup("test.count"); count != nil && count.Value.String() != "1" {
		fmt.Fprintf(
			os.Stderr,
			"NOTE: -count=%s repeated the tests in one process; the collation fixtures"+
				" (bench_mainnet_test.go, bench_workload_test.go) are memoized per process"+
				" and were built once. Run separate `go test -count=1` processes to repeat those too.\n",
			count.Value.String(),
		)
	}

	return code
}

// The fixture file is 2.7 MB of JSON and every reader of it only reads: the
// accounts are decoded into a dictionary, never edited in place. Re-reading and
// re-unmarshalling it per call cost a quarter of a second each time, on a path
// several assembly variants share.
//
// Memoization is per process and survives -count; runPackageTests above says so
// on any run that asked for more than one iteration.
var (
	benchMainnetFixtureOnce  sync.Once
	benchMainnetFixtureValue benchMainnetFixture
	benchMainnetFixtureErr   error
)

func loadBenchMainnetFixture(tb testing.TB) benchMainnetFixture {
	tb.Helper()

	benchMainnetFixtureOnce.Do(func() {
		raw, err := os.ReadFile(benchMainnetFixturePath)
		if err != nil {
			benchMainnetFixtureErr = fmt.Errorf("read mainnet fixture: %w", err)

			return
		}
		if err = json.Unmarshal(raw, &benchMainnetFixtureValue); err != nil {
			benchMainnetFixtureErr = fmt.Errorf("decode mainnet fixture: %w", err)
		}
	})
	if benchMainnetFixtureErr != nil {
		tb.Fatal(benchMainnetFixtureErr)
	}

	return benchMainnetFixtureValue
}

// benchShardAccountExists reports whether a ShardAccount holds an account at
// all: the Account it references is a one-bit union, account_none$0 or
// account$1.
func benchShardAccountExists(tb testing.TB, root *cell.Cell) bool {
	tb.Helper()

	var shardAccount tlb.ShardAccount
	if err := parseExact(&shardAccount, root); err != nil {
		tb.Fatalf("parse shard account: %v", err)
	}
	slice, err := shardAccount.Account.BeginParse()
	if err != nil {
		tb.Fatalf("open account: %v", err)
	}
	exists, err := slice.LoadBoolBit()
	if err != nil {
		tb.Fatalf("read account tag: %v", err)
	}
	return exists
}

func benchMainnetBlock(tb testing.TB) *tlb.Block {
	tb.Helper()

	blockBOC, err := base64.StdEncoding.DecodeString(loadBenchMainnetFixture(tb).BlockBOCBase64)
	if err != nil {
		tb.Fatalf("decode block boc: %v", err)
	}
	root, err := cell.FromBOC(blockBOC)
	if err != nil {
		tb.Fatalf("parse block boc: %v", err)
	}
	var block tlb.Block
	if err = parseExact(&block, root); err != nil {
		tb.Fatalf("parse block: %v", err)
	}
	return &block
}

type benchMainnetAccountsKey struct {
	filler   int
	genUtime uint32
}

type benchMainnetAccountSet struct {
	accounts    *tlb.ShardAccountsAugDict
	nonexistent int
}

var (
	benchMainnetAccountsMu    sync.Mutex
	benchMainnetAccountsCache = map[benchMainnetAccountsKey]benchMainnetAccountSet{}
)

// benchMainnetAccounts is the predecessor account dictionary: the fixture's real
// mainnet contracts, plus filler idle contracts to give the dictionary the depth
// a real shard has.
//
// It is memoized because it is by far the most expensive thing any of these
// assemblies does — a profile of the race suite put benchSetAccount at 90% of
// all Go-visible work in the heavy tests — and because every variant that asks
// for the same filler count gets a byte-identical dictionary: the real accounts
// come from one fixture, and the filler is generated from benchAddress,
// externalAcceptCode and a fixed balance at the request's GenUtime, which
// benchRebaseTime pins to the fixture block's own second for every variant. The
// generation time is part of the key rather than assumed, so a variant that ever
// did move it would build its own.
//
// Handing the same dictionary to several requests is safe because nothing writes
// to it: benchPreviousState reads the balance out of it and serializes it into a
// state cell, and collation works from that cell, never from this object.
func benchMainnetAccounts(tb testing.TB, filler int, genUtime uint32) (*tlb.ShardAccountsAugDict, int) {
	tb.Helper()

	key := benchMainnetAccountsKey{filler: filler, genUtime: genUtime}
	benchMainnetAccountsMu.Lock()
	defer benchMainnetAccountsMu.Unlock()
	if cached, ok := benchMainnetAccountsCache[key]; ok {
		return cached.accounts, cached.nonexistent
	}

	fixture := loadBenchMainnetFixture(tb)
	accounts, err := tlb.NewShardAccountsAugDict()
	if err != nil {
		tb.Fatal(err)
	}
	var nonexistent int
	for _, entry := range fixture.PreviousAccounts {
		workchain, rest, found := strings.Cut(entry.Account, ":")
		if !found || workchain != "0" {
			tb.Fatalf("account %q is not a basechain address", entry.Account)
		}
		addr, err := hex.DecodeString(rest)
		if err != nil || len(addr) != 32 {
			tb.Fatalf("account %q is not a 256-bit address: %v", entry.Account, err)
		}
		accountBOC, err := base64.StdEncoding.DecodeString(entry.ShardAccountBOCBase64)
		if err != nil {
			tb.Fatalf("decode shard account %s: %v", entry.Account, err)
		}
		accountRoot, err := cell.FromBOC(accountBOC)
		if err != nil {
			tb.Fatalf("parse shard account %s: %v", entry.Account, err)
		}
		// Some of these accounts do not exist yet — the block is what creates
		// them, so their prior state is account_none. A non-existent account has
		// no key in ShardAccounts at all, and leaving one there is not a
		// harmless extra: the reference collator adds a created account with
		// Add mode and fails outright on the key already being present.
		if !benchShardAccountExists(tb, accountRoot) {
			nonexistent++
			continue
		}
		key := cell.BeginCell().MustStoreSlice(addr, 256).EndCell()
		if err = accounts.Set(key, accountRoot); err != nil {
			tb.Fatalf("insert shard account %s: %v", entry.Account, err)
		}
	}
	idleCode := externalAcceptCode(tb)
	for i := range filler {
		benchSetAccount(tb, accounts, activeContract{
			address: benchAddress("mainnet-idle", i),
			code:    idleCode,
			balance: 1_000_000_000,
		}, genUtime)
	}

	benchMainnetAccountsCache[key] = benchMainnetAccountSet{accounts: accounts, nonexistent: nonexistent}

	return accounts, nonexistent
}

// benchMainnetVariant names one assembled mainnet workload. Everything that
// changes what gets built belongs here; everything a caller adjusts afterwards
// — the block-size limits, the full-collated capability, a substituted
// predecessor state — belongs to the caller's own copy and must not be a key,
// or the cache degenerates back into one construction per test.
type benchMainnetVariant struct {
	filler int
	repeat int
	// fixture selects the export-grade assembly: a real masterchain block,
	// masterchain state and group snapshot, on top of a minted predecessor.
	fixture bool
	// mint asks benchMainnetRequestFrom for a real predecessor block.
	mint bool
}

type benchMainnetWorkload struct {
	req     ShardRequest
	shape   benchMainnetShape
	mcState *cell.Cell
	mcBlock *cell.Cell
}

var (
	benchMainnetMu    sync.Mutex
	benchMainnetCache = map[benchMainnetVariant]*benchMainnetWorkload{}
)

// benchMainnetWorkloadFor assembles one mainnet workload per process, for the
// same reason benchWorkloadFor does it for the synthetic profiles: filling a
// 100,000-account dictionary costs far more than the block that walks it. Some
// twenty test call sites were each building their own, and in the tests where
// this dominates the collation they exist to check is under two per cent of the
// runtime.
//
// Neither BuildShard nor VerifyShardCandidate mutates its request —
// TestBuildShardFinishIsByteIdenticalAcrossRuns builds nine candidates from one
// request and asserts the block, the collated data and the root hash are
// byte-identical, and the mainnet benchmarks have always reused a single
// request across b.N — so sharing the assembled inputs is safe, including
// against the rule that a reused parse must not move produced bytes.
//
// What is not safe is sharing the pointers a caller writes through, which is
// what benchMainnetHandout exists for.
func benchMainnetWorkloadFor(tb testing.TB, variant benchMainnetVariant) *benchMainnetWorkload {
	tb.Helper()

	benchMainnetMu.Lock()
	defer benchMainnetMu.Unlock()
	if cached, ok := benchMainnetCache[variant]; ok {
		return cached
	}
	built := buildBenchMainnetWorkload(tb, variant)
	benchMainnetCache[variant] = built

	return built
}

func buildBenchMainnetWorkload(tb testing.TB, variant benchMainnetVariant) *benchMainnetWorkload {
	tb.Helper()

	if variant.fixture {
		req, mcState, mcBlock := buildBenchMainnetFixtureRequest(tb, variant.repeat)

		return &benchMainnetWorkload{req: req, mcState: mcState, mcBlock: mcBlock}
	}
	base := emptyCandidateRequest(tb)
	if variant.mint {
		now, floorLT := benchMainnetTimeBase(tb)
		base = benchRebaseTime(tb, base, now, floorLT)
	}
	req, shape := benchMainnetRequestFrom(tb, variant.filler, variant.repeat, variant.mint, base)

	return &benchMainnetWorkload{req: req, shape: shape}
}

// benchMainnetHandout returns a caller's own view of a shared workload.
//
// ShardRequest is a value, so every field a test assigns — Previous.State,
// Externals, Internals, Header, Neighbors — already lands on the copy. The
// exception is Masterchain.Config, which is a pointer that nine call sites
// write through (block and collated size limits, the basechain block limits,
// and the full-collated capability in both directions). Every one of them
// writes a value field on the config or on the chainConfig value embedded in
// it, so one shallow copy isolates them all; the map and pointer fields behind
// it — workchains, execution, footprint, specials, fees — are read-only in this
// package's tests. Groups is copied for the same reason: benchRebaseTime writes
// its GenUTime through the pointer.
func benchMainnetHandout(workload *benchMainnetWorkload) ShardRequest {
	req := workload.req
	if req.Masterchain.Config != nil {
		config := *req.Masterchain.Config
		req.Masterchain.Config = &config
	}
	if req.Masterchain.Groups != nil {
		groupsSnapshot := *req.Masterchain.Groups
		req.Masterchain.Groups = &groupsSnapshot
	}
	req.Neighbors = slices.Clone(req.Neighbors)

	return req
}

// benchMainnetRequest builds a shard request whose accounts and inbound
// messages come from the fixture. filler pads the account dictionary with idle
// contracts: the block touches 234 accounts, and a dictionary that small would
// under-report the lookup and rebuild costs that dominate a real shard.
func benchMainnetRequest(tb testing.TB, filler int) (ShardRequest, benchMainnetShape) {
	workload := benchMainnetWorkloadFor(tb, benchMainnetVariant{filler: filler, repeat: 1})

	return benchMainnetHandout(workload), workload.shape
}

// benchMainnetRequestRepeated offers the block's internal traffic several times
// over, to reach the transaction count of a full block without inventing
// synthetic work: the contracts, their code and data, and the message bodies
// stay the ones mainnet produced. Each copy is re-emitted at its own logical
// time, so it is a distinct message with a distinct hash, and the receiving
// contract runs the same code path again — which is what a busy jetton or DEX
// account does in a real block anyway.
//
// Externals are deliberately not repeated: a wallet checks its seqno, so the
// second copy of one would be rejected rather than executed, and a benchmark
// full of rejections measures nothing.
func benchMainnetRequestRepeated(tb testing.TB, filler, repeat int) (ShardRequest, benchMainnetShape) {
	workload := benchMainnetWorkloadFor(tb, benchMainnetVariant{filler: filler, repeat: repeat})

	return benchMainnetHandout(workload), workload.shape
}

// benchMainnetTimeBase reports the wall clock and the logical time floor the
// fixture forces. The export needs them before it builds anything, because the
// masterchain block it hands the reference collator carries a generation time
// of its own, and a masterchain reference from after the block it is referenced
// by is not a fixture, it is a contradiction.
func benchMainnetTimeBase(tb testing.TB) (uint32, uint64) {
	tb.Helper()

	block := benchMainnetBlock(tb)
	transactions, err := block.ListTransactions()
	if err != nil {
		tb.Fatalf("list transactions: %v", err)
	}
	var floorLT uint64
	for _, transaction := range transactions {
		floorLT = max(floorLT, transaction.LT)
	}
	return block.BlockInfo.GenUtime, floorLT
}

// benchMainnetFixtureRequest is benchFixtureRequest for this workload: the same
// masterchain state, predecessor block and group snapshot the reference collator
// needs, over real traffic instead of a synthetic profile.
func benchMainnetFixtureRequest(tb testing.TB, repeat int) (ShardRequest, *cell.Cell, *cell.Cell) {
	tb.Helper()

	workload := benchMainnetWorkloadFor(tb, benchMainnetVariant{
		filler:  benchMainnetFiller,
		repeat:  repeat,
		fixture: true,
		mint:    true,
	})

	return benchMainnetHandout(workload), workload.mcState, workload.mcBlock
}

func buildBenchMainnetFixtureRequest(tb testing.TB, repeat int) (ShardRequest, *cell.Cell, *cell.Cell) {
	tb.Helper()

	now, floorLT := benchMainnetTimeBase(tb)
	base := benchRebaseTime(tb, emptyCandidateRequest(tb), now, floorLT)
	mcBlock := benchMasterchainBlock(tb, base)
	mcHash := mcBlock.HashKey()
	base.Masterchain.ID.RootHash = mcHash[:]
	base.Masterchain.Groups.MasterchainBlock = base.Masterchain.ID

	req, _ := benchMainnetRequestFrom(tb, benchMainnetFiller, repeat, true, base)
	shardState := loadPreviousShardState(tb, req)
	mcState := benchMasterchainState(tb, req, &shardState)

	snapshot := benchMasterSnapshot(tb, req.Masterchain.ID, mcState, req.Masterchain.GenUtime)
	if !snapshot.Ready {
		tb.Fatal("masterchain fixture state did not yield a ready group snapshot")
	}
	req.Masterchain.Groups = snapshot
	return req, mcState, mcBlock
}

// benchMainnetNeighbors gives the request the neighbour shape the synthetic
// profiles use: the shard is its own neighbour holding the queue those messages
// sit in, and the masterchain frontier is published through the queue a
// validator authenticates. Without it the queue is there but nothing imports
// from it, and the benchmark quietly collates externals only.
func benchMainnetNeighbors(tb testing.TB, req ShardRequest, startLT uint64) ShardRequest {
	tb.Helper()

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
			Block:           req.Previous.ID,
			Shard:           msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll},
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
	return req
}

// mint asks for a real predecessor block, which the reference collator needs
// but our own benchmarks do not.
func benchMainnetRequestFrom(
	tb testing.TB,
	filler, repeat int,
	mint bool,
	req ShardRequest,
) (ShardRequest, benchMainnetShape) {
	tb.Helper()

	fixture := loadBenchMainnetFixture(tb)
	block := benchMainnetBlock(tb)
	transactions, err := block.ListTransactions()
	if err != nil {
		tb.Fatalf("list transactions: %v", err)
	}

	// The contracts were compiled against the wall clock of their own block.
	// Wallets reject an external whose valid_until has passed, and storage fees
	// are charged for every second since last_paid, so running this traffic on
	// the synthetic 2030 time base would freeze accounts and bounce every
	// external. The request is rebased onto the block's own second instead.
	//
	// Logical time has to move too, and upward: the real accounts carry the
	// mainnet logical times of their last transaction, which sit far above
	// anything this fixture reaches, and a collator refuses an account that
	// transacted after the block it is building starts. Raising the
	// predecessor's bound is one edit; rewriting the logical time inside every
	// account would be surgery on the very state that makes this workload real.
	var floorLT uint64
	for _, transaction := range transactions {
		floorLT = max(floorLT, transaction.LT)
	}
	req = benchRebaseTime(tb, req, block.BlockInfo.GenUtime, floorLT)

	accounts, nonexistent := benchMainnetAccounts(tb, filler, req.Header.GenUtime)

	// Inbound work is the message each transaction consumed. Externals go in as
	// externals; internals are re-emitted onto this request's logical time base
	// — mainnet logical times sit far above anything this fixture can reach —
	// keeping the real sender, destination, value and body, which is what the
	// receiving contract actually runs on.
	startLT := requestStartLT(tb, req)
	var (
		externals []ExternalInput
		entries   []benchQueueEntry
		internals []*cell.Cell
	)
	for _, transaction := range transactions {
		if transaction.IO.In == nil {
			continue
		}
		switch inbound := transaction.IO.In.Msg.(type) {
		case *tlb.ExternalMessage:
			root, err := tlb.ToCell(inbound)
			if err != nil {
				tb.Fatalf("serialize external for %x: %v", transaction.AccountAddr, err)
			}
			externals = append(externals, externalInput(tb, root))
		case *tlb.InternalMessage:
			internals = append(internals, mustInternalCell(tb, inbound))
		}
	}
	// The queue entries belong to the predecessor block, so their logical times
	// have to sit at or below its end — the shard cannot import from itself
	// messages it has not sent yet.
	if repeat > 1 {
		once := internals
		internals = make([]*cell.Cell, 0, len(once)*repeat)
		for range repeat {
			internals = append(internals, once...)
		}
	}

	previousLT := loadPreviousShardState(tb, req).GenLT
	pooled := make([]*msgpool.InternalMessage, len(internals))
	for i, message := range internals {
		lt := previousLT - uint64(len(internals)) + uint64(i)
		var entry benchQueueEntry
		pooled[i], entry = benchMainnetQueued(tb, message, lt, req.Header.GenUtime-1)
		entries = append(entries, entry)
	}
	req.Internals = &msgpool.Cut{Messages: pooled}

	req.Externals = externals
	req.MaxExternalAttempts = max(len(externals), 1)
	req.Previous.State = benchPreviousState(tb, req.Previous.State, accounts, entries, nil)
	queueSize := uint64(len(entries))
	req.Previous.OutQueueSize = &queueSize

	if mint {
		req = benchMintPredecessorBlock(tb, req)
		startLT = requestStartLT(tb, req)
	}
	req = benchMainnetNeighbors(tb, req, startLT)

	return req, benchMainnetShape{
		accounts:  len(fixture.PreviousAccounts) - nonexistent + filler,
		created:   nonexistent,
		externals: len(externals),
		internals: len(internals),
		now:       block.BlockInfo.GenUtime,
	}
}

func mustInternalCell(tb testing.TB, message *tlb.InternalMessage) *cell.Cell {
	tb.Helper()

	root, err := tlb.ToCell(message)
	if err != nil {
		tb.Fatalf("serialize internal message: %v", err)
	}
	return root
}

// benchMainnetQueued re-emits a real internal message on this request's logical
// time base, returning both halves it is needed in: the pool entry the collator
// is offered, and the queue entry in the predecessor state that backs it.
func benchMainnetQueued(
	tb testing.TB,
	root *cell.Cell,
	createdLT uint64,
	createdAt uint32,
) (*msgpool.InternalMessage, benchQueueEntry) {
	tb.Helper()

	var message tlb.InternalMessage
	if err := parseExact(&message, root); err != nil {
		tb.Fatalf("parse internal message: %v", err)
	}
	message.CreatedLT = createdLT
	message.CreatedAt = createdAt
	rebased, err := tlb.ToCell(&message)
	if err != nil {
		tb.Fatalf("re-emit internal message: %v", err)
	}

	envelope, err := (tlb.MsgEnvelope{
		CurAddr:         tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular},
		NextAddr:        tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular, UseDestBits: 96},
		FwdFeeRemaining: message.FwdFee,
		Msg:             rebased,
	}).ToCell()
	if err != nil {
		tb.Fatal(err)
	}
	srcPrefix, err := accountPrefixFromAddress(message.SrcAddr)
	if err != nil {
		tb.Fatal(err)
	}
	dstPrefix, err := accountPrefixFromAddress(message.DstAddr)
	if err != nil {
		tb.Fatal(err)
	}
	enqueued, err := (tlb.EnqueuedMsg{EnqueuedLT: createdLT, Msg: envelope}).ToCell()
	if err != nil {
		tb.Fatal(err)
	}
	var parsedEnvelope tlb.MsgEnvelope
	if err = parseExact(&parsedEnvelope, envelope); err != nil {
		tb.Fatal(err)
	}
	key := msgpool.MakeQueueKey(msgpool.InterpolatePrefix(srcPrefix, dstPrefix, 96), rebased.HashKey())
	return &msgpool.InternalMessage{
			Key:          key,
			EnqueuedLT:   createdLT,
			QueueLT:      createdLT,
			EnvHash:      envelope.HashKey(),
			Envelope:     parsedEnvelope,
			EnvelopeCell: envelope,
			Root:         rebased,
			Source:       msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll},
			SourceSeqno:  1,
		}, benchQueueEntry{
			key:   key,
			value: enqueued,
		}
}

// benchRebaseTime moves a request onto another wall-clock second and lifts its
// logical time to at least floorLT. Every time in the request hangs off the
// previous state's, so the state is rewritten first and the rest follows it.
func benchRebaseTime(tb testing.TB, req ShardRequest, now uint32, floorLT uint64) ShardRequest {
	tb.Helper()

	var state tlb.ShardStateUnsplit
	if err := parseExact(&state, req.Previous.State); err != nil {
		tb.Fatal(err)
	}
	state.GenUTime = now - 1
	state.GenLT = max(state.GenLT, floorLT+1)
	root, err := tlb.ToCell(&state)
	if err != nil {
		tb.Fatal(err)
	}
	req.Previous.State = root
	req.Masterchain.GenUtime = state.GenUTime
	if req.Masterchain.Groups != nil {
		req.Masterchain.Groups.GenUTime = state.GenUTime
	}
	req.Header.GenUtime = now
	req.Header.GenUtimeMS = uint64(now) * 1_000
	return req
}

func BenchmarkCollateMainnet(b *testing.B) {
	req, shape := benchMainnetRequest(b, benchMainnetFiller)
	builder := testBuilder()
	ctx := context.Background()
	candidate, err := builder.BuildShard(ctx, req)
	if err != nil {
		b.Fatal(err)
	}
	assertBenchMainnetRan(b, candidate, shape)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := builder.BuildShard(ctx, req); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	reportMainnetShape(b, candidate, shape)
}

// benchMainnetHeavyRepeat brings the workload to roughly a thousand
// transactions. Three is where it stops being honest to go further: at four the
// gas per transaction halves, because the repeated deliveries start draining the
// receiving accounts and later copies bounce instead of running — a cheaper
// transaction that measures the wrong thing.
const benchMainnetHeavyRepeat = 3

// benchMainnetExportRepeat lets the fixture export pick a different multiplier
// than the benchmarks use. The reference collator and this one only do the same
// work while the block limits are slack; past that they admit different amounts,
// and a speed comparison across different work is not a comparison.
func benchMainnetExportRepeat() int {
	if raw := os.Getenv("GTON_BENCH_MAINNET_REPEAT"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			return n
		}
	}
	return benchMainnetHeavyRepeat
}

func BenchmarkCollateMainnetHeavy(b *testing.B) {
	req, shape := benchMainnetRequestRepeated(b, benchMainnetFiller, benchMainnetHeavyRepeat)
	builder := testBuilder()
	ctx := context.Background()
	candidate, err := builder.BuildShard(ctx, req)
	if err != nil {
		b.Fatal(err)
	}
	assertBenchMainnetRan(b, candidate, shape)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := builder.BuildShard(ctx, req); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	reportMainnetShape(b, candidate, shape)
}

func BenchmarkCollateMainnetHeavyPhases(b *testing.B) {
	req, shape := benchMainnetRequestRepeated(b, benchMainnetFiller, benchMainnetHeavyRepeat)
	builder := testBuilder()
	timer := newPhaseTimer()
	candidate, err := builder.BuildShard(context.Background(), req)
	if err != nil {
		b.Fatal(err)
	}
	assertBenchMainnetRan(b, candidate, shape)

	b.ResetTimer()
	for b.Loop() {
		collateWithPhases(b, builder, req, timer)
	}
	b.StopTimer()
	timer.report(b, collationPhases)
	reportMainnetShape(b, candidate, shape)
}

func BenchmarkVerifyMainnetHeavy(b *testing.B) {
	req, shape := benchMainnetRequestRepeated(b, benchMainnetFiller, benchMainnetHeavyRepeat)
	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		b.Fatal(err)
	}
	assertBenchMainnetRan(b, candidate, shape)

	verification := shardVerificationRequest(req, candidate)
	verification.NeighborShardEndLT = req.NeighborShardEndLT
	verification.Semantics = NewSemanticVerifier(tvm.NewTVM())
	if err = VerifyShardCandidate(context.Background(), verification); err != nil {
		b.Fatalf("verify heavy mainnet candidate: %v", err)
	}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := VerifyShardCandidate(ctx, verification); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	reportMainnetShape(b, candidate, shape)
}

// The heavy fixture benchmarks collate and verify exactly what the export
// writes for the reference node — a minted predecessor and all — so the two
// implementations are timed on the same bytes rather than on nearly the same
// workload.
func BenchmarkCollateMainnetHeavyFixture(b *testing.B) {
	req, _, _ := benchMainnetFixtureRequest(b, benchMainnetExportRepeat())
	builder := testBuilder()
	ctx := context.Background()
	candidate, err := builder.BuildShard(ctx, req)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := builder.BuildShard(ctx, req); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(candidate.Stats.Transactions), "tx/block")
	b.ReportMetric(float64(candidate.Stats.GasUsed), "gas/block")
	b.ReportMetric(float64(len(candidate.BlockBOC)), "blockB")
}

// benchMainnetPredecessorBOC serializes a request's predecessor state once, so
// a store-shaped copy of it can be rebuilt as often as a caller needs one.
func benchMainnetPredecessorBOC(tb testing.TB, req ShardRequest) []byte {
	tb.Helper()

	boc, err := req.Previous.State.ToBOCWithOptionsErr(cell.BOCSerializeOptions{
		WithIndex:     true,
		WithCacheBits: true,
	})
	if err != nil {
		tb.Fatal(err)
	}

	return boc
}

// benchMainnetLazyPredecessor parses a fresh store-shaped predecessor: the root
// resident, everything below it an unresolved stub. Each call returns a new
// tree, which is the whole point — a lazy tree materializes as it is read, so
// one shared between iterations is a different input on the second one.
func benchMainnetLazyPredecessor(tb testing.TB, predecessorBOC []byte) *cell.Cell {
	tb.Helper()

	state, err := cell.FromBOCWithOptions(predecessorBOC, cell.BOCParseOptions{Lazy: true})
	if err != nil {
		tb.Fatal(err)
	}

	return state
}

// The lazy fixture parses its predecessor per iteration rather than once, and
// that is not tidiness. A store-shaped tree materializes the cells it is read
// through, so a predecessor shared across iterations is fully lazy for the first
// collation and progressively resident afterwards — and the pruning predicate
// that decides childless boundaries reads exactly that. Reusing one made
// iteration 0 produce a 688,134-byte block and every later iteration a
// 688,052-byte one: two different blocks timed as if they were one, and any
// identity gate built on this row would flap between them.
//
// The parse is excluded from the timer rather than merged into the measurement:
// it is fixture assembly, not collation. It is not a rounding error — on the
// measurement box (i7-6700, go1.26.2, 8 cores) it costs 83.0 ms and 39.1 MB
// against the 83.4 ms of the collation it precedes, so this row now takes about
// twice as long to run. What it does not do is move the number: measured against
// the shared-predecessor version, ns/op went from 85.9 ms to 83.4 ms (-2.9%) and
// B/op from 37.0 MB to 34.5 MB, with allocs/op up 1,363 (+0.46%) — the reported
// figure moved down, not up. BenchmarkMainnetLazyPredecessorParse below is the
// row that keeps that exclusion honest; it is a measurement, not a gate.
//
// WHAT THE EXCLUSION CANNOT EXCLUDE, stated because a reader comparing this row
// to its pre-fix self deserves it: StopTimer removes the parse's wall time, and
// because testing accumulates the memstats deltas across the timer windows it
// removes the parse's ~39 MB from B/op as well. It does not remove the GC WORK
// that garbage causes. A collection triggered by allocations made outside the
// timer can fire inside it and is charged to ns/op there, so this row now runs
// under roughly twice the garbage per iteration that the shared-predecessor
// version did — ~39 MB out of timer against 34.5 MB in it. The -2.9% above says
// that did not show up as a penalty on this fixture and this box, which is a
// measurement and not a guarantee. Two consequences: this row is comparable to
// the pre-fix number only approximately, and it must not be compared against the
// other collation rows in this file, none of which rebuild their predecessor.
func BenchmarkCollateMainnetHeavyLazyFixture(b *testing.B) {
	req, _, _ := benchMainnetFixtureRequest(b, benchMainnetExportRepeat())
	// capFullCollatedData is OFF on this row, explicitly and not by inheritance.
	// The subject here is what the parent's SHAPE costs collation; the proof
	// building the capability adds is a different subject with its own rows, which
	// go through benchMainnetCollatedRequest. Clearing it in the request states
	// the choice, and the assertion below is what keeps it a choice: with the
	// capability off the collated data degenerates to a 33-byte marker, so a
	// "collatedB" metric taken from this row would have reported 33 bytes as if it
	// were a size. It is asserted instead of reported.
	req.Masterchain.Config.capabilities &^= capFullCollatedData
	predecessorBOC := benchMainnetPredecessorBOC(b, req)
	req.Previous.State = benchMainnetLazyPredecessor(b, predecessorBOC)

	builder := testBuilder()
	ctx := context.Background()
	candidate, err := builder.BuildShard(ctx, req)
	if err != nil {
		b.Fatal(err)
	}
	if len(candidate.CollatedData) != benchMainnetCollatedMarkerBytes {
		b.Fatalf("collated data is %d bytes, want the %d-byte marker: this row is meant to run "+
			"WITHOUT capFullCollatedData", len(candidate.CollatedData), benchMainnetCollatedMarkerBytes)
	}
	blockBytes := len(candidate.BlockBOC)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		b.StopTimer()
		req.Previous.State = benchMainnetLazyPredecessor(b, predecessorBOC)
		b.StartTimer()
		built, err := builder.BuildShard(ctx, req)
		if err != nil {
			b.Fatal(err)
		}
		// The drift this fix exists for is visible in one integer, so it is
		// checked on every iteration rather than trusted.
		if len(built.BlockBOC) != blockBytes {
			b.Fatalf("iteration produced a %d-byte block against the %d-byte first one: "+
				"the predecessor is not being rebuilt", len(built.BlockBOC), blockBytes)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(candidate.Stats.Transactions), "tx/block")
	b.ReportMetric(float64(candidate.Stats.GasUsed), "gas/block")
	b.ReportMetric(float64(blockBytes), "blockB")
}

// The per-iteration price of the fixture rebuild the row above excludes.
func BenchmarkMainnetLazyPredecessorParse(b *testing.B) {
	req, _, _ := benchMainnetFixtureRequest(b, benchMainnetExportRepeat())
	predecessorBOC := benchMainnetPredecessorBOC(b, req)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		state := benchMainnetLazyPredecessor(b, predecessorBOC)
		if state == nil {
			b.Fatal("no predecessor")
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(len(predecessorBOC)), "predecessorB")
}

// The store-shaped row: the same workload with its predecessor served through
// cell records and CreateWithLazyRefsUnsafe, which is the shape production
// hands a collation out of celldb. The BOC row above prices the FIXTURE's
// parser (FromBOCWithOptions{Lazy}), whose per-cell materialization cost is
// its own and not production's; comparing this row to the resident one is what
// prices the lazy PARENT SHAPE the way the node actually pays for it. The
// predecessor is rebuilt per iteration for the same reason the BOC row
// rebuilds its parse — a store-shaped tree materializes as it is read, so a
// shared one is lazy only for the first collation — but here the rebuild is
// one root-record decode over a prebuilt record map, cheap enough to stay
// inside the timer instead of paying StopTimer's memstats round-trip.
func BenchmarkCollateMainnetHeavyStoreShaped(b *testing.B) {
	req, _, _ := benchMainnetFixtureRequest(b, benchMainnetExportRepeat())
	// Same subject split as the row above: the parent's shape, not the proof
	// building capFullCollatedData adds on top of it.
	req.Masterchain.Config.capabilities &^= capFullCollatedData
	resident := req.Previous.State
	store := newLazyPredecessorStore(b, resident)
	req.Previous.State = store.root(b, resident)

	builder := testBuilder()
	ctx := context.Background()
	candidate, err := builder.BuildShard(ctx, req)
	if err != nil {
		b.Fatal(err)
	}
	if len(candidate.CollatedData) != benchMainnetCollatedMarkerBytes {
		b.Fatalf("collated data is %d bytes, want the %d-byte marker: this row is meant to run "+
			"WITHOUT capFullCollatedData", len(candidate.CollatedData), benchMainnetCollatedMarkerBytes)
	}
	blockBytes := len(candidate.BlockBOC)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		req.Previous.State = store.root(b, resident)
		built, err := builder.BuildShard(ctx, req)
		if err != nil {
			b.Fatal(err)
		}
		if len(built.BlockBOC) != blockBytes {
			b.Fatalf("iteration produced a %d-byte block against the %d-byte first one: "+
				"the predecessor is not being rebuilt", len(built.BlockBOC), blockBytes)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(candidate.Stats.Transactions), "tx/block")
	b.ReportMetric(float64(candidate.Stats.GasUsed), "gas/block")
	b.ReportMetric(float64(blockBytes), "blockB")
}

func BenchmarkVerifyMainnetHeavyFixture(b *testing.B) {
	req, _, _ := benchMainnetFixtureRequest(b, benchMainnetExportRepeat())
	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		b.Fatal(err)
	}
	verification := shardVerificationRequest(req, candidate)
	verification.NeighborShardEndLT = req.NeighborShardEndLT
	verification.Semantics = NewSemanticVerifier(tvm.NewTVM())
	if err = VerifyShardCandidate(context.Background(), verification); err != nil {
		b.Fatalf("verify heavy fixture candidate: %v", err)
	}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := VerifyShardCandidate(ctx, verification); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(candidate.Stats.Transactions), "tx/block")
	b.ReportMetric(float64(len(candidate.BlockBOC)), "blockB")
}

// The collated variants run the configuration a validator on mainnet actually
// runs. With capFullCollatedData set, collation carries a proof-size estimator
// on every first cell load, builds a Merkle proof per touched account, and
// turns the collated data from a near-empty BOC into a real proof of the
// previous state; validation, in turn, decodes and checks that proof instead of
// skipping it. Every other mainnet benchmark in this file leaves the capability
// clear, so none of that path is measured by them — these rows are reported
// next to those, never in place of them, because a block with the capability on
// is not the same block.
//
// The capability needs a real predecessor block, which is why these build on
// benchMainnetFixtureRequest rather than on benchMainnetRequest: the collated
// data has to prove the previous state, and a hand-assembled one has no block
// to prove it against.
func benchMainnetCollatedRequest(tb testing.TB, repeat int) ShardRequest {
	tb.Helper()

	req, _, _ := benchMainnetFixtureRequest(tb, repeat)
	// Turned on only after the predecessor is minted, the same order the
	// synthetic heavy-collated profile uses: the minting collation has no
	// neighbours, and the capability demands a neighbour set matching the
	// registered shards. The Config is built per request by loadMainnetConfig,
	// so this cannot leak into the non-collated rows sharing the process.
	req.Masterchain.Config.capabilities |= capFullCollatedData
	return req
}

// benchMainnetCollatedFloor is the size below which the collated data cannot be
// carrying a state proof. With the capability clear the same block produces a
// few dozen bytes — just the consensus extra root — so a row that silently lost
// the capability would still benchmark, at a number that means nothing.
const benchMainnetCollatedFloor = 64 << 10

// benchMainnetCollatedMarkerBytes is what collated data degenerates to with
// capFullCollatedData clear: the consensus extra root alone. A row that means to
// run without the capability asserts this exact size, so "no proof was built"
// and "the proof came out suspiciously small" can never be confused.
const benchMainnetCollatedMarkerBytes = 33

func assertBenchMainnetCollated(tb testing.TB, candidate *Candidate) {
	tb.Helper()

	if candidate.Stats.Transactions == 0 || candidate.Stats.GasUsed == 0 {
		tb.Fatalf("collated mainnet workload ran nothing: %d transactions, %d gas",
			candidate.Stats.Transactions, candidate.Stats.GasUsed)
	}
	if len(candidate.CollatedData) < benchMainnetCollatedFloor {
		tb.Fatalf("collated data is %d bytes, too small to hold a state proof — "+
			"capFullCollatedData is not in effect", len(candidate.CollatedData))
	}
	tb.Logf("collated mainnet workload: %d transactions, %d gas, %d block bytes, %d collated bytes",
		candidate.Stats.Transactions, candidate.Stats.GasUsed,
		len(candidate.BlockBOC), len(candidate.CollatedData))
}

func reportMainnetCollated(b *testing.B, candidate *Candidate) {
	b.ReportMetric(float64(candidate.Stats.Transactions), "tx/block")
	b.ReportMetric(float64(candidate.Stats.GasUsed), "gas/block")
	b.ReportMetric(float64(len(candidate.BlockBOC)), "blockB")
	b.ReportMetric(float64(len(candidate.CollatedData)), "collatedB")
}

// THE GOGC LEVER, measured on the current tree, because a claim about it kept
// being repeated and then deleted rather than booked. This is the honest figure
// and its method.
//
// Method, on BenchmarkCollateMainnetHeavyCollated (Apple M1 Pro, go1.26.4,
// darwin/arm64): process CPU (user+sys from /usr/bin/time -l) at -benchtime 90x
// minus the same at 30x, three samples each, so the fixture build, the process
// start and the one warm-up collation all cancel and what remains is 60 real
// collations. A single-run process CPU number does NOT work here and was tried
// first: at GOGC=100 two consecutive whole-process runs came out at 8.35 s and
// 16.38 s of user time, a 2x spread that swamps the effect. The differencing
// removes it.
//
//	CPU per collation   GOGC=100  92.2 ms  (89.0-95.5 across sample pairs)
//	                    GOGC=400  67.1 ms  (62.2-73.2)
//	                    -> -27.3% on the sample means, -18% .. -35% at the bounds
//
// Wall time per op did NOT move outside its own noise (50.0/50.4 ms at GOGC=100
// against 52.6/48.1 ms at GOGC=400): this is a CPU lever, not a latency one, and
// nothing here licenses a wall-clock claim.
//
// The heap it acts on, live OBJECTS and live BYTES reported separately as they
// must be, sampled through runtime/metrics after three collations and two forced
// GCs: 461,895 live objects and 91.6 MB live bytes, IDENTICAL at both GOGC values
// to within 0.01% — live heap is a property of the program and GOGC does not
// change it. What GOGC changes is the goal computed from it: /gc/heap/goal:bytes
// went 183.6 MB at GOGC=100 to 459.5 MB at GOGC=400, and maximum resident set
// size followed, 413-671 MB to 639-652 MB. That is the price of the 27%, and it
// is why this is recorded as a measurement and not as a recommendation — see
// service/validator/retention.go on why nothing in this tree derives a budget
// from GOMEMLIMIT.
//
// The box was at load average 12-13 on 10 cores throughout. That widens the
// upper tails of both arms, so the bounds above are conservative.
func BenchmarkCollateMainnetCollated(b *testing.B) {
	benchmarkCollateMainnetCollated(b, 1)
}

func BenchmarkCollateMainnetHeavyCollated(b *testing.B) {
	benchmarkCollateMainnetCollated(b, benchMainnetHeavyRepeat)
}

func benchmarkCollateMainnetCollated(b *testing.B, repeat int) {
	b.Helper()

	req := benchMainnetCollatedRequest(b, repeat)
	builder := testBuilder()
	ctx := context.Background()
	candidate, err := builder.BuildShard(ctx, req)
	if err != nil {
		b.Fatal(err)
	}
	assertBenchMainnetCollated(b, candidate)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := builder.BuildShard(ctx, req); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	reportMainnetCollated(b, candidate)
}

func BenchmarkVerifyMainnetCollated(b *testing.B) {
	benchmarkVerifyMainnetCollated(b, 1)
}

func BenchmarkVerifyMainnetHeavyCollated(b *testing.B) {
	benchmarkVerifyMainnetCollated(b, benchMainnetHeavyRepeat)
}

func benchmarkVerifyMainnetCollated(b *testing.B, repeat int) {
	b.Helper()

	req := benchMainnetCollatedRequest(b, repeat)
	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		b.Fatal(err)
	}
	assertBenchMainnetCollated(b, candidate)

	verification := shardVerificationRequest(req, candidate)
	verification.NeighborShardEndLT = req.NeighborShardEndLT
	verification.Semantics = NewSemanticVerifier(tvm.NewTVM())
	if err = VerifyShardCandidate(context.Background(), verification); err != nil {
		// This used to fail, and not because of the fixture: six accounts in
		// this block carry a storage-stat dictionary and every one of them
		// transacts between two and sixteen times, while the collated proof of
		// that dictionary only covered what the account's first transaction
		// read — the replay of the second walked into a pruned branch and the
		// executor reported "dict has special cells in tree structure". The
		// proof is now created from the fully traced builder at the end of
		// collation; TestFullCollatedMainnetCandidateVerifies is what holds that
		// closed, this stays a benchmark precondition.
		b.Fatalf("verify collated mainnet candidate: %v", err)
	}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := VerifyShardCandidate(ctx, verification); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	reportMainnetCollated(b, candidate)
}

func BenchmarkCollateMainnetPhases(b *testing.B) {
	req, shape := benchMainnetRequest(b, benchMainnetFiller)
	builder := testBuilder()
	timer := newPhaseTimer()
	candidate, err := builder.BuildShard(context.Background(), req)
	if err != nil {
		b.Fatal(err)
	}
	assertBenchMainnetRan(b, candidate, shape)

	b.ResetTimer()
	for b.Loop() {
		collateWithPhases(b, builder, req, timer)
	}
	b.StopTimer()
	timer.report(b, collationPhases)
	reportMainnetShape(b, candidate, shape)
}

func BenchmarkVerifyMainnetPhases(b *testing.B) {
	req, shape := benchMainnetRequest(b, benchMainnetFiller)
	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		b.Fatal(err)
	}
	assertBenchMainnetRan(b, candidate, shape)

	verification := shardVerificationRequest(req, candidate)
	verification.NeighborShardEndLT = req.NeighborShardEndLT
	verification.Semantics = NewSemanticVerifier(tvm.NewTVM())
	timer := newPhaseTimer()

	b.ResetTimer()
	for b.Loop() {
		verifyWithPhases(b, verification, timer)
	}
	b.StopTimer()
	timer.report(b, verificationPhases)
	reportMainnetShape(b, candidate, shape)
}

func BenchmarkVerifyMainnet(b *testing.B) {
	req, shape := benchMainnetRequest(b, benchMainnetFiller)
	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		b.Fatal(err)
	}
	assertBenchMainnetRan(b, candidate, shape)

	verification := shardVerificationRequest(req, candidate)
	verification.NeighborShardEndLT = req.NeighborShardEndLT
	verification.Semantics = NewSemanticVerifier(tvm.NewTVM())
	if err = VerifyShardCandidate(context.Background(), verification); err != nil {
		b.Fatalf("verify mainnet candidate: %v", err)
	}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := VerifyShardCandidate(ctx, verification); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	reportMainnetShape(b, candidate, shape)
}

// benchMainnetFiller keeps the account dictionary the size the other profiles
// use, so collation numbers stay comparable across workloads.
const benchMainnetFiller = 100_000

// assertBenchMainnetRan fails when the fixture degenerated into an empty block.
// Real accounts can bounce real messages — that is legitimate traffic — but a
// block that ran nothing means the request was assembled wrongly, and it would
// otherwise be reported as a very fast collation.
func assertBenchMainnetRan(tb testing.TB, candidate *Candidate, shape benchMainnetShape) {
	tb.Helper()

	offered := shape.externals + shape.internals
	if candidate.Stats.Transactions == 0 {
		tb.Fatalf("mainnet workload produced no transactions from %d offered messages", offered)
	}
	if candidate.Stats.GasUsed == 0 {
		tb.Fatalf("mainnet workload burned no gas over %d transactions", candidate.Stats.Transactions)
	}
	tb.Logf("mainnet workload: %d accounts, %d externals + %d internals offered, "+
		"%d transactions, %d gas, %d block bytes",
		shape.accounts, shape.externals, shape.internals,
		candidate.Stats.Transactions, candidate.Stats.GasUsed, len(candidate.BlockBOC))
}

func reportMainnetShape(b *testing.B, candidate *Candidate, shape benchMainnetShape) {
	b.ReportMetric(float64(candidate.Stats.Transactions), "tx/block")
	b.ReportMetric(float64(candidate.Stats.GasUsed), "gas/block")
	b.ReportMetric(float64(len(candidate.BlockBOC)), "blockB")
}
