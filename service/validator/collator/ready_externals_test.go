package collator

import (
	"bytes"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

func TestBuildShardWithReadyExternalsDrainsAlreadyAdmittedMessages(t *testing.T) {
	req, pool, stream := readyExternalFixture(t, externalAcceptCode(t), 1)
	addReadyExternal(t, pool, readyExternalAddress(), 1)

	candidate, _, err := testBuilder().buildShardWithReadyExternals(
		t.Context(),
		req,
		stream,
		time.Time{},
		time.Time{},
		1,
		time.Time{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Stats.ExternalIncluded != 1 || candidate.Stats.ExternalBatches != 1 {
		t.Fatalf("unexpected ready external stats: %+v", candidate.Stats)
	}
	if candidate.Stats.ExternalStop != ExternalStopReadyDrained || candidate.Stats.ExternalWait != 0 {
		t.Fatalf("unexpected ready stop: %+v", candidate.Stats)
	}
}

func TestBuildShardWithReadyExternalsConsumesAdmissionsUntilSlotBoundary(t *testing.T) {
	req, pool, stream := readyExternalFixture(t, externalAcceptCode(t), 1)
	waitUntil := time.Now().Add(200 * time.Millisecond)

	type buildResult struct {
		candidate *Candidate
		err       error
	}
	result := make(chan buildResult, 1)
	go func() {
		candidate, _, err := testBuilder().buildShardWithReadyExternals(
			t.Context(),
			req,
			stream,
			waitUntil,
			waitUntil,
			1,
			time.Time{},
		)
		result <- buildResult{candidate: candidate, err: err}
	}()

	time.Sleep(30 * time.Millisecond)
	addReadyExternal(t, pool, readyExternalAddress(), 2)

	select {
	case built := <-result:
		if built.err != nil {
			t.Fatal(built.err)
		}
		if built.candidate.Stats.ExternalIncluded != 1 || built.candidate.Stats.ExternalBatches != 1 {
			t.Fatalf("unexpected live external stats: %+v", built.candidate.Stats)
		}
		if built.candidate.Stats.ExternalStop != ExternalStopDeadline {
			t.Fatalf("external stop = %v, want slot deadline", built.candidate.Stats.ExternalStop)
		}
		if built.candidate.Stats.ExternalWait < 100*time.Millisecond {
			t.Fatalf("external wait = %s, want a real wait through the slot boundary", built.candidate.Stats.ExternalWait)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("live external collation did not finish")
	}
}

func TestBuildShardWithReadyExternalsSharesAttemptBudgetAcrossBatches(t *testing.T) {
	req, pool, stream := readyExternalFixture(t, externalRejectCode(t), 1)
	req.MaxExternalAttempts = 2
	for i := uint64(0); i < 4; i++ {
		addReadyExternal(t, pool, readyExternalAddress(), i+1)
	}

	candidate, _, err := testBuilder().buildShardWithReadyExternals(
		t.Context(),
		req,
		stream,
		time.Time{},
		time.Time{},
		1,
		time.Time{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Stats.ExternalAttempts != 2 || candidate.Stats.ExternalNotAccepted != 2 ||
		candidate.Stats.ExternalSkippedLimit != 1 {
		t.Fatalf("unexpected cumulative attempt stats: %+v", candidate.Stats)
	}
	if candidate.Stats.ExternalStop != ExternalStopAttemptLimit || candidate.Stats.ExternalBatches != 3 {
		t.Fatalf("unexpected cumulative attempt stop: %+v", candidate.Stats)
	}
}

func TestBuildMasterWithReadyExternalsUsesOnlyStartSnapshot(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)
	pool := msgpool.New(msgpool.Config{})
	t.Cleanup(pool.Close)
	dst := address.NewAddress(0, 0xff, fixture.configAddress)
	addReadyExternal(t, pool, dst, 1)
	stream, err := pool.OpenExternalSnapshot(
		msgpool.ShardIdent{Workchain: address.MasterchainID, Shard: msgpool.ShardAll},
		1,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stream.Close() })
	addReadyExternal(t, pool, dst, 2)

	candidate, _, err := testBuilder().buildMasterWithReadyExternals(
		t.Context(),
		fixture.request,
		stream,
		time.Time{},
		1,
	)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Stats.ExternalAttempts != 1 || candidate.Stats.ExternalBatches != 1 {
		t.Fatalf("unexpected masterchain external stats: %+v", candidate.Stats)
	}
	if candidate.Stats.ExternalStop != ExternalStopReadyDrained || candidate.Stats.ExternalWait != 0 {
		t.Fatalf("masterchain followed later admissions: %+v", candidate.Stats)
	}
}

func TestBuildMasterWithReadyExternalsHonorsExternalPhaseDeadline(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)
	pool := msgpool.New(msgpool.Config{})
	t.Cleanup(pool.Close)
	dst := address.NewAddress(0, 0xff, fixture.configAddress)
	addReadyExternal(t, pool, dst, 1)
	stream, err := pool.OpenExternalSnapshot(
		msgpool.ShardIdent{Workchain: address.MasterchainID, Shard: msgpool.ShardAll},
		1,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stream.Close() })

	candidate, _, err := testBuilder().buildMasterWithReadyExternals(
		t.Context(),
		fixture.request,
		stream,
		time.Now().Add(-time.Millisecond),
		1,
	)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Stats.ExternalAttempts != 0 || candidate.Stats.ExternalSkippedLimit != 1 {
		t.Fatalf("masterchain processed externals after its phase deadline: %+v", candidate.Stats)
	}
	if candidate.Stats.ExternalStop != ExternalStopDeadline {
		t.Fatalf("masterchain external stop = %v, want deadline", candidate.Stats.ExternalStop)
	}
}

func TestBuildShardReadyExternalSizeRetryReplaysFrozenTranscript(t *testing.T) {
	req, _ := benchMainnetRequest(t, 0)
	pool := msgpool.New(msgpool.Config{})
	t.Cleanup(pool.Close)
	openEmptyStream := func() *msgpool.ExternalStream {
		stream, err := pool.OpenExternalStream(targetShardIdent(req.Shard), 500)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = stream.Close() })
		return stream
	}

	natural, _, err := testBuilder().buildShardWithReadyExternals(
		t.Context(),
		req,
		openEmptyStream(),
		time.Time{},
		time.Time{},
		500,
		time.Time{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if natural.Stats.ExternalAttempts == 0 {
		t.Fatal("fixture contains no external work")
	}
	req.Masterchain.Config.maxBlockBytes = uint32(len(natural.BlockBOC)) - 1

	smaller, _, err := testBuilder().buildShardWithReadyExternals(
		t.Context(),
		req,
		openEmptyStream(),
		time.Time{},
		time.Time{},
		500,
		time.Time{},
	)
	if err != nil {
		t.Fatalf("ready collation did not recover from size overflow: %v", err)
	}
	if len(smaller.BlockBOC) > int(req.Masterchain.Config.maxBlockBytes) {
		t.Fatalf("rebuilt block is %d bytes, limit is %d", len(smaller.BlockBOC), req.Masterchain.Config.maxBlockBytes)
	}
	if smaller.Stats.Transactions > natural.Stats.Transactions {
		t.Fatalf("size retry grew from %d to %d transactions", natural.Stats.Transactions, smaller.Stats.Transactions)
	}
	if smaller.Stats.ExternalStop != ExternalStopReadyDrained || smaller.Stats.ExternalWait != 0 {
		t.Fatalf("size retry observed the live source twice: %+v", smaller.Stats)
	}
}

func readyExternalFixture(
	t *testing.T,
	code *cell.Cell,
	capacity int,
) (ShardRequest, *msgpool.Pool, *msgpool.ExternalStream) {
	t.Helper()

	req := emptyCandidateRequest(t)
	addr := readyExternalAddress()
	req.Previous.State = stateWithAccounts(t, req.Previous.State, activeContracts(
		t,
		req.Header.GenUtime,
		activeContract{address: addr, code: code, balance: 100_000_000_000},
	))
	pool := msgpool.New(msgpool.Config{})
	t.Cleanup(pool.Close)
	stream, err := pool.OpenExternalStream(targetShardIdent(req.Shard), capacity)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stream.Close() })

	return req, pool, stream
}

func readyExternalAddress() *address.Address {
	return address.NewAddress(0, 0, bytes.Repeat([]byte{0x61}, 32))
}

func addReadyExternal(t *testing.T, pool *msgpool.Pool, dst *address.Address, tag uint64) {
	t.Helper()

	message, err := tlb.ToCell(&tlb.ExternalMessage{
		DstAddr: dst,
		Body:    cell.BeginCell().MustStoreUInt(tag, 64).EndCell(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = pool.AddExternal(nil, message, nil, msgpool.ExternalPriorityLocal); err != nil {
		t.Fatal(err)
	}
}
