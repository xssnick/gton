package validator

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/xssnick/gton/service/hooks"
	"github.com/xssnick/gton/service/validator/msgpool"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func benchService(b *testing.B) *Service {
	b.Helper()
	factory := New(validatorTestOptions(Options{StatsInterval: -1}))
	ext, err := factory(validatorTestNode())
	if err != nil {
		b.Fatal(err)
	}
	s := ext.(*Service)
	if err = s.pool.Internals().ReconcileDestinations([]msgpool.ShardIdent{allShard}); err != nil {
		b.Fatal(err)
	}
	if err = s.Start(context.Background()); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = s.Close(ctx)
	})
	return s
}

// benchBlockEvent builds the benchmark block shape: 100 imported externals
// and 250 exported internals.
func benchBlockEvent(tb testing.TB) hooks.BlockAppliedEvent {
	tb.Helper()
	inDict, err := cell.NewAugDict(256, tlb.AugInMsgDescr{})
	if err != nil {
		tb.Fatal(err)
	}
	txStub := cell.BeginCell().MustStoreUInt(0xdead, 32).EndCell()
	for i := 0; i < 100; i++ {
		msg := extMsgCell(tb, testAddr(byte(i)), uint64(1000+i))
		value := cell.BeginCell().
			MustStoreUInt(0b000, 3). // msg_import_ext
			MustStoreRef(msg).
			MustStoreRef(txStub).
			EndCell()
		if err = inDict.SetIntKey(new(big.Int).SetBytes(msg.Hash()), value); err != nil {
			tb.Fatal(err)
		}
	}
	outDict, err := cell.NewAugDict(256, tlb.AugOutMsgDescr{})
	if err != nil {
		tb.Fatal(err)
	}
	for i := 0; i < 250; i++ {
		msg := feedInternalMsg(tb, byte(i), uint64(1_000_000+i))
		env := feedEnvelope(tb, msg)
		value := cell.BeginCell().
			MustStoreUInt(0b001, 3). // msg_export_new
			MustStoreRef(env).
			MustStoreRef(txStub).
			EndCell()
		if err = outDict.SetIntKey(new(big.Int).SetBytes(msg.Hash()), value); err != nil {
			tb.Fatal(err)
		}
	}
	stub := cell.BeginCell().EndCell()
	extra := cell.BeginCell().
		MustStoreUInt(0x4a33f6fd, 32).
		MustStoreRef(inDict.AsCell()).
		MustStoreRef(outDict.AsCell()).
		MustStoreRef(stub).
		MustStoreSlice(make([]byte, 64), 512).
		EndCell()
	root := cell.BeginCell().
		MustStoreUInt(0x11ef55aa, 32).
		MustStoreUInt(0, 32).
		MustStoreRef(stub).
		MustStoreRef(stub).
		MustStoreRef(stub).
		MustStoreRef(extra).
		EndCell()
	return appliedEvent(root)
}

// BenchmarkHookStaleSkip: the catch-up path — gate check plus the pending
// head bookkeeping.
func BenchmarkHookStaleSkip(b *testing.B) {
	s := benchService(b)
	ev := benchBlockEvent(b)
	ev.Meta.GenUTime = uint32(time.Now().Add(-2 * time.Hour).Unix())

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.feed.Observe(appliedBlock(ev))
	}
}

// BenchmarkHookProcessBlock250: the full inline bookkeeping of a fresh
// 250-transaction block — 100 imported externals (normalized-hash cleanup)
// plus 250 exported internals (delta parse and run advance).
func BenchmarkHookProcessBlock250(b *testing.B) {
	s := benchService(b)
	internals := s.pool.Internals()
	ev := benchBlockEvent(b)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		// Advance the seqno every iteration: a pinned one would hit the
		// per-source high-water dedup after the first pass and the bench
		// would measure the gate instead of the hook.
		ev.Meta.ID.SeqNo = uint32(i) + 1
		if err := internals.Seed(allShard, allShard, msgpool.SourceRef{Seqno: uint32(i)}, nil, 0); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		s.feed.Observe(appliedBlock(ev))
	}
}

// BenchmarkNormHashes100 isolates the externals-cleanup parse.
func BenchmarkNormHashes100(b *testing.B) {
	ev := benchBlockEvent(b)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := msgpool.AppliedNormHashesFromBlockRoot(ev.BlockRoot); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDelta250 isolates the internals delta parse.
func BenchmarkDelta250(b *testing.B) {
	ev := benchBlockEvent(b)
	pool := msgpool.New(msgpool.Config{})
	b.Cleanup(pool.Close)
	internals := pool.Internals()
	if err := internals.ReconcileDestinations([]msgpool.ShardIdent{allShard}); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := internals.DeltasFromBlockRoot(allShard, msgpool.SourceRef{Seqno: 1}, ev.BlockRoot, 0); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkQueueSizeCheck isolates the runtime size invariant read.
func BenchmarkQueueSizeCheck(b *testing.B) {
	msg := feedInternalMsg(b, 0x22, 1000)
	state := feedStateRoot(b, map[msgpool.QueueKey]tlb.EnqueuedMsg{
		feedQueueKey(b, msg, 0x22): {EnqueuedLT: 1000, Msg: feedEnvelope(b, msg)},
	})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := msgpool.QueueSizeFromStateRoot(state); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSeedFromState10k: the reseed worst case — a full walk of a
// 10k-message queue plus the run installation.
func BenchmarkSeedFromState10k(b *testing.B) {
	queued := make(map[msgpool.QueueKey]tlb.EnqueuedMsg, 10_000)
	for i := 0; i < 10_000; i++ {
		msg := feedInternalMsg(b, byte(i), uint64(1_000_000+i))
		queued[feedQueueKey(b, msg, byte(i))] = tlb.EnqueuedMsg{
			EnqueuedLT: uint64(1_000_000 + i), Msg: feedEnvelope(b, msg),
		}
	}
	state := feedStateRoot(b, queued)
	s := benchService(b)
	internals := s.pool.Internals()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		seeds, total, err := internals.SeedsFromStateRoot(allShard, msgpool.SourceRef{Seqno: 1}, state)
		if err != nil {
			b.Fatal(err)
		}
		if err = internals.Seed(allShard, allShard, msgpool.SourceRef{Seqno: 1}, seeds[0].Messages, total); err != nil {
			b.Fatal(err)
		}
	}
}
