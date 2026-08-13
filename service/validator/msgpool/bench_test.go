package msgpool

import (
	"fmt"
	"testing"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

// BenchmarkAddNew measures pooling a fresh, already-deserialized message —
// the real ingress hot path (norm hash outside the lock, one short critical
// section, no BOC work). Both production callers hand over the root cell
// without the received bytes, so raw is nil here too: passing a buffer would
// benchmark a path nothing takes.
func BenchmarkAddNew(b *testing.B) {
	p := New(Config{MempoolLimit: 1 << 30, PerAddressLimit: 1 << 30, MempoolBytesLimit: 1 << 40})
	b.Cleanup(p.Close)
	msgs := make([]testMsgB, b.N)
	for i := range msgs {
		var addr [32]byte
		copy(addr[:], fmt.Sprintf("%016d-bench-addr", i))
		msgs[i] = buildExtMsgBench(addr)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := p.AddExternal(nil, msgs[i].root, nil, 0); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAddNewWithExternalStreams(b *testing.B) {
	for _, count := range []int{1, 4} {
		b.Run(fmt.Sprintf("streams=%d", count), func(b *testing.B) {
			p := New(Config{MempoolLimit: 1 << 30, PerAddressLimit: 1 << 30, MempoolBytesLimit: 1 << 40})
			b.Cleanup(p.Close)
			for range count {
				stream, err := p.OpenExternalStream(allShard, 500)
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { _ = stream.Close() })
			}
			msgs := make([]testMsgB, b.N)
			for i := range msgs {
				var addr [32]byte
				copy(addr[:], fmt.Sprintf("%016d-stream-bench", i))
				msgs[i] = buildExtMsgBench(addr)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := p.AddExternal(nil, msgs[i].root, nil, 0); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkAddDuplicate measures the dedup fast path: resolved from the
// cached root hash before any assembly.
func BenchmarkAddDuplicate(b *testing.B) {
	p := New(Config{})
	b.Cleanup(p.Close)
	m := buildExtMsgBench(testAddr(0x01))
	if err := p.AddExternal(m.raw, m.root, nil, 0); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := p.AddExternal(m.raw, m.root, nil, 0); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSelectForBlock measures batch selection over a full pool.
func BenchmarkSelectForBlock(b *testing.B) {
	p := New(Config{})
	b.Cleanup(p.Close)
	for i := 0; i < 2048; i++ {
		var addr [32]byte
		copy(addr[:], fmt.Sprintf("%08d-addr", i))
		m := buildExtMsgBench(addr)
		if err := p.AddExternal(m.raw, m.root, nil, i%3); err != nil {
			b.Fatal(err)
		}
	}
	shard := ShardIdent{Workchain: 0, Shard: ShardAll}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if got := p.SelectForBlock(shard, 0); len(got) != 2048 {
			b.Fatalf("selection size %d", len(got))
		}
	}
}

// BenchmarkOpenExternalSnapshot measures the masterchain path: freeze the
// ready set, then drain it through the same bounded queue capacity as C++
// without preparing every message payload up front.
func BenchmarkOpenExternalSnapshot(b *testing.B) {
	p := New(Config{})
	b.Cleanup(p.Close)
	for i := 0; i < 2048; i++ {
		var addr [32]byte
		copy(addr[:], fmt.Sprintf("%08d-snapshot", i))
		m := buildExtMsgBench(addr)
		if err := p.AddExternal(m.raw, m.root, nil, i%3); err != nil {
			b.Fatal(err)
		}
	}
	shard := ShardIdent{Workchain: 0, Shard: ShardAll}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stream, err := p.OpenExternalSnapshot(shard, 500)
		if err != nil {
			b.Fatal(err)
		}
		total := 0
		for {
			batch := stream.TakeReady(500)
			if len(batch) == 0 {
				break
			}
			total += len(batch)
		}
		if total != 2048 {
			b.Fatalf("snapshot size %d", total)
		}
	}
}

// BenchmarkSelectForBlockLimited measures the common collation case: a
// bounded batch out of a big pool.
func BenchmarkSelectForBlockLimited(b *testing.B) {
	p := New(Config{})
	b.Cleanup(p.Close)
	for i := 0; i < 2048; i++ {
		var addr [32]byte
		copy(addr[:], fmt.Sprintf("%08d-addr", i))
		m := buildExtMsgBench(addr)
		if err := p.AddExternal(m.raw, m.root, nil, 0); err != nil {
			b.Fatal(err)
		}
	}
	shard := ShardIdent{Workchain: 0, Shard: ShardAll}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if got := p.SelectForBlock(shard, 256); len(got) != 256 {
			b.Fatalf("selection size %d", len(got))
		}
	}
}

func BenchmarkSelectForBlockLimitedPriorities(b *testing.B) {
	p := New(Config{})
	b.Cleanup(p.Close)
	for i := 0; i < 2048; i++ {
		var addr [32]byte
		copy(addr[:], fmt.Sprintf("%08d-addr", i))
		m := buildExtMsgBench(addr)
		if err := p.AddExternal(m.raw, m.root, nil, i%3); err != nil {
			b.Fatal(err)
		}
	}
	shard := ShardIdent{Workchain: 0, Shard: ShardAll}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if got := p.SelectForBlock(shard, 256); len(got) != 256 {
			b.Fatalf("selection size %d", len(got))
		}
	}
}

// BenchmarkSelectForBlockLimitedParallel measures lock contention between
// concurrent collations. Candidate ranking and snapshot construction happen
// after the pool lock is released.
func BenchmarkSelectForBlockLimitedParallel(b *testing.B) {
	p := New(Config{})
	b.Cleanup(p.Close)
	for i := 0; i < 2048; i++ {
		var addr [32]byte
		copy(addr[:], fmt.Sprintf("%08d-addr", i))
		m := buildExtMsgBench(addr)
		if err := p.AddExternal(m.raw, m.root, nil, 0); err != nil {
			b.Fatal(err)
		}
	}
	shard := ShardIdent{Workchain: 0, Shard: ShardAll}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if got := p.SelectForBlock(shard, 256); len(got) != 256 {
				b.Fatalf("selection size %d", len(got))
			}
		}
	})
}

// BenchmarkEraseAppliedNormalizedVariants measures cleanup when many raw
// encodings share one normalized message hash.
func BenchmarkEraseAppliedNormalizedVariants(b *testing.B) {
	for _, variants := range []int{64, 256, 1024} {
		b.Run(fmt.Sprintf("variants=%d", variants), func(b *testing.B) {
			messages := make([]testMsgB, variants)
			addr := testAddr(0x77)
			for i := range messages {
				messages[i] = buildExtMsgBenchFee(0, addr, uint64(i))
			}
			assembled, err := newExternalMessage(len(messages[0].raw), messages[0].root, nil)
			if err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				p := New(Config{
					Clock:             newFakeClock(),
					MempoolLimit:      variants,
					PerAddressLimit:   variants,
					MempoolBytesLimit: 1 << 40,
				})
				for _, message := range messages {
					if err = p.AddExternal(message.raw, message.root, nil, 0); err != nil {
						b.Fatal(err)
					}
				}
				b.StartTimer()

				p.EraseApplied([][32]byte{assembled.HashNorm})
			}
		})
	}
}

type testMsgB struct {
	raw  []byte
	root *cell.Cell
}

func buildExtMsgBench(addr [32]byte) testMsgB {
	return buildExtMsgBenchFee(0, addr, 0)
}

func buildExtMsgBenchFee(wc int32, addr [32]byte, importFee uint64) testMsgB {
	c := cell.BeginCell().
		MustStoreUInt(0b10, 2).
		MustStoreUInt(0b00, 2).
		MustStoreAddr(addrOf(wc, addr)).
		MustStoreCoins(importFee).
		MustStoreUInt(0b01, 2).
		MustStoreRef(cell.BeginCell().MustStoreSlice(addr[:], 256).EndCell()).
		EndCell()
	return testMsgB{raw: c.ToBOC(), root: c}
}
