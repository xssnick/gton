package msgpool

import (
	"fmt"
	"testing"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

// BenchmarkAddNew measures pooling a fresh, already-deserialized message —
// the real ingress hot path (norm hash outside the lock, one short critical
// section, no BOC work). Production callers pass the exact received BOC size
// without retaining the ingress buffer.
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
		if _, err := p.AddExternal(len(msgs[i].raw), msgs[i].root, nil, 0); err != nil {
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
				if _, err := p.AddExternal(len(msgs[i].raw), msgs[i].root, nil, 0); err != nil {
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
	if _, err := p.AddExternal(len(m.raw), m.root, nil, 0); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := p.AddExternal(len(m.raw), m.root, nil, 0); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkExternalMessageSizeAccounting isolates the ingress-size change on
// a multi-cell message. The legacy sub-benchmark reproduces the removed DAG
// walk so the cost stays directly comparable without restoring old production
// code between runs.
func BenchmarkExternalMessageSizeAccounting(b *testing.B) {
	body := cell.BeginCell().EndCell()
	for i := range 256 {
		body = cell.BeginCell().
			MustStoreUInt(uint64(i), 16).
			MustStoreRef(body).
			EndCell()
	}
	root := cell.BeginCell().
		MustStoreUInt(0b10, 2).
		MustStoreUInt(0b00, 2).
		MustStoreAddr(addrOf(0, testAddr(0x7a))).
		MustStoreCoins(0).
		MustStoreUInt(0b01, 2).
		MustStoreRef(body).
		EndCell()
	serializedSize := len(root.ToBOC())

	b.Run("exact-ingress-size", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := newExternalMessage(serializedSize, root, nil); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("legacy-structural-estimate", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			size := benchmarkEstimateBOCSize(root)
			if _, err := newExternalMessage(size, root, nil); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func benchmarkEstimateBOCSize(root *cell.Cell) int {
	visited := map[*cell.Cell]struct{}{}
	stack := []*cell.Cell{root}
	total := 0
	for len(stack) > 0 {
		current := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if _, seen := visited[current]; seen {
			continue
		}
		visited[current] = struct{}{}

		refs := int(current.RefsNum())
		total += 2 + int((current.BitsSize()+7)/8) + refs
		for index := range refs {
			ref, err := current.PeekRef(index)
			if err == nil {
				stack = append(stack, ref)
			}
		}
	}

	return total
}

// BenchmarkSelectForBlock measures batch selection over a full pool.
func BenchmarkSelectForBlock(b *testing.B) {
	p := New(Config{})
	b.Cleanup(p.Close)
	for i := 0; i < 2048; i++ {
		var addr [32]byte
		copy(addr[:], fmt.Sprintf("%08d-addr", i))
		m := buildExtMsgBench(addr)
		if _, err := p.AddExternal(len(m.raw), m.root, nil, i%3); err != nil {
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
		if _, err := p.AddExternal(len(m.raw), m.root, nil, i%3); err != nil {
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
		if _, err := p.AddExternal(len(m.raw), m.root, nil, 0); err != nil {
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
		if _, err := p.AddExternal(len(m.raw), m.root, nil, i%3); err != nil {
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
		if _, err := p.AddExternal(len(m.raw), m.root, nil, 0); err != nil {
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
					if _, err = p.AddExternal(len(message.raw), message.root, nil, 0); err != nil {
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

// BenchmarkTakeReadyFromDeepPool is the shape the collator meets in a slot: a
// mempool far deeper than the stream buffer, drained a batch at a time.
//
// Topping the buffer back up after every batch is a full scan of every slab,
// under the pool mutex, re-ranking a pool the batch barely moved. With a buffer
// of thousands and a batch of hundreds the buffer never emptied, so that scan
// ran on every take rather than on the takes that needed it.
func BenchmarkTakeReadyFromDeepPool(b *testing.B) {
	const (
		pooled   = 8192
		capacity = 4096
		batch    = 500
	)
	p := New(Config{MempoolLimit: 1 << 30, PerAddressLimit: 1 << 30, MempoolBytesLimit: 1 << 40})
	b.Cleanup(p.Close)
	for i := range pooled {
		var addr [32]byte
		copy(addr[:], fmt.Sprintf("%016d-deep-take-bench", i))
		msg := buildExtMsgBench(addr)
		if _, err := p.AddExternal(len(msg.raw), msg.root, nil, 0); err != nil {
			b.Fatal(err)
		}
	}
	stream, err := p.OpenExternalStream(allShard, capacity)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = stream.Close() })

	b.ReportAllocs()
	b.ResetTimer()
	taken := 0
	for b.Loop() {
		taken += len(stream.TakeReady(batch))
	}
	b.StopTimer()
	if taken == 0 {
		b.Fatal("the stream returned nothing; the fixture is not exercising a take")
	}
}
