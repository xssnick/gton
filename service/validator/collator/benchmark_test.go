package collator

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func BenchmarkBuildRejectedExternalBatch(b *testing.B) {
	for _, size := range []int{1, 16, 256} {
		b.Run(fmt.Sprintf("messages=%d", size), func(b *testing.B) {
			req := emptyCandidateRequest(b)
			rejecting := address.NewAddress(0, 0, bytes.Repeat([]byte{0x6a}, 32))
			req.Previous.State = stateWithAccounts(b, req.Previous.State, activeContracts(b, req.Header.GenUtime,
				activeContract{address: rejecting, code: externalRejectCode(b), balance: 100_000_000_000},
			))
			req.Externals = make([]ExternalInput, size)
			for i := range req.Externals {
				message, err := tlb.ToCell(&tlb.ExternalMessage{
					DstAddr: rejecting,
					Body:    cell.BeginCell().MustStoreUInt(uint64(i), 16).EndCell(),
				})
				if err != nil {
					b.Fatal(err)
				}
				req.Externals[i] = externalInput(b, message)
			}
			req.MaxExternalAttempts = size
			builder := testBuilder()

			b.ReportAllocs()
			for b.Loop() {
				candidate, err := builder.BuildShard(context.Background(), req)
				if err != nil {
					b.Fatal(err)
				}
				if int(candidate.Stats.ExternalAttempts) != size {
					b.Fatalf("Build attempted %d externals, want %d", candidate.Stats.ExternalAttempts, size)
				}
			}
		})
	}
}
