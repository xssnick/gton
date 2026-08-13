package collator

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func BenchmarkMinimumDispatchAccount(b *testing.B) {
	for _, accountCount := range []int{16, 256, 4096} {
		b.Run(fmt.Sprintf("accounts=%d", accountCount), func(b *testing.B) {
			queue := benchmarkDispatchQueue(b, accountCount)
			want := benchmarkDispatchAccount(accountCount - 1)

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				selected, err := minimumDispatchAccount(queue)
				if err != nil {
					b.Fatal(err)
				}
				if selected.AccountID != want {
					b.Fatalf("minimum dispatch account = %x, want %x", selected.AccountID, want)
				}
			}
		})
	}
}

func benchmarkDispatchQueue(tb testing.TB, accountCount int) *tlb.DispatchQueueAugDict {
	tb.Helper()
	queue, err := tlb.NewDispatchQueueAugDict()
	if err != nil {
		tb.Fatal(err)
	}
	for i := range accountCount {
		messages := cell.NewDict(64)
		lt := uint64(accountCount - i)
		if err = messages.Set(dispatchLTKey(lt), cell.BeginCell().EndCell()); err != nil {
			tb.Fatal(err)
		}
		accountQueue, serializeErr := (tlb.AccountDispatchQueue{
			Messages: messages,
			Count:    1,
		}).ToCell()
		if serializeErr != nil {
			tb.Fatal(serializeErr)
		}
		if err = queue.Set(dispatchAccountKey(benchmarkDispatchAccount(i)), accountQueue); err != nil {
			tb.Fatal(err)
		}
	}
	return queue
}

func benchmarkDispatchAccount(index int) [32]byte {
	var accountID [32]byte
	binary.BigEndian.PutUint64(accountID[24:], uint64(index))
	return accountID
}
