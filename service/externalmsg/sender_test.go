package externalmsg

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xssnick/gton/internal/extmsg"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestSenderChecksAddressLimitBeforeAdmission(t *testing.T) {
	now := time.Unix(1700000000, 0)
	addr := address.MustParseRawAddr("0:b113a994b5024a16719f69139328eb759596c38a25f59028b146fecdc3621dfe")
	body, _, _ := testExternalMessageBOC(t, addr)

	limiter := extmsg.NewAddressLimiter(1, 10*time.Second, 16)
	if err := limiter.Add(extmsg.AddressKeyFor(addr), now); err != nil {
		t.Fatalf("preload limiter: %v", err)
	}

	checks := 0
	broadcasts := 0
	sender, err := NewSender(SenderOptions{
		Check: func(context.Context, []byte, *cell.Cell, *tlb.ExternalMessage) (CheckResult, error) {
			checks++
			return CheckResult{}, nil
		},
		Broadcast: func(context.Context, []byte, *address.Address, *cell.Cell, *tlb.ExternalMessage) error {
			broadcasts++
			return nil
		},
		Now:     func() time.Time { return now },
		Limiter: limiter,
	})
	if err != nil {
		t.Fatalf("new sender: %v", err)
	}

	if err = sender.Send(context.Background(), body); !errors.Is(err, extmsg.ErrAddressRateLimited) {
		t.Fatalf("send err = %v, want address rate limited", err)
	}
	if checks != 0 {
		t.Fatalf("admission checks = %d, want 0", checks)
	}
	if broadcasts != 0 {
		t.Fatalf("broadcasts = %d, want 0", broadcasts)
	}
}

func TestSenderDuplicateSendForHashSkipsAdmission(t *testing.T) {
	now := time.Unix(1700000000, 0)
	addr := address.MustParseRawAddr("0:b113a994b5024a16719f69139328eb759596c38a25f59028b146fecdc3621dfe")
	body, root, msg := testExternalMessageBOC(t, addr)

	checks := 0
	broadcasts := 0
	sender, err := NewSender(SenderOptions{
		Check: func(context.Context, []byte, *cell.Cell, *tlb.ExternalMessage) (CheckResult, error) {
			checks++
			return CheckResult{Root: root, Message: msg}, nil
		},
		Broadcast: func(context.Context, []byte, *address.Address, *cell.Cell, *tlb.ExternalMessage) error {
			broadcasts++
			return nil
		},
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("new sender: %v", err)
	}

	if err = sender.Send(context.Background(), body); err != nil {
		t.Fatalf("first send: %v", err)
	}
	duplicateRoot, err := sender.SendForHash(context.Background(), body)
	if err != nil {
		t.Fatalf("duplicate send for hash: %v", err)
	}
	if checks != 1 {
		t.Fatalf("admission checks = %d, want 1", checks)
	}
	if broadcasts != 1 {
		t.Fatalf("broadcasts = %d, want 1", broadcasts)
	}
	got := duplicateRoot.Hash()
	want := root.Hash()
	if !bytes.Equal(got[:], want[:]) {
		t.Fatalf("duplicate root hash = %x, want %x", got, want)
	}
}

func TestSenderPreservesExternalNetworkError(t *testing.T) {
	addr := address.MustParseRawAddr("0:b113a994b5024a16719f69139328eb759596c38a25f59028b146fecdc3621dfe")
	body, root, msg := testExternalMessageBOC(t, addr)
	sender, err := NewSender(SenderOptions{
		Check: func(context.Context, []byte, *cell.Cell, *tlb.ExternalMessage) (CheckResult, error) {
			return CheckResult{Root: root, Message: msg}, nil
		},
		Broadcast: func(context.Context, []byte, *address.Address, *cell.Cell, *tlb.ExternalMessage) error {
			return extmsg.ErrNetworkOffline
		},
	})
	if err != nil {
		t.Fatalf("new sender: %v", err)
	}

	err = sender.Send(context.Background(), body)
	if !errors.Is(err, ErrBroadcastFailed) {
		t.Fatalf("send error = %v, want broadcast stage", err)
	}
	if !errors.Is(err, extmsg.ErrNetworkOffline) {
		t.Fatalf("send error = %v, want network offline", err)
	}
}

func testExternalMessageBOC(t *testing.T, addr *address.Address) ([]byte, *cell.Cell, *tlb.ExternalMessage) {
	t.Helper()

	msg := &tlb.ExternalMessage{
		DstAddr:   addr,
		ImportFee: tlb.ZeroCoins,
		Body:      cell.BeginCell().EndCell(),
	}
	root, err := tlb.ToCell(msg)
	if err != nil {
		t.Fatalf("build external message: %v", err)
	}
	return root.ToBOC(), root, msg
}
