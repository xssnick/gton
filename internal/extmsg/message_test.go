package extmsg

import (
	"bytes"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestParseMessage(t *testing.T) {
	addr := address.MustParseRawAddr("0:b113a994b5024a16719f69139328eb759596c38a25f59028b146fecdc3621dfe")
	root, err := tlb.ToCell(&tlb.ExternalMessage{
		DstAddr:   addr,
		ImportFee: tlb.ZeroCoins,
		Body:      cell.BeginCell().EndCell(),
	})
	if err != nil {
		t.Fatalf("build external message: %v", err)
	}
	body := root.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false})

	parsedRoot, msg, err := ParseMessage(body)
	if err != nil {
		t.Fatalf("parse external message: %v", err)
	}
	if parsedRoot.HashKey() != root.HashKey() {
		t.Fatal("parsed root hash mismatch")
	}
	if msg.DstAddr == nil || !bytes.Equal(msg.DstAddr.Data(), addr.Data()) {
		t.Fatalf("parsed destination = %v, want %v", msg.DstAddr, addr)
	}
	if got := AddressKeyFor(msg.DstAddr); got.Workchain != 0 || got.Account != AddressKeyFor(addr).Account {
		t.Fatalf("address key = %+v", got)
	}
}

func TestParseMessageRejectsInvalidWireData(t *testing.T) {
	tests := []struct {
		name string
		body []byte
	}{
		{
			name: "empty body",
		},
		{
			name: "non external message cell",
			body: cell.BeginCell().EndCell().ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false}),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, _, err := ParseMessage(test.body); err == nil {
				t.Fatal("expected parse error")
			}
		})
	}
}
