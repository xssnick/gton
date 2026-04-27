package p2p

import (
	"encoding/binary"
	"net"
	"testing"

	adnladdr "github.com/xssnick/tonutils-go/adnl/address"
	"github.com/xssnick/tonutils-go/tl"
)

func TestParseCompatibleAddressListSkipsUnsupportedAddresses(t *testing.T) {
	body, err := tl.Serialize(CompatAddressList{
		Addresses: []any{
			&adnladdr.UDP{
				IP:   net.ParseIP("127.0.0.1").To4(),
				Port: 30303,
			},
			CompatAddressQUIC{
				IP:   net.ParseIP("127.0.0.1").To4(),
				Port: 30304,
			},
			CompatAddressReverse{},
		},
		Version:    1,
		ReinitDate: 2,
		Priority:   3,
		ExpireAt:   4,
	}, false)
	if err != nil {
		t.Fatalf("serialize compat address list: %v", err)
	}
	payload := make([]byte, 4+len(body))
	binary.LittleEndian.PutUint32(payload[:4], compatAddressListID)
	copy(payload[4:], body)

	var standard adnladdr.List
	if _, err = tl.Parse(&standard, payload, true); err == nil {
		t.Fatalf("expected standard parser to fail on unsupported address types")
	}

	parsed, err := parseCompatibleAddressList(payload)
	if err != nil {
		t.Fatalf("parse compat address list: %v", err)
	}

	if len(parsed.Addresses) != 1 {
		t.Fatalf("expected 1 supported address, got %d", len(parsed.Addresses))
	}
	if got := adnladdr.PortValue(parsed.Addresses[0]); got != 30303 {
		t.Fatalf("unexpected parsed port %d", got)
	}
	if parsed.Version != 1 || parsed.ReinitDate != 2 || parsed.Priority != 3 || parsed.ExpireAt != 4 {
		t.Fatalf("address list metadata was not preserved: %+v", parsed)
	}
}
