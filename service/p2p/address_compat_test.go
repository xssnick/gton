package p2p

import (
	"encoding/binary"
	"net"
	"testing"

	adnladdr "github.com/xssnick/tonutils-go/adnl/address"
	"github.com/xssnick/tonutils-go/tl"
)

func TestParseCompatibleAddressListKeepsSupportedAddresses(t *testing.T) {
	body, err := tl.Serialize(CompatAddressList{
		Addresses: []any{
			&adnladdr.UDP{
				IP:   net.ParseIP("127.0.0.1").To4(),
				Port: 30303,
			},
			&adnladdr.QUIC{
				IP:   net.ParseIP("127.0.0.1").To4(),
				Port: 31303,
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

	if len(parsed.Addresses) != 2 {
		t.Fatalf("expected 2 supported addresses, got %d", len(parsed.Addresses))
	}
	if got := adnladdr.PortValue(parsed.Addresses[0]); got != 30303 {
		t.Fatalf("unexpected parsed UDP port %d", got)
	}
	if _, ok := parsed.Addresses[1].(adnladdr.QUIC); !ok {
		t.Fatalf("unexpected parsed QUIC address type %T", parsed.Addresses[1])
	}
	if got := adnladdr.PortValue(parsed.Addresses[1]); got != 31303 {
		t.Fatalf("unexpected parsed QUIC port %d", got)
	}
	if parsed.Version != 1 || parsed.ReinitDate != 2 || parsed.Priority != 3 || parsed.ExpireAt != 4 {
		t.Fatalf("address list metadata was not preserved: %+v", parsed)
	}
}

func TestNativeQUICAddressRegistrationIsPreserved(t *testing.T) {
	wire, err := tl.Serialize(adnladdr.List{
		Addresses: []adnladdr.Address{
			&adnladdr.QUIC{
				IP:   net.ParseIP("192.0.2.1").To4(),
				Port: 31303,
			},
		},
	}, true)
	if err != nil {
		t.Fatalf("serialize native address list: %v", err)
	}

	var parsed adnladdr.List
	if _, err = tl.Parse(&parsed, wire, true); err != nil {
		t.Fatalf("parse native address list: %v", err)
	}
	if len(parsed.Addresses) != 1 {
		t.Fatalf("parsed address count = %d, want 1", len(parsed.Addresses))
	}
	if _, ok := parsed.Addresses[0].(adnladdr.QUIC); !ok {
		t.Fatalf("parsed address type = %T, want address.QUIC", parsed.Addresses[0])
	}
}

func TestParseCompatibleAddressListRejectsTrailingData(t *testing.T) {
	body, err := tl.Serialize(CompatAddressList{
		Addresses: []any{
			&adnladdr.UDP{
				IP:   net.ParseIP("127.0.0.1").To4(),
				Port: 30303,
			},
		},
	}, false)
	if err != nil {
		t.Fatalf("serialize compat address list: %v", err)
	}

	wire := make([]byte, 4, 5+len(body))
	binary.LittleEndian.PutUint32(wire, compatAddressListID)
	wire = append(wire, body...)
	wire = append(wire, 0)

	if _, err = parseCompatibleAddressList(wire); err == nil {
		t.Fatal("parseCompatibleAddressList accepted trailing data")
	}
}
