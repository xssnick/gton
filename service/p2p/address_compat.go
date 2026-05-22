package p2p

import (
	"context"
	"crypto/ed25519"
	"encoding/binary"
	"fmt"
	"strings"

	adnladdr "github.com/xssnick/tonutils-go/adnl/address"
	"github.com/xssnick/tonutils-go/adnl/dht"
	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/tl"
)

func init() {
	tl.Register(CompatAddressTCP{}, "adnl.address.tcp ip:int port:int = adnl.Address")
	tl.Register(CompatAddressTCP6{}, "adnl.address.tcp6 ip:int128 port:int = adnl.Address")
	tl.Register(CompatAddressTunnel{}, "adnl.address.tunnel to:int256 pubkey:PublicKey = adnl.Address")
	tl.Register(CompatAddressReverse{}, "adnl.address.reverse = adnl.Address")
	tl.Register(CompatAddressQUIC{}, "adnl.address.quic ip:int port:int = adnl.Address")
	tl.Register(CompatAddressList{}, "")
}

type CompatAddressTCP struct {
	IP   []byte `tl:"int"`
	Port int32  `tl:"int"`
}

type CompatAddressTCP6 struct {
	IP   []byte `tl:"int128"`
	Port int32  `tl:"int"`
}

type CompatAddressTunnel struct {
	To     []byte `tl:"int256"`
	PubKey any    `tl:"struct boxed [pub.ed25519,pub.aes,pub.unenc,pub.overlay]"`
}

type CompatAddressReverse struct{}

type CompatAddressQUIC struct {
	IP   []byte `tl:"int"`
	Port int32  `tl:"int"`
}

type CompatAddressList struct {
	Addresses  []any `tl:"vector struct boxed [adnl.address.udp,adnl.address.udp6,adnl.address.tcp,adnl.address.tcp6,adnl.address.tunnel,adnl.address.reverse,adnl.address.quic]"`
	Version    int32 `tl:"int"`
	ReinitDate int32 `tl:"int"`
	Priority   int32 `tl:"int"`
	ExpireAt   int32 `tl:"int"`
}

var compatAddressListID = tl.CRC("adnl.addressList addrs:(vector adnl.Address) version:int reinit_date:int priority:int expire_at:int = adnl.AddressList")

func findPeerAddresses(ctx context.Context, client dhtBackend, key []byte) (*adnladdr.List, ed25519.PublicKey, error) {
	list, pub, err := client.FindAddresses(ctx, key)
	if err == nil {
		return list, pub, nil
	}
	if !strings.Contains(err.Error(), "failed to parse address list") && !strings.Contains(err.Error(), "struct id") {
		return nil, nil, err
	}

	// Some public nodes still publish address lists with constructors that are
	// not exposed by tonutils-go. Keep this fallback as an explicit network
	// protocol boundary: it accepts those older records but only returns UDP
	// addresses that the current node can actually dial.
	value, _, findErr := client.FindValue(ctx, &dht.Key{
		ID:    key,
		Name:  []byte("address"),
		Index: 0,
	})
	if findErr != nil {
		return nil, nil, findErr
	}

	compat, parseErr := parseCompatibleAddressList(value.Data)
	if parseErr != nil {
		return nil, nil, parseErr
	}

	pubKey, ok := value.KeyDescription.ID.(keys.PublicKeyED25519)
	if !ok {
		return nil, nil, fmt.Errorf("unsupported key type %T", value.KeyDescription.ID)
	}
	return compat, pubKey.Key, nil
}

func parseCompatibleAddressList(data []byte) (*adnladdr.List, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("address list payload is too short")
	}
	if binary.LittleEndian.Uint32(data[:4]) != compatAddressListID {
		return nil, fmt.Errorf("unexpected address list constructor")
	}

	var raw CompatAddressList
	if _, err := tl.Parse(&raw, data[4:], false); err != nil {
		return nil, err
	}

	list := &adnladdr.List{
		Version:    raw.Version,
		ReinitDate: raw.ReinitDate,
		Priority:   raw.Priority,
		ExpireAt:   raw.ExpireAt,
	}
	for _, addr := range raw.Addresses {
		switch typed := addr.(type) {
		case adnladdr.UDP:
			list.Addresses = append(list.Addresses, typed)
		case *adnladdr.UDP:
			if typed != nil {
				list.Addresses = append(list.Addresses, typed)
			}
		case adnladdr.UDP6:
			list.Addresses = append(list.Addresses, typed)
		case *adnladdr.UDP6:
			if typed != nil {
				list.Addresses = append(list.Addresses, typed)
			}
		}
	}
	if len(list.Addresses) == 0 {
		return nil, fmt.Errorf("no supported UDP addresses in address list")
	}
	return list, nil
}
