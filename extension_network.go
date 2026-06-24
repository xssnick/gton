package gton

import (
	"context"
	"errors"

	"github.com/xssnick/gton/internal/extmsg"
	"github.com/xssnick/gton/service/p2p"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type extensionNetwork struct {
	node *p2p.Node
}

func (n extensionNetwork) SendExternalMessage(ctx context.Context, body []byte, dst *address.Address) error {
	key, err := extensionExternalMessageAddressKey(dst)
	if err != nil {
		return err
	}
	return n.node.SendExternalMessage(ctx, body, key)
}

func (n extensionNetwork) SendCheckedExternalMessage(ctx context.Context, body []byte, dst *address.Address, root *cell.Cell, msg *tlb.ExternalMessage) error {
	key, err := extensionExternalMessageAddressKey(dst)
	if err != nil {
		return err
	}
	return n.node.SendCheckedExternalMessage(ctx, body, key, root, msg)
}

func extensionExternalMessageAddressKey(addr *address.Address) (extmsg.AddressKey, error) {
	if addr == nil {
		return extmsg.AddressKey{}, errors.New("external message destination address is nil")
	}
	if addr.Type() != address.StdAddress || addr.BitsLen() != 256 {
		return extmsg.AddressKey{}, errors.New("external message destination address is not a std 256-bit address")
	}

	key := extmsg.AddressKey{Workchain: addr.Workchain()}
	copy(key.Account[:], addr.Data())
	return key, nil
}
