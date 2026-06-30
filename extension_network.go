package gton

import (
	"context"
	"errors"

	"github.com/xssnick/gton/service/externalmsg"
	"github.com/xssnick/gton/service/p2p"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type extensionNetwork struct {
	node    *p2p.Node
	checker *externalmsg.Checker
}

func (n extensionNetwork) SendExternalMessage(ctx context.Context, body []byte, dst *address.Address) error {
	return n.node.SendExternalMessage(ctx, body, dst)
}

func (n extensionNetwork) SendCheckedExternalMessage(ctx context.Context, body []byte, dst *address.Address, root *cell.Cell, msg *tlb.ExternalMessage) error {
	return n.node.SendCheckedExternalMessage(ctx, body, dst, root, msg)
}

func (n extensionNetwork) CheckExternalMessage(ctx context.Context, body []byte, root *cell.Cell, msg *tlb.ExternalMessage) (externalmsg.CheckResult, error) {
	if n.checker == nil {
		return externalmsg.CheckResult{}, errors.New("external message checker is not configured")
	}
	if root != nil && msg != nil {
		return n.checker.Check(ctx, body, root, msg)
	}
	return n.checker.CheckBOC(ctx, body)
}
