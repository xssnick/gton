package gton

import (
	"context"
	"errors"
	"fmt"

	"github.com/xssnick/gton/internal/extmsg"
	"github.com/xssnick/gton/service/blocksync"
	"github.com/xssnick/gton/service/externalmsg"
	"github.com/xssnick/gton/service/p2p"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type externalMessageNetwork struct {
	node      *p2p.Node
	checker   *externalmsg.Checker
	blockSync *blocksync.Service
}

func (n externalMessageNetwork) SendExternalMessage(ctx context.Context, body []byte, dst *address.Address) error {
	return externalMessageNetworkError(n.node.SendExternalMessage(ctx, body, dst))
}

func (n externalMessageNetwork) SendCheckedExternalMessage(ctx context.Context, body []byte, dst *address.Address, root *cell.Cell, msg *tlb.ExternalMessage) error {
	return externalMessageNetworkError(n.node.SendCheckedExternalMessage(ctx, body, dst, root, msg))
}

func externalMessageNetworkError(err error) error {
	if !errors.Is(err, p2p.ErrOffline) {
		return err
	}
	// Keep the concrete transport sentinel out of the public network error
	// chain; callers should depend only on the external-message boundary.
	return fmt.Errorf("%w: %v", extmsg.ErrNetworkOffline, err)
}

func (n externalMessageNetwork) CheckExternalMessage(ctx context.Context, body []byte, root *cell.Cell, msg *tlb.ExternalMessage) (externalmsg.CheckResult, error) {
	if root != nil && msg != nil {
		return n.checker.Check(ctx, body, root, msg)
	}
	return n.checker.CheckBOC(ctx, body)
}

func (n externalMessageNetwork) SubmitBlockLocally(block p2p.DownloadedBlock) {
	n.blockSync.SubmitBlockLocally(block)
}
