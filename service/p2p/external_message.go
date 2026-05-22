package p2p

import (
	"context"
	"errors"
	"fmt"
	"time"

	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func (s *overlaySubscription) serveSendExtMessage(ctx context.Context, msg tonnodeapi.ExternalMessage) (tl.Serializable, error) {
	if err := s.node.SendExternalMessage(ctx, msg.Data); err != nil {
		return nil, err
	}
	return Success{}, nil
}

func (n *Node) SendExternalMessage(ctx context.Context, data []byte) error {
	if len(data) == 0 {
		return errors.New("external message is empty")
	}
	if len(data) > maxOverlayPayloadSize {
		return fmt.Errorf("external message is too large: %d", len(data))
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	workchain, err := externalMessageDestinationWorkchain(data)
	if err != nil {
		return err
	}

	sub, err := n.subscriptionForWorkchain(workchain)
	if err != nil {
		return err
	}

	payload, err := tl.Serialize(tonnodeapi.NewExternalMessageBroadcast{
		Message: tonnodeapi.ExternalMessage{Data: append([]byte(nil), data...)},
	}, true)
	if err != nil {
		return fmt.Errorf("serialize external message broadcast: %w", err)
	}
	if len(payload) > maxOverlayPayloadSize {
		return fmt.Errorf("external message broadcast payload is too large: %d", len(payload))
	}

	fingerprint := broadcastFingerprint(sub.spec.ShortID, payload)
	if !n.deduper.Mark(fingerprint, time.Now()) {
		return nil
	}

	req := rebroadcastRequest{
		subscription: sub,
		kind:         "tonNode.externalMessageBroadcast",
		payload:      payload,
	}
	if n.allowRebroadcast(&req) {
		if !n.localRebroadcastQueue.Push(req) {
			return errors.New("local external message rebroadcast queue is full")
		}
	}
	return nil
}

func (n *Node) subscriptionForWorkchain(workchain int32) (*overlaySubscription, error) {
	if n.runCtx == nil || len(n.zeroStateFileHash) == 0 {
		return nil, errors.New("node is not started")
	}

	spec, err := buildOverlaySpec(n.zeroStateFileHash, workchain, topShard, overlayName(workchain, topShard))
	if err != nil {
		return nil, err
	}

	sub, _ := n.getOrCreateSubscription(spec)
	sub.setActive(true, time.Time{})
	n.startSubscription(sub)
	return sub, nil
}

func externalMessageDestinationWorkchain(data []byte) (int32, error) {
	root, err := cell.FromBOC(data)
	if err != nil {
		return 0, fmt.Errorf("parse external message BOC: %w", err)
	}

	loader, err := root.BeginParse()
	if err != nil {
		return 0, fmt.Errorf("begin parse external message: %w", err)
	}

	var msg tlb.ExternalMessage
	if err = tlb.LoadFromCell(&msg, loader); err != nil {
		return 0, fmt.Errorf("parse external message: %w", err)
	}
	if msg.DstAddr == nil {
		return 0, errors.New("external message has no destination address")
	}
	return msg.DstAddr.Workchain(), nil
}
