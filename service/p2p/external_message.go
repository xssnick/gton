package p2p

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/xssnick/gton/internal/extmsg"
	"github.com/xssnick/gton/service/externalmsg"
	"github.com/xssnick/tonutils-go/address"
	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func (n *Node) SendExternalMessage(ctx context.Context, data []byte, dst *address.Address) error {
	if n.IsOffline() {
		return ErrOffline
	}
	addrKey, err := checkedExternalMessageAddressKey(dst)
	if err != nil {
		return err
	}
	return n.sendExternalMessage(ctx, data, addrKey, nil, nil, false, true)
}

func (n *Node) SendCheckedExternalMessage(ctx context.Context, data []byte, dst *address.Address, root *cell.Cell, msg *tlb.ExternalMessage) error {
	if n.IsOffline() {
		return ErrOffline
	}
	addrKey, err := checkedExternalMessageAddressKey(dst)
	if err != nil {
		return err
	}
	return n.sendExternalMessage(ctx, data, addrKey, root, msg, true, true)
}

func (n *Node) sendExternalMessage(ctx context.Context, data []byte, addrKey extmsg.AddressKey, root *cell.Cell, msg *tlb.ExternalMessage, checked bool, isLocal bool) error {
	if n.IsOffline() {
		return ErrOffline
	}
	if len(data) == 0 {
		return errors.New("external message is empty")
	}
	if len(data) > maxOverlayPayloadSize {
		return fmt.Errorf("external message is too large: %d", len(data))
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	payload, err := tl.Serialize(tonnodeapi.NewExternalMessageBroadcast{
		Message: tonnodeapi.ExternalMessage{Data: data},
	}, true)
	if err != nil {
		return fmt.Errorf("serialize external message broadcast: %w", err)
	}
	if len(payload) > maxOverlayPayloadSize {
		return fmt.Errorf("external message broadcast payload is too large: %d", len(payload))
	}

	now := time.Now()
	targets, err := n.externalMessageTargets(addrKey, data)
	if err != nil {
		return err
	}

	pending := make([]externalMessageTarget, 0, len(targets))
	for _, target := range targets {
		if !n.allowDuplicateExternals && n.externalMessageSeen(target.hash, now) {
			continue
		}
		pending = append(pending, target)
	}
	if len(pending) == 0 {
		return nil
	}

	ev := ExternalMessageEvent{
		IsLocal: isLocal,
		Body:    data,
		Root:    root,
		Message: msg,
	}

	if checked {
		err = n.acceptCheckedExternalMessage(ctx, ev)
	} else {
		err = n.acceptExternalMessage(ctx, ev)
	}
	if err != nil {
		return err
	}

	costBytes := externalBroadcastCostBytes(pending, len(payload))
	if err = n.externalBroadcastPacer.Wait(ctx, costBytes); err != nil {
		return err
	}
	return n.enqueueExternalMessageTargets(addrKey, pending, payload, time.Now())
}

func (n *Node) acceptExternalMessage(ctx context.Context, event ExternalMessageEvent) error {
	if n.externalMessageAdmission == nil {
		return nil
	}
	return n.externalMessageAdmission.AcceptExternalMessage(ctx, event)
}

func (n *Node) externalMessageSeen(hash string, now time.Time) bool {
	return n.processedExternalMessages.Seen(hash, now) || n.myExternalMessages.Seen(hash, now)
}

func (n *Node) acceptCheckedExternalMessage(ctx context.Context, event ExternalMessageEvent) error {
	if n.externalMessageAdmission == nil {
		return nil
	}
	return n.externalMessageAdmission.AcceptCheckedExternalMessage(ctx, event)
}

type externalMessageTarget struct {
	sub  *overlaySubscription
	hash string
}

func (n *Node) enqueueExternalMessageTargets(addrKey extmsg.AddressKey, targets []externalMessageTarget, payload []byte, now time.Time) error {
	if err := n.addExternalMessageAddressLimit(addrKey, now); err != nil {
		return err
	}

	queued := 0
	for _, target := range targets {
		req := rebroadcastRequest{
			subscription: target.sub,
			kind:         "tonNode.externalMessageBroadcast",
			payload:      payload,
			local:        true,
		}
		if !target.sub.enqueueRebroadcast(req) {
			continue
		}
		n.myExternalMessages.Mark(target.hash, now)
		queued++
	}
	if queued == 0 {
		n.externalMessageLimiter.Remove(addrKey, now)
		return errors.New("local external message rebroadcast queues are full")
	}
	return nil
}

func externalBroadcastCostBytes(targets []externalMessageTarget, payloadLen int) int64 {
	var cost int64
	for _, target := range targets {
		req := rebroadcastRequest{
			subscription: target.sub,
			kind:         "tonNode.externalMessageBroadcast",
			payloadSize:  payloadLen,
			local:        true,
		}
		cost += int64(payloadLen) * int64(target.sub.rebroadcastFanoutForRequest(req))
	}
	return cost
}

func (n *Node) externalMessageTargets(addrKey extmsg.AddressKey, data []byte) ([]externalMessageTarget, error) {
	customTargets, skipPublic := n.customExternalMessageTargets(addrKey, data)
	targets := make([]externalMessageTarget, 0, len(customTargets)+1)
	targets = append(targets, customTargets...)

	if !skipPublic {
		sub, err := n.subscriptionForWorkchain(addrKey.Workchain)
		if err != nil {
			return nil, err
		}
		targets = append(targets, externalMessageTarget{
			sub:  sub,
			hash: externalMessageFingerprint(sub.spec.ShortID, data),
		})
	}
	return targets, nil
}

func (n *Node) customExternalMessageTargets(addrKey extmsg.AddressKey, data []byte) ([]externalMessageTarget, bool) {
	leafShard := externalMessageLeafShard(addrKey)
	targets := make([]externalMessageTarget, 0)
	skipPublic := false
	for _, sub := range n.subscriptionsSnapshot() {
		if sub == nil || sub.spec.Kind != overlayKindCustomFixed || !sub.isActive() {
			continue
		}
		if _, ok := sub.spec.MsgSenders[n.localID]; !ok {
			continue
		}
		if !sub.spec.sendsShard(addrKey.Workchain, leafShard) {
			continue
		}
		targets = append(targets, externalMessageTarget{
			sub:  sub,
			hash: externalMessageFingerprint(sub.spec.ShortID, data),
		})
		if sub.spec.SkipPublicMsgSend {
			skipPublic = true
		}
	}
	return targets, skipPublic
}

func externalMessageLeafShard(key extmsg.AddressKey) int64 {
	return int64(binary.BigEndian.Uint64(key.Account[:8]) | 1)
}

func (n *Node) subscriptionForWorkchain(workchain int32) (*overlaySubscription, error) {
	if len(n.zeroStateFileHash) == 0 {
		return nil, errors.New("node is not started")
	}

	spec, err := buildOverlaySpec(n.zeroStateFileHash, workchain, topShard, overlayName(workchain, topShard))
	if err != nil {
		return nil, err
	}

	sub, err := n.getOrCreateSubscription(spec)
	if err != nil {
		return nil, err
	}
	sub.setActive(true, time.Time{})
	n.startSubscription(sub)
	return sub, nil
}

type parsedExternalMessage struct {
	root    *cell.Cell
	message *tlb.ExternalMessage
	address extmsg.AddressKey
}

func parseExternalMessageData(data []byte) (parsedExternalMessage, error) {
	root, message, err := externalmsg.ParseMessage(data)
	if err != nil {
		return parsedExternalMessage{}, err
	}
	return parsedExternalMessage{
		root:    root,
		message: message,
		address: externalmsg.AddressKey(message.DstAddr),
	}, nil
}

func checkedExternalMessageAddressKey(addr *address.Address) (extmsg.AddressKey, error) {
	if addr == nil {
		return extmsg.AddressKey{}, errors.New("external message destination address is nil")
	}
	if addr.Type() != address.StdAddress || addr.BitsLen() != 256 {
		return extmsg.AddressKey{}, errors.New("external message destination address is not a std 256-bit address")
	}
	return externalmsg.AddressKey(addr), nil
}

func (n *Node) addExternalMessageAddressLimit(key extmsg.AddressKey, now time.Time) error {
	err := n.externalMessageLimiter.Add(key, now)
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w %d:%x", err, key.Workchain, key.Account[:])
}
