package p2p

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/xssnick/gton/internal/extmsg"
	"github.com/xssnick/tonutils-go/address"
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

	addrKey, err := externalMessageDestinationAddress(data)
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

	now := time.Now()
	targets, err := n.externalMessageTargets(addrKey, data)
	if err != nil {
		return err
	}

	pending := make([]externalMessageTarget, 0, len(targets))
	for _, target := range targets {
		if n.processedExternalMessages.Seen(target.hash, now) || n.myExternalMessages.Seen(target.hash, now) {
			continue
		}
		pending = append(pending, target)
	}
	if len(pending) == 0 {
		return nil
	}
	if err = n.addExternalMessageAddressLimit(addrKey, now); err != nil {
		return err
	}

	queued := 0
	for _, target := range pending {
		req := rebroadcastRequest{
			subscription: target.sub,
			kind:         "tonNode.externalMessageBroadcast",
			payload:      append([]byte(nil), payload...),
			local:        true,
		}
		if !target.sub.enqueueRebroadcast(req) {
			continue
		}
		n.myExternalMessages.Mark(target.hash, now)
		queued++
	}
	if queued == 0 {
		n.dropExternalMessageAddressLimit(addrKey, now)
		return errors.New("local external message rebroadcast queues are full")
	}
	return nil
}

type externalMessageTarget struct {
	sub  *overlaySubscription
	hash string
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

func externalMessageDestinationAddress(data []byte) (extmsg.AddressKey, error) {
	root, err := cell.FromBOC(data)
	if err != nil {
		return extmsg.AddressKey{}, fmt.Errorf("parse external message BOC: %w", err)
	}

	loader, err := root.BeginParse()
	if err != nil {
		return extmsg.AddressKey{}, fmt.Errorf("begin parse external message: %w", err)
	}

	magic, err := loader.PreloadUInt(2)
	if err != nil || magic != 0b10 {
		return extmsg.AddressKey{}, errors.New("external message must begin with ext_in_msg_info$10")
	}
	var msg tlb.ExternalMessage
	if err = tlb.LoadFromCell(&msg, loader); err != nil {
		return extmsg.AddressKey{}, fmt.Errorf("parse external message: %w", err)
	}
	if msg.DstAddr == nil {
		return extmsg.AddressKey{}, errors.New("external message has no destination address")
	}
	if msg.DstAddr.Type() != address.StdAddress || msg.DstAddr.BitsLen() != 256 {
		return extmsg.AddressKey{}, errors.New("external message destination address is not a std 256-bit address")
	}
	return externalMessageAddressKey(msg.DstAddr), nil
}

func externalMessageAddressKey(addr *address.Address) extmsg.AddressKey {
	key := extmsg.AddressKey{Workchain: addr.Workchain()}
	copy(key.Account[:], addr.Data())
	return key
}

func (n *Node) addExternalMessageAddressLimit(key extmsg.AddressKey, now time.Time) error {
	if n.externalMessageLimiter == nil {
		n.externalMessageLimiter = extmsg.NewDefaultAddressLimiter()
	}
	err := n.externalMessageLimiter.Add(key, now)
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w %d:%x", err, key.Workchain, key.Account[:])
}

func (n *Node) dropExternalMessageAddressLimit(key extmsg.AddressKey, now time.Time) {
	if n.externalMessageLimiter == nil {
		return
	}
	n.externalMessageLimiter.Remove(key, now)
}
