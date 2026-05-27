package p2p

import (
	"context"
	"time"

	"github.com/xssnick/tonutils-go/ton"
)

const (
	pendingBroadcastDecodeTTL   = 10 * time.Second
	pendingBroadcastDecodeDelay = 250 * time.Millisecond
)

type pendingBlockBroadcastDecode struct {
	fingerprint string
	overlay     string
	delivery    Delivery
	trusted     bool
	kind        string
	block       ton.BlockIDExt
	sourceKey   string
	receivedAt  time.Time
	msg         any
}

func (n *Node) schedulePendingBlockBroadcastDecode(req pendingBlockBroadcastDecode) {
	if n == nil || req.fingerprint == "" || req.msg == nil {
		return
	}

	n.pendingBroadcastMx.Lock()
	if n.pendingBroadcasts == nil {
		n.pendingBroadcasts = map[string]struct{}{}
	}
	if _, ok := n.pendingBroadcasts[req.fingerprint]; ok {
		n.pendingBroadcastMx.Unlock()
		return
	}
	n.pendingBroadcasts[req.fingerprint] = struct{}{}
	n.pendingBroadcastMx.Unlock()

	n.runAsync(func() {
		defer n.forgetPendingBlockBroadcastDecode(req.fingerprint)

		ctx := n.runCtx
		if ctx == nil {
			ctx = context.Background()
		}
		n.retryPendingBlockBroadcastDecode(ctx, req)
	})
}

func (n *Node) forgetPendingBlockBroadcastDecode(fingerprint string) {
	n.pendingBroadcastMx.Lock()
	delete(n.pendingBroadcasts, fingerprint)
	n.pendingBroadcastMx.Unlock()
}

func (n *Node) retryPendingBlockBroadcastDecode(ctx context.Context, req pendingBlockBroadcastDecode) {
	deadline := time.NewTimer(pendingBroadcastDecodeTTL)
	defer deadline.Stop()

	delay := time.NewTicker(pendingBroadcastDecodeDelay)
	defer delay.Stop()

	stateReady := n.compressedBlockStateReadyNotify()
	for {
		downloaded, err := n.decodeBroadcastBlock(ctx, req.msg)
		if err == nil {
			accepted := acceptedBroadcast{
				fingerprint:        req.fingerprint,
				deduped:            true,
				skipAcceptedMetric: true,
				event: &BroadcastEvent{
					Overlay:    req.overlay,
					Delivery:   req.delivery,
					Trusted:    req.trusted,
					Kind:       req.kind,
					Block:      cloneBlockID(req.block),
					SourceKey:  req.sourceKey,
					ReceivedAt: req.receivedAt,
					Downloaded: downloaded,
				},
			}
			n.acceptBroadcast(accepted)
			return
		}
		if !isBroadcastDecompressionStateNotReady(err) {
			n.log.Debug().
				Err(err).
				Str("block", formatBlockRef(req.block)).
				Str("kind", req.kind).
				Msg("dropping pending block broadcast because payload decode failed")
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-deadline.C:
			n.log.Debug().
				Str("block", formatBlockRef(req.block)).
				Str("kind", req.kind).
				Msg("dropping pending block broadcast because previous state did not arrive")
			return
		case <-stateReady:
			stateReady = n.compressedBlockStateReadyNotify()
		case <-delay.C:
		}
	}
}
