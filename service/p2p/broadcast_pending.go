package p2p

import (
	"context"
	"fmt"
	"time"

	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	pendingBroadcastDecodeTTL      = 3 * time.Minute
	pendingBroadcastDecodeMaxItems = 1024
	pendingBroadcastDecodeMaxBytes = int64(64 << 20)
	pendingBroadcastDecodeOverhead = 256
)

type pendingBlockBroadcastDecode struct {
	fingerprint  string
	overlay      string
	delivery     Delivery
	trusted      bool
	kind         string
	block        ton.BlockIDExt
	prev         ton.BlockIDExt
	sourcePeerID PeerID
	receivedAt   time.Time
	msg          any
	proofRoot    *cell.Cell
	expiresAt    time.Time
	bytes        int64
}

func (n *Node) schedulePendingBlockBroadcastDecode(req pendingBlockBroadcastDecode) {
	if n == nil || req.fingerprint == "" || req.msg == nil {
		return
	}

	now := time.Now()
	req.expiresAt = now.Add(pendingBroadcastDecodeTTL)
	req.bytes = pendingBlockBroadcastDecodeBytes(req)

	n.pendingBroadcastMx.Lock()
	if n.pendingBroadcasts == nil {
		n.pendingBroadcasts = map[string]pendingBlockBroadcastDecode{}
	}
	n.prunePendingBlockBroadcastDecodesLocked(now)
	if old, ok := n.pendingBroadcasts[req.fingerprint]; ok {
		n.pendingBroadcastBytes -= old.bytes
	}
	n.pendingBroadcasts[req.fingerprint] = req
	n.pendingBroadcastBytes += req.bytes
	n.prunePendingBlockBroadcastOverflowLocked()
	n.pendingBroadcastMx.Unlock()
}

func (n *Node) forgetPendingBlockBroadcastDecode(fingerprint string) {
	n.pendingBroadcastMx.Lock()
	n.deletePendingBlockBroadcastDecodeLocked(fingerprint)
	n.pendingBroadcastMx.Unlock()
}

func (n *Node) processPendingBlockBroadcastDecodesAsync() {
	if n == nil {
		return
	}

	n.pendingBroadcastMx.Lock()
	if len(n.pendingBroadcasts) == 0 {
		n.pendingBroadcastMx.Unlock()
		return
	}
	if n.pendingBroadcastProcessing {
		n.pendingBroadcastProcessAgain = true
		n.pendingBroadcastMx.Unlock()
		return
	}
	n.pendingBroadcastProcessing = true
	n.pendingBroadcastMx.Unlock()

	n.runAsync(func() {
		ctx := n.runCtx
		if ctx == nil {
			ctx = context.Background()
		}

		for {
			n.processPendingBlockBroadcastDecodes(ctx, time.Now())

			n.pendingBroadcastMx.Lock()
			processAgain := n.pendingBroadcastProcessAgain && len(n.pendingBroadcasts) > 0 && ctx.Err() == nil
			n.pendingBroadcastProcessAgain = false
			if !processAgain {
				n.pendingBroadcastProcessing = false
				n.pendingBroadcastMx.Unlock()
				return
			}
			n.pendingBroadcastMx.Unlock()
		}
	})
}

func (n *Node) processPendingBlockBroadcastDecodes(ctx context.Context, now time.Time) {
	reqs := n.pendingBlockBroadcastDecodeSnapshot(now)
	for _, req := range reqs {
		if ctx.Err() != nil {
			return
		}
		if !n.canAcceptBroadcast(req.kind, false) {
			continue
		}

		downloaded, err := n.decodePendingBlockBroadcast(ctx, req)
		if err != nil {
			if isBroadcastDecompressionStateNotReady(err) {
				continue
			}
			n.forgetPendingBlockBroadcastDecode(req.fingerprint)
			n.log.Debug().
				Err(err).
				Str("block", formatBlockRef(req.block)).
				Str("kind", req.kind).
				Msg("dropping pending block broadcast because payload decode failed")
			continue
		}

		n.forgetPendingBlockBroadcastDecode(req.fingerprint)
		downloaded.SourcePeerID = req.sourcePeerID
		accepted := acceptedBroadcast{
			fingerprint:        req.fingerprint,
			deduped:            true,
			skipAcceptedMetric: true,
			event: &BroadcastEvent{
				Overlay:      req.overlay,
				Delivery:     req.delivery,
				Trusted:      req.trusted,
				Kind:         req.kind,
				Block:        cloneBlockID(req.block),
				SourcePeerID: req.sourcePeerID,
				ReceivedAt:   req.receivedAt,
				Downloaded:   downloaded,
			},
		}
		n.acceptBroadcast(accepted)
	}
}

func (n *Node) decodePendingBlockBroadcast(ctx context.Context, req pendingBlockBroadcastDecode) (*DownloadedBlock, error) {
	switch data := req.msg.(type) {
	case tonnodeapi.BlockBroadcastCompressedV2:
		if req.proofRoot == nil {
			return nil, fmt.Errorf("pending compressed V2 broadcast %s has no parsed proof root", formatBlockRef(req.block))
		}
		downloaded, _, err := n.decodeBlockBroadcastCompressedV2WithProofRoot(ctx, data, req.proofRoot, req.prev)
		return downloaded, err
	default:
		return nil, fmt.Errorf("unexpected pending block broadcast %T", req.msg)
	}
}

func (n *Node) pendingBlockBroadcastDecodeSnapshot(now time.Time) []pendingBlockBroadcastDecode {
	n.pendingBroadcastMx.Lock()
	defer n.pendingBroadcastMx.Unlock()

	n.prunePendingBlockBroadcastDecodesLocked(now)
	if len(n.pendingBroadcasts) == 0 {
		return nil
	}

	reqs := make([]pendingBlockBroadcastDecode, 0, len(n.pendingBroadcasts))
	for _, req := range n.pendingBroadcasts {
		reqs = append(reqs, req)
	}
	return reqs
}

func (n *Node) prunePendingBlockBroadcastDecodesLocked(now time.Time) {
	for key, req := range n.pendingBroadcasts {
		if !req.expiresAt.After(now) {
			n.deletePendingBlockBroadcastDecodeLocked(key)
			n.deduper.Forget(key)
		}
	}
}

func (n *Node) prunePendingBlockBroadcastOverflowLocked() {
	for len(n.pendingBroadcasts) > pendingBroadcastDecodeMaxItems || n.pendingBroadcastBytes > pendingBroadcastDecodeMaxBytes {
		var oldestKey string
		var oldestAt time.Time
		for key, req := range n.pendingBroadcasts {
			if oldestKey == "" || req.receivedAt.Before(oldestAt) {
				oldestKey = key
				oldestAt = req.receivedAt
			}
		}
		if oldestKey == "" {
			return
		}
		n.deletePendingBlockBroadcastDecodeLocked(oldestKey)
		n.deduper.Forget(oldestKey)
	}
}

func (n *Node) deletePendingBlockBroadcastDecodeLocked(fingerprint string) {
	req, ok := n.pendingBroadcasts[fingerprint]
	if !ok {
		return
	}
	delete(n.pendingBroadcasts, fingerprint)
	n.pendingBroadcastBytes -= req.bytes
	if n.pendingBroadcastBytes < 0 {
		n.pendingBroadcastBytes = 0
	}
}

func pendingBlockBroadcastDecodeBytes(req pendingBlockBroadcastDecode) int64 {
	switch msg := req.msg.(type) {
	case tonnodeapi.BlockBroadcastCompressedV2:
		return int64(len(msg.Proof)+len(msg.DataCompressed)) + pendingBroadcastDecodeOverhead
	default:
		return pendingBroadcastDecodeOverhead
	}
}
