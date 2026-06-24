package p2p

import (
	"context"
	"fmt"
	"time"

	tnstore "github.com/xssnick/gton/service/storage"

	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	pendingBroadcastDecodeTTL      = 3 * time.Minute
	pendingBroadcastDecodeMaxItems = 1024
	pendingBroadcastDecodeMaxBytes = int64(256 << 20)
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
	rebroadcast  *rebroadcastRequest
	expiresAt    time.Time
	bytes        int64
}

func (n *Node) schedulePendingBlockBroadcastDecode(req pendingBlockBroadcastDecode) {
	if req.fingerprint == "" || req.msg == nil {
		return
	}

	now := time.Now()
	req.expiresAt = now.Add(pendingBroadcastDecodeTTL)
	req.bytes = pendingBlockBroadcastDecodeBytes(req)

	n.pendingBroadcastMx.Lock()
	if n.pendingBroadcasts == nil {
		n.pendingBroadcasts = map[string]pendingBlockBroadcastDecode{}
	}
	if n.pendingBroadcastByPrev == nil {
		n.pendingBroadcastByPrev = map[tnstore.BlockRootHash]map[string]struct{}{}
	}
	n.prunePendingBlockBroadcastDecodesLocked(now)
	if old, ok := n.pendingBroadcasts[req.fingerprint]; ok {
		n.deletePendingBlockBroadcastDecodeLocked(old.fingerprint)
	}
	n.pendingBroadcasts[req.fingerprint] = req
	n.addPendingBlockBroadcastPrevIndexLocked(req)
	n.pendingBroadcastBytes += req.bytes
	n.prunePendingBlockBroadcastOverflowLocked()
	n.pendingBroadcastMx.Unlock()

	n.processPendingBlockBroadcastDecodesForPrevAsync(req.prev)
}

func (n *Node) forgetPendingBlockBroadcastDecode(fingerprint string) {
	n.pendingBroadcastMx.Lock()
	n.deletePendingBlockBroadcastDecodeLocked(fingerprint)
	n.pendingBroadcastMx.Unlock()
}

func (n *Node) processPendingBlockBroadcastDecodesForPrevAsync(prev ton.BlockIDExt) {
	if len(prev.RootHash) != 32 {
		return
	}

	key := tnstore.BlockKey(prev)

	n.pendingBroadcastMx.Lock()
	if len(n.pendingBroadcastByPrev[key]) == 0 {
		n.pendingBroadcastMx.Unlock()
		return
	}
	if n.pendingBroadcastReadyPrev == nil {
		n.pendingBroadcastReadyPrev = map[tnstore.BlockRootHash]ton.BlockIDExt{}
	}
	n.pendingBroadcastReadyPrev[key] = prev
	if n.pendingBroadcastProcessing {
		n.pendingBroadcastMx.Unlock()
		return
	}
	n.pendingBroadcastProcessing = true
	n.pendingBroadcastMx.Unlock()

	n.runPendingBlockBroadcastDecodeProcessorAsync()
}

func (n *Node) runPendingBlockBroadcastDecodeProcessorAsync() {
	n.runAsync(func() {
		ctx := n.runtimeContext()

		for {
			n.pendingBroadcastMx.Lock()
			prevs := make([]ton.BlockIDExt, 0, len(n.pendingBroadcastReadyPrev))
			for key, prev := range n.pendingBroadcastReadyPrev {
				prevs = append(prevs, prev)
				delete(n.pendingBroadcastReadyPrev, key)
			}
			done := len(prevs) == 0 || len(n.pendingBroadcasts) == 0 || ctx.Err() != nil
			if done {
				n.pendingBroadcastProcessing = false
				n.pendingBroadcastMx.Unlock()
				return
			}
			n.pendingBroadcastMx.Unlock()

			now := time.Now()
			for _, prev := range prevs {
				if ctx.Err() != nil {
					break
				}
				n.processPendingBlockBroadcastDecodesForPrev(ctx, prev, now)
			}
		}
	})
}

func (n *Node) processPendingBlockBroadcastDecodesForPrev(ctx context.Context, prev ton.BlockIDExt, now time.Time) {
	reqs := n.pendingBlockBroadcastDecodeSnapshotForPrev(prev, now)
	n.processPendingBlockBroadcastDecodeRequests(ctx, reqs)
}

func (n *Node) processPendingBlockBroadcastDecodeRequests(ctx context.Context, reqs []pendingBlockBroadcastDecode) {
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
		rebroadcast := pendingBlockBroadcastRebroadcast(req.rebroadcast)
		block := cloneBlockID(req.block)
		accepted := acceptedBroadcast{
			fingerprint:        req.fingerprint,
			deduped:            true,
			skipAcceptedMetric: true,
			block:              &block,
			rebroadcast:        rebroadcast,
			event: &BroadcastEvent{
				Overlay:      req.overlay,
				Delivery:     req.delivery,
				Trusted:      req.trusted,
				Kind:         req.kind,
				Block:        block,
				SourcePeerID: req.sourcePeerID,
				ReceivedAt:   req.receivedAt,
				Downloaded:   downloaded,
			},
		}
		n.acceptBroadcast(accepted)
	}
}

func pendingBlockBroadcastRebroadcast(req *rebroadcastRequest) *rebroadcastRequest {
	if req == nil {
		return nil
	}

	rebroadcast := *req
	rebroadcast.skipOverlayRebroadcast = true
	return &rebroadcast
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

func (n *Node) pendingBlockBroadcastDecodeSnapshotForPrev(prev ton.BlockIDExt, now time.Time) []pendingBlockBroadcastDecode {
	n.pendingBroadcastMx.Lock()
	defer n.pendingBroadcastMx.Unlock()

	n.prunePendingBlockBroadcastDecodesLocked(now)
	if len(n.pendingBroadcasts) == 0 {
		return nil
	}

	fingerprints := n.pendingBroadcastByPrev[tnstore.BlockKey(prev)]
	if len(fingerprints) == 0 {
		return nil
	}

	reqs := make([]pendingBlockBroadcastDecode, 0, len(fingerprints))
	for fingerprint := range fingerprints {
		req, ok := n.pendingBroadcasts[fingerprint]
		if ok {
			reqs = append(reqs, req)
		}
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
	n.deletePendingBlockBroadcastPrevIndexLocked(req)
	n.pendingBroadcastBytes -= req.bytes
	if n.pendingBroadcastBytes < 0 {
		n.pendingBroadcastBytes = 0
	}
}

func (n *Node) addPendingBlockBroadcastPrevIndexLocked(req pendingBlockBroadcastDecode) {
	key := tnstore.BlockKey(req.prev)
	if n.pendingBroadcastByPrev == nil {
		n.pendingBroadcastByPrev = map[tnstore.BlockRootHash]map[string]struct{}{}
	}
	fingerprints := n.pendingBroadcastByPrev[key]
	if fingerprints == nil {
		fingerprints = map[string]struct{}{}
		n.pendingBroadcastByPrev[key] = fingerprints
	}
	fingerprints[req.fingerprint] = struct{}{}
}

func (n *Node) deletePendingBlockBroadcastPrevIndexLocked(req pendingBlockBroadcastDecode) {
	key := tnstore.BlockKey(req.prev)
	fingerprints := n.pendingBroadcastByPrev[key]
	if len(fingerprints) == 0 {
		return
	}
	delete(fingerprints, req.fingerprint)
	if len(fingerprints) == 0 {
		delete(n.pendingBroadcastByPrev, key)
	}
}

func pendingBlockBroadcastDecodeBytes(req pendingBlockBroadcastDecode) int64 {
	bytes := int64(pendingBroadcastDecodeOverhead)
	switch msg := req.msg.(type) {
	case tonnodeapi.BlockBroadcastCompressedV2:
		bytes += int64(len(msg.Proof) + len(msg.DataCompressed))
	}
	if req.rebroadcast != nil {
		bytes += int64(req.rebroadcast.payloadLen())
	}
	return bytes
}
