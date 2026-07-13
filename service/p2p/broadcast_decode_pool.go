package p2p

import (
	"context"
	"runtime"
	"time"

	"github.com/xssnick/gton/service/blockproof"

	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const broadcastDecodeQueueSize = 64

// offloadedBroadcastDecode is a full-block broadcast whose validator
// signatures already passed the predecode check on the listen thread; the
// expensive payload decode runs on the bounded decode pool so multi-MB
// blocks do not stall ADNL listeners or delay FEC relay to other peers.
type offloadedBroadcastDecode struct {
	fingerprint  string
	overlay      string
	delivery     Delivery
	trusted      bool
	kind         string
	block        ton.BlockIDExt
	sourcePeerID PeerID
	receivedAt   time.Time
	msg          any
	proofRoot    *cell.Cell
	preSigSet    *blockproof.ValidatorSignatureSet
	rebroadcast  *rebroadcastRequest
}

func broadcastDecodeWorkerCount() int {
	workers := runtime.GOMAXPROCS(0) / 4
	if workers < 2 {
		workers = 2
	}
	if workers > 4 {
		workers = 4
	}
	return workers
}

// enqueueBroadcastDecode hands the decode to the pool; false means the queue
// is full (or the node is shutting down) and the caller must decode inline.
// The pool is tied to the node's single run: once the run context ends the
// workers exit and later enqueues are refused.
func (n *Node) enqueueBroadcastDecode(req offloadedBroadcastDecode) bool {
	if n.runtimeContext().Err() != nil {
		return false
	}
	n.decodeWorkersOnce.Do(func() {
		n.decodeQueue = make(chan offloadedBroadcastDecode, broadcastDecodeQueueSize)
		for i := 0; i < broadcastDecodeWorkerCount(); i++ {
			n.runAsync(n.runBroadcastDecodeWorker)
		}
	})

	select {
	case n.decodeQueue <- req:
		return true
	default:
		return false
	}
}

func (n *Node) runBroadcastDecodeWorker() {
	ctx := n.runtimeContext()
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-n.decodeQueue:
			n.processOffloadedBroadcastDecode(ctx, req)
		}
	}
}

func (n *Node) processOffloadedBroadcastDecode(ctx context.Context, req offloadedBroadcastDecode) {
	if ctx.Err() != nil {
		return
	}
	if !n.canAcceptBroadcast(req.kind, false) {
		n.deduper.Forget(req.fingerprint)
		n.noteBroadcastDrop(req.overlay, req.kind, "broadcast_admission_closed")
		return
	}

	downloaded, sigSet, cached := n.cachedDecodedBroadcast(req.kind, req.block)
	if cached {
		n.noteBroadcast("decode_reused", req.overlay, req.kind)
		if req.preSigSet != nil {
			sigSet = req.preSigSet
		}
	} else {
		var err error
		started := n.startBroadcastPipelineStage()
		downloaded, sigSet, err = n.decodeBroadcastBlock(ctx, req.msg, req.proofRoot, req.preSigSet)
		result := broadcastPipelineResultSuccess
		if err != nil {
			result = broadcastPipelineResultError
			if isBroadcastDecompressionStateNotReady(err) {
				result = broadcastPipelineResultMiss
			}
		}
		n.observeBroadcastPipelineStageSince(started, broadcastPipelineStageDecodeAsync, req.kind, req.delivery, result)
		if err != nil {
			n.handleOffloadedBroadcastDecodeError(req, err)
			return
		}
		n.rememberDecodedBroadcast(req.kind, req.block, downloaded, sigSet)
	}
	if downloaded == nil {
		n.noteBroadcastDrop(req.overlay, req.kind, "decode_failed")
		return
	}

	downloaded.SourcePeerID = req.sourcePeerID
	if sigSet != nil {
		downloaded.SignaturesVerifiedKey = sigSet.ContentKey(req.block)
	}

	block := cloneBlockID(req.block)
	n.acceptBroadcast(acceptedBroadcast{
		fingerprint:        req.fingerprint,
		deduped:            true,
		skipAcceptedMetric: true,
		block:              &block,
		rebroadcast:        pendingBlockBroadcastRebroadcast(req.rebroadcast),
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
	})
}

func (n *Node) handleOffloadedBroadcastDecodeError(req offloadedBroadcastDecode, err error) {
	if isBroadcastDecompressionStateNotReady(err) {
		pendingState, ok := broadcastDecompressionStateNotReady(err)
		_, isV2 := req.msg.(tonnodeapi.BlockBroadcastCompressedV2)
		if ok && isV2 {
			var verifiedSignaturesKey []byte
			if req.preSigSet != nil {
				verifiedSignaturesKey = req.preSigSet.ContentKey(req.block)
			}
			n.schedulePendingBlockBroadcastDecode(pendingBlockBroadcastDecode{
				fingerprint:           req.fingerprint,
				overlay:               req.overlay,
				delivery:              req.delivery,
				trusted:               req.trusted,
				kind:                  req.kind,
				block:                 req.block,
				sourcePeerID:          req.sourcePeerID,
				receivedAt:            req.receivedAt,
				msg:                   req.msg,
				prev:                  pendingState.prev,
				proofRoot:             pendingState.proofRoot,
				rebroadcast:           pendingBlockBroadcastRebroadcast(req.rebroadcast),
				verifiedSignaturesKey: verifiedSignaturesKey,
			})
			n.log.Debug().
				Err(err).
				Str("block", formatBlockRef(req.block)).
				Str("kind", req.kind).
				Msg("queued offloaded block broadcast until previous state is available")
			return
		}
		n.log.Debug().
			Err(err).
			Str("block", formatBlockRef(req.block)).
			Str("kind", req.kind).
			Msg("dropping offloaded block broadcast because state-ready artifact is missing")
		n.noteBroadcastDrop(req.overlay, req.kind, "state_artifact_missing")
		return
	}

	n.log.Debug().
		Err(err).
		Str("block", formatBlockRef(req.block)).
		Str("kind", req.kind).
		Msg("dropping offloaded block broadcast because payload decode failed")
	n.noteBroadcastDrop(req.overlay, req.kind, "decode_failed")
}
