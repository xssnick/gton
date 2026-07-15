package p2p

import "time"

const (
	broadcastPipelineStageFECDecode         = "fec_decode"
	broadcastPipelineStageClassify          = "classify"
	broadcastPipelineStageCandidateDecode   = "candidate_decode"
	broadcastPipelineStageShardDescValidate = "shard_desc_validate"
	broadcastPipelineStageFinalitySigCheck  = "block_finality_signature_check"
	broadcastPipelineStageFinalityAssemble  = "block_finality_assemble"
	broadcastPipelineStageHotCacheNotify    = "hot_cache_notify"
	broadcastPipelineStageExactPop          = "exact_pop"
	broadcastPipelineStageDecodeAsync       = "decode_async"

	broadcastPipelineResultSuccess = "success"
	broadcastPipelineResultDrop    = "drop"
	broadcastPipelineResultMiss    = "miss"
	broadcastPipelineResultError   = "error"
)

func (n *Node) SetBroadcastPipelineObserver(observer BroadcastPipelineObserver) {
	n.broadcastPipelineObserver = observer
}

func (n *Node) startBroadcastPipelineStage() time.Time {
	if n.broadcastPipelineObserver == nil {
		return time.Time{}
	}
	return time.Now()
}

func (n *Node) observeBroadcastPipelineStageSince(started time.Time, stage string, kind string, delivery Delivery, result string) {
	if started.IsZero() || n.broadcastPipelineObserver == nil {
		return
	}

	n.observeBroadcastPipelineStageDuration(stage, kind, delivery, result, time.Since(started))
}

func (n *Node) observeBroadcastPipelineStageDuration(stage string, kind string, delivery Delivery, result string, duration time.Duration) {
	if n.broadcastPipelineObserver == nil {
		return
	}

	n.broadcastPipelineObserver.ObserveBroadcastPipelineStage(BroadcastPipelineStageObservation{
		Stage:    stage,
		Kind:     kind,
		Delivery: delivery,
		Result:   result,
		Duration: duration,
	})
}
