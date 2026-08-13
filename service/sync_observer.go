package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xssnick/gton/service/p2p"

	"github.com/xssnick/tonutils-go/ton"
)

type SyncObserver interface {
	ObserveSyncBlock(SyncBlockObservation)
	ObserveSyncObtain(SyncObtainObservation)
	ObserveSyncPersist(SyncPersistObservation)
}

type SyncBlockSource string

const (
	SyncBlockSourceUnknown            SyncBlockSource = "unknown"
	SyncBlockSourceBroadcast          SyncBlockSource = "broadcast"
	SyncBlockSourceBroadcastQueue     SyncBlockSource = "broadcast_queue"
	SyncBlockSourceBroadcastCandidate SyncBlockSource = "broadcast_candidate"
	SyncBlockSourceBroadcastCache     SyncBlockSource = "broadcast_cache"
	SyncBlockSourceBroadcastHint      SyncBlockSource = "broadcast_hint"
	SyncBlockSourceQueue              SyncBlockSource = "queue"
	SyncBlockSourcePeerProbe          SyncBlockSource = "peer_probe"
	SyncBlockSourceNextBlock          SyncBlockSource = "next_block"
	SyncBlockSourceIndexed            SyncBlockSource = "indexed"
	SyncBlockSourceNextDescription    SyncBlockSource = "next_description"
	SyncBlockSourcePeerCatchUp        SyncBlockSource = "peer_catch_up"
	SyncBlockSourceCatchUp            SyncBlockSource = "catch_up"
	SyncBlockSourceProbe              SyncBlockSource = "probe"
	SyncBlockSourceStored             SyncBlockSource = "stored"
	SyncBlockSourceInternal           SyncBlockSource = "internal"
)

type SyncBlockOrigin string

const (
	SyncBlockOriginUnknown   SyncBlockOrigin = "unknown"
	SyncBlockOriginBroadcast SyncBlockOrigin = "broadcast"
	SyncBlockOriginDownload  SyncBlockOrigin = "download"
	SyncBlockOriginStored    SyncBlockOrigin = "stored"
	SyncBlockOriginOther     SyncBlockOrigin = "other"
)

type SyncBlockObservation struct {
	Pipeline         string
	Chain            string
	Shard            string
	Source           SyncBlockSource
	Origin           SyncBlockOrigin
	Result           string
	CatchUp          bool
	DownloadDuration time.Duration
	PrepareDuration  time.Duration
	ApplyDuration    time.Duration
}

type SyncObtainObservation struct {
	Pipeline string
	Stage    string
	Result   string
	CatchUp  bool
	Duration time.Duration
}

type SyncPersistObservation struct {
	Mode          string
	Result        string
	QueueDuration time.Duration
	Duration      time.Duration
	States        int
	Stages        []SyncPersistStageObservation
}

type SyncPersistStageObservation struct {
	Stage    string
	Duration time.Duration
}

func (s *SyncCoordinator) observeSyncBlock(observation SyncBlockObservation) {
	if s.sync == nil {
		return
	}
	if observation.Pipeline == "" {
		observation.Pipeline = "unknown"
	}
	if observation.Chain == "" {
		observation.Chain = "unknown"
	}
	if observation.Shard == "" {
		observation.Shard = "unknown"
	}
	if observation.Source == "" {
		observation.Source = SyncBlockSourceUnknown
	}
	if observation.Origin == "" {
		observation.Origin = SyncBlockOriginUnknown
	}
	if observation.Result == "" {
		observation.Result = "unknown"
	}
	s.sync.ObserveSyncBlock(observation)
}

func (s *SyncCoordinator) observeSyncObtain(observation SyncObtainObservation) {
	if s.sync == nil {
		return
	}
	if observation.Pipeline == "" {
		observation.Pipeline = "unknown"
	}
	if observation.Stage == "" {
		observation.Stage = "unknown"
	}
	if observation.Result == "" {
		observation.Result = "unknown"
	}
	s.sync.ObserveSyncObtain(observation)
}

func syncBlockOriginForSource(source SyncBlockSource) SyncBlockOrigin {
	switch source {
	case SyncBlockSourceBroadcast, SyncBlockSourceBroadcastQueue, SyncBlockSourceBroadcastCandidate, SyncBlockSourceBroadcastCache, SyncBlockSourceBroadcastHint, SyncBlockSourceQueue:
		return SyncBlockOriginBroadcast
	case SyncBlockSourcePeerProbe, SyncBlockSourceNextBlock, SyncBlockSourceIndexed, SyncBlockSourceNextDescription, SyncBlockSourcePeerCatchUp, SyncBlockSourceCatchUp, SyncBlockSourceProbe:
		return SyncBlockOriginDownload
	case SyncBlockSourceStored:
		return SyncBlockOriginStored
	case SyncBlockSourceInternal:
		return SyncBlockOriginOther
	case "":
		return SyncBlockOriginUnknown
	default:
		return SyncBlockOriginOther
	}
}

func syncBlockOriginForKind(kind string) SyncBlockOrigin {
	switch kind {
	case "tonNode.blockBroadcast", "tonNode.blockBroadcastCompressed", "tonNode.blockBroadcastCompressedV2", "tonNode.newShardBlockBroadcast", "tonNode.blockFinalityBroadcast":
		return SyncBlockOriginBroadcast
	case "local full block cache", "local next block cache", "stored block":
		return SyncBlockOriginStored
	default:
		return SyncBlockOriginDownload
	}
}

func syncBlockSourceForKind(defaultSource SyncBlockSource, kind string) SyncBlockSource {
	switch kind {
	case "tonNode.blockBroadcast", "tonNode.blockBroadcastCompressed", "tonNode.blockBroadcastCompressedV2", "tonNode.blockFinalityBroadcast":
		return SyncBlockSourceBroadcastCache
	case "tonNode.newShardBlockBroadcast":
		return SyncBlockSourceBroadcastHint
	case "local full block cache", "local next block cache", "stored block":
		return SyncBlockSourceStored
	default:
		return defaultSource
	}
}

func syncBlockResultForError(err error) string {
	if err == nil {
		return "success"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	if isTimeoutError(err) {
		return "timeout"
	}
	if errors.Is(err, p2p.ErrBlockNotAvailable) || errors.Is(err, p2p.ErrStateNotAvailable) {
		return "miss"
	}
	if isExpectedRetryError(err) {
		return "retry"
	}
	return "error"
}

func (s *SyncCoordinator) observeSyncPersist(observation SyncPersistObservation) {
	if s.sync == nil {
		return
	}
	if observation.Mode == "" {
		observation.Mode = "unknown"
	}
	if observation.Result == "" {
		observation.Result = "unknown"
	}
	for i := range observation.Stages {
		if observation.Stages[i].Stage == "" {
			observation.Stages[i].Stage = "unknown"
		}
		if observation.Stages[i].Duration < 0 {
			observation.Stages[i].Duration = 0
		}
	}
	s.sync.ObserveSyncPersist(observation)
}

func syncChainLabel(block ton.BlockIDExt) string {
	if block.Workchain == -1 && block.Shard == topShard {
		return "masterchain"
	}
	if block.Workchain == 0 {
		return "shardchain"
	}
	return fmt.Sprintf("workchain_%d", block.Workchain)
}

func syncShardLabel(block ton.BlockIDExt) string {
	if block.Workchain == -1 && block.Shard == topShard {
		return "masterchain"
	}
	if block.Workchain == 0 && block.Shard == topShard {
		return "basechain"
	}
	return fmt.Sprintf("%016x", uint64(block.Shard))
}
