package service

import (
	"context"
	"fmt"

	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
)

var _ p2p.BlockReceivedObserver = (*SyncCoordinator)(nil)

type blockReceivedObserverRunner struct {
	log      zerolog.Logger
	observer BlockReceivedObserver
}

func newBlockReceivedObserverRunner(log zerolog.Logger, observer BlockReceivedObserver) *blockReceivedObserverRunner {
	if observer == nil {
		return nil
	}

	return &blockReceivedObserverRunner{
		log:      log,
		observer: observer,
	}
}

func (s *SyncCoordinator) ObserveBlockReceived(ctx context.Context, event p2p.BlockReceivedEvent) {
	if event.FromBroadcast && event.IsSigned && event.Downloaded != nil {
		// A decoded shard broadcast typically lands hundreds of ms before the
		// master that includes it; preparing it now turns the commit-stage
		// resolve into a take of ready state-update cells (masterchain blocks
		// are rejected by the enqueue).
		s.enqueueShardBlockPrepare(shardPrepareRequest{
			block:      event.Downloaded.ID,
			downloaded: event.Downloaded,
		})
	}
	if s.blockReceivedObserver == nil {
		return
	}

	s.blockReceivedObserver.run(ctx, BlockReceivedEvent{
		IsSigned:  event.IsSigned,
		BlockBOC:  event.Downloaded.BlockBOC,
		ProofBOC:  event.Downloaded.ProofBOC,
		BlockRoot: event.Downloaded.Block,
		Meta:      blockReceivedMeta(event.Downloaded),
	})
}

func (r *blockReceivedObserverRunner) run(ctx context.Context, event BlockReceivedEvent) {
	block := ""
	if event.Meta != nil {
		block = storage.FormatBlockRef(event.Meta.ID)
	}

	if err := r.observer.ObserveBlockReceived(ctx, event); err != nil {
		log := r.log.Warn().
			Err(err).
			Str("observer", fmt.Sprintf("%T", r.observer))
		if block != "" {
			log = log.Str("block", block)
		}
		log.Msg("block received observer failed")
	}
}

func blockReceivedMeta(downloaded *p2p.DownloadedBlock) *storage.BlockMeta {
	if downloaded.Meta == nil {
		return &storage.BlockMeta{ID: downloaded.ID}
	}
	if !downloaded.Meta.ID.Equals(&downloaded.ID) {
		cloned := downloaded.Meta.Clone()
		cloned.ID = downloaded.ID
		return cloned
	}
	return downloaded.Meta
}

func blockReceivedPreparedMeta(block PreparedBlock) *storage.BlockMeta {
	if !block.Meta.ID.Equals(&block.ID) {
		cloned := block.Meta.Clone()
		cloned.ID = block.ID
		return cloned
	}
	return block.Meta
}
