package service

import (
	"context"
	"fmt"

	"github.com/xssnick/gton/service/hooks"
	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
)

var _ p2p.BlockReceivedObserver = (*Service)(nil)

type blockReceivedHookRunner struct {
	log       zerolog.Logger
	extension hooks.Extension
}

func newBlockReceivedHookRunner(log zerolog.Logger, extension hooks.Extension) *blockReceivedHookRunner {
	if extension == nil {
		return nil
	}

	return &blockReceivedHookRunner{
		log:       log,
		extension: extension,
	}
}

func (s *Service) ObserveBlockReceived(ctx context.Context, event p2p.BlockReceivedEvent) {
	if s.blockReceivedHooks == nil {
		return
	}

	s.blockReceivedHooks.run(ctx, hooks.BlockReceivedEvent{
		IsSigned:  event.IsSigned,
		BlockBOC:  event.Downloaded.BlockBOC,
		ProofBOC:  event.Downloaded.ProofBOC,
		BlockRoot: event.Downloaded.Block,
		Meta:      blockReceivedMeta(event.Downloaded),
	})
}

func (s *Service) observePreparedBlockReceived(ctx context.Context, block PreparedBlock, isSigned bool) {
	if s.blockReceivedHooks == nil {
		return
	}

	s.blockReceivedHooks.run(ctx, hooks.BlockReceivedEvent{
		IsSigned:  isSigned,
		BlockBOC:  block.BlockBOC,
		ProofBOC:  block.ProofBOC,
		BlockRoot: block.BlockRoot,
		Meta:      blockReceivedPreparedMeta(block),
	})
}

func (s *Service) BlockReceivedHooksEnabled() bool {
	return s.blockReceivedHooks != nil
}

func (r *blockReceivedHookRunner) run(ctx context.Context, event hooks.BlockReceivedEvent) {
	block := ""
	if event.Meta != nil {
		block = storage.FormatBlockRef(event.Meta.ID)
	}

	if err := r.extension.OnBlockReceived(ctx, event); err != nil {
		log := r.log.Warn().
			Err(err).
			Str("extension", fmt.Sprintf("%T", r.extension))
		if block != "" {
			log = log.Str("block", block)
		}
		log.Msg("block received hook failed")
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
