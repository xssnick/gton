package service

import (
	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

type liveBlockPublisher interface {
	PublishLiveBlockArtifacts(artifacts storage.LiveBlockArtifacts) error
	MarkLiveBlockFlushed(block ton.BlockIDExt)
}

func (s *Service) configureLiveBlockPublisher(publisher CurrentStatePublisher) {
	cache, ok := publisher.(liveBlockPublisher)
	if !ok || s.node == nil {
		return
	}
	s.node.SetBlockCacheObserver(cache)
}

func (s *Service) publishLiveBlockArtifacts(downloaded p2p.DownloadedBlock, state *storage.BlockState, flushed bool) {
	cache, ok := s.liveState.(liveBlockPublisher)
	if !ok {
		return
	}

	root, err := downloadedBlockRoot(downloaded)
	if err != nil {
		s.log.Debug().
			Err(err).
			Str("block", downloaded.BlockRef()).
			Msg("skip live block cache update")
		return
	}

	var blockData []byte
	cacheFlushed := false
	if downloaded.VerifiedFileHash {
		blockData = downloaded.BlockBOC
		cacheFlushed = flushed
	}

	var proofs []storage.LiveBlockProofArtifact
	if len(downloaded.ProofBOC) > 0 {
		isKeyBlock := false
		if downloaded.Meta != nil {
			isKeyBlock = downloaded.Meta.Has(storage.BlockMetaIsKeyBlock)
		}
		for _, kind := range storage.StoredProofKindsForBlock(appliedBlockProofIsLink(downloaded.ID), isKeyBlock) {
			proofs = append(proofs, storage.LiveBlockProofArtifact{
				Kind: kind,
				Data: downloaded.ProofBOC,
			})
		}
	}

	var meta *storage.BlockMeta
	if downloaded.Meta != nil {
		meta = downloaded.Meta.Clone()
	}
	if err = cache.PublishLiveBlockArtifacts(storage.LiveBlockArtifacts{
		Block:            downloaded.ID,
		Root:             root,
		BlockData:        blockData,
		Meta:             meta,
		State:            state,
		Proofs:           proofs,
		BlockDataFlushed: cacheFlushed,
		ProofsFlushed:    cacheFlushed,
	}); err != nil {
		s.log.Debug().
			Err(err).
			Str("block", storage.FormatBlockRef(downloaded.ID)).
			Msg("skip live block cache update")
	}
}
