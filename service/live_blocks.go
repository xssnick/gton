package service

import (
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type liveBlockPublisher interface {
	PublishLiveBlockArtifacts(artifacts storage.LiveBlockArtifacts) error
	NonfinalBlockCacheEnabled() bool
	PublishNonfinalBlockArtifacts(artifacts storage.LiveBlockArtifacts, kind storage.LiveBlockNonfinalKind) error
	MarkLiveBlockFlushed(block ton.BlockIDExt)
}

type liveNonfinalCellLoaderSetter interface {
	SetNonfinalCellLoader(cell.LazyCellLoader)
}

type liveBlockCacheProvider interface {
	LiveBlockCache() *storage.LiveBlockCache
}

func (s *Service) configureLiveBlockPublisher(publisher CurrentStatePublisher) {
	if cache, ok := publisher.(liveNonfinalCellLoaderSetter); ok {
		cache.SetNonfinalCellLoader(s.stateCellLoader())
	}

	cache, ok := publisher.(liveBlockPublisher)
	if !ok || s.node == nil {
		return
	}
	s.node.SetBlockCacheObserver(cache)
}

func (s *Service) publishLiveBlockArtifacts(downloaded PreparedBlock, state *storage.BlockState) {
	if downloaded.BlockRoot == nil {
		s.log.Debug().
			Str("block", downloaded.BlockRef()).
			Msg("skip live block cache update")
		return
	}

	var blockData []byte
	if len(downloaded.BlockBOC) > 0 {
		blockData = downloaded.BlockBOC
	}

	var proofs []storage.LiveBlockProofArtifact
	if len(downloaded.ProofBOC) > 0 {
		isKeyBlock := false
		if downloaded.Meta != nil {
			isKeyBlock = downloaded.Meta.Has(storage.BlockMetaIsKeyBlock)
		}
		for _, kind := range storage.StoredProofKindsForServedBlock(downloaded.ID, downloaded.IsLink, isKeyBlock) {
			proofs = append(proofs, storage.LiveBlockProofArtifact{
				Kind: kind,
				Data: downloaded.ProofBOC,
			})
		}
	}

	artifacts := storage.LiveBlockArtifacts{
		Block:     downloaded.ID,
		Root:      downloaded.BlockRoot,
		BlockData: blockData,
		Meta:      liveBlockArtifactMeta(downloaded.ID, downloaded.Meta, blockData, proofs),
		State:     state,
		Proofs:    proofs,
	}

	liveArtifacts := artifacts
	if s.node != nil {
		if err := s.node.PublishLiveBlockArtifacts(artifacts); err != nil {
			s.log.Debug().
				Err(err).
				Str("block", storage.FormatBlockRef(downloaded.ID)).
				Msg("skip p2p live block cache update")
		}
		if provider, ok := s.liveState.(liveBlockCacheProvider); ok && provider.LiveBlockCache() == s.node.LiveBlockCache() {
			liveArtifacts.BlockData = nil
			liveArtifacts.Proofs = nil
		}
	}

	cache, ok := s.liveState.(liveBlockPublisher)
	if !ok {
		return
	}
	if err := cache.PublishLiveBlockArtifacts(liveArtifacts); err != nil {
		s.log.Debug().
			Err(err).
			Str("block", storage.FormatBlockRef(downloaded.ID)).
			Msg("skip live block cache update")
	}
}

func liveBlockArtifactMeta(block ton.BlockIDExt, meta *storage.BlockMeta, blockData []byte, proofs []storage.LiveBlockProofArtifact) *storage.BlockMeta {
	if meta == nil && len(blockData) == 0 && len(proofs) == 0 {
		return nil
	}

	cloned := meta.Clone()
	if cloned == nil {
		cloned = &storage.BlockMeta{ID: block}
	}
	cloned.ID = block
	if len(blockData) > 0 {
		cloned.Mark(storage.BlockMetaHasBlockData)
	}
	for _, proof := range proofs {
		cloned.Mark(storage.BlockMetaFlagForProof(proof.Kind))
	}
	return cloned
}
