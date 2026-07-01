package service

import (
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func (s *Service) NonfinalCellLoader() cell.LazyCellLoader {
	return s.stateCellLoader()
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
	if s.liveBlockCache != nil {
		if err := s.liveBlockCache.PublishLiveBlockArtifacts(artifacts); err != nil {
			s.log.Debug().
				Err(err).
				Str("block", storage.FormatBlockRef(downloaded.ID)).
				Msg("skip live block cache update")
		}
	}
	if s.liveStateUsesBlockCache {
		liveArtifacts.BlockData = nil
		liveArtifacts.Proofs = nil
	}

	if s.liveState == nil {
		return
	}
	if err := s.liveState.PublishLiveBlockArtifacts(liveArtifacts); err != nil {
		s.log.Debug().
			Err(err).
			Str("block", storage.FormatBlockRef(downloaded.ID)).
			Msg("skip live block cache update")
	}
}

func (s *Service) publishLiveCurrentBlockMarkers(current *storage.CurrentState) {
	if s.liveState == nil || current == nil {
		return
	}

	publish := func(state storage.BlockState) {
		meta, err := storage.BuildBlockMetaFromState(state)
		if err != nil {
			s.log.Debug().
				Err(err).
				Str("block", storage.FormatBlockRef(state.Block)).
				Msg("skip live current block marker")
			return
		}

		err = s.liveState.PublishLiveBlockArtifacts(storage.LiveBlockArtifacts{
			Block:           state.Block,
			Meta:            meta,
			State:           &state,
			ArtifactFlushed: true,
			StateFlushed:    true,
		})
		if err != nil {
			s.log.Debug().
				Err(err).
				Str("block", storage.FormatBlockRef(state.Block)).
				Msg("skip live current block marker")
		}
	}

	publish(current.Masterchain)
	for _, key := range storage.SortedShardKeys(current.Shards) {
		publish(current.Shards[key])
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
