package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/state"
	"github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type p2pStateSource struct {
	node *p2p.Node
	log  zerolog.Logger
}

type downloadedStateWithBlockArtifact struct {
	storage.DownloadedState
	artifact *storage.ServedBlockFull
}

func (d *downloadedStateWithBlockArtifact) BlockArtifact() *storage.ServedBlockFull {
	return d.artifact.Clone()
}

func NewP2PStateSource(node *p2p.Node, logger ...*zerolog.Logger) state.Source {
	log := zerolog.Nop()
	if len(logger) > 0 && logger[0] != nil {
		log = logger[0].With().Str("component", "state").Logger()
	}

	return &p2pStateSource{node: node, log: log}
}

func (s *p2pStateSource) ZeroStateBlock(ctx context.Context) (ton.BlockIDExt, error) {
	_ = ctx

	block, err := s.node.ZeroStateBlock()
	if errors.Is(err, storage.ErrNotFound) {
		return ton.BlockIDExt{}, fmt.Errorf("zero state is not configured")
	}
	return block, err
}

func (s *p2pStateSource) InitBlock(ctx context.Context) (ton.BlockIDExt, error) {
	_ = ctx

	block, err := s.node.InitBlock()
	if errors.Is(err, storage.ErrNotFound) {
		return ton.BlockIDExt{}, fmt.Errorf("init block is not configured")
	}
	return block, err
}

func (s *p2pStateSource) IsHardfork(ctx context.Context, block ton.BlockIDExt) bool {
	_ = ctx

	return s.node.IsHardfork(block)
}

func (s *p2pStateSource) ZeroState(ctx context.Context, block ton.BlockIDExt) (storage.DownloadedState, error) {
	s.log.Info().
		Str("block", storage.FormatBlockRef(block)).
		Msg("requesting zero state")

	downloaded, err := s.node.DownloadState(ctx, block, block, 0, nil)
	return downloaded, normalizeStateSourceError(err)
}

func (s *p2pStateSource) NextKeyBlocks(ctx context.Context, from ton.BlockIDExt, limit int32) (state.KeyBlockBatch, error) {
	batch, err := s.node.NextKeyBlocks(ctx, from, limit)
	return state.KeyBlockBatch{
		Blocks:     batch.Blocks,
		Incomplete: batch.Incomplete,
	}, err
}

func (s *p2pStateSource) InitBlockProof(ctx context.Context, block ton.BlockIDExt) (state.ProofDownload, error) {
	s.log.Debug().
		Str("block", storage.FormatBlockRef(block)).
		Msg("requesting trusted init block proof link")

	proof, err := s.node.DownloadBlockProof(ctx, block, true)
	return state.ProofDownload{Data: proof.Data, Link: proof.Link}, err
}

func (s *p2pStateSource) MasterchainProof(ctx context.Context, block ton.BlockIDExt, requireKey bool) ([]byte, error) {
	if requireKey {
		s.log.Debug().
			Str("block", storage.FormatBlockRef(block)).
			Msg("requesting key block proof")

		downloaded, err := s.keyBlockProof(ctx, block, false)
		if err == nil && !downloaded.Link {
			return downloaded.Data, nil
		}
		if err != nil && !isExpectedProofUnavailable(err) {
			return nil, fmt.Errorf("download key block proof %s: %w", storage.FormatBlockRef(block), err)
		}

		evt := s.log.Debug().
			Str("block", storage.FormatBlockRef(block)).
			Bool("proof_link", downloaded.Link)
		if err != nil {
			evt = evt.Err(err)
		}
		evt.Msg("key block proof unavailable as full proof, downloading block full")
	}

	s.log.Debug().
		Str("block", storage.FormatBlockRef(block)).
		Bool("require_key", requireKey).
		Msg("requesting masterchain block full for proof")

	downloaded, err := s.node.DownloadBlockFull(ctx, block)
	if err != nil {
		return nil, fmt.Errorf("download block full %s: %w", storage.FormatBlockRef(block), err)
	}
	if downloaded == nil {
		return nil, fmt.Errorf("download block full %s: empty response", storage.FormatBlockRef(block))
	}
	return downloaded.ProofBOC, nil
}

func (s *p2pStateSource) keyBlockProof(ctx context.Context, block ton.BlockIDExt, allowPartial bool) (state.ProofDownload, error) {
	s.log.Debug().
		Str("block", storage.FormatBlockRef(block)).
		Bool("allow_partial", allowPartial).
		Msg("requesting key block proof")

	proof, err := s.node.DownloadKeyBlockProof(ctx, block, allowPartial)
	return state.ProofDownload{Data: proof.Data, Link: proof.Link}, err
}

func (s *p2pStateSource) DownloadState(ctx context.Context, block ton.BlockIDExt, master ton.BlockIDExt, splitDepth uint32) (storage.DownloadedState, error) {
	s.log.Info().
		Str("block", storage.FormatBlockRef(block)).
		Str("masterchain", storage.FormatBlockRef(master)).
		Uint32("split_depth", splitDepth).
		Msg("requesting state snapshot")

	artifact, stateRootHash, err := s.blockStateRootArtifact(ctx, block)
	if err != nil {
		return nil, fmt.Errorf("load expected state root for %s: %w", storage.FormatBlockRef(block), err)
	}

	downloaded, err := s.node.DownloadState(ctx, block, master, splitDepth, stateRootHash)
	if err != nil {
		return nil, normalizeStateSourceError(err)
	}
	return &downloadedStateWithBlockArtifact{
		DownloadedState: downloaded,
		artifact:        artifact,
	}, nil
}

func (s *p2pStateSource) BlockFull(ctx context.Context, block ton.BlockIDExt) (*storage.ServedBlockFull, error) {
	artifact, _, err := s.blockStateRootArtifact(ctx, block)
	return artifact, err
}

func (s *p2pStateSource) blockStateRootArtifact(ctx context.Context, block ton.BlockIDExt) (*storage.ServedBlockFull, []byte, error) {
	downloaded, err := s.node.DownloadBlockFull(ctx, block)
	if err != nil {
		return nil, nil, fmt.Errorf("download block: %w", err)
	}
	if downloaded == nil {
		return nil, nil, fmt.Errorf("download block: empty response")
	}
	if len(downloaded.BlockBOC) == 0 {
		return nil, nil, fmt.Errorf("downloaded block %s has no block data", storage.FormatBlockRef(block))
	}
	if len(downloaded.ProofBOC) == 0 {
		return nil, nil, fmt.Errorf("downloaded block %s has no proof", storage.FormatBlockRef(block))
	}

	root := downloaded.Block
	if root == nil {
		return nil, nil, fmt.Errorf("downloaded block %s is missing parsed cell", storage.FormatBlockRef(block))
	}
	if downloaded.IsLink && root.GetType() == cell.MerkleProofCellType {
		root, err = cell.UnwrapProof(root, downloaded.ID.RootHash)
		if err != nil {
			return nil, nil, fmt.Errorf("unwrap block proof link: %w", err)
		}
	}

	meta, err := storage.BuildBlockMetaFromBlockCell(downloaded.ID, root)
	if err != nil {
		return nil, nil, err
	}
	if len(meta.StateRootHash) == 0 {
		return nil, nil, fmt.Errorf("block has no state update target")
	}

	s.log.Debug().
		Str("block", storage.FormatBlockRef(block)).
		Str("state_root_hash", fmt.Sprintf("%x", meta.StateRootHash)).
		Msg("resolved block state root for snapshot validation")
	return &storage.ServedBlockFull{
		ID:     downloaded.ID,
		Proof:  downloaded.ProofBOC,
		Block:  downloaded.BlockBOC,
		Meta:   meta,
		IsLink: storage.ServedBlockProofIsLink(downloaded.ID, downloaded.IsLink),
	}, meta.StateRootHash, nil
}

func normalizeStateSourceError(err error) error {
	if err == nil || !errors.Is(err, p2p.ErrStateNotAvailable) {
		return err
	}
	return fmt.Errorf("%w: %w", state.ErrStateNotAvailable, err)
}

func isExpectedProofUnavailable(err error) bool {
	return errors.Is(err, p2p.ErrBlockNotAvailable)
}
