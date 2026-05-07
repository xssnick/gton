package state

import (
	"context"
	"errors"
	"fmt"

	"flexserver/service/p2p"
	"flexserver/service/storage"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type Source interface {
	LatestMasterchainBlock(ctx context.Context) (ton.BlockIDExt, error)
	InitBlock(ctx context.Context) (ton.BlockIDExt, error)
	ZeroStateBlock(ctx context.Context) (ton.BlockIDExt, error)
	ZeroState(ctx context.Context, block ton.BlockIDExt) (storage.DownloadedState, error)
	NextKeyBlocks(ctx context.Context, from ton.BlockIDExt, limit int32) (p2p.KeyBlockBatch, error)
	InitBlockProof(ctx context.Context, block ton.BlockIDExt) (p2p.ProofDownload, error)
	MasterchainProof(ctx context.Context, block ton.BlockIDExt, requireKey bool) ([]byte, error)
	ShardBlocks(ctx context.Context, master ton.BlockIDExt) ([]ton.BlockIDExt, error)
	DownloadState(ctx context.Context, block ton.BlockIDExt, master ton.BlockIDExt, splitDepth uint32) (storage.DownloadedState, error)
}

type P2PSource struct {
	node *p2p.Node
	log  zerolog.Logger
}

func NewP2PSource(node *p2p.Node, logger ...*zerolog.Logger) *P2PSource {
	log := zerolog.Nop()
	if len(logger) > 0 && logger[0] != nil {
		log = logger[0].With().Str("component", "state").Logger()
	}

	return &P2PSource{node: node, log: log}
}

func (s *P2PSource) LatestMasterchainBlock(ctx context.Context) (ton.BlockIDExt, error) {
	latest, err := s.node.ObservedMasterchainBlock()
	if err == nil {
		s.log.Info().
			Str("latest", storage.FormatBlockRef(latest)).
			Msg("using runtime latest masterchain block before state sync")
		return latest, nil
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return ton.BlockIDExt{}, err
	}

	s.log.Info().Msg("waiting for latest masterchain block before state sync")
	block, err := s.node.WaitObservedMasterchainBlock(ctx)
	if err != nil {
		return ton.BlockIDExt{}, err
	}

	s.log.Info().
		Str("latest", storage.FormatBlockRef(block)).
		Msg("latest masterchain block received")
	return block, nil
}

func (s *P2PSource) ZeroStateBlock(ctx context.Context) (ton.BlockIDExt, error) {
	_ = ctx

	block, err := s.node.ZeroStateBlock()
	if errors.Is(err, storage.ErrNotFound) {
		return ton.BlockIDExt{}, fmt.Errorf("zero state is not configured")
	}
	return block, err
}

func (s *P2PSource) InitBlock(ctx context.Context) (ton.BlockIDExt, error) {
	_ = ctx

	block, err := s.node.InitBlock()
	if errors.Is(err, storage.ErrNotFound) {
		return ton.BlockIDExt{}, fmt.Errorf("init block is not configured")
	}
	return block, err
}

func (s *P2PSource) ZeroState(ctx context.Context, block ton.BlockIDExt) (storage.DownloadedState, error) {
	s.log.Info().
		Str("block", storage.FormatBlockRef(block)).
		Msg("requesting zero state")

	return s.node.DownloadState(ctx, block, block, 0, nil)
}

func (s *P2PSource) NextKeyBlocks(ctx context.Context, from ton.BlockIDExt, limit int32) (p2p.KeyBlockBatch, error) {
	return s.node.NextKeyBlocks(ctx, from, limit)
}

func (s *P2PSource) InitBlockProof(ctx context.Context, block ton.BlockIDExt) (p2p.ProofDownload, error) {
	s.log.Debug().
		Str("block", storage.FormatBlockRef(block)).
		Msg("requesting trusted init block proof link")

	return s.node.DownloadBlockProof(ctx, block, true)
}

func (s *P2PSource) keyBlockProof(ctx context.Context, block ton.BlockIDExt, allowPartial bool) (p2p.ProofDownload, error) {
	s.log.Debug().
		Str("block", storage.FormatBlockRef(block)).
		Bool("allow_partial", allowPartial).
		Msg("requesting key block proof")

	return s.node.DownloadKeyBlockProof(ctx, block, allowPartial)
}

func (s *P2PSource) ShardBlocks(ctx context.Context, master ton.BlockIDExt) ([]ton.BlockIDExt, error) {
	s.log.Info().
		Str("masterchain", storage.FormatBlockRef(master)).
		Msg("loading shard list from masterchain state")

	state, err := s.masterState(ctx, master)
	if err != nil {
		return nil, err
	}

	return ShardBlocksFromMasterState(state)
}

func (s *P2PSource) DownloadState(ctx context.Context, block ton.BlockIDExt, master ton.BlockIDExt, splitDepth uint32) (storage.DownloadedState, error) {
	s.log.Info().
		Str("block", storage.FormatBlockRef(block)).
		Str("masterchain", storage.FormatBlockRef(master)).
		Uint32("split_depth", splitDepth).
		Msg("requesting state snapshot")

	stateRootHash, err := s.blockStateRootHash(ctx, block)
	if err != nil {
		return nil, fmt.Errorf("load expected state root for %s: %w", storage.FormatBlockRef(block), err)
	}

	return s.node.DownloadState(ctx, block, master, splitDepth, stateRootHash)
}

func (s *P2PSource) masterState(ctx context.Context, master ton.BlockIDExt) (*storage.BlockState, error) {
	stateRootHash, err := s.blockStateRootHash(ctx, master)
	if err != nil {
		return nil, fmt.Errorf("load expected masterchain state root for %s: %w", storage.FormatBlockRef(master), err)
	}

	downloaded, err := s.node.DownloadState(ctx, master, master, 0, stateRootHash)
	if err != nil {
		return nil, fmt.Errorf("download masterchain state %s: %w", storage.FormatBlockRef(master), err)
	}
	defer func() {
		if cleanupErr := downloaded.Cleanup(); cleanupErr != nil {
			s.log.Warn().
				Err(cleanupErr).
				Str("block", storage.FormatBlockRef(master)).
				Msg("failed to cleanup temporary masterchain state snapshot")
		}
	}()

	state, err := downloaded.Decode(ctx)
	if err != nil {
		return nil, fmt.Errorf("decode masterchain state %s: %w", storage.FormatBlockRef(master), err)
	}
	return state, nil
}

func (s *P2PSource) blockStateRootHash(ctx context.Context, block ton.BlockIDExt) ([]byte, error) {
	downloaded, err := s.node.DownloadBlockFull(ctx, block)
	if err != nil {
		return nil, fmt.Errorf("download block: %w", err)
	}
	if downloaded == nil {
		return nil, fmt.Errorf("download block: empty response")
	}

	root := downloaded.Block
	if root == nil {
		return nil, fmt.Errorf("downloaded block %s is missing parsed cell", storage.FormatBlockRef(block))
	}
	if downloaded.IsLink && root.GetType() == cell.MerkleProofCellType {
		root, err = cell.UnwrapProof(root, downloaded.ID.RootHash)
		if err != nil {
			return nil, fmt.Errorf("unwrap block proof link: %w", err)
		}
	}

	meta, err := storage.BuildBlockMetaFromBlockCell(downloaded.ID, root)
	if err != nil {
		return nil, err
	}
	if len(meta.StateRootHash) == 0 {
		return nil, fmt.Errorf("block has no state update target")
	}

	s.log.Debug().
		Str("block", storage.FormatBlockRef(block)).
		Str("state_root_hash", fmt.Sprintf("%x", meta.StateRootHash)).
		Msg("resolved block state root for snapshot validation")
	return meta.StateRootHash, nil
}

func ShardBlocksFromMasterState(state *storage.BlockState) ([]ton.BlockIDExt, error) {
	if state.Block.Workchain != -1 {
		return nil, fmt.Errorf("expected masterchain state, got %s", storage.FormatBlockRef(state.Block))
	}
	if state.Parsed == nil || state.Parsed.McStateExtra == nil {
		return nil, fmt.Errorf("masterchain state %s is missing parsed mc_state_extra", storage.FormatBlockRef(state.Block))
	}

	var extra tlb.McStateExtra
	if err := tlb.LoadFromCell(&extra, state.Parsed.McStateExtra.BeginParse()); err != nil {
		return nil, fmt.Errorf("parse mc_state_extra for %s: %w", storage.FormatBlockRef(state.Block), err)
	}
	if extra.ShardHashes == nil {
		return nil, fmt.Errorf("masterchain state %s does not contain shard hashes", storage.FormatBlockRef(state.Block))
	}

	shards, err := ton.LoadShardsFromHashes(extra.ShardHashes, false)
	if err != nil {
		return nil, fmt.Errorf("load shard hashes for %s: %w", storage.FormatBlockRef(state.Block), err)
	}

	res := make([]ton.BlockIDExt, 0, len(shards))
	for _, shard := range shards {
		res = append(res, *shard)
	}
	return res, nil
}
