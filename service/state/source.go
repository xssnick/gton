package state

import (
	"context"
	"flexserver/service/p2p"
	"flexserver/service/storage"
	"fmt"
	"math/bits"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type Source interface {
	LatestMasterchainBlock(ctx context.Context) (ton.BlockIDExt, error)
	PersistentMasterchainBlock(ctx context.Context, latest ton.BlockIDExt) (ton.BlockIDExt, error)
	ShardBlocks(ctx context.Context, master ton.BlockIDExt) ([]ton.BlockIDExt, error)
	DownloadState(ctx context.Context, block ton.BlockIDExt, master ton.BlockIDExt, splitDepth uint32) (DownloadedState, error)
}

const (
	keyBlockLookupLimit        = 8
	initialStateMinAge         = time.Hour
	initialStateDownloadWindow = 8 * time.Hour
)

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
	s.log.Info().Msg("waiting for latest masterchain block before state sync")

	block, err := s.node.WaitMasterchainBlock(ctx)
	if err != nil {
		return ton.BlockIDExt{}, err
	}

	s.log.Info().
		Str("latest", storage.FormatBlockRef(block)).
		Msg("latest masterchain block received")
	return block, nil
}

func (s *P2PSource) PersistentMasterchainBlock(ctx context.Context, latest ton.BlockIDExt) (ton.BlockIDExt, error) {
	initBlock, ok := s.node.TrustedInitBlock()
	if !ok || initBlock.SeqNo >= latest.SeqNo {
		s.log.Debug().
			Str("latest", storage.FormatBlockRef(latest)).
			Msg("using latest masterchain block for state snapshot")
		return latest, nil
	}

	s.log.Info().
		Str("from", storage.FormatBlockRef(initBlock)).
		Str("latest", storage.FormatBlockRef(latest)).
		Msg("selecting persistent masterchain state")

	blocks, err := s.keyBlockIDs(ctx, initBlock, latest)
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	if len(blocks) == 0 {
		return latest, nil
	}

	block, ok, err := s.bestPersistentKeyBlock(ctx, blocks, time.Now())
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	if ok {
		return block, nil
	}

	s.log.Info().
		Str("block", storage.FormatBlockRef(blocks[0])).
		Int("key_blocks", len(blocks)).
		Msg("using oldest known key block for state snapshot")
	return blocks[0], nil
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

func (s *P2PSource) DownloadState(ctx context.Context, block ton.BlockIDExt, master ton.BlockIDExt, splitDepth uint32) (DownloadedState, error) {
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

	state, err := downloaded.Decode(ctx, nil)
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

type keyBlockCandidate struct {
	block ton.BlockIDExt
	utime uint32
}

func (s *P2PSource) keyBlockIDs(ctx context.Context, initBlock ton.BlockIDExt, latest ton.BlockIDExt) ([]ton.BlockIDExt, error) {
	all := []ton.BlockIDExt{initBlock}
	from := initBlock

	for from.SeqNo < latest.SeqNo {
		s.log.Info().
			Str("from", storage.FormatBlockRef(from)).
			Str("latest", storage.FormatBlockRef(latest)).
			Int("limit", keyBlockLookupLimit).
			Msg("requesting next key blocks")

		batch, _, err := s.node.NextKeyBlocks(ctx, from, keyBlockLookupLimit)
		if err != nil {
			return nil, fmt.Errorf("load next key blocks after %s: %w", storage.FormatBlockRef(from), err)
		}
		if len(batch) == 0 {
			break
		}

		advanced := false
		firstAccepted := ton.BlockIDExt{}
		lastAccepted := ton.BlockIDExt{}
		accepted := 0
		for _, block := range batch {
			if block.SeqNo <= from.SeqNo {
				continue
			}
			if block.SeqNo > latest.SeqNo {
				return all, nil
			}

			if accepted == 0 {
				firstAccepted = block
			}
			lastAccepted = block
			accepted++
			all = append(all, block)
			from = block
			advanced = true
		}

		evt := s.log.Info().
			Str("current", storage.FormatBlockRef(from)).
			Str("latest", storage.FormatBlockRef(latest)).
			Int("batch", len(batch)).
			Int("accepted", accepted).
			Int("key_blocks", len(all))
		if accepted > 0 {
			evt = evt.
				Str("first", storage.FormatBlockRef(firstAccepted)).
				Str("last", storage.FormatBlockRef(lastAccepted))
		}
		evt.Msg("key block lookup progress")

		if !advanced {
			break
		}
	}

	return all, nil
}

func (s *P2PSource) keyBlockCandidate(ctx context.Context, block ton.BlockIDExt) (keyBlockCandidate, error) {
	s.log.Info().
		Str("block", storage.FormatBlockRef(block)).
		Msg("downloading key block data for state snapshot selection")

	downloaded, err := s.node.DownloadBlockFull(ctx, block)
	if err != nil {
		return keyBlockCandidate{}, fmt.Errorf("download key block %s: %w", storage.FormatBlockRef(block), err)
	}

	if downloaded.Block == nil {
		return keyBlockCandidate{}, fmt.Errorf("downloaded key block %s is missing parsed cell", storage.FormatBlockRef(block))
	}

	meta, err := storage.BuildBlockMetaFromBlockCell(downloaded.ID, downloaded.Block)
	if err != nil {
		return keyBlockCandidate{}, fmt.Errorf("build key block meta %s: %w", storage.FormatBlockRef(block), err)
	}
	if !meta.Has(storage.BlockMetaIsKeyBlock) {
		return keyBlockCandidate{}, nil
	}

	return keyBlockCandidate{block: downloaded.ID, utime: meta.GenUTime}, nil
}

func (s *P2PSource) bestPersistentKeyBlock(ctx context.Context, blocks []ton.BlockIDExt, now time.Time) (ton.BlockIDExt, bool, error) {
	loaded := map[int]keyBlockCandidate{}
	load := func(idx int) (keyBlockCandidate, error) {
		if candidate, ok := loaded[idx]; ok {
			return candidate, nil
		}

		candidate, err := s.keyBlockCandidate(ctx, blocks[idx])
		if err != nil {
			return keyBlockCandidate{}, err
		}
		loaded[idx] = candidate
		return candidate, nil
	}

	nowUnix := uint64(now.Unix())
	minAge := uint64(initialStateMinAge / time.Second)
	minTTL := uint64(initialStateDownloadWindow / time.Second)

	for i := len(blocks) - 1; i >= 0; i-- {
		candidate, err := load(i)
		if err != nil {
			return ton.BlockIDExt{}, false, err
		}
		if candidate.utime == 0 {
			continue
		}

		persistent := i == 0
		if i > 0 {
			prev, err := load(i - 1)
			if err != nil {
				return ton.BlockIDExt{}, false, err
			}
			persistent = prev.utime != 0 && isPersistentState(candidate.utime, prev.utime)
		}

		ttl := persistentStateTTL(candidate.utime)
		s.log.Info().
			Str("block", storage.FormatBlockRef(candidate.block)).
			Bool("persistent", persistent).
			Time("expires_at", time.Unix(int64(ttl), 0)).
			Msg("checking state snapshot candidate")

		if uint64(candidate.utime)+minAge > nowUnix {
			continue
		}
		if !persistent {
			continue
		}
		if ttl <= nowUnix+minTTL {
			continue
		}

		s.log.Info().
			Str("block", storage.FormatBlockRef(candidate.block)).
			Msg("selected persistent masterchain state")
		return candidate.block, true, nil
	}

	return ton.BlockIDExt{}, false, nil
}

func choosePersistentKeyBlock(candidates []keyBlockCandidate, now time.Time) (ton.BlockIDExt, bool) {
	nowUnix := uint64(now.Unix())
	minAge := uint64(initialStateMinAge / time.Second)
	minTTL := uint64(initialStateDownloadWindow / time.Second)

	for i := len(candidates) - 1; i >= 0; i-- {
		candidate := candidates[i]
		if uint64(candidate.utime)+minAge > nowUnix {
			continue
		}

		persistent := i == 0 || isPersistentState(candidate.utime, candidates[i-1].utime)
		if !persistent {
			continue
		}
		if persistentStateTTL(candidate.utime) <= nowUnix+minTTL {
			continue
		}

		return candidate.block, true
	}

	return ton.BlockIDExt{}, false
}

func isPersistentState(ts, prevTS uint32) bool {
	return ts/(1<<17) != prevTS/(1<<17)
}

func persistentStateTTL(ts uint32) uint64 {
	x := uint64(ts) / (1 << 17)
	if x == 0 {
		return uint64(ts)
	}

	return uint64(ts) + ((uint64(1) << 18) << bits.TrailingZeros64(x))
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
