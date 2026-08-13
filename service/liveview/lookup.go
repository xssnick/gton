package liveview

import (
	"bytes"
	"context"
	"errors"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func (s *Store) BlockState(ctx context.Context, block ton.BlockIDExt) (*storage.BlockState, error) {
	state, err := s.cachedBlockState(block)
	if err == nil && (block.SeqNo != 0 || state.Cell != nil) {
		return state, nil
	}
	if !s.backingBlockAllowed(block) {
		return nil, storage.ErrNotFound
	}

	return s.backing.BlockState(ctx, block)
}

func (s *Store) LoadStateCellTree(ctx context.Context, block ton.BlockIDExt, rootHash []byte) (*cell.Cell, error) {
	state, err := s.cachedBlockState(block)
	if err == nil && state.Cell != nil {
		if len(rootHash) > 0 && !bytes.Equal(state.StateRootHash, rootHash) {
			return nil, storage.ErrNotFound
		}

		hash := state.Cell.HashKeyAt(0)
		if !bytes.Equal(hash[:], state.StateRootHash) {
			return nil, storage.ErrNotFound
		}
		return state.Cell, nil
	}

	if !s.backingBlockAllowed(block) {
		return nil, storage.ErrNotFound
	}
	return s.backing.LoadStateCellTree(ctx, block, rootHash)
}

func (s *Store) BlockMeta(ctx context.Context, block ton.BlockIDExt) (*storage.BlockMeta, error) {
	if cached, err := s.cachedBlockMeta(block); err == nil {
		return cached, nil
	}
	if !s.backingBlockAllowed(block) {
		return nil, storage.ErrNotFound
	}
	return s.backing.BlockMeta(ctx, block)
}

func (s *Store) LookupBlockBySeqNo(ctx context.Context, ref storage.BlockSeqRef) (ton.BlockIDExt, error) {
	if block, err := s.cachedBlockBySeqNo(ref); err == nil {
		return block, nil
	}
	if !s.backingSeqnoLookupAllowed(ref) {
		return ton.BlockIDExt{}, storage.ErrNotFound
	}
	block, err := s.backing.LookupBlockBySeqNo(ctx, ref)
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	if !s.backingBlockAllowed(block) {
		return ton.BlockIDExt{}, storage.ErrNotFound
	}
	return block, nil
}

func lookupBackingBlockBySeqNoForPrefix(ctx context.Context, backing Backing, ref storage.BlockSeqRef) (ton.BlockIDExt, error) {
	if prefix, ok := backing.(prefixBacking); ok {
		return prefix.LookupBlockBySeqNoForPrefix(ctx, ref)
	}
	// Backing predates prefix-aware lookups. The exact-shard methods preserve
	// released behavior for implementations without the optional capability.
	return backing.LookupBlockBySeqNo(ctx, ref)
}

func (s *Store) LookupBlockBySeqNoForPrefix(ctx context.Context, ref storage.BlockSeqRef) (ton.BlockIDExt, error) {
	if block, err := s.cachedBlockBySeqNo(ref); err == nil {
		return block, nil
	}
	if !s.backingSeqnoLookupAllowed(ref) {
		best, err := s.cachedBlockBySeqNoForPrefix(ref)
		if err != nil {
			return ton.BlockIDExt{}, err
		}
		return best.block, nil
	}

	block, err := lookupBackingBlockBySeqNoForPrefix(ctx, s.backing, ref)
	if err == nil && s.backingBlockAllowed(block) {
		return block, nil
	} else if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return ton.BlockIDExt{}, err
	}
	best, err := s.cachedBlockBySeqNoForPrefix(ref)
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	return best.block, nil
}

func (s *Store) LookupBlockByLT(ctx context.Context, key storage.BlockHistoryKey, lt uint64) (ton.BlockIDExt, error) {
	if block, err := s.cachedBlockByLT(key, lt); err == nil {
		return block, nil
	}
	return s.lookupBackingBlockByHistory(ctx, key, logicalTimeIndexLookup(lt))
}

func (s *Store) LookupBlockByLTForPrefix(ctx context.Context, key storage.BlockHistoryKey, lt uint64) (ton.BlockIDExt, error) {
	if block, err := s.cachedDirectBlockByLTForPrefix(key, lt); err == nil {
		return block, nil
	}
	return s.lookupBlockByHistoryForPrefixAfterCache(ctx, key, logicalTimeIndexLookup(lt))
}

func (s *Store) LookupBlockByAccountLT(ctx context.Context, workchain int32, account []byte, lt uint64) (ton.BlockIDExt, error) {
	var best ton.BlockIDExt
	found := false
	candidates, err := storage.AccountShardCandidates(workchain, account)
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	for _, shard := range candidates {
		block, err := s.cachedBlockByLT(storage.BlockHistoryKey{Workchain: workchain, Shard: shard}, lt)
		if err == nil {
			if !found || best.SeqNo > block.SeqNo {
				best = block
				found = true
			}
		}
	}
	if found {
		return best, nil
	}
	block, err := s.backing.LookupBlockByAccountLT(ctx, workchain, account, lt)
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	if !s.backingBlockAllowed(block) {
		return ton.BlockIDExt{}, storage.ErrNotFound
	}
	return block, nil
}

func (s *Store) LookupBlockByUnixTime(ctx context.Context, key storage.BlockHistoryKey, utime uint32) (ton.BlockIDExt, error) {
	if block, err := s.cachedBlockByUnixTime(key, utime); err == nil {
		return block, nil
	}
	return s.lookupBackingBlockByHistory(ctx, key, unixTimeIndexLookup(utime))
}

func (s *Store) LookupBlockByUnixTimeForPrefix(ctx context.Context, key storage.BlockHistoryKey, utime uint32) (ton.BlockIDExt, error) {
	if block, err := s.cachedDirectBlockByUnixTimeForPrefix(key, utime); err == nil {
		return block, nil
	}
	return s.lookupBlockByHistoryForPrefixAfterCache(ctx, key, unixTimeIndexLookup(utime))
}

type blockPrefixCandidate struct {
	block           ton.BlockIDExt
	exact           bool
	artifactFlushed bool
}

func (s *Store) BlockRoot(ctx context.Context, block ton.BlockIDExt) (*cell.Cell, error) {
	if root, err := s.cachedBlockRoot(block); err == nil {
		return root, nil
	}
	if !s.backingBlockAllowed(block) {
		return nil, storage.ErrNotFound
	}

	loaded, err := s.loadStoredBlock(ctx, block)
	if err != nil {
		return nil, err
	}
	return loaded.root, nil
}

func (s *Store) BlockData(ctx context.Context, block ton.BlockIDExt) ([]byte, error) {
	data, err := s.liveBlockCache.BlockData(ctx, block)
	if err == nil {
		return data, nil
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return nil, err
	}
	if !s.backingBlockAllowed(block) {
		return nil, storage.ErrNotFound
	}

	data, err = s.loadStoredBlockData(ctx, block)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (s *Store) BlockProof(ctx context.Context, kind storage.ServedProofKind, block ton.BlockIDExt) ([]byte, error) {
	proof, err := s.liveBlockCache.BlockProof(ctx, kind, block)
	if err == nil {
		return proof, nil
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return nil, err
	}
	if !s.backingBlockAllowed(block) {
		return nil, storage.ErrNotFound
	}
	return s.backing.BlockProof(ctx, kind, block)
}

func (s *Store) BlockFragments(ctx context.Context, block ton.BlockIDExt) (*BlockView, error) {
	if fragments, err := s.cachedBlockFragments(block); err == nil {
		return fragments, nil
	}
	key, ok := liveBlockLookupKeyFromBlock(block)
	if !ok {
		return nil, storage.ErrNotFound
	}

	fragments, err := s.fragmentLoad.do(ctx, key, func() (*BlockView, error) {
		// Load detached from the initiating request so one disconnecting client
		// cannot fail the shared result for concurrent waiters.
		ctx := context.WithoutCancel(ctx)
		if fragments, err := s.cachedBlockFragments(block); err == nil {
			return fragments, nil
		}

		blockRoot, err := s.BlockRoot(ctx, block)
		if err != nil {
			return nil, err
		}

		stateRootHash, err := stateRootHashFromBlock(block, blockRoot)
		if err != nil {
			return nil, err
		}

		stateRoot, err := s.LoadStateCellTree(ctx, block, stateRootHash)
		if err != nil {
			return nil, err
		}

		fragments, err := NewBlockView(block, blockRoot, stateRoot)
		if err != nil {
			return nil, err
		}
		// Prewarmed like the publish path: this rebuild also wins the race
		// against a deferred prewarm whose block was queried the moment it was
		// published, and a view installed cold there would stay cold.
		if err = prewarmFragments(fragments); err != nil {
			return nil, err
		}
		return s.rememberBlockFragments(block, fragments), nil
	})
	if err != nil {
		return nil, err
	}
	return fragments, nil
}

func (s *Store) loadStoredBlockData(ctx context.Context, block ton.BlockIDExt) ([]byte, error) {
	key, ok := liveBlockLookupKeyFromBlock(block)
	if !ok {
		return nil, storage.ErrNotFound
	}

	data, err := s.blockDataLoad.do(ctx, key, func() ([]byte, error) {
		// Load detached from the initiating request so one disconnecting client
		// cannot fail the shared result for concurrent waiters.
		ctx := context.WithoutCancel(ctx)
		cached, err := s.liveBlockCache.CachedBlockData(ctx, block)
		if err == nil {
			return cached.Data, nil
		}
		if !errors.Is(err, storage.ErrNotFound) {
			return nil, err
		}

		data, err := s.backing.BlockData(ctx, block)
		if err != nil {
			return nil, err
		}

		err = s.liveBlockCache.PublishLiveBlockArtifacts(storage.LiveBlockCacheArtifacts{
			Block:           block,
			BlockData:       data,
			ArtifactFlushed: true,
		})
		if err != nil {
			return nil, err
		}
		return data, nil
	})
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (s *Store) loadStoredBlock(ctx context.Context, block ton.BlockIDExt) (*liveBlockLoadResult, error) {
	key, ok := liveBlockLookupKeyFromBlock(block)
	if !ok {
		return nil, storage.ErrNotFound
	}

	loaded, err := s.blockLoad.do(ctx, key, func() (*liveBlockLoadResult, error) {
		// Load detached from the initiating request so one disconnecting client
		// cannot fail the shared result for concurrent waiters.
		ctx := context.WithoutCancel(ctx)
		cached, err := s.liveBlockCache.CachedBlockData(ctx, block)
		if err == nil {
			data := cached.Data
			root, rootErr := s.cachedBlockRoot(block)
			if rootErr != nil {
				parsed, err := ParseTrustedBlockBOC(block, data)
				if err != nil {
					return nil, err
				}
				root = parsed
				if err = s.PublishLiveBlockArtifacts(storage.LiveBlockArtifacts{
					Block:           block,
					Root:            root,
					BlockData:       data,
					ArtifactFlushed: cached.ArtifactFlushed,
				}); err != nil {
					return nil, err
				}
			}
			return &liveBlockLoadResult{root: root, data: data}, nil
		}
		if !errors.Is(err, storage.ErrNotFound) {
			return nil, err
		}

		data, err := s.backing.BlockData(ctx, block)
		if err != nil {
			return nil, err
		}

		root, err := ParseTrustedBlockBOC(block, data)
		if err != nil {
			return nil, err
		}
		if err = s.PublishLiveBlockArtifacts(storage.LiveBlockArtifacts{
			Block:           block,
			Root:            root,
			BlockData:       data,
			ArtifactFlushed: true,
		}); err != nil {
			return nil, err
		}
		return &liveBlockLoadResult{root: root, data: data}, nil
	})
	if err != nil {
		return nil, err
	}
	return loaded, nil
}

func (s *Store) ZeroState(ctx context.Context, block ton.BlockIDExt) ([]byte, error) {
	return s.backing.ZeroState(ctx, block)
}

// MasterchainSeqnoReady reports whether the given masterchain seqno is already
// served without waiting.
