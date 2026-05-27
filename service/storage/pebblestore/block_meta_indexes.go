package pebblestore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
)

func (s *Store) BlockState(ctx context.Context, block ton.BlockIDExt) (*storage.BlockState, error) {
	started := time.Now()
	state, err := s.blockStateMeta(ctx, block)
	if err != nil {
		return nil, err
	}

	event := s.log.Debug().
		Str("block", storage.FormatBlockRef(block))
	event.Msg("loading block state lazy root from storage")

	rootLoadStarted := time.Now()
	root, err := s.loadLazyCellFromGeneration(ctx, 0, state.StateRootHash)
	if err != nil {
		return nil, fmt.Errorf("load state root cell: %w", err)
	}
	s.log.Debug().
		Str("block", storage.FormatBlockRef(state.Block)).
		Dur("elapsed", time.Since(rootLoadStarted)).
		Msg("block state lazy root loaded")

	parsed, err := storage.ParseStateProof(&block, root, nil, state.StateRootHash, nil)
	if err != nil {
		return nil, fmt.Errorf("parse block state: %w", err)
	}
	parsed.StateFileHash = state.StateFileHash
	parsed.MasterchainRef = state.MasterchainRef

	s.log.Debug().
		Str("block", storage.FormatBlockRef(block)).
		Dur("elapsed", time.Since(started)).
		Msg("block state loaded")
	return parsed, nil
}

func (s *Store) blockStateMeta(ctx context.Context, block ton.BlockIDExt) (storage.BlockState, error) {
	metaRaw, err := s.getHotCopy(ctx, hotKeyStateMeta(block))
	if err != nil {
		return storage.BlockState{}, err
	}
	rootHash, fileHash, masterRef, err := decodeBlockStateMeta(metaRaw)
	if err != nil {
		return storage.BlockState{}, err
	}
	if len(rootHash) == 0 {
		return storage.BlockState{}, storage.ErrNotFound
	}

	return storage.BlockState{
		Block:          block,
		StateRootHash:  bytes.Clone(rootHash),
		StateFileHash:  bytes.Clone(fileHash),
		MasterchainRef: masterRef,
	}, nil
}

func (s *Store) SaveBlockMeta(meta *storage.BlockMeta) error {
	return s.mergeAndStoreBlockMeta(meta)
}

func (s *Store) BlockMeta(ctx context.Context, block ton.BlockIDExt) (*storage.BlockMeta, error) {
	raw, err := s.getHotCopy(ctx, hotKeyBlockMeta(block))
	if err != nil {
		return nil, err
	}
	meta, err := decodeBlockMeta(block, raw)
	if err != nil {
		return nil, err
	}
	return meta, nil
}

func (s *Store) LookupBlockBySeqNo(ctx context.Context, key storage.BlockHistoryKey, seqno uint32) (ton.BlockIDExt, error) {
	raw, err := s.getHotCopy(ctx, hotKeyBlockSeqIndex(key, seqno))
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	block, err := decodeBlockID(raw)
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	return block, nil
}

func (s *Store) NextKeyBlocks(ctx context.Context, after uint32, limit int) ([]ton.BlockIDExt, error) {
	if limit <= 0 || after == ^uint32(0) {
		return nil, storage.ErrNotFound
	}

	db, err := s.acquireHotDB(ctx)
	if err != nil {
		return nil, err
	}
	defer s.releaseHotDB()

	snap := db.NewSnapshot()
	defer func() { _ = snap.Close() }()

	iter, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: bytes.Clone(hotPrefixKeyBlockSeq),
		UpperBound: appendPrefixUpperBound(hotPrefixKeyBlockSeq),
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = iter.Close() }()

	blocks := make([]ton.BlockIDExt, 0, limit)
	for ok := iter.SeekGE(hotKeyKeyBlockSeqIndex(after + 1)); ok && len(blocks) < limit; ok = iter.Next() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		block, err := decodeBlockID(iter.Value())
		if err != nil {
			return nil, err
		}
		if !isMasterchainBlock(block) {
			continue
		}

		raw, closer, err := pebbleReaderGet(snap, hotKeyBlockMeta(block))
		if errors.Is(err, storage.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		meta, err := decodeBlockMeta(block, raw)
		_ = closer.Close()
		if err != nil {
			return nil, err
		}
		if !meta.Has(storage.BlockMetaIsKeyBlock) {
			continue
		}
		blocks = append(blocks, block)
	}
	if err = iter.Error(); err != nil {
		return nil, err
	}
	if len(blocks) == 0 {
		return nil, storage.ErrNotFound
	}
	return blocks, nil
}

func (s *Store) LookupBlockByLT(ctx context.Context, key storage.BlockHistoryKey, lt uint64) (ton.BlockIDExt, error) {
	prefix := hotKeyBlockLTPrefix(key)

	db, err := s.acquireHotDB(ctx)
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	defer s.releaseHotDB()

	snap := db.NewSnapshot()
	defer func() { _ = snap.Close() }()

	iter, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	defer func() { _ = iter.Close() }()

	block, err := lookupBlockByLTWithIterator(iter, key, lt)
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	return block, iter.Error()
}

func (s *Store) LookupBlockByAccountLT(ctx context.Context, workchain int32, account []byte, lt uint64) (ton.BlockIDExt, error) {
	shards := storage.AccountShardCandidates(workchain, account)
	if len(shards) == 0 {
		return ton.BlockIDExt{}, storage.ErrNotFound
	}

	prefix := hotKeyBlockLTWorkchainPrefix(workchain)

	db, err := s.acquireHotDB(ctx)
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	defer s.releaseHotDB()

	snap := db.NewSnapshot()
	defer func() { _ = snap.Close() }()

	iter, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	defer func() { _ = iter.Close() }()

	var best ton.BlockIDExt
	found := false

	for _, shard := range shards {
		select {
		case <-ctx.Done():
			return ton.BlockIDExt{}, ctx.Err()
		default:
		}

		block, err := lookupBlockByLTWithIterator(iter, storage.BlockHistoryKey{Workchain: workchain, Shard: shard}, lt)
		if errors.Is(err, storage.ErrNotFound) {
			continue
		}
		if err != nil {
			return ton.BlockIDExt{}, err
		}

		if !found || best.SeqNo > block.SeqNo {
			best = block
			found = true
		}
	}
	if err = iter.Error(); err != nil {
		return ton.BlockIDExt{}, err
	}
	if found {
		return best, nil
	}
	return ton.BlockIDExt{}, storage.ErrNotFound
}

func lookupBlockByLTWithIterator(iter *pebble.Iterator, key storage.BlockHistoryKey, lt uint64) (ton.BlockIDExt, error) {
	prefix := hotKeyBlockLTPrefix(key)
	if !iter.SeekGE(hotKeyBlockLTSeekGE(key, lt)) || !bytes.HasPrefix(iter.Key(), prefix) {
		return ton.BlockIDExt{}, storage.ErrNotFound
	}
	return decodeBlockID(iter.Value())
}

func (s *Store) LookupBlockByUnixTime(ctx context.Context, key storage.BlockHistoryKey, utime uint32) (ton.BlockIDExt, error) {
	return s.lookupBlockByLowerBoundIndex(ctx, hotKeyBlockUTimePrefix(key), hotKeyBlockUTimeSeekGE(key, utime))
}

func (s *Store) mergeAndStoreBlockMeta(next *storage.BlockMeta) error {
	db, err := s.acquireHotDB(context.Background())
	if err != nil {
		return err
	}
	defer s.releaseHotDB()

	s.hotWriteMu.Lock()
	defer s.hotWriteMu.Unlock()
	hotBatch := db.NewBatch()
	defer func() { _ = hotBatch.Close() }()

	if err := s.setMergedBlockMeta(hotBatch, next); err != nil {
		return err
	}
	return hotBatch.Commit(pebble.NoSync)
}

func (s *Store) setMergedBlockMeta(batch *pebble.Batch, next *storage.BlockMeta) error {
	key := hotKeyBlockMeta(next.ID)
	existingRaw, closer, err := pebbleReaderGet(s.hot, key)
	existed := false
	if err != nil {
		if !errors.Is(err, storage.ErrNotFound) {
			return err
		}
		existingRaw = nil
	} else {
		defer func() { _ = closer.Close() }()
		existed = true
	}

	var existing *storage.BlockMeta
	if existed {
		existing, err = decodeBlockMeta(next.ID, existingRaw)
		if err != nil {
			return err
		}
	}
	merged := storage.MergeBlockMeta(existing, next)
	encoded := encodeBlockMeta(merged)

	if err = batch.Set(key, encoded, pebble.NoSync); err != nil {
		return err
	}
	if err = batch.Set(hotKeyBlockSeqIndex(storage.BlockHistoryKey{Workchain: merged.ID.Workchain, Shard: merged.ID.Shard}, merged.ID.SeqNo), encodeBlockID(merged.ID), pebble.NoSync); err != nil {
		return err
	}
	if isMasterchainBlock(merged.ID) && merged.Has(storage.BlockMetaIsKeyBlock) {
		if err = batch.Set(hotKeyKeyBlockSeqIndex(merged.ID.SeqNo), encodeBlockID(merged.ID), pebble.NoSync); err != nil {
			return err
		}
	}
	if merged.EndLT != 0 {
		if err = batch.Set(hotKeyBlockLTIndex(merged), encodeBlockID(merged.ID), pebble.NoSync); err != nil {
			return err
		}
	}
	if merged.GenUTime != 0 {
		if err = batch.Set(hotKeyBlockUTimeIndex(merged), encodeBlockID(merged.ID), pebble.NoSync); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) lookupBlockByLowerBoundIndex(ctx context.Context, prefix []byte, seek []byte) (ton.BlockIDExt, error) {
	db, err := s.acquireHotDB(ctx)
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	defer s.releaseHotDB()

	snap := db.NewSnapshot()
	defer func() { _ = snap.Close() }()

	iter, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	defer func() { _ = iter.Close() }()

	select {
	case <-ctx.Done():
		return ton.BlockIDExt{}, ctx.Err()
	default:
	}

	if !iter.SeekGE(seek) {
		return ton.BlockIDExt{}, storage.ErrNotFound
	}
	if !bytes.HasPrefix(iter.Key(), prefix) {
		return ton.BlockIDExt{}, storage.ErrNotFound
	}
	block, err := decodeBlockID(iter.Value())
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	return block, nil
}
