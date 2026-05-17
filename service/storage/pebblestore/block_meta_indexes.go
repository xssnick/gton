package pebblestore

import (
	"bytes"
	"context"
	"errors"
	"github.com/xssnick/gton/service/storage"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble/v2"
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
	root, err := s.loadLazyCellFromGeneration(ctx, 0, state.StateCellHash)
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
	rootHash, cellHash, fileHash, masterRef, err := decodeBlockStateMeta(metaRaw)
	if err != nil {
		return storage.BlockState{}, err
	}
	if len(rootHash) == 0 {
		return storage.BlockState{}, storage.ErrNotFound
	}

	return storage.BlockState{
		Block:          block,
		StateRootHash:  bytes.Clone(rootHash),
		StateCellHash:  bytes.Clone(cellHash),
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

func (s *Store) LookupBlockByLT(ctx context.Context, key storage.BlockHistoryKey, lt uint64) (ton.BlockIDExt, error) {
	prefix := hotKeyBlockLTPrefix(key)
	var geBlock ton.BlockIDExt
	var geFound bool
	var ltBlock ton.BlockIDExt
	var ltFound bool

	if err := func() error {
		db, err := s.acquireHotDB(ctx)
		if err != nil {
			return err
		}
		defer s.releaseHotDB()

		snap := db.NewSnapshot()
		defer func() { _ = snap.Close() }()

		iter, err := snap.NewIter(&pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: prefixUpperBound(prefix),
		})
		if err != nil {
			return err
		}
		defer func() { _ = iter.Close() }()

		seekGE := hotKeyBlockLTSeekGE(key, lt)
		if iter.SeekGE(seekGE) && bytes.HasPrefix(iter.Key(), prefix) {
			block, err := decodeBlockID(iter.Value())
			if err != nil {
				return err
			}
			geBlock = block
			geFound = true
		}

		seekLT := hotKeyBlockLTSeek(key, lt)
		if iter.SeekLT(seekLT) && bytes.HasPrefix(iter.Key(), prefix) {
			block, err := decodeBlockID(iter.Value())
			if err != nil {
				return err
			}
			ltBlock = block
			ltFound = true
		}
		return nil
	}(); err != nil {
		return ton.BlockIDExt{}, err
	}

	if geFound {
		meta, err := s.BlockMeta(ctx, geBlock)
		switch {
		case err == nil && meta.StartLT <= lt && (meta.EndLT == 0 || lt <= meta.EndLT):
			return geBlock, nil
		case err == nil:
		case errors.Is(err, storage.ErrNotFound):
		default:
			return ton.BlockIDExt{}, err
		}
	}

	if !ltFound {
		return ton.BlockIDExt{}, storage.ErrNotFound
	}
	return ltBlock, nil
}

func (s *Store) LookupBlockByUnixTime(ctx context.Context, key storage.BlockHistoryKey, utime uint32) (ton.BlockIDExt, error) {
	return s.lookupBlockByBoundedIndex(ctx, hotKeyBlockUTimePrefix(key), hotKeyBlockUTimeSeek(key, utime))
}

func (s *Store) mergeAndStoreBlockMeta(next *storage.BlockMeta) error {
	if next == nil {
		return fmt.Errorf("block meta is nil")
	}
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
	if next == nil {
		return fmt.Errorf("block meta is nil")
	}

	key := hotKeyBlockMeta(next.ID)
	existingRaw, err := pebbleReaderGetCopy(s.hot, key)
	existed := false
	if err != nil {
		if !errors.Is(err, storage.ErrNotFound) {
			return err
		}
		existingRaw = nil
	} else {
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

func (s *Store) lookupBlockByBoundedIndex(ctx context.Context, prefix []byte, seek []byte) (ton.BlockIDExt, error) {
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

	if !iter.SeekLT(seek) {
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
