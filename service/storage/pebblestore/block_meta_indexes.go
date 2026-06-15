package pebblestore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
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
	db, err := s.acquireHotDB(ctx)
	if err != nil {
		return storage.BlockState{}, err
	}
	defer s.releaseHotDB()

	return blockStateMetaFromReader(db, block)
}

func blockStateMetaFromReader(reader pebbleReader, block ton.BlockIDExt) (storage.BlockState, error) {
	metaRaw, closer, err := pebbleReaderGet(reader, hotKeyBlockMeta(block))
	if err != nil {
		return storage.BlockState{}, err
	}
	defer func() { _ = closer.Close() }()

	meta, err := decodeBlockMeta(block, metaRaw)
	if err != nil {
		return storage.BlockState{}, err
	}
	if !meta.Has(storage.BlockMetaHasStateCells) || len(meta.StateRootHash) == 0 {
		return storage.BlockState{}, storage.ErrNotFound
	}
	masterRef, err := blockStateMasterchainRefFromMeta(reader, meta)
	if err != nil {
		return storage.BlockState{}, err
	}

	return storage.BlockState{
		Block:          block,
		StateRootHash:  bytes.Clone(meta.StateRootHash),
		StateFileHash:  bytes.Clone(meta.StateFileHash),
		MasterchainRef: masterRef,
	}, nil
}

func blockStateMasterchainRefFromMeta(reader pebbleReader, meta *storage.BlockMeta) (*ton.BlockIDExt, error) {
	if meta == nil || isMasterchainBlock(meta.ID) || !meta.MasterchainRefKnown() {
		return nil, nil
	}

	ref, err := lookupBlockBySeqNoWithReader(reader, storage.BlockHistoryKey{Workchain: -1, Shard: topShard}, meta.MasterchainRefSeqno)
	if err != nil {
		return nil, fmt.Errorf("lookup masterchain ref #%d: %w", meta.MasterchainRefSeqno, err)
	}
	return &ref, nil
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
	return s.lookupBlockBySeqNo(ctx, key, seqno)
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

		seqno, err := decodeKeyBlockSeqIndexKey(iter.Key())
		if err != nil {
			return nil, err
		}
		block, err := decodeBlockIDFromHashes(-1, topShard, seqno, iter.Value())
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

	ref, err := lookupBlockByLTWithIterator(iter, key, lt)
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	return ref.block, iter.Error()
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

	var best blockLTIndexRef
	found := false

	for _, shard := range shards {
		select {
		case <-ctx.Done():
			return ton.BlockIDExt{}, ctx.Err()
		default:
		}

		ref, err := lookupBlockByLTWithIterator(iter, storage.BlockHistoryKey{Workchain: workchain, Shard: shard}, lt)
		if errors.Is(err, storage.ErrNotFound) {
			continue
		}
		if err != nil {
			return ton.BlockIDExt{}, err
		}

		if !found || best.seqno > ref.seqno {
			best = ref
			found = true
		}
	}
	if err = iter.Error(); err != nil {
		return ton.BlockIDExt{}, err
	}
	if found {
		return best.block, nil
	}
	return ton.BlockIDExt{}, storage.ErrNotFound
}

type blockSeqIndexRef struct {
	key   storage.BlockHistoryKey
	seqno uint32
}

type blockLTIndexRef struct {
	block ton.BlockIDExt
	seqno uint32
}

func lookupBlockByLTWithIterator(iter *pebble.Iterator, key storage.BlockHistoryKey, lt uint64) (blockLTIndexRef, error) {
	prefix := hotKeyBlockLTPrefix(key)
	if !iter.SeekGE(hotKeyBlockLTSeekGE(key, lt)) || !bytes.HasPrefix(iter.Key(), prefix) {
		return blockLTIndexRef{}, storage.ErrNotFound
	}
	ref, err := decodeBlockLTIndexKey(iter.Key())
	if err != nil {
		return blockLTIndexRef{}, err
	}
	block, err := decodeBlockIDFromHashes(ref.key.Workchain, ref.key.Shard, ref.seqno, iter.Value())
	if err != nil {
		return blockLTIndexRef{}, err
	}
	return blockLTIndexRef{block: block, seqno: ref.seqno}, nil
}

func (s *Store) LookupBlockByUnixTime(ctx context.Context, key storage.BlockHistoryKey, utime uint32) (ton.BlockIDExt, error) {
	return s.lookupBlockByLowerBoundIndex(ctx, hotKeyBlockUTimePrefix(key), hotKeyBlockUTimeSeekGE(key, utime), decodeBlockUTimeIndexKey)
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
	if err = batch.Set(hotKeyBlockSeqIndex(storage.BlockHistoryKey{Workchain: merged.ID.Workchain, Shard: merged.ID.Shard}, merged.ID.SeqNo), encodeBlockIDHashes(merged.ID), pebble.NoSync); err != nil {
		return err
	}
	if isMasterchainBlock(merged.ID) && merged.Has(storage.BlockMetaIsKeyBlock) {
		if err = batch.Set(hotKeyKeyBlockSeqIndex(merged.ID.SeqNo), encodeBlockIDHashes(merged.ID), pebble.NoSync); err != nil {
			return err
		}
	}
	if merged.EndLT != 0 {
		if err = batch.Set(hotKeyBlockLTIndex(merged), encodeBlockIDHashes(merged.ID), pebble.NoSync); err != nil {
			return err
		}
	}
	if merged.GenUTime != 0 {
		if err = batch.Set(hotKeyBlockUTimeIndex(merged), encodeBlockIDHashes(merged.ID), pebble.NoSync); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) setMergedBlockMetas(batch *pebble.Batch, metas map[storage.BlockRootHash]*storage.BlockMeta) error {
	for _, key := range sortedBlockMetaKeys(metas) {
		if err := s.setMergedBlockMeta(batch, metas[key]); err != nil {
			return err
		}
	}
	return nil
}

func sortedBlockMetaKeys(metas map[storage.BlockRootHash]*storage.BlockMeta) []storage.BlockRootHash {
	keys := make([]storage.BlockRootHash, 0, len(metas))
	for key := range metas {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i][:], keys[j][:]) < 0
	})
	return keys
}

func (s *Store) lookupBlockBySeqNo(ctx context.Context, key storage.BlockHistoryKey, seqno uint32) (ton.BlockIDExt, error) {
	db, err := s.acquireHotDB(ctx)
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	defer s.releaseHotDB()

	return lookupBlockBySeqNoWithReader(db, key, seqno)
}

func lookupBlockBySeqNoWithReader(reader pebbleReader, key storage.BlockHistoryKey, seqno uint32) (ton.BlockIDExt, error) {
	raw, closer, err := pebbleReaderGet(reader, hotKeyBlockSeqIndex(key, seqno))
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	defer func() { _ = closer.Close() }()
	return decodeBlockIDFromHashes(key.Workchain, key.Shard, seqno, raw)
}

func (s *Store) lookupBlockByLowerBoundIndex(ctx context.Context, prefix []byte, seek []byte, decodeKey func([]byte) (blockSeqIndexRef, error)) (ton.BlockIDExt, error) {
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
	ref, err := decodeKey(iter.Key())
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	return decodeBlockIDFromHashes(ref.key.Workchain, ref.key.Shard, ref.seqno, iter.Value())
}

func decodeKeyBlockSeqIndexKey(key []byte) (uint32, error) {
	if len(key) != len(hotPrefixKeyBlockSeq)+4 || !bytes.HasPrefix(key, hotPrefixKeyBlockSeq) {
		return 0, fmt.Errorf("invalid key block seq index key size %d", len(key))
	}
	return binary.BigEndian.Uint32(key[len(hotPrefixKeyBlockSeq):]), nil
}

func decodeBlockLTIndexKey(key []byte) (blockSeqIndexRef, error) {
	const suffix = 8 + 4
	prefixLen := len(hotPrefixBlockLT)
	if len(key) != prefixLen+4+8+suffix || !bytes.HasPrefix(key, hotPrefixBlockLT) {
		return blockSeqIndexRef{}, fmt.Errorf("invalid block lt index key size %d", len(key))
	}
	pos := prefixLen
	historyKey := storage.BlockHistoryKey{
		Workchain: int32(binary.BigEndian.Uint32(key[pos : pos+4])),
		Shard:     int64(binary.BigEndian.Uint64(key[pos+4 : pos+12])),
	}
	pos += 4 + 8 + 8
	return blockSeqIndexRef{key: historyKey, seqno: binary.BigEndian.Uint32(key[pos : pos+4])}, nil
}

func decodeBlockUTimeIndexKey(key []byte) (blockSeqIndexRef, error) {
	const suffix = 4 + 4
	prefixLen := len(hotPrefixBlockUTime)
	if len(key) != prefixLen+4+8+suffix || !bytes.HasPrefix(key, hotPrefixBlockUTime) {
		return blockSeqIndexRef{}, fmt.Errorf("invalid block utime index key size %d", len(key))
	}
	pos := prefixLen
	historyKey := storage.BlockHistoryKey{
		Workchain: int32(binary.BigEndian.Uint32(key[pos : pos+4])),
		Shard:     int64(binary.BigEndian.Uint64(key[pos+4 : pos+12])),
	}
	pos += 4 + 8 + 4
	return blockSeqIndexRef{key: historyKey, seqno: binary.BigEndian.Uint32(key[pos : pos+4])}, nil
}

func cloneBlockIDPtr(ref *ton.BlockIDExt) *ton.BlockIDExt {
	if ref == nil {
		return nil
	}
	cloned := *ref
	return &cloned
}
