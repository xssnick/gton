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

	ref, err := lookupBlockBySeqNoWithReader(reader, storage.BlockSeqRef{Workchain: -1, Shard: topShard, SeqNo: meta.MasterchainRefSeqno})
	if err != nil {
		return nil, fmt.Errorf("lookup masterchain ref #%d: %w", meta.MasterchainRefSeqno, err)
	}
	return &ref, nil
}

func (s *Store) SaveBlockMeta(meta *storage.BlockMeta) error {
	if err := validateStoredBlockMetaUpdate(meta); err != nil {
		return err
	}
	return s.mergeAndStoreBlockMeta(meta)
}

func validateStoredBlockMetaUpdate(meta *storage.BlockMeta) error {
	if meta == nil {
		return fmt.Errorf("block meta is missing")
	}
	if len(meta.NextRefs) > 0 {
		return fmt.Errorf("block meta %s cannot set next refs directly", storage.FormatBlockRef(meta.ID))
	}
	if meta.Flags&servedBlockPayloadMetaFlags() != 0 {
		return fmt.Errorf("block meta %s cannot set artifact flags directly", storage.FormatBlockRef(meta.ID))
	}
	if !blockMetaHasStoredMetadata(meta) {
		return fmt.Errorf("block meta %s is empty", storage.FormatBlockRef(meta.ID))
	}
	if err := validateBlockMetaBlockIDHashes(meta); err != nil {
		return err
	}
	return nil
}

func validateBlockMetaBlockIDHashes(meta *storage.BlockMeta) error {
	if err := storage.ValidateBlockIDHashes(meta.ID); err != nil {
		return err
	}
	for _, ref := range meta.PrevRefs {
		if err := storage.ValidateBlockIDHashes(ref); err != nil {
			return err
		}
	}
	for _, ref := range meta.NextRefs {
		if err := storage.ValidateBlockIDHashes(ref); err != nil {
			return err
		}
	}
	return nil
}

func validateFixedHash(label string, hash []byte) error {
	if len(hash) == 32 {
		return nil
	}
	return fmt.Errorf("%s has invalid hash size %d", label, len(hash))
}

func blockMetaHasStoredMetadata(meta *storage.BlockMeta) bool {
	return meta.Flags&^servedBlockPayloadMetaFlags() != 0 ||
		meta.GenUTime != 0 ||
		meta.StartLT != 0 ||
		meta.EndLT != 0 ||
		len(meta.StateRootHash) != 0 ||
		len(meta.StateFileHash) != 0 ||
		meta.MasterchainRefSeqno != 0 ||
		len(meta.PrevRefs) != 0
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

func (s *Store) LookupBlockBySeqNo(ctx context.Context, ref storage.BlockSeqRef) (ton.BlockIDExt, error) {
	return s.lookupServedBlockBySeqNo(ctx, ref)
}

func (s *Store) NextKeyBlocks(ctx context.Context, after uint32, limit int) ([]ton.BlockIDExt, error) {
	metas, err := s.nextKeyBlockMetas(ctx, after, limit, true)
	if err != nil {
		return nil, err
	}

	blocks := make([]ton.BlockIDExt, 0, len(metas))
	for _, meta := range metas {
		blocks = append(blocks, meta.ID)
	}
	return blocks, nil
}

// NextKeyBlockMetas returns metadata for known key blocks with seqno > after in
// ascending order. Unlike NextKeyBlocks, which serves peers and therefore only
// returns blocks whose full data is served, it has no served-full requirement.
func (s *Store) NextKeyBlockMetas(ctx context.Context, after uint32, limit int) ([]*storage.BlockMeta, error) {
	return s.nextKeyBlockMetas(ctx, after, limit, false)
}

func (s *Store) nextKeyBlockMetas(ctx context.Context, after uint32, limit int, requireServedFull bool) ([]*storage.BlockMeta, error) {
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
		UpperBound: prefixUpperBound(hotPrefixKeyBlockSeq),
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = iter.Close() }()

	metas := make([]*storage.BlockMeta, 0, limit)
	for ok := iter.SeekGE(hotKeyKeyBlockSeqIndex(after + 1)); ok && len(metas) < limit; ok = iter.Next() {
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
		if requireServedFull && !meta.Has(storage.BlockMetaHasServedFull) {
			continue
		}
		metas = append(metas, meta)
	}
	if err = iter.Error(); err != nil {
		return nil, err
	}
	if len(metas) == 0 {
		return nil, storage.ErrNotFound
	}
	return metas, nil
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
		if iterErr := iter.Error(); iterErr != nil {
			return ton.BlockIDExt{}, iterErr
		}
		return ton.BlockIDExt{}, err
	}
	if err = iter.Error(); err != nil {
		return ton.BlockIDExt{}, err
	}
	if err = requireServedFullBlock(snap, ref.block); err != nil {
		return ton.BlockIDExt{}, err
	}
	return ref.block, nil
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
			if iterErr := iter.Error(); iterErr != nil {
				return ton.BlockIDExt{}, iterErr
			}
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
	if !found {
		return ton.BlockIDExt{}, storage.ErrNotFound
	}
	if err = requireServedFullBlock(snap, best.block); err != nil {
		return ton.BlockIDExt{}, err
	}
	return best.block, nil
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
	block, err := decodeBlockIDFromHashes(ref.Workchain, ref.Shard, ref.SeqNo, iter.Value())
	if err != nil {
		return blockLTIndexRef{}, err
	}
	return blockLTIndexRef{block: block, seqno: ref.SeqNo}, nil
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
	if err := validateBlockMetaBlockIDHashes(merged); err != nil {
		return err
	}
	encoded := encodeBlockMeta(merged)

	if err = batch.Set(key, encoded, pebble.NoSync); err != nil {
		return err
	}
	return setBlockMetaHistoryIndexes(batch, existing, merged)
}

func setBlockMetaHistoryIndexes(batch *pebble.Batch, existing *storage.BlockMeta, meta *storage.BlockMeta) error {
	if blockMetaIndexedInHistory(existing) && !blockMetaIndexedInHistory(meta) {
		return deleteBlockMetaHistoryIndexes(batch, existing)
	}
	if !blockMetaIndexedInHistory(meta) {
		return nil
	}
	if err := storage.ValidateBlockIDHashes(meta.ID); err != nil {
		return err
	}
	ref := storage.BlockSeqRefFromBlock(meta.ID)

	if blockMetaIndexedInHistory(existing) {
		existingRef := storage.BlockSeqRefFromBlock(existing.ID)
		if existing.EndLT != 0 && existing.EndLT != meta.EndLT {
			if err := batch.Delete(hotKeyBlockLTIndex(existingRef, existing.EndLT), pebble.NoSync); err != nil {
				return err
			}
		}
		if existing.GenUTime != 0 && existing.GenUTime != meta.GenUTime {
			if err := batch.Delete(hotKeyBlockUTimeIndex(existingRef, existing.GenUTime), pebble.NoSync); err != nil {
				return err
			}
		}
	}

	if err := batch.Set(hotKeyBlockSeqIndex(ref), encodeBlockIDHashes(meta.ID), pebble.NoSync); err != nil {
		return err
	}
	if isMasterchainBlock(meta.ID) && meta.Has(storage.BlockMetaIsKeyBlock) {
		if err := batch.Set(hotKeyKeyBlockSeqIndex(meta.ID.SeqNo), encodeBlockIDHashes(meta.ID), pebble.NoSync); err != nil {
			return err
		}
	}
	if meta.EndLT != 0 {
		if err := batch.Set(hotKeyBlockLTIndex(ref, meta.EndLT), encodeBlockIDHashes(meta.ID), pebble.NoSync); err != nil {
			return err
		}
	}
	if meta.GenUTime != 0 {
		if err := batch.Set(hotKeyBlockUTimeIndex(ref, meta.GenUTime), encodeBlockIDHashes(meta.ID), pebble.NoSync); err != nil {
			return err
		}
	}
	return nil
}

func blockMetaIndexedInHistory(meta *storage.BlockMeta) bool {
	return meta != nil && blockMetaHasStoredMetadata(meta)
}

func deleteBlockMetaHistoryIndexes(batch *pebble.Batch, meta *storage.BlockMeta) error {
	if meta == nil {
		return nil
	}
	ref := storage.BlockSeqRefFromBlock(meta.ID)
	if err := batch.Delete(hotKeyBlockSeqIndex(ref), pebble.NoSync); err != nil {
		return err
	}
	if isMasterchainBlock(meta.ID) && meta.Has(storage.BlockMetaIsKeyBlock) {
		if err := batch.Delete(hotKeyKeyBlockSeqIndex(meta.ID.SeqNo), pebble.NoSync); err != nil {
			return err
		}
	}
	if meta.EndLT != 0 {
		if err := batch.Delete(hotKeyBlockLTIndex(ref, meta.EndLT), pebble.NoSync); err != nil {
			return err
		}
	}
	if meta.GenUTime != 0 {
		if err := batch.Delete(hotKeyBlockUTimeIndex(ref, meta.GenUTime), pebble.NoSync); err != nil {
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

func (s *Store) lookupBlockBySeqNo(ctx context.Context, ref storage.BlockSeqRef) (ton.BlockIDExt, error) {
	db, err := s.acquireHotDB(ctx)
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	defer s.releaseHotDB()

	return lookupBlockBySeqNoWithReader(db, ref)
}

func (s *Store) lookupServedBlockBySeqNo(ctx context.Context, ref storage.BlockSeqRef) (ton.BlockIDExt, error) {
	db, err := s.acquireHotDB(ctx)
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	defer s.releaseHotDB()

	block, err := lookupBlockBySeqNoWithReader(db, ref)
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	if err = requireServedFullBlock(db, block); err != nil {
		return ton.BlockIDExt{}, err
	}
	return block, nil
}

func lookupBlockBySeqNoWithReader(reader pebbleReader, ref storage.BlockSeqRef) (ton.BlockIDExt, error) {
	raw, closer, err := pebbleReaderGet(reader, hotKeyBlockSeqIndex(ref))
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	defer func() { _ = closer.Close() }()
	return decodeBlockIDFromHashes(ref.Workchain, ref.Shard, ref.SeqNo, raw)
}

func (s *Store) lookupBlockByLowerBoundIndex(ctx context.Context, prefix []byte, seek []byte, decodeKey func([]byte) (storage.BlockSeqRef, error)) (ton.BlockIDExt, error) {
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

	if !iter.SeekGE(seek) || !bytes.HasPrefix(iter.Key(), prefix) {
		if err = iter.Error(); err != nil {
			return ton.BlockIDExt{}, err
		}
		return ton.BlockIDExt{}, storage.ErrNotFound
	}
	if err = iter.Error(); err != nil {
		return ton.BlockIDExt{}, err
	}

	ref, err := decodeKey(iter.Key())
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	block, err := decodeBlockIDFromHashes(ref.Workchain, ref.Shard, ref.SeqNo, iter.Value())
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	if err = requireServedFullBlock(snap, block); err != nil {
		return ton.BlockIDExt{}, err
	}
	return block, nil
}

func requireServedFullBlock(reader pebbleReader, block ton.BlockIDExt) error {
	served, err := blockMetaServedFullFromReader(reader, block)
	if err != nil {
		return err
	}
	if !served {
		return storage.ErrNotFound
	}
	return nil
}

func blockMetaServedFullFromReader(reader pebbleReader, block ton.BlockIDExt) (bool, error) {
	metaRaw, closer, err := pebbleReaderGet(reader, hotKeyBlockMeta(block))
	if err != nil {
		return false, err
	}
	defer func() { _ = closer.Close() }()

	flags, err := decodeBlockMetaFlags(metaRaw)
	if err != nil {
		return false, err
	}
	return flags&storage.BlockMetaHasServedFull != 0, nil
}

func decodeKeyBlockSeqIndexKey(key []byte) (uint32, error) {
	if len(key) != len(hotPrefixKeyBlockSeq)+4 || !bytes.HasPrefix(key, hotPrefixKeyBlockSeq) {
		return 0, fmt.Errorf("invalid key block seq index key size %d", len(key))
	}
	return binary.BigEndian.Uint32(key[len(hotPrefixKeyBlockSeq):]), nil
}

func decodeBlockLTIndexKey(key []byte) (storage.BlockSeqRef, error) {
	const suffix = 8 + 4
	prefixLen := len(hotPrefixBlockLT)
	if len(key) != prefixLen+4+8+suffix || !bytes.HasPrefix(key, hotPrefixBlockLT) {
		return storage.BlockSeqRef{}, fmt.Errorf("invalid block lt index key size %d", len(key))
	}
	pos := prefixLen
	ref := storage.BlockSeqRef{
		Workchain: int32(binary.BigEndian.Uint32(key[pos : pos+4])),
		Shard:     int64(binary.BigEndian.Uint64(key[pos+4 : pos+12])),
	}
	pos += 4 + 8 + 8
	ref.SeqNo = binary.BigEndian.Uint32(key[pos : pos+4])
	return ref, nil
}

func decodeBlockUTimeIndexKey(key []byte) (storage.BlockSeqRef, error) {
	const suffix = 4 + 4
	prefixLen := len(hotPrefixBlockUTime)
	if len(key) != prefixLen+4+8+suffix || !bytes.HasPrefix(key, hotPrefixBlockUTime) {
		return storage.BlockSeqRef{}, fmt.Errorf("invalid block utime index key size %d", len(key))
	}
	pos := prefixLen
	ref := storage.BlockSeqRef{
		Workchain: int32(binary.BigEndian.Uint32(key[pos : pos+4])),
		Shard:     int64(binary.BigEndian.Uint64(key[pos+4 : pos+12])),
	}
	pos += 4 + 8 + 4
	ref.SeqNo = binary.BigEndian.Uint32(key[pos : pos+4])
	return ref, nil
}
