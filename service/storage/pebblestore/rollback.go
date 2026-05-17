package pebblestore

import (
	"bytes"
	"context"
	"encoding/binary"
	"github.com/xssnick/gton/service/storage"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"github.com/xssnick/tonutils-go/ton"
)

const rollbackMasterShard = int64(-1 << 63)

type RollbackStats struct {
	DeletedKeys int
}

func (s *Store) Rollback(ctx context.Context, current *storage.CurrentState) (RollbackStats, error) {
	if current == nil {
		return RollbackStats{}, fmt.Errorf("rollback current state is nil")
	}
	target := current.Masterchain.Block
	if target.Workchain != -1 || target.Shard != rollbackMasterShard {
		return RollbackStats{}, fmt.Errorf("rollback target is not a masterchain block: %s", storage.FormatBlockRef(target))
	}

	keepStates := rollbackKeepStates(current)

	db, err := s.acquireHotDB(ctx)
	if err != nil {
		return RollbackStats{}, err
	}
	defer s.releaseHotDB()

	s.hotWriteMu.Lock()
	defer s.hotWriteMu.Unlock()

	batch := db.NewBatch()
	defer func() { _ = batch.Close() }()

	futureBlocks, stats, err := s.rollbackDeleteBlockMeta(ctx, db, batch, target.SeqNo, keepStates)
	if err != nil {
		return RollbackStats{}, err
	}
	deleted, err := s.rollbackDeleteStateMeta(ctx, db, batch, target.SeqNo, keepStates)
	if err != nil {
		return RollbackStats{}, err
	}
	stats.DeletedKeys += deleted
	deleted, err = s.rollbackDeleteIndexedBlocks(ctx, db, batch, futureBlocks, target.SeqNo)
	if err != nil {
		return RollbackStats{}, err
	}
	stats.DeletedKeys += deleted

	deleted, err = s.rollbackDeleteHotKey(db, batch, hotKeyStateSyncProgress())
	if err != nil {
		return RollbackStats{}, err
	}
	stats.DeletedKeys += deleted
	deleted, err = s.rollbackDeleteHotKey(db, batch, hotKeyVerifiedKeyBlockProgress())
	if err != nil {
		return RollbackStats{}, err
	}
	stats.DeletedKeys += deleted

	if err = batch.Set(hotKeyCurrentState(), encodeCurrentState(current), pebble.NoSync); err != nil {
		return RollbackStats{}, err
	}

	if err = batch.Commit(pebble.Sync); err != nil {
		return RollbackStats{}, err
	}
	return stats, nil
}

func (s *Store) rollbackDeleteHotKey(db *pebble.DB, batch *pebble.Batch, key []byte) (int, error) {
	exists, err := pebbleReaderHas(db, key)
	if err != nil {
		return 0, err
	}
	if !exists {
		return 0, nil
	}
	if err = batch.Delete(key, pebble.NoSync); err != nil {
		return 0, err
	}
	return 1, nil
}

func rollbackKeepStates(current *storage.CurrentState) map[string]struct{} {
	keep := map[string]struct{}{
		storage.BlockKey(current.Masterchain.Block): {},
	}
	for _, shard := range current.Shards {
		keep[storage.BlockKey(shard.Block)] = struct{}{}
	}
	return keep
}

func (s *Store) rollbackDeleteBlockMeta(ctx context.Context, db *pebble.DB, batch *pebble.Batch, cutoff uint32, keepStates map[string]struct{}) (map[string]struct{}, RollbackStats, error) {
	futureBlocks := map[string]struct{}{}
	var stats RollbackStats
	err := s.rollbackScanPrefix(ctx, db, hotPrefixBlockMeta, func(key []byte, value []byte) error {
		block, err := decodeRollbackBlockKey(hotPrefixBlockMeta, key)
		if err != nil {
			return err
		}
		meta, err := decodeBlockMeta(block, value)
		if err != nil {
			return err
		}
		if !rollbackDeleteBlockMeta(block, meta, cutoff, keepStates) {
			return nil
		}
		if err = batch.Delete(key, pebble.NoSync); err != nil {
			return err
		}
		futureBlocks[storage.BlockKey(block)] = struct{}{}
		stats.DeletedKeys++
		return nil
	})
	return futureBlocks, stats, err
}

func rollbackDeleteBlockMeta(block ton.BlockIDExt, meta *storage.BlockMeta, cutoff uint32, keepStates map[string]struct{}) bool {
	if block.Workchain == -1 && block.Shard == rollbackMasterShard {
		return block.SeqNo > cutoff
	}
	if meta != nil && meta.MasterchainRef != nil {
		return meta.MasterchainRef.SeqNo > cutoff
	}
	if meta != nil && meta.Has(storage.BlockMetaHasStateSnapshot) {
		_, keep := keepStates[storage.BlockKey(block)]
		return !keep
	}
	return false
}

func (s *Store) rollbackDeleteStateMeta(ctx context.Context, db *pebble.DB, batch *pebble.Batch, cutoff uint32, keepStates map[string]struct{}) (int, error) {
	deleted := 0
	err := s.rollbackScanPrefix(ctx, db, hotPrefixStateMeta, func(key []byte, _ []byte) error {
		block, err := decodeRollbackBlockKey(hotPrefixStateMeta, key)
		if err != nil {
			return err
		}
		if !rollbackDeleteState(block, cutoff, keepStates) {
			return nil
		}
		if err = batch.Delete(key, pebble.NoSync); err != nil {
			return err
		}
		deleted++
		return nil
	})
	if err != nil {
		return deleted, err
	}
	return deleted, nil
}

func rollbackDeleteState(block ton.BlockIDExt, cutoff uint32, keepStates map[string]struct{}) bool {
	if _, keep := keepStates[storage.BlockKey(block)]; keep {
		return false
	}
	if block.Workchain == -1 && block.Shard == rollbackMasterShard {
		return block.SeqNo > cutoff
	}
	return true
}

func (s *Store) rollbackDeleteIndexedBlocks(ctx context.Context, db *pebble.DB, batch *pebble.Batch, futureBlocks map[string]struct{}, cutoff uint32) (int, error) {
	deleted := 0
	scans := []rollbackScan{
		{prefix: hotPrefixNextBlock, delete: rollbackDeleteNextBlock(futureBlocks, cutoff)},
		{prefix: hotPrefixBlockSeq, delete: rollbackDeleteIndexValue(futureBlocks, cutoff)},
		{prefix: hotPrefixBlockLT, delete: rollbackDeleteIndexValue(futureBlocks, cutoff)},
		{prefix: hotPrefixBlockUTime, delete: rollbackDeleteIndexValue(futureBlocks, cutoff)},
		{prefix: hotPrefixBlockDataRef, delete: rollbackDeleteBlockIDKey(hotPrefixBlockDataRef, futureBlocks, cutoff)},
		{prefix: hotPrefixProofRef, delete: rollbackDeleteProofKey(hotPrefixProofRef, futureBlocks, cutoff)},
		{prefix: hotPrefixKeyProofRef, delete: rollbackDeleteProofKey(hotPrefixKeyProofRef, futureBlocks, cutoff)},
		{prefix: hotPrefixStateFileRef, delete: rollbackDeleteStateFileKey(futureBlocks, cutoff)},
		{prefix: hotPrefixArchiveInfo, delete: rollbackDeleteArchiveInfo(cutoff)},
	}
	for _, scan := range scans {
		err := s.rollbackScanPrefix(ctx, db, scan.prefix, func(key []byte, value []byte) error {
			yes, err := scan.delete(key, value)
			if err != nil || !yes {
				return err
			}
			if err = batch.Delete(key, pebble.NoSync); err != nil {
				return err
			}
			deleted++
			return nil
		})
		if err != nil {
			return deleted, err
		}
	}
	return deleted, nil
}

type rollbackScan struct {
	prefix []byte
	delete func(key []byte, value []byte) (bool, error)
}

func rollbackDeleteNextBlock(futureBlocks map[string]struct{}, cutoff uint32) func([]byte, []byte) (bool, error) {
	return func(key []byte, value []byte) (bool, error) {
		prev, err := decodeRollbackBlockKey(hotPrefixNextBlock, key)
		if err != nil {
			return false, err
		}
		next, err := decodeBlockID(value)
		if err != nil {
			return false, err
		}
		return rollbackFutureBlock(prev, futureBlocks, cutoff) || rollbackFutureBlock(next, futureBlocks, cutoff), nil
	}
}

func rollbackDeleteIndexValue(futureBlocks map[string]struct{}, cutoff uint32) func([]byte, []byte) (bool, error) {
	return func(_ []byte, value []byte) (bool, error) {
		block, err := decodeBlockID(value)
		if err != nil {
			return false, err
		}
		return rollbackFutureBlock(block, futureBlocks, cutoff), nil
	}
}

func rollbackDeleteBlockIDKey(prefix []byte, futureBlocks map[string]struct{}, cutoff uint32) func([]byte, []byte) (bool, error) {
	return func(key []byte, _ []byte) (bool, error) {
		block, err := decodeRollbackBlockKey(prefix, key)
		if err != nil {
			return false, err
		}
		return rollbackFutureBlock(block, futureBlocks, cutoff), nil
	}
}

func rollbackDeleteProofKey(prefix []byte, futureBlocks map[string]struct{}, cutoff uint32) func([]byte, []byte) (bool, error) {
	return func(key []byte, _ []byte) (bool, error) {
		if len(key) != len(prefix)+1+80 {
			return false, fmt.Errorf("invalid proof key size %d", len(key))
		}
		block, err := decodeBlockID(key[len(prefix)+1:])
		if err != nil {
			return false, err
		}
		return rollbackFutureBlock(block, futureBlocks, cutoff), nil
	}
}

func rollbackDeleteStateFileKey(futureBlocks map[string]struct{}, cutoff uint32) func([]byte, []byte) (bool, error) {
	return func(key []byte, _ []byte) (bool, error) {
		if len(key) != len(hotPrefixStateFileRef)+80+80+8 {
			return false, fmt.Errorf("invalid state file key size %d", len(key))
		}
		pos := len(hotPrefixStateFileRef)
		block, err := decodeBlockID(key[pos : pos+80])
		if err != nil {
			return false, err
		}
		pos += 80
		master, err := decodeBlockID(key[pos : pos+80])
		if err != nil {
			return false, err
		}
		return master.SeqNo > cutoff || rollbackFutureBlock(block, futureBlocks, cutoff), nil
	}
}

func rollbackDeleteArchiveInfo(cutoff uint32) func([]byte, []byte) (bool, error) {
	return func(key []byte, _ []byte) (bool, error) {
		if len(key) != len(hotPrefixArchiveInfo)+4+4+8 {
			return false, fmt.Errorf("invalid archive info key size %d", len(key))
		}
		seqno := binary.BigEndian.Uint32(key[len(hotPrefixArchiveInfo) : len(hotPrefixArchiveInfo)+4])
		return seqno > cutoff, nil
	}
}

func rollbackFutureBlock(block ton.BlockIDExt, futureBlocks map[string]struct{}, cutoff uint32) bool {
	if block.Workchain == -1 && block.Shard == rollbackMasterShard && block.SeqNo > cutoff {
		return true
	}
	_, ok := futureBlocks[storage.BlockKey(block)]
	return ok
}

func (s *Store) rollbackScanPrefix(ctx context.Context, db *pebble.DB, prefix []byte, fn func(key []byte, value []byte) error) error {
	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return err
	}
	defer func() { _ = iter.Close() }()

	for ok := iter.First(); ok; ok = iter.Next() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		key := bytes.Clone(iter.Key())
		value := bytes.Clone(iter.Value())
		if err = fn(key, value); err != nil {
			return err
		}
	}
	return iter.Error()
}

func decodeRollbackBlockKey(prefix []byte, key []byte) (ton.BlockIDExt, error) {
	if len(key) != len(prefix)+80 {
		return ton.BlockIDExt{}, fmt.Errorf("invalid block key size %d for prefix %x", len(key), prefix)
	}
	return decodeBlockID(key[len(prefix):])
}
