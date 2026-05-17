package pebblestore

import (
	"bytes"
	"context"
	"encoding/binary"
	"github.com/xssnick/gton/service/storage"
	"fmt"
	"sort"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/xssnick/tonutils-go/ton"
)

const (
	persistentStateSerializerStateVersion  = 1
	persistentStateSerializerActiveVersion = 1
	persistentStateDescriptionVersion      = 1
)

func (s *Store) PersistentStateSerializerState(ctx context.Context) (*storage.PersistentStateSerializerState, error) {
	raw, err := s.getHotCopy(ctx, hotKeyPersistentStateSerializer())
	if err != nil {
		return nil, err
	}
	return decodePersistentStateSerializerState(raw)
}

func (s *Store) SavePersistentStateSerializerState(ctx context.Context, state *storage.PersistentStateSerializerState) error {
	if state == nil {
		return fmt.Errorf("persistent state serializer state is nil")
	}
	if !isMasterchainBlock(state.LastBlock) {
		return fmt.Errorf("persistent state serializer last block is not masterchain: %s", storage.FormatBlockRef(state.LastBlock))
	}
	if state.LastWrittenBlock.SeqNo != 0 && !isMasterchainBlock(state.LastWrittenBlock) {
		return fmt.Errorf("persistent state serializer last written block is not masterchain: %s", storage.FormatBlockRef(state.LastWrittenBlock))
	}

	return s.setHotRecord(ctx, hotKeyPersistentStateSerializer(), encodePersistentStateSerializerState(state), pebble.NoSync)
}

func (s *Store) ActivePersistentStateSerialization(ctx context.Context) (*storage.PersistentStateSerializerActive, error) {
	raw, err := s.getHotCopy(ctx, hotKeyPersistentStateSerializerActive())
	if err != nil {
		return nil, err
	}
	return decodePersistentStateSerializerActive(raw)
}

func (s *Store) SaveActivePersistentStateSerialization(ctx context.Context, active *storage.PersistentStateSerializerActive) error {
	if active == nil {
		return fmt.Errorf("active persistent state serialization is nil")
	}
	if !isMasterchainBlock(active.Block) {
		return fmt.Errorf("active persistent state serialization block is not masterchain: %s", storage.FormatBlockRef(active.Block))
	}

	return s.setHotRecord(ctx, hotKeyPersistentStateSerializerActive(), encodePersistentStateSerializerActive(active), pebble.NoSync)
}

func (s *Store) DeleteActivePersistentStateSerialization(ctx context.Context) error {
	return s.deleteHotRecord(ctx, hotKeyPersistentStateSerializerActive(), pebble.NoSync)
}

func (s *Store) SavePersistentStateDescription(ctx context.Context, desc *storage.PersistentStateDescription) error {
	if desc == nil {
		return fmt.Errorf("persistent state description is nil")
	}
	if !isMasterchainBlock(desc.MasterchainBlock) {
		return fmt.Errorf("persistent state description masterchain block is invalid: %s", storage.FormatBlockRef(desc.MasterchainBlock))
	}
	if desc.StartTime == 0 || desc.EndTime <= uint64(desc.StartTime) {
		return fmt.Errorf("persistent state description has invalid time range: start=%d end=%d", desc.StartTime, desc.EndTime)
	}

	now := uint64(time.Now().Unix())
	if desc.EndTime <= now {
		return nil
	}

	encoded := encodePersistentStateDescription(desc)
	key := hotKeyPersistentStateDescription(desc.MasterchainBlock.SeqNo)
	prefix := hotKeyPersistentStateDescriptionPrefix()

	db, err := s.acquireHotDB(ctx)
	if err != nil {
		return err
	}
	defer s.releaseHotDB()

	s.hotWriteMu.Lock()
	defer s.hotWriteMu.Unlock()
	batch := db.NewBatch()
	defer func() { _ = batch.Close() }()

	if err := deleteExpiredPersistentStateDescriptions(db, batch, prefix, now); err != nil {
		return err
	}
	if err := batch.Set(key, encoded, pebble.NoSync); err != nil {
		return err
	}
	return batch.Commit(pebble.NoSync)
}

func (s *Store) PersistentStateDescriptions(ctx context.Context) ([]storage.PersistentStateDescription, error) {
	prefix := hotKeyPersistentStateDescriptionPrefix()
	now := uint64(time.Now().Unix())
	var descriptions []storage.PersistentStateDescription

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

		for iter.First(); iter.Valid() && bytes.HasPrefix(iter.Key(), prefix); iter.Next() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			desc, err := decodePersistentStateDescription(iter.Value())
			if err != nil {
				return err
			}
			if desc.EndTime <= now {
				continue
			}
			descriptions = append(descriptions, *desc)
		}
		return iter.Error()
	}(); err != nil {
		return nil, err
	}

	sort.Slice(descriptions, func(i, j int) bool {
		return descriptions[i].MasterchainBlock.SeqNo < descriptions[j].MasterchainBlock.SeqNo
	})
	return descriptions, nil
}

func deleteExpiredPersistentStateDescriptions(db *pebble.DB, batch *pebble.Batch, prefix []byte, now uint64) error {
	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return err
	}
	defer func() { _ = iter.Close() }()

	for iter.First(); iter.Valid() && bytes.HasPrefix(iter.Key(), prefix); iter.Next() {
		desc, err := decodePersistentStateDescription(iter.Value())
		if err != nil {
			return err
		}
		if desc.EndTime > now {
			continue
		}
		if err = batch.Delete(iter.Key(), pebble.NoSync); err != nil {
			return err
		}
	}
	return iter.Error()
}

func encodePersistentStateSerializerState(state *storage.PersistentStateSerializerState) []byte {
	buf := make([]byte, 0, 1+80+80+4)
	buf = append(buf, persistentStateSerializerStateVersion)
	buf = append(buf, encodeBlockID(state.LastBlock)...)
	buf = append(buf, encodeBlockID(state.LastWrittenBlock)...)
	return binary.BigEndian.AppendUint32(buf, state.LastWrittenBlockUTime)
}

func decodePersistentStateSerializerState(data []byte) (*storage.PersistentStateSerializerState, error) {
	const size = 1 + 80 + 80 + 4
	if len(data) != size {
		return nil, fmt.Errorf("persistent state serializer state size mismatch: %d", len(data))
	}
	if data[0] != persistentStateSerializerStateVersion {
		return nil, fmt.Errorf("unsupported persistent state serializer state version %d", data[0])
	}
	lastBlock, err := decodeBlockID(data[1 : 1+80])
	if err != nil {
		return nil, err
	}
	lastWrittenBlock, err := decodeBlockID(data[1+80 : 1+160])
	if err != nil {
		return nil, err
	}
	return &storage.PersistentStateSerializerState{
		LastBlock:             lastBlock,
		LastWrittenBlock:      lastWrittenBlock,
		LastWrittenBlockUTime: binary.BigEndian.Uint32(data[1+160:]),
	}, nil
}

func encodePersistentStateSerializerActive(active *storage.PersistentStateSerializerActive) []byte {
	buf := make([]byte, 0, 1+80+8)
	buf = append(buf, persistentStateSerializerActiveVersion)
	buf = append(buf, encodeBlockID(active.Block)...)
	return binary.BigEndian.AppendUint64(buf, active.StartedAtUnix)
}

func decodePersistentStateSerializerActive(data []byte) (*storage.PersistentStateSerializerActive, error) {
	const size = 1 + 80 + 8
	if len(data) != size {
		return nil, fmt.Errorf("active persistent state serializer size mismatch: %d", len(data))
	}
	if data[0] != persistentStateSerializerActiveVersion {
		return nil, fmt.Errorf("unsupported active persistent state serializer version %d", data[0])
	}
	block, err := decodeBlockID(data[1 : 1+80])
	if err != nil {
		return nil, err
	}
	return &storage.PersistentStateSerializerActive{
		Block:         block,
		StartedAtUnix: binary.BigEndian.Uint64(data[1+80:]),
	}, nil
}

func encodePersistentStateDescription(desc *storage.PersistentStateDescription) []byte {
	buf := make([]byte, 0, 1+4+8+80+4+len(desc.ShardBlocks)*(80+4))
	buf = append(buf, persistentStateDescriptionVersion)
	buf = binary.BigEndian.AppendUint32(buf, desc.StartTime)
	buf = binary.BigEndian.AppendUint64(buf, desc.EndTime)
	buf = append(buf, encodeBlockID(desc.MasterchainBlock)...)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(desc.ShardBlocks)))
	for _, shard := range desc.ShardBlocks {
		buf = append(buf, encodeBlockID(shard.Block)...)
		buf = binary.BigEndian.AppendUint32(buf, shard.SplitDepth)
	}
	return buf
}

func decodePersistentStateDescription(data []byte) (*storage.PersistentStateDescription, error) {
	if len(data) < 1+4+8+80+4 {
		return nil, fmt.Errorf("persistent state description payload too small")
	}
	if data[0] != persistentStateDescriptionVersion {
		return nil, fmt.Errorf("unsupported persistent state description version %d", data[0])
	}

	pos := 1
	desc := &storage.PersistentStateDescription{
		StartTime: binary.BigEndian.Uint32(data[pos : pos+4]),
		EndTime:   binary.BigEndian.Uint64(data[pos+4 : pos+12]),
	}
	pos += 12

	master, err := decodeBlockID(data[pos : pos+80])
	if err != nil {
		return nil, err
	}
	desc.MasterchainBlock = master
	pos += 80

	count := int(binary.BigEndian.Uint32(data[pos : pos+4]))
	pos += 4
	if len(data)-pos != count*(80+4) {
		return nil, fmt.Errorf("persistent state description shard payload size mismatch")
	}

	desc.ShardBlocks = make([]storage.PersistentStateDescriptionShard, 0, count)
	for i := 0; i < count; i++ {
		block, err := decodeBlockID(data[pos : pos+80])
		if err != nil {
			return nil, err
		}
		pos += 80
		desc.ShardBlocks = append(desc.ShardBlocks, storage.PersistentStateDescriptionShard{
			Block:      block,
			SplitDepth: binary.BigEndian.Uint32(data[pos : pos+4]),
		})
		pos += 4
	}
	return desc, nil
}

func isMasterchainBlock(block ton.BlockIDExt) bool {
	return block.Workchain == -1 && block.Shard == int64(-1<<63)
}
