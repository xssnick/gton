package pebblestore

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
)

func (s *Store) manifestLocked() cellGenerationManifest {
	return cellGenerationManifest{
		active:       s.activeCellGeneration,
		next:         s.nextCellGeneration,
		activeOrigin: s.activeCellOrigin,
		pending:      cloneCellGenerationPendingMigration(s.pendingCellMigration),
		retired:      cloneUint64Slice(s.retiredGenerations),
	}
}

type cellGenerationManifest struct {
	active       uint64
	next         uint64
	activeOrigin ton.BlockIDExt
	pending      *cellGenerationPendingMigration
	retired      []uint64
}

type cellGenerationPendingMigration struct {
	generation uint64
	origin     ton.BlockIDExt
}

func loadOrInitCellGenerationManifest(db *pebble.DB) (cellGenerationManifest, error) {
	key := hotKeyCellGenerationManifest()
	raw, err := pebbleReaderGetCopy(db, key)
	if err == nil {
		return decodeCellGenerationManifest(raw)
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return cellGenerationManifest{}, err
	}
	hasRecords, err := pebbleHasAnyUserRecord(db)
	if err != nil {
		return cellGenerationManifest{}, err
	}
	if hasRecords {
		return cellGenerationManifest{}, fmt.Errorf("cell generation manifest is missing in non-empty metadb")
	}

	manifest := cellGenerationManifest{
		active: initialCellGenerationID,
		next:   initialCellGenerationID + 1,
	}
	if err = db.Set(key, encodeCellGenerationManifest(manifest), pebble.Sync); err != nil {
		return cellGenerationManifest{}, err
	}
	return manifest, nil
}

func loadCellGenerationManifest(db *pebble.DB) (cellGenerationManifest, error) {
	raw, err := pebbleReaderGetCopy(db, hotKeyCellGenerationManifest())
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return cellGenerationManifest{}, fmt.Errorf("cell generation manifest is missing")
		}
		return cellGenerationManifest{}, err
	}
	return decodeCellGenerationManifest(raw)
}

func encodeCellGenerationManifest(manifest cellGenerationManifest) []byte {
	buf := make([]byte, 0, 1+8+8+80+1+8+80+4+len(manifest.retired)*8)
	buf = append(buf, cellGenerationManifestVersion)
	buf = binary.BigEndian.AppendUint64(buf, manifest.active)
	buf = binary.BigEndian.AppendUint64(buf, manifest.next)
	buf = appendManifestBlockID(buf, manifest.activeOrigin)
	if manifest.pending == nil {
		buf = append(buf, 0)
	} else {
		buf = append(buf, 1)
		buf = binary.BigEndian.AppendUint64(buf, manifest.pending.generation)
		buf = appendManifestBlockID(buf, manifest.pending.origin)
	}
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(manifest.retired)))
	for _, generation := range manifest.retired {
		buf = binary.BigEndian.AppendUint64(buf, generation)
	}
	return buf
}

func appendManifestBlockID(buf []byte, id ton.BlockIDExt) []byte {
	buf = binary.BigEndian.AppendUint32(buf, uint32(id.Workchain))
	buf = binary.BigEndian.AppendUint64(buf, uint64(id.Shard))
	buf = binary.BigEndian.AppendUint32(buf, id.SeqNo)
	buf = appendFixedHash(buf, id.RootHash)
	return appendFixedHash(buf, id.FileHash)
}

func appendFixedHash(buf []byte, hash []byte) []byte {
	if len(hash) == 32 {
		return append(buf, hash...)
	}
	return append(buf, make([]byte, 32)...)
}

func decodeCellGenerationManifest(data []byte) (cellGenerationManifest, error) {
	if len(data) < 1+8+8+80+1+4 {
		return cellGenerationManifest{}, fmt.Errorf("cell generation manifest too small: %d", len(data))
	}
	if data[0] != cellGenerationManifestVersion {
		return cellGenerationManifest{}, fmt.Errorf("unsupported cell generation manifest version %d", data[0])
	}
	active := binary.BigEndian.Uint64(data[1:])
	next := binary.BigEndian.Uint64(data[9:])
	if active == 0 {
		return cellGenerationManifest{}, fmt.Errorf("cell generation manifest active generation is zero")
	}
	if next <= active {
		return cellGenerationManifest{}, fmt.Errorf("cell generation manifest next generation %d is not above active %d", next, active)
	}
	origin, err := decodeBlockID(data[17 : 17+80])
	if err != nil {
		return cellGenerationManifest{}, err
	}
	pos := 17 + 80
	var pending *cellGenerationPendingMigration
	switch data[pos] {
	case 0:
		pos++
	case 1:
		if len(data) < pos+1+8+80+4 {
			return cellGenerationManifest{}, fmt.Errorf("cell generation manifest pending payload too small: %d", len(data))
		}
		pos++
		pendingGeneration := binary.BigEndian.Uint64(data[pos:])
		pos += 8
		pendingOrigin, err := decodeBlockID(data[pos : pos+80])
		if err != nil {
			return cellGenerationManifest{}, err
		}
		pos += 80
		if pendingGeneration == 0 {
			return cellGenerationManifest{}, fmt.Errorf("cell generation manifest pending generation is zero")
		}
		if pendingGeneration == active {
			return cellGenerationManifest{}, fmt.Errorf("cell generation manifest pending generation equals active generation %d", active)
		}
		if pendingGeneration >= next {
			return cellGenerationManifest{}, fmt.Errorf("cell generation manifest pending generation %d is not below next generation %d", pendingGeneration, next)
		}
		pending = &cellGenerationPendingMigration{
			generation: pendingGeneration,
			origin:     pendingOrigin,
		}
	default:
		return cellGenerationManifest{}, fmt.Errorf("unsupported cell generation manifest pending flag %d", data[pos])
	}
	if len(data) < pos+4 {
		return cellGenerationManifest{}, fmt.Errorf("cell generation manifest retired payload too small: %d", len(data))
	}
	retiredCount := int(binary.BigEndian.Uint32(data[pos:]))
	pos += 4
	if len(data) != pos+retiredCount*8 {
		return cellGenerationManifest{}, fmt.Errorf("cell generation manifest size mismatch: %d", len(data))
	}
	retired := make([]uint64, 0, retiredCount)
	for i := 0; i < retiredCount; i++ {
		generation := binary.BigEndian.Uint64(data[pos:])
		pos += 8
		if generation == 0 {
			return cellGenerationManifest{}, fmt.Errorf("cell generation manifest retired generation is zero")
		}
		if generation == active {
			return cellGenerationManifest{}, fmt.Errorf("cell generation manifest retired generation equals active generation %d", active)
		}
		if pending != nil && generation == pending.generation {
			return cellGenerationManifest{}, fmt.Errorf("cell generation manifest retired generation equals pending generation %d", generation)
		}
		retired = appendRetiredCellGeneration(retired, generation)
	}
	return cellGenerationManifest{
		active:       active,
		next:         next,
		activeOrigin: origin,
		pending:      pending,
		retired:      retired,
	}, nil
}

func pebbleHasAnyUserRecord(db *pebble.DB) (bool, error) {
	iter, err := db.NewIter(&pebble.IterOptions{})
	if err != nil {
		return false, err
	}
	defer func() { _ = iter.Close() }()

	hasRecords := iter.First()
	if err = iter.Error(); err != nil {
		return false, err
	}
	return hasRecords, nil
}

func cloneCellGenerationPendingMigration(pending *cellGenerationPendingMigration) *cellGenerationPendingMigration {
	if pending == nil {
		return nil
	}
	cloned := *pending
	return &cloned
}

func cloneUint64Slice(values []uint64) []uint64 {
	if len(values) == 0 {
		return nil
	}
	return append([]uint64(nil), values...)
}

func appendRetiredCellGeneration(retired []uint64, generation uint64) []uint64 {
	if generation == 0 || containsUint64(retired, generation) {
		return retired
	}
	return append(retired, generation)
}

func removeUint64(values []uint64, value uint64) []uint64 {
	next := values[:0]
	for _, current := range values {
		if current == value {
			continue
		}
		next = append(next, current)
	}
	clear(values[len(next):])
	return next
}

func containsUint64(values []uint64, value uint64) bool {
	for _, current := range values {
		if current == value {
			return true
		}
	}
	return false
}
