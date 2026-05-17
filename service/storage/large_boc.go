package storage

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

type largeBOCEncodedRef struct {
	levelMask byte
	hashes    []byte
	depths    []byte
}

type largeBOCEncodedCellRecord struct {
	hash      []byte
	d1        byte
	d2        byte
	data      []byte
	refs      [4]largeBOCEncodedRef
	refsCount int
}

// LargeBOCMetaRecordFromCellRecord converts the persisted compact cell record
// into tonutils-go large-BOC metadata without materializing *cell.Cell.
func LargeBOCMetaRecordFromCellRecord(record *CellRecord) (cell.LargeBOCMetaRecord, error) {
	if record == nil {
		return cell.LargeBOCMetaRecord{}, fmt.Errorf("cell record is nil")
	}
	if len(record.Hash) != cellHashSize {
		return cell.LargeBOCMetaRecord{}, fmt.Errorf("cell hash size mismatch: %d", len(record.Hash))
	}

	refsNum := int(record.D1 & 7)
	if refsNum != len(record.Refs) {
		return cell.LargeBOCMetaRecord{}, fmt.Errorf("cell refs count mismatch: got=%d want=%d", len(record.Refs), refsNum)
	}
	if refsNum > 4 {
		return cell.LargeBOCMetaRecord{}, fmt.Errorf("cell refs count is too large: %d", refsNum)
	}

	bits, err := cellRecordBits(record)
	if err != nil {
		return cell.LargeBOCMetaRecord{}, err
	}
	if bits > 1023 {
		return cell.LargeBOCMetaRecord{}, fmt.Errorf("cell bits length is too large: %d", bits)
	}

	meta := cell.LargeBOCMetaRecord{
		D1:     record.D1,
		BitsSz: uint16(bits),
	}
	if record.D1>>5 == 0 && record.D1&8 == 0 {
		for _, ref := range record.Refs {
			if depth := cellRefDepthAtLevel(ref, 0); depth > meta.Depths[0] {
				meta.Depths[0] = depth
			}
		}
		if refsNum > 0 {
			meta.Depths[0]++
		}
	} else {
		hashes, depths, err := lazyCellHashesDepths(record)
		if err != nil {
			return cell.LargeBOCMetaRecord{}, err
		}
		hashesCount := CellRefHashesCount(record.D1 >> 5)
		if len(hashes) != hashesCount*cellHashSize {
			return cell.LargeBOCMetaRecord{}, fmt.Errorf("cell hashes size mismatch: got=%d want=%d", len(hashes), hashesCount*cellHashSize)
		}
		if len(depths) != hashesCount {
			return cell.LargeBOCMetaRecord{}, fmt.Errorf("cell depths count mismatch: got=%d want=%d", len(depths), hashesCount)
		}
		for i := 0; i < hashesCount; i++ {
			copy(meta.Hashes[i][:], hashes[i*cellHashSize:(i+1)*cellHashSize])
			meta.Depths[i] = depths[i]
		}
	}
	for i, ref := range record.Refs {
		hash, err := CellRefHash(ref)
		if err != nil {
			return cell.LargeBOCMetaRecord{}, fmt.Errorf("ref %d: %w", i, err)
		}
		copy(meta.Refs[i][:], hash)
	}
	return meta, nil
}

// LargeBOCMetaRecordFromEncodedCellRecord converts the persisted compact cell
// record bytes directly into tonutils-go large-BOC metadata.
func LargeBOCMetaRecordFromEncodedCellRecord(hash []byte, data []byte) (cell.LargeBOCMetaRecord, error) {
	record, err := parseLargeBOCEncodedCellRecord(hash, data)
	if err != nil {
		return cell.LargeBOCMetaRecord{}, err
	}
	bits, err := largeBOCEncodedCellBits(record.d2, record.data)
	if err != nil {
		return cell.LargeBOCMetaRecord{}, err
	}
	return largeBOCMetaRecordFromParsedEncodedCellRecord(record, bits)
}

func largeBOCMetaRecordFromParsedEncodedCellRecord(record largeBOCEncodedCellRecord, bits uint) (cell.LargeBOCMetaRecord, error) {
	if len(record.hash) != cellHashSize {
		return cell.LargeBOCMetaRecord{}, fmt.Errorf("cell hash size mismatch: %d", len(record.hash))
	}

	if bits > 1023 {
		return cell.LargeBOCMetaRecord{}, fmt.Errorf("cell bits length is too large: %d", bits)
	}

	meta := cell.LargeBOCMetaRecord{
		D1:     record.d1,
		BitsSz: uint16(bits),
	}
	if record.d1>>5 == 0 && record.d1&8 == 0 {
		for i := 0; i < record.refsCount; i++ {
			if depth := largeBOCEncodedRefDepthAtLevel(record.refs[i], 0); depth > meta.Depths[0] {
				meta.Depths[0] = depth
			}
		}
		if record.refsCount > 0 {
			meta.Depths[0]++
		}
	} else {
		if err := largeBOCEncodedCellHashesDepths(record, &meta); err != nil {
			return cell.LargeBOCMetaRecord{}, err
		}
	}

	for i := 0; i < record.refsCount; i++ {
		hash, err := largeBOCEncodedRefHash(record.refs[i])
		if err != nil {
			return cell.LargeBOCMetaRecord{}, fmt.Errorf("ref %d: %w", i, err)
		}
		copy(meta.Refs[i][:], hash)
	}
	return meta, nil
}

// LargeBOCPayloadRecordFromCellRecord converts the persisted compact cell
// record into a large-BOC payload record. The returned Data does not contain the
// BoC terminator bit; tonutils-go adds it during serialization.
func LargeBOCPayloadRecordFromCellRecord(record *CellRecord) (cell.LargeBOCPayloadRecord, error) {
	data, bits, err := largeBOCPayloadDataFromCellRecord(record)
	if err != nil {
		return cell.LargeBOCPayloadRecord{}, err
	}

	if tailBits := bits % 8; tailBits != 0 {
		data = bytes.Clone(data)
		data[len(data)-1] &= byte(0xff << (8 - tailBits))
	}
	return cell.LargeBOCPayloadRecord{Data: data}, nil
}

// AppendLargeBOCPayloadRecordFromCellRecord converts the persisted compact cell
// record into a large-BOC payload record backed by arena. It is useful for
// batched storage reads because it avoids one heap allocation per cell payload.
func AppendLargeBOCPayloadRecordFromCellRecord(record *CellRecord, arena []byte) (cell.LargeBOCPayloadRecord, []byte, error) {
	data, bits, err := largeBOCPayloadDataFromCellRecord(record)
	if err != nil {
		return cell.LargeBOCPayloadRecord{}, arena, err
	}

	start := len(arena)
	arena = append(arena, data...)
	payload := arena[start:len(arena)]
	if tailBits := bits % 8; tailBits != 0 {
		payload[len(payload)-1] &= byte(0xff << (8 - tailBits))
	}
	return cell.LargeBOCPayloadRecord{Data: payload}, arena, nil
}

// AppendLargeBOCPayloadRecordFromEncodedCellRecord converts persisted compact
// cell bytes directly into a large-BOC payload record backed by arena.
func AppendLargeBOCPayloadRecordFromEncodedCellRecord(encoded []byte, arena []byte) (cell.LargeBOCPayloadRecord, []byte, error) {
	data, bits, err := largeBOCPayloadDataFromEncodedCellRecord(encoded)
	if err != nil {
		return cell.LargeBOCPayloadRecord{}, arena, err
	}

	start := len(arena)
	arena = append(arena, data...)
	payload := arena[start:len(arena)]
	if tailBits := bits % 8; tailBits != 0 {
		payload[len(payload)-1] &= byte(0xff << (8 - tailBits))
	}
	return cell.LargeBOCPayloadRecord{Data: payload}, arena, nil
}

// AppendLargeBOCRecordFromEncodedCellRecord converts persisted compact cell
// bytes directly into a one-pass large-BOC record backed by arena. The encoded
// record is parsed once and payload bytes are copied out because storage values
// may be invalidated after the read call returns.
func AppendLargeBOCRecordFromEncodedCellRecord(hash []byte, encoded []byte, arena []byte) (cell.LargeBOCRecord, []byte, error) {
	record, err := parseLargeBOCEncodedCellRecord(hash, encoded)
	if err != nil {
		return cell.LargeBOCRecord{}, arena, err
	}
	bits, err := largeBOCEncodedCellBits(record.d2, record.data)
	if err != nil {
		return cell.LargeBOCRecord{}, arena, err
	}

	meta, err := largeBOCMetaRecordFromParsedEncodedCellRecord(record, bits)
	if err != nil {
		return cell.LargeBOCRecord{}, arena, err
	}
	payload, arena, err := appendLargeBOCPayloadRecordFromParsedEncodedCellRecord(record, bits, arena)
	if err != nil {
		return cell.LargeBOCRecord{}, arena, err
	}
	return cell.LargeBOCRecord{Meta: meta, Payload: payload}, arena, nil
}

func appendLargeBOCPayloadRecordFromParsedEncodedCellRecord(record largeBOCEncodedCellRecord, bits uint, arena []byte) (cell.LargeBOCPayloadRecord, []byte, error) {
	if bits > 1023 {
		return cell.LargeBOCPayloadRecord{}, arena, fmt.Errorf("cell bits length is too large: %d", bits)
	}

	start := len(arena)
	arena = append(arena, record.data...)
	payload := arena[start:len(arena)]
	if tailBits := bits % 8; tailBits != 0 {
		payload[len(payload)-1] &= byte(0xff << (8 - tailBits))
	}
	return cell.LargeBOCPayloadRecord{Data: payload}, arena, nil
}

func largeBOCPayloadDataFromCellRecord(record *CellRecord) ([]byte, uint, error) {
	if record == nil {
		return nil, 0, fmt.Errorf("cell record is nil")
	}

	bits, err := cellRecordBits(record)
	if err != nil {
		return nil, 0, err
	}
	if bits > 1023 {
		return nil, 0, fmt.Errorf("cell bits length is too large: %d", bits)
	}

	bodyLen := int((bits + 7) / 8)
	if len(record.Data) < bodyLen {
		return nil, 0, fmt.Errorf("cell body size mismatch: got=%d want=%d", len(record.Data), bodyLen)
	}
	return record.Data[:bodyLen], bits, nil
}

func largeBOCPayloadDataFromEncodedCellRecord(encoded []byte) ([]byte, uint, error) {
	if len(encoded) < 2 {
		return nil, 0, fmt.Errorf("cell record payload too small")
	}

	d2 := encoded[1]
	bodyLen := int(d2/2 + d2%2)
	if len(encoded)-2 < bodyLen {
		return nil, 0, fmt.Errorf("cell record payload truncated")
	}

	data := encoded[2 : 2+bodyLen]
	bits, err := largeBOCEncodedCellBits(d2, data)
	if err != nil {
		return nil, 0, err
	}
	if bits > 1023 {
		return nil, 0, fmt.Errorf("cell bits length is too large: %d", bits)
	}
	return data, bits, nil
}

func parseLargeBOCEncodedCellRecord(hash []byte, data []byte) (largeBOCEncodedCellRecord, error) {
	if len(data) < 2 {
		return largeBOCEncodedCellRecord{}, fmt.Errorf("cell record payload too small")
	}

	pos := 0
	storedD1 := data[pos]
	compactRefs := storedD1&encodedCellRecordCompactRefsFlag != 0
	record := largeBOCEncodedCellRecord{
		hash: hash,
		d1:   storedD1 &^ encodedCellRecordCompactRefsFlag,
		d2:   data[pos+1],
	}
	pos += 2

	record.refsCount = int(record.d1 & 7)
	if record.refsCount > len(record.refs) {
		return largeBOCEncodedCellRecord{}, fmt.Errorf("invalid cell refs count %d", record.refsCount)
	}

	dataLen := int(record.d2/2 + record.d2%2)
	if len(data)-pos < dataLen {
		return largeBOCEncodedCellRecord{}, fmt.Errorf("cell record payload truncated")
	}
	record.data = data[pos : pos+dataLen]
	pos += dataLen

	var slowRefs byte
	if compactRefs && record.refsCount > 0 {
		if pos >= len(data) {
			return largeBOCEncodedCellRecord{}, fmt.Errorf("cell record compact ref layout truncated")
		}
		slowRefs = data[pos]
		pos++
		if slowRefs&^byte((1<<uint(record.refsCount))-1) != 0 {
			return largeBOCEncodedCellRecord{}, fmt.Errorf("cell record compact ref layout has invalid slow refs mask %d", slowRefs)
		}
	}

	for i := 0; i < record.refsCount; i++ {
		if compactRefs && slowRefs&(1<<uint(i)) == 0 {
			if len(data)-pos < cellHashSize+cellDepthSize {
				return largeBOCEncodedCellRecord{}, fmt.Errorf("cell record compact ref metadata truncated")
			}
			record.refs[i] = largeBOCEncodedRef{
				hashes: data[pos : pos+cellHashSize],
				depths: data[pos+cellHashSize : pos+cellHashSize+cellDepthSize],
			}
			pos += cellHashSize + cellDepthSize
			continue
		}

		if pos >= len(data) {
			return largeBOCEncodedCellRecord{}, fmt.Errorf("cell record ref metadata truncated")
		}
		levelMask := data[pos]
		pos++

		hashesCount := CellRefHashesCount(levelMask)
		hashesLen := hashesCount * cellHashSize
		depthsLen := hashesCount * cellDepthSize
		if len(data)-pos < hashesLen+depthsLen {
			return largeBOCEncodedCellRecord{}, fmt.Errorf("cell record ref metadata truncated")
		}
		record.refs[i] = largeBOCEncodedRef{
			levelMask: levelMask,
			hashes:    data[pos : pos+hashesLen],
			depths:    data[pos+hashesLen : pos+hashesLen+depthsLen],
		}
		pos += hashesLen + depthsLen
	}
	if pos != len(data) {
		return largeBOCEncodedCellRecord{}, fmt.Errorf("cell record payload has trailing bytes")
	}
	return record, nil
}

func largeBOCEncodedCellBits(d2 byte, data []byte) (uint, error) {
	bodyLen := int(d2/2 + d2%2)
	if len(data) != bodyLen {
		return 0, fmt.Errorf("cell body size mismatch: got=%d want=%d", len(data), bodyLen)
	}
	if d2%2 == 0 {
		return uint(bodyLen * 8), nil
	}
	if bodyLen == 0 {
		return 0, fmt.Errorf("invalid partial cell body size")
	}

	last := data[bodyLen-1]
	terminatorBit := -1
	for i := 0; i < 7; i++ {
		if (last>>i)&1 == 1 {
			terminatorBit = i
			break
		}
	}
	if terminatorBit < 0 {
		return 0, fmt.Errorf("overlong cell bits encoding")
	}
	return uint((bodyLen-1)*8 + 7 - terminatorBit), nil
}

func largeBOCEncodedCellHashesDepths(record largeBOCEncodedCellRecord, meta *cell.LargeBOCMetaRecord) error {
	levelMask := cell.LevelMask{Mask: record.d1 >> 5}
	hashesCount := CellRefHashesCount(levelMask.Mask)

	typ, err := largeBOCEncodedCellType(record)
	if err != nil {
		return err
	}
	if typ == cell.PrunedCellType {
		view, err := prunedCellRecordView(record.hash, record.d1, record.d2, record.data)
		if err != nil {
			return err
		}
		viewMeta := view.GetMetadata()
		if len(viewMeta.Hashes) != hashesCount || len(viewMeta.Depths) != hashesCount {
			return fmt.Errorf("pruned cell metadata size mismatch: hashes=%d depths=%d want=%d", len(viewMeta.Hashes), len(viewMeta.Depths), hashesCount)
		}
		for i := 0; i < hashesCount; i++ {
			copy(meta.Hashes[i][:], viewMeta.Hashes[i][:])
			meta.Depths[i] = viewMeta.Depths[i]
		}
		return nil
	}

	isMerkle := typ == cell.MerkleProofCellType || typ == cell.MerkleUpdateCellType
	var prevHash [cellHashSize]byte
	hasPrevHash := false
	var hashBuf [2 + 128 + 4*cellDepthSize + 4*cellHashSize]byte
	hashIndex := 0

	for level := 0; level <= levelMask.GetLevel(); level++ {
		if !levelMask.IsSignificant(level) {
			continue
		}

		pos := 0
		hashBuf[pos] = (record.d1 & 0x0f) + levelMask.Apply(level).Mask*32
		hashBuf[pos+1] = record.d2
		pos += 2

		if hasPrevHash {
			pos += copy(hashBuf[pos:], prevHash[:])
		} else {
			pos += copy(hashBuf[pos:], record.data)
		}

		childLevel := level
		if isMerkle {
			childLevel++
		}

		var depth uint16
		for i := 0; i < record.refsCount; i++ {
			childDepth := largeBOCEncodedRefDepthAtLevel(record.refs[i], childLevel)
			binary.BigEndian.PutUint16(hashBuf[pos:pos+cellDepthSize], childDepth)
			pos += cellDepthSize
			if childDepth > depth {
				depth = childDepth
			}
		}
		if record.refsCount > 0 {
			depth++
		}

		meta.Depths[hashIndex] = depth
		if hashIndex == hashesCount-1 {
			copy(meta.Hashes[hashIndex][:], record.hash)
			hashIndex++
			continue
		}

		for i := 0; i < record.refsCount; i++ {
			pos += copy(hashBuf[pos:], largeBOCEncodedRefHashAtLevel(record.refs[i], childLevel))
		}

		sum := sha256.Sum256(hashBuf[:pos])
		copy(prevHash[:], sum[:])
		copy(meta.Hashes[hashIndex][:], sum[:])
		hasPrevHash = true
		hashIndex++
	}
	return nil
}

func largeBOCEncodedCellType(record largeBOCEncodedCellRecord) (cell.Type, error) {
	if record.d1&8 == 0 {
		return cell.OrdinaryCellType, nil
	}
	if len(record.data) == 0 {
		return 0, fmt.Errorf("special cell body is empty")
	}
	return cell.Type(record.data[0]), nil
}

func largeBOCEncodedRefHash(ref largeBOCEncodedRef) ([]byte, error) {
	count := CellRefHashesCount(ref.levelMask)
	if len(ref.hashes) != count*cellHashSize {
		return nil, fmt.Errorf("invalid ref hashes size: got=%d want=%d", len(ref.hashes), count*cellHashSize)
	}
	if len(ref.depths) != count*cellDepthSize {
		return nil, fmt.Errorf("invalid ref depths size: got=%d want=%d", len(ref.depths), count*cellDepthSize)
	}
	return ref.hashes[len(ref.hashes)-cellHashSize:], nil
}

func largeBOCEncodedRefHashAtLevel(ref largeBOCEncodedRef, level int) []byte {
	index := cellRefHashIndex(ref.levelMask, level)
	return ref.hashes[index*cellHashSize : (index+1)*cellHashSize]
}

func largeBOCEncodedRefDepthAtLevel(ref largeBOCEncodedRef, level int) uint16 {
	index := cellRefHashIndex(ref.levelMask, level)
	return binary.BigEndian.Uint16(ref.depths[index*cellDepthSize : (index+1)*cellDepthSize])
}
