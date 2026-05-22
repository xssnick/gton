package pebblestore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
)

func encodeBlockID(id ton.BlockIDExt) []byte {
	buf := make([]byte, 0, 4+8+4+32+32)
	buf = binary.BigEndian.AppendUint32(buf, uint32(id.Workchain))
	buf = binary.BigEndian.AppendUint64(buf, uint64(id.Shard))
	buf = binary.BigEndian.AppendUint32(buf, id.SeqNo)
	buf = append(buf, id.RootHash...)
	buf = append(buf, id.FileHash...)
	return buf
}

func decodeBlockID(data []byte) (ton.BlockIDExt, error) {
	if len(data) != 80 {
		return ton.BlockIDExt{}, fmt.Errorf("invalid block id size %d", len(data))
	}
	return ton.BlockIDExt{
		Workchain: int32(binary.BigEndian.Uint32(data[:4])),
		Shard:     int64(binary.BigEndian.Uint64(data[4:12])),
		SeqNo:     binary.BigEndian.Uint32(data[12:16]),
		RootHash:  bytes.Clone(data[16:48]),
		FileHash:  bytes.Clone(data[48:80]),
	}, nil
}

func encodeBlockMeta(meta *storage.BlockMeta) []byte {
	if meta == nil {
		return nil
	}
	buf := make([]byte, 0, 256)
	buf = append(buf, blockMetaVersion)
	buf = binary.BigEndian.AppendUint32(buf, uint32(meta.Flags))
	buf = binary.BigEndian.AppendUint32(buf, meta.GenUTime)
	buf = binary.BigEndian.AppendUint64(buf, meta.StartLT)
	buf = binary.BigEndian.AppendUint64(buf, meta.EndLT)
	buf = appendLenBytes(buf, meta.StateRootHash)
	buf = appendLenBytes(buf, meta.StateFileHash)
	if meta.MasterchainRef == nil {
		buf = append(buf, 0)
	} else {
		buf = append(buf, 1)
		buf = append(buf, encodeBlockID(*meta.MasterchainRef)...)
	}
	buf = append(buf, byte(len(meta.PrevRefs)))
	for _, ref := range meta.PrevRefs {
		buf = append(buf, encodeBlockID(ref)...)
	}
	return buf
}

func decodeBlockMeta(id ton.BlockIDExt, data []byte) (*storage.BlockMeta, error) {
	if len(data) < 1+4+4+8+8+1+1+1+1 {
		return nil, fmt.Errorf("block meta payload too small")
	}
	if data[0] != blockMetaVersion {
		return nil, fmt.Errorf("unsupported block meta version %d", data[0])
	}
	pos := 1
	meta := &storage.BlockMeta{
		ID:       id,
		Flags:    storage.BlockMetaFlags(binary.BigEndian.Uint32(data[pos : pos+4])),
		GenUTime: binary.BigEndian.Uint32(data[pos+4 : pos+8]),
		StartLT:  binary.BigEndian.Uint64(data[pos+8 : pos+16]),
		EndLT:    binary.BigEndian.Uint64(data[pos+16 : pos+24]),
	}
	pos += 24
	var err error
	meta.StateRootHash, pos, err = readLenBytes(data, pos)
	if err != nil {
		return nil, err
	}
	meta.StateFileHash, pos, err = readLenBytes(data, pos)
	if err != nil {
		return nil, err
	}
	if pos >= len(data) {
		return nil, fmt.Errorf("block meta truncated")
	}
	switch data[pos] {
	case 0:
		pos++
	case 1:
		pos++
		if pos+80 > len(data) {
			return nil, fmt.Errorf("block meta masterchain ref truncated")
		}
		ref, err := decodeBlockID(data[pos : pos+80])
		if err != nil {
			return nil, err
		}
		meta.MasterchainRef = &ref
		pos += 80
	default:
		return nil, fmt.Errorf("invalid block meta masterchain ref flag %d", data[pos])
	}
	if pos >= len(data) {
		return nil, fmt.Errorf("block meta prev refs count missing")
	}
	prevCount := int(data[pos])
	pos++
	meta.PrevRefs = make([]ton.BlockIDExt, 0, prevCount)
	for i := 0; i < prevCount; i++ {
		if pos+80 > len(data) {
			return nil, fmt.Errorf("block meta prev refs truncated")
		}
		ref, err := decodeBlockID(data[pos : pos+80])
		if err != nil {
			return nil, err
		}
		meta.PrevRefs = append(meta.PrevRefs, ref)
		pos += 80
	}
	if pos != len(data) {
		return nil, fmt.Errorf("block meta has %d trailing bytes", len(data)-pos)
	}
	return meta, nil
}

func encodeBlockStateMeta(state *storage.BlockState) []byte {
	buf := make([]byte, 0, 169)
	buf = append(buf, blockStateMetaVersion)
	buf = appendLenBytes(buf, state.StateRootHash)
	buf = appendLenBytes(buf, state.StateCellHash)
	buf = appendLenBytes(buf, state.StateFileHash)
	if state.MasterchainRef == nil {
		buf = append(buf, 0)
	} else {
		buf = append(buf, 1)
		buf = append(buf, encodeBlockID(*state.MasterchainRef)...)
	}
	return buf
}

func decodeBlockStateMeta(data []byte) ([]byte, []byte, []byte, *ton.BlockIDExt, error) {
	if len(data) < 1+1+1+1 {
		return nil, nil, nil, nil, fmt.Errorf("block state meta payload too small")
	}
	if data[0] != blockStateMetaVersion {
		return nil, nil, nil, nil, fmt.Errorf("unsupported block state meta version %d", data[0])
	}
	pos := 1

	root, pos, err := readLenBytes(data, pos)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	cellHash, pos, err := readLenBytes(data, pos)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	file, pos, err := readLenBytes(data, pos)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	if pos >= len(data) {
		return nil, nil, nil, nil, fmt.Errorf("block state meta masterchain ref flag missing")
	}
	var masterRef *ton.BlockIDExt
	switch data[pos] {
	case 0:
		pos++
	case 1:
		pos++
		if pos+80 > len(data) {
			return nil, nil, nil, nil, fmt.Errorf("block state meta masterchain ref truncated")
		}
		ref, err := decodeBlockID(data[pos : pos+80])
		if err != nil {
			return nil, nil, nil, nil, err
		}
		masterRef = &ref
		pos += 80
	default:
		return nil, nil, nil, nil, fmt.Errorf("invalid block state meta masterchain ref flag %d", data[pos])
	}
	if pos != len(data) {
		return nil, nil, nil, nil, fmt.Errorf("block state meta has %d trailing bytes", len(data)-pos)
	}
	return root, cellHash, file, masterRef, nil
}

func encodeCurrentState(state *storage.CurrentState) []byte {
	buf := make([]byte, 0, 1024)
	buf = append(buf, currentStateVersion)
	buf = binary.BigEndian.AppendUint64(buf, uint64(state.SyncedAt.UnixNano()))
	buf = binary.BigEndian.AppendUint32(buf, state.ShardClientSeqno)
	buf = append(buf, encodeBlockID(state.Masterchain.Block)...)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(state.Shards)))
	for _, key := range storage.SortedShardKeys(state.Shards) {
		buf = append(buf, encodeBlockID(state.Shards[key].Block)...)
	}
	return buf
}

func decodeCurrentState(data []byte) (*storage.CurrentState, error) {
	if len(data) < 1+8+4+80+4 {
		return nil, fmt.Errorf("current state payload too small")
	}
	if data[0] != currentStateVersion {
		return nil, fmt.Errorf("unsupported current state version %d", data[0])
	}
	pos := 1
	state := &storage.CurrentState{
		SyncedAt: time.Unix(0, int64(binary.BigEndian.Uint64(data[pos:pos+8]))),
		Shards:   map[storage.ShardKey]storage.BlockState{},
	}
	pos += 8
	state.ShardClientSeqno = binary.BigEndian.Uint32(data[pos : pos+4])
	pos += 4
	master, err := decodeBlockID(data[pos : pos+80])
	if err != nil {
		return nil, err
	}
	state.Masterchain = storage.BlockState{Block: master}
	pos += 80
	shardCount := int(binary.BigEndian.Uint32(data[pos : pos+4]))
	pos += 4
	for i := 0; i < shardCount; i++ {
		if pos+80 > len(data) {
			return nil, fmt.Errorf("current state shards truncated")
		}
		block, err := decodeBlockID(data[pos : pos+80])
		if err != nil {
			return nil, err
		}
		pos += 80
		state.Shards[storage.ShardKeyFromBlock(block)] = storage.BlockState{Block: block}
	}
	if pos != len(data) {
		return nil, fmt.Errorf("current state has %d trailing bytes", len(data)-pos)
	}
	return state, nil
}

func encodeCellRecord(record *storage.CellRecord) []byte {
	buf := make([]byte, cellRecordEncodedLen(record.D2, record.Refs))
	encodeCellRecordTo(buf, record)
	return buf
}

func encodeCellRecordTo(buf []byte, record *storage.CellRecord) {
	slowRefs, compactRefs := cellRecordCompactRefLayout(record.Refs)
	pos := 0
	d1 := record.D1
	if compactRefs {
		d1 |= cellRecordCompactRefsFlag
	}
	buf[pos] = d1
	buf[pos+1] = record.D2
	pos += 2

	copy(buf[pos:], record.Data)
	pos += len(record.Data)
	if compactRefs {
		buf[pos] = slowRefs
		pos++
	}
	for i, ref := range record.Refs {
		if compactRefs && slowRefs&(1<<uint(i)) == 0 {
			copy(buf[pos:pos+cellRecordHashSize], ref.Hashes)
			pos += cellRecordHashSize
			copy(buf[pos:pos+cellRecordDepthSize], ref.Depths)
			pos += cellRecordDepthSize
			continue
		}

		buf[pos] = ref.LevelMask
		pos++
		copy(buf[pos:], ref.Hashes)
		pos += len(ref.Hashes)
		copy(buf[pos:], ref.Depths)
		pos += len(ref.Depths)
	}
}

func cellRecordEncodedLen(d2 byte, refs []storage.CellRefRecord) int {
	size := 2 + int(d2/2+d2%2)
	slowRefs, compactRefs := cellRecordCompactRefLayout(refs)
	if compactRefs {
		size++
	}
	for i, ref := range refs {
		if compactRefs && slowRefs&(1<<uint(i)) == 0 {
			size += cellRecordHashSize + cellRecordDepthSize
			continue
		}
		size += 1 + len(ref.Hashes) + len(ref.Depths)
	}
	return size
}

func cellRecordRefCommon(ref storage.CellRefRecord) bool {
	return ref.LevelMask == 0 && len(ref.Hashes) == cellRecordHashSize && len(ref.Depths) == cellRecordDepthSize
}

func cellRecordCompactRefLayout(refs []storage.CellRefRecord) (byte, bool) {
	if len(refs) == 0 {
		return 0, false
	}

	refsSize := 0
	compactRefsSize := 1
	hasCommonRef := false
	var slowRefs byte
	for i, ref := range refs {
		refSize := 1 + len(ref.Hashes) + len(ref.Depths)
		refsSize += refSize
		if cellRecordRefCommon(ref) {
			hasCommonRef = true
			compactRefsSize += cellRecordHashSize + cellRecordDepthSize
			continue
		}
		slowRefs |= 1 << uint(i)
		compactRefsSize += refSize
	}
	return slowRefs, hasCommonRef && compactRefsSize <= refsSize
}

func decodeCellRecord(hash []byte, data []byte) (*storage.CellRecord, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("cell record payload too small")
	}
	return decodeCellRecordBytes(hash, data, true)
}

func decodeCellRecordBytes(hash []byte, data []byte, clone bool) (*storage.CellRecord, error) {
	if len(hash) != 32 {
		return nil, fmt.Errorf("cell hash size mismatch: %d", len(hash))
	}

	pos := 0
	if len(data)-pos < 2 {
		return nil, fmt.Errorf("cell record payload too small")
	}

	storedD1 := data[pos]
	compactRefs := storedD1&cellRecordCompactRefsFlag != 0
	record := &storage.CellRecord{
		Hash: hash,
		D1:   storedD1 &^ cellRecordCompactRefsFlag,
		D2:   data[pos+1],
	}
	if clone {
		record.Hash = bytes.Clone(hash)
	}
	pos += 2

	refsCount := int(record.D1 & 7)
	if refsCount > 4 {
		return nil, fmt.Errorf("invalid cell refs count %d", refsCount)
	}
	dataLen := int(record.D2/2 + record.D2%2)
	if len(data)-pos < dataLen {
		return nil, fmt.Errorf("cell record payload truncated")
	}
	record.Data = data[pos : pos+dataLen]
	if clone {
		record.Data = bytes.Clone(record.Data)
	}
	pos += dataLen

	record.Refs = make([]storage.CellRefRecord, 0, refsCount)
	var slowRefs byte
	if compactRefs && refsCount > 0 {
		if pos >= len(data) {
			return nil, fmt.Errorf("cell record compact ref layout truncated")
		}
		slowRefs = data[pos]
		pos++
		if slowRefs&^byte((1<<uint(refsCount))-1) != 0 {
			return nil, fmt.Errorf("cell record compact ref layout has invalid slow refs mask %d", slowRefs)
		}
	}
	for i := 0; i < refsCount; i++ {
		if compactRefs && slowRefs&(1<<uint(i)) == 0 {
			if len(data)-pos < cellRecordHashSize+cellRecordDepthSize {
				return nil, fmt.Errorf("cell record compact ref metadata truncated")
			}
			hashes := data[pos : pos+cellRecordHashSize]
			pos += cellRecordHashSize
			depths := data[pos : pos+cellRecordDepthSize]
			pos += cellRecordDepthSize
			if clone {
				hashes = bytes.Clone(hashes)
				depths = bytes.Clone(depths)
			}
			record.Refs = append(record.Refs, storage.CellRefRecord{
				LevelMask: 0,
				Hashes:    hashes,
				Depths:    depths,
			})
			continue
		}

		if pos >= len(data) {
			return nil, fmt.Errorf("cell record ref metadata truncated")
		}
		levelMask := data[pos]
		pos++
		hashesCount := storage.CellRefHashesCount(levelMask)
		hashesLen := hashesCount * 32
		depthsLen := hashesCount * 2
		if len(data)-pos < hashesLen+depthsLen {
			return nil, fmt.Errorf("cell record ref metadata truncated")
		}
		hashes := data[pos : pos+hashesLen]
		pos += hashesLen
		depths := data[pos : pos+depthsLen]
		pos += depthsLen
		if clone {
			hashes = bytes.Clone(hashes)
			depths = bytes.Clone(depths)
		}
		record.Refs = append(record.Refs, storage.CellRefRecord{
			LevelMask: levelMask,
			Hashes:    hashes,
			Depths:    depths,
		})
	}
	if pos != len(data) {
		return nil, fmt.Errorf("cell record payload has trailing bytes")
	}
	return record, nil
}

func encodeInt64(v int64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(v))
	return buf[:]
}

func encodeArtifactRef(ref *storage.ArtifactRef) []byte {
	path := []byte(ref.Path)
	buf := make([]byte, 0, 1+8+8+4+len(path))
	buf = append(buf, artifactRefVersion)
	buf = binary.BigEndian.AppendUint64(buf, uint64(ref.Offset))
	buf = binary.BigEndian.AppendUint64(buf, uint64(ref.Size))
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(path)))
	return append(buf, path...)
}

func decodeArtifactRef(data []byte) (*storage.ArtifactRef, error) {
	const fixed = 1 + 8 + 8 + 4
	if len(data) < fixed {
		return nil, fmt.Errorf("artifact ref payload truncated")
	}
	if data[0] != artifactRefVersion {
		return nil, fmt.Errorf("artifact ref version mismatch")
	}
	offset := int64(binary.BigEndian.Uint64(data[1:9]))
	size := int64(binary.BigEndian.Uint64(data[9:17]))
	pathLen := int(binary.BigEndian.Uint32(data[17:21]))
	if len(data) != fixed+pathLen {
		return nil, fmt.Errorf("artifact ref payload size mismatch")
	}
	if offset < 0 || size < 0 {
		return nil, fmt.Errorf("artifact ref has invalid range")
	}
	return &storage.ArtifactRef{
		Path:   string(data[fixed:]),
		Offset: offset,
		Size:   size,
	}, nil
}

func encodeArchivePackageMeta(meta archivePackageMeta) []byte {
	path := []byte(meta.path)
	buf := make([]byte, 0, 1+8+4+4+4+8+8+4+4+8+4+len(path))
	buf = append(buf, archivePackageVersion)
	buf = binary.BigEndian.AppendUint64(buf, uint64(meta.archiveID))
	buf = binary.BigEndian.AppendUint32(buf, meta.baseSeq)
	buf = binary.BigEndian.AppendUint32(buf, meta.startSeq)
	buf = binary.BigEndian.AppendUint32(buf, uint32(meta.workchain))
	buf = binary.BigEndian.AppendUint64(buf, uint64(meta.shard))
	buf = binary.BigEndian.AppendUint64(buf, uint64(meta.size))
	buf = binary.BigEndian.AppendUint32(buf, meta.firstMasterSeq)
	buf = binary.BigEndian.AppendUint32(buf, meta.firstMasterUTime)
	buf = binary.BigEndian.AppendUint64(buf, meta.firstMasterLT)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(path)))
	return append(buf, path...)
}

func decodeArchivePackageMeta(data []byte) (archivePackageMeta, error) {
	const fixed = 1 + 8 + 4 + 4 + 4 + 8 + 8 + 4 + 4 + 8 + 4
	if len(data) < fixed {
		return archivePackageMeta{}, fmt.Errorf("archive package payload truncated")
	}
	if data[0] != archivePackageVersion {
		return archivePackageMeta{}, fmt.Errorf("archive package version mismatch")
	}
	pos := 1
	meta := archivePackageMeta{
		archiveID: int64(binary.BigEndian.Uint64(data[pos : pos+8])),
	}
	pos += 8
	meta.baseSeq = binary.BigEndian.Uint32(data[pos : pos+4])
	pos += 4
	meta.startSeq = binary.BigEndian.Uint32(data[pos : pos+4])
	pos += 4
	meta.workchain = int32(binary.BigEndian.Uint32(data[pos : pos+4]))
	pos += 4
	meta.shard = int64(binary.BigEndian.Uint64(data[pos : pos+8]))
	pos += 8
	meta.size = int64(binary.BigEndian.Uint64(data[pos : pos+8]))
	pos += 8
	meta.firstMasterSeq = binary.BigEndian.Uint32(data[pos : pos+4])
	pos += 4
	meta.firstMasterUTime = binary.BigEndian.Uint32(data[pos : pos+4])
	pos += 4
	meta.firstMasterLT = binary.BigEndian.Uint64(data[pos : pos+8])
	pos += 8
	pathLen := int(binary.BigEndian.Uint32(data[pos : pos+4]))
	pos += 4
	if len(data) != fixed+pathLen {
		return archivePackageMeta{}, fmt.Errorf("archive package payload size mismatch")
	}
	if meta.size < 0 {
		return archivePackageMeta{}, fmt.Errorf("archive package has invalid signed fields")
	}
	meta.path = string(data[pos:])
	return meta, nil
}

type persistentStateFileRecord struct {
	ref           *storage.ArtifactRef
	fileHash      []byte
	stateRootHash []byte
}

func encodePersistentStateFileRecord(file *storage.PersistentStateFile) []byte {
	ref := encodeArtifactRef(file.Ref)
	buf := make([]byte, 0, 1+1+len(file.FileHash)+1+len(file.StateRootHash)+4+len(ref))
	buf = append(buf, persistentStateVersion)
	buf = appendLenBytes(buf, file.FileHash)
	buf = appendLenBytes(buf, file.StateRootHash)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(ref)))
	return append(buf, ref...)
}

func decodePersistentStateFileRecord(data []byte) (*persistentStateFileRecord, error) {
	if len(data) < 1+1+1+4 {
		return nil, fmt.Errorf("persistent state file payload truncated")
	}
	if data[0] != persistentStateVersion {
		return nil, fmt.Errorf("persistent state file version mismatch")
	}
	pos := 1

	fileHash, next, err := readLenBytes(data, pos)
	if err != nil {
		return nil, err
	}
	pos = next

	stateRootHash, next, err := readLenBytes(data, pos)
	if err != nil {
		return nil, err
	}
	pos = next

	if len(data)-pos < 4 {
		return nil, fmt.Errorf("persistent state file payload truncated")
	}
	refLen := int(binary.BigEndian.Uint32(data[pos : pos+4]))
	pos += 4
	if refLen <= 0 || len(data)-pos != refLen {
		return nil, fmt.Errorf("persistent state file payload size mismatch")
	}
	ref, err := decodeArtifactRef(data[pos:])
	if err != nil {
		return nil, err
	}
	return &persistentStateFileRecord{
		ref:           ref,
		fileHash:      fileHash,
		stateRootHash: stateRootHash,
	}, nil
}

func appendLenBytes(dst []byte, data []byte) []byte {
	dst = append(dst, byte(len(data)))
	return append(dst, data...)
}

func readLenBytes(src []byte, pos int) ([]byte, int, error) {
	if pos >= len(src) {
		return nil, pos, fmt.Errorf("payload truncated")
	}
	ln := int(src[pos])
	pos++
	if pos+ln > len(src) {
		return nil, pos, fmt.Errorf("payload truncated")
	}
	return bytes.Clone(src[pos : pos+ln]), pos + ln, nil
}

func blockMetaServedFlags(isLink bool) storage.BlockMetaFlags {
	flags := storage.BlockMetaHasServedFull
	if isLink {
		flags |= storage.BlockMetaServedFullIsLink
	}
	return flags
}
