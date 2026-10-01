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

func encodeBlockIDHashes(id ton.BlockIDExt) []byte {
	buf := make([]byte, 0, 64)
	buf = append(buf, id.RootHash...)
	return append(buf, id.FileHash...)
}

func decodeBlockIDFromHashes(workchain int32, shard int64, seqno uint32, data []byte) (ton.BlockIDExt, error) {
	if len(data) != 64 {
		return ton.BlockIDExt{}, fmt.Errorf("invalid block id hash payload size %d", len(data))
	}
	return ton.BlockIDExt{
		Workchain: workchain,
		Shard:     shard,
		SeqNo:     seqno,
		RootHash:  bytes.Clone(data[:32]),
		FileHash:  bytes.Clone(data[32:64]),
	}, nil
}

// blockMetaMinEncodedLen is the fixed header of an encoded block meta:
// version byte, flags word, gen utime, start/end lt, the two hash length
// bytes, the masterchain ref flag byte and the prev/next ref count bytes.
const blockMetaMinEncodedLen = 1 + 4 + 4 + 8 + 8 + 1 + 1 + 1 + 1 + 1

func encodeBlockMeta(meta *storage.BlockMeta) []byte {
	buf := make([]byte, 0, 256)
	buf = append(buf, blockMetaVersion)
	buf = binary.BigEndian.AppendUint32(buf, uint32(meta.Flags))
	buf = binary.BigEndian.AppendUint32(buf, meta.GenUTime)
	buf = binary.BigEndian.AppendUint64(buf, meta.StartLT)
	buf = binary.BigEndian.AppendUint64(buf, meta.EndLT)
	buf = appendLenBytes(buf, meta.StateRootHash)
	buf = appendLenBytes(buf, meta.StateFileHash)
	if !meta.MasterchainRefKnown() {
		buf = append(buf, 0)
	} else {
		buf = append(buf, 1)
		buf = binary.BigEndian.AppendUint32(buf, meta.MasterchainRefSeqno)
	}
	buf = append(buf, byte(len(meta.PrevRefs)))
	for _, ref := range meta.PrevRefs {
		buf = append(buf, encodeBlockID(ref)...)
	}
	buf = append(buf, byte(len(meta.NextRefs)))
	for _, ref := range meta.NextRefs {
		buf = append(buf, encodeBlockID(ref)...)
	}
	return buf
}

// decodeBlockMetaFlags reads just the flags word from an encoded block meta
// without decoding the whole record. It applies the same header validation
// (minimal payload length and version byte) as decodeBlockMeta; the flags
// live in the fixed header, so deeper payload corruption is not detected.
func decodeBlockMetaFlags(data []byte) (storage.BlockMetaFlags, error) {
	if len(data) < blockMetaMinEncodedLen {
		return 0, fmt.Errorf("block meta payload too small")
	}
	if data[0] != blockMetaVersion {
		return 0, fmt.Errorf("unsupported block meta version %d", data[0])
	}
	return storage.BlockMetaFlags(binary.BigEndian.Uint32(data[1:5])), nil
}

func decodeBlockMeta(id ton.BlockIDExt, data []byte) (*storage.BlockMeta, error) {
	if len(data) < blockMetaMinEncodedLen {
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
		if pos+4 > len(data) {
			return nil, fmt.Errorf("block meta masterchain ref truncated")
		}
		meta.MasterchainRefSeqno = binary.BigEndian.Uint32(data[pos : pos+4])
		pos += 4
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
	if pos >= len(data) {
		return nil, fmt.Errorf("block meta next refs count missing")
	}
	nextCount := int(data[pos])
	pos++
	meta.NextRefs = make([]ton.BlockIDExt, 0, nextCount)
	for i := 0; i < nextCount; i++ {
		if pos+80 > len(data) {
			return nil, fmt.Errorf("block meta next refs truncated")
		}
		ref, err := decodeBlockID(data[pos : pos+80])
		if err != nil {
			return nil, err
		}
		meta.NextRefs = append(meta.NextRefs, ref)
		pos += 80
	}
	if pos != len(data) {
		return nil, fmt.Errorf("block meta has %d trailing bytes", len(data)-pos)
	}
	return meta, nil
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

func encodeCellGenerationMigrationProgress(state *storage.CurrentState) []byte {
	buf := make([]byte, 0, 1024)
	buf = append(buf, cellGenerationMigrationVersion)
	buf = binary.BigEndian.AppendUint64(buf, uint64(state.SyncedAt.UnixNano()))
	buf = binary.BigEndian.AppendUint32(buf, state.ShardClientSeqno)
	buf = appendMigrationProgressBlockState(buf, &state.Masterchain)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(state.Shards)))
	for _, key := range storage.SortedShardKeys(state.Shards) {
		shard := state.Shards[key]
		buf = appendMigrationProgressBlockState(buf, &shard)
	}
	return buf
}

func decodeCellGenerationMigrationProgress(data []byte) (*storage.CurrentState, error) {
	if len(data) < 1+8+4+80+1+1+1+1+4 {
		return nil, fmt.Errorf("cell generation migration progress payload too small")
	}
	if data[0] != cellGenerationMigrationVersion {
		return nil, fmt.Errorf("unsupported cell generation migration progress version %d", data[0])
	}

	pos := 1
	state := &storage.CurrentState{
		SyncedAt: time.Unix(0, int64(binary.BigEndian.Uint64(data[pos:pos+8]))),
		Shards:   map[storage.ShardKey]storage.BlockState{},
	}
	pos += 8
	state.ShardClientSeqno = binary.BigEndian.Uint32(data[pos : pos+4])
	pos += 4

	master, next, err := readMigrationProgressBlockState(data, pos)
	if err != nil {
		return nil, fmt.Errorf("decode migration progress masterchain state: %w", err)
	}
	state.Masterchain = master
	pos = next

	if pos+4 > len(data) {
		return nil, fmt.Errorf("migration progress shard count missing")
	}
	shardCount := int(binary.BigEndian.Uint32(data[pos : pos+4]))
	pos += 4
	for i := 0; i < shardCount; i++ {
		shard, next, err := readMigrationProgressBlockState(data, pos)
		if err != nil {
			return nil, fmt.Errorf("decode migration progress shard state %d: %w", i, err)
		}
		state.Shards[storage.ShardKeyFromBlock(shard.Block)] = shard
		pos = next
	}
	if pos != len(data) {
		return nil, fmt.Errorf("cell generation migration progress has %d trailing bytes", len(data)-pos)
	}
	return state, nil
}

func appendMigrationProgressBlockState(dst []byte, state *storage.BlockState) []byte {
	dst = append(dst, encodeBlockID(state.Block)...)
	dst = appendLenBytes(dst, state.StateRootHash)
	dst = appendLenBytes(dst, state.StateFileHash)
	if state.MasterchainRef == nil {
		return append(dst, 0)
	}
	dst = append(dst, 1)
	return append(dst, encodeBlockID(*state.MasterchainRef)...)
}

func readMigrationProgressBlockState(data []byte, pos int) (storage.BlockState, int, error) {
	if pos+80 > len(data) {
		return storage.BlockState{}, pos, fmt.Errorf("block id truncated")
	}
	block, err := decodeBlockID(data[pos : pos+80])
	if err != nil {
		return storage.BlockState{}, pos, err
	}
	pos += 80

	root, pos, err := readLenBytes(data, pos)
	if err != nil {
		return storage.BlockState{}, pos, err
	}
	fileHash, pos, err := readLenBytes(data, pos)
	if err != nil {
		return storage.BlockState{}, pos, err
	}

	if pos >= len(data) {
		return storage.BlockState{}, pos, fmt.Errorf("masterchain ref flag missing")
	}
	var masterRef *ton.BlockIDExt
	switch data[pos] {
	case 0:
		pos++
	case 1:
		pos++
		if pos+80 > len(data) {
			return storage.BlockState{}, pos, fmt.Errorf("masterchain ref truncated")
		}
		ref, err := decodeBlockID(data[pos : pos+80])
		if err != nil {
			return storage.BlockState{}, pos, err
		}
		masterRef = &ref
		pos += 80
	default:
		return storage.BlockState{}, pos, fmt.Errorf("invalid masterchain ref flag %d", data[pos])
	}

	return storage.BlockState{
		Block:          block,
		StateRootHash:  root,
		StateFileHash:  fileHash,
		MasterchainRef: masterRef,
	}, pos, nil
}

func encodeInt64(v int64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(v))
	return buf[:]
}

const (
	artifactRefArchivePackageKind  = 1
	artifactRefKeyProofPackageKind = 2
)

func encodeArtifactRef(ref *storage.ArtifactRef) []byte {
	kind := byte(artifactRefArchivePackageKind)
	packageID := ref.ArchivePackageID
	if packageID < 0 {
		kind = artifactRefKeyProofPackageKind
		packageID = keyProofPackageIDFromRef(packageID)
	}
	buf := make([]byte, 0, 1+1+8+8+8)
	buf = append(buf, artifactRefVersion, kind)
	buf = binary.BigEndian.AppendUint64(buf, uint64(packageID))
	buf = binary.BigEndian.AppendUint64(buf, uint64(ref.Offset))
	return binary.BigEndian.AppendUint64(buf, uint64(ref.Size))
}

func decodeArtifactRef(data []byte) (*storage.ArtifactRef, error) {
	const fixed = 1 + 1 + 8 + 8 + 8
	if len(data) != fixed {
		return nil, fmt.Errorf("artifact ref payload size mismatch")
	}
	if data[0] != artifactRefVersion {
		return nil, fmt.Errorf("artifact ref version mismatch")
	}
	packageID := int64(binary.BigEndian.Uint64(data[2:10]))
	switch data[1] {
	case artifactRefArchivePackageKind:
	case artifactRefKeyProofPackageKind:
		packageID = keyProofPackageRefID(uint32(packageID))
	default:
		return nil, fmt.Errorf("unsupported artifact ref kind %d", data[1])
	}
	offset := int64(binary.BigEndian.Uint64(data[10:18]))
	size := int64(binary.BigEndian.Uint64(data[18:26]))
	if offset < 0 || size < 0 {
		return nil, fmt.Errorf("artifact ref has invalid range")
	}
	return &storage.ArtifactRef{
		ArchivePackage:   true,
		ArchivePackageID: packageID,
		Offset:           offset,
		Size:             size,
	}, nil
}

func encodeStateFileRef(size int64) []byte {
	buf := make([]byte, 0, 1+8)
	buf = append(buf, artifactRefVersion)
	return binary.BigEndian.AppendUint64(buf, uint64(size))
}

func decodeStateFileRef(data []byte) (int64, error) {
	if len(data) != 1+8 {
		return 0, fmt.Errorf("state file ref payload size mismatch")
	}
	if data[0] != artifactRefVersion {
		return 0, fmt.Errorf("state file ref version mismatch")
	}
	size := int64(binary.BigEndian.Uint64(data[1:9]))
	if size < 0 {
		return 0, fmt.Errorf("state file ref size is invalid")
	}
	return size, nil
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
	size          int64
	fileHash      []byte
	stateRootHash []byte
}

func encodePersistentStateFileRecord(file *storage.PersistentStateFile) []byte {
	buf := make([]byte, 0, 1+1+len(file.FileHash)+1+len(file.StateRootHash)+8)
	buf = append(buf, persistentStateVersion)
	buf = appendLenBytes(buf, file.FileHash)
	buf = appendLenBytes(buf, file.StateRootHash)
	return binary.BigEndian.AppendUint64(buf, uint64(file.Ref.Size))
}

func decodePersistentStateFileRecord(data []byte) (*persistentStateFileRecord, error) {
	if len(data) < 1+1+1+8 {
		return nil, fmt.Errorf("persistent state file payload truncated")
	}
	version := data[0]
	if version != persistentStateVersion && version != persistentStateCellsCountVersion {
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

	tailSize := 8
	if version == persistentStateCellsCountVersion {
		tailSize += 8
	}
	if len(data)-pos != tailSize {
		return nil, fmt.Errorf("persistent state file payload truncated")
	}
	size := int64(binary.BigEndian.Uint64(data[pos : pos+8]))
	if size < 0 {
		return nil, fmt.Errorf("persistent state file size is invalid")
	}
	return &persistentStateFileRecord{
		size:          size,
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
