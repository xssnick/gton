package pebblestore

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
)

var (
	hotPrefixBlockMeta             = []byte{0x01}
	hotPrefixNextBlock             = []byte{0x02}
	hotPrefixBlockSeq              = []byte{0x03}
	hotPrefixBlockLT               = []byte{0x04}
	hotPrefixBlockUTime            = []byte{0x05}
	hotPrefixCurrentState          = []byte{0x06}
	hotPrefixStateMeta             = []byte{0x07}
	hotPrefixArchiveInfo           = []byte{0x0B}
	hotPrefixStateSync             = []byte{0x0C}
	hotPrefixBlockDataRef          = []byte{0x0D}
	hotPrefixProofRef              = []byte{0x0E}
	hotPrefixArchiveFile           = []byte{0x0F}
	hotPrefixZeroStateRef          = []byte{0x11}
	hotPrefixKeyProofRef           = []byte{0x12}
	hotPrefixStateFileRef          = []byte{0x13}
	hotPrefixVerifiedKey           = []byte{0x14}
	hotPrefixPackCommitted         = []byte{0x15}
	hotPrefixPackStart             = []byte{0x16}
	hotPrefixStateSerializer       = []byte{0x17}
	hotPrefixStateDescription      = []byte{0x18}
	hotPrefixCellGeneration        = []byte{0x19}
	hotPrefixArchivePackage        = []byte{0x1A}
	hotPrefixStateSerializerActive = []byte{0x1B}
	hotPrefixKeyBlockSeq           = []byte{0x1C}
)

func hotKeyCellGenerationManifest() []byte {
	return bytes.Clone(hotPrefixCellGeneration)
}

func hotKeyBlockMeta(id ton.BlockIDExt) []byte {
	return appendPrefixAndBlockID(hotPrefixBlockMeta, id)
}

func hotKeyNextBlock(id ton.BlockIDExt) []byte {
	return appendPrefixAndBlockID(hotPrefixNextBlock, id)
}

func hotKeyBlockSeqIndex(key storage.BlockHistoryKey, seqno uint32) []byte {
	buf := appendHistoryPrefix(hotPrefixBlockSeq, key)
	return binary.BigEndian.AppendUint32(buf, seqno)
}

func hotKeyKeyBlockSeqIndex(seqno uint32) []byte {
	return binary.BigEndian.AppendUint32(bytes.Clone(hotPrefixKeyBlockSeq), seqno)
}

func hotKeyBlockLTPrefix(key storage.BlockHistoryKey) []byte {
	return appendHistoryPrefix(hotPrefixBlockLT, key)
}

func hotKeyBlockLTIndex(meta *storage.BlockMeta) []byte {
	buf := hotKeyBlockLTPrefix(storage.BlockHistoryKey{Workchain: meta.ID.Workchain, Shard: meta.ID.Shard})
	buf = binary.BigEndian.AppendUint64(buf, meta.EndLT)
	return binary.BigEndian.AppendUint32(buf, meta.ID.SeqNo)
}

func hotKeyBlockLTSeek(key storage.BlockHistoryKey, lt uint64) []byte {
	buf := hotKeyBlockLTPrefix(key)
	buf = binary.BigEndian.AppendUint64(buf, lt)
	return binary.BigEndian.AppendUint32(buf, math.MaxUint32)
}

func hotKeyBlockLTSeekGE(key storage.BlockHistoryKey, lt uint64) []byte {
	buf := hotKeyBlockLTPrefix(key)
	buf = binary.BigEndian.AppendUint64(buf, lt)
	return binary.BigEndian.AppendUint32(buf, 0)
}

func hotKeyBlockUTimePrefix(key storage.BlockHistoryKey) []byte {
	return appendHistoryPrefix(hotPrefixBlockUTime, key)
}

func hotKeyBlockUTimeIndex(meta *storage.BlockMeta) []byte {
	buf := hotKeyBlockUTimePrefix(storage.BlockHistoryKey{Workchain: meta.ID.Workchain, Shard: meta.ID.Shard})
	buf = binary.BigEndian.AppendUint32(buf, meta.GenUTime)
	return binary.BigEndian.AppendUint32(buf, meta.ID.SeqNo)
}

func hotKeyBlockUTimeSeek(key storage.BlockHistoryKey, utime uint32) []byte {
	buf := hotKeyBlockUTimePrefix(key)
	buf = binary.BigEndian.AppendUint32(buf, utime)
	return binary.BigEndian.AppendUint32(buf, math.MaxUint32)
}

func hotKeyCurrentState() []byte {
	return bytes.Clone(hotPrefixCurrentState)
}

func hotKeyStateSyncProgress() []byte {
	return bytes.Clone(hotPrefixStateSync)
}

func hotKeyVerifiedKeyBlockProgress() []byte {
	return bytes.Clone(hotPrefixVerifiedKey)
}

func hotKeyPersistentStateSerializer() []byte {
	return bytes.Clone(hotPrefixStateSerializer)
}

func hotKeyPersistentStateSerializerActive() []byte {
	return bytes.Clone(hotPrefixStateSerializerActive)
}

func hotKeyPersistentStateDescription(masterchainSeqno uint32) []byte {
	buf := append([]byte(nil), hotPrefixStateDescription...)
	return binary.BigEndian.AppendUint32(buf, masterchainSeqno)
}

func hotKeyPersistentStateDescriptionPrefix() []byte {
	return bytes.Clone(hotPrefixStateDescription)
}

func hotKeyStateMeta(id ton.BlockIDExt) []byte {
	return appendPrefixAndBlockID(hotPrefixStateMeta, id)
}

func hotKeyStateMetaMasterchainPrefix() []byte {
	buf := append([]byte(nil), hotPrefixStateMeta...)
	buf = binary.BigEndian.AppendUint32(buf, ^uint32(0))
	return binary.BigEndian.AppendUint64(buf, uint64(1)<<63)
}

func hotKeyArchiveInfo(masterchainSeqno int32, workchain int32, shard int64) []byte {
	buf := append([]byte(nil), hotPrefixArchiveInfo...)
	buf = binary.BigEndian.AppendUint32(buf, uint32(masterchainSeqno))
	buf = binary.BigEndian.AppendUint32(buf, uint32(workchain))
	return binary.BigEndian.AppendUint64(buf, uint64(shard))
}

func hotKeyBlockDataRef(id ton.BlockIDExt) []byte {
	return appendPrefixAndBlockID(hotPrefixBlockDataRef, id)
}

func hotKeyProofRef(kind storage.ServedProofKind, id ton.BlockIDExt) []byte {
	buf := append([]byte(nil), hotPrefixProofRef...)
	buf = append(buf, byte(proofKindOrder(kind)))
	return append(buf, encodeBlockID(id)...)
}

func hotKeyStoredProofRef(kind storage.ServedProofKind, id ton.BlockIDExt) []byte {
	if isKeyProofKind(kind) {
		return hotKeyKeyProofRef(kind, id)
	}
	return hotKeyProofRef(kind, id)
}

func hotKeyKeyProofRef(kind storage.ServedProofKind, id ton.BlockIDExt) []byte {
	buf := append([]byte(nil), hotPrefixKeyProofRef...)
	buf = append(buf, byte(proofKindOrder(kind)))
	return append(buf, encodeBlockID(id)...)
}

func hotKeyZeroStateRef(id ton.BlockIDExt) []byte {
	return appendPrefixAndBlockID(hotPrefixZeroStateRef, id)
}

func hotKeyPersistentStateFile(block ton.BlockIDExt, masterchainBlock ton.BlockIDExt, effectiveShard int64) []byte {
	buf := appendPrefixAndBlockID(hotPrefixStateFileRef, block)
	buf = append(buf, encodeBlockID(masterchainBlock)...)
	return binary.BigEndian.AppendUint64(buf, uint64(effectiveShard))
}

func hotKeyArchiveFile(archiveID int64) []byte {
	buf := append([]byte(nil), hotPrefixArchiveFile...)
	return binary.BigEndian.AppendUint64(buf, uint64(archiveID))
}

func hotKeyPackCommitted(path string) []byte {
	buf := append([]byte(nil), hotPrefixPackCommitted...)
	return append(buf, path...)
}

func hotKeyPackCommittedPrefix() []byte {
	return bytes.Clone(hotPrefixPackCommitted)
}

func hotKeyArchivePackageStart(seqno uint32) []byte {
	buf := append([]byte(nil), hotPrefixPackStart...)
	return binary.BigEndian.AppendUint32(buf, seqno)
}

func hotKeyArchivePackageStartPrefix() []byte {
	return bytes.Clone(hotPrefixPackStart)
}

func hotKeyArchivePackage(archiveID int64) []byte {
	buf := append([]byte(nil), hotPrefixArchivePackage...)
	return binary.BigEndian.AppendUint64(buf, uint64(archiveID))
}

func hotKeyArchivePackagePrefix() []byte {
	return bytes.Clone(hotPrefixArchivePackage)
}

func appendHistoryPrefix(prefix []byte, key storage.BlockHistoryKey) []byte {
	buf := append([]byte(nil), prefix...)
	buf = binary.BigEndian.AppendUint32(buf, uint32(key.Workchain))
	return binary.BigEndian.AppendUint64(buf, uint64(key.Shard))
}

func appendPrefixAndBlockID(prefix []byte, id ton.BlockIDExt) []byte {
	buf := append([]byte(nil), prefix...)
	return append(buf, encodeBlockID(id)...)
}

func prefixUpperBound(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}
	end := bytes.Clone(prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 0xFF {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

func proofKindOrder(kind storage.ServedProofKind) int {
	switch kind {
	case storage.ServedProofBlock:
		return 1
	case storage.ServedProofBlockLink:
		return 2
	case storage.ServedProofKeyBlock:
		return 3
	case storage.ServedProofKeyBlockLink:
		return 4
	default:
		return 0
	}
}
