package pebblestore

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
)

var (
	hotPrefixMetaDBVersion         = []byte{0x00}
	hotPrefixBlockMeta             = []byte{0x01}
	hotPrefixBlockSeq              = []byte{0x03}
	hotPrefixBlockLT               = []byte{0x04}
	hotPrefixBlockUTime            = []byte{0x05}
	hotPrefixCurrentState          = []byte{0x06}
	hotPrefixArchiveInfo           = []byte{0x0B}
	hotPrefixStateSync             = []byte{0x0C}
	hotPrefixBlockDataRef          = []byte{0x0D}
	hotPrefixProofRef              = []byte{0x0E}
	hotPrefixZeroStateRef          = []byte{0x11}
	hotPrefixKeyProofRef           = []byte{0x12}
	hotPrefixStateFileRef          = []byte{0x13}
	hotPrefixPackStart             = []byte{0x16}
	hotPrefixStateSerializer       = []byte{0x17}
	hotPrefixStateDescription      = []byte{0x18}
	hotPrefixCellGeneration        = []byte{0x19}
	hotPrefixArchivePackage        = []byte{0x1A}
	hotPrefixStateSerializerActive = []byte{0x1B}
	hotPrefixKeyBlockSeq           = []byte{0x1C}
	hotPrefixCellGenerationCurrent = []byte{0x1E}
	hotPrefixArchivePackageIndex   = []byte{0x1F}
	hotPrefixPackAppendDirty       = []byte{0x20}
	hotPrefixPackDeletePending     = []byte{0x21}
	hotPrefixMessageTransaction    = []byte{0x22}
)

func hotKeyMetaDBVersion() []byte {
	return bytes.Clone(hotPrefixMetaDBVersion)
}

func hotKeyCellGenerationManifest() []byte {
	return bytes.Clone(hotPrefixCellGeneration)
}

func hotKeyCellGenerationCurrent(generation uint64) []byte {
	return binary.BigEndian.AppendUint64(bytes.Clone(hotPrefixCellGenerationCurrent), generation)
}

func hotKeyBlockMeta(id ton.BlockIDExt) []byte {
	return appendPrefixAndBlockID(hotPrefixBlockMeta, id)
}

func hotKeyBlockSeqIndex(ref storage.BlockSeqRef) []byte {
	buf := appendHistoryPrefix(hotPrefixBlockSeq, ref.HistoryKey())
	return binary.BigEndian.AppendUint32(buf, ref.SeqNo)
}

func hotKeyKeyBlockSeqIndex(seqno uint32) []byte {
	return binary.BigEndian.AppendUint32(bytes.Clone(hotPrefixKeyBlockSeq), seqno)
}

func hotKeyBlockLTPrefix(key storage.BlockHistoryKey) []byte {
	return appendHistoryPrefix(hotPrefixBlockLT, key)
}

func hotKeyBlockLTWorkchainPrefix(workchain int32) []byte {
	buf := append([]byte(nil), hotPrefixBlockLT...)
	return binary.BigEndian.AppendUint32(buf, uint32(workchain))
}

func hotKeyBlockLTIndex(ref storage.BlockSeqRef, endLT uint64) []byte {
	buf := hotKeyBlockLTPrefix(ref.HistoryKey())
	buf = binary.BigEndian.AppendUint64(buf, endLT)
	return binary.BigEndian.AppendUint32(buf, ref.SeqNo)
}

func hotKeyBlockLTSeekGE(key storage.BlockHistoryKey, lt uint64) []byte {
	buf := hotKeyBlockLTPrefix(key)
	buf = binary.BigEndian.AppendUint64(buf, lt)
	return binary.BigEndian.AppendUint32(buf, 0)
}

func hotKeyBlockUTimePrefix(key storage.BlockHistoryKey) []byte {
	return appendHistoryPrefix(hotPrefixBlockUTime, key)
}

func hotKeyBlockUTimeIndex(ref storage.BlockSeqRef, utime uint32) []byte {
	buf := hotKeyBlockUTimePrefix(ref.HistoryKey())
	buf = binary.BigEndian.AppendUint32(buf, utime)
	return binary.BigEndian.AppendUint32(buf, ref.SeqNo)
}

func hotKeyBlockUTimeSeekGE(key storage.BlockHistoryKey, utime uint32) []byte {
	buf := hotKeyBlockUTimePrefix(key)
	buf = binary.BigEndian.AppendUint32(buf, utime)
	return binary.BigEndian.AppendUint32(buf, 0)
}

func hotKeyMessageTransaction(kind storage.MessageTransactionKind, key storage.MessageTransactionKey) []byte {
	buf := append([]byte(nil), hotPrefixMessageTransaction...)
	buf = append(buf, byte(kind))
	buf = appendMessageTransactionAddress(buf, key.Source)
	buf = appendMessageTransactionAddress(buf, key.Destination)
	return binary.BigEndian.AppendUint64(buf, key.CreatedLT)
}

func appendMessageTransactionAddress(buf []byte, addr storage.MessageTransactionAddress) []byte {
	buf = binary.BigEndian.AppendUint32(buf, uint32(addr.Workchain))
	return append(buf, addr.Account[:]...)
}

func hotKeyCurrentState() []byte {
	return bytes.Clone(hotPrefixCurrentState)
}

func hotKeyStateSyncProgress() []byte {
	return bytes.Clone(hotPrefixStateSync)
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

func hotKeyArchiveInfo(baseSeqno uint32, startSeqno uint32, workchain int32, shard int64) []byte {
	buf := append([]byte(nil), hotPrefixArchiveInfo...)
	buf = binary.BigEndian.AppendUint32(buf, baseSeqno)
	buf = binary.BigEndian.AppendUint32(buf, startSeqno)
	buf = binary.BigEndian.AppendUint32(buf, uint32(workchain))
	return binary.BigEndian.AppendUint64(buf, uint64(shard))
}

func hotKeyArchiveInfoBasePrefix(baseSeqno uint32) []byte {
	buf := append([]byte(nil), hotPrefixArchiveInfo...)
	return binary.BigEndian.AppendUint32(buf, baseSeqno)
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

func hotKeyArchivePackageIndex(baseSeqno uint32) []byte {
	buf := append([]byte(nil), hotPrefixArchivePackageIndex...)
	return binary.BigEndian.AppendUint32(buf, baseSeqno)
}

func hotKeyPackAppendDirty(path string) []byte {
	return append(bytes.Clone(hotPrefixPackAppendDirty), path...)
}

func hotKeyPackAppendDirtyPrefix() []byte {
	return bytes.Clone(hotPrefixPackAppendDirty)
}

func hotKeyPackDeletePending(path string) []byte {
	return append(bytes.Clone(hotPrefixPackDeletePending), path...)
}

func hotKeyPackDeletePendingPrefix() []byte {
	return bytes.Clone(hotPrefixPackDeletePending)
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

func decodePrefixedBlockID(prefix []byte, key []byte) (ton.BlockIDExt, error) {
	if len(key) != len(prefix)+80 {
		return ton.BlockIDExt{}, fmt.Errorf("invalid block key size %d for prefix %x", len(key), prefix)
	}
	return decodeBlockID(key[len(prefix):])
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
