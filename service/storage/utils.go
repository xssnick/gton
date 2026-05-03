package storage

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const topShard = int64(-1 << 63)

var (
	bocMagic          = [4]byte{0xB5, 0xEE, 0x9C, 0x72}
	bocIdxMagic       = [4]byte{0x68, 0xFF, 0x65, 0xF3}
	bocIdxCRC32CMagic = [4]byte{0xAC, 0xC3, 0xA7, 0x28}
)

func StoredProofKinds(blockData []byte, isLink bool) []ServedProofKind {
	return StoredProofKindsForBlock(isLink, isKeyBlockData(blockData))
}

func StoredProofKindsForBlock(isLink bool, isKeyBlock bool) []ServedProofKind {
	kinds := make([]ServedProofKind, 0, 2)
	if isLink {
		kinds = append(kinds, ServedProofBlockLink)
		if isKeyBlock {
			kinds = append(kinds, ServedProofKeyBlockLink)
		}
		return kinds
	}

	kinds = append(kinds, ServedProofBlock)
	if isKeyBlock {
		kinds = append(kinds, ServedProofKeyBlock)
	}
	return kinds
}

func FormatBlockRef(block ton.BlockIDExt) string {
	return fmt.Sprintf("wc=%d shard=%016x seqno=%d", block.Workchain, uint64(block.Shard), block.SeqNo)
}

func ParseStateBOC(expected *ton.BlockIDExt, data []byte, wantRootHash []byte, wantFileHash []byte) (*BlockState, error) {
	cellsCount, err := BOCCellsCount(data)
	if err != nil {
		return nil, fmt.Errorf("parse state boc header: %w", err)
	}

	root, err := cell.FromBOC(data)
	if err != nil {
		return nil, fmt.Errorf("parse state boc: %w", err)
	}
	state, err := ParseStateCell(expected, root, data, wantRootHash, wantFileHash)
	if err != nil {
		return nil, err
	}
	state.CellsCount = cellsCount
	return state, nil
}

func BOCCellsCount(data []byte) (uint64, error) {
	return bocCellsCount(data)
}

func ParseStateCell(expected *ton.BlockIDExt, root *cell.Cell, rawBOC []byte, wantRootHash []byte, wantFileHash []byte) (*BlockState, error) {
	return parseStateCell(expected, root, rawBOC, wantRootHash, wantFileHash, false)
}

func ParseStateProof(expected *ton.BlockIDExt, root *cell.Cell, rawBOC []byte, wantRootHash []byte, wantFileHash []byte) (*BlockState, error) {
	return parseStateCell(expected, root, rawBOC, wantRootHash, wantFileHash, true)
}

func parseStateCell(expected *ton.BlockIDExt, root *cell.Cell, rawBOC []byte, wantRootHash []byte, wantFileHash []byte, proof bool) (*BlockState, error) {
	if expected == nil {
		return nil, fmt.Errorf("expected block id is nil")
	}
	if root == nil {
		return nil, fmt.Errorf("state root is nil")
	}
	rootHash := root.HashKey(0)
	rootCellHash := root.HashKey()

	var fileHash []byte
	if len(rawBOC) == 0 && len(wantFileHash) > 0 {
		rawBOC = root.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false})
	}
	if len(wantRootHash) > 0 && !bytes.Equal(rootHash[:], wantRootHash) {
		return nil, fmt.Errorf("state root hash mismatch for %s", FormatBlockRef(*expected))
	}

	if len(rawBOC) > 0 {
		sum := sha256.Sum256(rawBOC)
		fileHash = bytes.Clone(sum[:])
		if len(wantFileHash) > 0 && !bytes.Equal(fileHash, wantFileHash) {
			return nil, fmt.Errorf("state file hash mismatch for %s", FormatBlockRef(*expected))
		}
	}

	parsed, err := parseShardState(root, proof)
	if err != nil {
		return nil, err
	}

	workchain, shard := tlb.ConvertShardIdentToShard(parsed.ShardIdent)
	if workchain != expected.Workchain || int64(shard) != expected.Shard {
		return nil, fmt.Errorf(
			"state shard mismatch for %s: got wc=%d shard=%016x",
			FormatBlockRef(*expected),
			workchain,
			shard,
		)
	}
	if parsed.Seqno != expected.SeqNo {
		return nil, fmt.Errorf("state seqno mismatch for %s: got %d", FormatBlockRef(*expected), parsed.Seqno)
	}
	if expected.Workchain == -1 && parsed.McStateExtra == nil {
		return nil, fmt.Errorf("masterchain state for %s is missing mc_state_extra", FormatBlockRef(*expected))
	}

	return &BlockState{
		Block:         *expected,
		StateRootHash: rootHash[:],
		StateCellHash: rootCellHash[:],
		StateFileHash: fileHash,
		CellsCount:    0,
		Cell:          root,
		Parsed:        parsed,
	}, nil
}

func parseShardState(root *cell.Cell, proof bool) (*tlb.ShardStateUnsplit, error) {
	if !proof {
		var parsed tlb.ShardStateUnsplit
		if err := tlb.LoadFromCell(&parsed, root.BeginParse()); err != nil {
			return nil, fmt.Errorf("parse shard state: %w", err)
		}
		return &parsed, nil
	}

	var header shardStateHeader
	if err := tlb.LoadFromCell(&header, root.BeginParse()); err != nil {
		return nil, fmt.Errorf("parse shard state header: %w", err)
	}

	return &tlb.ShardStateUnsplit{
		GlobalID:        header.GlobalID,
		ShardIdent:      header.ShardIdent,
		Seqno:           header.Seqno,
		VertSeqno:       header.VertSeqno,
		GenUTime:        header.GenUTime,
		GenLT:           header.GenLT,
		MinRefMCSeqno:   header.MinRefMCSeqno,
		OutMsgQueueInfo: header.OutMsgQueueInfo,
		BeforeSplit:     header.BeforeSplit,
		Stats:           header.Stats,
		McStateExtra:    header.McStateExtra,
	}, nil
}

type shardStateHeader struct {
	_               tlb.Magic      `tlb:"#9023afe2"`
	GlobalID        int32          `tlb:"## 32"`
	ShardIdent      tlb.ShardIdent `tlb:"."`
	Seqno           uint32         `tlb:"## 32"`
	VertSeqno       uint32         `tlb:"## 32"`
	GenUTime        uint32         `tlb:"## 32"`
	GenLT           uint64         `tlb:"## 64"`
	MinRefMCSeqno   uint32         `tlb:"## 32"`
	OutMsgQueueInfo *cell.Cell     `tlb:"^"`
	BeforeSplit     bool           `tlb:"bool"`
	Accounts        *cell.Cell     `tlb:"^"`
	Stats           *cell.Cell     `tlb:"^"`
	McStateExtra    *cell.Cell     `tlb:"maybe ^"`
}

func bocCellsCount(data []byte) (uint64, error) {
	if len(data) < 10 {
		return 0, fmt.Errorf("invalid boc")
	}

	var magic [4]byte
	copy(magic[:], data[:4])
	switch magic {
	case bocMagic, bocIdxMagic, bocIdxCRC32CMagic:
	default:
		return 0, fmt.Errorf("invalid boc magic header")
	}

	flags := data[4]
	cellNumSizeBytes := int(flags & 0b00000111)
	if cellNumSizeBytes < 1 || cellNumSizeBytes > 4 {
		return 0, fmt.Errorf("invalid boc size descriptor")
	}
	dataSizeBytes := int(data[5])
	if dataSizeBytes < 1 || dataSizeBytes > 8 {
		return 0, fmt.Errorf("invalid boc offset descriptor")
	}
	if len(data) < 6+cellNumSizeBytes {
		return 0, fmt.Errorf("invalid boc counters")
	}

	return readBigEndianUint(data[6 : 6+cellNumSizeBytes]), nil
}

func readBigEndianUint(data []byte) uint64 {
	switch len(data) {
	case 1:
		return uint64(data[0])
	case 2:
		return uint64(binary.BigEndian.Uint16(data))
	case 3:
		return uint64(data[0])<<16 | uint64(data[1])<<8 | uint64(data[2])
	case 4:
		return uint64(binary.BigEndian.Uint32(data))
	default:
		var v uint64
		for _, b := range data {
			v = (v << 8) | uint64(b)
		}
		return v
	}
}

func isKeyBlockData(data []byte) bool {
	root, err := cell.FromBOC(data)
	if err != nil {
		return false
	}

	var block tlb.Block
	if err = tlb.LoadFromCell(&block, root.BeginParse()); err != nil {
		return false
	}
	return block.BlockInfo.KeyBlock
}
