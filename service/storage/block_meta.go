package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type BlockHistoryKey struct {
	Workchain int32
	Shard     int64
}

type BlockSeqRef struct {
	Workchain int32
	Shard     int64
	SeqNo     uint32
}

func BlockSeqRefFromBlock(block ton.BlockIDExt) BlockSeqRef {
	return BlockSeqRef{
		Workchain: block.Workchain,
		Shard:     block.Shard,
		SeqNo:     block.SeqNo,
	}
}

func (r BlockSeqRef) HistoryKey() BlockHistoryKey {
	return BlockHistoryKey{
		Workchain: r.Workchain,
		Shard:     r.Shard,
	}
}

var ErrInvalidBlockIDHashes = errors.New("block id has invalid hashes")

func BlockIDHashesKnown(id ton.BlockIDExt) bool {
	return len(id.RootHash) == 32 && len(id.FileHash) == 32
}

func ValidateBlockIDHashes(id ton.BlockIDExt) error {
	if BlockIDHashesKnown(id) {
		return nil
	}
	return ErrInvalidBlockIDHashes
}

// AccountShardCandidates returns the shard IDs along the account prefix path.
func AccountShardCandidates(workchain int32, account []byte) []int64 {
	if workchain == masterchainID {
		return []int64{masterchainShard}
	}
	if len(account) < 8 {
		return nil
	}

	prefix := binary.BigEndian.Uint64(account[:8])
	shards := make([]int64, 0, 61)
	shards = append(shards, masterchainShard)
	for length := 1; length <= 60; length++ {
		x := uint64(1) << (63 - uint(length))
		shard := (prefix & ^(x - 1)) | x
		shards = append(shards, int64(shard))
	}
	return shards
}

type BlockMetaFlags uint32

const (
	BlockMetaHasServedFull BlockMetaFlags = 1 << iota
	BlockMetaServedFullIsLink
	BlockMetaHasBlockData
	BlockMetaHasProofBlock
	BlockMetaHasProofBlockLink
	BlockMetaHasProofKeyBlock
	BlockMetaHasProofKeyBlockLink
	BlockMetaHasStateSnapshot
	BlockMetaIsKeyBlock
	BlockMetaHasStateCells
)

const (
	masterchainID    int32 = -1
	masterchainShard int64 = -1 << 63
)

type BlockMeta struct {
	ID            ton.BlockIDExt
	Flags         BlockMetaFlags
	GenUTime      uint32
	StartLT       uint64
	EndLT         uint64
	StateRootHash []byte
	StateFileHash []byte
	// MasterchainRefSeqno matches C++ BlockHandle::masterchain_ref_block seqno:
	// the masterchain block that applied/included this shard block, not BlockInfo.MasterRef.
	MasterchainRefSeqno uint32
	PrevRefs            []ton.BlockIDExt
	NextRefs            []ton.BlockIDExt
}

func (m *BlockMeta) Clone() *BlockMeta {
	if m == nil {
		return nil
	}

	cloned := &BlockMeta{
		ID:                  m.ID,
		Flags:               m.Flags,
		GenUTime:            m.GenUTime,
		StartLT:             m.StartLT,
		EndLT:               m.EndLT,
		StateRootHash:       bytes.Clone(m.StateRootHash),
		StateFileHash:       bytes.Clone(m.StateFileHash),
		MasterchainRefSeqno: m.MasterchainRefSeqno,
	}
	if len(m.PrevRefs) > 0 {
		cloned.PrevRefs = make([]ton.BlockIDExt, len(m.PrevRefs))
		copy(cloned.PrevRefs, m.PrevRefs)
	}
	if len(m.NextRefs) > 0 {
		cloned.NextRefs = make([]ton.BlockIDExt, len(m.NextRefs))
		copy(cloned.NextRefs, m.NextRefs)
	}
	return cloned
}

func (m *BlockMeta) Has(flag BlockMetaFlags) bool {
	return m.Flags&flag != 0
}

func (m *BlockMeta) Mark(flag BlockMetaFlags) {
	m.Flags |= flag
}

func (m *BlockMeta) MasterchainRefKnown() bool {
	if m == nil || (m.ID.Workchain == masterchainID && m.ID.Shard == masterchainShard) {
		return false
	}
	return m.MasterchainRefSeqno != 0 || m.ID.SeqNo == 0
}

func (m *BlockMeta) HasProof(kind ServedProofKind) bool {
	switch kind {
	case ServedProofBlock:
		return m.Has(BlockMetaHasProofBlock)
	case ServedProofBlockLink:
		return m.Has(BlockMetaHasProofBlockLink)
	case ServedProofKeyBlock:
		return m.Has(BlockMetaHasProofKeyBlock)
	case ServedProofKeyBlockLink:
		return m.Has(BlockMetaHasProofKeyBlockLink)
	default:
		return false
	}
}

func (m *BlockMeta) proofCandidates() []ServedProofKind {
	if m.Has(BlockMetaServedFullIsLink) {
		return []ServedProofKind{ServedProofBlockLink, ServedProofKeyBlockLink, ServedProofBlock, ServedProofKeyBlock}
	}
	return []ServedProofKind{ServedProofBlock, ServedProofKeyBlock, ServedProofBlockLink, ServedProofKeyBlockLink}
}

func ProofCandidates(meta *BlockMeta) []ServedProofKind {
	if meta == nil {
		return nil
	}
	return meta.proofCandidates()
}

func BlockMetaFlagForProof(kind ServedProofKind) BlockMetaFlags {
	switch kind {
	case ServedProofBlock:
		return BlockMetaHasProofBlock
	case ServedProofBlockLink:
		return BlockMetaHasProofBlockLink
	case ServedProofKeyBlock:
		return BlockMetaHasProofKeyBlock
	case ServedProofKeyBlockLink:
		return BlockMetaHasProofKeyBlockLink
	default:
		return 0
	}
}

func MergeBlockMeta(base *BlockMeta, next *BlockMeta) *BlockMeta {
	if base == nil {
		return next.Clone()
	}
	if next == nil {
		return base.Clone()
	}

	merged := base.Clone()
	merged.Flags |= next.Flags
	if next.GenUTime != 0 {
		merged.GenUTime = next.GenUTime
	}
	if next.StartLT != 0 {
		merged.StartLT = next.StartLT
	}
	if next.EndLT != 0 {
		merged.EndLT = next.EndLT
	}
	if len(next.StateRootHash) > 0 {
		merged.StateRootHash = bytes.Clone(next.StateRootHash)
	}
	if len(next.StateFileHash) > 0 {
		merged.StateFileHash = bytes.Clone(next.StateFileHash)
	}
	if next.MasterchainRefKnown() {
		merged.MasterchainRefSeqno = next.MasterchainRefSeqno
	}
	if len(next.PrevRefs) > 0 {
		merged.PrevRefs = make([]ton.BlockIDExt, len(next.PrevRefs))
		copy(merged.PrevRefs, next.PrevRefs)
	}
	if len(next.NextRefs) > 0 {
		merged.NextRefs = make([]ton.BlockIDExt, len(next.NextRefs))
		copy(merged.NextRefs, next.NextRefs)
	}
	return merged
}

func MergeBlockMetaFromBlockData(meta *BlockMeta, block ton.BlockIDExt, data []byte) (*BlockMeta, error) {
	if len(data) == 0 {
		return meta, nil
	}
	parsed, err := BuildBlockMetaFromBlockData(block, data)
	if err != nil {
		return nil, err
	}
	return MergeBlockMeta(meta, parsed), nil
}

func SortedShardKeys(shards map[ShardKey]BlockState) []ShardKey {
	keys := make([]ShardKey, 0, len(shards))
	for key := range shards {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Workchain != keys[j].Workchain {
			return keys[i].Workchain < keys[j].Workchain
		}
		return keys[i].Shard < keys[j].Shard
	})
	return keys
}

func BuildBlockMetaFromBlockData(id ton.BlockIDExt, data []byte) (*BlockMeta, error) {
	root, err := cell.FromBOC(data)
	if err != nil {
		return nil, fmt.Errorf("parse block boc: %w", err)
	}

	return BuildBlockMetaFromBlockCell(id, root)
}

func BuildBlockMetaFromBlockCell(id ton.BlockIDExt, root *cell.Cell) (*BlockMeta, error) {
	block, err := ParseVerifiedBlockCell(id, root)
	if err != nil {
		return nil, err
	}

	return BuildBlockMetaFromParsedBlock(id, block)
}

func ParseVerifiedBlockCell(id ton.BlockIDExt, root *cell.Cell) (*tlb.Block, error) {
	loader, err := root.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("begin parse block %s: %w", FormatBlockRef(id), err)
	}

	var block tlb.Block
	if err := tlb.LoadFromCell(&block, loader); err != nil {
		return nil, fmt.Errorf("load tlb block %s: %w", FormatBlockRef(id), err)
	}
	if err := VerifyBlockIdentity(id, &block); err != nil {
		return nil, err
	}
	return &block, nil
}

func BlockTransactionCountFromBlockData(id ton.BlockIDExt, data []byte) (uint32, error) {
	root, err := cell.FromBOC(data)
	if err != nil {
		return 0, fmt.Errorf("parse block boc: %w", err)
	}

	return BlockTransactionCountFromBlockCell(id, root)
}

func BlockTransactionCountFromBlockCell(id ton.BlockIDExt, root *cell.Cell) (uint32, error) {
	block, err := ParseVerifiedBlockCell(id, root)
	if err != nil {
		return 0, err
	}

	return BlockTransactionCountFromParsed(id, block)
}

func BlockTransactionCountFromParsed(id ton.BlockIDExt, block *tlb.Block) (uint32, error) {
	if block == nil || block.Extra == nil || block.Extra.ShardAccountBlocks == nil {
		return 0, fmt.Errorf("block %s does not contain shard account blocks", FormatBlockRef(id))
	}

	txCount, err := countBlockTransactions(block.Extra.ShardAccountBlocks)
	if err != nil {
		return 0, fmt.Errorf("count transactions in %s: %w", FormatBlockRef(id), err)
	}

	return txCount, nil
}

func countBlockTransactions(root *cell.Cell) (uint32, error) {
	var shardAccounts tlb.ShardAccountBlocks
	loader, err := root.BeginParse()
	if err != nil {
		return 0, fmt.Errorf("load shard account blocks: %w", err)
	}
	if err := tlb.LoadFromCell(&shardAccounts, loader); err != nil {
		return 0, fmt.Errorf("load shard account blocks: %w", err)
	}
	if shardAccounts.Accounts == nil {
		return 0, nil
	}

	accounts, err := shardAccounts.Accounts.Range(false, false)
	if err != nil {
		return 0, fmt.Errorf("load shard account dictionary: %w", err)
	}

	var total uint64
	for _, kv := range accounts {
		if err := tlb.LoadFromCell(new(tlb.CurrencyCollection), kv.Value); err != nil {
			return 0, fmt.Errorf("load account currency collection: %w", err)
		}

		var accountBlock tlb.AccountBlock
		if err := tlb.LoadFromCell(&accountBlock, kv.Value); err != nil {
			return 0, fmt.Errorf("load account block: %w", err)
		}
		if accountBlock.Transactions == nil {
			continue
		}

		txs, err := accountBlock.Transactions.Range(false, false)
		if err != nil {
			return 0, fmt.Errorf("load account transactions: %w", err)
		}
		total += uint64(len(txs))
		if total > uint64(^uint32(0)) {
			return 0, fmt.Errorf("transaction count overflows uint32")
		}
	}

	return uint32(total), nil
}

func VerifyBlockIdentity(id ton.BlockIDExt, block *tlb.Block) error {
	workchain, shard := tlb.ConvertShardIdentToShard(block.BlockInfo.Shard)
	if block.BlockInfo.SeqNo != id.SeqNo || workchain != id.Workchain || shard != uint64(id.Shard) {
		return fmt.Errorf(
			"block identity mismatch: expected %s, got wc=%d shard=%016x seqno=%d",
			FormatBlockRef(id),
			workchain,
			shard,
			block.BlockInfo.SeqNo,
		)
	}

	return nil
}

func BuildBlockMetaFromParsedBlock(id ton.BlockIDExt, block *tlb.Block) (*BlockMeta, error) {
	if err := ValidateBlockIDHashes(id); err != nil {
		return nil, err
	}

	meta := &BlockMeta{
		ID:       id,
		Flags:    BlockMetaHasBlockData,
		GenUTime: block.BlockInfo.GenUtime,
		StartLT:  block.BlockInfo.StartLt,
		EndLT:    block.BlockInfo.EndLt,
	}
	if block.BlockInfo.KeyBlock {
		meta.Mark(BlockMetaIsKeyBlock)
	}
	if block.StateUpdate != nil {
		stateUpdate, err := block.StateUpdate.BeginParse()
		if err != nil {
			return nil, fmt.Errorf("load block state update: %w", err)
		}
		nextState, err := stateUpdate.PeekRefCellAt(1)
		if err != nil {
			return nil, fmt.Errorf("load block state update target: %w", err)
		}
		nextStateHash := nextState.HashKey(0)
		meta.StateRootHash = nextStateHash[:]
	}
	if block.BlockInfo.NotMaster {
		if block.BlockInfo.MasterRef == nil {
			return nil, fmt.Errorf("shard block %s has no masterchain ref", FormatBlockRef(id))
		}
	}

	prev := make([]ton.BlockIDExt, 0, 2)
	prevShard := id.Shard
	if block.BlockInfo.AfterSplit {
		prevShard = int64(tlb.ShardID(uint64(id.Shard)).GetParent())
	}
	if block.BlockInfo.AfterMerge {
		prevShard = int64(tlb.ShardID(uint64(id.Shard)).GetChild(true))
	}
	prev = append(prev, blockRefToBlockIDExt(id.Workchain, prevShard, block.BlockInfo.PrevRef.Prev1))
	if block.BlockInfo.PrevRef.Prev2 != nil {
		prev2Shard := id.Shard
		if block.BlockInfo.AfterMerge {
			prev2Shard = int64(tlb.ShardID(uint64(id.Shard)).GetChild(false))
		}
		prev = append(prev, blockRefToBlockIDExt(id.Workchain, prev2Shard, *block.BlockInfo.PrevRef.Prev2))
	}
	meta.PrevRefs = prev
	return meta, nil
}

func BuildBlockMetaFromState(state BlockState) (*BlockMeta, error) {
	if err := ValidateBlockIDHashes(state.Block); err != nil {
		return nil, err
	}

	meta := &BlockMeta{
		ID:            state.Block,
		Flags:         BlockMetaHasStateSnapshot | BlockMetaHasStateCells,
		StateRootHash: bytes.Clone(state.StateRootHash),
		StateFileHash: bytes.Clone(state.StateFileHash),
	}
	if state.Parsed != nil {
		meta.GenUTime = state.Parsed.GenUTime
	}
	if state.MasterchainRef != nil {
		meta.MasterchainRefSeqno = state.MasterchainRef.SeqNo
	}
	return meta, nil
}

func blockRefToBlockIDExt(workchain int32, shard int64, ref tlb.ExtBlkRef) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: workchain,
		Shard:     shard,
		SeqNo:     ref.SeqNo,
		RootHash:  bytes.Clone(ref.RootHash),
		FileHash:  bytes.Clone(ref.FileHash),
	}
}
