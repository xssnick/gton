package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/bits"
	"sort"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type Storage interface {
	PeerServingStorage
	PeerServingStorageWriter
	StateStorage
	BlockMetaStorage
	CellStorage
	Close() error
}

type BlockMetaStorage interface {
	SaveBlockMeta(meta *BlockMeta) error
	BlockMeta(ctx context.Context, block ton.BlockIDExt) (*BlockMeta, error)
	NextKeyBlocks(ctx context.Context, after uint32, limit int) ([]ton.BlockIDExt, error)
	LookupBlockBySeqNo(ctx context.Context, key BlockHistoryKey, seqno uint32) (ton.BlockIDExt, error)
	LookupBlockByLT(ctx context.Context, key BlockHistoryKey, lt uint64) (ton.BlockIDExt, error)
	LookupBlockByUnixTime(ctx context.Context, key BlockHistoryKey, utime uint32) (ton.BlockIDExt, error)
}

type CellStorage interface {
	SaveCells(records []*CellRecord) error
	CellRecord(ctx context.Context, hash []byte) (*CellRecord, error)
	LoadCell(ctx context.Context, hash []byte) (*cell.Cell, error)
	LazyCellLoader() cell.LazyCellLoader
}

type BlockHistoryKey struct {
	Workchain int32
	Shard     int64
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
)

const masterchainShard = int64(-1 << 63)

type BlockMeta struct {
	ID            ton.BlockIDExt
	Flags         BlockMetaFlags
	GenUTime      uint32
	StartLT       uint64
	EndLT         uint64
	StateRootHash []byte
	StateFileHash []byte
	// MasterchainRef matches C++ BlockHandle::masterchain_ref_block:
	// the masterchain block that first included this shard block.
	MasterchainRef *ton.BlockIDExt
	PrevRefs       []ton.BlockIDExt
}

func (m *BlockMeta) Clone() *BlockMeta {
	if m == nil {
		return nil
	}

	cloned := &BlockMeta{
		ID:            m.ID,
		Flags:         m.Flags,
		GenUTime:      m.GenUTime,
		StartLT:       m.StartLT,
		EndLT:         m.EndLT,
		StateRootHash: bytes.Clone(m.StateRootHash),
		StateFileHash: bytes.Clone(m.StateFileHash),
	}
	if m.MasterchainRef != nil {
		ref := *m.MasterchainRef
		cloned.MasterchainRef = &ref
	}
	if len(m.PrevRefs) > 0 {
		cloned.PrevRefs = make([]ton.BlockIDExt, len(m.PrevRefs))
		copy(cloned.PrevRefs, m.PrevRefs)
	}
	return cloned
}

func (m *BlockMeta) Has(flag BlockMetaFlags) bool {
	return m.Flags&flag != 0
}

func (m *BlockMeta) Mark(flag BlockMetaFlags) {
	m.Flags |= flag
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

type CellRecord struct {
	Hash []byte
	D1   byte
	D2   byte
	Data []byte
	Refs []CellRefRecord
}

type EncodedCellRecord struct {
	Hash cell.Hash
	Data []byte
}

type CellRefRecord struct {
	LevelMask byte
	Hashes    []byte
	Depths    []byte
}

const (
	encodedCellRecordCompactRefsFlag = 0x10
	encodedCellRecordHashSize        = 32
	encodedCellRecordDepthSize       = 2
)

func (r *CellRecord) Clone() *CellRecord {
	if r == nil {
		return nil
	}
	cloned := &CellRecord{
		Hash: bytes.Clone(r.Hash),
		D1:   r.D1,
		D2:   r.D2,
		Data: bytes.Clone(r.Data),
		Refs: make([]CellRefRecord, len(r.Refs)),
	}
	for i := range r.Refs {
		cloned.Refs[i] = CellRefRecord{
			LevelMask: r.Refs[i].LevelMask,
			Hashes:    bytes.Clone(r.Refs[i].Hashes),
			Depths:    bytes.Clone(r.Refs[i].Depths),
		}
	}
	return cloned
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
	if merged.MasterchainRef == nil && next.MasterchainRef != nil {
		ref := *next.MasterchainRef
		merged.MasterchainRef = &ref
	}
	if len(next.PrevRefs) > 0 {
		merged.PrevRefs = make([]ton.BlockIDExt, len(next.PrevRefs))
		copy(merged.PrevRefs, next.PrevRefs)
	}
	return merged
}

func MergeBlockMetaFromBlockData(meta *BlockMeta, block ton.BlockIDExt, data []byte) *BlockMeta {
	if len(data) == 0 {
		return meta
	}
	parsed, err := BuildBlockMetaFromBlockData(block, data)
	if err != nil {
		return meta
	}
	return MergeBlockMeta(meta, parsed)
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
	if root == nil {
		return nil, fmt.Errorf("block root is nil for %s", FormatBlockRef(id))
	}

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

func VerifyBlockIdentity(id ton.BlockIDExt, block *tlb.Block) error {
	if block == nil {
		return fmt.Errorf("block is nil for %s", FormatBlockRef(id))
	}

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
		ref := blockRefToBlockIDExt(-1, masterchainShard, *block.BlockInfo.MasterRef)
		meta.MasterchainRef = &ref
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

func BuildBlockMetaFromState(state BlockState) *BlockMeta {
	meta := &BlockMeta{
		ID:            state.Block,
		Flags:         BlockMetaHasStateSnapshot,
		StateRootHash: bytes.Clone(state.StateRootHash),
		StateFileHash: bytes.Clone(state.StateFileHash),
	}
	if state.Parsed != nil {
		meta.GenUTime = state.Parsed.GenUTime
	}
	if state.MasterchainRef != nil {
		ref := *state.MasterchainRef
		meta.MasterchainRef = &ref
	}
	return meta
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

func CellRecordFromCell(cl *cell.Cell) (*CellRecord, error) {
	if cl == nil {
		return nil, fmt.Errorf("cell is nil")
	}
	if cl.IsLazy() {
		loader, err := cl.BeginParse()
		if err != nil {
			return nil, err
		}
		cl = loader.BaseCell()
	}

	cellBits := cl.BitsSize()
	if cellBits > 1023 {
		return nil, fmt.Errorf("cell bits length is too large: %d", cellBits)
	}
	refsCount := int(cl.RefsNum())
	d1, d2 := cellRecordDescriptors(cl, refsCount, cellBits)
	data := cellRecordBody(cl, cellBits, int((cellBits+7)/8))
	hash := cl.HashKey()

	refs := make([]CellRefRecord, 0, refsCount)
	for i := 0; i < refsCount; i++ {
		ref, err := cl.PeekRef(i)
		if err != nil {
			return nil, err
		}
		refs = append(refs, cellRefRecordFromCell(ref))
	}

	return &CellRecord{
		Hash: bytes.Clone(hash[:]),
		D1:   d1,
		D2:   d2,
		Data: data,
		Refs: refs,
	}, nil
}

func CellRecordFromCellMetadata(cl *cell.Cell, meta cell.Metadata) (*CellRecord, error) {
	if cl == nil {
		return nil, fmt.Errorf("cell is nil")
	}
	if cl.IsLazy() {
		loader, err := cl.BeginParse()
		if err != nil {
			return nil, err
		}
		cl = loader.BaseCell()
		meta = cl.GetMetadata()
	}

	cellBits := cl.BitsSize()
	if cellBits > 1023 {
		return nil, fmt.Errorf("cell bits length is too large: %d", cellBits)
	}
	if len(meta.Refs) > 4 {
		return nil, fmt.Errorf("cell refs count is too large: %d", len(meta.Refs))
	}

	d1, d2 := cellRecordDescriptorsForLevelMask(cl, meta.LevelMask, len(meta.Refs), cellBits)
	data := cellRecordBody(cl, cellBits, int((cellBits+7)/8))

	refs := make([]CellRefRecord, len(meta.Refs))
	for i, ref := range meta.Refs {
		record, err := cellRefRecordFromMetadata(ref)
		if err != nil {
			return nil, fmt.Errorf("ref %d: %w", i, err)
		}
		refs[i] = record
	}

	return &CellRecord{
		Hash: bytes.Clone(meta.Hash[:]),
		D1:   d1,
		D2:   d2,
		Data: data,
		Refs: refs,
	}, nil
}

func PrepareEncodedCellRecordFromCellMetadata(cl *cell.Cell, meta cell.Metadata) (EncodedCellRecord, error) {
	if cl == nil {
		return EncodedCellRecord{}, fmt.Errorf("cell is nil")
	}
	if cl.IsLazy() {
		loader, err := cl.BeginParse()
		if err != nil {
			return EncodedCellRecord{}, err
		}
		cl = loader.BaseCell()
		meta = cl.GetMetadata()
	}

	cellBits := cl.BitsSize()
	if cellBits > 1023 {
		return EncodedCellRecord{}, fmt.Errorf("cell bits length is too large: %d", cellBits)
	}
	if len(meta.Refs) > 4 {
		return EncodedCellRecord{}, fmt.Errorf("cell refs count is too large: %d", len(meta.Refs))
	}

	d1, d2 := cellRecordDescriptorsForLevelMask(cl, meta.LevelMask, len(meta.Refs), cellBits)
	size, err := encodedCellRecordLenFromMetadata(d2, meta.Refs)
	if err != nil {
		return EncodedCellRecord{}, err
	}

	encoded := make([]byte, size)
	encodeCellRecordMetadataTo(encoded, cl, meta.Refs, d1, d2)
	return EncodedCellRecord{Hash: meta.Hash, Data: encoded}, nil
}

func PrepareStateUpdateCells(update *cell.Cell) (map[cell.Hash][]byte, error) {
	if err := cell.ValidateMerkleUpdate(update); err != nil {
		return nil, err
	}

	updateTo, err := merkleUpdateTarget(update)
	if err != nil {
		return nil, err
	}
	return prepareReachableStateUpdateCells(updateTo)
}

func ValidateStateUpdateCells(update *cell.Cell, records map[cell.Hash][]byte) error {
	if records == nil {
		return fmt.Errorf("prepared state update cells are missing")
	}

	expected, err := PrepareStateUpdateCells(update)
	if err != nil {
		return err
	}
	if len(records) != len(expected) {
		return fmt.Errorf("prepared state update cells count mismatch: got=%d want=%d", len(records), len(expected))
	}

	for hash, want := range expected {
		got := records[hash]
		if len(got) == 0 {
			return fmt.Errorf("prepared state update cells are missing %x", hash[:])
		}
		if !bytes.Equal(got, want) {
			return fmt.Errorf("prepared state update cell %x payload mismatch", hash[:])
		}
	}
	for hash := range records {
		if _, ok := expected[hash]; !ok {
			return fmt.Errorf("prepared state update cells contain unexpected %x", hash[:])
		}
	}
	return nil
}

func prepareReachableStateUpdateCells(root *cell.Cell) (map[cell.Hash][]byte, error) {
	if root == nil {
		return nil, nil
	}

	records := map[cell.Hash][]byte{}
	err := walkReachableStateUpdateCells(root.Virtualize(0), func(current *cell.Cell, meta cell.Metadata) error {
		record, err := prepareReachableStateUpdateCellRecord(current, meta)
		if err != nil {
			return fmt.Errorf("build reachable state update cell record %x: %w", meta.Hash[:], err)
		}
		if record.Hash != meta.Hash {
			return fmt.Errorf("reachable state update cell hash mismatch: got=%x want=%x", record.Hash[:], meta.Hash[:])
		}
		records[meta.Hash] = record.Data
		return nil
	})
	if err != nil {
		return nil, err
	}
	return records, nil
}

func prepareReachableStateUpdateCellRecord(cl *cell.Cell, meta cell.Metadata) (EncodedCellRecord, error) {
	if cl.GetType() != cell.PrunedCellType {
		return PrepareEncodedCellRecordFromCellMetadata(cl, meta)
	}

	pruned, err := materializePrunedStateCell(cl)
	if err != nil {
		return EncodedCellRecord{}, err
	}
	return PrepareEncodedCellRecordFromCellMetadata(pruned, meta)
}

func materializePrunedStateCell(cl *cell.Cell) (*cell.Cell, error) {
	if cl == nil || cl.GetType() != cell.PrunedCellType || !cl.IsVirtualized() {
		return cl, nil
	}

	loader, err := cl.BeginParse()
	if err != nil {
		return nil, err
	}
	bits, data, err := loader.RestBits()
	if err != nil {
		return nil, err
	}

	builder := cell.BeginCell()
	if err = builder.StoreSlice(data, bits); err != nil {
		return nil, err
	}
	raw, err := builder.EndCellSpecial(true)
	if err != nil {
		return nil, err
	}
	return raw, nil
}

func walkReachableStateUpdateCells(root *cell.Cell, visit func(*cell.Cell, cell.Metadata) error) error {
	stack := []*cell.Cell{root}
	seen := map[cell.Hash]struct{}{}
	for len(stack) > 0 {
		current := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if current == nil {
			continue
		}
		if current.IsLazy() {
			loader, err := current.BeginParse()
			if err != nil {
				return fmt.Errorf("load reachable state update cell %x: %w", current.Hash(), err)
			}
			current = loader.BaseCell()
		}

		if current.GetType() == cell.PrunedCellType && current.ActualLevel() == current.EffectiveLevel()+1 {
			continue
		}

		meta := current.GetMetadata()
		hash := meta.Hash
		if _, ok := seen[hash]; ok {
			continue
		}
		seen[hash] = struct{}{}

		if err := visit(current, meta); err != nil {
			return err
		}
		if current.GetType() == cell.PrunedCellType {
			continue
		}

		for i, ref := range meta.Refs {
			if ref.Lazy {
				return fmt.Errorf("reachable state update ref %d from %x is lazy", i, hash[:])
			}
			refCell, err := current.PeekRef(i)
			if err != nil {
				return fmt.Errorf("load reachable state update ref %d from %x: %w", i, hash[:], err)
			}
			stack = append(stack, refCell)
		}
	}
	return nil
}

func PrepareReachableStateCells(root *cell.Cell) (map[cell.Hash][]byte, error) {
	if root == nil {
		return nil, nil
	}

	records := map[cell.Hash][]byte{}
	err := WalkReachableStateCells(root, func(current *cell.Cell, meta cell.Metadata) error {
		record, err := PrepareEncodedCellRecordFromCellMetadata(current, meta)
		if err != nil {
			return fmt.Errorf("build reachable state cell record %x: %w", meta.Hash[:], err)
		}
		if record.Hash != meta.Hash {
			return fmt.Errorf("reachable state cell hash mismatch: got=%x want=%x", record.Hash[:], meta.Hash[:])
		}
		records[meta.Hash] = record.Data
		return nil
	})
	if err != nil {
		return nil, err
	}
	return records, nil
}

func WalkReachableStateCells(root *cell.Cell, visit func(*cell.Cell, cell.Metadata) error) error {
	stack := []*cell.Cell{root}
	seen := map[cell.Hash]struct{}{}
	for len(stack) > 0 {
		current := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if current == nil {
			continue
		}
		if current.IsLazy() {
			continue
		}

		meta, refCells, err := reachableStateCellMetadata(current)
		if err != nil {
			return err
		}
		hash := meta.Hash
		if _, ok := seen[hash]; ok {
			continue
		}
		seen[hash] = struct{}{}

		if err := visit(current, meta); err != nil {
			return err
		}

		for i, ref := range meta.Refs {
			if ref.Lazy {
				continue
			}
			refCell := refCells[i]
			if refCell == nil || refCell.IsLazy() {
				return fmt.Errorf("reachable state ref %d from %x has no body", i, hash[:])
			}
			stack = append(stack, refCell)
		}
	}
	return nil
}

func reachableStateCellMetadata(cl *cell.Cell) (cell.Metadata, [4]*cell.Cell, error) {
	meta := cl.GetMetadata()
	if len(meta.Refs) > 4 {
		return cell.Metadata{}, [4]*cell.Cell{}, fmt.Errorf("cell refs count is too large: %d", len(meta.Refs))
	}

	refs := make([]cell.RefMetadata, len(meta.Refs))
	var refCells [4]*cell.Cell
	for i, metaRef := range meta.Refs {
		if metaRef.Lazy {
			refs[i] = metaRef
			continue
		}

		refCell, err := cl.PeekRef(i)
		if err != nil {
			return cell.Metadata{}, [4]*cell.Cell{}, fmt.Errorf("load reachable state ref %d from %x: %w", i, meta.Hash[:], err)
		}
		refMeta := refCell.GetMetadata()
		refs[i] = cell.RefMetadata{
			Hash:      refMeta.Hash,
			LevelMask: refMeta.LevelMask,
			Hashes:    refMeta.Hashes,
			Depths:    refMeta.Depths,
			Lazy:      refCell.IsLazy(),
		}
		refCells[i] = refCell
	}
	meta.Refs = refs
	return meta, refCells, nil
}

func merkleUpdateTarget(update *cell.Cell) (*cell.Cell, error) {
	if update == nil {
		return nil, fmt.Errorf("merkle update cell is nil")
	}
	loader, err := update.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("load merkle update cell: %w", err)
	}
	update = loader.BaseCell()
	if update.Level() != 0 {
		return nil, fmt.Errorf("merkle update has non-zero level")
	}
	if update.GetType() != cell.MerkleUpdateCellType {
		return nil, fmt.Errorf("not a MerkleUpdate cell")
	}
	if loader.RefsNum() != 2 {
		return nil, fmt.Errorf("wrong references count for a merkle update special cell")
	}

	updateTo, err := loader.PeekRefCellAt(1)
	if err != nil {
		return nil, fmt.Errorf("failed to load merkle update second ref: %w", err)
	}
	return updateTo, nil
}

func DecodeCellRecordTrusted(hash []byte, data []byte) *CellRecord {
	pos := 0
	storedD1 := data[pos]
	compactRefs := storedD1&encodedCellRecordCompactRefsFlag != 0
	record := &CellRecord{
		Hash: hash,
		D1:   storedD1 &^ encodedCellRecordCompactRefsFlag,
		D2:   data[pos+1],
	}
	pos += 2

	dataLen := int(record.D2/2 + record.D2%2)
	record.Data = data[pos : pos+dataLen]
	pos += dataLen

	refsCount := int(record.D1 & 7)
	record.Refs = make([]CellRefRecord, refsCount)
	var slowRefs byte
	if compactRefs && refsCount > 0 {
		slowRefs = data[pos]
		pos++
	}
	for i := 0; i < refsCount; i++ {
		if compactRefs && slowRefs&(1<<uint(i)) == 0 {
			hashes := data[pos : pos+encodedCellRecordHashSize]
			pos += encodedCellRecordHashSize
			depths := data[pos : pos+encodedCellRecordDepthSize]
			pos += encodedCellRecordDepthSize

			record.Refs[i] = CellRefRecord{
				LevelMask: 0,
				Hashes:    hashes,
				Depths:    depths,
			}
			continue
		}

		levelMask := data[pos]
		pos++

		hashesCount := CellRefHashesCount(levelMask)
		hashesLen := hashesCount * encodedCellRecordHashSize
		depthsLen := hashesCount * encodedCellRecordDepthSize
		hashes := data[pos : pos+hashesLen]
		pos += hashesLen
		depths := data[pos : pos+depthsLen]
		pos += depthsLen

		record.Refs[i] = CellRefRecord{
			LevelMask: levelMask,
			Hashes:    hashes,
			Depths:    depths,
		}
	}
	return record
}

func cellRecordDescriptors(cl *cell.Cell, refsCount int, bitLen uint) (byte, byte) {
	return cellRecordDescriptorsForLevelMask(cl, cl.LevelMask(), refsCount, bitLen)
}

func cellRecordDescriptorsForLevelMask(cl *cell.Cell, levelMask cell.LevelMask, refsCount int, bitLen uint) (byte, byte) {
	refs := byte(refsCount)
	if cl.IsSpecial() {
		refs += 8
	}
	d1 := refs + levelMask.Mask*32

	d2 := byte((bitLen / 8) * 2)
	if bitLen%8 != 0 {
		d2++
	}
	return d1, d2
}

func cellRecordBody(cl *cell.Cell, cellBits uint, bodyLen int) []byte {
	if bodyLen == 0 {
		return nil
	}
	data := cl.MustBeginParse().MustLoadSlice(cellBits)
	body := make([]byte, bodyLen)
	copy(body, data)
	if tailBits := cellBits % 8; tailBits != 0 {
		body[bodyLen-1] &= 0xff << (8 - tailBits)
		body[bodyLen-1] |= 1 << (7 - tailBits)
	}
	return body
}

func encodedCellRecordLenFromMetadata(d2 byte, refs []cell.RefMetadata) (int, error) {
	size := 2 + int(d2/2+d2%2)
	slowRefs, compactRefs, err := encodedCellRecordCompactMetadataRefLayout(refs)
	if err != nil {
		return 0, err
	}
	if compactRefs {
		size++
	}
	for i, ref := range refs {
		hashesCount := CellRefHashesCount(ref.LevelMask.Mask)
		if compactRefs && slowRefs&(1<<uint(i)) == 0 {
			size += encodedCellRecordHashSize + encodedCellRecordDepthSize
			continue
		}
		size += 1 + hashesCount*(encodedCellRecordHashSize+encodedCellRecordDepthSize)
	}
	return size, nil
}

func encodeCellRecordMetadataTo(buf []byte, cl *cell.Cell, refs []cell.RefMetadata, d1 byte, d2 byte) {
	slowRefs, compactRefs, _ := encodedCellRecordCompactMetadataRefLayout(refs)
	pos := 0
	if compactRefs {
		d1 |= encodedCellRecordCompactRefsFlag
	}
	buf[pos] = d1
	buf[pos+1] = d2
	pos += 2

	pos += cl.SerializeBOCBodyTo(buf[pos:])
	if compactRefs {
		buf[pos] = slowRefs
		pos++
	}
	for i, ref := range refs {
		if compactRefs && slowRefs&(1<<uint(i)) == 0 {
			copy(buf[pos:pos+encodedCellRecordHashSize], ref.Hashes[0][:])
			pos += encodedCellRecordHashSize
			binary.BigEndian.PutUint16(buf[pos:pos+encodedCellRecordDepthSize], ref.Depths[0])
			pos += encodedCellRecordDepthSize
			continue
		}

		buf[pos] = ref.LevelMask.Mask
		pos++
		for _, hash := range ref.Hashes {
			copy(buf[pos:pos+encodedCellRecordHashSize], hash[:])
			pos += encodedCellRecordHashSize
		}
		for _, depth := range ref.Depths {
			binary.BigEndian.PutUint16(buf[pos:pos+encodedCellRecordDepthSize], depth)
			pos += encodedCellRecordDepthSize
		}
	}
}

func encodedCellRecordCompactMetadataRefLayout(refs []cell.RefMetadata) (byte, bool, error) {
	if len(refs) == 0 {
		return 0, false, nil
	}

	refsSize := 0
	compactRefsSize := 1
	hasCommonRef := false
	var slowRefs byte
	for i, ref := range refs {
		hashesCount := CellRefHashesCount(ref.LevelMask.Mask)
		if len(ref.Hashes) != hashesCount || len(ref.Depths) != hashesCount {
			return 0, false, fmt.Errorf("invalid ref metadata for %x: hashes=%d depths=%d want=%d", ref.Hash[:], len(ref.Hashes), len(ref.Depths), hashesCount)
		}
		if hashesCount > 0 && ref.Hashes[hashesCount-1] != ref.Hash {
			return 0, false, fmt.Errorf("top hash mismatch: got=%x want=%x", ref.Hashes[hashesCount-1][:], ref.Hash[:])
		}

		refSize := 1 + hashesCount*(encodedCellRecordHashSize+encodedCellRecordDepthSize)
		refsSize += refSize
		if ref.LevelMask.Mask == 0 && len(ref.Hashes) == 1 && len(ref.Depths) == 1 {
			hasCommonRef = true
			compactRefsSize += encodedCellRecordHashSize + encodedCellRecordDepthSize
			continue
		}
		slowRefs |= 1 << uint(i)
		compactRefsSize += refSize
	}
	return slowRefs, hasCommonRef && compactRefsSize <= refsSize, nil
}

func cellRefRecordFromCell(ref *cell.Cell) CellRefRecord {
	levelMask := ref.LevelMask()
	mask := levelMask.Mask
	hashesCount := CellRefHashesCount(mask)
	hashes := make([]byte, hashesCount*32)
	depths := make([]byte, hashesCount*2)

	posHash := 0
	posDepth := 0
	for level := 0; level <= levelMask.GetLevel(); level++ {
		if !levelMask.IsSignificant(level) {
			continue
		}
		hash := ref.HashKey(level)
		copy(hashes[posHash:posHash+32], hash[:])
		posHash += 32
		binary.BigEndian.PutUint16(depths[posDepth:posDepth+2], ref.Depth(level))
		posDepth += 2
	}
	return CellRefRecord{
		LevelMask: mask,
		Hashes:    hashes,
		Depths:    depths,
	}
}

func cellRefRecordFromMetadata(ref cell.RefMetadata) (CellRefRecord, error) {
	count := CellRefHashesCount(ref.LevelMask.Mask)
	if len(ref.Hashes) != count {
		return CellRefRecord{}, fmt.Errorf("invalid hashes count: got=%d want=%d", len(ref.Hashes), count)
	}
	if len(ref.Depths) != count {
		return CellRefRecord{}, fmt.Errorf("invalid depths count: got=%d want=%d", len(ref.Depths), count)
	}
	if count > 0 && ref.Hashes[count-1] != ref.Hash {
		return CellRefRecord{}, fmt.Errorf("top hash mismatch: got=%x want=%x", ref.Hashes[count-1][:], ref.Hash[:])
	}

	hashes := make([]byte, count*32)
	depths := make([]byte, count*2)
	for i := 0; i < count; i++ {
		copy(hashes[i*32:(i+1)*32], ref.Hashes[i][:])
		binary.BigEndian.PutUint16(depths[i*2:(i+1)*2], ref.Depths[i])
	}
	return CellRefRecord{
		LevelMask: ref.LevelMask.Mask,
		Hashes:    hashes,
		Depths:    depths,
	}, nil
}

func CellRefHashesCount(levelMask byte) int {
	return bits.OnesCount8(levelMask) + 1
}

func CellRefHash(ref CellRefRecord) ([]byte, error) {
	count := CellRefHashesCount(ref.LevelMask)
	if len(ref.Hashes) != count*32 {
		return nil, fmt.Errorf("invalid ref hashes size: got=%d want=%d", len(ref.Hashes), count*32)
	}
	if len(ref.Depths) != count*2 {
		return nil, fmt.Errorf("invalid ref depths size: got=%d want=%d", len(ref.Depths), count*2)
	}
	return ref.Hashes[len(ref.Hashes)-32:], nil
}

func cellRecordBits(record *CellRecord) (uint, error) {
	bodyLen := int(record.D2/2 + record.D2%2)
	if len(record.Data) != bodyLen {
		return 0, fmt.Errorf("cell body size mismatch: got=%d want=%d", len(record.Data), bodyLen)
	}
	if record.D2%2 == 0 {
		return uint(bodyLen * 8), nil
	}
	if bodyLen == 0 {
		return 0, fmt.Errorf("invalid partial cell body size")
	}

	last := record.Data[bodyLen-1]
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
