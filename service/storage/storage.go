package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/bits"
	"sort"
	"time"

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
	LookupBlockBySeqNo(ctx context.Context, key BlockHistoryKey, seqno uint32) (ton.BlockIDExt, error)
	LookupBlockByLT(ctx context.Context, key BlockHistoryKey, lt uint64) (ton.BlockIDExt, error)
	LookupBlockByUnixTime(ctx context.Context, key BlockHistoryKey, utime uint32) (ton.BlockIDExt, error)
}

type CellStorage interface {
	SaveCells(records []*CellRecord) error
	CellRecord(ctx context.Context, hash []byte) (*CellRecord, error)
	LoadCell(ctx context.Context, hash []byte) (*cell.Cell, error)
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

type BlockMeta struct {
	ID             ton.BlockIDExt
	Flags          BlockMetaFlags
	GenUTime       uint32
	StartLT        uint64
	EndLT          uint64
	StateRootHash  []byte
	StateFileHash  []byte
	MasterchainRef *ton.BlockIDExt
	PrevRefs       []ton.BlockIDExt
	UpdatedAt      time.Time
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
		UpdatedAt:     m.UpdatedAt,
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

type CellRefRecord struct {
	LevelMask byte
	Hashes    []byte
	Depths    []byte
}

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
	if next.MasterchainRef != nil {
		ref := *next.MasterchainRef
		merged.MasterchainRef = &ref
	}
	if len(next.PrevRefs) > 0 {
		merged.PrevRefs = make([]ton.BlockIDExt, len(next.PrevRefs))
		copy(merged.PrevRefs, next.PrevRefs)
	}
	if !next.UpdatedAt.IsZero() {
		merged.UpdatedAt = next.UpdatedAt
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

	var block tlb.Block
	if err := tlb.LoadFromCell(&block, root.BeginParse()); err != nil {
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
		ID:        id,
		Flags:     BlockMetaHasBlockData,
		GenUTime:  block.BlockInfo.GenUtime,
		StartLT:   block.BlockInfo.StartLt,
		EndLT:     block.BlockInfo.EndLt,
		UpdatedAt: time.Now(),
	}
	if block.BlockInfo.KeyBlock {
		meta.Mark(BlockMetaIsKeyBlock)
	}
	if block.StateUpdate != nil {
		nextState, err := block.StateUpdate.PeekRef(1)
		if err != nil {
			return nil, fmt.Errorf("load block state update target: %w", err)
		}
		nextStateHash := nextState.HashKey(0)
		meta.StateRootHash = nextStateHash[:]
	}
	if block.BlockInfo.MasterRef != nil {
		meta.MasterchainRef = &ton.BlockIDExt{
			Workchain: -1,
			Shard:     topShard,
			SeqNo:     block.BlockInfo.MasterRef.SeqNo,
			RootHash:  bytes.Clone(block.BlockInfo.MasterRef.RootHash),
			FileHash:  bytes.Clone(block.BlockInfo.MasterRef.FileHash),
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

func BuildBlockMetaFromState(state BlockState) *BlockMeta {
	meta := &BlockMeta{
		ID:            state.Block,
		Flags:         BlockMetaHasStateSnapshot,
		StateRootHash: bytes.Clone(state.StateRootHash),
		StateFileHash: bytes.Clone(state.StateFileHash),
		UpdatedAt:     time.Now(),
	}
	if state.Parsed != nil {
		meta.GenUTime = state.Parsed.GenUTime
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

func cellRecordDescriptors(cl *cell.Cell, refsCount int, bitLen uint) (byte, byte) {
	refs := byte(refsCount)
	if cl.IsSpecial() {
		refs += 8
	}
	d1 := refs + cl.LevelMask().Mask*32

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
	data := cl.BeginParse().MustLoadSlice(cellBits)
	body := make([]byte, bodyLen)
	copy(body, data)
	if tailBits := cellBits % 8; tailBits != 0 {
		body[bodyLen-1] &= 0xff << (8 - tailBits)
		body[bodyLen-1] |= 1 << (7 - tailBits)
	}
	return body
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

func RebuildCellRecord(record *CellRecord, loadRef func(hash []byte) (*cell.Cell, error)) (*cell.Cell, error) {
	if record == nil {
		return nil, fmt.Errorf("cell record is nil")
	}

	builder := cell.BeginCell()
	cellBits, err := cellRecordBits(record)
	if err != nil {
		return nil, formatCellRebuildError(record, err)
	}
	if err := builder.StoreSlice(record.Data, cellBits); err != nil {
		return nil, err
	}
	for _, refRecord := range record.Refs {
		refHash, err := CellRefHash(refRecord)
		if err != nil {
			return nil, formatCellRebuildError(record, err)
		}
		ref, err := loadRef(refHash)
		if err != nil {
			return nil, err
		}
		if err = builder.StoreRef(ref); err != nil {
			return nil, err
		}
	}

	rebuilt, err := builder.EndCellSpecial(record.D1&8 != 0)
	if err != nil {
		return nil, formatCellRebuildError(record, err)
	}
	gotHash := rebuilt.HashKey()
	if !bytes.Equal(gotHash[:], record.Hash) {
		return nil, fmt.Errorf("rebuilt cell hash mismatch: got=%x want=%x", gotHash[:], record.Hash)
	}
	return rebuilt, nil
}

func formatCellRebuildError(record *CellRecord, err error) error {
	cellBits, _ := cellRecordBits(record)
	return fmt.Errorf(
		"rebuild cell %x special=%v bits=%d refs=%d prefix=%x: %w",
		record.Hash,
		record.D1&8 != 0,
		cellBits,
		len(record.Refs),
		recordDataPrefix(record.Data, 16),
		err,
	)
}

func recordDataPrefix(data []byte, max int) []byte {
	if len(data) <= max {
		return data
	}
	return data[:max]
}

type cellLoadFrame struct {
	hash      cell.Hash
	record    *CellRecord
	refs      [4]cell.Hash
	refsCount int
	refsReady bool
}

type cellGraphCache struct {
	index map[cell.Hash]uint32
	cells []*cell.Cell
}

func newCellGraphCache() cellGraphCache {
	return cellGraphCache{index: map[cell.Hash]uint32{}}
}

func (c *cellGraphCache) get(hash cell.Hash) *cell.Cell {
	idx, ok := c.index[hash]
	if !ok {
		return nil
	}
	return c.cells[idx]
}

func (c *cellGraphCache) set(hash cell.Hash, cl *cell.Cell) {
	c.index[hash] = uint32(len(c.cells))
	c.cells = append(c.cells, cl)
}

func (c *cellGraphCache) len() int {
	return len(c.cells)
}

type LoadCellGraphProgress struct {
	RecordsLoaded int64
	CellsBuilt    int64
	StackDepth    int
	CacheSize     int
	Done          bool
}

func LoadCellGraph(ctx context.Context, hash []byte, loadRecord func(hash []byte) (*CellRecord, error), progress ...func(LoadCellGraphProgress)) (*cell.Cell, error) {
	rootHash, err := hashBytesToCellHash(hash)
	if err != nil {
		return nil, err
	}

	cache := newCellGraphCache()
	stack := []cellLoadFrame{{hash: rootHash}}
	var recordsLoaded int64
	var cellsBuilt int64
	var iterations uint64

	reportProgress := func(done bool) {
		if len(progress) == 0 || progress[0] == nil {
			return
		}
		progress[0](LoadCellGraphProgress{
			RecordsLoaded: recordsLoaded,
			CellsBuilt:    cellsBuilt,
			StackDepth:    len(stack),
			CacheSize:     cache.len(),
			Done:          done,
		})
	}

	for len(stack) > 0 {
		if iterations&0x3fff == 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}
		}
		iterations++

		top := &stack[len(stack)-1]
		if cache.get(top.hash) != nil {
			stack = stack[:len(stack)-1]
			continue
		}

		if top.record == nil {
			record, err := loadRecord(top.hash[:])
			if err != nil {
				return nil, err
			}
			top.record = record
			recordsLoaded++
			if recordsLoaded&0x1ffff == 0 {
				reportProgress(false)
			}
		}

		if !top.refsReady {
			if len(top.record.Refs) > len(top.refs) {
				return nil, fmt.Errorf("invalid cell refs count %d", len(top.record.Refs))
			}
			for i := range top.record.Refs {
				refHashBytes, err := CellRefHash(top.record.Refs[i])
				if err != nil {
					return nil, err
				}
				refHash, err := hashBytesToCellHash(refHashBytes)
				if err != nil {
					return nil, err
				}
				top.refs[i] = refHash
			}
			top.refsCount = len(top.record.Refs)
			top.refsReady = true
		}

		var pushed bool
		for i := top.refsCount - 1; i >= 0; i-- {
			refHash := top.refs[i]
			if cache.get(refHash) != nil {
				continue
			}
			if refHash == top.hash {
				return nil, fmt.Errorf("recursive cell reference %x", refHash[:])
			}

			stack = append(stack, cellLoadFrame{hash: refHash})
			pushed = true
			break
		}
		if pushed {
			continue
		}

		builder := cell.BeginCell()
		cellBits, err := cellRecordBits(top.record)
		if err != nil {
			return nil, formatCellRebuildError(top.record, err)
		}
		if err := builder.StoreSlice(top.record.Data, cellBits); err != nil {
			return nil, err
		}
		for i := 0; i < top.refsCount; i++ {
			refHash := top.refs[i]
			ref := cache.get(refHash)
			if ref == nil {
				return nil, fmt.Errorf("missing child cell %x", refHash[:])
			}
			if err := builder.StoreRef(ref); err != nil {
				return nil, err
			}
		}

		rebuilt, err := builder.EndCellSpecial(top.record.D1&8 != 0)
		if err != nil {
			return nil, formatCellRebuildError(top.record, err)
		}
		gotHash := rebuilt.HashKey()
		if gotHash != top.hash {
			return nil, fmt.Errorf("rebuilt cell hash mismatch: got=%x want=%x", gotHash[:], top.hash[:])
		}

		cache.set(top.hash, rebuilt)
		cellsBuilt++
		if cellsBuilt&0x1ffff == 0 {
			reportProgress(false)
		}
		stack = stack[:len(stack)-1]
	}

	root := cache.get(rootHash)
	if root == nil {
		return nil, fmt.Errorf("missing root cell %x", hash)
	}
	reportProgress(true)
	return root, nil
}

func hashBytesToCellHash(hash []byte) (cell.Hash, error) {
	var key cell.Hash
	if len(hash) != len(key) {
		return key, fmt.Errorf("cell hash size mismatch: %d", len(hash))
	}
	copy(key[:], hash)
	return key, nil
}

func CollectCellRecords(root *cell.Cell) ([]*CellRecord, error) {
	if root == nil {
		return nil, nil
	}

	seen := map[cell.Hash]struct{}{}
	records := make([]*CellRecord, 0, 1024)
	stack := []*cell.Cell{root}

	for len(stack) > 0 {
		idx := len(stack) - 1
		current := stack[idx]
		stack = stack[:idx]

		hash := current.HashKey()
		if _, ok := seen[hash]; ok {
			continue
		}
		seen[hash] = struct{}{}

		record, err := CellRecordFromCell(current)
		if err != nil {
			return nil, err
		}
		records = append(records, record)

		for i := uint(0); i < current.RefsNum(); i++ {
			ref, err := current.PeekRef(int(i))
			if err != nil {
				return nil, err
			}
			stack = append(stack, ref)
		}
	}

	sort.Slice(records, func(i, j int) bool {
		return bytes.Compare(records[i].Hash, records[j].Hash) < 0
	})
	return records, nil
}
