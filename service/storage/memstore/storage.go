package memstore

import (
	"context"
	"flexserver/service/storage"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type Store struct {
	*PeerStore
	*StateStore

	mu       sync.RWMutex
	metas    map[string]*storage.BlockMeta
	cells    map[string]*storage.CellRecord
	accounts map[string][]storage.AccountTxIndexEntry
}

func New() *Store {
	return &Store{
		PeerStore:  NewPeerStore(),
		StateStore: NewStateStore(),
		metas:      map[string]*storage.BlockMeta{},
		cells:      map[string]*storage.CellRecord{},
		accounts:   map[string][]storage.AccountTxIndexEntry{},
	}
}

func (s *Store) Close() error {
	return nil
}

func (s *Store) SaveBlockFull(block *storage.ServedBlockFull) error {
	if err := s.PeerStore.SaveBlockFull(block); err != nil {
		return err
	}

	return s.SaveBlockMeta(servedBlockFullMeta(block))
}

func (s *Store) SaveArchiveImport(imported *storage.ServedArchiveImport) error {
	if imported == nil {
		return fmt.Errorf("served archive import is nil")
	}
	if err := s.PeerStore.SaveArchiveImport(imported); err != nil {
		return err
	}

	for _, full := range imported.FullBlocks {
		if err := s.SaveBlockMeta(servedBlockFullMeta(full)); err != nil {
			return err
		}
	}
	for _, block := range imported.BlockData {
		meta := storage.MergeBlockMetaFromBlockData(&storage.BlockMeta{
			ID:        block.ID,
			Flags:     storage.BlockMetaHasBlockData,
			UpdatedAt: time.Now(),
		}, block.ID, block.Data)
		if err := s.SaveBlockMeta(meta); err != nil {
			return err
		}
	}
	for _, proof := range imported.Proofs {
		if err := s.SaveBlockMeta(&storage.BlockMeta{
			ID:        proof.ID,
			Flags:     storage.BlockMetaFlagForProof(proof.Kind),
			UpdatedAt: time.Now(),
		}); err != nil {
			return err
		}
	}
	return nil
}

func servedBlockFullMeta(block *storage.ServedBlockFull) *storage.BlockMeta {
	meta := &storage.BlockMeta{
		ID:        block.ID,
		Flags:     storage.BlockMetaHasServedFull,
		UpdatedAt: time.Now(),
	}
	if block.IsLink {
		meta.Mark(storage.BlockMetaServedFullIsLink)
	}
	if block.Meta != nil {
		meta = storage.MergeBlockMeta(meta, block.Meta)
		meta.ID = block.ID
	} else {
		meta = storage.MergeBlockMetaFromBlockData(meta, block.ID, block.Block)
	}
	if len(block.Proof) > 0 {
		for _, kind := range storage.StoredProofKindsForBlock(block.IsLink, meta.Has(storage.BlockMetaIsKeyBlock)) {
			meta.Mark(storage.BlockMetaFlagForProof(kind))
		}
	}
	return meta
}

func (s *Store) SaveBlockData(block ton.BlockIDExt, data []byte, ref *storage.ArtifactRef) {
	s.PeerStore.SaveBlockData(block, data, ref)

	meta := storage.MergeBlockMetaFromBlockData(&storage.BlockMeta{
		ID:        block,
		Flags:     storage.BlockMetaHasBlockData,
		UpdatedAt: time.Now(),
	}, block, data)
	_ = s.SaveBlockMeta(meta)
}

func (s *Store) SaveBlockProof(kind storage.ServedProofKind, block ton.BlockIDExt, data []byte, ref *storage.ArtifactRef) {
	s.PeerStore.SaveBlockProof(kind, block, data, ref)
	_ = s.SaveBlockMeta(&storage.BlockMeta{
		ID:        block,
		Flags:     storage.BlockMetaFlagForProof(kind),
		UpdatedAt: time.Now(),
	})
}

func (s *Store) SaveCurrentState(ctx context.Context, state *storage.CurrentState) error {
	if err := s.StateStore.SaveCurrentState(ctx, state); err != nil {
		return err
	}
	if err := s.saveBlockStateArtifacts(&state.Masterchain); err != nil {
		return err
	}
	for _, key := range storage.SortedShardKeys(state.Shards) {
		shard := state.Shards[key]
		if err := s.saveBlockStateArtifacts(&shard); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) SaveBlockState(ctx context.Context, state *storage.BlockState) error {
	if err := s.StateStore.SaveBlockState(ctx, state); err != nil {
		return err
	}
	return s.saveBlockStateArtifacts(state)
}

func (s *Store) SaveBlockStateAndCurrentState(ctx context.Context, block *storage.BlockState, current *storage.CurrentState) error {
	var blocks []*storage.BlockState
	if block != nil {
		blocks = []*storage.BlockState{block}
	}
	return s.SaveBlockStatesAndCurrentState(ctx, blocks, current)
}

func (s *Store) SaveBlockStatesAndCurrentState(ctx context.Context, blocks []*storage.BlockState, current *storage.CurrentState) error {
	if err := s.StateStore.SaveBlockStatesAndCurrentState(ctx, blocks, current); err != nil {
		return err
	}
	for _, block := range blocks {
		if block == nil {
			continue
		}
		if err := s.saveBlockStateArtifacts(block); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) ImportStateCellTree(ctx context.Context, block ton.BlockIDExt, root *cell.Cell, parsedCells []cell.Cell, totalCells uint64) (*cell.Cell, error) {
	lazyRoots, err := s.ImportStateCellTrees(ctx, []storage.StateCellTreeImport{{
		Block:       block,
		Root:        root,
		ParsedCells: parsedCells,
		TotalCells:  totalCells,
	}})
	if err != nil {
		return nil, err
	}
	return lazyRoots[0], nil
}

func (s *Store) ImportStateCellTrees(ctx context.Context, trees []storage.StateCellTreeImport) ([]*cell.Cell, error) {
	lazyRoots, err := s.StateStore.ImportStateCellTrees(ctx, trees)
	if err != nil {
		return nil, err
	}
	var records []*storage.CellRecord
	for _, tree := range trees {
		if len(tree.ParsedCells) > 0 {
			for i := range tree.ParsedCells {
				record, err := storage.CellRecordFromCell(&tree.ParsedCells[i])
				if err != nil {
					return nil, err
				}
				records = append(records, record)
			}
			continue
		}

		next, err := storage.CollectCellRecords(tree.Root)
		if err != nil {
			return nil, err
		}
		records = append(records, next...)
	}

	if err = s.SaveCells(records); err != nil {
		return nil, err
	}
	return lazyRoots, nil
}

func (s *Store) saveBlockStateArtifacts(state *storage.BlockState) error {
	if state == nil {
		return fmt.Errorf("block state is nil")
	}
	if err := s.SaveBlockMeta(storage.BuildBlockMetaFromState(*state)); err != nil {
		return err
	}

	root := state.Cell
	if root == nil {
		return nil
	}

	records, err := storage.CollectCellRecords(root)
	if err != nil {
		return err
	}
	return s.SaveCells(records)
}

func (s *Store) SaveBlockMeta(meta *storage.BlockMeta) error {
	if meta == nil {
		return fmt.Errorf("block meta is nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := storage.BlockKey(meta.ID)
	s.metas[key] = storage.MergeBlockMeta(s.metas[key], meta)
	return nil
}

func (s *Store) BlockMeta(_ context.Context, block ton.BlockIDExt) (*storage.BlockMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	meta := s.metas[storage.BlockKey(block)]
	if meta == nil {
		return nil, storage.ErrNotFound
	}
	return meta.Clone(), nil
}

func (s *Store) LookupBlockBySeqNo(_ context.Context, key storage.BlockHistoryKey, seqno uint32) (ton.BlockIDExt, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, meta := range s.metas {
		if meta.ID.Workchain == key.Workchain && meta.ID.Shard == key.Shard && meta.ID.SeqNo == seqno {
			return meta.ID, nil
		}
	}
	return ton.BlockIDExt{}, storage.ErrNotFound
}

func (s *Store) LookupBlockByLT(_ context.Context, key storage.BlockHistoryKey, lt uint64) (ton.BlockIDExt, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var best *storage.BlockMeta
	for _, meta := range s.metas {
		if meta.ID.Workchain != key.Workchain || meta.ID.Shard != key.Shard {
			continue
		}
		if meta.StartLT != 0 && lt >= meta.StartLT && lt <= meta.EndLT {
			return meta.ID, nil
		}
		if meta.EndLT == 0 || meta.EndLT > lt {
			continue
		}
		if best == nil || meta.EndLT > best.EndLT || (meta.EndLT == best.EndLT && meta.ID.SeqNo > best.ID.SeqNo) {
			best = meta
		}
	}
	if best == nil {
		return ton.BlockIDExt{}, storage.ErrNotFound
	}
	return best.ID, nil
}

func (s *Store) LookupBlockByUnixTime(_ context.Context, key storage.BlockHistoryKey, utime uint32) (ton.BlockIDExt, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var best *storage.BlockMeta
	for _, meta := range s.metas {
		if meta.ID.Workchain != key.Workchain || meta.ID.Shard != key.Shard {
			continue
		}
		if meta.GenUTime > utime {
			continue
		}
		if best == nil || meta.GenUTime > best.GenUTime || (meta.GenUTime == best.GenUTime && meta.ID.SeqNo > best.ID.SeqNo) {
			best = meta
		}
	}
	if best == nil {
		return ton.BlockIDExt{}, storage.ErrNotFound
	}
	return best.ID, nil
}

func (s *Store) SaveCells(records []*storage.CellRecord) error {
	if len(records) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, record := range records {
		if len(record.Hash) == 0 {
			return fmt.Errorf("cell record hash is empty")
		}
		s.cells[string(record.Hash)] = record.Clone()
	}
	return nil
}

func (s *Store) CellRecord(_ context.Context, hash []byte) (*storage.CellRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	record := s.cells[string(hash)]
	if record == nil {
		return nil, storage.ErrNotFound
	}
	return record.Clone(), nil
}

func (s *Store) LoadCell(ctx context.Context, hash []byte) (*cell.Cell, error) {
	return storage.LoadCellGraph(ctx, hash, func(sum []byte) (*storage.CellRecord, error) {
		return s.CellRecord(ctx, sum)
	})
}

func (s *Store) SaveAccountTxIndex(entry storage.AccountTxIndexEntry) error {
	if len(entry.AccountKey) == 0 {
		return fmt.Errorf("account key is empty")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := string(entry.AccountKey)
	entries := append(s.accounts[key], entry.Clone())
	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].LT == entries[j].LT {
			return entries[i].Block.SeqNo > entries[j].Block.SeqNo
		}
		return entries[i].LT > entries[j].LT
	})
	s.accounts[key] = entries
	return nil
}

func (s *Store) ListAccountTx(_ context.Context, accountKey []byte, beforeLT uint64, limit int) ([]storage.AccountTxIndexEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entries := s.accounts[string(accountKey)]
	if len(entries) == 0 {
		return nil, nil
	}

	if limit <= 0 || limit > len(entries) {
		limit = len(entries)
	}

	res := make([]storage.AccountTxIndexEntry, 0, limit)
	for _, entry := range entries {
		if beforeLT != 0 && entry.LT >= beforeLT {
			continue
		}
		res = append(res, entry.Clone())
		if len(res) >= limit {
			break
		}
	}
	return res, nil
}
