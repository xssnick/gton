package service

import (
	"sort"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

func cloneBlockStateSlice(states []*storage.BlockState) []*storage.BlockState {
	if len(states) == 0 {
		return nil
	}
	cloned := make([]*storage.BlockState, 0, len(states))
	for _, state := range states {
		cloned = append(cloned, storage.CloneBlockState(state))
	}
	return cloned
}

type appliedStateSet struct {
	states map[string]appliedStateEntry
}

type appliedStateEntry struct {
	state *storage.BlockState
	cells map[cell.Hash][]byte
}

type appliedStateCheckpoint struct {
	keys   []string
	states []*storage.BlockState
	cells  []storage.EncodedCellRecord
}

func (s *appliedStateSet) remember(state *storage.BlockState) {
	s.rememberWithCells(state, nil)
}

func (s *appliedStateSet) rememberWithCells(state *storage.BlockState, cells map[cell.Hash][]byte) {
	if state == nil {
		return
	}
	if s.states == nil {
		s.states = map[string]appliedStateEntry{}
	}
	s.states[storage.BlockKey(state.Block)] = appliedStateEntry{
		state: storage.CloneBlockState(state),
		cells: cells,
	}
}

func (s *appliedStateSet) rememberAll(states []*storage.BlockState) {
	for _, state := range states {
		s.remember(state)
	}
}

func (s *appliedStateSet) rememberAllEntries(entries []appliedStateEntry) {
	for _, entry := range entries {
		s.rememberWithCells(entry.state, entry.cells)
	}
}

func (s *appliedStateSet) take() []*storage.BlockState {
	entries := s.takeEntries()
	states := make([]*storage.BlockState, 0, len(entries))
	for _, entry := range entries {
		states = append(states, storage.CloneBlockState(entry.state))
	}
	return states
}

func (s *appliedStateSet) takeEntries() []appliedStateEntry {
	entries := s.cloneEntries()
	s.states = nil
	return entries
}

func (s *appliedStateSet) takeCheckpoint() ([]*storage.BlockState, []storage.EncodedCellRecord) {
	entries := s.takeEntries()
	checkpoint := appliedStateCheckpointFromEntries(entries)
	return checkpoint.states, checkpoint.cells
}

func (s *appliedStateSet) checkpoint() appliedStateCheckpoint {
	if len(s.states) == 0 {
		return appliedStateCheckpoint{}
	}

	keys := sortedStateEntryKeys(s.states)
	states := make([]*storage.BlockState, 0, len(keys))
	totalCells := 0
	for _, key := range keys {
		entry := s.states[key]
		states = append(states, storage.CloneBlockState(entry.state))
		totalCells += len(entry.cells)
	}

	return appliedStateCheckpoint{
		keys:   keys,
		states: states,
		cells:  encodedCellRecordsFromEntries(keys, s.states, totalCells),
	}
}

func (s *appliedStateSet) completeCheckpoint(checkpoint appliedStateCheckpoint) {
	if len(checkpoint.keys) == 0 || len(s.states) == 0 {
		return
	}

	for _, key := range checkpoint.keys {
		delete(s.states, key)
	}
	if len(s.states) == 0 {
		s.states = nil
	}
}

func appliedStateCheckpointFromEntries(entries []appliedStateEntry) appliedStateCheckpoint {
	if len(entries) == 0 {
		return appliedStateCheckpoint{}
	}

	keys := make([]string, 0, len(entries))
	states := make([]*storage.BlockState, 0, len(entries))
	totalCells := 0
	for _, entry := range entries {
		if entry.state == nil {
			continue
		}
		keys = append(keys, storage.BlockKey(entry.state.Block))
		states = append(states, storage.CloneBlockState(entry.state))
		totalCells += len(entry.cells)
	}

	return appliedStateCheckpoint{
		keys:   keys,
		states: states,
		cells:  encodedCellRecordsFromEntrySlice(entries, totalCells),
	}
}

func encodedCellRecordsFromEntries(keys []string, entries map[string]appliedStateEntry, totalCells int) []storage.EncodedCellRecord {
	if totalCells == 0 {
		return nil
	}

	seen := make(map[cell.Hash]struct{}, totalCells)
	records := make([]storage.EncodedCellRecord, 0, totalCells)
	for _, key := range keys {
		records = appendEncodedCellRecords(records, seen, entries[key].cells)
	}
	return records
}

func encodedCellRecordsFromEntrySlice(entries []appliedStateEntry, totalCells int) []storage.EncodedCellRecord {
	if totalCells == 0 {
		return nil
	}

	seen := make(map[cell.Hash]struct{}, totalCells)
	records := make([]storage.EncodedCellRecord, 0, totalCells)
	for _, entry := range entries {
		records = appendEncodedCellRecords(records, seen, entry.cells)
	}
	return records
}

func appendEncodedCellRecords(records []storage.EncodedCellRecord, seen map[cell.Hash]struct{}, cells map[cell.Hash][]byte) []storage.EncodedCellRecord {
	for hash, data := range cells {
		if len(data) == 0 {
			continue
		}
		if _, ok := seen[hash]; ok {
			continue
		}
		seen[hash] = struct{}{}
		records = append(records, storage.EncodedCellRecord{Hash: hash, Data: data})
	}
	return records
}

func (s *appliedStateSet) clone() []*storage.BlockState {
	entries := s.cloneEntries()
	states := make([]*storage.BlockState, 0, len(entries))
	for _, entry := range entries {
		states = append(states, storage.CloneBlockState(entry.state))
	}
	return states
}

func (s *appliedStateSet) cloneEntries() []appliedStateEntry {
	if len(s.states) == 0 {
		return nil
	}

	cloned := make(map[string]appliedStateEntry, len(s.states))
	for key, entry := range s.states {
		cloned[key] = appliedStateEntry{
			state: storage.CloneBlockState(entry.state),
			cells: entry.cells,
		}
	}
	return sortedClonedStateEntries(cloned)
}

func sortedClonedStateEntries(entries map[string]appliedStateEntry) []appliedStateEntry {
	if len(entries) == 0 {
		return nil
	}
	cloned := make([]appliedStateEntry, 0, len(entries))
	keys := sortedStateEntryKeys(entries)
	for _, key := range keys {
		entry := entries[key]
		cloned = append(cloned, appliedStateEntry{
			state: storage.CloneBlockState(entry.state),
			cells: entry.cells,
		})
	}
	return cloned
}

func sortedStateEntryKeys(entries map[string]appliedStateEntry) []string {
	keys := make([]string, 0, len(entries))
	for key := range entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
