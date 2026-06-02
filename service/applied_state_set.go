package service

import (
	"bytes"
	"sort"

	"github.com/xssnick/gton/service/storage"
)

type appliedStateSet struct {
	states map[storage.BlockRootHash]appliedStateEntry
}

type appliedStateEntry struct {
	state    *storage.BlockState
	artifact *storage.ServedBlockFull
	links    []storage.ServedBlockLink
}

type appliedStateCheckpoint struct {
	keys    []storage.BlockRootHash
	entries []storage.StateCheckpointBlock
}

func (s *appliedStateSet) remember(state *storage.BlockState) {
	s.rememberWithArtifacts(state, nil, nil)
}

func (s *appliedStateSet) rememberWithArtifacts(state *storage.BlockState, artifact *storage.ServedBlockFull, links []storage.ServedBlockLink) {
	if state == nil {
		return
	}
	if s.states == nil {
		s.states = map[storage.BlockRootHash]appliedStateEntry{}
	}
	s.states[storage.BlockKey(state.Block)] = appliedStateEntry{
		state:    storage.CloneBlockState(state),
		artifact: cloneServedBlockFullSharedPayload(artifact),
		links:    cloneServedBlockLinks(links),
	}
}

func (s *appliedStateSet) rememberAllEntries(entries []appliedStateEntry) {
	for _, entry := range entries {
		s.rememberEntry(entry)
	}
}

func (s *appliedStateSet) takeEntries() []appliedStateEntry {
	entries := s.cloneEntries()
	s.states = nil
	return entries
}

func (s *appliedStateSet) checkpoint() appliedStateCheckpoint {
	if len(s.states) == 0 {
		return appliedStateCheckpoint{}
	}

	keys := sortedStateEntryKeys(s.states)
	entries := make([]storage.StateCheckpointBlock, 0, len(keys))
	for _, key := range keys {
		entry := s.states[key]
		entries = append(entries, storage.StateCheckpointBlock{
			State:    storage.CloneBlockState(entry.state),
			Artifact: cloneServedBlockFullSharedPayload(entry.artifact),
			Links:    cloneServedBlockLinks(entry.links),
		})
	}

	return appliedStateCheckpoint{
		keys:    keys,
		entries: entries,
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

func (s *appliedStateSet) cloneEntries() []appliedStateEntry {
	if len(s.states) == 0 {
		return nil
	}

	cloned := make(map[storage.BlockRootHash]appliedStateEntry, len(s.states))
	for key, entry := range s.states {
		cloned[key] = cloneAppliedStateEntry(entry)
	}
	return sortedClonedStateEntries(cloned)
}

func sortedClonedStateEntries(entries map[storage.BlockRootHash]appliedStateEntry) []appliedStateEntry {
	if len(entries) == 0 {
		return nil
	}
	cloned := make([]appliedStateEntry, 0, len(entries))
	keys := sortedStateEntryKeys(entries)
	for _, key := range keys {
		entry := entries[key]
		cloned = append(cloned, cloneAppliedStateEntry(entry))
	}
	return cloned
}

func sortedStateEntryKeys(entries map[storage.BlockRootHash]appliedStateEntry) []storage.BlockRootHash {
	keys := make([]storage.BlockRootHash, 0, len(entries))
	for key := range entries {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i][:], keys[j][:]) < 0
	})
	return keys
}

func (s *appliedStateSet) rememberEntry(entry appliedStateEntry) {
	if entry.state == nil {
		return
	}
	if s.states == nil {
		s.states = map[storage.BlockRootHash]appliedStateEntry{}
	}
	s.states[storage.BlockKey(entry.state.Block)] = cloneAppliedStateEntry(entry)
}

func cloneAppliedStateEntry(entry appliedStateEntry) appliedStateEntry {
	return appliedStateEntry{
		state:    storage.CloneBlockState(entry.state),
		artifact: cloneServedBlockFullSharedPayload(entry.artifact),
		links:    cloneServedBlockLinks(entry.links),
	}
}

func cloneServedBlockFullSharedPayload(block *storage.ServedBlockFull) *storage.ServedBlockFull {
	if block == nil {
		return nil
	}
	return &storage.ServedBlockFull{
		ID:                     block.ID,
		Proof:                  block.Proof,
		Block:                  block.Block,
		Meta:                   block.Meta.Clone(),
		IsLink:                 block.IsLink,
		ArchiveShardSplitDepth: block.ArchiveShardSplitDepth,
	}
}

func cloneServedBlockLinks(links []storage.ServedBlockLink) []storage.ServedBlockLink {
	if len(links) == 0 {
		return nil
	}
	return append([]storage.ServedBlockLink(nil), links...)
}
