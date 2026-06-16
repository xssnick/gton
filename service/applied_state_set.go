package service

import (
	"bytes"
	"sort"

	"github.com/xssnick/gton/service/storage"
)

type appliedStateSet struct {
	states        map[storage.BlockRootHash]appliedStateEntry
	artifactBytes uint64
}

type appliedStateEntry struct {
	state    *storage.BlockState
	artifact appliedBlockArtifact
}

type appliedBlockArtifact struct {
	block *storage.ServedBlockFull
	links []storage.ServedBlockLink
	bytes uint64
}

type appliedStateCheckpoint struct {
	keys    []storage.BlockRootHash
	entries []storage.StateCheckpointBlock
}

func (s *appliedStateSet) rememberWithArtifacts(state *storage.BlockState, artifact *storage.ServedBlockFull, links []storage.ServedBlockLink) {
	if state == nil {
		return
	}
	if s.states == nil {
		s.states = map[storage.BlockRootHash]appliedStateEntry{}
	}
	key := storage.BlockKey(state.Block)
	if existing, ok := s.states[key]; ok {
		s.artifactBytes -= existing.artifact.bytes
	}
	entry := appliedStateEntry{
		state:    checkpointBlockStateMetadata(state),
		artifact: appliedBlockArtifactFrom(artifact, links).clone(),
	}
	s.states[key] = entry
	s.artifactBytes += entry.artifact.bytes
}

func (s *appliedStateSet) rememberAllEntries(entries []appliedStateEntry) {
	for _, entry := range entries {
		s.rememberEntry(entry)
	}
}

func (s *appliedStateSet) takeEntries() []appliedStateEntry {
	entries := s.cloneEntries()
	s.states = nil
	s.artifactBytes = 0
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
			Artifact: cloneServedBlockFullSharedPayload(entry.artifact.block),
			Links:    cloneServedBlockLinks(entry.artifact.links),
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
		if entry, ok := s.states[key]; ok {
			s.artifactBytes -= entry.artifact.bytes
		}
		delete(s.states, key)
	}
	if len(s.states) == 0 {
		s.states = nil
		s.artifactBytes = 0
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
	key := storage.BlockKey(entry.state.Block)
	if existing, ok := s.states[key]; ok {
		s.artifactBytes -= existing.artifact.bytes
	}
	cloned := cloneAppliedStateEntry(entry)
	s.states[key] = cloned
	s.artifactBytes += cloned.artifact.bytes
}

func (s *appliedStateSet) byteSize() uint64 {
	return s.artifactBytes
}

func cloneAppliedStateEntry(entry appliedStateEntry) appliedStateEntry {
	return appliedStateEntry{
		state:    storage.CloneBlockState(entry.state),
		artifact: entry.artifact.clone(),
	}
}

func checkpointBlockStateMetadata(state *storage.BlockState) *storage.BlockState {
	if state == nil {
		return nil
	}
	metadata := storage.BlockStateWithoutCells(state)
	if len(metadata.StateRootHash) == 0 && state.Cell != nil {
		hash := state.Cell.HashKey(0)
		metadata.StateRootHash = bytes.Clone(hash[:])
	}
	return &metadata
}

func appliedBlockArtifactFrom(block *storage.ServedBlockFull, links []storage.ServedBlockLink) appliedBlockArtifact {
	return appliedBlockArtifact{
		block: block,
		links: links,
		bytes: servedBlockFullPayloadBytes(block),
	}
}

func (a appliedBlockArtifact) clone() appliedBlockArtifact {
	if a.block == nil {
		return appliedBlockArtifact{}
	}
	return appliedBlockArtifact{
		block: cloneServedBlockFullSharedPayload(a.block),
		links: cloneServedBlockLinks(a.links),
		bytes: a.bytes,
	}
}

func servedBlockFullPayloadBytes(block *storage.ServedBlockFull) uint64 {
	return uint64(len(block.Block)) + uint64(len(block.Proof))
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
