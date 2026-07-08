package service

import (
	"bytes"
	"sort"

	"github.com/xssnick/gton/service/storage"
)

type appliedStateSet struct {
	states        map[storage.BlockRootHash]appliedStateEntry
	artifactBytes uint64
	nextVersion   uint64
}

type appliedStateEntry struct {
	state    *storage.BlockState
	artifact appliedBlockArtifact
	version  uint64
}

type appliedBlockArtifact struct {
	block *storage.ServedBlockFull
	links []storage.ServedBlockLink
	bytes uint64
}

type appliedStateCheckpoint struct {
	keys     []storage.BlockRootHash
	versions []uint64
	entries  []storage.StateCheckpointBlock
}

func (s *appliedStateSet) rememberWithArtifacts(state *storage.BlockState, artifact *storage.ServedBlockFull, links []storage.ServedBlockLink) {
	if state == nil {
		return
	}

	s.rememberEntry(appliedStateEntry{
		state:    checkpointBlockStateMetadata(state),
		artifact: appliedBlockArtifactFrom(artifact, links).clone(),
	})
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
	versions := make([]uint64, 0, len(keys))
	entries := make([]storage.StateCheckpointBlock, 0, len(keys))
	for _, key := range keys {
		entry := s.states[key]
		versions = append(versions, entry.version)
		entries = append(entries, storage.StateCheckpointBlock{
			State:    storage.CloneBlockState(entry.state),
			Artifact: cloneServedBlockFullSharedPayload(entry.artifact.block),
			Links:    cloneServedBlockLinks(entry.artifact.links),
		})
	}

	return appliedStateCheckpoint{
		keys:     keys,
		versions: versions,
		entries:  entries,
	}
}

func (s *appliedStateSet) completeCheckpoint(checkpoint appliedStateCheckpoint) {
	if len(checkpoint.keys) == 0 || len(s.states) == 0 {
		return
	}

	for idx, key := range checkpoint.keys {
		if entry, ok := s.states[key]; ok {
			if len(checkpoint.versions) == len(checkpoint.keys) && entry.version != checkpoint.versions[idx] {
				continue
			}
			s.artifactBytes -= entry.artifact.bytes
			delete(s.states, key)
		}
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
	cloned := cloneAppliedStateEntry(entry)
	if existing, ok := s.states[key]; ok {
		s.artifactBytes -= existing.artifact.bytes
		if !cloned.artifact.complete() && existing.artifact.complete() {
			cloned.artifact = existing.artifact.clone()
		}
	}
	s.nextVersion++
	cloned.version = s.nextVersion
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
		version:  entry.version,
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

func (a appliedBlockArtifact) complete() bool {
	return a.block != nil && len(a.block.Block) > 0 && len(a.block.Proof) > 0
}

func servedBlockFullPayloadBytes(block *storage.ServedBlockFull) uint64 {
	if block == nil {
		return 0
	}
	return uint64(len(block.Block)) + uint64(len(block.Proof))
}

func cloneServedBlockFullSharedPayload(block *storage.ServedBlockFull) *storage.ServedBlockFull {
	if block == nil {
		return nil
	}
	return &storage.ServedBlockFull{
		ID:                     cloneServiceBlockID(block.ID),
		Proof:                  block.Proof,
		Block:                  block.Block,
		Meta:                   block.Meta.Clone(),
		IsLink:                 block.IsLink,
		ArchiveShardSplitDepth: block.ArchiveShardSplitDepth,
		MessageEntries:         block.MessageEntries,
	}
}

func cloneServedBlockLinks(links []storage.ServedBlockLink) []storage.ServedBlockLink {
	if len(links) == 0 {
		return nil
	}
	cloned := make([]storage.ServedBlockLink, len(links))
	for i := range links {
		cloned[i] = storage.ServedBlockLink{
			Prev: cloneServiceBlockID(links[i].Prev),
			Next: cloneServiceBlockID(links[i].Next),
		}
	}
	return cloned
}
