package pebblestore

import (
	"sort"

	"github.com/xssnick/gton/service/storage"
)

// packWritebackBytesThreshold caps how many freshly appended pack bytes may
// stay dirty in the page cache before the prewrite path schedules a background
// sync for the file. Durability is still enforced only by the checkpoint-time
// sync; this merely spreads the disk writeback over the checkpoint window so
// the final sync has almost nothing left to write.
const packWritebackBytesThreshold = int64(32 << 20)

// prewrittenArtifactRecord keeps the pack refs produced by a streamed append
// for one staged checkpoint block until a checkpoint consumes them. The block
// payload itself is already on disk, so the retained write is slimmed down to
// metadata and refs. Guarded by artifactPublishMu.
type prewrittenArtifactRecord struct {
	write      checkpointArtifactWrite
	blockSize  int
	proofSize  int
	isLink     bool
	splitDepth uint32
}

// PrewriteCheckpointArtifacts appends the artifact payloads of staged blocks
// into archive pack files ahead of the owning state checkpoint. The resulting
// refs are kept per block and consumed by the next SaveStateCheckpointEntries
// that includes the block; blocks replaced after the prewrite fall back to an
// inline append there. A crash before the checkpoint leaves a dirty pack tail
// that startup recovery truncates, exactly as for checkpoint-time appends, and
// nothing references the tail because refs are only published by checkpoints.
func (s *Store) PrewriteCheckpointArtifacts(entries []storage.StateCheckpointBlock) error {
	appendEntries := make([]storage.StateCheckpointBlock, 0, len(entries))
	for _, entry := range entries {
		if entry.Artifact == nil {
			continue
		}
		appendEntries = append(appendEntries, entry)
	}
	if len(appendEntries) == 0 {
		return nil
	}

	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return errPebbleClosed
	}

	s.artifactPublishMu.Lock()
	writes, registrations, err := s.buildCheckpointArtifactWrites(appendEntries)
	if err != nil {
		s.artifactPublishMu.Unlock()
		return err
	}
	if err = s.storePrewrittenArtifactWritesLocked(appendEntries, writes, registrations); err != nil {
		s.artifactPublishMu.Unlock()
		return err
	}
	due := s.packWritebackDue()
	s.artifactPublishMu.Unlock()

	s.syncPackWriteback(due)
	return nil
}

func (s *Store) storePrewrittenArtifactWritesLocked(entries []storage.StateCheckpointBlock, writes []checkpointArtifactWrite, registrations []archivePackRegistration) error {
	if s.prewrittenArtifacts == nil {
		s.prewrittenArtifacts = make(map[storage.BlockRootHash]prewrittenArtifactRecord)
	}
	for i := range writes {
		artifact := entries[i].Artifact
		write := writes[i]
		// The payload bytes are already appended to the pack file; keep only the
		// block identity and metadata so pending records do not pin whole blocks
		// in memory between checkpoints.
		slim := *write.block
		slim.Block = nil
		slim.Proof = nil
		write.block = &slim
		s.prewrittenArtifacts[storage.BlockKey(artifact.ID)] = prewrittenArtifactRecord{
			write:      write,
			blockSize:  len(artifact.Block),
			proofSize:  len(artifact.Proof),
			isLink:     artifact.IsLink,
			splitDepth: artifact.ArchiveShardSplitDepth,
		}
	}

	if len(registrations) == 0 {
		return nil
	}
	if s.prewrittenPackRegs == nil {
		s.prewrittenPackRegs = make(map[int64]archivePackRegistration)
	}
	for _, reg := range registrations {
		if !reg.valid {
			continue
		}
		current, ok := s.prewrittenPackRegs[reg.archiveID]
		if !ok {
			s.prewrittenPackRegs[reg.archiveID] = reg
			continue
		}
		if err := mergeArchivePackRegistration(&current, reg); err != nil {
			return err
		}
		s.prewrittenPackRegs[reg.archiveID] = current
	}
	return nil
}

// takePrewrittenArtifactWrite consumes the streamed append result for the
// entry when the staged artifact still matches what was appended. Callers must
// hold artifactPublishMu.
func (s *Store) takePrewrittenArtifactWrite(entry storage.StateCheckpointBlock) (checkpointArtifactWrite, bool) {
	artifact := entry.Artifact
	if artifact == nil || len(s.prewrittenArtifacts) == 0 {
		return checkpointArtifactWrite{}, false
	}
	key := storage.BlockKey(artifact.ID)
	record, ok := s.prewrittenArtifacts[key]
	if !ok {
		return checkpointArtifactWrite{}, false
	}
	delete(s.prewrittenArtifacts, key)
	if record.blockSize != len(artifact.Block) ||
		record.proofSize != len(artifact.Proof) ||
		record.isLink != artifact.IsLink ||
		record.splitDepth != artifact.ArchiveShardSplitDepth {
		// The staged artifact was replaced after the prewrite. The appended copy
		// stays as an unreferenced pack entry; append the current one inline.
		return checkpointArtifactWrite{}, false
	}
	return record.write, true
}

// claimPrewrittenPackRegs seeds claims with the staged, not-yet-committed pack
// registrations. The per-base load reads only committed state, so without the
// seed a later append in the same checkpoint window would hand the staged
// indexes out again to different packs. Callers must hold artifactPublishMu.
func (s *Store) claimPrewrittenPackRegs(claimed *archivePackClaims) error {
	for _, reg := range s.prewrittenPackRegs {
		desc := archivePackDescriptor{
			baseSeqno:  reg.baseSeq,
			startSeqno: reg.startSeq,
			workchain:  reg.workchain,
			shard:      reg.shard,
		}
		if !claimed.claim(reg.archiveID, desc) {
			return archivePackClaimError(claimed, reg.archiveID, desc)
		}
		if index := archivePackageIndex(reg.archiveID); index >= archiveFirstDynamicPackageIndex {
			claimed.advancePackageIndex(reg.baseSeq, index)
		}
	}
	return nil
}

// drainPrewrittenPackRegistrations hands all accumulated pack registrations to
// the running checkpoint. Registrations may cover blocks staged after the
// checkpoint snapshot; publishing those early is safe because the checkpoint
// sync covers every append that produced them. Callers must hold
// artifactPublishMu.
func (s *Store) drainPrewrittenPackRegistrations() []archivePackRegistration {
	if len(s.prewrittenPackRegs) == 0 {
		return nil
	}
	registrations := make([]archivePackRegistration, 0, len(s.prewrittenPackRegs))
	for _, reg := range s.prewrittenPackRegs {
		registrations = append(registrations, reg)
	}
	s.prewrittenPackRegs = nil
	sort.Slice(registrations, func(i, j int) bool {
		return registrations[i].archiveID < registrations[j].archiveID
	})
	return registrations
}

// packWritebackDue returns pack files whose dirty tail crossed the writeback
// threshold and advances their writeback watermarks. The watermark is advanced
// optimistically: a failed background sync only delays writeback until the
// checkpoint sync, which remains the durability barrier.
func (s *Store) packWritebackDue() []string {
	s.artifactMu.Lock()
	defer s.artifactMu.Unlock()

	var due []string
	for _, pending := range []map[string]pendingPackWrite{s.pendingArchiveSync, s.pendingKeyProofSync} {
		for path, write := range pending {
			if write.size-write.writebackSize < packWritebackBytesThreshold {
				continue
			}
			write.writebackSize = write.size
			pending[path] = write
			due = append(due, path)
		}
	}
	sort.Strings(due)
	return due
}

func (s *Store) syncPackWriteback(paths []string) {
	for _, path := range paths {
		if err := syncFile(path); err != nil {
			s.log.Warn().Err(err).Str("path", path).Msg("background pack writeback sync failed")
		}
	}
}
