package pebblestore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"

	"github.com/cockroachdb/pebble/v2"
	"github.com/xssnick/gton/service/archive/packfile"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
)

const topShard = int64(-1 << 63)

type archivePackRegistration struct {
	valid            bool
	archiveID        int64
	path             string
	size             int64
	baseSeq          uint32
	startSeq         uint32
	workchain        int32
	shard            int64
	firstMasterSeq   uint32
	firstMasterUTime uint32
	firstMasterLT    uint64
}

type archivePackageMeta struct {
	archiveID        int64
	baseSeq          uint32
	startSeq         uint32
	workchain        int32
	shard            int64
	path             string
	size             int64
	firstMasterSeq   uint32
	firstMasterUTime uint32
	firstMasterLT    uint64
}

type artifactAppendResult struct {
	ref          *storage.ArtifactRef
	registration archivePackRegistration
}

type pendingPackSync struct {
	path string
	seq  uint64
	size int64
}

func (s *Store) syncPendingArtifactFiles() error {
	if err := s.syncPendingArchiveFiles(); err != nil {
		return err
	}
	return s.syncPendingKeyProofFiles()
}

func (s *Store) syncPendingArchiveFiles() error {
	return s.syncPendingPackFiles(s.pendingArchiveSync, "archive")
}

func (s *Store) syncPendingKeyProofFiles() error {
	return s.syncPendingPackFiles(s.pendingKeyProofSync, "key proof")
}

func (s *Store) syncPendingPackFiles(pending map[string]uint64, label string) error {
	s.artifactMu.Lock()
	if len(pending) == 0 {
		s.artifactMu.Unlock()
		return nil
	}

	items := make([]pendingPackSync, 0, len(pending))
	for path, seq := range pending {
		stat, err := os.Stat(path)
		if err != nil {
			s.artifactMu.Unlock()
			return fmt.Errorf("stat %s pending pack %s: %w", label, path, err)
		}
		items = append(items, pendingPackSync{
			path: path,
			seq:  seq,
			size: stat.Size(),
		})
	}
	s.artifactMu.Unlock()

	sort.Slice(items, func(i, j int) bool {
		return items[i].path < items[j].path
	})

	dirs := make(map[string]struct{}, len(items))
	for _, item := range items {
		if err := syncFile(item.path); err != nil {
			return fmt.Errorf("sync %s pack %s: %w", label, item.path, err)
		}
		dirs[filepath.Dir(item.path)] = struct{}{}
	}

	dirPaths := make([]string, 0, len(dirs))
	for dir := range dirs {
		dirPaths = append(dirPaths, dir)
	}
	sort.Strings(dirPaths)
	for _, dir := range dirPaths {
		if err := syncDir(dir); err != nil {
			return fmt.Errorf("sync %s pack dir %s: %w", label, dir, err)
		}
	}
	if err := s.commitSyncedPackSizes(items); err != nil {
		return err
	}

	s.artifactMu.Lock()
	for _, item := range items {
		if pending[item.path] == item.seq {
			delete(pending, item.path)
		}
	}
	s.artifactMu.Unlock()
	return nil
}

func syncFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	return file.Sync()
}

func syncDir(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	if err = file.Sync(); err != nil && !errors.Is(err, syscall.EINVAL) {
		return err
	}
	return nil
}

func (s *Store) appendArchiveEntry(kind string, block ton.BlockIDExt, meta *storage.BlockMeta, splitDepth uint32, data []byte) (artifactAppendResult, error) {
	location, err := s.archiveEntryLocation(block, meta, splitDepth)
	if err != nil {
		return artifactAppendResult{}, err
	}
	firstMasterSeq, firstMasterUTime, firstMasterLT, err := archiveEntryFirstMaster(block, meta)
	if err != nil {
		return artifactAppendResult{}, err
	}

	s.artifactMu.Lock()
	defer s.artifactMu.Unlock()

	path := s.archivePackPath(location.sliceSeqno, location.workchain, location.shard)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return artifactAppendResult{}, err
	}
	if err := s.ensureCleanPackTail(path, s.pendingArchiveSync, s.dirtyArchivePacks); err != nil {
		return artifactAppendResult{}, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return artifactAppendResult{}, err
	}
	defer func() { _ = file.Close() }()

	ptr, err := packfile.Append(file, packfile.EntryName(kind, block), data, false)
	if err != nil {
		s.markDirtyPackTail(s.dirtyArchivePacks, path)
		return artifactAppendResult{}, err
	}
	stat, err := file.Stat()
	if err != nil {
		return artifactAppendResult{}, err
	}
	s.markPendingPackSync(s.pendingArchiveSync, path)

	ref := &storage.ArtifactRef{
		Path:   s.relativeArtifactPath(path),
		Offset: ptr.Offset,
		Size:   ptr.Size,
	}
	return artifactAppendResult{
		ref: ref,
		registration: archivePackRegistration{
			valid:            true,
			archiveID:        location.archiveID,
			path:             ref.Path,
			size:             stat.Size(),
			baseSeq:          location.baseSeqno,
			startSeq:         location.sliceSeqno,
			workchain:        location.workchain,
			shard:            location.shard,
			firstMasterSeq:   firstMasterSeq,
			firstMasterUTime: firstMasterUTime,
			firstMasterLT:    firstMasterLT,
		},
	}, nil
}

func (s *Store) appendProofEntry(kind storage.ServedProofKind, block ton.BlockIDExt, meta *storage.BlockMeta, splitDepth uint32, data []byte) (artifactAppendResult, error) {
	if isKeyProofKind(kind) {
		ref, err := s.appendKeyBlockProofEntry(kind, block, data)
		if err != nil {
			return artifactAppendResult{}, err
		}
		return artifactAppendResult{ref: ref}, nil
	}
	return s.appendArchiveEntry(packEntryKindForProofKind(kind), block, meta, splitDepth, data)
}

func (s *Store) appendKeyBlockProofEntry(kind storage.ServedProofKind, block ton.BlockIDExt, data []byte) (*storage.ArtifactRef, error) {
	s.artifactMu.Lock()
	defer s.artifactMu.Unlock()

	path := s.keyBlockProofPackPath(block.SeqNo)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	if err := s.ensureCleanPackTail(path, s.pendingKeyProofSync, s.dirtyKeyProofPacks); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()

	ptr, err := packfile.Append(file, packfile.EntryName(packEntryKindForProofKind(kind), block), data, false)
	if err != nil {
		s.markDirtyPackTail(s.dirtyKeyProofPacks, path)
		return nil, err
	}
	s.markPendingPackSync(s.pendingKeyProofSync, path)
	return &storage.ArtifactRef{
		Path:   s.relativeArtifactPath(path),
		Offset: ptr.Offset,
		Size:   ptr.Size,
	}, nil
}

func (s *Store) copyProofRefToKeyPack(kind storage.ServedProofKind, block ton.BlockIDExt, ref *storage.ArtifactRef) (*storage.ArtifactRef, error) {
	data, err := s.readArtifactRef(context.Background(), ref)
	if err != nil {
		return nil, err
	}
	return s.appendKeyBlockProofEntry(kind, block, data)
}

func (s *Store) archivePackPath(seqno uint32, workchain int32, shard int64) string {
	group := seqno / 100000
	name := fmt.Sprintf("archive.%05d", seqno)
	if workchain != -1 || shard != topShard {
		name = fmt.Sprintf("%s.%d_%016x", name, workchain, uint64(shard))
	}
	return filepath.Join(
		s.dir,
		"archive",
		"packages",
		fmt.Sprintf("arch%04d", group),
		name+".pack",
	)
}

func (s *Store) keyBlockProofPackPath(seqno uint32) string {
	packageID := seqno - seqno%keyArchiveMasterchainBlocks
	group := packageID / 1000000
	return filepath.Join(
		s.dir,
		"archive",
		"packages",
		fmt.Sprintf("key%03d", group),
		fmt.Sprintf("key.archive.%06d.pack", packageID),
	)
}

type archiveEntryLocation struct {
	archiveID  int64
	baseSeqno  uint32
	sliceSeqno uint32
	workchain  int32
	shard      int64
}

func (s *Store) archiveEntryLocation(block ton.BlockIDExt, meta *storage.BlockMeta, splitDepth uint32) (archiveEntryLocation, error) {
	masterSeqno, err := archiveEntryMasterSeqno(block, meta)
	if err != nil {
		return archiveEntryLocation{}, err
	}

	isKey := block.Workchain == -1 && block.Shard == topShard && meta != nil && meta.Has(storage.BlockMetaIsKeyBlock)
	baseSeqno, err := s.archivePackageBaseSeqno(masterSeqno, isKey)
	if err != nil {
		return archiveEntryLocation{}, err
	}
	sliceSeqno := archiveSliceSeqno(baseSeqno, masterSeqno)
	workchain, shard := archiveEntryShardPrefix(block, splitDepth)

	return archiveEntryLocation{
		archiveID:  archivePackID(baseSeqno, sliceSeqno, workchain, shard),
		baseSeqno:  baseSeqno,
		sliceSeqno: sliceSeqno,
		workchain:  workchain,
		shard:      shard,
	}, nil
}

func archiveEntryMasterSeqno(block ton.BlockIDExt, meta *storage.BlockMeta) (uint32, error) {
	if block.Workchain == -1 && block.Shard == topShard {
		return block.SeqNo, nil
	}
	if meta == nil || meta.MasterchainRef == nil {
		return 0, fmt.Errorf("masterchain reference is required for archive shard block %s", storage.FormatBlockRef(block))
	}
	return meta.MasterchainRef.SeqNo, nil
}

func archiveEntryFirstMaster(block ton.BlockIDExt, meta *storage.BlockMeta) (uint32, uint32, uint64, error) {
	seqno, err := archiveEntryMasterSeqno(block, meta)
	if err != nil {
		return 0, 0, 0, err
	}
	var utime uint32
	var lt uint64
	if meta != nil {
		utime = meta.GenUTime
		lt = meta.StartLT
		if lt == 0 {
			lt = meta.EndLT
		}
	}
	return seqno, utime, lt, nil
}

func archiveEntryShardPrefix(block ton.BlockIDExt, splitDepth uint32) (int32, int64) {
	if block.Workchain == -1 && block.Shard == topShard {
		return -1, topShard
	}
	return block.Workchain, archivePackShardPrefix(block.Workchain, block.Shard, splitDepth)
}

func archivePackShardPrefix(workchain int32, shard int64, splitDepth uint32) int64 {
	if workchain == -1 && shard == topShard {
		return topShard
	}
	if splitDepth == 0 {
		return shard
	}
	if splitDepth >= 63 {
		return int64(uint64(shard) | 1)
	}

	value := uint64(shard) | 1
	mask := ^uint64(0) << (64 - splitDepth)
	return int64((value & mask) | (uint64(1) << (63 - splitDepth)))
}

func archiveSliceSeqno(baseSeqno uint32, masterSeqno uint32) uint32 {
	if masterSeqno <= baseSeqno {
		return baseSeqno
	}
	return baseSeqno + ((masterSeqno-baseSeqno)/archiveSliceMasterchainBlocks)*archiveSliceMasterchainBlocks
}

func archivePackID(baseSeqno uint32, sliceSeqno uint32, workchain int32, shard int64) int64 {
	idx := archivePackIndex(baseSeqno, sliceSeqno, workchain, shard)
	return int64(uint64(idx)<<32 | uint64(baseSeqno))
}

func archivePackIndex(baseSeqno uint32, sliceSeqno uint32, workchain int32, shard int64) uint32 {
	sliceIndex := uint32(0)
	if sliceSeqno > baseSeqno {
		sliceIndex = (sliceSeqno - baseSeqno) / archiveSliceMasterchainBlocks
	}
	idx := sliceIndex * archivePackIndexSliceStride
	if workchain == -1 && shard == topShard {
		return idx
	}
	return idx + 1 + archiveShardIndex(workchain, shard)
}

const (
	archivePackIndexSliceStride     = 1 << 20
	archivePackIndexMaxShardDepth   = 12
	archivePackIndexWorkchainStride = 1 << (archivePackIndexMaxShardDepth + 1)
)

func archiveShardIndex(workchain int32, shard int64) uint32 {
	workchainOffset := uint32(workchain+1) * archivePackIndexWorkchainStride
	depth := archiveShardPrefixLength(shard)
	if depth <= 0 {
		return workchainOffset
	}
	if depth > archivePackIndexMaxShardDepth {
		depth = archivePackIndexMaxShardDepth
	}

	prefix := uint64(shard) >> (64 - depth)
	shardOffset := (uint32(1) << uint(depth)) - 1 + uint32(prefix)
	return workchainOffset + shardOffset
}

func archiveShardPrefixLength(shard int64) int {
	value := uint64(shard)
	if value == 0 {
		return 0
	}
	return 63 - bits.TrailingZeros64(value)
}

func isShardSplitNextLinkConflict(prev ton.BlockIDExt, existing ton.BlockIDExt, next ton.BlockIDExt) bool {
	if prev.Workchain == -1 || existing.Workchain != prev.Workchain || next.Workchain != prev.Workchain {
		return false
	}
	if prev.SeqNo == ^uint32(0) || existing.SeqNo != prev.SeqNo+1 || next.SeqNo != prev.SeqNo+1 {
		return false
	}
	if existing.Shard == next.Shard {
		return false
	}
	return archiveShardIsDirectChild(prev.Shard, existing.Shard) && archiveShardIsDirectChild(prev.Shard, next.Shard)
}

func archiveShardIsDirectChild(parent int64, child int64) bool {
	parentDepth := archiveShardPrefixLength(parent)
	childDepth := archiveShardPrefixLength(child)
	if childDepth != parentDepth+1 {
		return false
	}
	return archiveShardIntersects(parent, child)
}

func archiveShardIntersects(left int64, right int64) bool {
	leftDepth := archiveShardPrefixLength(left)
	rightDepth := archiveShardPrefixLength(right)
	depth := leftDepth
	if rightDepth < depth {
		depth = rightDepth
	}
	if depth <= 0 {
		return true
	}

	mask := ^uint64(0) << (64 - depth)
	return uint64(left)&mask == uint64(right)&mask
}

func (s *Store) archivePackageBaseSeqno(masterSeqno uint32, isKey bool) (uint32, error) {
	if isKey {
		return masterSeqno, nil
	}

	rounded := masterSeqno - masterSeqno%archivePackageMasterchainBlocks
	latest, err := s.latestArchivePackageStart(masterSeqno)
	if errors.Is(err, storage.ErrNotFound) {
		return rounded, nil
	}
	if err != nil {
		return 0, err
	}
	if latest > rounded {
		return latest, nil
	}
	return rounded, nil
}

func (s *Store) latestArchivePackageStart(masterSeqno uint32) (uint32, error) {
	db, err := s.acquireHotDB(context.Background())
	if err != nil {
		return 0, err
	}
	defer s.releaseHotDB()

	upper := hotKeyArchivePackageStart(masterSeqno + 1)
	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: hotKeyArchivePackageStartPrefix(),
		UpperBound: upper,
	})
	if err != nil {
		return 0, err
	}
	defer func() { _ = iter.Close() }()

	if !iter.Last() {
		if err = iter.Error(); err != nil {
			return 0, err
		}
		return 0, storage.ErrNotFound
	}
	key := iter.Key()
	if len(key) != len(hotPrefixPackStart)+4 || !bytes.HasPrefix(key, hotPrefixPackStart) {
		return 0, storage.ErrNotFound
	}
	return binary.BigEndian.Uint32(key[len(hotPrefixPackStart):]), nil
}

func (s *Store) registerArchivePacks(batch *pebble.Batch, registrations []archivePackRegistration) error {
	for _, reg := range registrations {
		if !reg.valid {
			continue
		}
		if err := s.setArchiveFileRef(batch, reg.archiveID, &storage.ArtifactRef{
			Path: reg.path,
			Size: reg.size,
		}); err != nil {
			return err
		}
		if err := batch.Set(hotKeyArchivePackageStart(reg.baseSeq), []byte{1}, pebble.NoSync); err != nil {
			return err
		}
		if err := s.setArchivePackageMeta(batch, reg); err != nil {
			return err
		}
		if err := s.registerArchiveInfoRange(batch, reg.startSeq, reg.workchain, reg.shard, reg.archiveID); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) setArchivePackageMeta(batch *pebble.Batch, reg archivePackRegistration) error {
	meta := archivePackageMeta{
		archiveID:        reg.archiveID,
		baseSeq:          reg.baseSeq,
		startSeq:         reg.startSeq,
		workchain:        reg.workchain,
		shard:            reg.shard,
		path:             reg.path,
		size:             reg.size,
		firstMasterSeq:   reg.firstMasterSeq,
		firstMasterUTime: reg.firstMasterUTime,
		firstMasterLT:    reg.firstMasterLT,
	}

	key := hotKeyArchivePackage(reg.archiveID)
	old, closer, err := pebbleReaderGet(s.hot, key)
	if err == nil {
		defer func() { _ = closer.Close() }()
		current, err := decodeArchivePackageMeta(old)
		if err != nil {
			return err
		}
		if current.path != meta.path {
			return fmt.Errorf("archive package path mismatch archive_id=%d old=%s new=%s", reg.archiveID, current.path, meta.path)
		}
		if current.baseSeq != meta.baseSeq || current.startSeq != meta.startSeq || current.workchain != meta.workchain || current.shard != meta.shard {
			return fmt.Errorf("archive package descriptor mismatch archive_id=%d", reg.archiveID)
		}
		if current.size > meta.size {
			meta.size = current.size
		}
		if current.firstMasterSeq != 0 && (meta.firstMasterSeq == 0 || current.firstMasterSeq < meta.firstMasterSeq) {
			meta.firstMasterSeq = current.firstMasterSeq
			meta.firstMasterUTime = current.firstMasterUTime
			meta.firstMasterLT = current.firstMasterLT
		} else if current.firstMasterSeq == meta.firstMasterSeq {
			if meta.firstMasterUTime == 0 {
				meta.firstMasterUTime = current.firstMasterUTime
			}
			if meta.firstMasterLT == 0 {
				meta.firstMasterLT = current.firstMasterLT
			}
		}
	} else if !errors.Is(err, storage.ErrNotFound) {
		return err
	}
	return batch.Set(key, encodeArchivePackageMeta(meta), pebble.NoSync)
}

func (s *Store) setArchiveFileRef(batch *pebble.Batch, archiveID int64, ref *storage.ArtifactRef) error {
	key := hotKeyArchiveFile(archiveID)
	old, closer, err := pebbleReaderGet(s.hot, key)
	if err == nil {
		defer func() { _ = closer.Close() }()
		current, err := decodeArtifactRef(old)
		if err != nil {
			return err
		}
		if current.Path != ref.Path {
			return fmt.Errorf("archive file ref path mismatch archive_id=%d old=%s new=%s", archiveID, current.Path, ref.Path)
		}
		if current.Size >= ref.Size {
			return nil
		}
	} else if !errors.Is(err, storage.ErrNotFound) {
		return err
	}
	return batch.Set(key, encodeArtifactRef(ref), pebble.NoSync)
}

func (s *Store) registerArchiveInfoRange(batch *pebble.Batch, startSeq uint32, workchain int32, shard int64, archiveID int64) error {
	for i := uint32(0); i < archiveSliceMasterchainBlocks; i++ {
		seqno := startSeq + i
		if err := batch.Set(hotKeyArchiveInfo(int32(seqno), workchain, shard), encodeInt64(archiveID), pebble.NoSync); err != nil {
			return err
		}
	}
	return nil
}

func isKeyProofKind(kind storage.ServedProofKind) bool {
	return kind == storage.ServedProofKeyBlock || kind == storage.ServedProofKeyBlockLink
}

func blockMetaFlagsForProofKind(kind storage.ServedProofKind) storage.BlockMetaFlags {
	flags := storage.BlockMetaFlagForProof(kind)
	if isKeyProofKind(kind) {
		flags |= storage.BlockMetaIsKeyBlock
	}
	return flags
}

func packEntryKindForProofKind(kind storage.ServedProofKind) string {
	switch kind {
	case storage.ServedProofBlockLink, storage.ServedProofKeyBlockLink:
		return packfile.KindProofLink
	default:
		return packfile.KindProof
	}
}

func (s *Store) writeStateArtifactFile(block ton.BlockIDExt, data []byte) (*storage.ArtifactRef, error) {
	name, err := storage.ZeroStateFileName(block)
	if err != nil {
		return nil, err
	}
	dir := s.StateFilesDir()
	if err = os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	tmpDir := filepath.Join(s.dir, "archive", "tmp")
	if err = os.MkdirAll(tmpDir, 0o755); err != nil {
		return nil, err
	}
	tmp, err := os.CreateTemp(tmpDir, name+".*.tmp")
	if err != nil {
		return nil, err
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()

	if _, err = tmp.Write(data); err != nil {
		_ = tmp.Close()
		return nil, err
	}
	if err = tmp.Sync(); err != nil {
		_ = tmp.Close()
		return nil, err
	}
	if err = tmp.Close(); err != nil {
		return nil, err
	}

	finalPath := filepath.Join(dir, name)
	if err = os.Rename(tmpPath, finalPath); err != nil {
		return nil, err
	}
	if err = syncDir(dir); err != nil {
		return nil, err
	}
	return &storage.ArtifactRef{
		Path: s.relativeArtifactPath(finalPath),
		Size: int64(len(data)),
	}, nil
}

func (s *Store) markPendingPackSync(pending map[string]uint64, path string) {
	s.artifactSyncSeq++
	pending[path] = s.artifactSyncSeq
}

func (s *Store) commitSyncedPackSizes(items []pendingPackSync) error {
	if len(items) == 0 {
		return nil
	}

	return s.withHotBatch(func(batch *pebble.Batch) error {
		for _, item := range items {
			path := s.relativeArtifactPath(item.path)
			if err := s.setPackCommittedSize(batch, path, item.size); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) setPackCommittedSize(batch *pebble.Batch, path string, size int64) error {
	key := hotKeyPackCommitted(path)
	old, closer, err := pebbleReaderGet(s.hot, key)
	if err == nil {
		defer func() { _ = closer.Close() }()
		if len(old) != 8 {
			return fmt.Errorf("invalid committed pack size record")
		}
		if int64(binary.BigEndian.Uint64(old)) >= size {
			return nil
		}
	} else if !errors.Is(err, storage.ErrNotFound) {
		return err
	}
	return batch.Set(key, encodeInt64(size), pebble.NoSync)
}

func (s *Store) packCommittedSize(path string) (int64, error) {
	raw, closer, err := pebbleReaderGet(s.hot, hotKeyPackCommitted(path))
	if errors.Is(err, storage.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer func() { _ = closer.Close() }()
	if len(raw) != 8 {
		return 0, fmt.Errorf("invalid committed pack size record")
	}
	return int64(binary.BigEndian.Uint64(raw)), nil
}

func (s *Store) ensureCleanPackTail(path string, pending map[string]uint64, dirty map[string]struct{}) error {
	if _, ok := pending[path]; ok {
		return nil
	}
	if _, ok := dirty[path]; !ok {
		return nil
	}

	if err := s.truncateUncommittedPackTail(path); err != nil {
		return err
	}
	delete(dirty, path)
	return nil
}

func (s *Store) truncateUncommittedPackTail(path string) error {
	if path == "" {
		return nil
	}

	stat, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}

	committedSize, err := s.packCommittedSize(s.relativeArtifactPath(path))
	if err != nil {
		return err
	}
	if stat.Size() <= committedSize {
		return nil
	}

	if err := os.Truncate(path, committedSize); err != nil {
		return fmt.Errorf("truncate uncommitted pack %s from %d to %d: %w", path, stat.Size(), committedSize, err)
	}
	return nil
}

func (s *Store) markDirtyPackTail(dirty map[string]struct{}, path string) {
	if path == "" {
		return
	}
	dirty[path] = struct{}{}
}

func isMissingArtifactError(err error) bool {
	return errors.Is(err, os.ErrNotExist) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

func (s *Store) reconcileCommittedArtifactFiles() error {
	committed := map[string]int64{}
	iter, err := s.hot.NewIter(&pebble.IterOptions{
		LowerBound: hotKeyPackCommittedPrefix(),
		UpperBound: appendPrefixUpperBound(hotPrefixPackCommitted),
	})
	if err != nil {
		return err
	}
	defer func() { _ = iter.Close() }()

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		value := iter.Value()
		if len(value) != 8 {
			return fmt.Errorf("invalid committed pack size record")
		}
		relPath := string(key[len(hotPrefixPackCommitted):])
		size := int64(binary.BigEndian.Uint64(value))
		committed[relPath] = size
		path := s.artifactPath(relPath)
		stat, err := os.Stat(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return err
		}
		if stat.Size() > size {
			if err = os.Truncate(path, size); err != nil {
				return fmt.Errorf("truncate committed pack %s to %d: %w", path, size, err)
			}
		}
	}
	if err = iter.Error(); err != nil {
		return err
	}
	return s.removeUncommittedPackFiles(committed)
}

func (s *Store) removeUncommittedPackFiles(committed map[string]int64) error {
	root := filepath.Join(s.dir, "archive", "packages")
	return filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".pack") {
			return nil
		}
		rel := s.relativeArtifactPath(path)
		if _, ok := committed[rel]; ok {
			return nil
		}
		return os.Remove(path)
	})
}

func appendPrefixUpperBound(prefix []byte) []byte {
	upper := bytes.Clone(prefix)
	for i := len(upper) - 1; i >= 0; i-- {
		if upper[i] != 0xff {
			upper[i]++
			return upper[:i+1]
		}
	}
	return nil
}

type archivePackStoreResult struct {
	stored         bool
	reusedExisting bool
}

func storeArchivePack(src string, dst string) (archivePackStoreResult, error) {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return archivePackStoreResult{}, err
	}
	if filepath.Clean(src) == filepath.Clean(dst) {
		return archivePackStoreResult{}, validateArchivePack(dst)
	}

	if err := validateArchivePack(src); err != nil {
		return archivePackStoreResult{}, err
	}
	if _, err := os.Stat(dst); err == nil {
		merged, err := mergeArchivePack(src, dst)
		if err != nil {
			return archivePackStoreResult{}, err
		}
		return archivePackStoreResult{stored: merged, reusedExisting: true}, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return archivePackStoreResult{}, err
	}
	if err := os.Rename(src, dst); err == nil {
		return archivePackStoreResult{stored: true}, validateArchivePack(dst)
	}

	tmp, err := os.CreateTemp(filepath.Dir(dst), filepath.Base(dst)+".*.tmp")
	if err != nil {
		return archivePackStoreResult{}, err
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()
	if err = tmp.Close(); err != nil {
		return archivePackStoreResult{}, err
	}

	if err = copyFileSync(src, tmpPath); err != nil {
		return archivePackStoreResult{}, err
	}
	if err = validateArchivePack(tmpPath); err != nil {
		return archivePackStoreResult{}, err
	}
	if err = os.Rename(tmpPath, dst); err != nil {
		return archivePackStoreResult{}, err
	}
	if err = os.Remove(src); err != nil && !errors.Is(err, os.ErrNotExist) {
		return archivePackStoreResult{}, err
	}
	return archivePackStoreResult{stored: true}, nil
}

func mergeArchivePack(src string, dst string) (bool, error) {
	if err := validateArchivePack(dst); err != nil {
		return false, err
	}

	existing, err := archivePackEntryNames(dst)
	if err != nil {
		return false, err
	}

	out, err := os.OpenFile(dst, os.O_RDWR, 0o644)
	if err != nil {
		return false, err
	}
	defer func() { _ = out.Close() }()
	stat, err := out.Stat()
	if err != nil {
		return false, err
	}
	originalSize := stat.Size()

	in, err := os.Open(src)
	if err != nil {
		return false, err
	}
	defer func() { _ = in.Close() }()

	merged := false
	if err = packfile.Read(context.Background(), in, func(entry packfile.Entry) error {
		if _, ok := existing[entry.Name]; ok {
			return nil
		}
		if _, err := packfile.Append(out, entry.Name, entry.Data, false); err != nil {
			return err
		}
		existing[entry.Name] = struct{}{}
		merged = true
		return nil
	}); err != nil {
		return false, err
	}
	if merged {
		if err = out.Sync(); err != nil {
			if truncateErr := out.Truncate(originalSize); truncateErr != nil {
				return false, errors.Join(err, fmt.Errorf("rollback archive pack merge %s to %d: %w", dst, originalSize, truncateErr))
			}
			return false, err
		}
	}
	if err = os.Remove(src); err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	return merged, nil
}

func archivePackEntryNames(path string) (map[string]struct{}, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()

	names := map[string]struct{}{}
	if err = packfile.Read(context.Background(), file, func(entry packfile.Entry) error {
		names[entry.Name] = struct{}{}
		return nil
	}); err != nil {
		return nil, err
	}
	return names, nil
}

func validateArchivePack(path string) error {
	return packfile.Validate(path)
}

func copyFileSync(src string, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer func() { _ = out.Close() }()

	if _, err = io.Copy(out, in); err != nil {
		return err
	}
	return out.Sync()
}
