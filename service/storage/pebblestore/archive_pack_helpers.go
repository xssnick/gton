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
	"sync"
	"syscall"

	"github.com/cockroachdb/pebble/v2"
	"github.com/xssnick/gton/service/archive/packfile"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
)

const (
	topShard                = int64(-1 << 63)
	artifactPackSyncWorkers = 4
)

type pathSyncFunc func(string) error

type artifactSyncTask struct {
	index int
	path  string
}

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

type archiveAppendRequest struct {
	kind       string
	block      ton.BlockIDExt
	meta       *storage.BlockMeta
	splitDepth uint32
	data       []byte
}

type archiveAppendRecord struct {
	request          archiveAppendRequest
	requestIndex     int
	location         archiveEntryLocation
	firstMasterSeq   uint32
	firstMasterUTime uint32
	firstMasterLT    uint64
	path             string
}

type pendingPackSync struct {
	path string
	seq  uint64
	size int64
}

type pendingPackWrite struct {
	seq  uint64
	size int64
}

type syncedArtifactPacks struct {
	archive  []pendingPackSync
	keyProof []pendingPackSync
}

func (p syncedArtifactPacks) empty() bool {
	return len(p.archive) == 0 && len(p.keyProof) == 0
}

func (s *Store) syncPendingArtifactPacks() (syncedArtifactPacks, error) {
	archive, err := s.syncPendingPackFiles(s.pendingArchiveSync, "archive")
	if err != nil {
		return syncedArtifactPacks{}, err
	}
	keyProof, err := s.syncPendingPackFiles(s.pendingKeyProofSync, "key proof")
	if err != nil {
		return syncedArtifactPacks{}, err
	}
	return syncedArtifactPacks{
		archive:  archive,
		keyProof: keyProof,
	}, nil
}

func (s *Store) syncPendingPackFiles(pending map[string]pendingPackWrite, label string) ([]pendingPackSync, error) {
	s.artifactMu.Lock()
	if len(pending) == 0 {
		s.artifactMu.Unlock()
		return nil, nil
	}

	items := make([]pendingPackSync, 0, len(pending))
	for path, write := range pending {
		items = append(items, pendingPackSync{
			path: path,
			seq:  write.seq,
			size: write.size,
		})
	}
	s.artifactMu.Unlock()

	sort.Slice(items, func(i, j int) bool {
		return items[i].path < items[j].path
	})

	dirs := make(map[string]struct{}, len(items))
	paths := make([]string, 0, len(items))
	for _, item := range items {
		paths = append(paths, item.path)
		dirs[filepath.Dir(item.path)] = struct{}{}
	}

	err := syncPathsParallel(paths, artifactPackSyncWorkers, func(path string) error {
		if err := syncFile(path); err != nil {
			return fmt.Errorf("sync %s pack %s: %w", label, path, err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	dirPaths := make([]string, 0, len(dirs))
	for dir := range dirs {
		dirPaths = append(dirPaths, dir)
	}
	sort.Strings(dirPaths)
	err = syncPathsParallel(dirPaths, artifactPackSyncWorkers, func(path string) error {
		if err := syncDir(path); err != nil {
			return fmt.Errorf("sync %s pack dir %s: %w", label, path, err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return items, nil
}

func syncPathsParallel(paths []string, workerLimit int, syncPath pathSyncFunc) error {
	if len(paths) == 0 {
		return nil
	}
	if workerLimit <= 1 || len(paths) == 1 {
		for _, path := range paths {
			if err := syncPath(path); err != nil {
				return err
			}
		}
		return nil
	}

	workers := workerLimit
	if len(paths) < workers {
		workers = len(paths)
	}

	tasks := make(chan artifactSyncTask)
	errs := make([]error, len(paths))
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range tasks {
				errs[task.index] = syncPath(task.path)
			}
		}()
	}

	for index, path := range paths {
		tasks <- artifactSyncTask{index: index, path: path}
	}
	close(tasks)
	wg.Wait()

	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) clearSyncedArtifactPacks(packs syncedArtifactPacks) {
	if packs.empty() {
		return
	}
	s.artifactMu.Lock()
	s.clearSyncedPackFilesLocked(s.pendingArchiveSync, packs.archive)
	s.clearSyncedPackFilesLocked(s.pendingKeyProofSync, packs.keyProof)
	s.artifactMu.Unlock()
}

func (s *Store) clearSyncedPackFilesLocked(pending map[string]pendingPackWrite, items []pendingPackSync) {
	for _, item := range items {
		if current, ok := pending[item.path]; ok && current.seq == item.seq {
			delete(pending, item.path)
		}
	}
}

func (s *Store) abandonPendingArtifactPacks() {
	s.artifactMu.Lock()
	s.abandonPendingPackFilesLocked(s.pendingArchiveSync, s.dirtyArchivePacks)
	s.abandonPendingPackFilesLocked(s.pendingKeyProofSync, s.dirtyKeyProofPacks)
	s.artifactMu.Unlock()
}

func (s *Store) abandonPendingPackFilesLocked(pending map[string]pendingPackWrite, dirty map[string]struct{}) {
	for path := range pending {
		s.markDirtyPackTail(dirty, path)
		delete(pending, path)
	}
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

func (s *Store) appendArchiveEntries(requests []archiveAppendRequest) ([]*storage.ArtifactRef, []archivePackRegistration, error) {
	if len(requests) == 0 {
		return nil, nil, nil
	}

	pendingStarts := archiveAppendPackageStarts(requests)
	baseCache := make(map[uint32]uint32)
	baseSeqnoFor := func(masterSeqno uint32, isKey bool) (uint32, error) {
		if isKey {
			return masterSeqno, nil
		}
		if cached, ok := baseCache[masterSeqno]; ok {
			return cached, nil
		}

		baseSeqno, err := s.archivePackageBaseSeqno(masterSeqno, false)
		if err != nil {
			return 0, err
		}
		if pending, ok := latestPendingArchivePackageStart(pendingStarts, masterSeqno); ok && pending > baseSeqno {
			baseSeqno = pending
		}
		baseCache[masterSeqno] = baseSeqno
		return baseSeqno, nil
	}

	records := make([]archiveAppendRecord, 0, len(requests))
	for idx, request := range requests {
		masterSeqno, err := archiveEntryMasterSeqno(request.block, request.meta)
		if err != nil {
			return nil, nil, err
		}
		isKey := archiveEntryIsKey(request.block, request.meta)
		baseSeqno, err := baseSeqnoFor(masterSeqno, isKey)
		if err != nil {
			return nil, nil, err
		}
		location := archiveEntryLocationForBase(request.block, request.splitDepth, masterSeqno, baseSeqno)

		firstMasterSeq, firstMasterUTime, firstMasterLT, err := archiveEntryFirstMaster(request.block, request.meta)
		if err != nil {
			return nil, nil, err
		}
		records = append(records, archiveAppendRecord{
			request:          request,
			requestIndex:     idx,
			location:         location,
			firstMasterSeq:   firstMasterSeq,
			firstMasterUTime: firstMasterUTime,
			firstMasterLT:    firstMasterLT,
			path:             s.archivePackPath(location.sliceSeqno, location.workchain, location.shard),
		})
	}

	sort.SliceStable(records, func(i, j int) bool {
		if records[i].path != records[j].path {
			return records[i].path < records[j].path
		}
		return records[i].requestIndex < records[j].requestIndex
	})

	refs := make([]*storage.ArtifactRef, len(requests))
	registrations := make([]archivePackRegistration, 0, len(records))

	s.artifactMu.Lock()
	defer s.artifactMu.Unlock()

	for idx := 0; idx < len(records); {
		path := records[idx].path
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, nil, err
		}
		if err := s.ensureCleanPackTail(path, s.pendingArchiveSync, s.dirtyArchivePacks); err != nil {
			return nil, nil, err
		}

		file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
		if err != nil {
			return nil, nil, err
		}

		appender, err := packfile.NewAppender(file)
		if err != nil {
			_ = file.Close()
			s.markDirtyPackTail(s.dirtyArchivePacks, path)
			return nil, nil, err
		}

		groupReg := archivePackRegistration{}
		for idx < len(records) && records[idx].path == path {
			record := records[idx]
			ptr, appendErr := appender.Append(packfile.EntryName(record.request.kind, record.request.block), record.request.data)
			if appendErr != nil {
				_ = file.Close()
				s.markDirtyPackTail(s.dirtyArchivePacks, path)
				return nil, nil, appendErr
			}

			ref := &storage.ArtifactRef{
				Path:   s.relativeArtifactPath(path),
				Offset: ptr.Offset,
				Size:   ptr.Size,
			}
			refs[record.requestIndex] = ref

			reg := archivePackRegistration{
				valid:            true,
				archiveID:        record.location.archiveID,
				path:             ref.Path,
				baseSeq:          record.location.baseSeqno,
				startSeq:         record.location.sliceSeqno,
				workchain:        record.location.workchain,
				shard:            record.location.shard,
				firstMasterSeq:   record.firstMasterSeq,
				firstMasterUTime: record.firstMasterUTime,
				firstMasterLT:    record.firstMasterLT,
			}
			if !groupReg.valid {
				groupReg = reg
			} else if mergeErr := mergeArchivePackRegistration(&groupReg, reg); mergeErr != nil {
				_ = file.Close()
				s.markDirtyPackTail(s.dirtyArchivePacks, path)
				return nil, nil, mergeErr
			}
			idx++
		}

		size := appender.Size()
		closeErr := file.Close()
		if closeErr != nil {
			s.markDirtyPackTail(s.dirtyArchivePacks, path)
			return nil, nil, closeErr
		}

		groupReg.size = size
		registrations = append(registrations, groupReg)
		s.markPendingPackSync(s.pendingArchiveSync, path, size)
	}

	return refs, registrations, nil
}

func archiveAppendPackageStarts(requests []archiveAppendRequest) []uint32 {
	starts := make([]uint32, 0, len(requests))
	seen := make(map[uint32]struct{})
	for _, request := range requests {
		if !archiveEntryIsKey(request.block, request.meta) {
			continue
		}
		seqno := request.block.SeqNo
		if _, ok := seen[seqno]; ok {
			continue
		}
		seen[seqno] = struct{}{}
		starts = append(starts, seqno)
	}
	sort.Slice(starts, func(i, j int) bool {
		return starts[i] < starts[j]
	})
	return starts
}

func latestPendingArchivePackageStart(starts []uint32, masterSeqno uint32) (uint32, bool) {
	idx := sort.Search(len(starts), func(i int) bool {
		return starts[i] > masterSeqno
	})
	if idx == 0 {
		return 0, false
	}
	return starts[idx-1], true
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

	appender, err := packfile.NewAppender(file)
	if err != nil {
		_ = file.Close()
		s.markDirtyPackTail(s.dirtyKeyProofPacks, path)
		return nil, err
	}
	ptr, err := appender.Append(packfile.EntryName(packEntryKindForProofKind(kind), block), data)
	if err != nil {
		_ = file.Close()
		s.markDirtyPackTail(s.dirtyKeyProofPacks, path)
		return nil, err
	}
	size := appender.Size()
	if err = file.Close(); err != nil {
		s.markDirtyPackTail(s.dirtyKeyProofPacks, path)
		return nil, err
	}
	s.markPendingPackSync(s.pendingKeyProofSync, path, size)
	return &storage.ArtifactRef{
		Path:   s.relativeArtifactPath(path),
		Offset: ptr.Offset,
		Size:   ptr.Size,
	}, nil
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

	baseSeqno, err := s.archivePackageBaseSeqno(masterSeqno, archiveEntryIsKey(block, meta))
	if err != nil {
		return archiveEntryLocation{}, err
	}
	return archiveEntryLocationForBase(block, splitDepth, masterSeqno, baseSeqno), nil
}

func archiveEntryLocationForBase(block ton.BlockIDExt, splitDepth uint32, masterSeqno uint32, baseSeqno uint32) archiveEntryLocation {
	sliceSeqno := archiveSliceSeqno(baseSeqno, masterSeqno)
	workchain, shard := archiveEntryShardPrefix(block, splitDepth)

	return archiveEntryLocation{
		archiveID:  archivePackID(baseSeqno, sliceSeqno, workchain, shard),
		baseSeqno:  baseSeqno,
		sliceSeqno: sliceSeqno,
		workchain:  workchain,
		shard:      shard,
	}
}

func archiveEntryIsKey(block ton.BlockIDExt, meta *storage.BlockMeta) bool {
	return block.Workchain == -1 && block.Shard == topShard && meta != nil && meta.Has(storage.BlockMetaIsKeyBlock)
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

func selectShardSplitNextLink(prev ton.BlockIDExt, existing ton.BlockIDExt, next ton.BlockIDExt) (ton.BlockIDExt, bool) {
	if prev.Workchain == -1 || existing.Workchain != prev.Workchain || next.Workchain != prev.Workchain {
		return ton.BlockIDExt{}, false
	}
	if prev.SeqNo == ^uint32(0) || existing.SeqNo != prev.SeqNo+1 || next.SeqNo != prev.SeqNo+1 {
		return ton.BlockIDExt{}, false
	}
	if existing.Shard == next.Shard {
		return ton.BlockIDExt{}, false
	}
	if !archiveShardIsDirectChild(prev.Shard, existing.Shard) || !archiveShardIsDirectChild(prev.Shard, next.Shard) {
		return ton.BlockIDExt{}, false
	}

	leftShard := archiveShardChild(prev.Shard, true)
	if existing.Shard == leftShard {
		return existing, true
	}
	if next.Shard == leftShard {
		return next, true
	}
	return ton.BlockIDExt{}, false
}

func archiveShardIsDirectChild(parent int64, child int64) bool {
	parentDepth := archiveShardPrefixLength(parent)
	childDepth := archiveShardPrefixLength(child)
	if childDepth != parentDepth+1 {
		return false
	}
	return archiveShardIntersects(parent, child)
}

func archiveShardChild(shard int64, left bool) int64 {
	value := uint64(shard)
	bit := value & -value
	step := bit >> 1
	if left {
		return int64(value - step)
	}
	return int64(value + step)
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
	registrations, err := mergeArchivePackRegistrations(registrations)
	if err != nil {
		return err
	}

	packageStarts := make(map[uint32]struct{}, len(registrations))
	for _, reg := range registrations {
		if err := s.setArchiveFileRef(batch, reg.archiveID, &storage.ArtifactRef{
			Path: reg.path,
			Size: reg.size,
		}); err != nil {
			return err
		}
		if _, ok := packageStarts[reg.baseSeq]; !ok {
			if err := s.setArchivePackageStart(batch, reg.baseSeq); err != nil {
				return err
			}
			packageStarts[reg.baseSeq] = struct{}{}
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

func mergeArchivePackRegistrations(registrations []archivePackRegistration) ([]archivePackRegistration, error) {
	merged := make([]archivePackRegistration, 0, len(registrations))
	byArchiveID := make(map[int64]int, len(registrations))
	for _, reg := range registrations {
		if !reg.valid {
			continue
		}

		idx, ok := byArchiveID[reg.archiveID]
		if !ok {
			byArchiveID[reg.archiveID] = len(merged)
			merged = append(merged, reg)
			continue
		}
		if err := mergeArchivePackRegistration(&merged[idx], reg); err != nil {
			return nil, err
		}
	}
	return merged, nil
}

func mergeArchivePackRegistration(dst *archivePackRegistration, next archivePackRegistration) error {
	if dst.path != next.path ||
		dst.baseSeq != next.baseSeq ||
		dst.startSeq != next.startSeq ||
		dst.workchain != next.workchain ||
		dst.shard != next.shard {
		return fmt.Errorf("archive package descriptor mismatch archive_id=%d old={%s} new={%s}", next.archiveID, archivePackRegistrationDescriptor(*dst), archivePackRegistrationDescriptor(next))
	}

	if next.size > dst.size {
		dst.size = next.size
	}
	mergeArchivePackFirstMaster(dst, next)
	return nil
}

func mergeArchivePackFirstMaster(dst *archivePackRegistration, next archivePackRegistration) {
	if next.firstMasterSeq != 0 && (dst.firstMasterSeq == 0 || next.firstMasterSeq < dst.firstMasterSeq) {
		dst.firstMasterSeq = next.firstMasterSeq
		dst.firstMasterUTime = next.firstMasterUTime
		dst.firstMasterLT = next.firstMasterLT
		return
	}
	if next.firstMasterSeq == dst.firstMasterSeq {
		if dst.firstMasterUTime == 0 {
			dst.firstMasterUTime = next.firstMasterUTime
		}
		if dst.firstMasterLT == 0 {
			dst.firstMasterLT = next.firstMasterLT
		}
	}
}

func archivePackRegistrationDescriptor(reg archivePackRegistration) string {
	return fmt.Sprintf("path=%s base=%d start=%d wc=%d shard=%016x", reg.path, reg.baseSeq, reg.startSeq, reg.workchain, uint64(reg.shard))
}

func archivePackageMetaDescriptor(meta archivePackageMeta) string {
	return fmt.Sprintf("path=%s base=%d start=%d wc=%d shard=%016x", meta.path, meta.baseSeq, meta.startSeq, meta.workchain, uint64(meta.shard))
}

func (s *Store) setArchivePackageStart(batch *pebble.Batch, seqno uint32) error {
	key := hotKeyArchivePackageStart(seqno)
	_, closer, err := pebbleReaderGet(s.hot, key)
	if err == nil {
		defer func() { _ = closer.Close() }()
		return nil
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return err
	}
	return batch.Set(key, []byte{1}, pebble.NoSync)
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
			return fmt.Errorf("archive package descriptor mismatch archive_id=%d old={%s} new={%s}", reg.archiveID, archivePackageMetaDescriptor(current), archivePackageMetaDescriptor(meta))
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
		encoded := encodeArchivePackageMeta(meta)
		if bytes.Equal(old, encoded) {
			return nil
		}
		return batch.Set(key, encoded, pebble.NoSync)
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
	registered, err := s.archiveInfoRangeRegistered(startSeq, workchain, shard, archiveID)
	if err != nil {
		return err
	}
	if registered {
		return nil
	}

	for i := uint32(0); i < archiveSliceMasterchainBlocks; i++ {
		seqno := startSeq + i
		if err := batch.Set(hotKeyArchiveInfo(int32(seqno), workchain, shard), encodeInt64(archiveID), pebble.NoSync); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) archiveInfoRangeRegistered(startSeq uint32, workchain int32, shard int64, archiveID int64) (bool, error) {
	for _, seqno := range []uint32{startSeq, startSeq + archiveSliceMasterchainBlocks - 1} {
		raw, closer, err := pebbleReaderGet(s.hot, hotKeyArchiveInfo(int32(seqno), workchain, shard))
		if errors.Is(err, storage.ErrNotFound) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if len(raw) != 8 {
			_ = closer.Close()
			return false, fmt.Errorf("invalid archive info payload")
		}
		storedArchiveID := int64(binary.BigEndian.Uint64(raw))
		_ = closer.Close()
		if storedArchiveID != archiveID {
			return false, nil
		}
	}
	return true, nil
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

func (s *Store) markPendingPackSync(pending map[string]pendingPackWrite, path string, size int64) {
	s.artifactSyncSeq++
	pending[path] = pendingPackWrite{
		seq:  s.artifactSyncSeq,
		size: size,
	}
}

func (s *Store) publishedArtifactFileSize(path string) (int64, error) {
	relPath, ok := s.archivePackArtifactPath(path)
	if !ok {
		return 0, fmt.Errorf("artifact path is not an archive pack: %s", path)
	}

	published, err := s.publishedArtifactFileSizes()
	if err != nil {
		return 0, err
	}
	return published[relPath], nil
}

func (s *Store) publishedArtifactFileSizes() (map[string]int64, error) {
	published := map[string]int64{}
	for _, prefix := range [][]byte{
		hotPrefixArchiveFile,
		hotPrefixBlockDataRef,
		hotPrefixProofRef,
		hotPrefixKeyProofRef,
	} {
		if err := s.collectPublishedArtifactRefs(published, prefix); err != nil {
			return nil, err
		}
	}
	if err := s.collectPublishedArchivePackageMetas(published); err != nil {
		return nil, err
	}
	return published, nil
}

func (s *Store) collectPublishedArtifactRefs(published map[string]int64, prefix []byte) error {
	iter, err := s.hot.NewIter(&pebble.IterOptions{
		LowerBound: bytes.Clone(prefix),
		UpperBound: appendPrefixUpperBound(prefix),
	})
	if err != nil {
		return err
	}
	defer func() { _ = iter.Close() }()

	for iter.First(); iter.Valid(); iter.Next() {
		ref, err := decodeArtifactRef(iter.Value())
		if err != nil {
			return err
		}
		if err = s.recordPublishedArtifactRange(published, ref.Path, ref.Offset, ref.Size); err != nil {
			return err
		}
	}
	return iter.Error()
}

func (s *Store) collectPublishedArchivePackageMetas(published map[string]int64) error {
	iter, err := s.hot.NewIter(&pebble.IterOptions{
		LowerBound: hotKeyArchivePackagePrefix(),
		UpperBound: appendPrefixUpperBound(hotPrefixArchivePackage),
	})
	if err != nil {
		return err
	}
	defer func() { _ = iter.Close() }()

	for iter.First(); iter.Valid(); iter.Next() {
		meta, err := decodeArchivePackageMeta(iter.Value())
		if err != nil {
			return err
		}
		if err = s.recordPublishedArtifactRange(published, meta.path, 0, meta.size); err != nil {
			return err
		}
	}
	return iter.Error()
}

func (s *Store) recordPublishedArtifactRange(published map[string]int64, path string, offset int64, size int64) error {
	relPath, ok := s.archivePackArtifactPath(path)
	if !ok {
		return fmt.Errorf("artifact path is not an archive pack: %s", path)
	}
	if offset < 0 || size < 0 {
		return fmt.Errorf("invalid artifact range path=%s offset=%d size=%d", path, offset, size)
	}
	const maxInt64 = int64(^uint64(0) >> 1)
	if offset > maxInt64-size {
		return fmt.Errorf("artifact range overflows path=%s offset=%d size=%d", path, offset, size)
	}

	end := offset + size
	if end > published[relPath] {
		published[relPath] = end
	}
	return nil
}

func (s *Store) archivePackArtifactPath(path string) (string, bool) {
	if path == "" {
		return "", false
	}
	if filepath.IsAbs(path) {
		path = s.relativeArtifactPath(path)
	}

	clean := filepath.Clean(path)
	slashPath := filepath.ToSlash(clean)
	if strings.HasPrefix(slashPath, "../") || strings.HasPrefix(slashPath, "/") {
		return "", false
	}
	if !strings.HasPrefix(slashPath, "archive/packages/") || !strings.HasSuffix(slashPath, ".pack") {
		return "", false
	}
	return filepath.FromSlash(slashPath), true
}

func (s *Store) ensureCleanPackTail(path string, pending map[string]pendingPackWrite, dirty map[string]struct{}) error {
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

	publishedSize, err := s.publishedArtifactFileSize(path)
	if err != nil {
		return err
	}
	if stat.Size() <= publishedSize {
		return nil
	}

	if err := os.Truncate(path, publishedSize); err != nil {
		return fmt.Errorf("truncate uncommitted pack %s from %d to %d: %w", path, stat.Size(), publishedSize, err)
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

func (s *Store) reconcileArtifactPackFiles() error {
	published, err := s.publishedArtifactFileSizes()
	if err != nil {
		return err
	}

	for relPath, size := range published {
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
				return fmt.Errorf("truncate artifact pack %s to published size %d: %w", path, size, err)
			}
		}
	}
	return s.removeUncommittedPackFiles(published)
}

func (s *Store) removeUncommittedPackFiles(published map[string]int64) error {
	root := filepath.Join(s.dir, "archive", "packages")
	return filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".pack") {
			return nil
		}
		rel := s.relativeArtifactPath(path)
		if _, ok := published[rel]; ok {
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
