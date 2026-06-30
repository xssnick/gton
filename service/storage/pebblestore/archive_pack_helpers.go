package pebblestore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
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

type archivePackDescriptor struct {
	baseSeqno  uint32
	startSeqno uint32
	workchain  int32
	shard      int64
}

type archivePackClaims struct {
	byID         map[int64]archivePackDescriptor
	byDescriptor map[archivePackDescriptor]int64
	nextIndex    map[uint32]uint32
	loadedBase   map[uint32]struct{}
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
	seq            uint64
	cleanSize      int64
	metricBaseSize int64
	size           int64
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
			s.observeArchivePackageBytes(current.size - current.metricBaseSize)
		}
	}
}

func packSizeBeforeAppend(path string) (int64, error) {
	stat, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("stat pack before append %s: %w", path, err)
	}
	return stat.Size(), nil
}

func (s *Store) deleteSyncedPackAppendMarkers(batch *pebble.Batch, packs syncedArtifactPacks) error {
	for _, item := range packs.archive {
		if err := s.deletePackAppendDirty(batch, item.path); err != nil {
			return err
		}
	}
	for _, item := range packs.keyProof {
		if err := s.deletePackAppendDirty(batch, item.path); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) abandonPendingArtifactPacks() {
	s.artifactMu.Lock()
	s.abandonPendingPackFilesLocked(s.pendingArchiveSync)
	s.abandonPendingPackFilesLocked(s.pendingKeyProofSync)
	s.artifactMu.Unlock()
}

func (s *Store) abandonPendingPackFilesLocked(pending map[string]pendingPackWrite) {
	for path, write := range pending {
		s.discardPackTail(path, write.cleanSize, "abandoned")
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
	claimedArchiveIDs := newArchivePackClaims()
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
		location, err := s.archiveEntryLocationForBase(request.block, request.splitDepth, masterSeqno, baseSeqno, claimedArchiveIDs)
		if err != nil {
			return nil, nil, err
		}

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
		if err := s.ensureCleanPackTail(path, s.pendingArchiveSync); err != nil {
			return nil, nil, err
		}
		metricBaseSize, err := packSizeBeforeAppend(path)
		if err != nil {
			return nil, nil, err
		}

		file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
		if err != nil {
			return nil, nil, err
		}

		appender, err := packfile.NewAppender(file)
		if err != nil {
			_ = file.Close()
			return nil, nil, err
		}
		cleanSize := appender.Size()
		relPath, cleanSize, err := s.beginPackAppend(s.pendingArchiveSync, path, cleanSize)
		if err != nil {
			_ = file.Close()
			return nil, nil, err
		}

		groupReg := archivePackRegistration{}
		for idx < len(records) && records[idx].path == path {
			record := records[idx]
			ptr, appendErr := appender.Append(packfile.EntryName(record.request.kind, record.request.block), record.request.data)
			if appendErr != nil {
				_ = file.Close()
				s.discardPackTail(path, cleanSize, "failed append")
				return nil, nil, appendErr
			}

			ref := &storage.ArtifactRef{
				Path:             relPath,
				ArchivePackage:   true,
				ArchivePackageID: record.location.archiveID,
				Offset:           ptr.Offset,
				Size:             ptr.Size,
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
				s.discardPackTail(path, cleanSize, "failed append registration")
				return nil, nil, mergeErr
			}
			idx++
		}

		size := appender.Size()
		closeErr := file.Close()
		if closeErr != nil {
			s.discardPackTail(path, cleanSize, "failed close")
			return nil, nil, closeErr
		}

		groupReg.size = size
		registrations = append(registrations, groupReg)
		s.markPendingPackSync(s.pendingArchiveSync, path, cleanSize, metricBaseSize, size)
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
	if err := s.ensureCleanPackTail(path, s.pendingKeyProofSync); err != nil {
		return nil, err
	}
	metricBaseSize, err := packSizeBeforeAppend(path)
	if err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}

	appender, err := packfile.NewAppender(file)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	cleanSize := appender.Size()
	_, cleanSize, err = s.beginPackAppend(s.pendingKeyProofSync, path, cleanSize)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	ptr, err := appender.Append(packfile.EntryName(packEntryKindForProofKind(kind), block), data)
	if err != nil {
		_ = file.Close()
		s.discardPackTail(path, cleanSize, "failed append")
		return nil, err
	}
	size := appender.Size()
	if err = file.Close(); err != nil {
		s.discardPackTail(path, cleanSize, "failed close")
		return nil, err
	}
	s.markPendingPackSync(s.pendingKeyProofSync, path, cleanSize, metricBaseSize, size)
	return &storage.ArtifactRef{
		ArchivePackage:   true,
		ArchivePackageID: keyProofPackageRefID(keyProofPackageID(block.SeqNo)),
		Offset:           ptr.Offset,
		Size:             ptr.Size,
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
	packageID := keyProofPackageID(seqno)
	group := packageID / 1000000
	return filepath.Join(
		s.dir,
		"archive",
		"packages",
		fmt.Sprintf("key%03d", group),
		fmt.Sprintf("key.archive.%06d.pack", packageID),
	)
}

func keyProofPackageID(seqno uint32) uint32 {
	return seqno - seqno%keyArchiveMasterchainBlocks
}

func keyProofPackageRefID(packageID uint32) int64 {
	return -1 - int64(packageID)
}

func keyProofPackageIDFromRef(refID int64) int64 {
	return -1 - refID
}

type archiveEntryLocation struct {
	archiveID  int64
	baseSeqno  uint32
	sliceSeqno uint32
	workchain  int32
	shard      int64
}

func (s *Store) archiveEntryLocationForBase(block ton.BlockIDExt, splitDepth uint32, masterSeqno uint32, baseSeqno uint32, claimed *archivePackClaims) (archiveEntryLocation, error) {
	desc := archiveEntryDescriptorForBase(block, splitDepth, masterSeqno, baseSeqno)
	archiveID, err := s.archivePackIDForDescriptor(desc, claimed)
	if err != nil {
		return archiveEntryLocation{}, err
	}
	return archiveEntryLocation{
		archiveID:  archiveID,
		baseSeqno:  desc.baseSeqno,
		sliceSeqno: desc.startSeqno,
		workchain:  desc.workchain,
		shard:      desc.shard,
	}, nil
}

func archiveEntryDescriptorForBase(block ton.BlockIDExt, splitDepth uint32, masterSeqno uint32, baseSeqno uint32) archivePackDescriptor {
	sliceSeqno := archiveSliceSeqno(baseSeqno, masterSeqno)
	workchain, shard := archiveEntryShardPrefix(block, splitDepth)

	return archivePackDescriptor{
		baseSeqno:  baseSeqno,
		startSeqno: sliceSeqno,
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
	if meta == nil || !meta.MasterchainRefKnown() {
		return 0, fmt.Errorf("masterchain reference is required for archive shard block %s", storage.FormatBlockRef(block))
	}
	return meta.MasterchainRefSeqno, nil
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
	return storage.ShardPrefix(shard, splitDepth)
}

func archiveSliceSeqno(baseSeqno uint32, masterSeqno uint32) uint32 {
	if masterSeqno <= baseSeqno {
		return baseSeqno
	}
	return baseSeqno + ((masterSeqno-baseSeqno)/archiveSliceMasterchainBlocks)*archiveSliceMasterchainBlocks
}

const archiveFirstDynamicPackageIndex uint32 = 1

func (s *Store) archivePackIDForDescriptor(desc archivePackDescriptor, claimed *archivePackClaims) (int64, error) {
	if err := s.loadArchivePackClaimsBase(claimed, desc.baseSeqno); err != nil {
		return 0, err
	}
	return archivePackIDFromClaims(desc, claimed)
}

func archivePackIDFromClaims(desc archivePackDescriptor, claimed *archivePackClaims) (int64, error) {
	if archiveID, ok := claimed.archiveID(desc); ok {
		return archiveID, nil
	}

	if archivePackDescriptorIsBaseMaster(desc) {
		id := archivePackageID(desc.baseSeqno, 0)
		if !claimed.claim(id, desc) {
			return 0, archivePackClaimError(claimed, id, desc)
		}
		return id, nil
	}

	next := claimed.nextPackageIndex(desc.baseSeqno)
	for index := next; index >= archiveFirstDynamicPackageIndex; index++ {
		id := archivePackageID(desc.baseSeqno, index)
		if claimed.claim(id, desc) {
			claimed.advancePackageIndex(desc.baseSeqno, index)
			return id, nil
		}
		if index == ^uint32(0) {
			break
		}
	}
	return 0, fmt.Errorf("archive package index exhausted for base=%d start=%d wc=%d shard=%016x", desc.baseSeqno, desc.startSeqno, desc.workchain, uint64(desc.shard))
}

func (s *Store) loadArchivePackClaimsBase(claimed *archivePackClaims, baseSeqno uint32) error {
	if claimed.baseLoaded(baseSeqno) {
		return nil
	}

	db, err := s.acquireHotDB(context.Background())
	if err != nil {
		return err
	}
	defer s.releaseHotDB()

	prefix := hotKeyArchiveInfoBasePrefix(baseSeqno)
	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return err
	}
	defer func() { _ = iter.Close() }()

	nextIndex := archiveFirstDynamicPackageIndex
	for valid := iter.First(); valid; valid = iter.Next() {
		desc, err := decodeArchiveInfoKey(iter.Key())
		if err != nil {
			return err
		}
		raw := iter.Value()
		if len(raw) != 8 {
			return fmt.Errorf("invalid archive info payload")
		}

		archiveID := int64(binary.BigEndian.Uint64(raw))
		if !claimed.claim(archiveID, desc) {
			return archivePackClaimError(claimed, archiveID, desc)
		}
		if index := archivePackageIndex(archiveID); index >= archiveFirstDynamicPackageIndex && index != ^uint32(0) {
			next := index + 1
			if next > nextIndex {
				nextIndex = next
			}
		}
	}
	if err = iter.Error(); err != nil {
		return err
	}

	storedNext, err := s.nextArchivePackageIndex(baseSeqno)
	if err != nil {
		return err
	}
	if storedNext > nextIndex {
		nextIndex = storedNext
	}
	claimed.markBaseLoaded(baseSeqno, nextIndex)
	return nil
}

func newArchivePackClaims() *archivePackClaims {
	return &archivePackClaims{
		byID:         make(map[int64]archivePackDescriptor),
		byDescriptor: make(map[archivePackDescriptor]int64),
		nextIndex:    make(map[uint32]uint32),
		loadedBase:   make(map[uint32]struct{}),
	}
}

func (c *archivePackClaims) archiveID(desc archivePackDescriptor) (int64, bool) {
	archiveID, ok := c.byDescriptor[desc]
	return archiveID, ok
}

func (c *archivePackClaims) claim(id int64, desc archivePackDescriptor) bool {
	if archiveID, ok := c.byDescriptor[desc]; ok {
		return archiveID == id
	}
	if used, ok := c.byID[id]; ok && used != desc {
		return false
	}
	c.byID[id] = desc
	c.byDescriptor[desc] = id
	return true
}

func (c *archivePackClaims) baseLoaded(baseSeqno uint32) bool {
	_, ok := c.loadedBase[baseSeqno]
	return ok
}

func (c *archivePackClaims) markBaseLoaded(baseSeqno uint32, nextIndex uint32) {
	c.loadedBase[baseSeqno] = struct{}{}
	c.nextIndex[baseSeqno] = nextIndex
}

func (c *archivePackClaims) nextPackageIndex(baseSeqno uint32) uint32 {
	next, ok := c.nextIndex[baseSeqno]
	if !ok || next < archiveFirstDynamicPackageIndex {
		return archiveFirstDynamicPackageIndex
	}
	return next
}

func (c *archivePackClaims) advancePackageIndex(baseSeqno uint32, index uint32) {
	if index == ^uint32(0) {
		return
	}
	next := index + 1
	if next > c.nextIndex[baseSeqno] {
		c.nextIndex[baseSeqno] = next
	}
}

func archivePackClaimError(claimed *archivePackClaims, id int64, desc archivePackDescriptor) error {
	if used, ok := claimed.byID[id]; ok && used != desc {
		return fmt.Errorf("archive package id collision archive_id=%d old={%s} new={%s}", id, archivePackDescriptorString(used), archivePackDescriptorString(desc))
	}
	if existing, ok := claimed.byDescriptor[desc]; ok && existing != id {
		return fmt.Errorf("archive package descriptor already assigned archive_id=%d new_archive_id=%d desc={%s}", existing, id, archivePackDescriptorString(desc))
	}
	return fmt.Errorf("archive package id claim failed archive_id=%d desc={%s}", id, archivePackDescriptorString(desc))
}

func archivePackDescriptorString(desc archivePackDescriptor) string {
	return fmt.Sprintf("base=%d start=%d wc=%d shard=%016x", desc.baseSeqno, desc.startSeqno, desc.workchain, uint64(desc.shard))
}

func decodeArchiveInfoKey(key []byte) (archivePackDescriptor, error) {
	size := len(hotPrefixArchiveInfo) + 4 + 4 + 4 + 8
	if len(key) != size || !bytes.HasPrefix(key, hotPrefixArchiveInfo) {
		return archivePackDescriptor{}, fmt.Errorf("invalid archive info key")
	}

	pos := len(hotPrefixArchiveInfo)
	desc := archivePackDescriptor{
		baseSeqno:  binary.BigEndian.Uint32(key[pos : pos+4]),
		startSeqno: binary.BigEndian.Uint32(key[pos+4 : pos+8]),
		workchain:  int32(binary.BigEndian.Uint32(key[pos+8 : pos+12])),
		shard:      int64(binary.BigEndian.Uint64(key[pos+12 : pos+20])),
	}
	return desc, nil
}

func (s *Store) nextArchivePackageIndex(baseSeqno uint32) (uint32, error) {
	raw, closer, err := pebbleReaderGet(s.hot, hotKeyArchivePackageIndex(baseSeqno))
	if errors.Is(err, storage.ErrNotFound) {
		return archiveFirstDynamicPackageIndex, nil
	}
	if err != nil {
		return 0, err
	}
	defer func() { _ = closer.Close() }()
	if len(raw) != 4 {
		return 0, fmt.Errorf("invalid archive package index payload")
	}
	next := binary.BigEndian.Uint32(raw)
	if next < archiveFirstDynamicPackageIndex {
		return 0, fmt.Errorf("invalid archive package next index %d for base=%d", next, baseSeqno)
	}
	return next, nil
}

func (s *Store) setArchivePackageNextIndex(batch *pebble.Batch, baseSeqno uint32, next uint32) error {
	current, err := s.nextArchivePackageIndex(baseSeqno)
	if err != nil {
		return err
	}
	if next < archiveFirstDynamicPackageIndex {
		return fmt.Errorf("invalid archive package next index %d for base=%d", next, baseSeqno)
	}
	if current >= next {
		return nil
	}
	return batch.Set(hotKeyArchivePackageIndex(baseSeqno), binary.BigEndian.AppendUint32(nil, next), pebble.NoSync)
}

func archivePackageID(baseSeqno uint32, index uint32) int64 {
	return int64(uint64(index)<<32 | uint64(baseSeqno))
}

func archivePackageIndex(archiveID int64) uint32 {
	return uint32(uint64(archiveID) >> 32)
}

func archivePackDescriptorIsBaseMaster(desc archivePackDescriptor) bool {
	return desc.startSeqno == desc.baseSeqno && desc.workchain == -1 && desc.shard == topShard
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
	if !storage.ShardIsDirectChild(prev.Shard, existing.Shard) || !storage.ShardIsDirectChild(prev.Shard, next.Shard) {
		return ton.BlockIDExt{}, false
	}

	leftShard := storage.ShardChild(prev.Shard, true)
	if existing.Shard == leftShard {
		return existing, true
	}
	if next.Shard == leftShard {
		return next, true
	}
	return ton.BlockIDExt{}, false
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
	nextPackageIndex := make(map[uint32]uint32)
	for _, reg := range registrations {
		if _, ok := packageStarts[reg.baseSeq]; !ok {
			if err := s.setArchivePackageStart(batch, reg.baseSeq); err != nil {
				return err
			}
			packageStarts[reg.baseSeq] = struct{}{}
		}
		if err := s.setArchivePackageMeta(batch, reg); err != nil {
			return err
		}
		if err := s.registerArchiveInfo(batch, reg.baseSeq, reg.startSeq, reg.workchain, reg.shard, reg.archiveID); err != nil {
			return err
		}
		if index := archivePackageIndex(reg.archiveID); index >= archiveFirstDynamicPackageIndex {
			next := index + 1
			if next > nextPackageIndex[reg.baseSeq] {
				nextPackageIndex[reg.baseSeq] = next
			}
		}
	}
	for baseSeqno, next := range nextPackageIndex {
		if err := s.setArchivePackageNextIndex(batch, baseSeqno, next); err != nil {
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

func (s *Store) archivePackageMetaByID(ctx context.Context, archiveID int64) (archivePackageMeta, error) {
	s.archivePackagesMu.RLock()
	if meta, ok := s.archivePackages[archiveID]; ok {
		s.archivePackagesMu.RUnlock()
		return meta, nil
	}
	s.archivePackagesMu.RUnlock()

	raw, err := s.getHotCopy(ctx, hotKeyArchivePackage(archiveID))
	if err != nil {
		return archivePackageMeta{}, err
	}
	meta, err := decodeArchivePackageMeta(raw)
	if err != nil {
		return archivePackageMeta{}, err
	}

	s.archivePackagesMu.Lock()
	s.archivePackages[archiveID] = meta
	s.archivePackagesMu.Unlock()
	return meta, nil
}

func (s *Store) deleteArchivePackageCache(ids map[int64]archivePackageMeta) {
	if len(ids) == 0 {
		return
	}
	s.archivePackagesMu.Lock()
	defer s.archivePackagesMu.Unlock()
	for id := range ids {
		delete(s.archivePackages, id)
	}
}

func (s *Store) deleteArchivePackageRegistrationCache(registrations []archivePackRegistration) {
	if len(registrations) == 0 {
		return
	}

	s.archivePackagesMu.Lock()
	defer s.archivePackagesMu.Unlock()
	for _, reg := range registrations {
		if !reg.valid {
			continue
		}
		delete(s.archivePackages, reg.archiveID)
	}
}

func (s *Store) registerArchiveInfo(batch *pebble.Batch, baseSeq uint32, startSeq uint32, workchain int32, shard int64, archiveID int64) error {
	registered, err := s.archiveInfoRegistered(baseSeq, startSeq, workchain, shard, archiveID)
	if err != nil {
		return err
	}
	if registered {
		return nil
	}

	return batch.Set(hotKeyArchiveInfo(baseSeq, startSeq, workchain, shard), encodeInt64(archiveID), pebble.NoSync)
}

func (s *Store) archiveInfoRegistered(baseSeq uint32, startSeq uint32, workchain int32, shard int64, archiveID int64) (bool, error) {
	raw, closer, err := pebbleReaderGet(s.hot, hotKeyArchiveInfo(baseSeq, startSeq, workchain, shard))
	if errors.Is(err, storage.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer func() { _ = closer.Close() }()

	if len(raw) != 8 {
		return false, fmt.Errorf("invalid archive info payload")
	}
	return int64(binary.BigEndian.Uint64(raw)) == archiveID, nil
}

func isKeyProofKind(kind storage.ServedProofKind) bool {
	return kind == storage.ServedProofKeyBlock || kind == storage.ServedProofKeyBlockLink
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
	relPath, err := s.relativeArtifactPath(finalPath)
	if err != nil {
		return nil, err
	}
	return &storage.ArtifactRef{
		Path: relPath,
		Size: int64(len(data)),
	}, nil
}

func (s *Store) markPendingPackSync(pending map[string]pendingPackWrite, path string, cleanSize int64, metricBaseSize int64, size int64) {
	s.artifactSyncSeq++
	if current, ok := pending[path]; ok {
		cleanSize = current.cleanSize
		metricBaseSize = current.metricBaseSize
	}
	pending[path] = pendingPackWrite{
		seq:            s.artifactSyncSeq,
		cleanSize:      cleanSize,
		metricBaseSize: metricBaseSize,
		size:           size,
	}
}

func (s *Store) beginPackAppend(pending map[string]pendingPackWrite, path string, cleanSize int64) (string, int64, error) {
	if current, ok := pending[path]; ok {
		relPath, err := s.packJournalPath(path)
		if err != nil {
			return "", 0, err
		}
		return relPath, current.cleanSize, nil
	}

	relPath, err := s.markPackAppendDirty(path, cleanSize)
	if err != nil {
		return "", 0, err
	}
	return relPath, cleanSize, nil
}

func (s *Store) archivePackArtifactPath(path string) (string, bool) {
	if path == "" {
		return "", false
	}
	if filepath.IsAbs(path) {
		relPath, err := s.relativeArtifactPath(path)
		if err != nil {
			return "", false
		}
		path = relPath
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

func (s *Store) ensureCleanPackTail(path string, pending map[string]pendingPackWrite) error {
	if _, ok := pending[path]; ok {
		return nil
	}
	return s.recoverDirtyPackPath(path)
}

func (s *Store) discardPackTail(path string, cleanSize int64, reason string) {
	if err := s.truncateUncommittedPackTail(path, cleanSize); err != nil {
		s.log.Warn().
			Err(err).
			Str("path", path).
			Str("reason", reason).
			Int64("clean_size", cleanSize).
			Msg("failed to truncate uncommitted pack tail")
		return
	}
	if err := s.clearPackAppendDirty(path); err != nil {
		s.log.Warn().
			Err(err).
			Str("path", path).
			Str("reason", reason).
			Int64("clean_size", cleanSize).
			Msg("failed to clear dirty pack marker")
		return
	}
}

func (s *Store) truncateUncommittedPackTail(path string, cleanSize int64) error {
	if path == "" {
		return nil
	}
	if cleanSize < 0 {
		return fmt.Errorf("invalid clean pack size path=%s size=%d", path, cleanSize)
	}

	if cleanSize == 0 {
		return removePackFile(path)
	}

	stat, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("dirty pack %s is missing", path)
	}
	if err != nil {
		return err
	}

	if stat.Size() < cleanSize {
		return fmt.Errorf("dirty pack %s size %d is below clean size %d", path, stat.Size(), cleanSize)
	}
	if stat.Size() == cleanSize {
		return nil
	}

	if err := os.Truncate(path, cleanSize); err != nil {
		return fmt.Errorf("truncate uncommitted pack %s from %d to %d: %w", path, stat.Size(), cleanSize, err)
	}
	if err := syncFile(path); err != nil {
		return fmt.Errorf("sync truncated pack %s: %w", path, err)
	}
	return nil
}

func isMissingArtifactError(err error) bool {
	return errors.Is(err, os.ErrNotExist) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}
