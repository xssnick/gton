package service

import (
	"context"
	"errors"
	"testing"

	"github.com/xssnick/gton/service/storage"
)

func TestEnsurePersistentStateSerializationDiskSpacePassesWhenEnoughSpace(t *testing.T) {
	store := &testPreviousPersistentStatePruneStore{}
	state := newTestStateLifecycle(store, StateLifecycleOptions{
		StorageDir:                         "/db",
		MinStateSerializationDiskFreeBytes: 30 << 30,
	})
	probe := func(path string) (syncDiskSpaceStatus, error) {
		return syncDiskSpaceStatus{Path: path, AvailableBytes: 30 << 30}, nil
	}

	if err := state.ensurePersistentStateSerializationDiskSpace(context.Background(), testBlockID(-1, topShard, 100), probe); err != nil {
		t.Fatalf("ensure disk space: %v", err)
	}
	if store.calls != 0 {
		t.Fatalf("emergency prune calls = %d, want 0", store.calls)
	}
}

func TestEnsurePersistentStateSerializationDiskSpacePrunesPreviousState(t *testing.T) {
	store := &testPreviousPersistentStatePruneStore{
		stats: storage.PersistentStatePruneStats{
			DeletedMasterSeqno: 99,
			DeletedFileRecords: 2,
			DeletedDiskFiles:   2,
			DeletedDiskBytes:   42 << 20,
		},
	}
	probes := []uint64{1 << 30, 31 << 30}
	state := newTestStateLifecycle(store, StateLifecycleOptions{
		StorageDir:                         "/db",
		MinStateSerializationDiskFreeBytes: 30 << 30,
	})
	probe := func(path string) (syncDiskSpaceStatus, error) {
		available := probes[0]
		probes = probes[1:]
		return syncDiskSpaceStatus{Path: path, AvailableBytes: available}, nil
	}

	target := testBlockID(-1, topShard, 100)
	if err := state.ensurePersistentStateSerializationDiskSpace(context.Background(), target, probe); err != nil {
		t.Fatalf("ensure disk space: %v", err)
	}
	if store.calls != 1 {
		t.Fatalf("emergency prune calls = %d, want 1", store.calls)
	}
	if store.beforeSeqno != target.SeqNo {
		t.Fatalf("prune before seqno = %d, want %d", store.beforeSeqno, target.SeqNo)
	}
}

func TestEnsurePersistentStateSerializationDiskSpaceFailsAfterPrune(t *testing.T) {
	store := &testPreviousPersistentStatePruneStore{}
	state := newTestStateLifecycle(store, StateLifecycleOptions{
		StorageDir:                         "/db",
		MinStateSerializationDiskFreeBytes: 30 << 30,
	})
	probe := func(path string) (syncDiskSpaceStatus, error) {
		return syncDiskSpaceStatus{Path: path, AvailableBytes: 1 << 30}, nil
	}

	err := state.ensurePersistentStateSerializationDiskSpace(context.Background(), testBlockID(-1, topShard, 100), probe)
	if !errors.Is(err, errStateSerializationLowDiskSpace) {
		t.Fatalf("ensure disk space error = %v, want low disk space", err)
	}
	if store.calls != 1 {
		t.Fatalf("emergency prune calls = %d, want 1", store.calls)
	}
}

func TestEnsurePersistentStateSerializationDiskSpaceKeepsAllStates(t *testing.T) {
	store := &testPreviousPersistentStatePruneStore{}
	state := newTestStateLifecycle(store, StateLifecycleOptions{
		StorageDir:                         "/db",
		MinStateSerializationDiskFreeBytes: 30 << 30,
	})
	state.maintenance.persistentStateKeepRecent = PersistentStateKeepAll
	probe := func(path string) (syncDiskSpaceStatus, error) {
		return syncDiskSpaceStatus{Path: path, AvailableBytes: 1 << 30}, nil
	}

	err := state.ensurePersistentStateSerializationDiskSpace(context.Background(), testBlockID(-1, topShard, 100), probe)
	if !errors.Is(err, errStateSerializationLowDiskSpace) {
		t.Fatalf("ensure disk space error = %v, want low disk space", err)
	}
	if store.calls != 0 {
		t.Fatalf("emergency prune calls = %d, want 0", store.calls)
	}
}

func TestEnforcePersistentStateRetentionAfterSerializationUsesDefault(t *testing.T) {
	store := &testPreviousPersistentStatePruneStore{}
	state := newTestStateLifecycle(store, StateLifecycleOptions{})
	target := testBlockID(-1, topShard, 100)

	state.enforcePersistentStateRetentionAfterSerialization(context.Background(), target, PersistentStateSerializationAll)

	if store.retentionCalls != 1 {
		t.Fatalf("persistent state retention calls = %d, want 1", store.retentionCalls)
	}
	if store.throughSeqno != target.SeqNo {
		t.Fatalf("persistent state retention through seqno = %d, want %d", store.throughSeqno, target.SeqNo)
	}
	if store.keepRecentGroups != DefaultPersistentStateKeepRecent {
		t.Fatalf("persistent state retention keep recent = %d, want %d", store.keepRecentGroups, DefaultPersistentStateKeepRecent)
	}
}

func TestEnforcePersistentStateRetentionAfterSerializationSkipsPartialScope(t *testing.T) {
	store := &testPreviousPersistentStatePruneStore{}
	state := newTestStateLifecycle(store, StateLifecycleOptions{})

	state.enforcePersistentStateRetentionAfterSerialization(context.Background(), testBlockID(-1, topShard, 100), PersistentStateSerializationBasechain)

	if store.retentionCalls != 0 {
		t.Fatalf("persistent state retention calls = %d, want 0", store.retentionCalls)
	}
}

func TestPrunePersistentStatesAfterSerializationKeepsAll(t *testing.T) {
	store := &testPreviousPersistentStatePruneStore{}
	state := newTestStateLifecycle(store, StateLifecycleOptions{})
	state.maintenance.persistentStateKeepRecent = PersistentStateKeepAll

	if err := state.prunePersistentStatesAfterSerialization(context.Background(), testBlockID(-1, topShard, 100)); err != nil {
		t.Fatalf("prune persistent states after serialization: %v", err)
	}
	if store.retentionCalls != 0 {
		t.Fatalf("persistent state retention calls = %d, want 0", store.retentionCalls)
	}
}

type testPreviousPersistentStatePruneStore struct {
	testStorage
	stats            storage.PersistentStatePruneStats
	calls            int
	beforeSeqno      uint32
	retentionCalls   int
	throughSeqno     uint32
	keepRecentGroups int
}

func (s *testPreviousPersistentStatePruneStore) PrunePreviousPersistentStateFiles(_ context.Context, beforeMasterSeqno uint32) (storage.PersistentStatePruneStats, error) {
	s.calls++
	s.beforeSeqno = beforeMasterSeqno
	return s.stats, nil
}

func (s *testPreviousPersistentStatePruneStore) PrunePersistentStateFilesToLimit(_ context.Context, throughMasterSeqno uint32, keepRecentGroups int) (storage.PersistentStatePruneStats, error) {
	s.retentionCalls++
	s.throughSeqno = throughMasterSeqno
	s.keepRecentGroups = keepRecentGroups
	return s.stats, nil
}

var _ testStorage = (*testPreviousPersistentStatePruneStore)(nil)
