package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xssnick/gton/service/storage"
)

const (
	exclusiveServiceTaskMaxReadAmp = int64(6)
	exclusiveServiceTaskMaxLag     = 300 * time.Second

	CellGenerationSwitchMaxReadAmp = int64(12)
)

var (
	errPersistentStateGCActive         = errors.New("persistent state gc is running")
	errArchiveTTLGCActive              = errors.New("archive ttl gc is running")
	errExclusiveServiceTaskHighReadAmp = errors.New("db read amplification is too high")
	errExclusiveServiceTaskHighLag     = errors.New("sync lag is too high")
)

type exclusiveServiceTask string

const (
	exclusiveServiceTaskNone                    exclusiveServiceTask = ""
	exclusiveServiceTaskStateSerialization      exclusiveServiceTask = "state_serialization"
	exclusiveServiceTaskCellGenerationMigration exclusiveServiceTask = "cell_generation_migration"
	exclusiveServiceTaskPersistentStateGC       exclusiveServiceTask = "persistent_state_gc"
	exclusiveServiceTaskArchiveTTLGC            exclusiveServiceTask = "archive_ttl_gc"
)

type exclusiveServiceTaskLease struct {
	service *Service
	task    exclusiveServiceTask
}

type exclusiveServiceTaskReadAmpStore interface {
	MaxReadAmp(ctx context.Context) (int64, error)
}

func (s *Service) canStartExclusiveServiceTask(ctx context.Context, task exclusiveServiceTask) error {
	s.exclusiveTaskMu.Lock()
	err := s.canStartExclusiveServiceTaskLocked(task)
	s.exclusiveTaskMu.Unlock()
	if err != nil {
		return err
	}
	return s.canStartExclusiveServiceTaskLimits(ctx, task)
}

func (s *Service) beginExclusiveServiceTask(ctx context.Context, task exclusiveServiceTask) (*exclusiveServiceTaskLease, error) {
	s.exclusiveTaskMu.Lock()
	if err := s.canStartExclusiveServiceTaskLocked(task); err != nil {
		s.exclusiveTaskMu.Unlock()
		return nil, err
	}
	s.exclusiveTaskMu.Unlock()

	if err := s.canStartExclusiveServiceTaskLimits(ctx, task); err != nil {
		return nil, err
	}

	s.exclusiveTaskMu.Lock()
	defer s.exclusiveTaskMu.Unlock()
	if err := s.canStartExclusiveServiceTaskLocked(task); err != nil {
		return nil, err
	}
	s.exclusiveTask = task
	return &exclusiveServiceTaskLease{service: s, task: task}, nil
}

func (s *Service) canStartExclusiveServiceTaskLimits(ctx context.Context, task exclusiveServiceTask) error {
	if exclusiveServiceTaskIsCleanup(task) {
		return nil
	}
	if err := s.canStartExclusiveServiceTaskReadAmp(ctx); err != nil {
		return err
	}
	return s.canStartExclusiveServiceTaskLag(ctx, time.Now())
}

func exclusiveServiceTaskIsCleanup(task exclusiveServiceTask) bool {
	return task == exclusiveServiceTaskPersistentStateGC || task == exclusiveServiceTaskArchiveTTLGC
}

func (s *Service) canStartExclusiveServiceTaskReadAmp(ctx context.Context) error {
	store, ok := s.storage.(exclusiveServiceTaskReadAmpStore)
	if !ok {
		return nil
	}

	readAmp, err := store.MaxReadAmp(ctx)
	if err != nil {
		return fmt.Errorf("check db read amplification: %w", err)
	}
	if readAmp > exclusiveServiceTaskMaxReadAmp {
		return fmt.Errorf("%w: max=%d limit=%d", errExclusiveServiceTaskHighReadAmp, readAmp, exclusiveServiceTaskMaxReadAmp)
	}
	return nil
}

func (s *Service) canStartExclusiveServiceTaskLag(ctx context.Context, now time.Time) error {
	lag, err := s.exclusiveServiceTaskMaxLag(ctx, now)
	if errors.Is(err, storage.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if lag > exclusiveServiceTaskMaxLag {
		return fmt.Errorf("%w: max=%s limit=%s", errExclusiveServiceTaskHighLag, lag.Truncate(time.Second), exclusiveServiceTaskMaxLag)
	}
	return nil
}

func (s *Service) exclusiveServiceTaskMaxLag(ctx context.Context, now time.Time) (time.Duration, error) {
	s.currentStatusMu.RLock()
	current := storage.CloneCurrentState(s.currentStatus)
	s.currentStatusMu.RUnlock()
	if current == nil {
		if s.storage == nil {
			return 0, storage.ErrNotFound
		}
		var err error
		current, err = s.storage.CurrentState(ctx)
		if errors.Is(err, storage.ErrNotFound) {
			return 0, storage.ErrNotFound
		}
		if err != nil {
			return 0, fmt.Errorf("load current state for sync lag check: %w", err)
		}
	}

	var maxLag time.Duration
	known := false
	record := func(state *storage.BlockState) error {
		lag, err := exclusiveServiceTaskBlockLag(ctx, s.storage, state, now)
		if errors.Is(err, storage.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if !known || lag > maxLag {
			maxLag = lag
			known = true
		}
		return nil
	}

	if err := record(&current.Masterchain); err != nil {
		return 0, err
	}
	for _, shard := range current.Shards {
		if err := record(&shard); err != nil {
			return 0, err
		}
	}
	if !known {
		return 0, storage.ErrNotFound
	}
	return maxLag, nil
}

func exclusiveServiceTaskBlockLag(ctx context.Context, store storage.Storage, state *storage.BlockState, now time.Time) (time.Duration, error) {
	if state == nil {
		return 0, storage.ErrNotFound
	}
	if state.Parsed != nil && state.Parsed.GenUTime != 0 {
		return time.Duration(now.Unix()-int64(state.Parsed.GenUTime)) * time.Second, nil
	}
	if store == nil {
		return 0, storage.ErrNotFound
	}

	meta, err := store.BlockMeta(ctx, state.Block)
	if errors.Is(err, storage.ErrNotFound) {
		return 0, storage.ErrNotFound
	}
	if err != nil {
		return 0, fmt.Errorf("load block time for sync lag check %s: %w", storage.FormatBlockRef(state.Block), err)
	}
	if meta == nil || meta.GenUTime == 0 {
		return 0, storage.ErrNotFound
	}
	return time.Duration(now.Unix()-int64(meta.GenUTime)) * time.Second, nil
}

func (s *Service) canStartExclusiveServiceTaskLocked(task exclusiveServiceTask) error {
	if task == exclusiveServiceTaskNone {
		return fmt.Errorf("exclusive service task is empty")
	}
	if s.exclusiveTask == exclusiveServiceTaskNone {
		return nil
	}
	return exclusiveServiceTaskError(s.exclusiveTask)
}

func (s *Service) exclusiveServiceTaskActive(task exclusiveServiceTask) bool {
	s.exclusiveTaskMu.Lock()
	defer s.exclusiveTaskMu.Unlock()
	return s.exclusiveTask == task
}

func (s *Service) backgroundTaskStatus() string {
	if s == nil {
		return "idle"
	}

	s.exclusiveTaskMu.Lock()
	task := s.exclusiveTask
	s.exclusiveTaskMu.Unlock()
	return task.backgroundStatus()
}

func (t exclusiveServiceTask) backgroundStatus() string {
	switch t {
	case exclusiveServiceTaskNone:
		return "idle"
	case exclusiveServiceTaskStateSerialization:
		return "serializing state"
	case exclusiveServiceTaskCellGenerationMigration:
		return "migrating cell DB"
	case exclusiveServiceTaskPersistentStateGC:
		return "pruning persistent states"
	case exclusiveServiceTaskArchiveTTLGC:
		return "pruning archives"
	default:
		return string(t)
	}
}

func (l *exclusiveServiceTaskLease) release() {
	if l == nil || l.service == nil {
		return
	}

	l.service.exclusiveTaskMu.Lock()
	if l.service.exclusiveTask == l.task {
		l.service.exclusiveTask = exclusiveServiceTaskNone
	}
	l.service.exclusiveTaskMu.Unlock()
}

func exclusiveServiceTaskError(task exclusiveServiceTask) error {
	switch task {
	case exclusiveServiceTaskStateSerialization:
		return errStateSerializationRunning
	case exclusiveServiceTaskCellGenerationMigration:
		return errCellGenerationMigrationRunning
	case exclusiveServiceTaskPersistentStateGC:
		return errPersistentStateGCActive
	case exclusiveServiceTaskArchiveTTLGC:
		return errArchiveTTLGCActive
	default:
		return fmt.Errorf("exclusive service task %q is running", task)
	}
}
