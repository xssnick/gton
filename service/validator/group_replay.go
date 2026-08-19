package validator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xssnick/gton/service/hooks"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/tonutils-go/ton"
)

const (
	groupReplayMasterchainWorkchain = int32(-1)
	groupReplayMasterchainShard     = int64(-1 << 63)
)

type groupReplayBlockKey struct {
	Workchain int32
	Shard     int64
	Seqno     uint32
	RootHash  [32]byte
	FileHash  [32]byte
}

// GroupTracker is a stable concurrent handle to the bootstrapped validator
// group tracker. It also implements collator.LocalGroupSource without making
// local acquisition depend on validator startup ordering.
type GroupTracker struct {
	current atomic.Pointer[groups.Tracker]
	tracker *groups.Tracker
	ready   chan struct{}

	mu       sync.Mutex
	buffered map[groupReplayBlockKey]groups.BufferedMasterchainState
}

func newGroupTracker(options groups.TrackerOptions) (*GroupTracker, error) {
	tracker, err := groups.NewTracker(options)
	if err != nil {
		return nil, err
	}

	return &GroupTracker{
		tracker:  tracker,
		ready:    make(chan struct{}),
		buffered: make(map[groupReplayBlockKey]groups.BufferedMasterchainState),
	}, nil
}

func (h *GroupTracker) Snapshot() (*groups.Snapshot, error) {
	tracker := h.current.Load()
	if tracker == nil {
		return nil, groups.ErrNoSnapshot
	}

	return tracker.Snapshot()
}

// Project derives a speculative snapshot from the currently published
// tracker. It does not mutate the live validator-group state.
func (h *GroupTracker) Project(previous *groups.Snapshot, input groups.ApplyInput) (*groups.Snapshot, error) {
	tracker := h.current.Load()
	if tracker == nil {
		return nil, groups.ErrNoSnapshot
	}

	return tracker.Project(previous, input)
}

func (h *GroupTracker) WaitProject(
	ctx context.Context,
	previous *groups.Snapshot,
	input groups.ApplyInput,
) (*groups.Snapshot, error) {
	tracker := h.current.Load()
	if tracker == nil {
		select {
		case <-h.ready:
			tracker = h.current.Load()
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return tracker.WaitProject(ctx, previous, input)
}

// Apply advances the shared tracker after bootstrap.
func (h *GroupTracker) Apply(input groups.ApplyInput) (groups.ApplyResult, error) {
	tracker := h.current.Load()
	if tracker == nil {
		return groups.ApplyResult{}, groups.ErrNoSnapshot
	}

	return tracker.Apply(input)
}

// Bootstrap reconstructs the shared tracker once. Validator and collator
// lifecycles may both request bootstrap; the first complete replay publishes
// the tracker and later requests observe the same immutable snapshot.
func (h *GroupTracker) Bootstrap(
	ctx context.Context,
	history groups.MasterchainHistory,
	buffered []groups.BufferedMasterchainState,
	asOf time.Time,
) ([]groups.Transition, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.current.Load() != nil {
		return nil, nil
	}

	states := make([]groups.BufferedMasterchainState, 0, len(h.buffered)+len(buffered))
	for _, state := range h.buffered {
		states = append(states, state)
	}
	states = append(states, buffered...)

	transitions, err := h.tracker.Bootstrap(ctx, history, states, asOf)
	if err != nil {
		return nil, err
	}
	h.current.Store(h.tracker)
	close(h.ready)
	clear(h.buffered)

	return transitions, nil
}

func (h *GroupTracker) applyOrBuffer(event hooks.BlockAppliedEvent) (groups.ApplyResult, bool, error) {
	tracker := h.current.Load()
	if tracker == nil {
		h.mu.Lock()
		tracker = h.current.Load()
		if tracker == nil {
			err := h.bufferLocked(event)
			h.mu.Unlock()

			return groups.ApplyResult{}, true, err
		}
		h.mu.Unlock()
	}

	result, err := tracker.Apply(groups.ApplyInput{
		Block: event.Meta.ID,
		Root:  event.CurrentState,
		AsOf:  time.Now(),
	})

	return result, false, err
}

func (h *GroupTracker) bufferLocked(event hooks.BlockAppliedEvent) error {
	if event.CurrentState == nil {
		return errors.New("masterchain state root is absent")
	}
	if err := validateGroupReplayBlockID(event.Meta.ID); err != nil {
		return err
	}

	key := groupReplayKey(event.Meta.ID)
	if existing, exists := h.buffered[key]; exists {
		// Cell.Hash is a read of bytes precomputed at finalization, so the
		// buffered state root needs no separate hash field.
		if existing.Root.Hash() == nil || !bytes.Equal(existing.Root.Hash(), event.CurrentState.Hash()) ||
			!storage.SameBlockIDs(existing.PrevRefs, event.Meta.PrevRefs) {
			return fmt.Errorf("conflicting buffered masterchain state %s", storage.FormatBlockRef(event.Meta.ID))
		}

		return nil
	}

	// Meta is borrowed and may be reused after this call, so the block ids are
	// copied element by element; sharing the slice would share its hash bytes.
	h.buffered[key] = groups.BufferedMasterchainState{
		Block:    *event.Meta.ID.Copy(),
		Root:     event.CurrentState,
		PrevRefs: storage.CloneBlockIDs(event.Meta.PrevRefs),
	}

	return nil
}

func validateGroupReplayBlockID(block ton.BlockIDExt) error {
	if block.Workchain != groupReplayMasterchainWorkchain || block.Shard != groupReplayMasterchainShard {
		return fmt.Errorf("block %s is not the masterchain root shard", storage.FormatBlockRef(block))
	}
	if err := storage.ValidateBlockIDHashes(block); err != nil {
		return err
	}

	return nil
}

func groupReplayKey(block ton.BlockIDExt) groupReplayBlockKey {
	key := groupReplayBlockKey{
		Workchain: block.Workchain,
		Shard:     block.Shard,
		Seqno:     block.SeqNo,
	}
	copy(key.RootHash[:], block.RootHash)
	copy(key.FileHash[:], block.FileHash)

	return key
}
