package groups

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// MasterchainHistory is the exact canonical state history consumed while a
// Tracker is bootstrapped. Implementations must resolve every method by the
// complete BlockIDExt; a seqno-only lookup can silently cross a stored fork.
type MasterchainHistory interface {
	CurrentState(ctx context.Context) (*storage.CurrentState, error)
	BlockState(ctx context.Context, block ton.BlockIDExt) (*storage.BlockState, error)
	BlockMeta(ctx context.Context, block ton.BlockIDExt) (*storage.BlockMeta, error)
}

// BufferedMasterchainState is one applied state observed before the durable
// current-state head was published. Block IDs and PrevRefs are immutable for
// the duration of Bootstrap; Root is an immutable cell tree.
type BufferedMasterchainState struct {
	Block    ton.BlockIDExt
	Root     *cell.Cell
	PrevRefs []ton.BlockIDExt
}

type replayBlockKey struct {
	Workchain int32
	Shard     int64
	Seqno     uint32
	RootHash  [32]byte
	FileHash  [32]byte
}

type replayRecord struct {
	Input  StateInput
	State  *State
	Parent *ton.BlockIDExt
}

type replayReader struct {
	history    MasterchainHistory
	storedHead *ton.BlockIDExt
	buffered   map[replayBlockKey]BufferedMasterchainState
	visited    map[replayBlockKey]struct{}
}

// Bootstrap reconstructs this empty Tracker from exact canonical masterchain
// history. It installs the final snapshot only after the complete replay has
// succeeded, so an error leaves the Tracker empty and retryable. Lifecycle
// transitions are suppressed before the selected head and returned only for
// that final state.
func (t *Tracker) Bootstrap(
	ctx context.Context,
	history MasterchainHistory,
	buffered []BufferedMasterchainState,
	asOf time.Time,
) ([]Transition, error) {
	if asOf.IsZero() {
		return nil, errors.New("validator groups observation time is required")
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// A successful replay always stores a non-nil snapshot below, so the
	// published snapshot is the whole bootstrap record.
	if t.snapshot.Load() != nil {
		return nil, ErrTrackerAlreadyBootstrapped
	}

	result, err := t.replay(ctx, history, buffered, asOf)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, ErrNoSnapshot
	}

	for _, snapshot := range result.history {
		t.recordSnapshotLocked(snapshot)
	}
	t.snapshot.Store(result.final.Snapshot)
	close(t.changed)
	t.changed = make(chan struct{})

	return result.final.Transitions, nil
}

type replayResult struct {
	final   ApplyResult
	history []*Snapshot
}

func (t *Tracker) replay(
	ctx context.Context,
	history MasterchainHistory,
	buffered []BufferedMasterchainState,
	asOf time.Time,
) (*replayResult, error) {
	current, err := history.CurrentState(ctx)
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return nil, fmt.Errorf("load current state for validator groups: %w", err)
	}
	hasStoredHead := err == nil

	bufferedByID, bufferedHead, err := indexBufferedMasterchainStates(buffered)
	if err != nil {
		return nil, err
	}

	var storedHead *ton.BlockIDExt
	if hasStoredHead {
		head := cloneReplayBlockID(current.Masterchain.Block)
		if err = validateMasterchainBlockID(head); err != nil {
			return nil, fmt.Errorf("stored masterchain head: %w", err)
		}
		storedHead = &head
	}

	effectiveHead, err := selectReplayHead(storedHead, bufferedHead)
	if err != nil {
		return nil, err
	}
	if effectiveHead == nil {
		return nil, nil
	}

	reader := replayReader{
		history:    history,
		storedHead: storedHead,
		buffered:   bufferedByID,
		visited:    make(map[replayBlockKey]struct{}, len(bufferedByID)),
	}
	records, rotationIndex, previousRotation, err := collectReplayRecords(ctx, &reader, *effectiveHead)
	if err != nil {
		return nil, err
	}
	if err = reader.requireCanonicalBufferedChain(); err != nil {
		return nil, err
	}

	var initialCollators []CollatorRegistryEntry
	if previousRotation != nil {
		initialCollators, err = ResolveCollatorsByValidator(previousRotation.Input)
		if err != nil {
			return nil, fmt.Errorf(
				"resolve collator registry at previous rotation %s: %w",
				storage.FormatBlockRef(previousRotation.Input.Block),
				err,
			)
		}
	}

	temporary, err := t.newReplayTracker(effectiveHead.SeqNo, initialCollators)
	if err != nil {
		return nil, err
	}

	var final ApplyResult
	for index := rotationIndex; index >= 0; index-- {
		record := &records[index]
		final, err = temporary.Apply(ApplyInput{
			Block: record.Input.Block,
			Root:  record.Input.Root,
			AsOf:  asOf,
		})
		if err != nil {
			return nil, fmt.Errorf(
				"replay validator groups at %s: %w",
				storage.FormatBlockRef(record.Input.Block),
				err,
			)
		}
	}

	replayed := make([]*Snapshot, 0, len(temporary.historyOrder))
	for _, key := range temporary.historyOrder {
		replayed = append(replayed, temporary.history[key])
	}

	return &replayResult{final: final, history: replayed}, nil
}

func (t *Tracker) newReplayTracker(
	headSeqno uint32,
	initialCollators []CollatorRegistryEntry,
) (*Tracker, error) {
	initial, err := normalizeCollatorRegistry(initialCollators)
	if err != nil {
		return nil, fmt.Errorf("initial collator registry: %w", err)
	}

	return &Tracker{
		maximalVerticalSeqno: t.maximalVerticalSeqno,
		startGroupsFromSeqno: max(t.startGroupsFromSeqno, headSeqno),
		unsafeRotations:      t.unsafeRotations,
		initialCollators:     initial,
		changed:              make(chan struct{}),
	}, nil
}

func collectReplayRecords(
	ctx context.Context,
	reader *replayReader,
	head ton.BlockIDExt,
) ([]replayRecord, int, *replayRecord, error) {
	records := make([]replayRecord, 0, 256)
	rotationIndex := -1
	var previousRotation *replayRecord
	bufferFloor, hasBuffered := oldestBufferedMasterchainState(reader.buffered)
	bufferFloorReached := !hasBuffered
	connectedToStoredHead := reader.storedHead != nil && head.Equals(reader.storedHead)

	block := head
	for {
		if err := ctx.Err(); err != nil {
			return nil, -1, nil, err
		}

		record, err := reader.load(ctx, block)
		if err != nil {
			return nil, -1, nil, err
		}
		records = append(records, record)
		index := len(records) - 1

		if reader.storedHead != nil && record.Input.Block.Equals(reader.storedHead) {
			connectedToStoredHead = true
		}
		if reader.storedHead == nil && record.Input.Block.SeqNo == 0 {
			connectedToStoredHead = true
		}
		if hasBuffered && record.Input.Block.SeqNo <= bufferFloor {
			bufferFloorReached = true
		}

		if record.State.RotatedAllShards {
			switch {
			case rotationIndex < 0:
				rotationIndex = index
			case !records[rotationIndex].State.IsKeyState && previousRotation == nil:
				previousRotation = &records[index]
			}
		}

		seedReady := rotationIndex >= 0 && (records[rotationIndex].State.IsKeyState || previousRotation != nil)
		if seedReady && connectedToStoredHead && bufferFloorReached {
			break
		}
		if record.Parent == nil {
			switch {
			case rotationIndex < 0:
				return nil, -1, nil, errors.New("masterchain history has no rotated_all_shards state")
			case !records[rotationIndex].State.IsKeyState && previousRotation == nil:
				return nil, -1, nil, fmt.Errorf(
					"non-key rotation %s has no previous rotation",
					storage.FormatBlockRef(records[rotationIndex].Input.Block),
				)
			default:
				return nil, -1, nil, fmt.Errorf(
					"masterchain history from %s does not reach stored head",
					storage.FormatBlockRef(head),
				)
			}
		}

		block = *record.Parent
	}

	return records, rotationIndex, previousRotation, nil
}

func (r *replayReader) load(ctx context.Context, block ton.BlockIDExt) (replayRecord, error) {
	key := replayKey(block)
	if state, exists := r.buffered[key]; exists && shouldUseBufferedMasterchainState(r.storedHead, block) {
		record, err := replayRecordFromBuffered(state)
		if err != nil {
			return replayRecord{}, err
		}
		r.visited[key] = struct{}{}

		return record, nil
	}

	state, err := r.history.BlockState(ctx, block)
	if err != nil {
		return replayRecord{}, fmt.Errorf("load validator group state %s: %w", storage.FormatBlockRef(block), err)
	}
	if !state.Block.Equals(&block) {
		return replayRecord{}, fmt.Errorf(
			"validator group state mismatch: got %s want %s",
			storage.FormatBlockRef(state.Block),
			storage.FormatBlockRef(block),
		)
	}
	if state.Cell == nil {
		return replayRecord{}, fmt.Errorf("validator group state %s has no root cell", storage.FormatBlockRef(block))
	}

	meta, err := r.history.BlockMeta(ctx, block)
	if err != nil {
		return replayRecord{}, fmt.Errorf("load validator group block metadata %s: %w", storage.FormatBlockRef(block), err)
	}
	if !meta.ID.Equals(&block) {
		return replayRecord{}, fmt.Errorf(
			"validator group block metadata mismatch: got %s want %s",
			storage.FormatBlockRef(meta.ID),
			storage.FormatBlockRef(block),
		)
	}

	record, err := parseReplayRecord(block, state.Cell, meta.PrevRefs)
	if err != nil {
		return replayRecord{}, err
	}
	if _, buffered := r.buffered[key]; buffered {
		r.visited[key] = struct{}{}
	}

	return record, nil
}

func replayRecordFromBuffered(state BufferedMasterchainState) (replayRecord, error) {
	return parseReplayRecord(state.Block, state.Root, state.PrevRefs)
}

func parseReplayRecord(
	block ton.BlockIDExt,
	root *cell.Cell,
	prevRefs []ton.BlockIDExt,
) (replayRecord, error) {
	parent, err := replayParent(block, prevRefs)
	if err != nil {
		return replayRecord{}, err
	}
	input := StateInput{Block: block, Root: root}
	state, err := ParseState(input)
	if err != nil {
		return replayRecord{}, fmt.Errorf(
			"parse validator group state %s: %w",
			storage.FormatBlockRef(block),
			err,
		)
	}

	return replayRecord{
		Input:  input,
		State:  state,
		Parent: parent,
	}, nil
}

func replayParent(block ton.BlockIDExt, prevRefs []ton.BlockIDExt) (*ton.BlockIDExt, error) {
	if block.SeqNo == 0 {
		if len(prevRefs) != 0 {
			return nil, fmt.Errorf("masterchain zerostate %s has predecessor", storage.FormatBlockRef(block))
		}

		return nil, nil
	}
	if len(prevRefs) != 1 {
		return nil, fmt.Errorf(
			"masterchain block %s has %d predecessors, want one",
			storage.FormatBlockRef(block),
			len(prevRefs),
		)
	}

	parent := prevRefs[0]
	if err := validateMasterchainBlockID(parent); err != nil {
		return nil, fmt.Errorf("masterchain predecessor of %s: %w", storage.FormatBlockRef(block), err)
	}
	if parent.SeqNo+1 != block.SeqNo {
		return nil, fmt.Errorf(
			"masterchain predecessor seqno %d does not precede %d",
			parent.SeqNo,
			block.SeqNo,
		)
	}

	return &parent, nil
}

func indexBufferedMasterchainStates(
	states []BufferedMasterchainState,
) (map[replayBlockKey]BufferedMasterchainState, *ton.BlockIDExt, error) {
	indexed := make(map[replayBlockKey]BufferedMasterchainState, len(states))
	stateRoots := make(map[replayBlockKey][32]byte, len(states))
	var head *ton.BlockIDExt
	for _, state := range states {
		if state.Root == nil {
			return nil, nil, fmt.Errorf("buffered masterchain state %s has no root cell", storage.FormatBlockRef(state.Block))
		}
		if err := validateMasterchainBlockID(state.Block); err != nil {
			return nil, nil, fmt.Errorf("buffered masterchain state: %w", err)
		}

		key := replayKey(state.Block)
		var stateRoot [32]byte
		copy(stateRoot[:], state.Root.Hash())
		if existing, exists := indexed[key]; exists {
			if stateRoots[key] != stateRoot || !storage.SameBlockIDs(existing.PrevRefs, state.PrevRefs) {
				return nil, nil, fmt.Errorf(
					"conflicting buffered masterchain state %s",
					storage.FormatBlockRef(state.Block),
				)
			}
			continue
		}

		owned := BufferedMasterchainState{
			Block:    cloneReplayBlockID(state.Block),
			Root:     state.Root,
			PrevRefs: storage.CloneBlockIDs(state.PrevRefs),
		}
		indexed[key] = owned
		stateRoots[key] = stateRoot
		if head == nil || state.Block.SeqNo > head.SeqNo {
			candidate := cloneReplayBlockID(state.Block)
			head = &candidate
			continue
		}
		if state.Block.SeqNo == head.SeqNo && !state.Block.Equals(head) {
			return nil, nil, fmt.Errorf(
				"conflicting buffered masterchain heads at seqno %d: %s and %s",
				state.Block.SeqNo,
				storage.FormatBlockRef(*head),
				storage.FormatBlockRef(state.Block),
			)
		}
	}

	return indexed, head, nil
}

func selectReplayHead(stored, buffered *ton.BlockIDExt) (*ton.BlockIDExt, error) {
	if stored == nil {
		return buffered, nil
	}
	if buffered == nil || buffered.SeqNo < stored.SeqNo {
		return stored, nil
	}
	if buffered.SeqNo == stored.SeqNo {
		if !buffered.Equals(stored) {
			return nil, fmt.Errorf(
				"buffered masterchain head %s conflicts with stored head %s",
				storage.FormatBlockRef(*buffered),
				storage.FormatBlockRef(*stored),
			)
		}

		return stored, nil
	}

	return buffered, nil
}

func (r *replayReader) requireCanonicalBufferedChain() error {
	for key, state := range r.buffered {
		if _, visited := r.visited[key]; !visited {
			return fmt.Errorf(
				"buffered masterchain state %s is not on the canonical startup chain",
				storage.FormatBlockRef(state.Block),
			)
		}
	}

	return nil
}

func oldestBufferedMasterchainState(states map[replayBlockKey]BufferedMasterchainState) (uint32, bool) {
	var oldest uint32
	hasOldest := false
	for _, state := range states {
		if !hasOldest || state.Block.SeqNo < oldest {
			oldest = state.Block.SeqNo
			hasOldest = true
		}
	}

	return oldest, hasOldest
}

func shouldUseBufferedMasterchainState(storedHead *ton.BlockIDExt, block ton.BlockIDExt) bool {
	return storedHead == nil || block.SeqNo > storedHead.SeqNo
}

func replayKey(block ton.BlockIDExt) replayBlockKey {
	key := replayBlockKey{
		Workchain: block.Workchain,
		Shard:     block.Shard,
		Seqno:     block.SeqNo,
	}
	copy(key.RootHash[:], block.RootHash)
	copy(key.FileHash[:], block.FileHash)

	return key
}

func cloneReplayBlockID(block ton.BlockIDExt) ton.BlockIDExt {
	cloned := block
	cloned.RootHash = bytes.Clone(block.RootHash)
	cloned.FileHash = bytes.Clone(block.FileHash)

	return cloned
}
