package pebblestore

import (
	"bytes"
	"context"
	"encoding/binary"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
)

func saveTestBlockState(ctx context.Context, store *Store, state *storage.BlockState) error {
	entries := testStateCheckpointEntries([]*storage.BlockState{state})
	if state != nil && state.Block.Workchain != -1 && state.Block.SeqNo != 0 && state.MasterchainRef == nil {
		entries = append(testStateCheckpointEntries([]*storage.BlockState{testDummyMasterState(state.Block.SeqNo)}), entries...)
	}
	_, err := store.SaveStateCheckpointEntries(ctx, entries, storage.StateCellRecords{}, nil)
	return err
}

func saveTestStateCheckpoint(ctx context.Context, store *Store, blocks []*storage.BlockState, current *storage.CurrentState) error {
	_, err := store.SaveStateCheckpointEntries(ctx, testStateCheckpointEntriesForCurrent(blocks, current), storage.StateCellRecords{}, current)
	return err
}

func testStateCheckpointEntries(states []*storage.BlockState) []storage.StateCheckpointBlock {
	return testStateCheckpointEntriesForCurrent(states, nil)
}

func testStateCheckpointEntriesForCurrent(states []*storage.BlockState, current *storage.CurrentState) []storage.StateCheckpointBlock {
	entries := make([]storage.StateCheckpointBlock, 0, len(states))
	for _, state := range states {
		if state == nil {
			continue
		}
		entries = append(entries, testStateCheckpointEntryForCurrent(state, current))
	}
	return entries
}

func testStateCheckpointEntry(state *storage.BlockState) storage.StateCheckpointBlock {
	return testStateCheckpointEntryForCurrent(state, nil)
}

func testStateCheckpointEntryForCurrent(state *storage.BlockState, current *storage.CurrentState) storage.StateCheckpointBlock {
	entry := storage.StateCheckpointBlock{State: state}
	if state != nil && state.Block.SeqNo != 0 {
		entry.Artifact = testStateCheckpointArtifactForCurrent(state, current)
	}
	return entry
}

func testStateCheckpointArtifactForCurrent(state *storage.BlockState, current *storage.CurrentState) *storage.ServedBlockFull {
	block := state.Block
	meta := &storage.BlockMeta{ID: block, GenUTime: block.SeqNo}
	if block.Workchain != -1 {
		if state.MasterchainRef != nil {
			meta.MasterchainRefSeqno = state.MasterchainRef.SeqNo
		} else if current != nil {
			meta.MasterchainRefSeqno = current.Masterchain.Block.SeqNo
		} else {
			meta.MasterchainRefSeqno = block.SeqNo
		}
	}
	return &storage.ServedBlockFull{
		ID:             block,
		Block:          []byte{0x01},
		Proof:          []byte{0x02},
		Meta:           meta,
		MessageEntries: []storage.MessageTransactionIndexEntry{},
	}
}

func testDummyMasterState(seqno uint32) *storage.BlockState {
	return &storage.BlockState{
		Block: ton.BlockIDExt{
			Workchain: -1,
			Shard:     topShard,
			SeqNo:     seqno,
			RootHash:  testDummyHash(0xf1, seqno),
			FileHash:  testDummyHash(0xf2, seqno),
		},
		StateRootHash: testDummyHash(0xf3, seqno),
	}
}

func testDummyHash(prefix byte, seqno uint32) []byte {
	hash := bytes.Repeat([]byte{prefix}, 32)
	binary.BigEndian.PutUint32(hash[len(hash)-4:], seqno)
	return hash
}
