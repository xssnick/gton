package liveview

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestWaitBlockArtifactsResumesOnExactLivePublication(t *testing.T) {
	root := cell.BeginCell().MustStoreUInt(0x51, 8).EndCell()
	stateRoot := cell.BeginCell().MustStoreUInt(0x52, 8).EndCell()
	block := ton.BlockIDExt{
		Workchain: 0,
		Shard:     int64(1) << 62,
		SeqNo:     35,
		RootHash:  root.Hash(),
		FileHash:  bytes.Repeat([]byte{0x53}, 32),
	}
	state := storage.BlockState{
		Block:         block,
		StateRootHash: stateRoot.Hash(),
		Cell:          stateRoot,
	}
	live := New(noopBacking{})

	result := make(chan error, 1)
	go func() {
		result <- live.WaitBlockArtifacts(t.Context(), block)
	}()

	select {
	case err := <-result:
		t.Fatalf("wait returned before sync publication: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	if err := live.PublishLiveBlockArtifacts(storage.LiveBlockArtifacts{
		Block:            block,
		Root:             root,
		State:            &state,
		AvailabilityOnly: true,
	}); err != nil {
		t.Fatalf("publish exact live artifacts: %v", err)
	}

	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("wait after sync publication: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("wait did not resume after exact live publication")
	}
}

func TestWaitBlockArtifactsStopsOnCancellation(t *testing.T) {
	live := New(noopBacking{})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := live.WaitBlockArtifacts(ctx, testLiveBlockID(0, int64(1)<<62, 35, 0x61))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("wait error = %v, want context.Canceled", err)
	}
}
