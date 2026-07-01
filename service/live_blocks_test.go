package service

import (
	"testing"

	tnstore "github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestPublishLiveBlockArtifactsAvailabilityPolicy(t *testing.T) {
	flusher := &testLiveCheckpointFlusher{}
	svc := &Service{log: zerolog.Nop(), liveState: flusher}
	block := testBlockID(-1, topShard, 501)
	downloaded := PreparedBlock{
		ID:        block,
		BlockRoot: cell.BeginCell().EndCell(),
		BlockBOC:  []byte{0x01},
	}
	state := &tnstore.BlockState{Block: block}

	svc.publishLiveBlockArtifacts(downloaded, state, liveBlockPublishOptions{availabilityOnly: true})
	if len(flusher.artifacts) != 1 {
		t.Fatalf("archive publish artifacts = %d, want 1", len(flusher.artifacts))
	}
	if !flusher.artifacts[0].AvailabilityOnly {
		t.Fatal("archive publish was not marked as availability-only")
	}

	flusher.artifacts = nil
	svc.publishLiveBlockArtifacts(downloaded, state, liveBlockPublishOptions{})
	if len(flusher.artifacts) != 1 {
		t.Fatalf("live publish artifacts = %d, want 1", len(flusher.artifacts))
	}
	if flusher.artifacts[0].AvailabilityOnly {
		t.Fatal("live publish was marked as availability-only")
	}
}
