package p2p

import (
	"context"
	"testing"
)

func TestObserveBlockReceivedSkipsWithoutObserver(t *testing.T) {
	node := &Node{}

	node.observeBlockReceived(context.Background(), &DownloadedBlock{ID: testBlockID(0, topShard, 43)}, true)
}

func TestObserveBlockReceivedPublishesWhenConfigured(t *testing.T) {
	observer := &testBlockReceivedObserver{}
	node := &Node{
		blockReceivedObserver: observer,
	}
	downloaded := &DownloadedBlock{ID: testBlockID(0, topShard, 44)}

	node.observeBlockReceived(context.Background(), downloaded, true)

	if len(observer.events) != 1 {
		t.Fatalf("block received events = %d, want 1", len(observer.events))
	}
	if !observer.events[0].IsSigned || observer.events[0].Downloaded != downloaded {
		t.Fatalf("unexpected block received event: %+v", observer.events[0])
	}
}

func TestObserveDownloadedBlockReceivedPublishesSigned(t *testing.T) {
	observer := &testBlockReceivedObserver{}
	node := &Node{
		blockReceivedObserver: observer,
	}
	downloaded := &DownloadedBlock{ID: testBlockID(0, topShard, 45)}

	node.observeDownloadedBlockReceived(context.Background(), downloaded)

	if len(observer.events) != 1 {
		t.Fatalf("block received events = %d, want 1", len(observer.events))
	}
	if !observer.events[0].IsSigned || observer.events[0].Downloaded != downloaded {
		t.Fatalf("unexpected downloaded block received event: %+v", observer.events[0])
	}
}
