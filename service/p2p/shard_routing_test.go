package p2p

import (
	"errors"
	"testing"

	"github.com/xssnick/gton/service/shard"

	"github.com/xssnick/tonutils-go/ton"
)

func TestQuerySubscriptionForBlockRejectsZeroShard(t *testing.T) {
	node := newTestNode(t)

	_, err := node.querySubscriptionForBlock(ton.BlockIDExt{Workchain: 0})
	if !errors.Is(err, shard.ErrInvalidID) {
		t.Fatalf("error = %v, want ErrInvalidID", err)
	}
}

func TestHistoricalQuerySubscriptionRejectsZeroShard(t *testing.T) {
	node := newTestNode(t)

	_, err := node.querySubscriptionForHistoricalBlock(ton.BlockIDExt{Workchain: 0})
	if !errors.Is(err, shard.ErrInvalidID) {
		t.Fatalf("error = %v, want ErrInvalidID", err)
	}
}

func TestFastSyncDesiredShardsRejectsZeroMasterchainShard(t *testing.T) {
	node := &Node{}

	_, err := node.fastSyncDesiredShards([]FastSyncShard{{Workchain: -1}})
	if !errors.Is(err, shard.ErrInvalidID) {
		t.Fatalf("error = %v, want ErrInvalidID", err)
	}
}

func TestFastSyncSubscriptionForBlockRejectsZeroMasterchainShard(t *testing.T) {
	node := &Node{fastSyncSubscriptions: map[FastSyncShard]*overlaySubscription{}}

	_, err := node.fastSyncSubscriptionForBlock(ton.BlockIDExt{Workchain: -1})
	if !errors.Is(err, shard.ErrInvalidID) {
		t.Fatalf("error = %v, want ErrInvalidID", err)
	}
}

func TestFastSyncFanoutTargetRejectsZeroShard(t *testing.T) {
	node := &Node{fastSyncSubscriptions: map[FastSyncShard]*overlaySubscription{}}
	block := ton.BlockIDExt{Workchain: 0}

	_, err := node.fastSyncFanoutTarget(acceptedBroadcast{
		block:       &block,
		rebroadcast: &rebroadcastRequest{kind: "tonNode.newBlockBroadcast"},
	})
	if !errors.Is(err, shard.ErrInvalidID) {
		t.Fatalf("error = %v, want ErrInvalidID", err)
	}
}

func BenchmarkReadyFastSyncQuerySubscription(b *testing.B) {
	node := &Node{fastSyncSubscriptions: map[FastSyncShard]*overlaySubscription{}}
	block := ton.BlockIDExt{Workchain: 0, Shard: topShard}

	var selected *overlaySubscription
	var err error
	b.ReportAllocs()
	for b.Loop() {
		selected, err = node.readyFastSyncQuerySubscription(block)
	}
	if err != nil || selected != nil {
		b.Fatalf("selected = %p, error = %v, want empty result", selected, err)
	}
}

func BenchmarkFastSyncFanoutTarget(b *testing.B) {
	node := &Node{fastSyncSubscriptions: map[FastSyncShard]*overlaySubscription{}}
	block := ton.BlockIDExt{Workchain: 0, Shard: topShard}
	accepted := acceptedBroadcast{
		block:       &block,
		rebroadcast: &rebroadcastRequest{kind: "tonNode.newBlockBroadcast"},
	}

	var selected *overlaySubscription
	var err error
	b.ReportAllocs()
	for b.Loop() {
		selected, err = node.fastSyncFanoutTarget(accepted)
	}
	if err != nil || selected != nil {
		b.Fatalf("selected = %p, error = %v, want empty result", selected, err)
	}
}
