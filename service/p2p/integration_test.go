package p2p

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestIntegrationReceivesBroadcasts(t *testing.T) {
	if os.Getenv("RUN_TON_INTEGRATION") == "" {
		t.Skip("set RUN_TON_INTEGRATION=1 to run live TON network test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	logger := stdoutLogger(zerolog.InfoLevel)
	node, err := New(Options{Logger: &logger})
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	if err := node.Start(ctx); err != nil {
		t.Fatalf("start node: %v", err)
	}

	var (
		gotMaster bool
		gotBase   bool
	)

	for !gotMaster || !gotBase {
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for broadcasts: master=%v base=%v", gotMaster, gotBase)
		case ev := <-node.Events():
			switch ev.Overlay {
			case "masterchain":
				gotMaster = true
			case "basechain":
				gotBase = true
			}
		}
	}
}

func TestIntegrationDownloadsBlockFull(t *testing.T) {
	if os.Getenv("RUN_TON_INTEGRATION") == "" {
		t.Skip("set RUN_TON_INTEGRATION=1 to run live TON network test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	logger := stdoutLogger(zerolog.InfoLevel)
	node, err := New(Options{Logger: &logger})
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	if err := node.Start(ctx); err != nil {
		t.Fatalf("start node: %v", err)
	}

	var target *BroadcastEvent
	for target == nil {
		select {
		case <-ctx.Done():
			t.Fatal("timed out waiting for a masterchain block broadcast")
		case ev := <-node.Events():
			if ev.Overlay == "masterchain" {
				copyEv := ev
				target = &copyEv
			}
		}
	}

	block, err := node.DownloadBlockFull(ctx, target.Block)
	if err != nil {
		t.Fatalf("download block %s: %v", target.BlockRef(), err)
	}

	if !block.ID.Equals(&target.Block) {
		t.Fatalf("downloaded unexpected block %s, want %s", block.BlockRef(), target.BlockRef())
	}
	if !block.VerifiedRootHash {
		t.Fatalf("expected root hash verification")
	}
}

func TestIntegrationDownloadsNextBlockFull(t *testing.T) {
	if os.Getenv("RUN_TON_INTEGRATION") == "" {
		t.Skip("set RUN_TON_INTEGRATION=1 to run live TON network test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	logger := stdoutLogger(zerolog.InfoLevel)
	node, err := New(Options{Logger: &logger})
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	if err := node.Start(ctx); err != nil {
		t.Fatalf("start node: %v", err)
	}

	seen := map[uint32]BroadcastEvent{}

	for {
		select {
		case <-ctx.Done():
			t.Fatal("timed out waiting for consecutive masterchain broadcasts")
		case ev := <-node.Events():
			if ev.Overlay != "masterchain" {
				continue
			}

			seen[ev.Block.SeqNo] = ev
			prev, ok := seen[ev.Block.SeqNo-1]
			if !ok {
				continue
			}

			block, err := node.DownloadNextBlockFull(ctx, prev.Block)
			if err != nil {
				t.Fatalf("download next block after %s: %v", prev.BlockRef(), err)
			}
			if !block.ID.Equals(&ev.Block) {
				t.Fatalf("downloaded unexpected next block %s, want %s", block.BlockRef(), ev.BlockRef())
			}
			if !block.VerifiedRootHash {
				t.Fatalf("expected root hash verification")
			}
			return
		}
	}
}
