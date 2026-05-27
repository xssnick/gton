package p2p

import (
	"context"
	"os"
	"testing"

	"github.com/xssnick/gton/internal/logutil"
	"github.com/xssnick/gton/service/storage/pebblestore"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/ton"
)

func discardLogger() zerolog.Logger {
	return logutil.Discard()
}

func stdoutLogger(level zerolog.Level) zerolog.Logger {
	return logutil.New(os.Stdout, level, false)
}

func newTestNode(tb testing.TB) *Node {
	tb.Helper()

	logger := discardLogger()
	node, err := New(Options{
		Logger:             &logger,
		PeerServingStorage: newTestPeerStore(),
		StateFilesDir:      tb.TempDir(),
		SignatureVerifier:  testAcceptBroadcastSignatureVerifier{},
	})
	if err != nil {
		tb.Fatalf("create test node: %v", err)
	}
	return node
}

type testAcceptBroadcastSignatureVerifier struct{}

func (testAcceptBroadcastSignatureVerifier) CheckBlockBroadcastSignatures(context.Context, BlockBroadcastSignatureCheck) error {
	return nil
}

func (testAcceptBroadcastSignatureVerifier) CheckShardDescriptionSignatures(context.Context, ShardDescriptionSignatureCheck) error {
	return nil
}

type testRejectBroadcastSignatureVerifier struct {
	err error
}

func (v testRejectBroadcastSignatureVerifier) CheckBlockBroadcastSignatures(context.Context, BlockBroadcastSignatureCheck) error {
	return v.err
}

func (v testRejectBroadcastSignatureVerifier) CheckShardDescriptionSignatures(context.Context, ShardDescriptionSignatureCheck) error {
	return v.err
}

func newTestPebbleStore(tb testing.TB) *pebblestore.Store {
	tb.Helper()

	store, err := pebblestore.Open(pebblestore.Options{Dir: tb.TempDir()})
	if err != nil {
		tb.Fatalf("open pebble store: %v", err)
	}
	tb.Cleanup(func() {
		_ = store.Close()
	})
	return store
}

func testBlockID(workchain int32, shard int64, seqno uint32) ton.BlockIDExt {
	root := make([]byte, 32)
	file := make([]byte, 32)
	root[0] = byte(seqno)
	root[31] = 0x01
	file[0] = byte(seqno)
	file[31] = 0x02
	return ton.BlockIDExt{
		Workchain: workchain,
		Shard:     shard,
		SeqNo:     seqno,
		RootHash:  root,
		FileHash:  file,
	}
}
