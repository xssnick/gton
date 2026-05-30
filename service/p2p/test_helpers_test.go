package p2p

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
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

func testPeerID(label string) PeerID {
	return PeerID(sha256.Sum256([]byte(label)))
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
	root := testBlockHash(0x01, workchain, shard, seqno)
	file := testBlockHash(0x02, workchain, shard, seqno)
	return ton.BlockIDExt{
		Workchain: workchain,
		Shard:     shard,
		SeqNo:     seqno,
		RootHash:  root,
		FileHash:  file,
	}
}

func testBlockHash(kind byte, workchain int32, shard int64, seqno uint32) []byte {
	var data [17]byte
	data[0] = kind
	binary.BigEndian.PutUint32(data[1:5], uint32(workchain))
	binary.BigEndian.PutUint64(data[5:13], uint64(shard))
	binary.BigEndian.PutUint32(data[13:17], seqno)
	hash := sha256.Sum256(data[:])
	return append([]byte(nil), hash[:]...)
}
