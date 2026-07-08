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

func (testAcceptBroadcastSignatureVerifier) CheckBlockFinalitySignatures(_ context.Context, req BlockFinalitySignatureCheck) (*BlockFinalitySignatureCheckResult, error) {
	return &BlockFinalitySignatureCheckResult{
		SignaturesVerifiedKey: []byte("test-finality"),
	}, nil
}

func (testAcceptBroadcastSignatureVerifier) ValidateShardDescriptionBroadcast(_ context.Context, req ShardDescriptionSignatureCheck) (*ShardBlockDescription, error) {
	return &ShardBlockDescription{
		Block:         req.Block,
		CatchainSeqno: uint32(req.CatchainSeqno),
	}, nil
}

type testRejectBroadcastSignatureVerifier struct {
	err error
}

func (v testRejectBroadcastSignatureVerifier) CheckBlockBroadcastSignatures(context.Context, BlockBroadcastSignatureCheck) error {
	return v.err
}

func (v testRejectBroadcastSignatureVerifier) CheckBlockFinalitySignatures(context.Context, BlockFinalitySignatureCheck) (*BlockFinalitySignatureCheckResult, error) {
	return nil, v.err
}

func (v testRejectBroadcastSignatureVerifier) ValidateShardDescriptionBroadcast(context.Context, ShardDescriptionSignatureCheck) (*ShardBlockDescription, error) {
	return nil, v.err
}

type testBroadcastAdmission bool

func (a testBroadcastAdmission) CanAcceptBroadcast(BroadcastAdmissionRequest) bool {
	return bool(a)
}

type testExternalMessageAdmission struct {
	events []ExternalMessageEvent
	err    error
}

func (a *testExternalMessageAdmission) AcceptExternalMessage(_ context.Context, event ExternalMessageEvent) error {
	a.events = append(a.events, event)
	return a.err
}

func (a *testExternalMessageAdmission) AcceptCheckedExternalMessage(_ context.Context, event ExternalMessageEvent) error {
	a.events = append(a.events, event)
	return a.err
}

type testBlockReceivedObserver struct {
	events []BlockReceivedEvent
	hooks  bool
}

func (o *testBlockReceivedObserver) ObserveBlockReceived(_ context.Context, event BlockReceivedEvent) {
	o.events = append(o.events, event)
}

func (o *testBlockReceivedObserver) BlockReceivedHooksEnabled() bool {
	return o.hooks
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
