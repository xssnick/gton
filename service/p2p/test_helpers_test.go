package p2p

import (
	"os"
	"testing"

	"flexserver/internal/logutil"

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
	node, err := New(Options{Logger: &logger})
	if err != nil {
		tb.Fatalf("create test node: %v", err)
	}
	return node
}

func testBlockID(workchain int32, shard int64, seqno uint32) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: workchain,
		Shard:     shard,
		SeqNo:     seqno,
		RootHash:  []byte{byte(seqno), 0x01},
		FileHash:  []byte{byte(seqno), 0x02},
	}
}
