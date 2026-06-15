package main

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

func TestEnsureStoredZeroStateMatchesGlobalConfig(t *testing.T) {
	ctx := context.Background()
	configured := testZeroStateBlock(0x11, 0x12)

	if err := ensureStoredZeroStateMatchesGlobalConfig(ctx, fakeZeroStateStore{err: storage.ErrNotFound}, configured); err != nil {
		t.Fatalf("missing stored zerostate: %v", err)
	}

	if err := ensureStoredZeroStateMatchesGlobalConfig(ctx, fakeZeroStateStore{blocks: []ton.BlockIDExt{configured}}, configured); err != nil {
		t.Fatalf("matching stored zerostate: %v", err)
	}

	basechain := testZeroStateBlock(0x21, 0x22)
	basechain.Workchain = 0
	if err := ensureStoredZeroStateMatchesGlobalConfig(ctx, fakeZeroStateStore{blocks: []ton.BlockIDExt{basechain, configured}}, configured); err != nil {
		t.Fatalf("stored basechain zerostate should not be compared with global config zerostate: %v", err)
	}

	err := ensureStoredZeroStateMatchesGlobalConfig(ctx, fakeZeroStateStore{
		blocks: []ton.BlockIDExt{testZeroStateBlock(0x21, 0x22)},
	}, configured)
	if err == nil {
		t.Fatal("expected zerostate mismatch error")
	}
	if !strings.Contains(err.Error(), "does not match global config zerostate") {
		t.Fatalf("unexpected mismatch error: %v", err)
	}
}

func TestStartupZeroStateRequired(t *testing.T) {
	zero := testZeroStateBlock(0x11, 0x12)
	initBlock := testZeroStateBlock(0x21, 0x22)
	initBlock.SeqNo = 123

	tests := []struct {
		name              string
		fromZero          bool
		initBlock         ton.BlockIDExt
		liteserverEnabled bool
		want              bool
	}{
		{
			name:      "from zero with normal init block",
			fromZero:  true,
			initBlock: initBlock,
			want:      true,
		},
		{
			name:      "zero init block",
			initBlock: zero,
			want:      true,
		},
		{
			name:      "normal init block",
			initBlock: initBlock,
			want:      false,
		},
		{
			name:              "liteserver enabled with normal init block",
			initBlock:         initBlock,
			liteserverEnabled: true,
			want:              true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := startupZeroStateRequired(tt.fromZero, tt.initBlock, tt.liteserverEnabled); got != tt.want {
				t.Fatalf("startupZeroStateRequired() = %v, want %v", got, tt.want)
			}
		})
	}
}

type fakeZeroStateStore struct {
	blocks []ton.BlockIDExt
	err    error
}

func (s fakeZeroStateStore) StoredZeroStateBlocks(context.Context) ([]ton.BlockIDExt, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.blocks, nil
}

func testZeroStateBlock(rootByte byte, fileByte byte) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: -1,
		Shard:     topShard,
		SeqNo:     0,
		RootHash:  bytes.Repeat([]byte{rootByte}, 32),
		FileHash:  bytes.Repeat([]byte{fileByte}, 32),
	}
}
