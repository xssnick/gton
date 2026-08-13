package p2p

import (
	"bytes"
	"math"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestShardDescriptionBlockIDRejectsNonTLBShardIdent(t *testing.T) {
	tests := []struct {
		name  string
		block ton.BlockIDExt
		want  string
	}{
		{
			name: "invalid workchain",
			block: ton.BlockIDExt{
				Workchain: math.MinInt32,
				Shard:     topShard,
				RootHash:  make([]byte, 32),
				FileHash:  make([]byte, 32),
			},
			want: "invalid workchain",
		},
		{
			name: "prefix depth 61",
			block: ton.BlockIDExt{
				Workchain: 0,
				Shard:     4,
				RootHash:  make([]byte, 32),
				FileHash:  make([]byte, 32),
			},
			want: "exceeds 60",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := shardDescriptionBlockIDFromBlock(test.block)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestLoadShardDescriptionBlockIDRejectsNonTLBShardIdent(t *testing.T) {
	tests := []struct {
		name  string
		shard shardDescriptionShardIdentTLB
		want  string
	}{
		{
			name: "invalid workchain",
			shard: shardDescriptionShardIdentTLB{
				WorkchainID: math.MinInt32,
			},
			want: "invalid workchain",
		},
		{
			name: "prefix depth 61",
			shard: shardDescriptionShardIdentTLB{
				PrefixBits: 61,
			},
			want: "exceeds 60",
		},
		{
			name: "non-zero suffix",
			shard: shardDescriptionShardIdentTLB{
				PrefixBits:  4,
				ShardPrefix: 1,
			},
			want: "non-zero bits",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			encoded, err := tlb.ToCell(&shardDescriptionBlockIDExtTLB{
				ShardID:  test.shard,
				RootHash: make([]byte, 32),
				FileHash: make([]byte, 32),
			})
			if err != nil {
				t.Fatalf("serialize malformed block id: %v", err)
			}
			loader, err := encoded.BeginParse()
			if err != nil {
				t.Fatalf("parse malformed block id cell: %v", err)
			}

			_, err = loadShardDescriptionBlockID(loader)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestShardDescriptionBlockIDRoundTripsMaximumPrefixDepth(t *testing.T) {
	block := ton.BlockIDExt{
		Workchain: 0,
		Shard:     8,
		SeqNo:     17,
		RootHash:  bytes.Repeat([]byte{0x11}, 32),
		FileHash:  bytes.Repeat([]byte{0x22}, 32),
	}
	wire, err := shardDescriptionBlockIDFromBlock(block)
	if err != nil {
		t.Fatalf("build depth-60 block id: %v", err)
	}
	encoded, err := tlb.ToCell(&wire)
	if err != nil {
		t.Fatalf("serialize depth-60 block id: %v", err)
	}
	loader, err := encoded.BeginParse()
	if err != nil {
		t.Fatalf("parse depth-60 block id cell: %v", err)
	}

	decoded, err := loadShardDescriptionBlockID(loader)
	if err != nil {
		t.Fatalf("decode depth-60 block id: %v", err)
	}
	if !decoded.Equals(&block) {
		t.Fatalf("decoded block = %+v, want %+v", decoded, block)
	}
}

func TestParseShardTopBlockDescriptionTreatsCatchainAsUint32BitPattern(t *testing.T) {
	const validatorSetHash uint32 = 0x89abcdef
	catchainSeqno := uint32(0x80000001)

	block := ton.BlockIDExt{
		Workchain: 0,
		Shard:     topShard,
		SeqNo:     7,
		RootHash:  make([]byte, 32),
		FileHash:  make([]byte, 32),
	}
	proofFor, err := tlb.ToCell(&shardDescriptionBlockIDExtTLB{
		ShardID: shardDescriptionShardIdentTLB{
			PrefixBits:  0,
			WorkchainID: block.Workchain,
			ShardPrefix: 0,
		},
		SeqNo:    block.SeqNo,
		RootHash: block.RootHash,
		FileHash: block.FileHash,
	})
	if err != nil {
		t.Fatalf("serialize proof target: %v", err)
	}

	dict := cell.NewDict(16)
	value := cell.BeginCell().
		MustStoreSlice(make([]byte, 32), 256).
		MustStoreUInt(5, 4).
		MustStoreSlice(make([]byte, 64), 512).
		EndCell()
	if err = dict.Set(
		cell.BeginCell().MustStoreUInt(0, 16).EndCell(),
		value,
	); err != nil {
		t.Fatalf("store signature: %v", err)
	}
	signatures := cell.BeginCell().
		MustStoreUInt(0x11, 8).
		MustStoreUInt(uint64(validatorSetHash), 32).
		MustStoreUInt(uint64(catchainSeqno), 32).
		MustStoreUInt(1, 32).
		MustStoreUInt(1, 64).
		MustStoreDict(dict).
		EndCell()

	description := cell.BeginCell().
		MustStoreUInt(topBlockDescriptionMagic, 8).
		MustStoreBuilder(proofFor.ToBuilder()).
		MustStoreBoolBit(true).
		MustStoreRef(signatures).
		MustStoreUInt(1, 8).
		MustStoreRef(cell.BeginCell().EndCell()).
		EndCell()

	_, err = ParseShardTopBlockDescription(block, int32(catchainSeqno), description)
	if err == nil {
		t.Fatal("dummy proof unexpectedly passed validation")
	}
	if strings.Contains(err.Error(), "negative catchain") || strings.Contains(err.Error(), "catchain seqno mismatch") {
		t.Fatalf("high-bit catchain seqno was not treated as uint32: %v", err)
	}
	if !strings.Contains(err.Error(), "check shard top block description proof") {
		t.Fatalf("parser did not advance past the matching high-bit catchain seqno: %v", err)
	}
}
