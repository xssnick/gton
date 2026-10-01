package service

import (
	"errors"
	"math/big"
	"testing"

	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestShardTopDescriptionContextClassifiesCurrentState(t *testing.T) {
	view, description := testShardTopContextView(t)

	if err := validateShardTopDescriptionContext(t.Context(), view, description); err != nil {
		t.Fatalf("validate current descriptor: %v", err)
	}

	t.Run("known masterchain fork", func(t *testing.T) {
		changed := cloneShardTopContextDescription(description)
		fork := *view.masterchain.Block.Copy()
		fork.RootHash[0] ^= 0xff
		changed.Chain[0].MasterchainRef = &fork

		err := validateShardTopDescriptionContext(t.Context(), view, changed)
		if err == nil || errors.Is(err, p2p.ErrBroadcastSignatureRetryable) {
			t.Fatalf("fork error = %v, want permanent rejection", err)
		}
	})

	t.Run("old shard top", func(t *testing.T) {
		changed := cloneShardTopContextDescription(description)
		changed.Block.SeqNo--
		changed.Chain[0].Block = changed.Block

		err := validateShardTopDescriptionContext(t.Context(), view, changed)
		if err == nil || errors.Is(err, p2p.ErrBroadcastSignatureRetryable) {
			t.Fatalf("old-top error = %v, want permanent rejection", err)
		}
	})

	t.Run("future vertical seqno", func(t *testing.T) {
		changed := cloneShardTopContextDescription(description)
		changed.Chain[0].VertSeqno++

		err := validateShardTopDescriptionContext(t.Context(), view, changed)
		if !errors.Is(err, p2p.ErrBroadcastSignatureRetryable) {
			t.Fatalf("future-vertical error = %v, want retryable", err)
		}
	})

	t.Run("future shard predecessor", func(t *testing.T) {
		changed := cloneShardTopContextDescription(description)
		changed.Block.SeqNo++
		changed.Chain[0].Block = changed.Block
		changed.Chain[0].PrevRefs[0].SeqNo++

		err := validateShardTopDescriptionContext(t.Context(), view, changed)
		if !errors.Is(err, p2p.ErrBroadcastSignatureRetryable) {
			t.Fatalf("future-predecessor error = %v, want retryable", err)
		}
	})
}

func TestShardTopDescriptionContextRunsBeforeAuthentication(t *testing.T) {
	view, description := testShardTopContextView(t)
	description.Chain[0].VertSeqno++
	parsed := &p2p.ParsedShardTopDescription{Description: description}

	err := (&SyncCoordinator{}).validateShardDescriptionAgainstView(t.Context(), view, parsed)
	if !errors.Is(err, p2p.ErrBroadcastSignatureRetryable) {
		t.Fatalf("future context with unavailable authentication = %v, want retryable", err)
	}
	if errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("authentication ran before contextual validation: %v", err)
	}
}

func TestShardTopDescriptionContextPassesCurrentDescriptorToAuthentication(t *testing.T) {
	view, description := testShardTopContextView(t)
	prepared := &blockproof.PreparedValidatorSet{}
	view.validatorSets = map[shardTopValidatorCacheKey]*blockproof.PreparedValidatorSet{
		{
			workchain:     description.Block.Workchain,
			shard:         description.Block.Shard,
			catchainSeqno: description.CatchainSeqno,
		}: prepared,
	}
	description.ValidatorSetHash = prepared.Hash() + 1

	err := (&SyncCoordinator{}).validateShardDescriptionAgainstView(
		t.Context(),
		view,
		&p2p.ParsedShardTopDescription{Description: description},
	)
	if !errors.Is(err, storage.ErrNotFound) || errors.Is(err, p2p.ErrBroadcastSignatureRetryable) {
		t.Fatalf("authentication error = %v, want permanent unknown validator set", err)
	}
}

func BenchmarkValidateShardTopDescriptionContext(b *testing.B) {
	view, description := testShardTopContextView(b)

	b.ReportAllocs()
	for b.Loop() {
		if err := validateShardTopDescriptionContext(b.Context(), view, description); err != nil {
			b.Fatal(err)
		}
	}
}

func testShardTopContextView(t testing.TB) (*shardTopValidationView, *p2p.ShardBlockDescription) {
	t.Helper()

	masterchain := testBlockID(-1, topShard, 100)
	oldShard := testBlockID(0, topShard, 10)
	tree := cell.BeginCell().
		MustStoreUInt(0, 1).
		MustStoreBuilder(testShardTopContextDescriptor(t, oldShard, 7).ToBuilder()).
		EndCell()
	shardHashes := cell.NewDict(32)
	if err := shardHashes.SetIntKey(
		big.NewInt(0),
		cell.BeginCell().MustStoreRef(tree).EndCell(),
	); err != nil {
		t.Fatal(err)
	}
	config := cell.NewDict(32)
	if err := config.SetIntKey(big.NewInt(0), cell.BeginCell().EndCell()); err != nil {
		t.Fatal(err)
	}
	extra := cell.BeginCell().
		MustStoreUInt(0xcc26, 16).
		MustStoreDict(shardHashes).
		MustStoreSlice(make([]byte, 32), 256).
		MustStoreRef(config.AsCell()).
		MustStoreRef(testMonitorMasterInfo(t, testMasterBlockID(1), false)).
		MustStoreCoins(0).
		MustStoreDict(nil).
		EndCell()
	state := &storage.BlockState{
		Block: masterchain,
		Cell:  cell.BeginCell().EndCell(),
		Parsed: &tlb.ShardStateUnsplit{
			Seqno:        masterchain.SeqNo,
			VertSeqno:    3,
			McStateExtra: extra,
		},
	}
	validatorContext, err := blockproof.NewShardTopValidatorContext(state)
	if err != nil {
		t.Fatal(err)
	}
	block := testBlockID(0, topShard, 11)
	description := &p2p.ShardBlockDescription{
		Block:         block,
		CatchainSeqno: 7,
		Chain: []p2p.ShardDescriptionLink{{
			Block:          block,
			PrevRefs:       []ton.BlockIDExt{oldShard},
			MasterchainRef: masterchain.Copy(),
			VertSeqno:      3,
		}},
	}

	return &shardTopValidationView{
		masterchain:      state,
		validatorContext: validatorContext,
	}, description
}

func testShardTopContextDescriptor(t testing.TB, block ton.BlockIDExt, catchainSeqno uint32) *cell.Cell {
	t.Helper()

	description, err := tlb.ToCell(&tlb.ShardDesc{
		SeqNo:              block.SeqNo,
		RootHash:           block.RootHash,
		FileHash:           block.FileHash,
		NextCatchainSeqNo:  catchainSeqno,
		NextValidatorShard: block.Shard,
		SplitMergeAt:       tlb.FutureSplitMergeNone{},
	})
	if err != nil {
		t.Fatal(err)
	}

	return description
}

func cloneShardTopContextDescription(description *p2p.ShardBlockDescription) *p2p.ShardBlockDescription {
	cloned := *description
	cloned.Block = *description.Block.Copy()
	cloned.Chain = append([]p2p.ShardDescriptionLink(nil), description.Chain...)
	for index := range cloned.Chain {
		cloned.Chain[index].Block = *description.Chain[index].Block.Copy()
		cloned.Chain[index].PrevRefs = make([]ton.BlockIDExt, len(description.Chain[index].PrevRefs))
		for predecessor := range cloned.Chain[index].PrevRefs {
			cloned.Chain[index].PrevRefs[predecessor] = *description.Chain[index].PrevRefs[predecessor].Copy()
		}
		cloned.Chain[index].MasterchainRef = description.Chain[index].MasterchainRef.Copy()
	}

	return &cloned
}
