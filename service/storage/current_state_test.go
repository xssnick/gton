package storage

import (
	"bytes"
	"testing"

	"github.com/xssnick/tonutils-go/ton"
)

func TestCurrentStateBlockRefsEqual(t *testing.T) {
	base := testCurrentStateForEquality()

	if !base.BlockRefsEqual(base) {
		t.Fatal("state does not match itself")
	}

	seqnoChanged := testCurrentStateForEquality()
	seqnoChanged.ShardClientSeqno++
	if seqnoChanged.BlockRefsEqual(base) {
		t.Fatal("block refs ignored shard client seqno")
	}

	masterChanged := testCurrentStateForEquality()
	masterChanged.Masterchain.Block.SeqNo++
	if masterChanged.BlockRefsEqual(base) {
		t.Fatal("block refs ignored masterchain block change")
	}

	shardChanged := testCurrentStateForEquality()
	key := SortedShardKeys(shardChanged.Shards)[0]
	shard := shardChanged.Shards[key]
	shard.Block.SeqNo++
	shardChanged.Shards[key] = shard
	if shardChanged.BlockRefsEqual(base) {
		t.Fatal("block refs ignored shard block change")
	}

	missingShard := testCurrentStateForEquality()
	clear(missingShard.Shards)
	if missingShard.BlockRefsEqual(base) {
		t.Fatal("block refs ignored shard count change")
	}

	differentShardKey := testCurrentStateWithDifferentShardKey()
	if differentShardKey.BlockRefsEqual(base) {
		t.Fatal("block refs ignored shard key change")
	}
}

func TestCurrentStateRootsEqual(t *testing.T) {
	base := testCurrentStateForEquality()

	if !base.RootsEqual(base) {
		t.Fatal("state roots do not match themselves")
	}

	masterChanged := testCurrentStateForEquality()
	masterChanged.Masterchain.StateRootHash[0] ^= 0xff
	if masterChanged.RootsEqual(base) {
		t.Fatal("roots ignored masterchain root change")
	}

	shardChanged := testCurrentStateForEquality()
	key := SortedShardKeys(shardChanged.Shards)[0]
	shard := shardChanged.Shards[key]
	shard.StateRootHash[0] ^= 0xff
	shardChanged.Shards[key] = shard
	if shardChanged.RootsEqual(base) {
		t.Fatal("roots ignored shard root change")
	}

	missingShard := testCurrentStateForEquality()
	clear(missingShard.Shards)
	if missingShard.RootsEqual(base) {
		t.Fatal("roots ignored shard count change")
	}

	differentShardKey := testCurrentStateWithDifferentShardKey()
	if differentShardKey.RootsEqual(base) {
		t.Fatal("roots ignored shard key change")
	}
}

func testCurrentStateWithDifferentShardKey() *CurrentState {
	state := testCurrentStateForEquality()
	key := SortedShardKeys(state.Shards)[0]
	shard := state.Shards[key]
	delete(state.Shards, key)
	state.Shards[ShardKey{Workchain: key.Workchain, Shard: key.Shard >> 1}] = shard
	return state
}

func testCurrentStateForEquality() *CurrentState {
	master := ton.BlockIDExt{
		Workchain: -1,
		Shard:     -1 << 63,
		SeqNo:     10,
		RootHash:  bytes.Repeat([]byte{0x11}, 32),
		FileHash:  bytes.Repeat([]byte{0x12}, 32),
	}
	shard := ton.BlockIDExt{
		Workchain: 0,
		Shard:     0x4000000000000000,
		SeqNo:     20,
		RootHash:  bytes.Repeat([]byte{0x21}, 32),
		FileHash:  bytes.Repeat([]byte{0x22}, 32),
	}

	return &CurrentState{
		ShardClientSeqno: 7,
		Masterchain: BlockState{
			Block:         master,
			StateRootHash: bytes.Repeat([]byte{0x31}, 32),
		},
		Shards: map[ShardKey]BlockState{
			ShardKeyFromBlock(shard): {
				Block:         shard,
				StateRootHash: bytes.Repeat([]byte{0x41}, 32),
			},
		},
	}
}
