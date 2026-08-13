package storage

import "bytes"

// BlockRefsEqual reports whether both states identify the same durable
// masterchain and shard blocks at the same shard-client position.
func (s *CurrentState) BlockRefsEqual(other *CurrentState) bool {
	if s.ShardClientSeqno != other.ShardClientSeqno {
		return false
	}
	if !s.Masterchain.Block.Equals(&other.Masterchain.Block) {
		return false
	}
	if len(s.Shards) != len(other.Shards) {
		return false
	}
	for _, key := range SortedShardKeys(s.Shards) {
		shard := s.Shards[key]
		otherShard, ok := other.Shards[key]
		if !ok {
			return false
		}
		if !shard.Block.Equals(&otherShard.Block) {
			return false
		}
	}
	return true
}

// RootsEqual reports whether both states contain the same masterchain and
// shard state roots.
func (s *CurrentState) RootsEqual(other *CurrentState) bool {
	if !bytes.Equal(s.Masterchain.StateRootHash, other.Masterchain.StateRootHash) {
		return false
	}
	if len(s.Shards) != len(other.Shards) {
		return false
	}
	for _, key := range SortedShardKeys(s.Shards) {
		shard := s.Shards[key]
		otherShard, ok := other.Shards[key]
		if !ok {
			return false
		}
		if !bytes.Equal(shard.StateRootHash, otherShard.StateRootHash) {
			return false
		}
	}
	return true
}
