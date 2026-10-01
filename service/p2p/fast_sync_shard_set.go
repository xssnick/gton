package p2p

import (
	"cmp"
	"fmt"
	"slices"

	sharddomain "github.com/xssnick/gton/service/shard"
)

type FastSyncShardSet struct {
	shards []FastSyncShard
}

func NewFastSyncShardSet(shards []FastSyncShard) FastSyncShardSet {
	shards = slices.Clone(shards)
	slices.SortFunc(shards, compareFastSyncShard)
	shards = slices.Compact(shards)
	return FastSyncShardSet{shards: shards}
}

func (s FastSyncShardSet) Shards() []FastSyncShard {
	return slices.Clone(s.shards)
}

// shardsRef returns the backing slice; see adnlIDsRef for the contract.
func (s FastSyncShardSet) shardsRef() []FastSyncShard {
	return s.shards
}

func (s FastSyncShardSet) Select(requested FastSyncShard) (FastSyncShard, error) {
	current := requested
	for {
		depth, err := sharddomain.PrefixLength(current.Shard)
		if err != nil {
			return FastSyncShard{}, fmt.Errorf("invalid requested fast-sync shard %d:%016x: %w", current.Workchain, uint64(current.Shard), err)
		}
		if s.contains(current) {
			return current, nil
		}
		if depth == 0 {
			return FastSyncShard{}, ErrFastSyncNotFound
		}

		current.Shard, err = sharddomain.Parent(current.Shard)
		if err != nil {
			return FastSyncShard{}, err
		}
	}
}

func (s FastSyncShardSet) contains(shard FastSyncShard) bool {
	_, found := slices.BinarySearchFunc(s.shards, shard, compareFastSyncShard)
	return found
}

func compareFastSyncShard(left, right FastSyncShard) int {
	if order := cmp.Compare(left.Workchain, right.Workchain); order != 0 {
		return order
	}
	return cmp.Compare(uint64(left.Shard), uint64(right.Shard))
}
