package storage

import "math/bits"

func ShardPrefixLength(shard int64) int {
	value := uint64(shard)
	if value == 0 {
		return 0
	}
	return 63 - bits.TrailingZeros64(value)
}

func ShardsIntersect(left int64, right int64) bool {
	leftDepth := ShardPrefixLength(left)
	rightDepth := ShardPrefixLength(right)
	depth := leftDepth
	if rightDepth < depth {
		depth = rightDepth
	}
	if depth <= 0 {
		return true
	}

	mask := ^uint64(0) << (64 - depth)
	return uint64(left)&mask == uint64(right)&mask
}

func ShardChild(shard int64, left bool) int64 {
	value := uint64(shard)
	bit := value & -value
	step := bit >> 1
	if left {
		return int64(value - step)
	}
	return int64(value + step)
}

func ShardIsDirectChild(parent int64, child int64) bool {
	parentDepth := ShardPrefixLength(parent)
	childDepth := ShardPrefixLength(child)
	if childDepth != parentDepth+1 {
		return false
	}
	return ShardsIntersect(parent, child)
}

func ShardPrefix(shard int64, splitDepth uint32) int64 {
	if splitDepth == 0 {
		return shard
	}
	if splitDepth >= 63 {
		return int64(uint64(shard) | 1)
	}

	value := uint64(shard) | 1
	mask := ^uint64(0) << (64 - splitDepth)
	return int64((value & mask) | (uint64(1) << (63 - splitDepth)))
}
