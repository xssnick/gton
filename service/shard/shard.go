// Package shard owns the TON shard-prefix domain rules.
package shard

import (
	"errors"
	"math/bits"
)

const MaxPrefixLength uint32 = 63

const Root int64 = -1 << 63

var (
	ErrInvalidID       = errors.New("shard: invalid id")
	ErrInvalidDepth    = errors.New("shard: invalid prefix depth")
	ErrCannotSplitID   = errors.New("shard: id cannot be split further")
	ErrRootHasNoParent = errors.New("shard: root has no parent")
)

// Validate rejects zero, which is not a TON shard identifier. In particular,
// zero never denotes the masterchain: its root shard is 0x8000000000000000.
func Validate(id int64) error {
	if id == 0 {
		return ErrInvalidID
	}
	return nil
}

// PrefixLength returns the number of prefix bits before the shard's tag bit.
func PrefixLength(id int64) (uint32, error) {
	if err := Validate(id); err != nil {
		return 0, err
	}
	return prefixLength(uint64(id)), nil
}

// Intersects reports whether two valid shard prefixes cover any common shard.
// Invalid identifiers never intersect.
func Intersects(left, right int64) bool {
	if left == 0 || right == 0 {
		return false
	}

	leftValue := uint64(left)
	rightValue := uint64(right)
	depth := prefixLength(leftValue)
	if rightDepth := prefixLength(rightValue); rightDepth < depth {
		depth = rightDepth
	}
	if depth == 0 {
		return true
	}

	mask := ^uint64(0) << (64 - depth)
	return leftValue&mask == rightValue&mask
}

// IsNeighbor mirrors ShardConfig::is_neighbor. Besides intersecting shard
// prefixes it includes the hypercube-adjacent shards whose prefixes differ in
// exactly one four-bit routing group. The masterchain is adjacent to every
// shard.
func IsNeighbor(leftWorkchain int32, left int64, rightWorkchain int32, right int64) bool {
	if left == 0 || right == 0 {
		return false
	}
	if (leftWorkchain == -1 && left == Root) || (rightWorkchain == -1 && right == Root) {
		return true
	}

	leftValue := uint64(left)
	rightValue := uint64(right)
	leftLowest := leftValue & -leftValue
	rightLowest := rightValue & -rightValue
	maskBoundary := max(leftLowest, rightLowest) << 1
	// C++ bits_negate64 is two's-complement negation (~x + 1), not a
	// bitwise complement. For this power-of-two boundary it keeps the prefix
	// above the shallower shard tag and clears every lower bit.
	difference := (leftValue ^ rightValue) & -maskBoundary
	if difference == 0 {
		return true
	}
	if leftWorkchain != rightWorkchain {
		return false
	}

	leadingGroups := bits.LeadingZeros64(difference) >> 2
	trailingGroups := bits.TrailingZeros64(difference) >> 2
	return leadingGroups+trailingGroups == 15
}

// Contains reports whether child is inside parent, including equality.
// Invalid identifiers are never contained.
func Contains(parent, child int64) bool {
	if parent == 0 || child == 0 {
		return false
	}

	parentValue := uint64(parent)
	childValue := uint64(child)
	parentDepth := prefixLength(parentValue)
	if prefixLength(childValue) < parentDepth {
		return false
	}
	if parentDepth == 0 {
		return true
	}

	mask := ^uint64(0) << (64 - parentDepth)
	return parentValue&mask == childValue&mask
}

// Child returns the direct left or right child of parent.
func Child(parent int64, left bool) (int64, error) {
	if err := Validate(parent); err != nil {
		return 0, err
	}

	value := uint64(parent)
	step := (value & -value) >> 1
	if step == 0 {
		return 0, ErrCannotSplitID
	}
	if left {
		return int64(value - step), nil
	}
	return int64(value + step), nil
}

// IsDirectChild reports whether child is one split below parent.
func IsDirectChild(parent, child int64) bool {
	if parent == 0 || child == 0 {
		return false
	}
	return prefixLength(uint64(child)) == prefixLength(uint64(parent))+1 && Contains(parent, child)
}

// Ancestor returns the prefix of id at depth. The requested depth must not be
// deeper than id itself.
func Ancestor(id int64, depth uint32) (int64, error) {
	idDepth, err := PrefixLength(id)
	if err != nil {
		return 0, err
	}
	if depth > idDepth {
		return 0, ErrInvalidDepth
	}
	if depth == 0 {
		return Root, nil
	}

	value := uint64(id)
	mask := ^uint64(0) << (64 - depth)
	return int64((value & mask) | (uint64(1) << (63 - depth))), nil
}

// Parent returns the direct parent of id.
func Parent(id int64) (int64, error) {
	depth, err := PrefixLength(id)
	if err != nil {
		return 0, err
	}
	if depth == 0 {
		return 0, ErrRootHasNoParent
	}
	return Ancestor(id, depth-1)
}

// FromAccountPrefix returns the shard at depth along an account-address
// prefix. Production traversal is bounded to TON's stricter 60-bit account
// depth, while the domain representation supports up to MaxPrefixLength.
func FromAccountPrefix(prefix uint64, depth uint32) (int64, error) {
	if depth > MaxPrefixLength {
		return 0, ErrInvalidDepth
	}
	if depth == 0 {
		return Root, nil
	}

	marker := uint64(1) << (63 - depth)
	return int64((prefix & ^(marker - 1)) | marker), nil
}

// Prefix returns the shard prefix used for a configured split depth. A zero
// depth disables normalization and therefore preserves id. This is the
// established archive-index representation and must remain stable.
func Prefix(id int64, splitDepth uint32) (int64, error) {
	if err := Validate(id); err != nil {
		return 0, err
	}
	if splitDepth > MaxPrefixLength {
		return 0, ErrInvalidDepth
	}
	if splitDepth == 0 {
		return id, nil
	}

	value := uint64(id) | 1
	if splitDepth == MaxPrefixLength {
		return int64(value), nil
	}

	mask := ^uint64(0) << (64 - splitDepth)
	return int64((value & mask) | (uint64(1) << (63 - splitDepth))), nil
}

func prefixLength(id uint64) uint32 {
	return uint32(63 - bits.TrailingZeros64(id))
}
