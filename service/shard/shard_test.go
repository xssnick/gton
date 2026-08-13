package shard

import (
	"errors"
	"math/bits"
	"testing"
)

var (
	root      = int64(-1 << 63)
	left      = int64(0x4000000000000000)
	right     = int64(-0x4000000000000000)
	leftRight = int64(0x6000000000000000)
	rightLeft = int64(-0x6000000000000000)
	deepest   = int64(1)
)

func TestPrefixLength(t *testing.T) {
	tests := []struct {
		name     string
		id       int64
		expected uint32
		err      error
	}{
		{name: "invalid zero", id: 0, err: ErrInvalidID},
		{name: "root", id: root, expected: 0},
		{name: "first level", id: left, expected: 1},
		{name: "second level", id: leftRight, expected: 2},
		{name: "deepest", id: deepest, expected: MaxPrefixLength},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := PrefixLength(test.id)
			if !errors.Is(err, test.err) {
				t.Fatalf("PrefixLength(%016x) error = %v, want %v", uint64(test.id), err, test.err)
			}
			if got != test.expected {
				t.Fatalf("PrefixLength(%016x) = %d, want %d", uint64(test.id), got, test.expected)
			}
		})
	}
}

func TestIntersectsAndContains(t *testing.T) {
	tests := []struct {
		name       string
		left       int64
		right      int64
		intersects bool
		contains   bool
	}{
		{name: "invalid parent", left: 0, right: leftRight},
		{name: "invalid child", left: left, right: 0},
		{name: "root contains child", left: root, right: leftRight, intersects: true, contains: true},
		{name: "parent contains child", left: left, right: leftRight, intersects: true, contains: true},
		{name: "child does not contain parent", left: leftRight, right: left, intersects: true},
		{name: "siblings are disjoint", left: left, right: right},
		{name: "opposite grandchildren are disjoint", left: leftRight, right: rightLeft},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := Intersects(test.left, test.right); got != test.intersects {
				t.Fatalf("Intersects(%016x, %016x) = %t, want %t", uint64(test.left), uint64(test.right), got, test.intersects)
			}
			if got := Contains(test.left, test.right); got != test.contains {
				t.Fatalf("Contains(%016x, %016x) = %t, want %t", uint64(test.left), uint64(test.right), got, test.contains)
			}
		})
	}
}

func TestIsNeighbor(t *testing.T) {
	tests := []struct {
		name                          string
		leftWorkchain, rightWorkchain int32
		left, right                   int64
		want                          bool
	}{
		{name: "invalid", left: 0, right: root},
		{name: "masterchain", leftWorkchain: -1, left: root, rightWorkchain: 0, right: leftRight, want: true},
		{name: "workchain root contains deep shard", left: root, right: 0x1800000000000000, want: true},
		{name: "intersecting", left: left, right: leftRight, want: true},
		{name: "deep hypercube neighbor", left: 0x1800000000000000, right: -0x2000000000000000, want: true},
		{name: "cross workchain intersecting", leftWorkchain: 0, left: left, rightWorkchain: 1, right: leftRight, want: true},
		{name: "cross workchain disjoint routing coordinate", leftWorkchain: 0, left: left, rightWorkchain: 1, right: right},
		{name: "cross workchain disjoint", leftWorkchain: 0, left: 0x0080000000000000, rightWorkchain: 1, right: 0x1280000000000000},
		{name: "same workchain opposite halves", left: left, right: right, want: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := IsNeighbor(test.leftWorkchain, test.left, test.rightWorkchain, test.right); got != test.want {
				t.Fatalf("IsNeighbor(%d:%016x, %d:%016x) = %t, want %t",
					test.leftWorkchain, uint64(test.left), test.rightWorkchain, uint64(test.right), got, test.want)
			}
		})
	}
}

func TestIsNeighborMatchesCppMask(t *testing.T) {
	shards := []int64{root}
	level := []int64{root}
	for range 6 {
		next := make([]int64, 0, len(level)*2)
		for _, parent := range level {
			leftChild, err := Child(parent, true)
			if err != nil {
				t.Fatalf("left child of %016x: %v", uint64(parent), err)
			}
			rightChild, err := Child(parent, false)
			if err != nil {
				t.Fatalf("right child of %016x: %v", uint64(parent), err)
			}
			next = append(next, leftChild, rightChild)
		}
		shards = append(shards, next...)
		level = next
	}

	for _, leftShard := range shards {
		for _, rightShard := range shards {
			for _, workchains := range [][2]int32{{0, 0}, {0, 1}, {-1, 0}} {
				want := cppIsNeighbor(workchains[0], leftShard, workchains[1], rightShard)
				got := IsNeighbor(workchains[0], leftShard, workchains[1], rightShard)
				if got != want {
					t.Fatalf("IsNeighbor(%d:%016x, %d:%016x) = %t, C++ mask wants %t",
						workchains[0], uint64(leftShard), workchains[1], uint64(rightShard), got, want)
				}
			}
		}
	}
}

// cppIsNeighbor spells the reference's bits_negate64 as ~x+1 so the test
// guards the translation boundary that IsNeighbor previously got wrong.
func cppIsNeighbor(leftWorkchain int32, left int64, rightWorkchain int32, right int64) bool {
	leftValue := uint64(left)
	rightValue := uint64(right)
	leftLowest := leftValue & (^leftValue + 1)
	rightLowest := rightValue & (^rightValue + 1)
	boundary := max(leftLowest, rightLowest) << 1
	difference := (leftValue ^ rightValue) & (^boundary + 1)
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

func TestChild(t *testing.T) {
	leftChild, err := Child(root, true)
	if err != nil {
		t.Fatalf("left child: %v", err)
	}
	rightChild, err := Child(root, false)
	if err != nil {
		t.Fatalf("right child: %v", err)
	}
	if leftChild != left || rightChild != right {
		t.Fatalf("root children = (%016x, %016x), want (%016x, %016x)", uint64(leftChild), uint64(rightChild), uint64(left), uint64(right))
	}
	if !IsDirectChild(root, leftChild) || !IsDirectChild(root, rightChild) {
		t.Fatal("root children must be direct")
	}
	if _, err := Child(0, true); !errors.Is(err, ErrInvalidID) {
		t.Fatalf("zero child error = %v, want ErrInvalidID", err)
	}
	if _, err := Child(deepest, true); !errors.Is(err, ErrCannotSplitID) {
		t.Fatalf("deepest child error = %v, want ErrCannotSplitID", err)
	}
}

func TestAncestorAndParent(t *testing.T) {
	ancestor, err := Ancestor(leftRight, 1)
	if err != nil {
		t.Fatalf("ancestor: %v", err)
	}
	if ancestor != left {
		t.Fatalf("ancestor = %016x, want %016x", uint64(ancestor), uint64(left))
	}

	parent, err := Parent(leftRight)
	if err != nil {
		t.Fatalf("parent: %v", err)
	}
	if parent != left {
		t.Fatalf("parent = %016x, want %016x", uint64(parent), uint64(left))
	}
	if _, err := Ancestor(left, 2); !errors.Is(err, ErrInvalidDepth) {
		t.Fatalf("deeper ancestor error = %v, want ErrInvalidDepth", err)
	}
	if _, err := Parent(root); !errors.Is(err, ErrRootHasNoParent) {
		t.Fatalf("root parent error = %v, want ErrRootHasNoParent", err)
	}
}

func TestPrefix(t *testing.T) {
	tests := []struct {
		name        string
		id          int64
		depth       uint32
		expected    int64
		expectedErr error
	}{
		{name: "invalid zero", expectedErr: ErrInvalidID},
		{name: "disabled normalization", id: leftRight, expected: leftRight},
		{name: "normalize to parent", id: leftRight, depth: 1, expected: left},
		{name: "deepest prefix", id: rightLeft, depth: MaxPrefixLength, expected: int64(uint64(rightLeft) | 1)},
		{name: "invalid depth", id: root, depth: MaxPrefixLength + 1, expectedErr: ErrInvalidDepth},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := Prefix(test.id, test.depth)
			if !errors.Is(err, test.expectedErr) {
				t.Fatalf("Prefix(%016x, %d) error = %v, want %v", uint64(test.id), test.depth, err, test.expectedErr)
			}
			if got != test.expected {
				t.Fatalf("Prefix(%016x, %d) = %016x, want %016x", uint64(test.id), test.depth, uint64(got), uint64(test.expected))
			}
		})
	}
}

func TestFromAccountPrefix(t *testing.T) {
	const prefix = uint64(0x6fffffffffffffff)

	got, err := FromAccountPrefix(prefix, 0)
	if err != nil || got != Root {
		t.Fatalf("root = %016x, want %016x", uint64(got), uint64(root))
	}
	got, err = FromAccountPrefix(prefix, 1)
	if err != nil || got != left {
		t.Fatalf("depth 1 = %016x, want %016x", uint64(got), uint64(left))
	}
	got, err = FromAccountPrefix(prefix, 2)
	if err != nil || got != leftRight {
		t.Fatalf("depth 2 = %016x, want %016x", uint64(got), uint64(leftRight))
	}
	if _, err = FromAccountPrefix(prefix, MaxPrefixLength+1); !errors.Is(err, ErrInvalidDepth) {
		t.Fatalf("invalid depth error = %v, want ErrInvalidDepth", err)
	}
}

func BenchmarkPrefixLength(b *testing.B) {
	var result uint32
	for b.Loop() {
		result, _ = PrefixLength(leftRight)
	}
	_ = result
}

func BenchmarkIntersects(b *testing.B) {
	var result bool
	for b.Loop() {
		result = Intersects(left, leftRight)
	}
	_ = result
}

func BenchmarkFromAccountPrefix(b *testing.B) {
	var result int64
	for b.Loop() {
		result, _ = FromAccountPrefix(0x6fffffffffffffff, 60)
	}
	_ = result
}
