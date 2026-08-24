package msgpool

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"testing"

	"github.com/xssnick/tonutils-go/address"
)

func TestAccountPrefixFromAddress(t *testing.T) {
	data := make([]byte, 32)
	copy(data, []byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef})

	type prefixTestCase struct {
		name string
		addr *address.Address
		want AccountPrefix
	}
	tests := []prefixTestCase{
		{
			name: "standard",
			addr: address.NewAddress(0, 0xff, data),
			want: AccountPrefix{Workchain: -1, Prefix: 0x0123456789abcdef},
		},
		{
			name: "variable",
			addr: address.NewAddressVar(0, 7, 64, data[:8]),
			want: AccountPrefix{Workchain: 7, Prefix: 0x0123456789abcdef},
		},
		{
			name: "variable extended workchain",
			addr: address.NewAddressVar(0, 200, 256, data),
			want: AccountPrefix{Workchain: 200, Prefix: 0x0123456789abcdef},
		},
		{
			name: "anycast one bit",
			addr: address.NewAddress(0, 0, data).
				WithAnycast(address.NewAnycast(1, []byte{0x80})),
			want: AccountPrefix{Workchain: 0, Prefix: 0x8123456789abcdef},
		},
		{
			name: "anycast partial byte",
			addr: address.NewAddress(0, 0, data).
				WithAnycast(address.NewAnycast(5, []byte{0xd8})),
			want: AccountPrefix{Workchain: 0, Prefix: 0xd923456789abcdef},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := AccountPrefixFromAddress(test.addr)
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("prefix = %+v, want %+v", got, test.want)
			}
		})
	}
}

func TestAccountPrefixFromAddressRejectsMalformedInput(t *testing.T) {
	validData := make([]byte, 32)
	type malformedAddressTestCase struct {
		name string
		addr *address.Address
	}
	tests := []malformedAddressTestCase{
		{name: "none", addr: address.NewAddressNone()},
		{name: "external", addr: address.NewAddressExt(0, 64, make([]byte, 8))},
		{name: "short variable", addr: address.NewAddressVar(0, 0, 63, make([]byte, 8))},
		{name: "truncated variable", addr: address.NewAddressVar(0, 0, 256, make([]byte, 31))},
		{name: "variable basechain", addr: address.NewAddressVar(0, 0, 64, make([]byte, 8))},
		{name: "variable masterchain", addr: address.NewAddressVar(0, -1, 64, make([]byte, 8))},
		{name: "variable standard form", addr: address.NewAddressVar(0, 17, 256, validData)},
		{name: "short data", addr: address.NewAddress(0, 0, make([]byte, 31))},
		{
			name: "zero anycast depth",
			addr: address.NewAddress(0, 0, validData).
				WithAnycast(address.NewAnycast(0, nil)),
		},
		{
			name: "large anycast depth",
			addr: address.NewAddress(0, 0, validData).
				WithAnycast(address.NewAnycast(31, make([]byte, 4))),
		},
		{
			name: "short anycast prefix",
			addr: address.NewAddress(0, 0, validData).
				WithAnycast(address.NewAnycast(9, []byte{0x80})),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := AccountPrefixFromAddress(test.addr); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestInterpolatePrefix(t *testing.T) {
	toInt32 := func(value uint32) int32 { return int32(value) }
	src := AccountPrefix{Workchain: toInt32(0x12345678), Prefix: 0x0123456789abcdef}
	dst := AccountPrefix{Workchain: toInt32(0x9abcdef0), Prefix: 0xfedcba9876543210}

	type interpolationTestCase struct {
		bits int
		want AccountPrefix
	}
	tests := []interpolationTestCase{
		{bits: -1, want: src},
		{bits: 0, want: src},
		{bits: 1, want: AccountPrefix{Workchain: toInt32(0x92345678), Prefix: src.Prefix}},
		{bits: 16, want: AccountPrefix{Workchain: toInt32(0x9abc5678), Prefix: src.Prefix}},
		{bits: 32, want: AccountPrefix{Workchain: dst.Workchain, Prefix: src.Prefix}},
		{bits: 36, want: AccountPrefix{Workchain: dst.Workchain, Prefix: 0xf123456789abcdef}},
		{bits: 92, want: AccountPrefix{Workchain: dst.Workchain, Prefix: 0xfedcba987654321f}},
		{bits: 96, want: dst},
		{bits: 100, want: dst},
	}

	for _, test := range tests {
		if got := InterpolatePrefix(src, dst, test.bits); got != test.want {
			t.Errorf("interpolate %d bits = %+v, want %+v", test.bits, got, test.want)
		}
	}
}

func TestInterpolatePrefixEveryBit(t *testing.T) {
	src := AccountPrefix{Workchain: -0x1234567, Prefix: 0x0123456789abcdef}
	dst := AccountPrefix{Workchain: 0x2345678, Prefix: 0xfedcba9876543210}

	var srcBits, dstBits [12]byte
	binary.BigEndian.PutUint32(srcBits[:4], uint32(src.Workchain))
	binary.BigEndian.PutUint64(srcBits[4:], src.Prefix)
	binary.BigEndian.PutUint32(dstBits[:4], uint32(dst.Workchain))
	binary.BigEndian.PutUint64(dstBits[4:], dst.Prefix)

	for dstBitCount := 0; dstBitCount <= RoutingAddressBits; dstBitCount++ {
		expectedBits := srcBits
		wholeBytes := dstBitCount / 8
		copy(expectedBits[:wholeBytes], dstBits[:wholeBytes])
		if remaining := dstBitCount % 8; remaining != 0 {
			mask := byte(0xff << (8 - remaining))
			expectedBits[wholeBytes] = expectedBits[wholeBytes]&^mask | dstBits[wholeBytes]&mask
		}
		want := AccountPrefix{
			Workchain: int32(binary.BigEndian.Uint32(expectedBits[:4])),
			Prefix:    binary.BigEndian.Uint64(expectedBits[4:]),
		}

		if got := InterpolatePrefix(src, dst, dstBitCount); got != want {
			t.Fatalf("interpolate %d bits = %+v, want %+v", dstBitCount, got, want)
		}
	}
}

func TestMakeQueueKey(t *testing.T) {
	var hash [32]byte
	for i := range hash {
		hash[i] = byte(i)
	}

	hop := AccountPrefix{Workchain: -1, Prefix: 0x0123456789abcdef}
	key := MakeQueueKey(hop, hash)
	want, err := hex.DecodeString("ffffffff0123456789abcdef000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(key[:], want) {
		t.Fatalf("queue key = %x, want %x", key, want)
	}

	if got := key.NextHop(); got != hop {
		t.Fatalf("next hop = %+v, want %+v", got, hop)
	}
	if got := key.MsgHash(); got != hash {
		t.Fatalf("message hash = %x, want %x", got, hash)
	}
}

// The pruning question, asked once per subtree instead of once per entry. Its
// two halves are easy to get wrong in opposite directions: answering "no" for a
// subtree that does hold the destination silently drops messages, and answering
// "yes" for everything makes the whole thing a no-op that still looks like it
// works.
func TestRoutesUnderPrunesOnlySubtreesTheDestinationCannotReach(t *testing.T) {
	var router destinationRouter
	router.workchains = map[int32]*routeNode{}
	// A split basechain: destination 0 owns the 0-prefixed half, destination 1
	// the 1-prefixed half. Masterchain belongs to neither.
	// Shard ids carry their depth in the position of the lowest set bit: the two
	// halves of a once-split basechain are 0x4000... and 0xC000....
	left := ShardIdent{Workchain: 0, Shard: 1 << 62}
	right := ShardIdent{Workchain: 0, Shard: 3 << 62}
	router.insert(left, 0)
	router.insert(right, 1)

	for _, tc := range []struct {
		name  string
		route AccountPrefix
		bits  int
		index int
		want  bool
	}{
		{name: "left half, left destination", route: AccountPrefix{Workchain: 0, Prefix: 0}, bits: 1, index: 0, want: true},
		{name: "left half, right destination", route: AccountPrefix{Workchain: 0, Prefix: 0}, bits: 1, index: 1, want: false},
		{name: "right half, right destination", route: AccountPrefix{Workchain: 0, Prefix: 1 << 63}, bits: 1, index: 1, want: true},
		{name: "right half, left destination", route: AccountPrefix{Workchain: 0, Prefix: 1 << 63}, bits: 1, index: 0, want: false},
		{
			// Nothing known past the workchain: both halves are still reachable,
			// so nothing may be pruned.
			name: "workchain only", route: AccountPrefix{Workchain: 0}, bits: 0, index: 1, want: true,
		},
		{name: "another workchain entirely", route: AccountPrefix{Workchain: -1}, bits: 0, index: 0, want: false},
	} {
		if got := router.routesUnder(tc.route, tc.bits, tc.index); got != tc.want {
			t.Errorf("%s: routesUnder = %t, want %t", tc.name, got, tc.want)
		}
	}
}

// A destination that owns a node routes everything below it, however many bits
// the caller happens to know. Losing this makes the filter drop the entire queue
// on an unsplit shard, which is every shard until it splits.
func TestRoutesUnderKeepsEverythingBelowAnOwnedNode(t *testing.T) {
	var router destinationRouter
	router.workchains = map[int32]*routeNode{}
	router.insert(ShardIdent{Workchain: 0, Shard: ShardAll}, 0)

	for _, bits := range []int{0, 1, 6, 32, 64} {
		route := AccountPrefix{Workchain: 0, Prefix: 0xDEADBEEF00000000}
		if !router.routesUnder(route, bits, 0) {
			t.Errorf("%d known bits: a destination owning the workchain root did not match", bits)
		}
	}
}
