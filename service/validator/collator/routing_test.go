package collator

import (
	"bytes"
	"testing"

	"github.com/xssnick/tonutils-go/address"

	"github.com/xssnick/gton/service/validator/msgpool"
)

func TestAccountIDFromAddressRewritesOnlyAnycastPrefix(t *testing.T) {
	data := make([]byte, 32)
	for i := range data {
		data[i] = byte(i)
	}
	addr := address.NewAddress(0, 0, data).
		WithAnycast(address.NewAnycast(13, []byte{0xab, 0xc8}))

	got, err := accountIDFromAddress(addr)
	if err != nil {
		t.Fatal(err)
	}
	want := [32]byte{
		0xab, 0xc9, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
		0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
		0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
		0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
	}
	if got != want {
		t.Fatalf("account ID = %x, want %x", got, want)
	}
}

func TestAccountIDFromAddressRejectsVariableAddress(t *testing.T) {
	data := bytes.Repeat([]byte{0x5a}, 32)
	if _, err := accountIDFromAddress(address.NewAddressVar(0, 17, 256, data)); err == nil {
		t.Fatal("expected an error")
	}
}

func TestAccountIDFromAddressEveryAnycastDepth(t *testing.T) {
	data := make([]byte, 32)
	for i := range data {
		data[i] = byte(i*17 + 3)
	}
	rewrite := [...]byte{0xde, 0xad, 0xbe, 0xef}

	for depth := 1; depth <= 30; depth++ {
		addr := address.NewAddress(0, 0, data).
			WithAnycast(address.NewAnycast(uint(depth), rewrite[:(depth+7)/8]))
		got, err := accountIDFromAddress(addr)
		if err != nil {
			t.Fatal(err)
		}

		var want [32]byte
		copy(want[:], data)
		for bit := 0; bit < depth; bit++ {
			mask := byte(1 << (7 - bit%8))
			want[bit/8] = want[bit/8]&^mask | rewrite[bit/8]&mask
		}
		if got != want {
			t.Fatalf("depth %d account ID = %x, want %x", depth, got, want)
		}
	}
}

func TestPerformHypercubeRouting(t *testing.T) {
	left := msgpool.ShardIdent{Workchain: 0, Shard: 0x4000000000000000}
	root := msgpool.ShardIdent{Workchain: 0, Shard: 0x8000000000000000}
	src := msgpool.AccountPrefix{Workchain: 0, Prefix: 0x1000000000000000}

	type routingTestCase struct {
		name        string
		src         msgpool.AccountPrefix
		dst         msgpool.AccountPrefix
		current     msgpool.ShardIdent
		used        int
		wantTransit int
		wantNext    int
		wantErr     bool
	}
	tests := []routingTestCase{
		{
			name:        "destination in shard",
			src:         src,
			dst:         msgpool.AccountPrefix{Workchain: 0, Prefix: 0x3000000000000000},
			current:     left,
			used:        32,
			wantTransit: 96,
			wantNext:    96,
		},
		{
			name:        "next hypercube step",
			src:         src,
			dst:         msgpool.AccountPrefix{Workchain: 0, Prefix: 0x9000000000000000},
			current:     left,
			used:        32,
			wantTransit: 32,
			wantNext:    36,
		},
		{
			name:    "transit outside shard",
			src:     src,
			dst:     msgpool.AccountPrefix{Workchain: 0, Prefix: 0x9000000000000000},
			current: left,
			used:    36,
			wantErr: true,
		},
		{
			name:        "to masterchain",
			src:         src,
			dst:         msgpool.AccountPrefix{Workchain: -1, Prefix: 0x9000000000000000},
			current:     left,
			used:        0,
			wantTransit: 0,
			wantNext:    96,
		},
		{
			name:        "from masterchain",
			src:         msgpool.AccountPrefix{Workchain: -1, Prefix: 1},
			dst:         msgpool.AccountPrefix{Workchain: 0, Prefix: 2},
			current:     msgpool.ShardIdent{Workchain: -1, Shard: 0x8000000000000000},
			used:        0,
			wantTransit: 0,
			wantNext:    96,
		},
		{
			name:        "different workchain",
			src:         src,
			dst:         msgpool.AccountPrefix{Workchain: 1, Prefix: 0x9000000000000000},
			current:     root,
			used:        0,
			wantTransit: 0,
			wantNext:    32,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			transit, next, err := performHypercubeRouting(test.src, test.dst, test.current, test.used)
			if test.wantErr {
				if err == nil {
					t.Fatal("expected an error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if transit != test.wantTransit || next != test.wantNext {
				t.Fatalf("routing = (%d, %d), want (%d, %d)", transit, next, test.wantTransit, test.wantNext)
			}
		})
	}
}

func TestPerformHypercubeRoutingEveryShardDepth(t *testing.T) {
	src := msgpool.AccountPrefix{Workchain: 0}
	for depth := 1; depth <= 60; depth++ {
		current := msgpool.ShardIdent{Workchain: 0, Shard: uint64(1) << (63 - depth)}
		dst := msgpool.AccountPrefix{Workchain: 0, Prefix: uint64(1) << (64 - depth)}

		transitBits, nextBits, err := performHypercubeRouting(src, dst, current, 32)
		if err != nil {
			t.Fatalf("depth %d: %v", depth, err)
		}
		step := ((depth-1)/4 + 1) * 4
		if transitBits != 28+step || nextBits != 32+step {
			t.Fatalf("depth %d routing = (%d, %d), want (%d, %d)",
				depth, transitBits, nextBits, 28+step, 32+step)
		}
		if !current.ContainsPrefix(msgpool.InterpolatePrefix(src, dst, transitBits)) {
			t.Fatalf("depth %d transit hop left the current shard", depth)
		}
		if current.ContainsPrefix(msgpool.InterpolatePrefix(src, dst, nextBits)) {
			t.Fatalf("depth %d next hop did not leave the current shard", depth)
		}
	}
}

func TestShardContains(t *testing.T) {
	root := msgpool.ShardIdent{Workchain: 0, Shard: 0x8000000000000000}
	left := msgpool.ShardIdent{Workchain: 0, Shard: 0x4000000000000000}

	if !root.ContainsPrefix(msgpool.AccountPrefix{Prefix: 0xffffffffffffffff}) || !left.ContainsPrefix(msgpool.AccountPrefix{Prefix: 0x3fffffffffffffff}) {
		t.Fatal("expected prefix to be contained")
	}
	if left.ContainsPrefix(msgpool.AccountPrefix{Prefix: 0x8000000000000000}) {
		t.Fatal("left shard contains a right-half prefix")
	}
	if left.ContainsPrefix(msgpool.AccountPrefix{Workchain: -1, Prefix: 0x3fffffffffffffff}) {
		t.Fatal("shard contains a prefix from another workchain")
	}
}
