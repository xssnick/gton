package groups

import (
	"bytes"
	"encoding/hex"
	"math"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/tl"
)

type sessionIDMutationCase struct {
	name   string
	mutate func(*SessionIDInput)
}

func TestGroupTLConstructorsAndLayout(t *testing.T) {
	t.Parallel()

	const (
		wantGroupMemberConstructor = uint32(0x8b9465e4)
		wantGroupNewConstructor    = uint32(0x9843a14d)
	)

	if got := tl.CRC(groupMemberTLSchema); got != wantGroupMemberConstructor {
		t.Fatalf("validator.groupMember constructor = %08x, want %08x", got, wantGroupMemberConstructor)
	}
	if got := tl.CRC(groupNewTLSchema); got != wantGroupNewConstructor {
		t.Fatalf("validator.groupNew constructor = %08x, want %08x", got, wantGroupNewConstructor)
	}

	configHash := sequentialBytes(0x00)
	publicKeyHash := sequentialBytes(0x20)
	adnl := sequentialBytes(0x40)
	boxedMember, err := tl.Serialize(GroupMemberTL{
		PublicKeyHash: publicKeyHash[:],
		ADNL:          adnl[:],
		Weight:        0x0102030405060708,
	}, true)
	if err != nil {
		t.Fatalf("serialize validator.groupMember: %v", err)
	}
	if got := boxedMember[:4]; !bytes.Equal(got, []byte{0xe4, 0x65, 0x94, 0x8b}) {
		t.Fatalf("boxed validator.groupMember constructor = %x, want e465948b", got)
	}

	serialized, err := tl.Serialize(GroupNewTL{
		Workchain:         -1,
		Shard:             math.MinInt64,
		VerticalSeqno:     0x11223344,
		LastKeyBlockSeqno: 0x55667788,
		CatchainSeqno:     0x99aabbcc,
		ConfigHash:        configHash[:],
		Members: []GroupMemberTL{{
			PublicKeyHash: publicKeyHash[:],
			ADNL:          adnl[:],
			Weight:        0x0102030405060708,
		}},
	}, true)
	if err != nil {
		t.Fatalf("serialize validator.groupNew: %v", err)
	}

	want := mustDecodeHex(t,
		"4da14398"+
			"ffffffff"+
			"0000000000000080"+
			"44332211"+
			"88776655"+
			"ccbbaa99"+
			"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"+
			"01000000"+
			"202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f"+
			"404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f"+
			"0807060504030201",
	)
	if !bytes.Equal(serialized, want) {
		t.Fatalf("validator.groupNew serialization\n got: %x\nwant: %x", serialized, want)
	}

	// The vector is bare in ton_api.tl: it has a length, but neither the
	// vector constructor nor validator.groupMember constructor is present.
	if got := serialized[60:64]; !bytes.Equal(got, []byte{1, 0, 0, 0}) {
		t.Fatalf("bare vector header = %x, want 01000000", got)
	}
	if got := serialized[64:96]; !bytes.Equal(got, publicKeyHash[:]) {
		t.Fatalf("first bare member begins with %x, want %x", got, publicKeyHash)
	}
}

func TestPublicKeyHash(t *testing.T) {
	t.Parallel()

	publicKey := sequentialBytes(0x00)
	got, err := PublicKeyHash(publicKey)
	if err != nil {
		t.Fatalf("PublicKeyHash: %v", err)
	}

	want := mustDecodeHash(t, "dac97a9aa4db0219421e1583c57ab0ea3316892969b19f3a75e64382c3e558f7")
	if got != want {
		t.Fatalf("PublicKeyHash = %x, want %x", got, want)
	}
}

func TestSessionIDGoldenVector(t *testing.T) {
	t.Parallel()

	got, err := SessionID(goldenSessionIDInput())
	if err != nil {
		t.Fatalf("SessionID: %v", err)
	}

	// Independent fixed vector for the boxed TL serialization and SHA-256 hash.
	want := mustDecodeHash(t, "2f94ebe36ffc5657189335e5e3476d1aedbd8ce3e02276042233441773f61f08")
	if got != want {
		t.Fatalf("SessionID = %x, want %x", got, want)
	}
}

func TestSessionIDUnsafeRotationUsesLittleEndianNamespace(t *testing.T) {
	t.Parallel()

	got, err := SessionID(SessionIDInput{
		UnsafeRotation: UnsafeRotation{ID: 0x11223344},
		Members:        []Member{},
	})
	if err != nil {
		t.Fatalf("SessionID: %v", err)
	}

	// This vector has 44 33 22 11 followed by 28 zero bytes in config_hash.
	// The vector pins the little-endian namespace without relying on host
	// endianness.
	want := mustDecodeHash(t, "03359f1ccc42713f2b11cf86f51ac1e19dfe537b35cc5138484fc6c582df640b")
	if got != want {
		t.Fatalf("unsafe rotation SessionID = %x, want %x", got, want)
	}
}

func TestSessionIDHashesEveryFieldAndMemberOrder(t *testing.T) {
	t.Parallel()

	mutations := []sessionIDMutationCase{
		{
			name: "workchain",
			mutate: func(input *SessionIDInput) {
				input.Workchain++
			},
		},
		{
			name: "shard",
			mutate: func(input *SessionIDInput) {
				input.Shard++
			},
		},
		{
			name: "maximal vertical seqno",
			mutate: func(input *SessionIDInput) {
				input.MaximalVerticalSeqno++
			},
		},
		{
			name: "last key block seqno",
			mutate: func(input *SessionIDInput) {
				input.LastKeyBlockSeqno++
			},
		},
		{
			name: "catchain seqno",
			mutate: func(input *SessionIDInput) {
				input.CatchainSeqno++
			},
		},
		{
			name: "unsafe rotation",
			mutate: func(input *SessionIDInput) {
				input.UnsafeRotation.ID = 1
			},
		},
		{
			name: "member public key hash",
			mutate: func(input *SessionIDInput) {
				input.Members[0].PublicKeyHash[0]++
			},
		},
		{
			name: "member ADNL",
			mutate: func(input *SessionIDInput) {
				input.Members[0].ADNL[0]++
			},
		},
		{
			name: "member weight",
			mutate: func(input *SessionIDInput) {
				input.Members[0].Weight++
			},
		},
		{
			name: "member order",
			mutate: func(input *SessionIDInput) {
				input.Members[0], input.Members[1] = input.Members[1], input.Members[0]
			},
		},
	}

	base := goldenSessionIDInput()
	want, err := SessionID(base)
	if err != nil {
		t.Fatalf("base SessionID: %v", err)
	}

	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			input := goldenSessionIDInput()
			mutation.mutate(&input)

			got, err := SessionID(input)
			if err != nil {
				t.Fatalf("SessionID: %v", err)
			}
			if got == want {
				t.Fatalf("SessionID did not change after mutating %s", mutation.name)
			}
		})
	}
}

func TestSessionIDRejectsWeightOutsideSignedTLLong(t *testing.T) {
	t.Parallel()

	input := goldenSessionIDInput()
	input.Members[1].Weight = math.MaxInt64 + 1

	_, err := SessionID(input)
	if err == nil {
		t.Fatal("SessionID accepted member weight above MaxInt64")
	}
	if !strings.Contains(err.Error(), "member 1 weight") {
		t.Fatalf("SessionID error = %q, want member index and weight", err)
	}
}

func goldenSessionIDInput() SessionIDInput {
	return SessionIDInput{
		Workchain:            -1,
		Shard:                math.MinInt64,
		MaximalVerticalSeqno: 3,
		LastKeyBlockSeqno:    123456,
		CatchainSeqno:        789,
		Members: []Member{
			{
				PublicKeyHash: sequentialBytes(0x00),
				ADNL:          sequentialBytes(0x20),
				Weight:        1,
			},
			{
				PublicKeyHash: sequentialBytes(0x40),
				ADNL:          sequentialBytes(0x60),
				Weight:        0x0102030405060708,
			},
		},
	}
}

func sequentialBytes(start byte) [32]byte {
	var value [32]byte
	for i := range value {
		value[i] = start + byte(i)
	}
	return value
}

func mustDecodeHash(t *testing.T, value string) [32]byte {
	t.Helper()

	decoded := mustDecodeHex(t, value)
	if len(decoded) != 32 {
		t.Fatalf("decoded hash length = %d, want 32", len(decoded))
	}

	var result [32]byte
	copy(result[:], decoded)
	return result
}

func mustDecodeHex(t *testing.T, value string) []byte {
	t.Helper()

	decoded, err := hex.DecodeString(value)
	if err != nil {
		t.Fatalf("decode hex: %v", err)
	}
	return decoded
}
