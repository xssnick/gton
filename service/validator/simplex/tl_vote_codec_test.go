package simplex

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"testing"

	"github.com/xssnick/tonutils-go/tl"
)

// The vote codec (SignedVote.Serialize / parseSignedVote) is hand-written for
// the hot path, so ConsensusSimplexVote and its registration stay as the
// reflection reference and these tests are the parity gate: the manual encoder
// must be byte-identical to tl.Serialize and the manual decoder must accept,
// reject and decode exactly what tl.Parse does. Being *stricter* than the
// reference is a liveness bug — it silently drops frames peers consider valid —
// so the decoder differential is run over fuzzed frames, not just golden ones.

func serializeSignedVoteReflect(sv *SignedVote) ([]byte, error) {
	return tl.Serialize(ConsensusSimplexVote{Vote: voteToTL(sv.Vote), Signature: sv.Signature}, true)
}

func parseSignedVoteReflect(data []byte) (Vote, []byte, error) {
	var x ConsensusSimplexVote
	rest, err := tl.Parse(&x, data, true)
	if err != nil {
		return Vote{}, nil, err
	}
	if len(rest) != 0 {
		return Vote{}, nil, fmt.Errorf("simplex/tl: %d trailing bytes in vote", len(rest))
	}
	v, err := voteFromTL(x.Vote)
	if err != nil {
		return Vote{}, nil, err
	}
	return v, x.Signature, nil
}

// codecCases enumerates every shape the vote codec has to get right: all three
// UnsignedVote constructors, the int32 slot boundaries, and the TL bytes length
// forms around the 0xFE short/long switch and the 4-byte padding steps.
func codecCases() []*SignedVote {
	var out []*SignedVote
	for _, slot := range []uint32{0, 1, 1 << 31, math.MaxUint32} {
		for _, v := range []Vote{
			NotarizeVote(candID(slot, 0xab)),
			FinalizeVote(candID(slot, 0x00)),
			SkipVote(slot),
		} {
			for _, sigLen := range []int{0, 1, 2, 3, 4, 63, 64, 65, 253, 254, 255, 256, 300} {
				out = append(out, &SignedVote{
					ValidatorIndex: 1,
					Vote:           v,
					Signature:      bytes.Repeat([]byte{byte(sigLen)}, sigLen),
				})
			}
		}
	}
	return out
}

func TestVoteCodecMatchesReflection(t *testing.T) {
	for _, sv := range codecCases() {
		want, err := serializeSignedVoteReflect(sv)
		if err != nil {
			t.Fatalf("%s sig=%d: reference serialize: %v", sv.Vote, len(sv.Signature), err)
		}
		got := sv.Serialize()
		if !bytes.Equal(got, want) {
			t.Fatalf("%s sig=%d: encode mismatch:\n got %x\nwant %x", sv.Vote, len(sv.Signature), got, want)
		}
		v, sig, err := parseSignedVote(got)
		if err != nil {
			t.Fatalf("%s sig=%d: decode: %v", sv.Vote, len(sv.Signature), err)
		}
		if v != sv.Vote || !bytes.Equal(sig, sv.Signature) {
			t.Fatalf("%s sig=%d: decode mismatch: %s / %x", sv.Vote, len(sv.Signature), v, sig)
		}
		// The signature outlives the transport frame (slot state, journal), so
		// it must be a copy, not a window into the input.
		if len(sig) > 0 && &sig[0] == &got[len(got)-len(sig)] {
			t.Fatalf("%s sig=%d: decoded signature aliases the frame", sv.Vote, len(sv.Signature))
		}
	}
}

// compareVoteDecoders is the differential assertion: manual and reflection
// decoders must agree on acceptance and on the decoded value.
func compareVoteDecoders(t *testing.T, data []byte) {
	t.Helper()

	gotV, gotSig, gotErr := parseSignedVote(data)
	wantV, wantSig, wantErr := parseSignedVoteReflect(data)
	if (gotErr == nil) != (wantErr == nil) {
		t.Fatalf("acceptance mismatch for %x:\n manual: %v\n reflect: %v", data, gotErr, wantErr)
	}
	if gotErr != nil {
		return
	}
	if gotV != wantV {
		t.Fatalf("vote mismatch for %x: manual %s, reflect %s", data, gotV, wantV)
	}
	if !bytes.Equal(gotSig, wantSig) {
		t.Fatalf("signature mismatch for %x: manual %x, reflect %x", data, gotSig, wantSig)
	}
}

// voteCodecSeeds are the fuzz seeds: valid frames of every shape plus the
// malformed neighbourhoods a mutator has trouble reaching on its own —
// truncations, wrong constructor ids, both long TL bytes forms and nonzero
// alignment padding (which the reference deliberately tolerates).
func voteCodecSeeds() [][]byte {
	var seeds [][]byte
	for _, sv := range codecCases() {
		wire := sv.Serialize()
		seeds = append(seeds, wire)
		for cut := 1; cut <= len(wire) && cut <= 8; cut++ {
			seeds = append(seeds, wire[:len(wire)-cut])
		}
		seeds = append(seeds, append(append([]byte{}, wire...), 0))
	}

	id := candID(7, 0x5c)
	base := (&SignedVote{Vote: NotarizeVote(id), Signature: bytes.Repeat([]byte{9}, 64)}).Serialize()
	for _, off := range []int{0, 4, 8} {
		for _, bad := range []uint32{0, 0xffffffff, idCertificate, idSkipVote, idNotarizeVote} {
			m := append([]byte{}, base...)
			binary.LittleEndian.PutUint32(m[off:], bad)
			seeds = append(seeds, m)
		}
	}
	// Nonzero padding in the trailing TL bytes field, and both long length
	// forms for a payload that fits the short one.
	pad := append([]byte{}, (&SignedVote{Vote: SkipVote(3), Signature: []byte{1, 2}}).Serialize()...)
	pad[len(pad)-1] = 0xee
	seeds = append(seeds, pad)

	head := append([]byte{}, base[:len(base)-68]...)
	long := append(append([]byte{}, head...), 0xfe, 64, 0, 0)
	long = append(long, bytes.Repeat([]byte{9}, 64)...)
	seeds = append(seeds, long)
	xlong := append(append([]byte{}, head...), 0xff, 64, 0, 0, 0, 0, 0, 0)
	xlong = append(xlong, bytes.Repeat([]byte{9}, 64)...)
	seeds = append(seeds, xlong)

	return seeds
}

func TestVoteDecoderDifferentialSeeds(t *testing.T) {
	seeds := voteCodecSeeds()
	for _, s := range seeds {
		compareVoteDecoders(t, s)
	}
	t.Logf("compared %d seed frames", len(seeds))
}

func FuzzVoteDecoder(f *testing.F) {
	for _, s := range voteCodecSeeds() {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		compareVoteDecoders(t, data)
	})
}
