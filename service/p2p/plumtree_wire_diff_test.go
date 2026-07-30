package p2p

import (
	"bytes"
	"crypto/ed25519"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"testing"

	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
)

// The production Plumtree types now decode through the hand-written
// ParseableTL methods in plumtree_wire.go, which means tl's reflection decoder
// no longer runs for them and cannot serve as a comparison baseline. These twin
// types carry byte-identical field layouts and tl tags but no manual methods,
// so they keep the reflection path alive purely for differential testing. Their
// TL names differ only so the registry keeps distinct constructor ids; the
// bodies after the type id are identical, and plumtreeWireSwapConstructor
// rewrites those four bytes when feeding a twin.

type twinBroadcastPlumtreeIHave struct {
	BroadcastID      []byte  `tl:"int256"`
	Timestamp        float64 `tl:"double"`
	PartIndex        int32   `tl:"int"`
	TreeIndex        int32   `tl:"int"`
	Source           any     `tl:"struct boxed [pub.ed25519]"`
	Certificate      any     `tl:"struct boxed [overlay.emptyCertificate,overlay.certificate,overlay.certificateV2]"`
	PayloadTimestamp float64 `tl:"double"`
	DataSize         int32   `tl:"int"`
	DataHash         []byte  `tl:"int256"`
	Signature        []byte  `tl:"bytes"`
}

type twinBroadcastPlumtreeFEC struct {
	Flags        int32   `tl:"int"`
	Timestamp    float64 `tl:"double"`
	Source       any     `tl:"struct boxed [pub.ed25519]"`
	Certificate  any     `tl:"struct boxed [overlay.emptyCertificate,overlay.certificate,overlay.certificateV2]"`
	FullDataHash []byte  `tl:"int256"`
	FullDataSize int32   `tl:"int"`
	PartIndex    int32   `tl:"int"`
	TreeIndex    int32   `tl:"int"`
	Data         []byte  `tl:"bytes"`
	Signature    []byte  `tl:"bytes"`
}

type twinBroadcastPlumtreeSimple struct {
	Flags       int32   `tl:"int"`
	Timestamp   float64 `tl:"double"`
	Source      any     `tl:"struct boxed [pub.ed25519]"`
	Certificate any     `tl:"struct boxed [overlay.emptyCertificate,overlay.certificate,overlay.certificateV2]"`
	BroadcastID []byte  `tl:"int256"`
	TreeIndex   int32   `tl:"int"`
	Data        []byte  `tl:"bytes"`
	Signature   []byte  `tl:"bytes"`
}

type twinPlumtreeControl struct {
	BroadcastID []byte  `tl:"int256"`
	Timestamp   float64 `tl:"double"`
	PartIndex   int32   `tl:"int"`
	TreeIndex   int32   `tl:"int"`
}

var (
	twinIHaveConstructorID   uint32
	twinFECConstructorID     uint32
	twinSimpleConstructorID  uint32
	twinControlConstructorID uint32
)

func init() {
	twinIHaveConstructorID = tl.Register(
		twinBroadcastPlumtreeIHave{},
		"twin.broadcastPlumtreeIHave broadcast_id:int256 timestamp:double "+
			"part_index:int tree_index:int src:PublicKey certificate:overlay.Certificate "+
			"payload_timestamp:double data_size:int data_hash:int256 signature:bytes = twin.Broadcast",
	)
	twinFECConstructorID = tl.Register(
		twinBroadcastPlumtreeFEC{},
		"twin.broadcastPlumtreeFec flags:int timestamp:double src:PublicKey "+
			"certificate:overlay.Certificate full_data_hash:int256 full_data_size:int "+
			"part_index:int tree_index:int data:bytes signature:bytes = twin.Broadcast",
	)
	twinSimpleConstructorID = tl.Register(
		twinBroadcastPlumtreeSimple{},
		"twin.broadcastPlumtreeSimple flags:int timestamp:double src:PublicKey "+
			"certificate:overlay.Certificate broadcast_id:int256 tree_index:int data:bytes "+
			"signature:bytes = twin.Broadcast",
	)
	twinControlConstructorID = tl.Register(
		twinPlumtreeControl{},
		"twin.plumtreeControl broadcast_id:int256 timestamp:double part_index:int "+
			"tree_index:int = twin.Broadcast",
	)
}

// TestPlumtreeWireTwinsTrackProduction fails if a production message gains or
// loses a field without the twin following, which would silently hollow out
// every differential test below.
func TestPlumtreeWireTwinsTrackProduction(t *testing.T) {
	pairs := []struct {
		name       string
		production any
		twin       any
	}{
		{"ihave", BroadcastPlumtreeIHave{}, twinBroadcastPlumtreeIHave{}},
		{"fec", BroadcastPlumtreeFEC{}, twinBroadcastPlumtreeFEC{}},
		{"simple", BroadcastPlumtreeSimple{}, twinBroadcastPlumtreeSimple{}},
		{"prune", BroadcastPlumtreePrune{}, twinPlumtreeControl{}},
		{"useful", BroadcastPlumtreeUseful{}, twinPlumtreeControl{}},
		{"repair", RepairPlumtreePart{}, twinPlumtreeControl{}},
	}

	for _, pair := range pairs {
		production := reflect.TypeOf(pair.production)
		twin := reflect.TypeOf(pair.twin)
		if production.NumField() != twin.NumField() {
			t.Fatalf(
				"%s: production has %d fields, twin has %d",
				pair.name,
				production.NumField(),
				twin.NumField(),
			)
		}
		for i := range production.NumField() {
			left, right := production.Field(i), twin.Field(i)
			if left.Name != right.Name ||
				left.Type != right.Type ||
				left.Tag.Get("tl") != right.Tag.Get("tl") {
				t.Fatalf(
					"%s field %d: production %s %s `%s`, twin %s %s `%s`",
					pair.name,
					i,
					left.Name,
					left.Type,
					left.Tag.Get("tl"),
					right.Name,
					right.Type,
					right.Tag.Get("tl"),
				)
			}
		}
	}
}

// TestPlumtreeWireCertificateLayoutIsPinned guards the one place where
// plumtree_wire.go writes out a layout it does not own. readCertificate decodes
// overlay.Certificate and overlay.CertificateV2 field by field for speed, so a
// field added, removed or reordered in tonutils-go would otherwise be decoded
// silently wrong. This fails instead, and the fix is to mirror the change in
// readCertificate.
func TestPlumtreeWireCertificateLayoutIsPinned(t *testing.T) {
	type field struct {
		name string
		typ  string
		tag  string
	}
	expected := map[string][]field{
		"Certificate": {
			{"IssuedBy", "interface {}", "struct boxed [pub.ed25519]"},
			{"ExpireAt", "uint32", "int"},
			{"MaxSize", "uint32", "int"},
			{"Signature", "[]uint8", "bytes"},
		},
		"CertificateV2": {
			{"IssuedBy", "interface {}", "struct boxed [pub.ed25519]"},
			{"ExpireAt", "uint32", "int"},
			{"MaxSize", "uint32", "int"},
			{"Flags", "int32", "int"},
			{"Signature", "[]uint8", "bytes"},
		},
	}

	for _, value := range []any{overlay.Certificate{}, overlay.CertificateV2{}} {
		typ := reflect.TypeOf(value)
		want := expected[typ.Name()]
		if typ.NumField() != len(want) {
			t.Fatalf(
				"%s has %d fields, readCertificate decodes %d",
				typ.Name(),
				typ.NumField(),
				len(want),
			)
		}
		for i, expect := range want {
			got := typ.Field(i)
			if got.Name != expect.name ||
				got.Type.String() != expect.typ ||
				got.Tag.Get("tl") != expect.tag {
				t.Fatalf(
					"%s field %d is %s %s `%s`, readCertificate decodes %s %s `%s`",
					typ.Name(),
					i,
					got.Name,
					got.Type,
					got.Tag.Get("tl"),
					expect.name,
					expect.typ,
					expect.tag,
				)
			}
		}
	}

	// The empty certificate must stay field-free: readCertificate consumes its
	// type id and nothing else.
	if n := reflect.TypeOf(overlay.CertificateEmpty{}).NumField(); n != 0 {
		t.Fatalf("overlay.CertificateEmpty gained %d fields", n)
	}
}

type plumtreeWireCase struct {
	name string
	wire []byte
	// parse decodes with both paths and reports whether each accepted the
	// input and whether the accepted values agree.
	parse func(data []byte) (fastOK, slowOK, equal bool, detail string)
}

// plumtreeWireComparator decodes data with the production type and, after
// swapping in the twin's constructor id, with the reflection twin.
func plumtreeWireComparator[P any, T any](twinID uint32) func([]byte) (bool, bool, bool, string) {
	return func(data []byte) (bool, bool, bool, string) {
		var production P
		fastRest, fastErr := tl.ParseNoCopy(&production, data, true)
		if fastErr == nil && len(fastRest) != 0 {
			fastErr = errors.New("unexpected trailing Plumtree object data")
		}

		var twin T
		slowErr := errors.New("truncated Plumtree object")
		var slowRest []byte
		if swapped, ok := plumtreeWireSwapConstructor(data, twinID); ok {
			slowRest, slowErr = tl.ParseNoCopy(&twin, swapped, true)
			if slowErr == nil && len(slowRest) != 0 {
				slowErr = errors.New("unexpected trailing Plumtree object data")
			}
		}

		if fastErr != nil || slowErr != nil {
			return fastErr == nil, slowErr == nil, true, ""
		}
		if !plumtreeWireEqual(reflect.ValueOf(production), reflect.ValueOf(twin)) {
			return true, true, false, fmt.Sprintf(
				"production=%+v twin=%+v",
				production,
				twin,
			)
		}
		return true, true, true, ""
	}
}

func plumtreeWireSwapConstructor(data []byte, id uint32) ([]byte, bool) {
	if len(data) < 4 {
		return nil, false
	}
	swapped := bytes.Clone(data)
	binary.LittleEndian.PutUint32(swapped[:4], id)
	return swapped, true
}

func plumtreeWireCases(t *testing.T) []plumtreeWireCase {
	t.Helper()

	random := rand.New(rand.NewSource(20260729))
	certificates := []struct {
		name  string
		value any
	}{
		{name: "empty", value: overlay.CertificateEmpty{}},
		{name: "legacy", value: overlay.Certificate{
			IssuedBy:  keys.PublicKeyED25519{Key: plumtreeWireRandomBytes(random, ed25519.PublicKeySize)},
			ExpireAt:  4294967295,
			MaxSize:   1 << 20,
			Signature: plumtreeWireRandomBytes(random, ed25519.SignatureSize),
		}},
		{name: "v2", value: overlay.CertificateV2{
			IssuedBy:  keys.PublicKeyED25519{Key: plumtreeWireRandomBytes(random, ed25519.PublicKeySize)},
			ExpireAt:  0,
			MaxSize:   0,
			Flags:     -1,
			Signature: plumtreeWireRandomBytes(random, 0),
		}},
	}
	// Sizes straddle the one-byte/0xFE length forms and every padding residue.
	dataSizes := []int{0, 1, 2, 3, 4, 5, 252, 253, 254, 255, 256, 257, 1024}
	timestamps := []float64{0, -1, 1785310926.5, math.MaxFloat64, math.SmallestNonzeroFloat64}

	var cases []plumtreeWireCase
	for _, certificate := range certificates {
		for i, size := range dataSizes {
			timestamp := timestamps[i%len(timestamps)]
			partIndex := int32(i)
			treeIndex := partIndex + plumtreeFECTreeOffset
			source := keys.PublicKeyED25519{
				Key: plumtreeWireRandomBytes(random, ed25519.PublicKeySize),
			}

			cases = append(cases, plumtreeWireCase{
				name: fmt.Sprintf("ihave/%s/%d", certificate.name, size),
				wire: plumtreeWireSerialize(t, BroadcastPlumtreeIHave{
					BroadcastID:      plumtreeWireRandomBytes(random, 32),
					Timestamp:        timestamp,
					PartIndex:        partIndex,
					TreeIndex:        treeIndex,
					Source:           source,
					Certificate:      certificate.value,
					PayloadTimestamp: -timestamp,
					DataSize:         int32(size + 1),
					DataHash:         plumtreeWireRandomBytes(random, 32),
					Signature:        plumtreeWireRandomBytes(random, size),
				}),
				parse: plumtreeWireComparator[
					BroadcastPlumtreeIHave,
					twinBroadcastPlumtreeIHave,
				](twinIHaveConstructorID),
			})

			cases = append(cases, plumtreeWireCase{
				name: fmt.Sprintf("fec/%s/%d", certificate.name, size),
				wire: plumtreeWireSerialize(t, BroadcastPlumtreeFEC{
					Flags:        int32(size),
					Timestamp:    timestamp,
					Source:       source,
					Certificate:  certificate.value,
					FullDataHash: plumtreeWireRandomBytes(random, 32),
					FullDataSize: int32(size + 7),
					PartIndex:    partIndex,
					TreeIndex:    treeIndex,
					Data:         plumtreeWireRandomBytes(random, size),
					Signature:    plumtreeWireRandomBytes(random, ed25519.SignatureSize),
				}),
				parse: plumtreeWireComparator[
					BroadcastPlumtreeFEC,
					twinBroadcastPlumtreeFEC,
				](twinFECConstructorID),
			})

			cases = append(cases, plumtreeWireCase{
				name: fmt.Sprintf("simple/%s/%d", certificate.name, size),
				wire: plumtreeWireSerialize(t, BroadcastPlumtreeSimple{
					Flags:       int32(-size),
					Timestamp:   timestamp,
					Source:      source,
					Certificate: certificate.value,
					BroadcastID: plumtreeWireRandomBytes(random, 32),
					TreeIndex:   plumtreeSimpleTree,
					Data:        plumtreeWireRandomBytes(random, size),
					Signature:   plumtreeWireRandomBytes(random, ed25519.SignatureSize),
				}),
				parse: plumtreeWireComparator[
					BroadcastPlumtreeSimple,
					twinBroadcastPlumtreeSimple,
				](twinSimpleConstructorID),
			})
		}
	}

	cases = append(cases,
		plumtreeWireCase{
			name: "prune",
			wire: plumtreeWireSerialize(t, BroadcastPlumtreePrune{
				BroadcastID: plumtreeWireRandomBytes(random, 32),
				Timestamp:   1785310926.5,
				PartIndex:   3,
				TreeIndex:   4,
			}),
			parse: plumtreeWireComparator[
				BroadcastPlumtreePrune,
				twinPlumtreeControl,
			](twinControlConstructorID),
		},
		plumtreeWireCase{
			name: "useful",
			wire: plumtreeWireSerialize(t, BroadcastPlumtreeUseful{
				BroadcastID: plumtreeWireRandomBytes(random, 32),
				Timestamp:   0,
				PartIndex:   0,
				TreeIndex:   0,
			}),
			parse: plumtreeWireComparator[
				BroadcastPlumtreeUseful,
				twinPlumtreeControl,
			](twinControlConstructorID),
		},
		plumtreeWireCase{
			name: "repair",
			wire: plumtreeWireSerialize(t, RepairPlumtreePart{
				BroadcastID: plumtreeWireRandomBytes(random, 32),
				Timestamp:   -12.25,
				PartIndex:   44,
				TreeIndex:   45,
			}),
			parse: plumtreeWireComparator[
				RepairPlumtreePart,
				twinPlumtreeControl,
			](twinControlConstructorID),
		},
	)
	return cases
}

func TestPlumtreeWireDecodersMatchReflectionOnValidInput(t *testing.T) {
	for _, testCase := range plumtreeWireCases(t) {
		fastOK, slowOK, equal, detail := testCase.parse(testCase.wire)
		if !slowOK {
			t.Fatalf("%s: reflection twin rejected its own serialization", testCase.name)
		}
		if !fastOK {
			t.Fatalf("%s: hand-written decoder rejected valid input", testCase.name)
		}
		if !equal {
			t.Fatalf("%s: decoders disagree: %s", testCase.name, detail)
		}
	}
}

func TestPlumtreeWireDecodersMatchReflectionOnTruncation(t *testing.T) {
	for _, testCase := range plumtreeWireCases(t) {
		for cut := range len(testCase.wire) {
			fastOK, slowOK, equal, detail := testCase.parse(testCase.wire[:cut])
			if fastOK != slowOK {
				t.Fatalf(
					"%s truncated to %d: hand-written accepted=%v, reflection accepted=%v",
					testCase.name,
					cut,
					fastOK,
					slowOK,
				)
			}
			if fastOK && !equal {
				t.Fatalf(
					"%s truncated to %d: decoders disagree: %s",
					testCase.name,
					cut,
					detail,
				)
			}
		}
	}
}

func TestPlumtreeWireDecodersMatchReflectionOnMutation(t *testing.T) {
	random := rand.New(rand.NewSource(981))
	for _, testCase := range plumtreeWireCases(t) {
		for range 400 {
			mutated := bytes.Clone(testCase.wire)
			// Never touch the leading type id: tl rejects on it before any
			// decoder runs, which would test nothing.
			switch random.Intn(3) {
			case 0:
				at := 4 + random.Intn(len(mutated)-4)
				mutated[at] ^= byte(1 << random.Intn(8))
			case 1:
				at := 4 + random.Intn(len(mutated)-4)
				mutated[at] = byte(random.Intn(256))
			default:
				mutated = append(mutated, byte(random.Intn(256)))
			}

			fastOK, slowOK, equal, detail := testCase.parse(mutated)
			if fastOK != slowOK {
				t.Fatalf(
					"%s mutated: hand-written accepted=%v, reflection accepted=%v, wire=%x",
					testCase.name,
					fastOK,
					slowOK,
					mutated,
				)
			}
			if fastOK && !equal {
				t.Fatalf("%s mutated: decoders disagree: %s", testCase.name, detail)
			}
		}
	}
}

// TestPlumtreeWireDecoderMatchesLongBytesForm covers the 0xFF length prefix,
// which the serializer only emits past 16MB but which a peer may still send.
func TestPlumtreeWireDecoderMatchesLongBytesForm(t *testing.T) {
	long := []byte{0xFF, 4, 0, 0, 0, 0, 0, 0, 'a', 'b', 'c', 'd'}

	cursor := plumtreeWireCursor{buf: long}
	got, err := cursor.readBytes()
	if err != nil {
		t.Fatalf("read 0xFF bytes form: %v", err)
	}
	want, rest, err := tl.FromBytesNoCopy(long)
	if err != nil {
		t.Fatalf("reflection read 0xFF bytes form: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("0xFF payload = %q, want %q", got, want)
	}
	if len(cursor.buf) != len(rest) {
		t.Fatalf("0xFF remainder = %d bytes, want %d", len(cursor.buf), len(rest))
	}
}

// TestPlumtreeWireDecoderAliasingMatchesReflection pins the memory semantics of
// both entry points: ParseNoCopy aliases bulk fields exactly as tl does, Parse
// copies them, and the source key is copied either way because
// keys.PublicKeyED25519 parses manually.
func TestPlumtreeWireDecoderAliasingMatchesReflection(t *testing.T) {
	random := rand.New(rand.NewSource(5))
	wire := plumtreeWireSerialize(t, BroadcastPlumtreeFEC{
		Flags:        1,
		Timestamp:    100,
		Source:       keys.PublicKeyED25519{Key: plumtreeWireRandomBytes(random, ed25519.PublicKeySize)},
		Certificate:  overlay.CertificateEmpty{},
		FullDataHash: plumtreeWireRandomBytes(random, 32),
		FullDataSize: 512,
		PartIndex:    1,
		TreeIndex:    2,
		Data:         plumtreeWireRandomBytes(random, 300),
		Signature:    plumtreeWireRandomBytes(random, ed25519.SignatureSize),
	})

	var noCopy BroadcastPlumtreeFEC
	if _, err := tl.ParseNoCopy(&noCopy, wire, true); err != nil {
		t.Fatalf("ParseNoCopy: %v", err)
	}
	twinWire, _ := plumtreeWireSwapConstructor(wire, twinFECConstructorID)
	var twin twinBroadcastPlumtreeFEC
	if _, err := tl.ParseNoCopy(&twin, twinWire, true); err != nil {
		t.Fatalf("reflection ParseNoCopy: %v", err)
	}

	if !plumtreeWireAliases(wire, noCopy.Data) ||
		!plumtreeWireAliases(twinWire, twin.Data) {
		t.Fatal("FEC data must alias the wire buffer in both decoders")
	}
	if !plumtreeWireAliases(wire, noCopy.FullDataHash) ||
		!plumtreeWireAliases(twinWire, twin.FullDataHash) {
		t.Fatal("int256 fields must alias the wire buffer in both decoders")
	}
	if plumtreeWireAliases(wire, noCopy.Source.(keys.PublicKeyED25519).Key) ||
		plumtreeWireAliases(twinWire, twin.Source.(keys.PublicKeyED25519).Key) {
		t.Fatal("source key must be copied out of the wire buffer in both decoders")
	}

	// Appending to an aliased field must not reach the following wire bytes;
	// the decoders cap capacity for exactly that reason.
	before := bytes.Clone(wire)
	_ = append(noCopy.FullDataHash, 0xFF)
	if !bytes.Equal(before, wire) {
		t.Fatal("appending to an aliased int256 field corrupted the wire buffer")
	}

	// The copying entry point must detach every field from the wire buffer.
	var copied BroadcastPlumtreeFEC
	if _, err := tl.Parse(&copied, wire, true); err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if plumtreeWireAliases(wire, copied.Data) ||
		plumtreeWireAliases(wire, copied.FullDataHash) {
		t.Fatal("Parse must copy fields out of the wire buffer")
	}
	if !bytes.Equal(copied.Data, noCopy.Data) ||
		!bytes.Equal(copied.FullDataHash, noCopy.FullDataHash) {
		t.Fatal("Parse and ParseNoCopy produced different field values")
	}
}

// BenchmarkPlumtreeIHaveDecodeHotPath measures what processMessage runs: the
// decoder called directly, with the constructor id already matched.
func BenchmarkPlumtreeIHaveDecodeHotPath(b *testing.B) {
	wire := plumtreeWireBenchmarkIHave(b)
	b.ReportAllocs()
	for b.Loop() {
		var message BroadcastPlumtreeIHave
		if err := plumtreeParsed(message.ParseNoCopy(wire[4:])); err != nil {
			b.Fatalf("decode: %v", err)
		}
	}
}

// BenchmarkPlumtreeIHaveDecodeViaTL measures the same decoder reached through
// tl's dispatch, which is what every other caller of these types gets.
func BenchmarkPlumtreeIHaveDecodeViaTL(b *testing.B) {
	wire := plumtreeWireBenchmarkIHave(b)
	b.ReportAllocs()
	for b.Loop() {
		var message BroadcastPlumtreeIHave
		if _, err := tl.ParseNoCopy(&message, wire, true); err != nil {
			b.Fatalf("decode: %v", err)
		}
	}
}

func BenchmarkPlumtreeIHaveDecodeReflection(b *testing.B) {
	wire := plumtreeWireBenchmarkIHave(b)
	twinWire, _ := plumtreeWireSwapConstructor(wire, twinIHaveConstructorID)
	b.ReportAllocs()
	for b.Loop() {
		var message twinBroadcastPlumtreeIHave
		if _, err := tl.ParseNoCopy(&message, twinWire, true); err != nil {
			b.Fatalf("decode: %v", err)
		}
	}
}

// BenchmarkPlumtreeIHaveDecodeCertifiedHotPath covers the certificate-bearing
// message, which is the common shape on this network and the one that made
// delegating overlay.Certificate to tl cost 2.2% of node CPU.
func BenchmarkPlumtreeIHaveDecodeCertifiedHotPath(b *testing.B) {
	wire := plumtreeWireBenchmarkCertifiedIHave(b)
	b.ReportAllocs()
	for b.Loop() {
		var message BroadcastPlumtreeIHave
		if err := plumtreeParsed(message.ParseNoCopy(wire[4:])); err != nil {
			b.Fatalf("decode: %v", err)
		}
	}
}

func BenchmarkPlumtreeIHaveDecodeCertifiedReflection(b *testing.B) {
	wire := plumtreeWireBenchmarkCertifiedIHave(b)
	twinWire, _ := plumtreeWireSwapConstructor(wire, twinIHaveConstructorID)
	b.ReportAllocs()
	for b.Loop() {
		var message twinBroadcastPlumtreeIHave
		if _, err := tl.ParseNoCopy(&message, twinWire, true); err != nil {
			b.Fatalf("decode: %v", err)
		}
	}
}

func plumtreeWireBenchmarkCertifiedIHave(b *testing.B) []byte {
	b.Helper()

	random := rand.New(rand.NewSource(91))
	wire, err := tl.Serialize(BroadcastPlumtreeIHave{
		BroadcastID: plumtreeWireRandomBytes(random, 32),
		Timestamp:   1785310926.5,
		PartIndex:   3,
		TreeIndex:   4,
		Source: keys.PublicKeyED25519{
			Key: plumtreeWireRandomBytes(random, ed25519.PublicKeySize),
		},
		Certificate: overlay.Certificate{
			IssuedBy: keys.PublicKeyED25519{
				Key: plumtreeWireRandomBytes(random, ed25519.PublicKeySize),
			},
			ExpireAt:  4294967295,
			MaxSize:   1 << 20,
			Signature: plumtreeWireRandomBytes(random, ed25519.SignatureSize),
		},
		PayloadTimestamp: 1785310926.25,
		DataSize:         1024,
		DataHash:         plumtreeWireRandomBytes(random, 32),
		Signature:        plumtreeWireRandomBytes(random, ed25519.SignatureSize),
	}, true)
	if err != nil {
		b.Fatalf("serialize certified benchmark message: %v", err)
	}
	return wire
}

func plumtreeWireBenchmarkIHave(b *testing.B) []byte {
	b.Helper()

	random := rand.New(rand.NewSource(77))
	wire, err := tl.Serialize(BroadcastPlumtreeIHave{
		BroadcastID:      plumtreeWireRandomBytes(random, 32),
		Timestamp:        1785310926.5,
		PartIndex:        3,
		TreeIndex:        4,
		Source:           keys.PublicKeyED25519{Key: plumtreeWireRandomBytes(random, ed25519.PublicKeySize)},
		Certificate:      overlay.CertificateEmpty{},
		PayloadTimestamp: 1785310926.25,
		DataSize:         1024,
		DataHash:         plumtreeWireRandomBytes(random, 32),
		Signature:        plumtreeWireRandomBytes(random, ed25519.SignatureSize),
	}, true)
	if err != nil {
		b.Fatalf("serialize benchmark message: %v", err)
	}
	return wire
}

func plumtreeWireSerialize(t *testing.T, message any) []byte {
	t.Helper()

	wire, err := tl.Serialize(message, true)
	if err != nil {
		t.Fatalf("serialize %T: %v", message, err)
	}
	return wire
}

func plumtreeWireRandomBytes(random *rand.Rand, size int) []byte {
	value := make([]byte, size)
	random.Read(value)
	return value
}

// plumtreeWireEqual is reflect.DeepEqual with one change: float64 fields are
// compared by bit pattern, so a mutated timestamp that decodes to NaN in both
// decoders counts as agreement rather than as a difference.
func plumtreeWireEqual(a, b reflect.Value) bool {
	if a.Kind() != b.Kind() {
		return false
	}

	switch a.Kind() {
	case reflect.Float64, reflect.Float32:
		return math.Float64bits(a.Float()) == math.Float64bits(b.Float())
	case reflect.Interface:
		if a.IsNil() || b.IsNil() {
			return a.IsNil() == b.IsNil()
		}
		return plumtreeWireEqual(a.Elem(), b.Elem())
	case reflect.Struct:
		if a.NumField() != b.NumField() {
			return false
		}
		for i := range a.NumField() {
			if !plumtreeWireEqual(a.Field(i), b.Field(i)) {
				return false
			}
		}
		return true
	case reflect.Slice:
		if a.IsNil() != b.IsNil() || a.Len() != b.Len() {
			return false
		}
		for i := range a.Len() {
			if !plumtreeWireEqual(a.Index(i), b.Index(i)) {
				return false
			}
		}
		return true
	default:
		return reflect.DeepEqual(a.Interface(), b.Interface())
	}
}

// plumtreeWireAliases reports whether field points into buffer's backing array,
// identified by address rather than by content so an equal-but-copied slice
// does not pass.
func plumtreeWireAliases(buffer, field []byte) bool {
	if len(field) == 0 || len(buffer) == 0 {
		return false
	}
	for i := range buffer {
		if &buffer[i] == &field[0] {
			return true
		}
	}
	return false
}
