package p2p

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
)

func TestPlumtreeConstructorIDs(t *testing.T) {
	t.Parallel()

	id := bytes.Repeat([]byte{0x11}, sha256.Size)
	source := keys.PublicKeyED25519{Key: ed25519.PublicKey(bytes.Repeat([]byte{0x22}, ed25519.PublicKeySize))}
	certificate := overlay.CertificateEmpty{}

	tests := []struct {
		name       string
		value      any
		schema     string
		expectedID uint32
	}{
		{
			name:       "not found",
			value:      BroadcastNotFound{},
			schema:     broadcastNotFoundSchema,
			expectedID: 0x95863624,
		},
		{
			name: "repair",
			value: RepairPlumtreePart{
				BroadcastID: id,
				Timestamp:   1.25,
				PartIndex:   0,
				TreeIndex:   0,
			},
			schema:     repairPlumtreePartSchema,
			expectedID: 0x7bd0590d,
		},
		{
			name: "simple",
			value: BroadcastPlumtreeSimple{
				Flags:       overlay.BroadcastFlagAnySender,
				Timestamp:   1.25,
				Source:      source,
				Certificate: certificate,
				BroadcastID: id,
				TreeIndex:   0,
				Data:        []byte{1},
				Signature:   []byte{2},
			},
			schema:     broadcastPlumtreeSimpleSchema,
			expectedID: 0x3eccd26d,
		},
		{
			name: "fec",
			value: BroadcastPlumtreeFEC{
				Flags:        overlay.BroadcastFlagAnySender,
				Timestamp:    1.25,
				Source:       source,
				Certificate:  certificate,
				FullDataHash: id,
				FullDataSize: 30,
				PartIndex:    0,
				TreeIndex:    1,
				Data:         []byte{1},
				Signature:    []byte{2},
			},
			schema:     broadcastPlumtreeFECSchema,
			expectedID: 0x8cba5fcf,
		},
		{
			name: "ihave",
			value: BroadcastPlumtreeIHave{
				BroadcastID:      id,
				Timestamp:        1.25,
				PartIndex:        0,
				TreeIndex:        1,
				Source:           source,
				Certificate:      certificate,
				PayloadTimestamp: 1.5,
				DataSize:         1,
				DataHash:         id,
				Signature:        []byte{2},
			},
			schema:     broadcastPlumtreeIHaveSchema,
			expectedID: 0x6150c07b,
		},
		{
			name: "prune",
			value: BroadcastPlumtreePrune{
				BroadcastID: id,
				Timestamp:   1.25,
				PartIndex:   0,
				TreeIndex:   1,
			},
			schema:     broadcastPlumtreePruneSchema,
			expectedID: 0xf0f73196,
		},
		{
			name: "useful",
			value: BroadcastPlumtreeUseful{
				BroadcastID: id,
				Timestamp:   1.25,
				PartIndex:   0,
				TreeIndex:   1,
			},
			schema:     broadcastPlumtreeUsefulSchema,
			expectedID: 0xaf825e83,
		},
		{
			name: "stats record",
			value: PlumtreeStatsRecord{
				Source:        id,
				Epoch:         7,
				TreeIndex:     3,
				EagerSimple:   []int64{1},
				EagerRotating: []int64{2},
			},
			schema:     plumtreeStatsRecordSchema,
			expectedID: 0xe5056a2e,
		},
		{
			name: "stats push",
			value: PlumtreeStatsPush{
				Record: PlumtreeStatsRecord{
					Source:        id,
					Epoch:         7,
					TreeIndex:     3,
					EagerSimple:   []int64{1},
					EagerRotating: []int64{2},
				},
			},
			schema:     plumtreeStatsPushSchema,
			expectedID: 0x22dfaa6e,
		},
		{
			name: "fec id",
			value: PlumtreeFECID{
				Flags:        overlay.BroadcastFlagAnySender,
				FullDataHash: id,
				FullDataSize: 30,
				PartSize:     1,
			},
			schema:     plumtreeFECIDSchema,
			expectedID: 0x06a25034,
		},
		{
			name: "simple to sign",
			value: BroadcastPlumtreeSimpleToSign{
				ID:        id,
				Timestamp: 1.25,
				TreeIndex: 0,
				DataSize:  1,
				DataHash:  id,
			},
			schema:     broadcastPlumtreeSimpleToSignSchema,
			expectedID: 0x38f3338e,
		},
		{
			name: "fec to sign",
			value: BroadcastPlumtreeFECToSign{
				ID:        id,
				Timestamp: 1.25,
				PartIndex: 0,
				TreeIndex: 1,
				DataSize:  1,
				DataHash:  id,
			},
			schema:     broadcastPlumtreeFECToSignSchema,
			expectedID: 0xc3e1c094,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if actual := tl.CRC(test.schema); actual != test.expectedID {
				t.Fatalf("schema CRC = %#08x, want %#08x", actual, test.expectedID)
			}

			boxed, err := tl.Serialize(test.value, true)
			if err != nil {
				t.Fatalf("serialize: %v", err)
			}
			if len(boxed) < 4 {
				t.Fatalf("serialized value is only %d bytes", len(boxed))
			}
			if actual := binary.LittleEndian.Uint32(boxed[:4]); actual != test.expectedID {
				t.Fatalf("wire constructor = %#08x, want %#08x", actual, test.expectedID)
			}
		})
	}
}

func TestPlumtreeSimpleWireGolden(t *testing.T) {
	t.Parallel()

	timestamp := math.Float64frombits(0x41d954fc40012345)
	publicKey := make(ed25519.PublicKey, ed25519.PublicKeySize)
	for i := range publicKey {
		publicKey[i] = byte(i)
	}
	broadcastID := make([]byte, sha256.Size)
	for i := range broadcastID {
		broadcastID[i] = byte(0xa0 + i)
	}

	boxed, err := tl.Serialize(BroadcastPlumtreeSimple{
		Flags:       overlay.BroadcastFlagAnySender,
		Timestamp:   timestamp,
		Source:      keys.PublicKeyED25519{Key: publicKey},
		Certificate: overlay.CertificateEmpty{},
		BroadcastID: broadcastID,
		TreeIndex:   plumtreeSimpleTree,
		Data:        []byte{0xde, 0xad, 0xbe},
		Signature:   []byte{1, 2, 3, 4, 5},
	}, true)
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}

	const expectedHex = "" +
		"6dd2cc3e" +
		"01000000" +
		"45230140fc54d941" +
		"c6b41348" +
		"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f" +
		"cfbcda32" +
		"a0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebf" +
		"00000000" +
		"03deadbe" +
		"0501020304050000"
	if actual := hex.EncodeToString(boxed); actual != expectedHex {
		t.Fatalf("wire bytes:\n%s\nwant:\n%s", actual, expectedHex)
	}

	var parsed BroadcastPlumtreeSimple
	if err = parsePlumtreeMessage(&parsed, boxed); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if math.Float64bits(parsed.Timestamp) != math.Float64bits(timestamp) {
		t.Fatalf(
			"timestamp bits = %#016x, want %#016x",
			math.Float64bits(parsed.Timestamp),
			math.Float64bits(timestamp),
		)
	}
	if !bytes.Equal(parsed.Data, []byte{0xde, 0xad, 0xbe}) {
		t.Fatalf("data = %x", parsed.Data)
	}

	parsed.Data[0] = 0xaa
	const dataOffset = 93
	if boxed[dataOffset] != 0xaa {
		t.Fatal("parsed data does not alias the boxed input")
	}

	disallowedSource := append([]byte{}, boxed...)
	binary.LittleEndian.PutUint32(
		disallowedSource[16:20],
		tl.CRC("overlay.query overlay:int256 = True"),
	)
	var rejected BroadcastPlumtreeSimple
	if err := parsePlumtreeMessage(&rejected, disallowedSource); err == nil {
		t.Fatal("parser accepted a non-PublicKey source constructor")
	}

	disallowedCertificate := append([]byte{}, boxed...)
	binary.LittleEndian.PutUint32(
		disallowedCertificate[52:56],
		tl.CRC("overlay.query overlay:int256 = True"),
	)
	if err := parsePlumtreeMessage(&rejected, disallowedCertificate); err == nil {
		t.Fatal("parser accepted a non-certificate constructor")
	}
}

func TestPlumtreeStatsPushPreflight(t *testing.T) {
	t.Parallel()

	source := bytes.Repeat([]byte{0x73}, sha256.Size)
	record := PlumtreeStatsRecord{
		Source:             source,
		Epoch:              7,
		TreeIndex:          3,
		EagerSimple:        []int64{1, 2, 3, 4},
		EagerRotating:      []int64{5, 6},
		DeliveredBroadcast: 8,
		PartsInP99:         30,
		UsefulAverageMS:    11,
		UsefulMaximumMS:    12,
		UsefulP99MS:        20,
	}
	serialize := func(record PlumtreeStatsRecord) []byte {
		wire, err := tl.Serialize(PlumtreeStatsPush{
			Record: record,
		}, true)
		if err != nil {
			t.Fatalf("serialize stats push: %v", err)
		}
		return wire
	}

	boxed := serialize(record)
	var parsed PlumtreeStatsPush
	if err := parsePlumtreeMessage(&parsed, boxed); err != nil {
		t.Fatalf("parse stats push: %v", err)
	}
	if !bytes.Equal(parsed.Record.Source, source) ||
		parsed.Record.Epoch != record.Epoch ||
		parsed.Record.TreeIndex != record.TreeIndex ||
		!slices.Equal(parsed.Record.EagerSimple, record.EagerSimple) ||
		!slices.Equal(parsed.Record.EagerRotating, record.EagerRotating) ||
		parsed.Record.DeliveredBroadcast != record.DeliveredBroadcast ||
		parsed.Record.PartsInP99 != record.PartsInP99 ||
		parsed.Record.UsefulAverageMS != record.UsefulAverageMS ||
		parsed.Record.UsefulMaximumMS != record.UsefulMaximumMS ||
		parsed.Record.UsefulP99MS != record.UsefulP99MS {
		t.Fatalf("parsed stats record = %+v, want %+v", parsed.Record, record)
	}

	oversizedSimple := record
	oversizedSimple.EagerSimple = []int64{1, 2, 3, 4, 5}
	oversizedRotating := record
	oversizedRotating.EagerRotating = []int64{1, 2, 3, 4, 5}

	const firstVectorCountOffset = 8 + sha256.Size + 8
	hugeCount := bytes.Clone(boxed)
	binary.LittleEndian.PutUint32(hugeCount[firstVectorCountOffset:], math.MaxUint32)

	tests := []struct {
		name string
		wire []byte
	}{
		{
			name: "oversized eager_0",
			wire: serialize(oversizedSimple),
		},
		{
			name: "oversized eager_index",
			wire: serialize(oversizedRotating),
		},
		{
			name: "huge eager_0 count",
			wire: hugeCount,
		},
		{
			name: "truncated",
			wire: boxed[:len(boxed)-1],
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var rejected PlumtreeStatsPush
			if err := parsePlumtreeMessage(&rejected, test.wire); err == nil {
				t.Fatal("parser accepted invalid stats push")
			}
			if rejected.Record.EagerSimple != nil ||
				rejected.Record.EagerRotating != nil {
				t.Fatal("parser populated vectors before rejecting stats push")
			}
		})
	}
}

func TestPlumtreeHashAndSignatureGoldens(t *testing.T) {
	t.Parallel()

	fullPayload := []byte("plumtree golden payload")
	fullHash := sha256.Sum256(fullPayload)
	partSize := plumtreeFECPartSize(int32(len(fullPayload)))
	broadcastID, err := plumtreeFECBroadcastID(
		overlay.BroadcastFlagAnySender,
		fullHash[:],
		int32(len(fullPayload)),
		partSize,
	)
	if err != nil {
		t.Fatalf("calculate FEC broadcast ID: %v", err)
	}
	const expectedBroadcastID = "860224d1281d77768098acb75b6e1f670a51ff29d08351370dc3aa5a65b871bf"
	if actual := hex.EncodeToString(broadcastID[:]); actual != expectedBroadcastID {
		t.Fatalf("broadcast ID = %s, want %s", actual, expectedBroadcastID)
	}

	timestamp := math.Float64frombits(0x41d954fc40012345)
	simpleData := []byte{0xde, 0xad, 0xbe}
	simpleHash := sha256.Sum256(simpleData)
	simpleToSign, err := plumtreeSimpleToSignBytes(
		broadcastID[:],
		timestamp,
		plumtreeSimpleTree,
		int32(len(simpleData)),
		simpleHash[:],
	)
	if err != nil {
		t.Fatalf("serialize simple to-sign: %v", err)
	}
	const expectedSimpleToSign = "" +
		"8e33f338" +
		expectedBroadcastID +
		"45230140fc54d941" +
		"00000000" +
		"03000000" +
		"08d2eb20ba68a2bf98f4b9718c779c2d254b88b67019e710f3065e03063de2bd"
	if actual := hex.EncodeToString(simpleToSign); actual != expectedSimpleToSign {
		t.Fatalf("simple to-sign = %s, want %s", actual, expectedSimpleToSign)
	}

	seed := make([]byte, ed25519.SeedSize)
	for i := range seed {
		seed[i] = byte(i)
	}
	signature := ed25519.Sign(ed25519.NewKeyFromSeed(seed), simpleToSign)
	const expectedSignature = "" +
		"6503b5e81b137e63b27461fa4b6184e76012706a22d1cb395f003a12ebe98294" +
		"3fbbc046b979b7d54787fec24b891b35b1b3057a670c9a756d8911087154630f"
	if actual := hex.EncodeToString(signature); actual != expectedSignature {
		t.Fatalf("signature = %s, want %s", actual, expectedSignature)
	}

	symbol := []byte{0x7f}
	symbolHash := sha256.Sum256(symbol)
	fecToSign, err := plumtreeFECToSignBytes(
		broadcastID[:],
		timestamp,
		7,
		8,
		int32(len(symbol)),
		symbolHash[:],
	)
	if err != nil {
		t.Fatalf("serialize FEC to-sign: %v", err)
	}
	const expectedFECToSign = "" +
		"94c0e1c3" +
		expectedBroadcastID +
		"45230140fc54d941" +
		"07000000" +
		"08000000" +
		"01000000" +
		"620bfdaa346b088fb49998d92f19a7eaf6bfc2fb0aee015753966da1028cb731"
	if actual := hex.EncodeToString(fecToSign); actual != expectedFECToSign {
		t.Fatalf("FEC to-sign = %s, want %s", actual, expectedFECToSign)
	}
}

func TestPlumtreeBroadcastRoundTrips(t *testing.T) {
	t.Parallel()

	id := bytes.Repeat([]byte{0x31}, sha256.Size)
	source := keys.PublicKeyED25519{
		Key: ed25519.PublicKey(bytes.Repeat([]byte{0x42}, ed25519.PublicKeySize)),
	}
	certificate := overlay.CertificateEmpty{}
	tests := []struct {
		name  string
		value any
	}{
		{
			name:  "not found",
			value: BroadcastNotFound{},
		},
		{
			name: "simple",
			value: BroadcastPlumtreeSimple{
				Flags:       overlay.BroadcastFlagAnySender,
				Timestamp:   math.Float64frombits(0x41d954fc40012345),
				Source:      source,
				Certificate: certificate,
				BroadcastID: id,
				TreeIndex:   0,
				Data:        []byte{1, 2, 3},
				Signature:   []byte{4, 5, 6},
			},
		},
		{
			name: "fec",
			value: BroadcastPlumtreeFEC{
				Flags:        overlay.BroadcastFlagAnySender,
				Timestamp:    math.Float64frombits(0x41d954fc40012345),
				Source:       source,
				Certificate:  certificate,
				FullDataHash: id,
				FullDataSize: 31,
				PartIndex:    2,
				TreeIndex:    3,
				Data:         []byte{1, 2},
				Signature:    []byte{4, 5, 6},
			},
		},
		{
			name: "ihave",
			value: BroadcastPlumtreeIHave{
				BroadcastID:      id,
				Timestamp:        math.Float64frombits(0x41d954fc40012345),
				PartIndex:        2,
				TreeIndex:        3,
				Source:           source,
				Certificate:      certificate,
				PayloadTimestamp: math.Float64frombits(0x41d954fc40054321),
				DataSize:         2,
				DataHash:         id,
				Signature:        []byte{4, 5, 6},
			},
		},
		{
			name: "prune",
			value: BroadcastPlumtreePrune{
				BroadcastID: id,
				Timestamp:   math.Float64frombits(0x41d954fc40012345),
				PartIndex:   2,
				TreeIndex:   3,
			},
		},
		{
			name: "useful",
			value: BroadcastPlumtreeUseful{
				BroadcastID: id,
				Timestamp:   math.Float64frombits(0x41d954fc40012345),
				PartIndex:   2,
				TreeIndex:   3,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			boxed, err := tl.Serialize(test.value, true)
			if err != nil {
				t.Fatalf("serialize: %v", err)
			}
			var parsed any
			if err = parsePlumtreeMessage(&parsed, boxed); err != nil {
				t.Fatalf("parse: %v", err)
			}
			reserialized, err := tl.Serialize(parsed, true)
			if err != nil {
				t.Fatalf("reserialize: %v", err)
			}
			if !bytes.Equal(reserialized, boxed) {
				t.Fatalf("reserialized bytes differ:\n%x\n%x", reserialized, boxed)
			}
		})
	}
}

func TestPlumtreeSigningTypesRoundTrip(t *testing.T) {
	t.Parallel()

	id := bytes.Repeat([]byte{0x51}, sha256.Size)
	timestamp := math.Float64frombits(0x41d954fc40012345)
	simple := BroadcastPlumtreeSimpleToSign{
		ID:        id,
		Timestamp: timestamp,
		TreeIndex: 0,
		DataSize:  3,
		DataHash:  id,
	}
	simpleBytes, err := tl.Serialize(simple, true)
	if err != nil {
		t.Fatalf("serialize simple: %v", err)
	}
	var parsedSimple BroadcastPlumtreeSimpleToSign
	rest, err := tl.ParseNoCopy(&parsedSimple, simpleBytes, true)
	if err != nil {
		t.Fatalf("parse simple: %v", err)
	}
	if len(rest) != 0 {
		t.Fatalf("simple trailing bytes = %d", len(rest))
	}
	if math.Float64bits(parsedSimple.Timestamp) != math.Float64bits(timestamp) {
		t.Fatalf("simple timestamp bits changed")
	}

	fec := BroadcastPlumtreeFECToSign{
		ID:        id,
		Timestamp: timestamp,
		PartIndex: 2,
		TreeIndex: 3,
		DataSize:  2,
		DataHash:  id,
	}
	fecBytes, err := tl.Serialize(fec, true)
	if err != nil {
		t.Fatalf("serialize FEC: %v", err)
	}
	var parsedFEC BroadcastPlumtreeFECToSign
	rest, err = tl.ParseNoCopy(&parsedFEC, fecBytes, true)
	if err != nil {
		t.Fatalf("parse FEC: %v", err)
	}
	if len(rest) != 0 {
		t.Fatalf("FEC trailing bytes = %d", len(rest))
	}
	if math.Float64bits(parsedFEC.Timestamp) != math.Float64bits(timestamp) {
		t.Fatalf("FEC timestamp bits changed")
	}
}

func TestValidatePlumtreeTimestampBoundaries(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_700_000_000, 250_000_000)
	nowSeconds := float64(now.Unix()) + 0.25
	tests := []struct {
		name      string
		timestamp float64
		wantError bool
	}{
		{name: "old boundary", timestamp: nowSeconds - 20},
		{name: "new boundary", timestamp: nowSeconds + 20},
		{
			name:      "older than boundary",
			timestamp: math.Nextafter(nowSeconds-20, math.Inf(-1)),
			wantError: true,
		},
		{
			name:      "newer than boundary",
			timestamp: math.Nextafter(nowSeconds+20, math.Inf(1)),
			wantError: true,
		},
		{name: "nan", timestamp: math.NaN(), wantError: true},
		{name: "positive infinity", timestamp: math.Inf(1), wantError: true},
		{name: "negative infinity", timestamp: math.Inf(-1), wantError: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := validatePlumtreeTimestamp(test.timestamp, now)
			if (err != nil) != test.wantError {
				t.Fatalf("validate timestamp error = %v, wantError %v", err, test.wantError)
			}
		})
	}
}

func TestValidatePlumtreeControlFields(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_700_000_000, 0)
	id := bytes.Repeat([]byte{1}, sha256.Size)
	tests := []struct {
		name      string
		id        []byte
		partIndex int32
		treeIndex int32
		wantError bool
	}{
		{name: "simple", id: id, partIndex: 0, treeIndex: 0},
		{name: "first FEC", id: id, partIndex: 0, treeIndex: 1},
		{name: "last FEC", id: id, partIndex: 44, treeIndex: 45},
		{name: "short id", id: id[:31], partIndex: 0, treeIndex: 0, wantError: true},
		{name: "long id", id: append(append([]byte{}, id...), 1), partIndex: 0, treeIndex: 0, wantError: true},
		{name: "zero id", id: make([]byte, sha256.Size), partIndex: 0, treeIndex: 0, wantError: true},
		{name: "negative part", id: id, partIndex: -1, treeIndex: 0, wantError: true},
		{name: "negative tree", id: id, partIndex: 0, treeIndex: -1, wantError: true},
		{name: "simple nonzero part", id: id, partIndex: 1, treeIndex: 0, wantError: true},
		{name: "mismatched FEC tree", id: id, partIndex: 2, treeIndex: 2, wantError: true},
		{name: "part past end", id: id, partIndex: 45, treeIndex: 45, wantError: true},
		{name: "tree past end", id: id, partIndex: 45, treeIndex: 46, wantError: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := validatePlumtreeControl(
				test.id,
				float64(now.Unix()),
				test.partIndex,
				test.treeIndex,
				now,
			)
			if (err != nil) != test.wantError {
				t.Fatalf("validate control error = %v, wantError %v", err, test.wantError)
			}
		})
	}
}

func TestValidatePlumtreeSizesAndFECMapping(t *testing.T) {
	t.Parallel()

	sizeTests := []struct {
		name      string
		size      int32
		wantError bool
	}{
		{name: "one", size: 1},
		{name: "maximum", size: plumtreeMaxPayloadSize},
		{name: "zero", size: 0, wantError: true},
		{name: "negative", size: -1, wantError: true},
		{name: "over maximum", size: plumtreeMaxPayloadSize + 1, wantError: true},
	}
	for _, test := range sizeTests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := validatePlumtreePayloadSize(test.size)
			if (err != nil) != test.wantError {
				t.Fatalf("validate payload size error = %v, wantError %v", err, test.wantError)
			}
		})
	}

	hash := bytes.Repeat([]byte{1}, sha256.Size)
	fecTests := []struct {
		name         string
		hash         []byte
		fullDataSize int32
		partIndex    int32
		treeIndex    int32
		symbolSize   int
		wantError    bool
	}{
		{name: "one byte", hash: hash, fullDataSize: 1, partIndex: 0, treeIndex: 1, symbolSize: 1},
		{name: "thirty bytes", hash: hash, fullDataSize: 30, partIndex: 44, treeIndex: 45, symbolSize: 1},
		{name: "thirty one bytes", hash: hash, fullDataSize: 31, partIndex: 1, treeIndex: 2, symbolSize: 2},
		{
			name:         "maximum",
			hash:         hash,
			fullDataSize: plumtreeMaxPayloadSize,
			partIndex:    44,
			treeIndex:    45,
			symbolSize:   int(plumtreeFECPartSize(plumtreeMaxPayloadSize)),
		},
		{
			name:         "short hash",
			hash:         hash[:31],
			fullDataSize: 30,
			partIndex:    0,
			treeIndex:    1,
			symbolSize:   1,
			wantError:    true,
		},
		{
			name:         "wrong symbol size",
			hash:         hash,
			fullDataSize: 31,
			partIndex:    0,
			treeIndex:    1,
			symbolSize:   1,
			wantError:    true,
		},
		{
			name:         "negative part",
			hash:         hash,
			fullDataSize: 30,
			partIndex:    -1,
			treeIndex:    0,
			symbolSize:   1,
			wantError:    true,
		},
		{
			name:         "part past end",
			hash:         hash,
			fullDataSize: 30,
			partIndex:    45,
			treeIndex:    46,
			symbolSize:   1,
			wantError:    true,
		},
	}
	for _, test := range fecTests {
		t.Run("fec "+test.name, func(t *testing.T) {
			t.Parallel()

			err := validatePlumtreeFECFields(
				test.hash,
				test.fullDataSize,
				test.partIndex,
				test.treeIndex,
				make([]byte, test.symbolSize),
			)
			if (err != nil) != test.wantError {
				t.Fatalf("validate FEC fields error = %v, wantError %v", err, test.wantError)
			}
		})
	}
}

func TestValidatePlumtreeMessages(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_700_000_000, 0)
	id := bytes.Repeat([]byte{1}, sha256.Size)
	source := keys.PublicKeyED25519{Key: ed25519.PublicKey(bytes.Repeat([]byte{2}, ed25519.PublicKeySize))}
	valid := BroadcastPlumtreeSimple{
		Timestamp:   float64(now.Unix()),
		Source:      source,
		Certificate: overlay.CertificateEmpty{},
		BroadcastID: id,
		TreeIndex:   0,
		Data:        []byte{1},
	}
	if err := validateBroadcastPlumtreeSimple(valid, now); err != nil {
		t.Fatalf("validate valid simple: %v", err)
	}

	ihave := BroadcastPlumtreeIHave{
		BroadcastID:      id,
		Timestamp:        float64(now.Unix()),
		PartIndex:        0,
		TreeIndex:        1,
		Source:           source,
		Certificate:      overlay.CertificateEmpty{},
		PayloadTimestamp: float64(now.Unix()),
		DataSize:         plumtreeFECPartSize(plumtreeMaxPayloadSize),
		DataHash:         id,
		Signature:        bytes.Repeat([]byte{7}, ed25519.SignatureSize),
	}
	if err := validateBroadcastPlumtreeIHave(ihave, now); err != nil {
		t.Fatalf("validate valid IHAVE: %v", err)
	}
	// A signature that can never verify must be refused before the announcement
	// is buffered, since nothing downstream caps its length.
	short := ihave
	short.Signature = []byte{7}
	if err := validateBroadcastPlumtreeIHave(short, now); err == nil {
		t.Fatal("IHAVE with a short signature was accepted")
	}
	ihave.DataSize++
	if err := validateBroadcastPlumtreeIHave(ihave, now); err == nil {
		t.Fatal("oversized FEC IHAVE was accepted")
	}
}

func TestPlumtreeExactBoxedParsers(t *testing.T) {
	t.Parallel()

	id := bytes.Repeat([]byte{1}, sha256.Size)
	pruneBytes, err := tl.Serialize(BroadcastPlumtreePrune{
		BroadcastID: id,
		Timestamp:   1.25,
		PartIndex:   0,
		TreeIndex:   1,
	}, true)
	if err != nil {
		t.Fatalf("serialize prune: %v", err)
	}

	var prune BroadcastPlumtreePrune
	if err = parsePlumtreeMessage(&prune, pruneBytes); err != nil {
		t.Fatalf("parse prune: %v", err)
	}
	if err = parsePlumtreeMessage(
		&prune,
		append(append([]byte{}, pruneBytes...), 0),
	); err == nil {
		t.Fatal("message parser accepted trailing data")
	}
	var notFound BroadcastNotFound
	if err = parsePlumtreeMessage(&notFound, pruneBytes); err == nil {
		t.Fatal("message parser accepted a mismatched constructor")
	}

	notFoundBytes, err := tl.Serialize(BroadcastNotFound{}, true)
	if err != nil {
		t.Fatalf("serialize not found: %v", err)
	}
	if err = parsePlumtreeMessage(&notFound, notFoundBytes); err != nil {
		t.Fatalf("parse not found: %v", err)
	}

	requestBytes, err := tl.Serialize(RepairPlumtreePart{
		BroadcastID: id,
		Timestamp:   1.25,
		PartIndex:   0,
		TreeIndex:   1,
	}, true)
	if err != nil {
		t.Fatalf("serialize repair request: %v", err)
	}
	if _, err = parseRepairPlumtreePart(requestBytes); err != nil {
		t.Fatalf("parse repair request: %v", err)
	}
	if _, err = parseRepairPlumtreePart(append(append([]byte{}, requestBytes...), 0)); err == nil {
		t.Fatal("repair request parser accepted trailing data")
	}
}

// fuzzParsePlumtreeBroadcast mirrors the constructor dispatch in
// plumtreeRuntime.processMessage so the fuzzer keeps exercising every concrete
// wire parser.
func fuzzParsePlumtreeBroadcast(data []byte) {
	if len(data) < 4 {
		return
	}
	switch binary.LittleEndian.Uint32(data[:4]) {
	case broadcastPlumtreeSimpleConstructorID:
		var message BroadcastPlumtreeSimple
		_ = parsePlumtreeMessage(&message, data)
	case broadcastPlumtreeFECConstructorID:
		var message BroadcastPlumtreeFEC
		_ = parsePlumtreeMessage(&message, data)
	case broadcastPlumtreeIHaveConstructorID:
		var message BroadcastPlumtreeIHave
		_ = parsePlumtreeMessage(&message, data)
	case broadcastPlumtreePruneConstructorID:
		var message BroadcastPlumtreePrune
		_ = parsePlumtreeMessage(&message, data)
	case broadcastPlumtreeUsefulConstructorID:
		var message BroadcastPlumtreeUseful
		_ = parsePlumtreeMessage(&message, data)
	case plumtreeStatsPushConstructorID:
		var message PlumtreeStatsPush
		_ = parsePlumtreeMessage(&message, data)
	case broadcastNotFoundConstructorID:
		var message BroadcastNotFound
		_ = parsePlumtreeMessage(&message, data)
	}
}

func FuzzParsePlumtreeBroadcast(f *testing.F) {
	id := bytes.Repeat([]byte{1}, sha256.Size)
	seed, err := tl.Serialize(BroadcastPlumtreePrune{
		BroadcastID: id,
		Timestamp:   1.25,
		PartIndex:   0,
		TreeIndex:   1,
	}, true)
	if err != nil {
		f.Fatalf("serialize seed: %v", err)
	}
	f.Add(seed)
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0})

	f.Fuzz(func(t *testing.T, data []byte) {
		fuzzParsePlumtreeBroadcast(data)
	})
}

// A simple payload must stay pinned to the simple tree. validatePlumtreeControl
// only relates the part and tree indices, and part 0 relates to FEC tree 1 just
// as legitimately as to tree 0 - so an unpinned simple payload could take an FEC
// slot, where getPartLocked never finds it again and where the IHAVE forwarded
// for it carries a signature every neighbour verifies against the FEC preimage
// and rejects, banning this node's plumtree traffic for the rejection window.
func TestValidateBroadcastPlumtreeSimplePinsTreeIndex(t *testing.T) {
	now := time.Now()
	message := BroadcastPlumtreeSimple{
		BroadcastID: bytes.Repeat([]byte{0x11}, 32),
		Timestamp:   float64(now.Unix()),
		TreeIndex:   plumtreeSimpleTree,
		Data:        []byte{1, 2, 3},
	}
	if err := validateBroadcastPlumtreeSimple(message, now); err != nil {
		t.Fatalf("simple tree rejected: %v", err)
	}

	for _, treeIndex := range []int32{
		plumtreeFECTreeOffset,
		plumtreeFECTreeOffset + 1,
		plumtreeTreeSlots - 1,
	} {
		wrong := message
		wrong.TreeIndex = treeIndex
		if err := validateBroadcastPlumtreeSimple(wrong, now); err == nil {
			t.Fatalf("simple payload accepted on tree %d", treeIndex)
		}
	}
}

// Reflection-based oracles for the compact serializers in
// plumtree_engine_payload.go: production writes the bytes by hand, these prove
// the two agree.
func plumtreeFECBroadcastID(
	flags int32,
	fullDataHash []byte,
	fullDataSize int32,
	partSize int32,
) ([sha256.Size]byte, error) {
	if err := validatePlumtreeInt256(fullDataHash, "full data hash"); err != nil {
		return [sha256.Size]byte{}, err
	}
	if err := validatePlumtreePayloadSize(fullDataSize); err != nil {
		return [sha256.Size]byte{}, err
	}
	if partSize <= 0 || partSize != plumtreeFECPartSize(fullDataSize) {
		return [sha256.Size]byte{}, fmt.Errorf("invalid Plumtree FEC part size %d", partSize)
	}

	boxed, err := tl.Serialize(PlumtreeFECID{
		Flags:        flags,
		FullDataHash: fullDataHash,
		FullDataSize: fullDataSize,
		PartSize:     partSize,
	}, true)
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("serialize Plumtree FEC id: %w", err)
	}
	return sha256.Sum256(boxed), nil
}

func plumtreeSimpleToSignBytes(
	broadcastID []byte,
	timestamp float64,
	treeIndex int32,
	dataSize int32,
	dataHash []byte,
) ([]byte, error) {
	if err := validatePlumtreeInt256(broadcastID, "broadcast id"); err != nil {
		return nil, err
	}
	if plumtreeInt256IsZero(broadcastID) {
		return nil, fmt.Errorf("empty Plumtree broadcast id")
	}
	if err := validatePlumtreeInt256(dataHash, "data hash"); err != nil {
		return nil, err
	}
	if math.IsNaN(timestamp) || math.IsInf(timestamp, 0) {
		return nil, fmt.Errorf("invalid Plumtree timestamp")
	}
	if treeIndex != plumtreeSimpleTree {
		return nil, fmt.Errorf("invalid Plumtree simple tree %d", treeIndex)
	}
	if err := validatePlumtreePayloadSize(dataSize); err != nil {
		return nil, err
	}

	boxed, err := tl.Serialize(BroadcastPlumtreeSimpleToSign{
		ID:        broadcastID,
		Timestamp: timestamp,
		TreeIndex: treeIndex,
		DataSize:  dataSize,
		DataHash:  dataHash,
	}, true)
	if err != nil {
		return nil, fmt.Errorf("serialize Plumtree simple signature payload: %w", err)
	}
	return boxed, nil
}

func plumtreeFECToSignBytes(
	broadcastID []byte,
	timestamp float64,
	partIndex int32,
	treeIndex int32,
	dataSize int32,
	dataHash []byte,
) ([]byte, error) {
	if err := validatePlumtreeInt256(broadcastID, "broadcast id"); err != nil {
		return nil, err
	}
	if plumtreeInt256IsZero(broadcastID) {
		return nil, fmt.Errorf("empty Plumtree broadcast id")
	}
	if err := validatePlumtreeInt256(dataHash, "data hash"); err != nil {
		return nil, err
	}
	if math.IsNaN(timestamp) || math.IsInf(timestamp, 0) {
		return nil, fmt.Errorf("invalid Plumtree timestamp")
	}
	if partIndex < 0 || partIndex >= plumtreeFECTotalParts {
		return nil, fmt.Errorf("invalid Plumtree FEC part index %d", partIndex)
	}
	if treeIndex != partIndex+plumtreeFECTreeOffset {
		return nil, fmt.Errorf("Plumtree FEC part %d uses tree %d", partIndex, treeIndex)
	}
	if dataSize <= 0 || dataSize > plumtreeFECPartSize(plumtreeMaxPayloadSize) {
		return nil, fmt.Errorf("invalid Plumtree FEC symbol size %d", dataSize)
	}

	boxed, err := tl.Serialize(BroadcastPlumtreeFECToSign{
		ID:        broadcastID,
		Timestamp: timestamp,
		PartIndex: partIndex,
		TreeIndex: treeIndex,
		DataSize:  dataSize,
		DataHash:  dataHash,
	}, true)
	if err != nil {
		return nil, fmt.Errorf("serialize Plumtree FEC signature payload: %w", err)
	}
	return boxed, nil
}
