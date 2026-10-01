package collator

import (
	"bytes"
	"crypto/ed25519"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/blockproof"
)

// Every TopBlockDescr reaching masterchain verification carries BlockSignatures:
// C++ prevalidate refuses one without them outside fake mode, and
// validateMasterTopBlockDescrBinding's comment says the descriptor is already
// signature-checked by acquisition. The signature-less fixtures elsewhere in the
// package therefore never reach validateTopBlockSignatures and the whole strict
// TL-B walk below it — including the recursive chained-certificate path — so
// these tests drive it directly.

const topSignaturesTestCatchainSeqno = uint32(0x51ce)

type topSignaturesFixture struct {
	prepared   *blockproof.PreparedValidatorSet
	signatures []ton.Signature
	weight     uint64
}

// newTopSignaturesFixture builds a validator set and the signature list
// blockproof serializes for it. The signatures are not verifiable — the TL-B
// validator checks shape only — but the node ids must be the real tl.Hash of
// the public key or blockproof refuses to serialize them.
func newTopSignaturesFixture(t testing.TB, count int) topSignaturesFixture {
	t.Helper()

	validators := make([]*tlb.ValidatorAddr, 0, count)
	signatures := make([]ton.Signature, 0, count)
	var weight uint64
	for i := 0; i < count; i++ {
		seed := make([]byte, ed25519.SeedSize)
		seed[0] = byte(i + 1)
		public := ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey)
		validators = append(validators, &tlb.ValidatorAddr{
			PublicKey: tlb.SigPubKeyED25519{Key: public},
			Weight:    uint64(i + 1),
			ADNLAddr:  bytes.Repeat([]byte{byte(i + 1)}, 32),
		})
		weight += uint64(i + 1)

		nodeID, err := tl.Hash(keys.PublicKeyED25519{Key: public})
		if err != nil {
			t.Fatal(err)
		}
		signature := bytes.Repeat([]byte{byte(0xa0 + i)}, ed25519.SignatureSize)
		signatures = append(signatures, ton.Signature{NodeIDShort: nodeID, Signature: signature})
	}

	prepared, err := blockproof.PrepareValidatorSet(topSignaturesTestCatchainSeqno, validators)
	if err != nil {
		t.Fatal(err)
	}
	return topSignaturesFixture{prepared: prepared, signatures: signatures, weight: weight}
}

// simplexCell serializes the fixture through blockproof, the other parser of
// block.tlb's BlockSignatures. Producing the valid cell there and consuming it
// here pins the two implementations against each other rather than pinning
// current collator behaviour twice.
func (f topSignaturesFixture) simplexCell(t testing.TB) *cell.Cell {
	t.Helper()

	set := blockproof.NewSimplexValidatorSignatureSet(
		f.prepared.CatchainSeqno(),
		f.prepared.Hash(),
		f.signatures,
		true,
		bytes.Repeat([]byte{0x7e}, 32),
		9,
		[]byte("top block descr candidate"),
	)
	root, err := set.FinalitySignaturesCell(f.prepared)
	if err != nil {
		t.Fatal(err)
	}
	return root
}

// ordinaryCell lays out block_signatures#11 by hand from block.tlb: blockproof
// only serializes the simplex twin, and the malformed rows need one field bent
// at a time. Passing no pairs uses the fixture's own ed25519 signatures.
func (f topSignaturesFixture) ordinaryCell(t testing.TB, pairs ...*cell.Cell) *cell.Cell {
	t.Helper()

	return f.signaturesBuilder(t, 0x11, pairs...).EndCell()
}

func (f topSignaturesFixture) signaturesBuilder(t testing.TB, tag uint64, pairs ...*cell.Cell) *cell.Builder {
	t.Helper()

	if len(pairs) == 0 {
		pairs = f.pairs(t)
	}
	dict := cell.NewDict(16)
	for i, pair := range pairs {
		key := cell.BeginCell().MustStoreUInt(uint64(i), 16).EndCell()
		if err := dict.Set(key, pair); err != nil {
			t.Fatal(err)
		}
	}
	builder := cell.BeginCell().
		MustStoreUInt(tag, 8).
		MustStoreUInt(uint64(f.prepared.Hash()), 32).
		MustStoreUInt(uint64(f.prepared.CatchainSeqno()), 32).
		MustStoreUInt(uint64(len(pairs)), 32).
		MustStoreUInt(f.weight, 64)
	if err := builder.StoreDict(dict); err != nil {
		t.Fatal(err)
	}
	return builder
}

// pairs builds sig_pair$_ node_id_short:bits256 sign:CryptoSignature with the
// plain ed25519_signature#5 arm, the shape blockproof also writes.
func (f topSignaturesFixture) pairs(t testing.TB) []*cell.Cell {
	t.Helper()

	pairs := make([]*cell.Cell, 0, len(f.signatures))
	for _, signature := range f.signatures {
		pairs = append(pairs, cell.BeginCell().
			MustStoreSlice(signature.NodeIDShort, 256).
			MustStoreUInt(0x5, 4).
			MustStoreSlice(signature.Signature[:32], 256).
			MustStoreSlice(signature.Signature[32:], 256).
			EndCell())
	}
	return pairs
}

// topSignaturesTestChainedPair builds the chained_signature#f arm: a
// ^SignedCertificate followed by the temporary key's simple signature.
func topSignaturesTestChainedPair(nodeID []byte, certificate *cell.Cell, tempKeyTag uint64) *cell.Cell {
	return cell.BeginCell().
		MustStoreSlice(nodeID, 256).
		MustStoreUInt(0xf, 4).
		MustStoreRef(certificate).
		MustStoreUInt(tempKeyTag, 4).
		MustStoreSlice(bytes.Repeat([]byte{0x66}, 32), 256).
		MustStoreSlice(bytes.Repeat([]byte{0x77}, 32), 256).
		EndCell()
}

// topSignaturesTestCertificate builds signed_certificate$_: certificate#4 with
// an ed25519_pubkey#8e81278a temporary key and its validity window, followed by
// the permanent key's signature over it.
func topSignaturesTestCertificate(tag, publicKeyTag uint64, signatureTag uint64, trailing bool) *cell.Cell {
	builder := cell.BeginCell().
		MustStoreUInt(tag, 4).
		MustStoreUInt(publicKeyTag, 32).
		MustStoreSlice(bytes.Repeat([]byte{0x33}, 32), 256).
		MustStoreUInt(1_700_000_000, 32).
		MustStoreUInt(1_700_003_600, 32).
		MustStoreUInt(signatureTag, 4).
		MustStoreSlice(bytes.Repeat([]byte{0x44}, 32), 256).
		MustStoreSlice(bytes.Repeat([]byte{0x55}, 32), 256)
	if trailing {
		builder.MustStoreUInt(1, 1)
	}
	return builder.EndCell()
}

// topSignaturesTestNestedCertificate signs a certificate with another chained
// signature: validateCryptoSignature recurses back into itself through
// validateSignedCertificate, and nothing else in the suite walks that arm.
func topSignaturesTestNestedCertificate(inner *cell.Cell) *cell.Cell {
	return cell.BeginCell().
		MustStoreUInt(0x4, 4).
		MustStoreUInt(0x8e81278a, 32).
		MustStoreSlice(bytes.Repeat([]byte{0x33}, 32), 256).
		MustStoreUInt(1_700_000_000, 32).
		MustStoreUInt(1_700_003_600, 32).
		MustStoreUInt(0xf, 4).
		MustStoreRef(inner).
		MustStoreUInt(0x5, 4).
		MustStoreSlice(bytes.Repeat([]byte{0x44}, 32), 256).
		MustStoreSlice(bytes.Repeat([]byte{0x55}, 32), 256).
		EndCell()
}

// TestTopBlockDescrSignatureFixtureMatchesBlockproof is the anchor for the
// table below: it proves the two "valid" fixtures are valid according to the
// independent parser, so a row that expects no error is asserting agreement
// between blockproof and the collator rather than a shared misreading.
func TestTopBlockDescrSignatureFixtureMatchesBlockproof(t *testing.T) {
	fixture := newTopSignaturesFixture(t, 3)

	for _, tc := range []struct {
		name    string
		root    *cell.Cell
		simplex bool
	}{
		{name: "ordinary", root: fixture.ordinaryCell(t)},
		{name: "simplex", root: fixture.simplexCell(t), simplex: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := blockproof.ParseBlockSignatureSetCell(tc.root)
			if err != nil {
				t.Fatalf("blockproof rejected the fixture: %v", err)
			}
			if parsed.DeclaredWeight != fixture.weight {
				t.Fatalf("declared weight = %d, want %d", parsed.DeclaredWeight, fixture.weight)
			}
			if got := parsed.ValidatorSignatures.SignatureCount(); got != len(fixture.signatures) {
				t.Fatalf("signature count = %d, want %d", got, len(fixture.signatures))
			}
			if parsed.ValidatorSignatures.IsSimplex() != tc.simplex {
				t.Fatalf("simplex = %t, want %t", parsed.ValidatorSignatures.IsSimplex(), tc.simplex)
			}
			if got := parsed.ValidatorSignatures.ValidatorSetHash(); got != fixture.prepared.Hash() {
				t.Fatalf("validator set hash = %08x, want %08x", got, fixture.prepared.Hash())
			}

			budget := tlbValidationBudget{remaining: topBlockDescrValidationBudget}
			if err = validateTopBlockSignatures(tc.root, &budget); err != nil {
				t.Fatalf("collator rejected a fixture blockproof accepts: %v", err)
			}
		})
	}
}

func TestVerifyTopBlockDescrSetValidatesSignatures(t *testing.T) {
	fixture := newTopSignaturesFixture(t, 3)
	block := testBlockID(0, -1<<63, 7, 0x5a)
	nodeID := fixture.signatures[0].NodeIDShort
	goodCertificate := topSignaturesTestCertificate(0x4, 0x8e81278a, 0x5, false)

	special, err := cell.CreateMerkleProof(fixture.ordinaryCell(t))
	if err != nil {
		t.Fatal(err)
	}

	trailingSet := fixture.signaturesBuilder(t, 0x11)
	trailingSet.MustStoreUInt(1, 1)

	// A 0x12 body without the session/slot/candidate tail: the same dictionary
	// that is complete under 0x11 must be rejected once the tag promises more.
	truncatedSimplex := fixture.signaturesBuilder(t, 0x12).EndCell()

	cases := []struct {
		name       string
		signatures *cell.Cell
		wantErr    string
	}{
		{name: "ordinary", signatures: fixture.ordinaryCell(t)},
		{name: "simplex tail", signatures: fixture.simplexCell(t)},
		{
			name: "chained signature",
			signatures: fixture.ordinaryCell(t,
				topSignaturesTestChainedPair(nodeID, goodCertificate, 0x5)),
		},
		{
			name: "nested certificate",
			signatures: fixture.ordinaryCell(t, topSignaturesTestChainedPair(
				nodeID, topSignaturesTestNestedCertificate(goodCertificate), 0x5)),
		},
		{
			name: "nested certificate tag",
			signatures: fixture.ordinaryCell(t, topSignaturesTestChainedPair(
				nodeID,
				topSignaturesTestNestedCertificate(
					topSignaturesTestCertificate(0x6, 0x8e81278a, 0x5, false)),
				0x5)),
			wantErr: "invalid certificate tag",
		},
		{
			name:       "unknown tag",
			signatures: fixture.signaturesBuilder(t, 0x13).EndCell(),
			wantErr:    "invalid BlockSignatures tag",
		},
		{
			name:       "trailing data",
			signatures: trailingSet.EndCell(),
			wantErr:    "trailing BlockSignatures data",
		},
		{
			name:       "simplex without tail",
			signatures: truncatedSimplex,
			wantErr:    "load simplex BlockSignatures fields",
		},
		{
			name:       "special cell",
			signatures: special,
			wantErr:    "special BlockSignatures cell",
		},
		{
			name: "unknown signature tag",
			signatures: fixture.ordinaryCell(t, cell.BeginCell().
				MustStoreSlice(nodeID, 256).
				MustStoreUInt(0x3, 4).
				MustStoreSlice(bytes.Repeat([]byte{0x11}, 64), 512).
				EndCell()),
			wantErr: "invalid signature tag 3",
		},
		{
			name: "trailing signature pair data",
			signatures: fixture.ordinaryCell(t, cell.BeginCell().
				MustStoreSlice(nodeID, 256).
				MustStoreUInt(0x5, 4).
				MustStoreSlice(bytes.Repeat([]byte{0x11}, 64), 512).
				MustStoreUInt(1, 1).
				EndCell()),
			wantErr: "trailing signature pair data",
		},
		{
			name: "certificate tag",
			signatures: fixture.ordinaryCell(t, topSignaturesTestChainedPair(
				nodeID, topSignaturesTestCertificate(0x5, 0x8e81278a, 0x5, false), 0x5)),
			wantErr: "invalid certificate tag",
		},
		{
			name: "certificate public key tag",
			signatures: fixture.ordinaryCell(t, topSignaturesTestChainedPair(
				nodeID, topSignaturesTestCertificate(0x4, 0x8e81278b, 0x5, false), 0x5)),
			wantErr: "invalid certificate public key tag",
		},
		{
			name: "certificate signature tag",
			signatures: fixture.ordinaryCell(t, topSignaturesTestChainedPair(
				nodeID, topSignaturesTestCertificate(0x4, 0x8e81278a, 0x6, false), 0x5)),
			wantErr: "invalid certificate signature",
		},
		{
			name: "trailing certificate data",
			signatures: fixture.ordinaryCell(t, topSignaturesTestChainedPair(
				nodeID, topSignaturesTestCertificate(0x4, 0x8e81278a, 0x5, true), 0x5)),
			wantErr: "trailing SignedCertificate data",
		},
		{
			name: "temporary key signature tag",
			signatures: fixture.ordinaryCell(t,
				topSignaturesTestChainedPair(nodeID, goodCertificate, 0x6)),
			wantErr: "invalid temporary key signature",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			descriptor := masterBuildSignedTopBlockDescr(t, block, tc.signatures)
			set := verificationTopBlockDescrSet(t, cell.BeginCell().MustStoreRef(descriptor).EndCell())
			err := verifyTopBlockDescrSet(set)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("signed TopBlockDescrSet rejected: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("TopBlockDescrSet error = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

// The TL-B budget is shared with the descriptor set, so a candidate can also
// burn it inside the signatures: a dictionary of empty pairs and a chain of
// certificates are both unbounded in the cell shape alone. The set-level guard
// is already covered by TestVerifyTopBlockDescrSetEnforcesSharedTLBBudget; this
// reaches the two consume() sites below it without building a huge fixture.
func TestValidateTopBlockSignaturesConsumesSharedBudget(t *testing.T) {
	fixture := newTopSignaturesFixture(t, 3)
	chained := fixture.ordinaryCell(t, topSignaturesTestChainedPair(
		fixture.signatures[0].NodeIDShort,
		topSignaturesTestCertificate(0x4, 0x8e81278a, 0x5, false),
		0x5,
	))

	for _, tc := range []struct {
		name      string
		root      *cell.Cell
		remaining int
	}{
		// The first dictionary entry costs one operation, every later one two.
		{name: "dictionary entries", root: fixture.ordinaryCell(t), remaining: 2},
		// One entry fits; the ^SignedCertificate ref behind it does not.
		{name: "chained certificate", root: chained, remaining: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			budget := tlbValidationBudget{remaining: tc.remaining}
			err := validateTopBlockSignatures(tc.root, &budget)
			if err == nil || !strings.Contains(err.Error(), "exceeds") {
				t.Fatalf("over-budget BlockSignatures error = %v", err)
			}
		})
	}
}

// The signatures ref is optional in the fixture builder because the masterchain
// parity goldens are hashed over the signature-less descriptor. This pins that
// default so a future signed fixture cannot silently move those hashes.
func TestMasterBuildTopBlockDescrDefaultsToUnsigned(t *testing.T) {
	block := testBlockID(0, -1<<63, 7, 0x5a)
	descriptor := masterBuildTopBlockDescr(t, block)

	loader, err := descriptor.BeginParse()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = loader.LoadUInt(8); err != nil {
		t.Fatal(err)
	}
	var proofFor masterTopBlockDescrID
	if err = tlb.LoadFromCell(&proofFor, loader); err != nil {
		t.Fatal(err)
	}
	hasSignatures, err := loader.LoadBoolBit()
	if err != nil {
		t.Fatal(err)
	}
	if hasSignatures {
		t.Fatal("the default TopBlockDescr fixture carries signatures")
	}
	if descriptor.RefsNum() != 1 {
		t.Fatalf("unsigned TopBlockDescr has %d refs, want 1", descriptor.RefsNum())
	}
}
