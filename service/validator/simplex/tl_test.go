package simplex

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

// TestConstructorIDsGolden pins the TL constructor ids so an accidental edit
// of a scheme string breaks loudly. pub.ed25519 = 0x4813b4c6 is the
// well-known TON value and anchors the whole CRC path.
func TestConstructorIDsGolden(t *testing.T) {
	golden := map[string]uint32{
		"candidateId":             0xb691cd3f,
		"candidateParent":         0x1a4b9af1,
		"candidateWithoutParents": 0x22cbcca9,
		"hashDataOrdinary":        0xe8f9bcdc,
		"hashDataEmpty":           0x72b4d933,
		"dataToSign":              0xa8e33df8,
		"notarizeVote":            0xcdf605a8,
		"finalizeVote":            0x40a7e105,
		"skipVote":                0x2f6b1f26,
		"vote":                    0xc37ef4f3,
		"voteSignature":           0xa264a978,
		"voteSignatureSet":        0x365ee1f8,
		"certificate":             0xbe7b573a,
		"db.key.vote":             0x65bc6ce2,
		"db.ourVote":              0x84733714,
		"db.cert":                 0xaf37a688,
		"db.key.poolState":        0x5ff2984b,
		"db.poolState":            0x97a6a6d6,
		"pub.ed25519":             0x4813b4c6,

		"stats.id":                0xc8aee82e,
		"stats.block":             0x3af82fee,
		"stats.empty":             0xc6599e1f,
		"stats.candidateReceived": 0x257ba2cf,
		"stats.timestampedEvent":  0x4d601011,
		"simplex.stats.voted":     0x07725e97,
		"simplex.stats.cert":      0x71a32c35,
	}
	got := map[string]uint32{
		// Types owned by tonutils-go; the scheme strings here must stay in
		// sync with its registrations (byte compatibility is separately
		// proven by the *MatchesTonutils tests).
		"candidateId":             tl.CRC("consensus.candidateId slot:int hash:int256 = consensus.CandidateId"),
		"candidateParent":         tl.CRC("consensus.candidateParent id:consensus.CandidateId = consensus.CandidateParent"),
		"candidateWithoutParents": tl.CRC("consensus.candidateWithoutParents = consensus.CandidateParent"),
		"hashDataOrdinary":        tl.CRC("consensus.candidateHashDataOrdinary block:tonNode.blockIdExt collated_file_hash:int256 parent:consensus.CandidateParent = consensus.CandidateHashData"),
		"hashDataEmpty":           tl.CRC("consensus.candidateHashDataEmpty block:tonNode.blockIdExt parent:consensus.candidateId = consensus.CandidateHashData"),
		"dataToSign":              tl.CRC("consensus.dataToSign session_id:int256 data:bytes = consensus.DataToSign"),
		"pub.ed25519":             tl.CRC("pub.ed25519 key:int256 = PublicKey"),
		// Types registered by this package.
		"notarizeVote":     tl.CRC(schemeNotarizeVote),
		"finalizeVote":     tl.CRC(schemeFinalizeVote),
		"skipVote":         tl.CRC(schemeSkipVote),
		"vote":             idSignedVote,
		"voteSignature":    tl.CRC(schemeVoteSignature),
		"voteSignatureSet": tl.CRC(schemeVoteSignatureSet),
		"certificate":      idCertificate,
		"db.key.vote":      tl.CRC(schemeDbKeyVote),
		"db.ourVote":       idDbOurVote,
		"db.cert":          idDbCert,
		"db.key.poolState": tl.CRC(schemeDbKeyPoolState),
		"db.poolState":     tl.CRC(schemeDbPoolState),
		// Trace events.
		"stats.id":                tl.CRC(schemeStatsID),
		"stats.block":             tl.CRC(schemeStatsCandidateBlock),
		"stats.empty":             tl.CRC(schemeStatsCandidateEmpty),
		"stats.candidateReceived": tl.CRC(schemeStatsCandidateReceived),
		"stats.timestampedEvent":  idStatsTimestampedEvent,
		"simplex.stats.voted":     tl.CRC(schemeSimplexStatsVoted),
		"simplex.stats.cert":      tl.CRC(schemeSimplexStatsCertObserved),
	}
	for name, want := range golden {
		if got[name] != want {
			t.Errorf("%s: got 0x%08x, want 0x%08x", name, got[name], want)
		}
	}
}

// TestWireGolden pins the exact wire bytes of every message and journal
// record so no refactoring of the TL layer can silently change the layout
// shared with every other node. The vectors were verified by hand against the
// ton_api.tl layout (LE constructor ids, int32/int64 LE, raw int256, TL
// bytes with padding, bare vectors of boxed items).
func TestWireGolden(t *testing.T) {
	id := candID(7, 0xab)
	sv := &SignedVote{ValidatorIndex: 2, Vote: NotarizeVote(id), Signature: []byte{1, 2, 3, 4}}
	cert := &Certificate{Vote: SkipVote(9), Signatures: []VoteSignature{
		{ValidatorIndex: 0, Signature: []byte{0xaa, 0xbb}},
		{ValidatorIndex: 3, Signature: []byte{0xcc}},
	}}
	for name, kv := range map[string][2]string{
		"vote": {hexOf(sv.Serialize()),
			"f3f47ec3a805f6cd3fcd91b607000000abababababababababababababababababababababababababababababababab0401020304000000"},
		"certificate": {hexOf(cert.Serialize()),
			"3a577bbe261f6b2f09000000f8e15e360200000078a964a20000000002aabb0078a964a20300000001cc0000"},
		"db.ourVote": {hexOf(EncodeOurVoteRecord(FinalizeVote(id), 5)),
			"1437738405e1a7403fcd91b607000000abababababababababababababababababababababababababababababababab0500000000000000"},
		"db.cert": {hexOf(EncodeCertificateRecord(cert)),
			"88a637af3a577bbe261f6b2f09000000f8e15e360200000078a964a20000000002aabb0078a964a20300000001cc0000"},
		"db.poolState": {hexOf(EncodePoolStateRecord(3)), "d6a6a69703000000"},
		"db.key.vote": {hexOf(VoteRecordKey(id.Hash)),
			"e26cbc65abababababababababababababababababababababababababababababababab"},
		"db.key.poolState": {hexOf(PoolStateKey()), "4b98f25f"},
	} {
		if kv[0] != kv[1] {
			t.Errorf("%s wire changed:\n got %s\nwant %s", name, kv[0], kv[1])
		}
	}
}

func hexOf(b []byte) string { return hex.EncodeToString(b) }

func candID(slot uint32, fill byte) CandidateID {
	id := CandidateID{Slot: slot}
	for i := range id.Hash {
		id.Hash[i] = fill
	}
	return id
}

// TestFinalizeVoteBytesMatchTonutils cross-checks our hand-rolled codec
// against the independently written tonutils-go TL registrations used by the
// existing simplex proof verification.
func TestFinalizeVoteBytesMatchTonutils(t *testing.T) {
	id := candID(7, 0xab)

	ref, err := tl.Serialize(ton.ConsensusSimplexFinalizeVote{
		ID: ton.ConsensusCandidateID{Slot: int32(id.Slot), Hash: id.Hash[:]},
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	if got := VoteBytes(FinalizeVote(id)); !bytes.Equal(got, ref) {
		t.Fatalf("finalize vote bytes mismatch:\n got %x\nwant %x", got, ref)
	}
}

func TestDataToSignMatchesTonutils(t *testing.T) {
	var session [32]byte
	for i := range session {
		session[i] = byte(i)
	}
	for _, payloadLen := range []int{0, 1, 2, 3, 4, 61, 253, 254, 255, 300, 1024} {
		payload := bytes.Repeat([]byte{0x5a}, payloadLen)
		ref, err := tl.Serialize(ton.ConsensusDataToSign{SessionID: session[:], Data: payload}, true)
		if err != nil {
			t.Fatal(err)
		}
		if got := DataToSign(session, payload); !bytes.Equal(got, ref) {
			t.Fatalf("dataToSign mismatch at len %d:\n got %x\nwant %x", payloadLen, got, ref)
		}
	}
}

func TestCandidateHashDataMatchesTonutils(t *testing.T) {
	refBlk := ton.BlockIDExt{Workchain: -1, Shard: -0x8000000000000000, SeqNo: 12345,
		RootHash: bytes.Repeat([]byte{0x11}, 32), FileHash: bytes.Repeat([]byte{0x22}, 32)}
	var collated [32]byte
	for i := range collated {
		collated[i] = 0x33
	}
	parent := candID(3, 0x44)

	// Ordinary candidate with a parent.
	c := &Candidate{Parent: Parent(parent), Block: refBlk, CollatedFileHash: collated}
	ref, err := tl.Serialize(ton.ConsensusCandidateHashDataOrdinary{
		Block:            refBlk,
		CollatedFileHash: collated[:],
		Parent: ton.ConsensusCandidateParent{
			ID: ton.ConsensusCandidateID{Slot: int32(parent.Slot), Hash: parent.Hash[:]},
		},
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	if got := c.HashDataBytes(); !bytes.Equal(got, ref) {
		t.Fatalf("ordinary hash data mismatch:\n got %x\nwant %x", got, ref)
	}

	// Ordinary candidate without parents.
	c.Parent = Genesis()
	ref, err = tl.Serialize(ton.ConsensusCandidateHashDataOrdinary{
		Block:            refBlk,
		CollatedFileHash: collated[:],
		Parent:           ton.ConsensusCandidateWithoutParents{},
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	if got := c.HashDataBytes(); !bytes.Equal(got, ref) {
		t.Fatalf("ordinary genesis hash data mismatch:\n got %x\nwant %x", got, ref)
	}

	// Empty candidate (bare parent id).
	ec := &Candidate{Empty: true, Parent: Parent(parent), Block: refBlk}
	ref, err = tl.Serialize(ton.ConsensusCandidateHashDataEmpty{
		Block:  refBlk,
		Parent: ton.ConsensusCandidateID{Slot: int32(parent.Slot), Hash: parent.Hash[:]},
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	if got := ec.HashDataBytes(); !bytes.Equal(got, ref) {
		t.Fatalf("empty hash data mismatch:\n got %x\nwant %x", got, ref)
	}
}

func TestVoteRoundtrip(t *testing.T) {
	votes := []Vote{
		NotarizeVote(candID(0, 0x00)),
		NotarizeVote(candID(9, 0x7f)),
		FinalizeVote(candID(1<<31, 0xff)),
		SkipVote(0),
		SkipVote(4294967295),
	}
	sig := bytes.Repeat([]byte{0x0d}, 64)
	for _, v := range votes {
		sv := &SignedVote{ValidatorIndex: 1, Vote: v, Signature: sig}
		got, _, err := parseSignedVote(sv.Serialize())
		if err != nil {
			t.Fatalf("%s: %v", v, err)
		}
		if got != v {
			t.Fatalf("roundtrip mismatch: got %s want %s", got, v)
		}
	}
}

func TestSignedVoteRoundtrip(t *testing.T) {
	sig := bytes.Repeat([]byte{0xcd}, 64)
	sv := &SignedVote{ValidatorIndex: 3, Vote: NotarizeVote(candID(5, 0xee)), Signature: sig}
	wire := sv.Serialize()

	v, gotSig, err := parseSignedVote(wire)
	if err != nil {
		t.Fatal(err)
	}
	if v != sv.Vote || !bytes.Equal(gotSig, sig) {
		t.Fatal("signed vote roundtrip mismatch")
	}
	// Trailing garbage must be rejected: parsing is exact.
	if _, _, err = parseSignedVote(append(wire, 0)); err == nil {
		t.Fatal("expected trailing bytes error")
	}
}

func TestCertificateRoundtrip(t *testing.T) {
	cert := &Certificate{Vote: SkipVote(11)}
	for i := 0; i < 5; i++ {
		cert.Signatures = append(cert.Signatures, VoteSignature{
			ValidatorIndex: uint32(i * 7),
			Signature:      bytes.Repeat([]byte{byte(i)}, 64),
		})
	}
	wire := cert.Serialize()
	got, err := parseCertificate(wire, 100)
	if err != nil {
		t.Fatal(err)
	}
	if got.Vote != cert.Vote || len(got.Signatures) != len(cert.Signatures) {
		t.Fatal("certificate roundtrip mismatch")
	}
	for i := range cert.Signatures {
		if got.Signatures[i].ValidatorIndex != cert.Signatures[i].ValidatorIndex ||
			!bytes.Equal(got.Signatures[i].Signature, cert.Signatures[i].Signature) {
			t.Fatal("certificate signature mismatch")
		}
	}
	if _, err = parseCertificate(wire, 4); err == nil {
		t.Fatal("expected signature count limit error")
	}
}

func TestJournalRecordsRoundtrip(t *testing.T) {
	v := FinalizeVote(candID(21, 0x01))
	rec := EncodeOurVoteRecord(v, 42)
	dec, err := DecodeVoteJournalRecord(rec, 10)
	if err != nil {
		t.Fatal(err)
	}
	if dec.Vote == nil || *dec.Vote != v || dec.Seqno != 42 {
		t.Fatal("our vote record mismatch")
	}

	cert := &Certificate{Vote: NotarizeVote(candID(2, 0x9a)),
		Signatures: []VoteSignature{{ValidatorIndex: 1, Signature: bytes.Repeat([]byte{7}, 64)}}}
	dec, err = DecodeVoteJournalRecord(EncodeCertificateRecord(cert), 10)
	if err != nil {
		t.Fatal(err)
	}
	if dec.Cert == nil || dec.Cert.Vote != cert.Vote {
		t.Fatal("certificate record mismatch")
	}

	if _, err = DecodePoolStateRecord(EncodePoolStateRecord(7)); err != nil {
		t.Fatal(err)
	}
	w, err := DecodePoolStateRecord(EncodePoolStateRecord(7))
	if err != nil || w != 7 {
		t.Fatalf("pool state record mismatch: %d %v", w, err)
	}
}
