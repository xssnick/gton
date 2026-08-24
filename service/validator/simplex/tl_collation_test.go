package simplex

import (
	"bytes"
	"crypto/ed25519"
	"encoding/binary"
	"testing"

	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

// TestCollationConstructorIDsGolden pins the delegation protocol ids.
func TestCollationConstructorIDsGolden(t *testing.T) {
	golden := map[string]uint32{
		"delegationToSign":     0x1ff5c190,
		"delegation":           0x5bdd7e60,
		"pleaseCollatePrepare": 0x841acb8b,
		"pleaseCollate":        0x686bdc2a,
		"candidateData.block":  0x3c36f9d5,
		"candidateData.empty":  0xf117f8ff,
		"candidate":            0x7173bf31,
		"broadcastExtraLegacy": 0x921297fa,
		"broadcastExtra":       0xf01eaecf,
	}
	got := map[string]uint32{
		"delegationToSign":     tl.CRC(schemeDelegationToSign),
		"delegation":           tl.CRC(schemeDelegation),
		"pleaseCollatePrepare": tl.CRC(schemePleaseCollatePrepare),
		"pleaseCollate":        tl.CRC(schemePleaseCollate),
		"candidateData.block":  tl.CRC(schemeCandidateBlock),
		"candidateData.empty":  tl.CRC(schemeCandidateEmpty),
		"candidate":            idCandidateWrapped,
		"broadcastExtraLegacy": idBroadcastExtraLegacy,
		"broadcastExtra":       idBroadcastExtra,
	}
	for name, want := range golden {
		if got[name] != want {
			t.Fatalf("%s id = %#08x, want %#08x", name, got[name], want)
		}
	}
}

func testDelegation(t *testing.T) *Delegation {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	return &Delegation{CollatorKey: pub, Signature: ed25519.Sign(priv, []byte("window"))}
}

func requireSameDelegation(t *testing.T, got, want *Delegation) {
	t.Helper()
	if got == nil {
		t.Fatal("delegation lost")
	}
	if !bytes.Equal(got.CollatorKey, want.CollatorKey) || !bytes.Equal(got.Signature, want.Signature) {
		t.Fatal("delegation mismatch after roundtrip")
	}
}

func TestBroadcastExtraRoundtrip(t *testing.T) {
	// The current reference emits the explicitly pinned legacy constructor for
	// a non-delegated validator candidate.
	plain := &BroadcastExtra{Slot: 7}
	data, err := plain.Serialize()
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 8 {
		t.Fatalf("plain extra is %d bytes, want 8", len(data))
	}
	if got := binary.LittleEndian.Uint32(data[:4]); got != idBroadcastExtraLegacy {
		t.Fatalf("plain constructor = %#x, want %#x", got, idBroadcastExtraLegacy)
	}
	back, err := ParseBroadcastExtra(data)
	if err != nil {
		t.Fatal(err)
	}
	if back.Slot != 7 || back.Delegation != nil {
		t.Fatalf("plain v3 roundtrip: %+v", back)
	}

	// The flags form with a delegation.
	deleg := testDelegation(t)
	full := &BroadcastExtra{Slot: 21, Delegation: deleg}
	if data, err = full.Serialize(); err != nil {
		t.Fatal(err)
	}
	if back, err = ParseBroadcastExtra(data); err != nil {
		t.Fatal(err)
	}
	if back.Slot != 21 {
		t.Fatalf("slot = %d", back.Slot)
	}
	requireSameDelegation(t, back.Delegation, deleg)

	// The flags constructor without a delegation is accepted as well, even
	// though broadcasters do not currently emit it.
	flagsPlain := make([]byte, 12)
	binary.LittleEndian.PutUint32(flagsPlain[:4], idBroadcastExtra)
	binary.LittleEndian.PutUint32(flagsPlain[8:], 34)
	if back, err = ParseBroadcastExtra(flagsPlain); err != nil {
		t.Fatal(err)
	}
	if back.Slot != 34 || back.Delegation != nil {
		t.Fatalf("plain flags roundtrip: %+v", back)
	}

	// Malformed inputs are rejected.
	for name, bad := range map[string][]byte{
		"short":        data[:6],
		"trailing":     append(append([]byte{}, data...), 0),
		"unknown ctor": append([]byte{1, 2, 3, 4}, data[4:]...),
	} {
		if _, err = ParseBroadcastExtra(bad); err == nil {
			t.Fatalf("%s input accepted", name)
		}
	}
}

func TestCandidateWrappedRoundtrip(t *testing.T) {
	blockData := ConsensusBlockData{
		Slot:      3,
		Parent:    ton.ConsensusCandidateWithoutParents{},
		Candidate: []byte("payload"),
		Signature: bytes.Repeat([]byte{0x5a}, 64),
	}

	// Without a delegation the encoding is a bare consensus.block.
	plain := &ConsensusCandidateWrapped{Data: blockData}
	data, err := plain.Serialize()
	if err != nil {
		t.Fatal(err)
	}
	wantBare, err := tl.Serialize(blockData, true)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, wantBare) {
		t.Fatal("non-delegated candidate is not bare")
	}
	back, err := ParseCandidateWrapped(data)
	if err != nil {
		t.Fatal(err)
	}
	if back.Delegation != nil {
		t.Fatal("unexpected delegation")
	}
	gotBlock, ok := back.Data.(ConsensusBlockData)
	if !ok {
		t.Fatalf("data type %T", back.Data)
	}
	if gotBlock.Slot != 3 || !bytes.Equal(gotBlock.Candidate, blockData.Candidate) {
		t.Fatalf("block data mismatch: %+v", gotBlock)
	}
	bare, err := ParseCandidateData(wantBare)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := bare.(ConsensusBlockData); !ok || got.Slot != blockData.Slot {
		t.Fatalf("bare candidate data = %T %+v", bare, bare)
	}
	if _, err = ParseCandidateData(append(bytes.Clone(wantBare), 0)); err == nil {
		t.Fatal("bare candidate data with trailing bytes was accepted")
	}

	// With a delegation around an empty candidate payload.
	deleg := testDelegation(t)
	emptyData := ConsensusEmptyData{
		Slot:   4,
		Parent: ton.ConsensusCandidateID{Slot: 3, Hash: bytes.Repeat([]byte{0x11}, 32)},
		Block: ton.BlockIDExt{
			Workchain: 0, Shard: -0x8000000000000000, SeqNo: 5,
			RootHash: bytes.Repeat([]byte{0x22}, 32), FileHash: bytes.Repeat([]byte{0x33}, 32),
		},
		Signature: bytes.Repeat([]byte{0x44}, 64),
	}
	full := &ConsensusCandidateWrapped{Data: emptyData, Delegation: deleg}
	if data, err = full.Serialize(); err != nil {
		t.Fatal(err)
	}
	if back, err = ParseCandidateWrapped(data); err != nil {
		t.Fatal(err)
	}
	gotEmpty, ok := back.Data.(ConsensusEmptyData)
	if !ok {
		t.Fatalf("data type %T", back.Data)
	}
	if gotEmpty.Slot != 4 || gotEmpty.Block.SeqNo != 5 {
		t.Fatalf("empty data mismatch: %+v", gotEmpty)
	}
	requireSameDelegation(t, back.Delegation, deleg)
	if _, err = ParseCandidateData(data); err == nil {
		t.Fatal("wrapped candidate was accepted as bare broadcast data")
	}

	// A foreign inner object is rejected even with a valid TL shape.
	foreign, err := tl.Serialize(ConsensusPleaseCollate{WindowStartSlot: 1, Signature: []byte("sig")}, true)
	if err != nil {
		t.Fatal(err)
	}
	bad := append([]byte{}, data[:8]...)
	bad[4], bad[5], bad[6], bad[7] = 0, 0, 0, 0 // flags: no delegation
	if _, err = ParseCandidateWrapped(append(bad, foreign...)); err == nil {
		t.Fatal("foreign inner object accepted")
	}
}

func TestPleaseCollateAndDelegationRoundtrip(t *testing.T) {
	delegation := ConsensusDelegationToSign{WindowStartSlot: 12, CollatorID: bytes.Repeat([]byte{0x77}, 32)}
	data, err := tl.Serialize(delegation, true)
	if err != nil {
		t.Fatal(err)
	}
	var backDelegation ConsensusDelegationToSign
	if _, err = tl.Parse(&backDelegation, data, true); err != nil {
		t.Fatal(err)
	}
	if backDelegation.WindowStartSlot != 12 || !bytes.Equal(backDelegation.CollatorID, delegation.CollatorID) {
		t.Fatalf("delegation mismatch: %+v", backDelegation)
	}

	prepare := ConsensusPleaseCollatePrepare{WindowStartSlot: 12}
	if data, err = tl.Serialize(prepare, true); err != nil {
		t.Fatal(err)
	}
	var backPrepare ConsensusPleaseCollatePrepare
	if _, err = tl.Parse(&backPrepare, data, true); err != nil {
		t.Fatal(err)
	}
	if backPrepare.WindowStartSlot != 12 {
		t.Fatalf("pleaseCollatePrepare mismatch: %+v", backPrepare)
	}

	please := ConsensusPleaseCollate{WindowStartSlot: 12, Signature: bytes.Repeat([]byte{0x88}, 64)}
	if data, err = tl.Serialize(please, true); err != nil {
		t.Fatal(err)
	}
	var backPlease ConsensusPleaseCollate
	if _, err = tl.Parse(&backPlease, data, true); err != nil {
		t.Fatal(err)
	}
	if backPlease.WindowStartSlot != 12 || !bytes.Equal(backPlease.Signature, please.Signature) {
		t.Fatalf("pleaseCollate mismatch: %+v", backPlease)
	}
}
