package network

import (
	"bytes"
	"crypto/ed25519"
	"encoding/binary"
	"testing"

	"github.com/xssnick/gton/service/validator/simplex"
)

func TestDelegationControlCodecsGoldenAndExact(t *testing.T) {
	prepare := simplex.ConsensusPleaseCollatePrepare{WindowStartSlot: 12}
	prepareWire, err := EncodePleaseCollatePrepare(prepare)
	if err != nil {
		t.Fatal(err)
	}
	if len(prepareWire) != 8 || binary.LittleEndian.Uint32(prepareWire) != 0x841acb8b {
		t.Fatalf("pleaseCollatePrepare wire = %x", prepareWire)
	}
	decodedPrepare, err := DecodePleaseCollatePrepare(prepareWire)
	if err != nil || decodedPrepare != prepare {
		t.Fatalf("pleaseCollatePrepare decode = %#v, %v", decodedPrepare, err)
	}
	if _, err = DecodePleaseCollatePrepare(append(bytes.Clone(prepareWire), 0)); err == nil {
		t.Fatal("pleaseCollatePrepare trailing byte was accepted")
	}

	commit := simplex.ConsensusPleaseCollate{
		WindowStartSlot: 12,
		Signature:       bytes.Repeat([]byte{0x88}, ed25519.SignatureSize),
	}
	commitWire, err := EncodePleaseCollate(commit)
	if err != nil {
		t.Fatal(err)
	}
	if binary.LittleEndian.Uint32(commitWire) != 0x686bdc2a {
		t.Fatalf("pleaseCollate constructor = %#08x", binary.LittleEndian.Uint32(commitWire))
	}
	decodedCommit, err := DecodePleaseCollate(commitWire)
	if err != nil || decodedCommit.WindowStartSlot != commit.WindowStartSlot ||
		!bytes.Equal(decodedCommit.Signature, commit.Signature) {
		t.Fatalf("pleaseCollate decode = %#v, %v", decodedCommit, err)
	}
	commit.Signature = commit.Signature[:ed25519.SignatureSize-1]
	if _, err = EncodePleaseCollate(commit); err == nil {
		t.Fatal("short delegation signature was accepted")
	}
}
