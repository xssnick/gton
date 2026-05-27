package p2p

import (
	"bytes"
	"context"
	"errors"
	"testing"

	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestNotifyCompressedBlockStateReadyClosesAndReplacesChannel(t *testing.T) {
	node := &Node{}

	first := node.compressedBlockStateReadyNotify()
	node.NotifyCompressedBlockStateReady()

	select {
	case <-first:
	default:
		t.Fatal("state-ready notification did not close the previous channel")
	}

	second := node.compressedBlockStateReadyNotify()
	if second == first {
		t.Fatal("state-ready notification channel was not replaced")
	}

	select {
	case <-second:
		t.Fatal("new state-ready notification channel should be open")
	default:
	}

	node.NotifyCompressedBlockStateReady()
	select {
	case <-second:
	default:
		t.Fatal("second state-ready notification was not delivered")
	}
}

func TestValidateMasterchainBroadcastSignaturesUsesCompressedV2ProofAndSignatures(t *testing.T) {
	node := newTestNode(t)
	verifier := &testMasterchainSignatureVerifier{}
	node.SetMasterchainBroadcastSignatureVerifier(verifier)

	block := testBlockID(-1, topShard, 300)
	proof := []byte{0x01, 0x02, 0x03}
	msg := tonnodeapi.BlockBroadcastCompressedV2{
		ID: block,
		SignatureSet: tonnodeapi.SignatureSetOrdinary{
			CatchainSeqno:    7,
			ValidatorSetHash: 11,
			Signatures:       []tonnodeapi.BlockSignature{},
		},
		Proof:          proof,
		DataCompressed: []byte{0xAA},
	}

	if err := node.validateMasterchainBroadcastSignatures(context.Background(), block, msg); err != nil {
		t.Fatalf("validate masterchain broadcast signatures: %v", err)
	}
	if verifier.calls != 1 {
		t.Fatalf("verifier calls = %d, want 1", verifier.calls)
	}
	if !verifier.block.Equals(&block) {
		t.Fatalf("verifier block = %+v, want %+v", verifier.block, block)
	}
	if !bytes.Equal(verifier.proof, proof) {
		t.Fatalf("verifier proof = %x, want %x", verifier.proof, proof)
	}
	if verifier.signatures == nil {
		t.Fatal("verifier received nil signatures")
	}
}

func TestValidateMasterchainBroadcastSignaturesPropagatesVerifierError(t *testing.T) {
	node := newTestNode(t)
	want := errors.New("bad signatures")
	node.SetMasterchainBroadcastSignatureVerifier(&testMasterchainSignatureVerifier{err: want})

	block := testBlockID(-1, topShard, 301)
	msg := tonnodeapi.BlockBroadcastCompressedV2{
		ID:           block,
		SignatureSet: tonnodeapi.SignatureSetOrdinary{},
		Proof:        []byte{0x01},
	}

	err := node.validateMasterchainBroadcastSignatures(context.Background(), block, msg)
	if !errors.Is(err, want) {
		t.Fatalf("validate error = %v, want %v", err, want)
	}
}

type testMasterchainSignatureVerifier struct {
	err        error
	calls      int
	block      ton.BlockIDExt
	proof      []byte
	signatures *cell.Cell
}

func (v *testMasterchainSignatureVerifier) ValidateMasterchainBroadcastSignatures(ctx context.Context, block ton.BlockIDExt, proof []byte, signatures *cell.Cell) error {
	v.calls++
	v.block = block
	v.proof = append([]byte(nil), proof...)
	v.signatures = signatures
	return v.err
}
