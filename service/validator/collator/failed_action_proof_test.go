package collator

import (
	"bytes"
	"math/big"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
	execop "github.com/xssnick/tonutils-go/tvm/op/exec"
	funcsop "github.com/xssnick/tonutils-go/tvm/op/funcs"
	stackop "github.com/xssnick/tonutils-go/tvm/op/stack"
)

// A PUSHREF inside an inline continuation can carry a predecessor subtree into
// SENDRAWMSG without either the VM load hook or the cell trace observing it.
// The action still walks the entire proposed message to compute its storage
// fee, even when the send fails, so a validator needs those cells in the
// collated proof although the message never appears in OutMessages.
func TestFullCollatedProofCoversFailedActionMessageReads(t *testing.T) {
	contract := address.NewAddress(0, 0, bytes.Repeat([]byte{0x31}, 32))
	destination := address.NewAddress(0, 0, bytes.Repeat([]byte{0x32}, 32))
	body := cell.BeginCell().MustStoreUInt(0xAB, 8).EndCell()
	for i := range 8 {
		body = cell.BeginCell().MustStoreUInt(uint64(i), 8).MustStoreRef(body).EndCell()
	}

	message, err := tlb.ToCell(&tlb.InternalMessage{
		IHRDisabled: true,
		SrcAddr:     address.NewAddressNone(),
		DstAddr:     destination,
		Amount:      tlb.FromNanoTONU(0),
		Body:        body,
	})
	if err != nil {
		t.Fatal(err)
	}

	continuation := cell.BeginCell()
	for _, op := range []*cell.Builder{
		stackop.PUSHINT(big.NewInt(1_000_000_000_000_000_000)).Serialize(),
		stackop.PUSHINT(big.NewInt(2)).Serialize(),
		funcsop.RAWRESERVE().Serialize(),
		stackop.PUSHREF(message).Serialize(),
		stackop.PUSHINT(big.NewInt(0)).Serialize(),
		funcsop.SENDRAWMSG().Serialize(),
	} {
		if err = continuation.StoreBuilder(op); err != nil {
			t.Fatal(err)
		}
	}
	code := externalAcceptCode(t).ToBuilder()
	for _, op := range []*cell.Builder{
		stackop.PUSHCONT(continuation.EndCell()).Serialize(),
		execop.JMPX().Serialize(),
	} {
		if err = code.StoreBuilder(op); err != nil {
			t.Fatal(err)
		}
	}

	req := fullCollatedContractRequest(t, contract, code.EndCell(), true)

	candidate, err := testBuilder().BuildShard(t.Context(), req)
	if err != nil {
		t.Fatalf("build failed-send candidate: %v", err)
	}
	if candidate.Stats.Transactions != 1 || candidate.Stats.ExternalIncluded != 1 ||
		candidate.Stats.NewMessages != 0 {
		t.Fatalf("unexpected failed-send stats: %+v", candidate.Stats)
	}

	verification := shardVerificationRequest(req, candidate)
	verification.NeighborShardEndLT = req.NeighborShardEndLT
	verification.Semantics = NewSemanticVerifier(tvm.NewTVM())
	verification.Neighbors = collatedNeighborQueues(t, req, candidate)
	if err = verifyShardCandidateForTest(t.Context(), verification); err != nil {
		t.Fatalf("proof-backed replay of failed send: %v", err)
	}
}

// Persistent data can lose its predecessor trace through the same inline
// continuation route as an action message. State-limit and storage-stat logic
// reads the committed c4 after the VM exits, so its reused cells must remain in
// the predecessor proof even though no opcode opened them.
func TestFullCollatedProofCoversCommittedDataFromInlineContinuation(t *testing.T) {
	contract := address.NewAddress(0, 0, bytes.Repeat([]byte{0x41}, 32))
	data := cell.BeginCell().MustStoreUInt(0xCD, 8).EndCell()
	for i := range 8 {
		data = cell.BeginCell().MustStoreUInt(uint64(i), 8).MustStoreRef(data).EndCell()
	}

	continuation := cell.BeginCell()
	for _, op := range []*cell.Builder{
		stackop.PUSHREF(data).Serialize(),
		execop.POPCTR(4).Serialize(),
	} {
		if err := continuation.StoreBuilder(op); err != nil {
			t.Fatal(err)
		}
	}
	code := externalAcceptCode(t).ToBuilder()
	for _, op := range []*cell.Builder{
		stackop.PUSHCONT(continuation.EndCell()).Serialize(),
		execop.JMPX().Serialize(),
	} {
		if err := code.StoreBuilder(op); err != nil {
			t.Fatal(err)
		}
	}

	req := fullCollatedContractRequest(t, contract, code.EndCell(), true)

	candidate, err := testBuilder().BuildShard(t.Context(), req)
	if err != nil {
		t.Fatalf("build committed-data candidate: %v", err)
	}
	if candidate.Stats.Transactions != 1 || candidate.Stats.ExternalIncluded != 1 {
		t.Fatalf("unexpected committed-data stats: %+v", candidate.Stats)
	}

	verification := shardVerificationRequest(req, candidate)
	verification.NeighborShardEndLT = req.NeighborShardEndLT
	verification.Semantics = NewSemanticVerifier(tvm.NewTVM())
	verification.Neighbors = collatedNeighborQueues(t, req, candidate)
	if err = verifyShardCandidateForTest(t.Context(), verification); err != nil {
		t.Fatalf("proof-backed replay of committed data: %v", err)
	}
}

func TestFullCollatedProofCoversCommittedDataAfterReplacingCode(t *testing.T) {
	contract := address.NewAddress(0, 0, bytes.Repeat([]byte{0x51}, 32))
	data := cell.BeginCell().MustStoreUInt(0xDE, 8).EndCell()
	for i := range 8 {
		data = cell.BeginCell().MustStoreUInt(uint64(i), 8).MustStoreRef(data).EndCell()
	}

	replacement := externalAcceptCode(t)
	continuation := cell.BeginCell()
	for _, op := range []*cell.Builder{
		stackop.PUSHREF(data).Serialize(),
		execop.POPCTR(4).Serialize(),
		stackop.PUSHREF(replacement).Serialize(),
		funcsop.SETCODE().Serialize(),
	} {
		if err := continuation.StoreBuilder(op); err != nil {
			t.Fatal(err)
		}
	}
	code := externalAcceptCode(t).ToBuilder()
	for _, op := range []*cell.Builder{
		stackop.PUSHCONT(continuation.EndCell()).Serialize(),
		execop.JMPX().Serialize(),
	} {
		if err := code.StoreBuilder(op); err != nil {
			t.Fatal(err)
		}
	}

	req := fullCollatedContractRequest(t, contract, code.EndCell(), false)
	candidate, err := testBuilder().BuildShard(t.Context(), req)
	if err != nil {
		t.Fatalf("build code-replacing candidate: %v", err)
	}
	if candidate.Stats.Transactions != 1 || candidate.Stats.ExternalIncluded != 1 {
		t.Fatalf("unexpected code-replacing stats: %+v", candidate.Stats)
	}

	verification := shardVerificationRequest(req, candidate)
	verification.NeighborShardEndLT = req.NeighborShardEndLT
	verification.Semantics = NewSemanticVerifier(tvm.NewTVM())
	verification.Neighbors = collatedNeighborQueues(t, req, candidate)
	if err = verifyShardCandidateForTest(t.Context(), verification); err != nil {
		t.Fatalf("proof-backed replay after replacing code: %v", err)
	}
}

func fullCollatedContractRequest(
	t *testing.T,
	contract *address.Address,
	code *cell.Cell,
	cacheStorageStats bool,
) ShardRequest {
	t.Helper()

	req := emptyCandidateRequest(t)
	req.Previous.State = stateWithAccounts(t, req.Previous.State, activeContracts(
		t,
		req.Header.GenUtime,
		activeContract{address: contract, code: code, balance: 100_000_000_000},
	))
	initial, err := testBuilder().BuildShard(t.Context(), req)
	if err != nil {
		t.Fatalf("build predecessor: %v", err)
	}
	blockRoot, err := cell.FromBOC(initial.BlockBOC)
	if err != nil {
		t.Fatal(err)
	}
	queueSize := initial.Stats.OutQueueSize
	req.Previous = PreviousBlock{
		ID:           initial.ID,
		Block:        blockRoot,
		State:        initial.State,
		OutQueueSize: &queueSize,
	}
	if cacheStorageStats {
		req.StorageStats = initial.StorageStats
	}
	requestTime := req.Header.GenUtime + 1
	req.Header.GenUtime = requestTime
	req.Header.GenUtimeMS = uint64(requestTime) * 1_000
	req.Masterchain.Config.capabilities |= capFullCollatedData
	attachFullCollatedTestNeighbors(t, &req)

	external, err := tlb.ToCell(&tlb.ExternalMessage{
		DstAddr: contract,
		Body:    cell.BeginCell().EndCell(),
	})
	if err != nil {
		t.Fatal(err)
	}
	req.Externals = []ExternalInput{externalInput(t, external)}
	return req
}
