package collator

import (
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
	"github.com/xssnick/gton/service/validator/simplex"
)

// The successor state a validator checks is applied on top of the predecessor,
// and where that predecessor comes from decides whether the node needs to hold
// the shard at all. With full collated data it comes from the candidate's own
// proof, so a node that has never applied this shard can still validate it —
// and, the other half of the same property, cannot paper over a proof narrower
// than the reference validator demands by reading its local tree instead.
func TestPreparedValidationCandidateBuildsTheSuccessorOnTheProvenPredecessor(t *testing.T) {
	req := benchMainnetCollatedRequest(t, 1)
	candidate, err := testBuilder().BuildShard(t.Context(), req)
	if err != nil {
		t.Fatalf("collate mainnet candidate: %v", err)
	}
	assertBenchMainnetCollated(t, candidate)

	// Nothing usable is left in the resident predecessor: every subtree is
	// pruned away and only the root hash survives, which is all the collated
	// binding checks compare.
	narrowed := req.Previous
	narrowed.State = narrowedStateRoot(t, req.Previous.State)

	request := ValidationRequest{
		Session:      ActivatedSession{Session: Session{Shard: req.Shard}},
		Previous:     []PreviousBlock{narrowed},
		BlockBOC:     candidate.BlockBOC,
		CollatedData: candidate.CollatedData,
	}
	artifact := CandidateArtifact{
		Candidate: simplex.Candidate{
			Block:            candidate.ID,
			CollatedFileHash: candidate.CollatedFileHash,
		},
		BlockBOC:     candidate.BlockBOC,
		CollatedData: candidate.CollatedData,
	}

	prepared, err := prepareValidationCandidate(
		t.Context(),
		artifact,
		req.CreatedBy,
		request.Session.Shard.IsMasterchain(),
		request.Previous,
	)
	if err != nil {
		t.Fatalf("restore candidate state without a resident predecessor: %v", err)
	}
	if !prepared.verified.collated.full {
		t.Fatal("fixture candidate does not carry full collated data")
	}
	if prepared.stateRoot != nil || prepared.candidate.State != nil {
		t.Fatal("preparation applied the state update before config binding")
	}
	if err = prepared.bindConfig(t.Context(), req.Masterchain.Config); err != nil {
		t.Fatalf("bind candidate config without a resident predecessor: %v", err)
	}
	if prepared.stateRoot.HashKeyAt(0) != candidate.State.HashKeyAt(0) {
		t.Fatal("state restored from the proof differs from the collated one")
	}
	if prepared.previous[0].State.HashKeyAt(0) != req.Previous.State.HashKeyAt(0) {
		t.Fatal("proven predecessor is not the block the candidate follows")
	}
	if prepared.previous[0].State == narrowed.State {
		t.Fatal("the narrowed resident predecessor was used after all")
	}
	// The resident list survives the substitution: it is the only handle left
	// on the trees this node actually holds.
	if len(prepared.resident) != 1 || prepared.resident[0].State != narrowed.State {
		t.Fatal("capsule lost the resident predecessor it was handed")
	}
	if prepared.verified.block.BlockInfo.SeqNo != candidate.ID.SeqNo {
		t.Fatalf("restored block seqno = %d, want %d", prepared.verified.block.BlockInfo.SeqNo, candidate.ID.SeqNo)
	}
	// One parse, and it is the parse the rest of the call uses: the capsule's
	// root is the candidate's own block cell, not a second decode of the BOC.
	if prepared.root == nil || prepared.root.HashKeyAt(0) != cellHashKeyOf(t, candidate.ID.RootHash) {
		t.Fatal("capsule does not carry the parsed candidate root")
	}
	if prepared.candidate.State != prepared.stateRoot ||
		prepared.candidate.StateUpdate.HashKeyAt(0) != candidate.StateUpdate.HashKeyAt(0) {
		t.Fatal("capsule candidate does not carry the successor it built")
	}

	// The control: with the capability clear there is no proof to stand in for
	// the resident tree, and the same narrowed predecessor must fail. Without
	// it this test would pass even if the substitution never happened.
	plainReq := benchMainnetCollatedRequest(t, 1)
	plainReq.Masterchain.Config.capabilities &^= capFullCollatedData
	plain, err := testBuilder().BuildShard(t.Context(), plainReq)
	if err != nil {
		t.Fatalf("collate candidate without full collated data: %v", err)
	}
	control := request
	control.Previous = []PreviousBlock{{
		ID:           plainReq.Previous.ID,
		Block:        plainReq.Previous.Block,
		State:        narrowedStateRoot(t, plainReq.Previous.State),
		OutQueueSize: plainReq.Previous.OutQueueSize,
	}}
	control.BlockBOC, control.CollatedData = plain.BlockBOC, plain.CollatedData
	controlArtifact := CandidateArtifact{
		Candidate: simplex.Candidate{
			Block:            plain.ID,
			CollatedFileHash: plain.CollatedFileHash,
		},
		BlockBOC:     plain.BlockBOC,
		CollatedData: plain.CollatedData,
	}
	controlPrepared, err := prepareValidationCandidate(
		t.Context(),
		controlArtifact,
		plainReq.CreatedBy,
		control.Session.Shard.IsMasterchain(),
		control.Previous,
	)
	if err != nil {
		t.Fatalf("prepare narrowed control before config binding: %v", err)
	}
	if err = controlPrepared.bindConfig(t.Context(), plainReq.Masterchain.Config); err == nil {
		t.Fatal("a narrowed predecessor restored a state without any proof to replace it")
	}
}

func cellHashKeyOf(tb testing.TB, hash []byte) cell.Hash {
	tb.Helper()

	var key cell.Hash
	if len(hash) != len(key) {
		tb.Fatalf("block root hash is %d bytes", len(hash))
	}
	copy(key[:], hash)

	return key
}

// Every message delivered inside the block without ever being queued costs the
// validator two outbound-queue lookups it must be able to answer. Collation
// never goes near those paths — the message was not queued — so they reach the
// proof only through the closure replay. This walks the proof the candidate
// actually carries and performs the validator's own lookups against it: a
// pruned branch here is the rejection a validator would produce.
func TestCollatedProofAnswersImmediateMessageQueueLookups(t *testing.T) {
	req := benchMainnetCollatedRequest(t, 1)
	candidate, err := testBuilder().BuildShard(t.Context(), req)
	if err != nil {
		t.Fatalf("collate mainnet candidate: %v", err)
	}
	if candidate.Stats.ImmediateDelivered == 0 {
		t.Fatal("fixture delivered no message immediately")
	}

	collated, err := verifyCollatedData(candidate, nil, req.Header.GenUtime)
	if err != nil {
		t.Fatalf("decode collated data: %v", err)
	}
	provenPrevious, err := collatedStateRoot(&collated, req.Previous.ID)
	if err != nil {
		t.Fatalf("load proven predecessor state: %v", err)
	}
	var previousState tlb.ShardStateUnsplit
	if err = parseProofExact(&previousState, provenPrevious); err != nil {
		t.Fatalf("decode proven predecessor state: %v", err)
	}
	var previousQueue tlb.OutMsgQueueInfo
	if err = parseProofExact(&previousQueue, previousState.OutMsgQueueInfo); err != nil {
		t.Fatalf("decode proven predecessor queue: %v", err)
	}

	checked := 0
	for _, key := range immediateQueueKeysOf(t, candidate, req.Masterchain.Config.globalVersion) {
		keyCell := cell.BeginCell().MustStoreSlice(key[:], 352).EndCell()
		if _, err = previousQueue.OutQueue.LoadValue(keyCell); err != nil && !isMissingKey(err) {
			t.Fatalf("collated proof cannot answer the queue lookup for %x: %v", key, err)
		}
		checked++
	}
	if checked == 0 {
		t.Fatal("no msg_export_imm descriptor found, so nothing was proven")
	}
}

// immediateQueueKeysOf rebuilds, from the block alone, the keys the validator
// derives for msg_export_imm — the same envelope route it reads, so a mismatch
// between what collation records and what validation asks for shows up here.
func immediateQueueKeysOf(tb testing.TB, candidate *Candidate, version uint32) []msgpool.QueueKey {
	tb.Helper()

	_, outMessages := candidateMessageDescriptors(tb, candidate, version)
	var keys []msgpool.QueueKey
	if _, err := outMessages.CheckForEachExtra(func(value, _ *cell.Slice, _ *cell.Cell) (bool, error) {
		tag, err := value.LoadUInt(3)
		if err != nil {
			return false, err
		}
		if tag != 0b010 { // msg_export_imm$010
			return true, nil
		}
		envelopeCell, err := value.LoadRefCell()
		if err != nil {
			return false, err
		}
		envelope, err := parseSemanticEnvelope(envelopeCell)
		if err != nil {
			return false, err
		}
		keys = append(keys, msgpool.MakeQueueKey(envelope.next, envelope.message.HashKey()))

		return true, nil
	}, false); err != nil {
		tb.Fatalf("walk outbound descriptors: %v", err)
	}

	return keys
}
