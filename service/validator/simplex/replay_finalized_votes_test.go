package simplex

import (
	"bytes"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

type payloadRecordingSigner struct {
	signer   Signer
	payloads [][]byte
}

func (s *payloadRecordingSigner) Sign(data []byte) ([]byte, error) {
	s.payloads = append(s.payloads, append([]byte(nil), data...))

	return s.signer.Sign(data)
}

// TestRestartSkipsOwnVotesOfFinalizedSlots: the bootstrap replay casts only own
// votes of slots at or above the restored finality horizon, as the reference
// does through slot_at. Votes of finalized slots are neither traced, nor signed,
// nor reported as dropped; a vote above the horizon is still re-applied.
func TestRestartSkipsOwnVotesOfFinalizedSlots(t *testing.T) {
	env := newTestEnv(t, withLocal(1))

	id0 := candID(0, 0x41)
	id2 := candID(2, 0x42)
	id3 := candID(3, 0x43)
	finalized := []Vote{NotarizeVote(id0), FinalizeVote(id0), SkipVote(1)}
	for _, v := range append(finalized, NotarizeVote(id3)) {
		if err := env.journal.saveOurVote(v); err != nil {
			t.Fatal(err)
		}
	}
	if err := env.journal.saveCertificate(env.buildCert(FinalizeVote(id2), 0, 2, 3)); err != nil {
		t.Fatal(err)
	}

	tr := &recTracer{}
	signer := &payloadRecordingSigner{signer: newTestSigner(env.keys[1])}
	var logs bytes.Buffer
	logger := zerolog.New(&logs)
	eng, err := NewEngine(Config{
		SessionID: env.session, ProtocolVersion: 3, Validators: env.vals, LocalIndex: 1,
		SlotsPerLeaderWindow: env.spw, Params: &env.params,
		Journal: env.journal, Transport: env.trans, Clock: env.clock, Hooks: env.hooks,
		Signer: signer, Tracer: tr, Logger: &logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	env.eng = eng
	env.start()
	env.requireNoFatal()

	requireEqual(t, eng.slots.firstNonFinalized, uint32(3), "finality horizon restored")
	requireEqual(t, eng.slots.at(3).votes[1].notarize.set, true, "vote above the horizon re-applied")

	for _, ev := range tr.events {
		if voted, ok := ev.(TraceVoted); ok && voted.Vote.Slot() < 3 {
			t.Fatalf("finalized slot vote traced: %s", voted.Vote)
		}
	}
	for _, v := range finalized {
		payload := DataToSign(env.session, VoteBytes(v))
		for _, signed := range signer.payloads {
			if bytes.Equal(signed, payload) {
				t.Fatalf("finalized slot vote signed: %s", v)
			}
		}
	}
	if strings.Contains(logs.String(), "references a finalized slot") {
		t.Fatalf("finalized slot vote reported as dropped: %s", logs.String())
	}
}
