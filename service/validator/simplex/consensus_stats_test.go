package simplex

import (
	"context"
	"crypto/ed25519"
	"testing"
	"time"
)

// The engine aggregated every certificate into one counter at the single point
// where the kind was still in hand, so a session settling slot after slot with
// skip certificates — no blocks, no progress — was indistinguishable from a
// healthy one that finalized the same number of times. It also kept its own
// consensus position entirely to itself.
func TestStatsSplitCertificateKindsAndPublishPosition(t *testing.T) {
	env := newTestEnv(t, withLocal(1))
	env.start()

	// Two slots skipped by a network quorum, then one finalized.
	for slot := uint32(0); slot < 2; slot++ {
		env.eng.HandleMessage(peer(2), 2, env.buildCert(SkipVote(slot), 0, 2, 3).Serialize())
	}
	id := candID(2, 0x41)
	env.eng.HandleMessage(peer(2), 2, env.buildCert(FinalizeVote(id), 0, 2, 3).Serialize())

	stats := env.eng.Stats()
	if got := stats.CertificatesByKind[VoteSkip]; got != 2 {
		t.Fatalf("skip certificates = %d, want 2", got)
	}
	if got := stats.CertificatesByKind[VoteFinalize]; got != 1 {
		t.Fatalf("finalize certificates = %d, want 1", got)
	}
	if got := stats.CertificatesStored; got != 3 {
		t.Fatalf("stored certificates = %d, want the same 3 the split accounts for", got)
	}
	if got := stats.SignaturesByKind[VoteSkip]; got != 6 {
		t.Fatalf("skip certificate signatures = %d, want 6 (two quorums of three)", got)
	}

	if !stats.HasFinalized || stats.FinalizedSlot != 2 {
		t.Fatalf("finalized slot = %d (known=%v), want 2", stats.FinalizedSlot, stats.HasFinalized)
	}
	if stats.Slot < 3 {
		t.Fatalf("present slot = %d, want past the finalized one", stats.Slot)
	}
	if stats.LastFinalizedAt.IsZero() {
		t.Fatal("no finalization timestamp published; its age is the whole liveness signal")
	}
	if stats.FirstBlockTimeout <= 0 {
		t.Fatal("the voter's live first-block timeout is not published")
	}

	// Participation: validator 1 is the local index and never signed here, and
	// the difference between a signing and a silent voter is what the roster
	// column in the log could only show one certificate at a time.
	if len(stats.LastSignedSlot) != testValidatorCount {
		t.Fatalf("per-validator participation = %d entries, want %d",
			len(stats.LastSignedSlot), testValidatorCount)
	}
	for _, index := range []int{0, 2, 3} {
		if stats.LastSignedSlot[index] != 2 {
			t.Fatalf("validator %d last signed slot = %d, want 2", index, stats.LastSignedSlot[index])
		}
	}
	if stats.LastSignedSlot[1] != 0 {
		t.Fatalf("a validator that signed nothing reports slot %d", stats.LastSignedSlot[1])
	}

	// The snapshot must be detached: it outlives the loop turn it was taken on.
	stats.LastSignedSlot[0] = 999
	if env.eng.Stats().LastSignedSlot[0] != 2 {
		t.Fatal("Stats aliased the engine's participation slice")
	}
	env.requireNoFatal()
}

// A metrics scrape must never enter the consensus loop, so the runner
// republishes the engine's own snapshot at the end of every turn. This drives
// the real loop on the real clock, which is the only configuration a scrape
// ever sees.
func TestRunnerStatsSnapshotIsServedWithoutTheLoop(t *testing.T) {
	vals, keys := testValidators(4)
	var session [32]byte
	eng, err := NewEngine(Config{
		SessionID:            session,
		ProtocolVersion:      3,
		Validators:           vals,
		LocalIndex:           0,
		SlotsPerLeaderWindow: 4,
		Journal:              NewMemoryJournal(4),
		Transport:            &fakeTransport{},
		Hooks:                acceptingHooks{},
		Signer:               newTestSigner(keys[0]),
	})
	if err != nil {
		t.Fatal(err)
	}

	runner := NewRunner(eng)
	if got := runner.StatsSnapshot(); got.Slot != 0 || got.CertificatesStored != 0 {
		t.Fatalf("snapshot before the loop started = %+v, want the zero value", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	defer func() {
		cancel()
		<-done
	}()

	skip := &Certificate{Vote: SkipVote(0)}
	for _, i := range []int{1, 2, 3} {
		skip.Signatures = append(skip.Signatures, VoteSignature{
			ValidatorIndex: uint32(i),
			Signature:      ed25519.Sign(keys[i], DataToSign(session, VoteBytes(skip.Vote))),
		})
	}
	runner.HandleMessage(peer(2), 2, skip.Serialize())

	deadline := time.After(3 * time.Second)
	for {
		snapshot := runner.StatsSnapshot()
		if snapshot.CertificatesByKind[VoteSkip] == 1 && snapshot.Slot == 1 {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("snapshot did not publish the skip certificate: %+v", snapshot)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// publishStats runs at the end of every consensus turn, so what it allocates is
// paid per turn for the life of the session. The published snapshot itself has
// to be allocated — a scrape may be copying the previous one — but the
// per-validator participation slice does not: it changes only when a
// certificate is stored, and a roster-sized copy on turns that did not touch it
// is a mainnet-sized allocation for an unchanged value.
//
// This also pins the safety property that makes the reuse legal: the slice a
// snapshot was handed keeps its values after later turns republish.
func TestPublishStatsReusesTheParticipationSliceUntilItChanges(t *testing.T) {
	vals, keys := testValidators(4)
	var session [32]byte
	eng, err := NewEngine(Config{
		SessionID:            session,
		ProtocolVersion:      3,
		Validators:           vals,
		LocalIndex:           0,
		SlotsPerLeaderWindow: 4,
		Journal:              NewMemoryJournal(4),
		Transport:            &fakeTransport{},
		Hooks:                acceptingHooks{},
		Signer:               newTestSigner(keys[0]),
	})
	if err != nil {
		t.Fatal(err)
	}
	runner := NewRunner(eng)

	eng.stats.LastSignedSlot = make([]uint32, testValidatorCount)
	eng.stats.LastSignedSlot[2] = 11
	runner.publishStats()
	held := runner.StatsSnapshot()

	// One Stats per turn and nothing roster-sized on top of it.
	if allocs := testing.AllocsPerRun(200, runner.publishStats); allocs > 1 {
		t.Fatalf("publishStats allocates %v objects per turn, want at most the published Stats", allocs)
	}

	// A change is copied, and the snapshot taken before it is untouched by the
	// copy — that is what lets the unchanged turns share the slice at all.
	eng.stats.LastSignedSlot[2] = 12
	runner.publishStats()
	if got := runner.StatsSnapshot().LastSignedSlot[2]; got != 12 {
		t.Fatalf("republished participation slot = %d, want the new 12", got)
	}
	if got := held.LastSignedSlot[2]; got != 11 {
		t.Fatalf("the snapshot taken earlier now reads %d, want the 11 it was handed", got)
	}
}
