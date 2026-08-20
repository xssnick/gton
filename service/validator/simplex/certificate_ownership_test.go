package simplex

import (
	"crypto/ed25519"
	"runtime"
	"slices"
	"sync"
	"testing"
)

type mutatingCertificateJournal struct {
	*MemoryJournal
}

func (j *mutatingCertificateJournal) SaveCertificate(cert *Certificate, done func(error)) {
	j.MemoryJournal.SaveCertificate(cert, func(err error) {
		if err == nil {
			cert.Vote = SkipVote(1000)
			cert.Signatures[0].ValidatorIndex = 3
			cert.Signatures[0].Signature[0] ^= 0xff
		}
		done(err)
	})
}

func TestValidatedBootstrapOwnsAndDoesNotExposeState(t *testing.T) {
	validators, keys := testValidators(4)
	sessionID := [32]byte{0xa1}
	vote := FinalizeVote(CandidateID{Slot: 7, Hash: [32]byte{0x61}})
	certificate := testCertificateFor(t, validators, keys, vote)
	state := &BootstrapState{
		FirstNonAnnouncedWindow: 8,
		OurVotes:                []Vote{NotarizeVote(CandidateID{Slot: 6, Hash: [32]byte{0x51}})},
		Certificates:            []*Certificate{certificate},
	}
	expected := cloneBootstrapState(state)

	validated, err := ValidateBootstrap(sessionID, validators, state)
	if err != nil {
		t.Fatal(err)
	}

	state.FirstNonAnnouncedWindow = 99
	state.OurVotes[0] = SkipVote(99)
	state.Certificates[0] = nil
	certificate.Vote = SkipVote(99)
	certificate.Signatures[0].ValidatorIndex = 3
	certificate.Signatures[0].Signature[0] ^= 0xff

	got := validated.State()
	if got == state || got.FirstNonAnnouncedWindow != expected.FirstNonAnnouncedWindow ||
		!slices.Equal(got.OurVotes, expected.OurVotes) ||
		len(got.Certificates) != 1 || !certificatesEqual(got.Certificates[0], expected.Certificates[0]) {
		t.Fatal("source mutation changed the validated bootstrap snapshot")
	}

	got.FirstNonAnnouncedWindow = 100
	got.OurVotes[0] = SkipVote(100)
	got.Certificates[0].Vote = SkipVote(100)
	got.Certificates[0].Signatures[0].Signature[0] ^= 0xff
	if !validated.matches(expected) {
		t.Fatal("State accessor exposed the validated bootstrap internals")
	}

	certificates := validated.Certificates()
	if len(certificates) != 1 || certificates[0].Vote() != vote {
		t.Fatalf("sealed bootstrap certificates = %+v, want %s", certificates, vote)
	}
	exposed := certificates[0].Certificate()
	exposed.Vote = SkipVote(101)
	exposed.Signatures[0].Signature[0] ^= 0xff
	certificates[0] = VerifiedCertificate{}

	again := validated.Certificates()
	if len(again) != 1 || again[0].Vote() != vote ||
		!certificatesEqual(again[0].Certificate(), expected.Certificates[0]) {
		t.Fatal("Certificates accessor exposed the validated bootstrap internals")
	}
}

func TestValidatedBootstrapSnapshotIsRaceIndependent(t *testing.T) {
	validators, keys := testValidators(4)
	sessionID := [32]byte{0xa1}
	vote := NotarizeVote(CandidateID{Slot: 14, Hash: [32]byte{0xa4}})
	certificate := testCertificateFor(t, validators, keys, vote)
	state := &BootstrapState{
		FirstNonAnnouncedWindow: 15,
		OurVotes:                []Vote{SkipVote(13)},
		Certificates:            []*Certificate{certificate},
	}

	validated, err := ValidateBootstrap(sessionID, validators, state)
	if err != nil {
		t.Fatal(err)
	}
	exposedState := validated.State()
	exposedCertificate := validated.Certificates()[0].Certificate()

	start := make(chan struct{})
	var mutations sync.WaitGroup
	mutations.Add(1)
	go func() {
		defer mutations.Done()
		<-start
		for range 10_000 {
			state.FirstNonAnnouncedWindow++
			state.OurVotes[0].ID.Slot++
			certificate.Signatures[0].Signature[0] ^= 0xff
			exposedState.OurVotes[0].ID.Slot++
			exposedCertificate.Signatures[0].Signature[0] ^= 0xff
			runtime.Gosched()
		}
	}()
	close(start)

	for range 200 {
		if validated.Certificates()[0].Vote() != vote {
			t.Fatal("validated certificate observed an external mutation")
		}
		snapshot := validated.State()
		if snapshot.FirstNonAnnouncedWindow != 15 || snapshot.OurVotes[0] != SkipVote(13) {
			t.Fatal("validated state observed an external mutation")
		}
	}
	mutations.Wait()
}

func TestJournalCannotMutateVerifiedCertificate(t *testing.T) {
	validators, keys := testValidators(4)
	sessionID := [32]byte{0xb5}
	hooks := newRecHooks(t)
	journal := &mutatingCertificateJournal{MemoryJournal: NewMemoryJournal(len(validators))}
	engine, err := NewEngine(Config{
		SessionID:            sessionID,
		Validators:           validators,
		LocalIndex:           ObserverIndex,
		SlotsPerLeaderWindow: 4,
		Journal:              journal,
		Transport:            &fakeTransport{},
		Hooks:                hooks,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = engine.Start(); err != nil {
		t.Fatal(err)
	}

	vote := NotarizeVote(CandidateID{Slot: 0, Hash: [32]byte{0xb6}})
	payload := DataToSign(sessionID, VoteBytes(vote))
	certificate := &Certificate{Vote: vote}
	for i := range 3 {
		certificate.Signatures = append(certificate.Signatures, VoteSignature{
			ValidatorIndex: uint32(i),
			Signature:      ed25519.Sign(keys[i], payload),
		})
	}
	engine.HandleMessage(peer(3), 3, certificate.Serialize())

	if err = engine.Err(); err != nil {
		t.Fatalf("journal mutation corrupted the engine-owned certificate: %v", err)
	}
	if len(hooks.notarizedSeals) != 1 || hooks.notarizedSeals[0].Vote() != vote {
		t.Fatalf("notarized seals = %+v, want immutable %s", hooks.notarizedSeals, vote)
	}
	carried := hooks.notarizedSeals[0].Certificate()
	if _, err = VerifyCertificate(sessionID, validators, carried); err != nil {
		t.Fatalf("journal mutation changed the sealed quorum: %v", err)
	}
}
