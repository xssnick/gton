package collator

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"strings"
	"testing"

	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/msgpool"
	"github.com/xssnick/gton/service/validator/simplex"
)

// trustedPipelineHarness is a Service wired to one specific Pipeline, with the
// session and window signArtifact needs. Nothing is started: what is under test
// is the ownership decision Service makes about a produced candidate, and that
// is fixed by the Pipeline it was constructed with.
type trustedPipelineHarness struct {
	service *Service
	session Session
	window  productionWindow
	leader  [32]byte
}

func newTrustedPipelineHarness(t *testing.T, pipeline Pipeline) *trustedPipelineHarness {
	t.Helper()

	leaderPub, leaderPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	keys := newRuntimeTestKeys(t)
	service, err := NewService(ServiceOptions{
		ProductionMode:     ProductionModeSelf,
		Storage:            newRuntimeMemoryStorage(),
		Pipeline:           pipeline,
		Keys:               keys,
		CollatorKeyID:      keys.id,
		AllowAllValidators: true,
		Emit:               func(context.Context, CandidateArtifact) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}

	var leader [ed25519.PublicKeySize]byte
	copy(leader[:], leaderPub)
	var sessionID [32]byte
	sessionID[0] = 0x7a
	session := Session{
		ID:                   sessionID,
		Shard:                groups.ShardID{Workchain: 0, Shard: -1 << 63},
		CatchainSeqno:        1,
		ConsensusVersion:     2,
		ProtocolVersion:      3,
		SlotsPerLeaderWindow: 1,
		Validators: []SessionValidator{{
			PublicKey: leader,
			Weight:    1,
		}},
	}

	return &trustedPipelineHarness{
		service: service,
		session: session,
		window: productionWindow{
			ID:         WindowID{SessionID: sessionID, StartSlot: 0},
			Leader:     0,
			Authority:  CandidateAuthoritySelf,
			SelfSigner: &runtimeCountingSigner{private: leaderPriv},
		},
		leader: leader,
	}
}

// candidate returns a sealed candidate as the canonical builder would hand one
// over: its two hashes are the digests of its two buffers, and it carries both
// the digested seal and the provenance binding its exported fields.
func (h *trustedPipelineHarness) candidate(t *testing.T) *Candidate {
	t.Helper()

	built := preparedRouteCandidate(t, h.session, 0, h.leader)
	built.digested = true

	return built
}

// swapPayloads replaces both byte buffers with bytes that hash to neither of
// the candidate's declared digests, without touching anything the provenance
// covers. It is the one mutation only the re-derivation can see, so a signature
// over it is proof that no sha256 was taken.
func swapPayloads(candidate *Candidate) {
	candidate.BlockBOC = bytes.Clone(candidate.BlockBOC)
	candidate.BlockBOC[0] ^= 1
	candidate.CollatedData = bytes.Clone(candidate.CollatedData)
	candidate.CollatedData[0] ^= 1
}

func newTrustedTestPipeline(t *testing.T) *LocalAcquisition {
	t.Helper()

	pool := msgpool.New(msgpool.Config{})
	t.Cleanup(pool.Close)
	acquisition, err := NewLocalAcquisition(LocalAcquisitionOptions{
		Builder:   testBuilder(),
		Store:     &localValidationStore{},
		Groups:    &localAcquisitionTestGroups{},
		Messages:  pool,
		Semantics: testCandidateTransitionVerifier,
	})
	if err != nil {
		t.Fatal(err)
	}

	return acquisition
}

// embeddedAcquisitionPipeline is the decorator shape the concrete-type check
// exists for: it promotes every method of our own acquisition, including any
// unexported marker one, while being free to override BuildCandidate.
type embeddedAcquisitionPipeline struct {
	*LocalAcquisition
}

func TestTrustedPipelineIsThisPackagesOwnAcquisitionOnly(t *testing.T) {
	own := newTrustedTestPipeline(t)
	if !pipelineBuildsOwnCandidates(own) {
		t.Fatal("our own acquisition is not recognized as the trusted pipeline")
	}
	if pipelineBuildsOwnCandidates(&runtimeTestPipeline{}) {
		t.Fatal("a foreign pipeline was recognized as the trusted pipeline")
	}
	if pipelineBuildsOwnCandidates(&embeddedAcquisitionPipeline{LocalAcquisition: own}) {
		t.Fatal("a decorator embedding our acquisition was recognized as the trusted pipeline")
	}

	if !newTrustedPipelineHarness(t, own).service.trustedPipeline {
		t.Fatal("a service driving our own acquisition did not record a trusted pipeline")
	}
	if newTrustedPipelineHarness(t, &runtimeTestPipeline{}).service.trustedPipeline {
		t.Fatal("a service driving a foreign pipeline recorded a trusted pipeline")
	}
}

// TestTrustedPipelineCandidateIsNotCopied pins the first half of the trade: our
// own acquisition hands over the Candidate finish() allocated and retains
// nothing, so the defensive copy of both payloads protects against nobody.
func TestTrustedPipelineCandidateIsNotCopied(t *testing.T) {
	harness := newTrustedPipelineHarness(t, newTrustedTestPipeline(t))
	built := harness.candidate(t)

	owned := harness.service.ownBuiltCandidate(built)
	if owned != built {
		t.Fatal("a candidate produced by our own pipeline was copied before signing")
	}
	if &owned.BlockBOC[0] != &built.BlockBOC[0] || &owned.CollatedData[0] != &built.CollatedData[0] {
		t.Fatal("a candidate produced by our own pipeline had its payload buffers copied")
	}
}

// TestTrustedPipelineSignsWithoutRederivingDigests pins the second half: the
// two sha256 over the megabyte buffers are not taken. The candidate is signed
// with payloads that hash to neither declared digest, which is exactly the
// corruption the skipped check used to catch and nothing else can see.
func TestTrustedPipelineSignsWithoutRederivingDigests(t *testing.T) {
	harness := newTrustedPipelineHarness(t, newTrustedTestPipeline(t))
	built := harness.candidate(t)
	swapPayloads(built)

	artifact, err := harness.service.signArtifact(
		harness.session, harness.window, 0, simplex.Genesis(), built,
	)
	if err != nil {
		t.Fatalf("a sealed candidate from our own pipeline re-derived its digests: %v", err)
	}
	if !artifact.Digested() {
		t.Fatal("a signed artifact from the trusted path does not carry its digest provenance")
	}
	if &artifact.BlockBOC[0] != &built.BlockBOC[0] {
		t.Fatal("the trusted path copied the block payload into the artifact")
	}
}

// TestForeignPipelineCandidateKeepsTheOwnershipDefense is the same candidate
// through a Pipeline this package did not write: both the copy and both digests
// must still happen.
func TestForeignPipelineCandidateKeepsTheOwnershipDefense(t *testing.T) {
	harness := newTrustedPipelineHarness(t, &runtimeTestPipeline{})
	built := harness.candidate(t)

	owned := harness.service.ownBuiltCandidate(built)
	if owned == built {
		t.Fatal("a candidate from a foreign pipeline was retained by pointer")
	}
	if &owned.BlockBOC[0] == &built.BlockBOC[0] || &owned.CollatedData[0] == &built.CollatedData[0] {
		t.Fatal("a candidate from a foreign pipeline aliases the pipeline's payload buffers")
	}

	swapPayloads(built)
	_, err := harness.service.signArtifact(harness.session, harness.window, 0, simplex.Genesis(), built)
	if err == nil || !strings.Contains(err.Error(), "file hash does not match block BOC") {
		t.Fatalf("foreign candidate with a swapped block payload = %v, want a digest rejection", err)
	}

	collatedOnly := harness.candidate(t)
	collatedOnly.CollatedData = bytes.Clone(collatedOnly.CollatedData)
	collatedOnly.CollatedData[0] ^= 1
	_, err = harness.service.signArtifact(
		harness.session, harness.window, 0, simplex.Genesis(), collatedOnly,
	)
	if err == nil || !strings.Contains(err.Error(), "collated file hash does not match collated data") {
		t.Fatalf("foreign candidate with swapped collated data = %v, want a digest rejection", err)
	}
}

// TestUnsealedCandidateIsDigestedOnTheTrustedPipeline closes the trusted path
// against a candidate that never made the statement the path relies on. The
// pipeline is ours; the candidate is not something finish() sealed, so nothing
// says its hashes were taken from its buffers and they are taken here.
func TestUnsealedCandidateIsDigestedOnTheTrustedPipeline(t *testing.T) {
	harness := newTrustedPipelineHarness(t, newTrustedTestPipeline(t))

	unsealed := map[string]func(*Candidate){
		"without the digest seal": func(candidate *Candidate) { candidate.digested = false },
		"without provenance":      func(candidate *Candidate) { candidate.provenance = nil },
	}
	for name, strip := range unsealed {
		built := harness.candidate(t)
		strip(built)
		swapPayloads(built)
		if _, err := harness.service.signArtifact(
			harness.session, harness.window, 0, simplex.Genesis(), built,
		); err == nil {
			t.Fatalf("a candidate %s was signed without re-deriving its digests", name)
		}

		owned := harness.service.ownBuiltCandidate(harness.candidate(t))
		strip(owned)
		if harness.service.trustsBuiltCandidate(owned) {
			t.Fatalf("a candidate %s is treated as trusted", name)
		}
	}
}
