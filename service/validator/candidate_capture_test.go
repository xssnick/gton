package validator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/xssnick/gton/service/shard"
	"github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func captureTestParent(t testing.TB) *ChainState {
	t.Helper()

	parent := chainStateBlock(shard.Root, 10, 0xA5)
	request := ChainStateRequest{
		Shard:  groups.ShardID{Workchain: 0, Shard: shard.Root},
		Blocks: []ton.BlockIDExt{parent},
	}
	state, err := newChainState(request, chainStateData(parent))
	if err != nil {
		t.Fatalf("build capture test parent: %v", err)
	}

	return state
}

// captureCountingObserver counts AddCapturedFailedCandidate, inheriting every
// other ValidationObserver method as a no-op.
type captureCountingObserver struct {
	selfRejectionObserver
	captured map[collator.MetricChain]int
}

func (o *captureCountingObserver) AddCapturedFailedCandidate(chain collator.MetricChain) {
	if o.captured == nil {
		o.captured = make(map[collator.MetricChain]int, 2)
	}
	o.captured[chain]++
}

func captureTestArtifact(tag uint64) (*CandidateArtifact, *cell.Cell) {
	blockRoot := cell.BeginCell().MustStoreUInt(tag, 8).EndCell()
	blockBOC := blockRoot.ToBOC()
	collated := cell.BeginCell().MustStoreUInt(0xC0, 8).EndCell().ToBOC()

	return &CandidateArtifact{BlockBOC: blockBOC, CollatedData: collated}, blockRoot
}

func captureTestCandidate(slot uint32, block *cell.Cell) *simplex.Candidate {
	candidate := &simplex.Candidate{Leader: 4}
	candidate.ID.Slot = slot
	candidate.ID.Hash = sha256.Sum256([]byte(fmt.Sprintf("candidate-%d", slot)))
	candidate.Block.Workchain = 0
	candidate.Block.Shard = -0x8000000000000000
	candidate.Block.SeqNo = slot + 1
	candidate.Block.RootHash = block.Hash()
	fileHash := sha256.Sum256(block.ToBOC())
	candidate.Block.FileHash = fileHash[:]

	return candidate
}

// TestIsCapturableValidationFailure pins the predicate that gates capture: it
// must fire on a semantic/TVM replay disagreement and stay silent for benign
// abstains and local plumbing faults.
func TestIsCapturableValidationFailure(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"semantic replay failure", fmt.Errorf("%w: TVM execution failed: failed to set value in dict: invalid dictionary fork node", collator.ErrSemanticExecution), true},
		{"semantic invalid verdict", fmt.Errorf("%w: bad candidate", ErrCandidateRejected), true},
		{"invalid input mapped to rejected", fmt.Errorf("%w: %w", ErrCandidateRejected, collator.ErrInvalidInput), true},
		{"not ready", fmt.Errorf("%w: retry later", ErrBlockNotReady), false},
		{"context canceled", context.Canceled, false},
		{"deadline", context.DeadlineExceeded, false},
		{"local plumbing fault", errors.New("validator local backend: predecessor carries no parsed block"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := isCapturableValidationFailure(tc.err); got != tc.want {
				t.Fatalf("isCapturableValidationFailure(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// TestFailedCandidateCaptureWritesAndRoundTrips writes one dump for a semantic
// replay failure and verifies its bytes and metadata are sufficient to begin a
// deterministic offline replay: the two BOCs read back byte-for-byte and decode,
// and the metadata names the candidate, its predecessors and the error.
func TestFailedCandidateCaptureWritesAndRoundTrips(t *testing.T) {
	dir := t.TempDir()
	observer := &captureCountingObserver{}
	logger := zerolog.Nop()
	capturer := newFailedCandidateCapturer(dir, observer, &logger).withIdentity("cafe", "beef")

	artifact, block := captureTestArtifact(0x71)
	candidate := captureTestCandidate(9, block)
	parent := captureTestParent(t)
	reason := fmt.Errorf("%w: TVM execution failed: failed to set value in dict: invalid dictionary fork node", collator.ErrSemanticExecution)

	capturer.capture(candidate, artifact, parent, collator.MetricChainShardchain, reason)

	if observer.captured[collator.MetricChainShardchain] != 1 {
		t.Fatalf("captured metric = %d, want 1", observer.captured[collator.MetricChainShardchain])
	}

	dumps := capturer.listDumpDirs()
	if len(dumps) != 1 {
		t.Fatalf("dump dirs = %d, want 1", len(dumps))
	}
	dumpDir := filepath.Join(dir, dumps[0])

	// The two BOCs must round-trip byte-for-byte and decode as cells: this is the
	// entry point of the deterministic replay.
	gotBlock, err := os.ReadFile(filepath.Join(dumpDir, "block.boc"))
	if err != nil {
		t.Fatal(err)
	}
	if string(gotBlock) != string(artifact.BlockBOC) {
		t.Fatal("block.boc does not round-trip the candidate wire")
	}
	decoded, err := cell.FromBOC(gotBlock)
	if err != nil {
		t.Fatalf("captured block.boc does not decode: %v", err)
	}
	if !bytes.Equal(decoded.Hash(), block.Hash()) {
		t.Fatal("captured block decodes to a different cell than the candidate block")
	}
	gotCollated, err := os.ReadFile(filepath.Join(dumpDir, "collated.boc"))
	if err != nil {
		t.Fatal(err)
	}
	if string(gotCollated) != string(artifact.CollatedData) {
		t.Fatal("collated.boc does not round-trip the candidate collated data")
	}

	metaBytes, err := os.ReadFile(filepath.Join(dumpDir, "meta.json"))
	if err != nil {
		t.Fatal(err)
	}
	var meta failedCandidateMeta
	if err = json.Unmarshal(metaBytes, &meta); err != nil {
		t.Fatalf("meta.json does not parse: %v", err)
	}
	if meta.Schema != failedCandidateSchema {
		t.Fatalf("schema = %q, want %q", meta.Schema, failedCandidateSchema)
	}
	if meta.Slot != 9 || meta.Leader != 4 {
		t.Fatalf("meta slot/leader = %d/%d, want 9/4", meta.Slot, meta.Leader)
	}
	if meta.SessionID != "cafe" || meta.Namespace != "beef" {
		t.Fatalf("meta identity = %q/%q, want cafe/beef", meta.SessionID, meta.Namespace)
	}
	if meta.Reason != reason.Error() {
		t.Fatalf("meta reason = %q, want %q", meta.Reason, reason.Error())
	}
	blockHash := sha256.Sum256(artifact.BlockBOC)
	if meta.BlockBOCBytes != len(artifact.BlockBOC) || meta.BlockBOCSHA256 != hexOf(blockHash[:]) {
		t.Fatal("meta block sha/size does not match the captured wire")
	}
	if len(meta.Predecessors) != 1 {
		t.Fatalf("meta predecessors = %d, want 1", len(meta.Predecessors))
	}
	if len(meta.NotCaptured) == 0 {
		t.Fatal("meta must document what is NOT captured for the replay")
	}
	if _, err = os.Stat(filepath.Join(dumpDir, "README.txt")); err != nil {
		t.Fatalf("README.txt is absent: %v", err)
	}
}

// TestFailedCandidateCaptureBoundEvicts pins the hard on-disk bound: the ninth
// dump evicts the oldest, leaving exactly maxKeptFailedCandidateDumps.
func TestFailedCandidateCaptureBoundEvicts(t *testing.T) {
	dir := t.TempDir()
	logger := zerolog.Nop()
	capturer := newFailedCandidateCapturer(dir, nil, &logger)

	base := time.Date(2026, 8, 19, 10, 0, 0, 0, time.UTC)
	clock := base
	capturer.now = func() time.Time { return clock }
	reason := fmt.Errorf("%w: TVM execution failed", collator.ErrSemanticExecution)

	var firstDir string
	for i := 0; i < maxKeptFailedCandidateDumps+1; i++ {
		// Space captures more than an hour apart so the rolling-hour cap never
		// blocks and the on-disk eviction bound is what this test measures.
		clock = base.Add(time.Duration(i) * 2 * time.Hour)
		artifact, block := captureTestArtifact(uint64(0x40 + i))
		candidate := captureTestCandidate(uint32(100+i), block)
		capturer.capture(candidate, artifact, captureTestParent(t), collator.MetricChainShardchain, reason)
		if i == 0 {
			dumps := capturer.listDumpDirs()
			if len(dumps) != 1 {
				t.Fatalf("after first capture dumps = %d, want 1", len(dumps))
			}
			firstDir = dumps[0]
		}
	}

	dumps := capturer.listDumpDirs()
	if len(dumps) != maxKeptFailedCandidateDumps {
		t.Fatalf("kept dumps = %d, want %d", len(dumps), maxKeptFailedCandidateDumps)
	}
	for _, name := range dumps {
		if name == firstDir {
			t.Fatal("the oldest dump was not evicted")
		}
	}
}

// TestFailedCandidateCaptureHourlyCap pins the rolling-hour rate limit: once the
// cap is reached no further dump is written within the hour, and capture resumes
// after the window advances.
func TestFailedCandidateCaptureHourlyCap(t *testing.T) {
	dir := t.TempDir()
	logger := zerolog.Nop()
	capturer := newFailedCandidateCapturer(dir, nil, &logger)

	base := time.Date(2026, 8, 19, 10, 0, 0, 0, time.UTC)
	clock := base
	capturer.now = func() time.Time { return clock }
	reason := fmt.Errorf("%w: TVM execution failed", collator.ErrSemanticExecution)

	capture := func(i int) {
		artifact, block := captureTestArtifact(uint64(0x40 + i))
		candidate := captureTestCandidate(uint32(200+i), block)
		capturer.capture(candidate, artifact, nil, collator.MetricChainShardchain, reason)
	}

	// Fill the hourly budget within a few seconds. maxKept >= perHour, so eviction
	// does not confound the count here.
	for i := 0; i < maxFailedCandidateDumpsPerHour; i++ {
		clock = base.Add(time.Duration(i) * time.Second)
		capture(i)
	}
	if got := len(capturer.listDumpDirs()); got != maxFailedCandidateDumpsPerHour {
		t.Fatalf("dumps after filling the budget = %d, want %d", got, maxFailedCandidateDumpsPerHour)
	}

	// One more within the same hour must be skipped.
	clock = base.Add(time.Minute)
	capture(maxFailedCandidateDumpsPerHour)
	if got := len(capturer.listDumpDirs()); got != maxFailedCandidateDumpsPerHour {
		t.Fatalf("dumps after an over-cap capture = %d, want %d (skip)", got, maxFailedCandidateDumpsPerHour)
	}

	// After the hour advances, capture resumes (the eviction bound still holds).
	clock = base.Add(2 * time.Hour)
	capture(maxFailedCandidateDumpsPerHour + 1)
	if got := len(capturer.listDumpDirs()); got != maxKeptFailedCandidateDumps {
		t.Fatalf("dumps after the window advanced = %d, want %d", got, maxKeptFailedCandidateDumps)
	}
}

// TestSessionRuntimeCapturesRefusedCandidateAtValidation pins the capture at its
// CALL SITE inside validateCandidateCore: a TVM/semantic replay error dumps the
// candidate, while a not-ready abstain does not. Deleting the one capture line
// from the validation path leaves the whole suite green except this test, which
// is the point — the capture exists to record an otherwise-ephemeral refusal.
func TestSessionRuntimeCapturesRefusedCandidateAtValidation(t *testing.T) {
	for _, tc := range []struct {
		name       string
		reason     error
		wantDumped bool
	}{
		{
			name:       "semantic replay error is captured",
			reason:     fmt.Errorf("%w: TVM execution failed: failed to set value in dict: invalid dictionary fork node", collator.ErrSemanticExecution),
			wantDumped: true,
		},
		{
			name:       "not-ready abstain is not captured",
			reason:     fmt.Errorf("%w: exact state unavailable", ErrBlockNotReady),
			wantDumped: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend := newRuntimeTestBackend()
			backend.validation = func(
				_ context.Context,
				_ *ChainState,
				_ *CandidateArtifact,
			) (CandidateValidation, error) {
				return CandidateValidation{}, tc.reason
			}
			runtime, privateKey := prepareRuntimeTest(
				t, 0x7c, newRuntimeTestStorage(), newRuntimeTestNetwork(), backend)
			defer runtime.Close()

			dir := t.TempDir()
			observer := &captureCountingObserver{}
			logger := zerolog.Nop()
			runtime.capturer = newFailedCandidateCapturer(dir, observer, &logger).withIdentity("sess", "ns")

			if err := runtime.states.start(context.Background(), runtimeTestStart()); err != nil {
				t.Fatal(err)
			}
			artifact := runtimeOrdinaryArtifact(t, runtime.config, privateKey, 0, simplex.Genesis())
			if err := runtime.candidates.stage(artifact, []byte{0x01}); err != nil {
				t.Fatal(err)
			}

			err := runtime.validateCandidate(context.Background(), &artifact.Candidate)
			if err == nil {
				t.Fatal("validation of a refused candidate returned no error")
			}

			dumped := len(capturerDumps(t, dir))
			if tc.wantDumped {
				if dumped != 1 {
					t.Fatalf("dumps written = %d, want 1", dumped)
				}
				if observer.captured[runtime.validationChain()] != 1 {
					t.Fatalf("capture metric = %d, want 1", observer.captured[runtime.validationChain()])
				}
			} else {
				if dumped != 0 {
					t.Fatalf("dumps written = %d, want 0 for a not-ready abstain", dumped)
				}
			}
		})
	}
}

func capturerDumps(t testing.TB, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatal(err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() && len(entry.Name()) > 0 && entry.Name()[0] != '.' {
			names = append(names, entry.Name())
		}
	}

	return names
}

func hexOf(b []byte) string {
	const hexdigits = "0123456789abcdef"
	out := make([]byte, len(b)*2)
	for i, v := range b {
		out[i*2] = hexdigits[v>>4]
		out[i*2+1] = hexdigits[v&0x0f]
	}

	return string(out)
}
