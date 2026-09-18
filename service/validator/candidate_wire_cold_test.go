package validator

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/xssnick/gton/service/validator/simplex"
)

type coldWireVariant struct {
	name     string
	artifact *CandidateArtifact
}

type coldWireInvalidCase struct {
	name   string
	mutate func(*lazyCandidateWire)
	want   string
}

func TestCandidateWireColdMatchesHotForSupportedProtocols(t *testing.T) {
	for protocol := uint8(0); protocol <= simplex.MaxProtocolVersion; protocol++ {
		t.Run(fmt.Sprintf("protocol_%d", protocol), func(t *testing.T) {
			t.Parallel()

			config, key := runtimeTestConfig(0x76, &runtimeTestJournal{})
			config.Protocol.ProtocolVersion = protocol
			codec, err := newCandidateCodec(config, CandidateLimits{
				MaxBlockBytes:        1 << 20,
				MaxCollatedDataBytes: 1 << 20,
			})
			if err != nil {
				t.Fatal(err)
			}
			ordinary := runtimeOrdinaryArtifact(t, config, key, 0, simplex.Genesis())
			delegated := runtimeDelegatedArtifact(t, config, key, ordinary)

			for _, test := range []coldWireVariant{
				{name: "validator", artifact: ordinary},
				{name: "delegated", artifact: delegated},
			} {
				t.Run(test.name, func(t *testing.T) {
					canonical, err := simplex.SerializeCandidate(
						test.artifact.Candidate, test.artifact.BlockBOC, test.artifact.CollatedData,
					)
					if err != nil {
						t.Fatal(err)
					}
					for _, state := range []string{"hot", "cold"} {
						t.Run(state, func(t *testing.T) {
							artifact, lazy, err := codec.decodeDeferred(canonical, &test.artifact.Candidate.ID)
							if err != nil {
								t.Fatal(err)
							}
							if lazy.roots.Load() == nil {
								t.Fatal("received candidate has no parsed cache")
							}
							if state == "cold" {
								lazy.releaseRoots()
								if lazy.roots.Load() != nil || lazy.wire != nil {
									t.Fatal("cooling must release the roots without building the wire")
								}
								if artifact.validationRoots == nil || artifact.validationRoots.block == nil {
									t.Fatal("cooling the wire invalidated an independent validation handoff")
								}
							}

							wire, hash, err := lazy.materialize()
							if err != nil {
								t.Fatal(err)
							}
							if !bytes.Equal(wire, canonical) || hash != sha256.Sum256(canonical) {
								t.Fatal("materialization changed canonical wire bytes or their identity")
							}
							if lazy.roots.Load() != nil {
								t.Fatal("materialized wire retained parsed roots")
							}
							lazy.releaseRoots()
							again, againHash, err := lazy.materialize()
							if err != nil || againHash != hash || &again[0] != &wire[0] {
								t.Fatal("materialization did not reuse the cached wire and hash")
							}
						})
					}
				})
			}
		})
	}
}

func TestCandidateWireColdRejectsInvalidPayload(t *testing.T) {
	t.Parallel()

	for _, test := range []coldWireInvalidCase{
		{
			name:   "block BOC",
			mutate: func(w *lazyCandidateWire) { w.blockBOC = []byte{0} },
			want:   "restore candidate block roots",
		},
		{
			name:   "collated BOC",
			mutate: func(w *lazyCandidateWire) { w.collatedData = []byte{0} },
			want:   "restore candidate collated roots",
		},
		{
			name: "block identity",
			mutate: func(w *lazyCandidateWire) {
				w.candidate.Block.RootHash = bytes.Repeat([]byte{0xee}, 32)
				w.candidate.ID = w.candidate.ComputeID(w.candidate.ID.Slot)
			},
			want: "block root hash",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			lazy, _ := newColdCandidateWireForTest(t)
			lazy.releaseRoots()
			test.mutate(lazy)

			wire, hash, err := lazy.materialize()
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
			if wire != nil || hash != [32]byte{} || lazy.roots.Load() != nil {
				t.Fatal("failed build published a wire, hash or parsed roots")
			}
			_, _, repeatedErr := lazy.materialize()
			if repeatedErr != err {
				t.Fatal("failed materialization was retried")
			}
		})
	}
}

func TestCandidateWireConcurrentCoolingAndMaterialization(t *testing.T) {
	t.Parallel()

	for range 32 {
		lazy, canonical := newColdCandidateWireForTest(t)
		expectedHash := sha256.Sum256(canonical)
		start := make(chan struct{})
		var readers sync.WaitGroup
		for range 16 {
			readers.Go(func() {
				<-start
				lazy.releaseRoots()
			})
			readers.Go(func() {
				<-start
				wire, hash, err := lazy.materialize()
				if err != nil || hash != expectedHash || !bytes.Equal(wire, canonical) {
					t.Errorf("concurrent materialization changed the wire: %v", err)
				}
			})
		}
		close(start)
		readers.Wait()
		if lazy.roots.Load() != nil {
			t.Fatal("concurrent cooling retained parsed roots")
		}
	}
}

func TestCandidateWireCoolingDoesNotWaitForMaterialization(t *testing.T) {
	t.Parallel()

	lazy, release, done := blockingLazyWireForTest([]byte{1, 2, 3})
	lazy.releaseRoots()
	close(release)
	<-done
}

func newColdCandidateWireForTest(t testing.TB) (*lazyCandidateWire, []byte) {
	t.Helper()

	config, key := runtimeTestConfig(0x77, &runtimeTestJournal{})
	codec, err := newCandidateCodec(config, CandidateLimits{
		MaxBlockBytes:        1 << 20,
		MaxCollatedDataBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	artifact := runtimeOrdinaryArtifact(t, config, key, 0, simplex.Genesis())
	canonical, err := simplex.SerializeCandidate(artifact.Candidate, artifact.BlockBOC, artifact.CollatedData)
	if err != nil {
		t.Fatal(err)
	}
	_, lazy, err := codec.decodeDeferred(canonical, &artifact.Candidate.ID)
	if err != nil {
		t.Fatal(err)
	}

	return lazy, canonical
}
