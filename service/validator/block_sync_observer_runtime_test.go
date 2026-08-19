package validator

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"testing"
	"time"

	"github.com/xssnick/gton/service/validator/simplex"
)

const blockSyncObserverTestTimeout = 2 * time.Second

func blockSyncObserverTestConfig(
	t testing.TB,
	id byte,
) (SessionConfig, ed25519.PrivateKey) {
	t.Helper()

	config, privateKey := runtimeTestConfig(id, &runtimeTestJournal{})
	config.Protocol.ProtocolVersion = 1
	config.StorageID.Protocol = config.Protocol
	config.CandidateLimits = CandidateLimits{
		MaxBlockBytes:        1 << 20,
		MaxCollatedDataBytes: 1 << 20,
	}

	return config, privateKey
}

func waitBlockSyncObserverResult[T any](t testing.TB, result <-chan T) T {
	t.Helper()

	select {
	case value := <-result:
		return value
	case <-time.After(blockSyncObserverTestTimeout):
		t.Fatal("timed out waiting for block-sync observer runtime")

		var zero T
		return zero
	}
}

func TestPrepareBlockSyncObserverRuntimeRejectsWrongRoleAndProtocol(t *testing.T) {
	config, _ := blockSyncObserverTestConfig(t, 0xb1)
	network := newRuntimeTestNetwork()

	tests := []struct {
		name   string
		mutate func(*SessionConfig)
	}{
		{
			name: "protocol zero",
			mutate: func(config *SessionConfig) {
				config.Protocol.ProtocolVersion = 0
			},
		},
		{
			name: "validator identity",
			mutate: func(config *SessionConfig) {
				config.Identity.Validator = &ValidatorIdentity{}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			invalid := config
			test.mutate(&invalid)
			if _, err := PrepareBlockSyncObserverRuntime(
				context.Background(),
				invalid,
				network,
				invalid.CandidateLimits,
			); err == nil {
				t.Fatal("invalid block-sync observer runtime was prepared")
			}
		})
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := PrepareBlockSyncObserverRuntime(
		canceled,
		config,
		network,
		config.CandidateLimits,
	); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled preparation error = %v, want context cancellation", err)
	}
}

func TestBlockSyncObserverRuntimeLifecycle(t *testing.T) {
	t.Run("readiness follows network start and close joins run", func(t *testing.T) {
		config, _ := blockSyncObserverTestConfig(t, 0xb2)
		network := newRuntimeTestNetwork()
		runtime, err := PrepareBlockSyncObserverRuntime(
			context.Background(),
			config,
			network,
			config.CandidateLimits,
		)
		if err != nil {
			t.Fatal(err)
		}
		if err = runtime.Recover(context.Background(), SessionStart{}); err != nil {
			t.Fatal(err)
		}

		startup := make(chan error, 1)
		returned := make(chan error, 1)
		go func() {
			returned <- runtime.runWithStartup(context.Background(), SessionStart{}, startup)
		}()
		if err = waitBlockSyncObserverResult(t, startup); err != nil {
			t.Fatal(err)
		}

		network.mu.Lock()
		receiver := network.receiver
		network.mu.Unlock()
		if receiver != runtime {
			t.Fatal("network readiness was reported before the observer receiver was bound")
		}
		if err = runtime.Update(context.Background(), runtimeTestState()); err != nil {
			t.Fatal(err)
		}
		if err = runtime.Recover(context.Background(), SessionStart{}); !errors.Is(err, ErrSessionRuntimeStarted) {
			t.Fatalf("active recovery error = %v, want ErrSessionRuntimeStarted", err)
		}

		if err = runtime.Close(); err != nil {
			t.Fatal(err)
		}
		if err = waitBlockSyncObserverResult(t, returned); err != nil {
			t.Fatalf("run after close = %v, want nil", err)
		}
		if err = runtime.Close(); err != nil {
			t.Fatal(err)
		}
		if err = runtime.Retire(); err != nil {
			t.Fatal(err)
		}
		if err = runtime.Run(context.Background(), SessionStart{}); !errors.Is(err, ErrSessionRuntimeClosed) {
			t.Fatalf("run after close error = %v, want ErrSessionRuntimeClosed", err)
		}
	})

	t.Run("startup and terminal failures propagate", func(t *testing.T) {
		config, _ := blockSyncObserverTestConfig(t, 0xb3)
		startErr := errors.New("start failed")
		startNetwork := newRuntimeTestNetwork()
		startNetwork.startErr = startErr
		startRuntime, err := PrepareBlockSyncObserverRuntime(
			context.Background(),
			config,
			startNetwork,
			config.CandidateLimits,
		)
		if err != nil {
			t.Fatal(err)
		}
		startup := make(chan error, 1)
		returned := make(chan error, 1)
		go func() {
			returned <- startRuntime.runWithStartup(context.Background(), SessionStart{}, startup)
		}()
		if err = waitBlockSyncObserverResult(t, startup); !errors.Is(err, startErr) {
			t.Fatalf("startup error = %v, want %v", err, startErr)
		}
		if err = waitBlockSyncObserverResult(t, returned); !errors.Is(err, startErr) {
			t.Fatalf("run error = %v, want %v", err, startErr)
		}
		if err = startRuntime.Close(); err != nil {
			t.Fatal(err)
		}

		failNetwork := newRuntimeTestNetwork()
		failRuntime, err := PrepareBlockSyncObserverRuntime(
			context.Background(),
			config,
			failNetwork,
			config.CandidateLimits,
		)
		if err != nil {
			t.Fatal(err)
		}
		startup = make(chan error, 1)
		returned = make(chan error, 1)
		go func() {
			returned <- failRuntime.runWithStartup(context.Background(), SessionStart{}, startup)
		}()
		if err = waitBlockSyncObserverResult(t, startup); err != nil {
			t.Fatal(err)
		}
		fatalErr := errors.New("observer failed")
		failRuntime.fail(fatalErr)
		if err = waitBlockSyncObserverResult(t, returned); !errors.Is(err, fatalErr) {
			t.Fatalf("terminal run error = %v, want %v", err, fatalErr)
		}
		if err = failRuntime.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestBlockSyncObserverRuntimeAuthenticatesCandidateWithoutPublishing(t *testing.T) {
	config, privateKey := blockSyncObserverTestConfig(t, 0xb4)
	network := newRuntimeTestNetwork()
	runtime, err := PrepareBlockSyncObserverRuntime(
		context.Background(),
		config,
		network,
		config.CandidateLimits,
	)
	if err != nil {
		t.Fatal(err)
	}
	startup := make(chan error, 1)
	returned := make(chan error, 1)
	go func() {
		returned <- runtime.runWithStartup(context.Background(), SessionStart{}, startup)
	}()
	if err = waitBlockSyncObserverResult(t, startup); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := runtime.Close(); closeErr != nil {
			t.Error(closeErr)
		}
		if runErr := waitBlockSyncObserverResult(t, returned); runErr != nil {
			t.Errorf("run after cleanup = %v", runErr)
		}
	})

	artifact := runtimeOrdinaryArtifact(t, config, privateKey, 0, simplex.Genesis())
	wire, err := simplex.SerializeCandidate(
		artifact.Candidate,
		artifact.BlockBOC,
		artifact.CollatedData,
	)
	if err != nil {
		t.Fatal(err)
	}
	received, err := runtime.ReceiveCandidate(context.Background(), artifact.Candidate.ID.Slot, wire)
	if err != nil {
		t.Fatal(err)
	}
	assertCandidateArtifactEqual(t, &received, artifact)
	if _, err = runtime.ReceiveCandidate(context.Background(), artifact.Candidate.ID.Slot+1, wire); err == nil {
		t.Fatal("candidate with a mismatched broadcast slot was accepted")
	}

	forged := *artifact
	forged.Candidate = artifact.Candidate
	forged.Candidate.Signature = bytes.Clone(artifact.Candidate.Signature)
	forged.Candidate.Signature[0] ^= 0xff
	forgedWire, err := simplex.SerializeCandidate(
		forged.Candidate,
		forged.BlockBOC,
		forged.CollatedData,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = runtime.ReceiveCandidate(context.Background(), artifact.Candidate.ID.Slot, forgedWire); err == nil {
		t.Fatal("forged candidate was accepted")
	}
	if err = runtime.PrecheckCandidateBroadcast(^uint32(0), [32]byte{0xff}, false); err != nil {
		t.Fatalf("transport-authorized candidate failed observer precheck: %v", err)
	}
	if _, err = runtime.ServeCandidate(
		context.Background(),
		simplex.PeerID{},
		CandidateRequest{},
	); !errors.Is(err, ErrCandidateUnavailable) {
		t.Fatalf("serve candidate error = %v, want ErrCandidateUnavailable", err)
	}
	if err = runtime.publishCandidate(context.Background(), artifact); !errors.Is(
		err,
		errBlockSyncObserverPublicationUnsupported,
	) {
		t.Fatalf("publish candidate error = %v, want unsupported", err)
	}
	if broadcasts := network.candidateBroadcasts(); len(broadcasts) != 0 {
		t.Fatalf("observer emitted %d candidate broadcasts", len(broadcasts))
	}
}
