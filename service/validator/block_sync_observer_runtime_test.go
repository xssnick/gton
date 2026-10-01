package validator

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"testing"
	"time"

	"github.com/xssnick/gton/service/validator/groups"
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

func blockSyncObserverQuorumTestConfig(t testing.TB, id byte, count int) SessionConfig {
	t.Helper()

	config, _ := blockSyncObserverTestConfig(t, id)
	config.Validators = make([]groups.Validator, count)
	for i := range count {
		seed := bytes.Repeat([]byte{id + byte(i) + 1}, ed25519.SeedSize)
		publicKey := ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey)
		copy(config.Validators[i].PublicKey[:], publicKey)
		config.Validators[i].PublicKeyHash = simplex.KeyNodeIDShort(publicKey)
		config.Validators[i].Weight = 1
	}

	return config
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
		if err = runtime.PrecheckCandidateBroadcast(0, [32]byte{1}, false); !errors.Is(
			err,
			ErrSessionRuntimeClosed,
		) {
			t.Fatalf("precheck after close error = %v, want ErrSessionRuntimeClosed", err)
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
	broadcast, err := simplex.SerializeCandidateForBroadcast(
		artifact.Candidate,
		artifact.BlockBOC,
		artifact.CollatedData,
	)
	if err != nil {
		t.Fatal(err)
	}
	extra, err := simplex.ParseBroadcastExtra(broadcast.Extra)
	if err != nil {
		t.Fatal(err)
	}
	received, err := runtime.ReceiveCandidate(
		context.Background(),
		artifact.Candidate.ID.Slot,
		broadcast.Data,
		extra.Delegation,
	)
	if err != nil {
		t.Fatal(err)
	}
	assertCandidateArtifactEqual(t, &received, artifact)
	assertPreparedBlockRoute(t, 1, &received)
	if _, err = runtime.ReceiveCandidate(
		context.Background(),
		artifact.Candidate.ID.Slot+1,
		broadcast.Data,
		extra.Delegation,
	); err == nil {
		t.Fatal("candidate with a mismatched broadcast slot was accepted")
	}

	forged := *artifact
	forged.Candidate = artifact.Candidate
	forged.Candidate.Signature = bytes.Clone(artifact.Candidate.Signature)
	forged.Candidate.Signature[0] ^= 0xff
	forgedBroadcast, err := simplex.SerializeCandidateForBroadcast(
		forged.Candidate,
		forged.BlockBOC,
		forged.CollatedData,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = runtime.ReceiveCandidate(
		context.Background(),
		artifact.Candidate.ID.Slot,
		forgedBroadcast.Data,
		extra.Delegation,
	); err == nil {
		t.Fatal("forged candidate was accepted")
	}
	firstBroadcast := [32]byte{0x11}
	if err = runtime.PrecheckCandidateBroadcast(1, firstBroadcast, false); err != nil {
		t.Fatalf("first provisional admission: %v", err)
	}
	if err = runtime.PrecheckCandidateBroadcast(1, firstBroadcast, false); err != nil {
		t.Fatalf("idempotent unsigned precheck: %v", err)
	}
	if err = runtime.PrecheckCandidateBroadcast(1, [32]byte{0x12}, false); err != nil {
		t.Fatalf("unsigned conflict poisoned the slot before authentication: %v", err)
	}
	if err = runtime.PrecheckCandidateBroadcast(1, firstBroadcast, true); err != nil {
		t.Fatalf("authenticated phase of the admitted broadcast: %v", err)
	}
	if err = runtime.PrecheckCandidateBroadcast(1, [32]byte{0x12}, true); err == nil {
		t.Fatal("conflicting authenticated broadcast was admitted for one slot")
	}
	if err = runtime.PrecheckCandidateBroadcast(1, firstBroadcast, true); err == nil {
		t.Fatal("authenticated duplicate was admitted twice")
	}
	if err = runtime.PrecheckCandidateBroadcast(^uint32(0), [32]byte{0xff}, false); err == nil {
		t.Fatal("far-future candidate passed the quorum-anchored observer horizon")
	}

	state := runtimeTestState()
	finalized := *artifact.Candidate.Block.Copy()
	state.FinalizedBlock = &finalized
	if err = runtime.Update(context.Background(), state); err != nil {
		t.Fatal(err)
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

func TestBlockSyncObserverBootstrapRequiresQuorumProgress(t *testing.T) {
	config := blockSyncObserverQuorumTestConfig(t, 0xb5, 4)
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

	far := ^uint32(0)
	for runtime.expectedLeader(far) != 0 {
		far -= config.Protocol.SlotsPerLeaderWindow
	}
	if err = runtime.PrecheckCandidateBroadcast(far, [32]byte{0xf0}, false); err != nil {
		t.Fatalf("unsigned bootstrap outlier: %v", err)
	}
	if err = runtime.PrecheckCandidateBroadcast(far, [32]byte{0xf0}, true); err != nil {
		t.Fatalf("authenticated bootstrap outlier: %v", err)
	}
	runtime.lifecycleMu.Lock()
	anchored := runtime.clockSet
	runtime.lifecycleMu.Unlock()
	if anchored {
		t.Fatal("one authenticated leader established the observer clock")
	}

	// The first outlier must not reject the real session progress. Two more
	// independently scheduled leaders bring the observed weight to 3-of-4; the
	// weighted median is then an honest in-range slot rather than the outlier.
	for i, slot := range []uint32{
		config.Protocol.SlotsPerLeaderWindow,
		2 * config.Protocol.SlotsPerLeaderWindow,
	} {
		id := [32]byte{byte(i + 1)}
		if err = runtime.PrecheckCandidateBroadcast(slot, id, false); err != nil {
			t.Fatalf("legitimate bootstrap slot %d before signature: %v", slot, err)
		}
		if err = runtime.PrecheckCandidateBroadcast(slot, id, true); err != nil {
			t.Fatalf("legitimate bootstrap slot %d after signature: %v", slot, err)
		}
	}

	runtime.lifecycleMu.Lock()
	anchored = runtime.clockSet
	anchor := runtime.clockSlot
	_, retainedOutlier := runtime.admissions[far]
	runtime.lifecycleMu.Unlock()
	if !anchored || anchor != 2*config.Protocol.SlotsPerLeaderWindow {
		t.Fatalf("quorum clock = %d (set=%v), want weighted median %d", anchor, anchored, 2*config.Protocol.SlotsPerLeaderWindow)
	}
	if retainedOutlier {
		t.Fatal("bootstrap outlier survived quorum horizon pruning")
	}
	if err = runtime.PrecheckCandidateBroadcast(far, [32]byte{0xf1}, false); err == nil {
		t.Fatal("far-future candidate passed the quorum-authenticated horizon")
	}
}
