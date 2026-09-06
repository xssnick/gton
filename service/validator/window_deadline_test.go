package validator

import (
	"context"
	"testing"
	"time"

	"github.com/xssnick/gton/service/validator/simplex"
)

type observedWindowDeadline struct {
	window   simplex.Window
	deadline time.Time
}

func TestSessionRuntimePreservesAdaptiveWindowDeadline(t *testing.T) {
	storage := newRuntimeTestStorage()
	network := newRuntimeTestNetwork()
	backend := newRuntimeTestBackend()
	observed := make(chan observedWindowDeadline, 4)
	backend.window = func(ctx context.Context, window LeaderWindow) error {
		deadline, _ := ctx.Deadline()
		select {
		case observed <- observedWindowDeadline{window: window.Window, deadline: deadline}:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	config, key := runtimeTestConfig(0x31, &runtimeTestJournal{})
	config.Protocol.SlotsPerLeaderWindow = 1
	config.StorageID.Protocol = config.Protocol
	keyID := config.Validators[0].PublicKeyHash
	config.Identity = SessionIdentity{ADNLID: keyID, Validator: &ValidatorIdentity{
		Index: 0, KeyID: keyID, Signer: runtimeTestSigner{key: key},
	}}
	config.OverlayMembers = [][32]byte{keyID}
	config.StorageID.IsValidator = true
	config.StorageID.ValidatorKeyID = keyID
	config.StorageID.LocalADNLID = keyID
	config.StorageID.ValidatorIndex = 0
	state := runtimeTestState()
	state.Params.TargetRate = 10 * time.Millisecond
	state.Params.FirstBlockTimeout = 100 * time.Millisecond
	state.Params.FirstBlockTimeoutMultiplier = 10
	state.Params.FirstBlockTimeoutCap = time.Second
	runtime, err := prepareSessionRuntime(context.Background(), config, state, RuntimeOptions{
		Storage: storage, Network: network, Backend: backend,
		Limits: CandidateLimits{MaxBlockBytes: 1 << 20, MaxCollatedDataBytes: 1 << 20},
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- runtime.Run(context.Background(), runtimeTestStart()) }()
	defer func() { runtime.Close(); <-done }()
	timeout := time.After(2 * time.Second)
	for {
		select {
		case got := <-observed:
			if got.window.StartSlot != 1 {
				continue
			}
			want := got.window.ObservedAt.Add(time.Second + state.Params.TargetRate)
			t.Logf("window=%d adaptive voter timeout=%s production lifetime=%s committee lifetime=%s",
				got.window.StartSlot, got.window.FirstBlockTimeout,
				got.deadline.Sub(got.window.ObservedAt), want.Sub(got.window.ObservedAt))
			if !got.deadline.Equal(want) {
				t.Fatalf("producer deadline = %s, want %s", got.deadline, want)
			}
			return
		case <-timeout:
			t.Fatal("no second window")
		}
	}
}
