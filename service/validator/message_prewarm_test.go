package validator

import (
	"context"
	"sync"
	"testing"

	"github.com/rs/zerolog"
	"github.com/xssnick/gton/service/hooks"
	"github.com/xssnick/gton/service/validator/msgpool"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type recordedServiceAccountPrewarmer struct {
	mu       sync.Mutex
	accounts []pooledAccountPrewarmKey
	roots    []cell.Hash
}

func (w *recordedServiceAccountPrewarmer) EnqueueRoot(root cell.Hash) bool {
	w.mu.Lock()
	w.roots = append(w.roots, root)
	w.mu.Unlock()

	return true
}

func (w *recordedServiceAccountPrewarmer) EnqueueAccount(workchain int32, account [32]byte) bool {
	w.mu.Lock()
	w.accounts = append(w.accounts, pooledAccountPrewarmKey{workchain: workchain, account: account})
	w.mu.Unlock()

	return true
}

func (*recordedServiceAccountPrewarmer) PrewarmAccountNow(int32, [32]byte) bool {
	return true
}

func (w *recordedServiceAccountPrewarmer) snapshot() []pooledAccountPrewarmKey {
	w.mu.Lock()
	defer w.mu.Unlock()

	return append([]pooledAccountPrewarmKey(nil), w.accounts...)
}

func (w *recordedServiceAccountPrewarmer) rootSnapshot() []cell.Hash {
	w.mu.Lock()
	defer w.mu.Unlock()

	return append([]cell.Hash(nil), w.roots...)
}

func TestPrewarmPooledInternalsDeduplicatesDestinations(t *testing.T) {
	warmer := &recordedServiceAccountPrewarmer{}
	service := &Service{accountPrewarmer: warmer}
	first := [32]byte{0x11}
	second := [32]byte{0x22}
	seen := pooledInternalsPrewarmSeen{
		accounts:  make(map[pooledAccountPrewarmKey]struct{}),
		envelopes: make(map[cell.Hash]struct{}),
	}
	firstEnvelope := cell.Hash{0xa1}
	secondEnvelope := cell.Hash{0xa2}
	variableEnvelope := cell.Hash{0xa3}

	service.prewarmPooledInternals([]*msgpool.InternalMessage{
		{DestinationWorkchain: 0, DestinationAccount: first, DestinationPrewarmable: true, EnvHash: firstEnvelope},
		{DestinationWorkchain: 0, DestinationAccount: first, DestinationPrewarmable: true, EnvHash: firstEnvelope},
		{DestinationWorkchain: -1, DestinationAccount: second, DestinationPrewarmable: true, EnvHash: secondEnvelope},
		{DestinationWorkchain: 0, DestinationAccount: [32]byte{0x33}, EnvHash: variableEnvelope},
	}, &seen)
	service.prewarmPooledInternals([]*msgpool.InternalMessage{
		{DestinationWorkchain: -1, DestinationAccount: second, DestinationPrewarmable: true, EnvHash: secondEnvelope},
	}, &seen)

	got := warmer.snapshot()
	want := []pooledAccountPrewarmKey{
		{workchain: 0, account: first},
		{workchain: -1, account: second},
	}
	if len(got) != len(want) {
		t.Fatalf("prewarmed accounts = %+v, want %+v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("prewarmed account %d = %+v, want %+v", index, got[index], want[index])
		}
	}
	roots := warmer.rootSnapshot()
	wantRoots := []cell.Hash{firstEnvelope, secondEnvelope, variableEnvelope}
	if len(roots) != len(wantRoots) {
		t.Fatalf("prewarmed envelope roots = %x, want %x", roots, wantRoots)
	}
	for index := range wantRoots {
		if roots[index] != wantRoots[index] {
			t.Fatalf("prewarmed envelope root %d = %x, want %x", index, roots[index], wantRoots[index])
		}
	}
}

func TestExternalPoolAdmissionPrewarmsDestination(t *testing.T) {
	pool := msgpool.New(msgpool.Config{Clock: validatorTestClock{}, MempoolLimit: 1})
	t.Cleanup(pool.Close)
	warmer := &recordedServiceAccountPrewarmer{}
	service := &Service{
		log:              zerolog.Nop(),
		pool:             pool,
		accountPrewarmer: warmer,
	}
	account := testAddr(0x44)
	root := extMsgCell(t, account, 1)

	if err := service.OnExternalMessage(context.Background(), hooks.ExternalMessageEvent{MessageRoot: root}); err != nil {
		t.Fatal(err)
	}
	if stats := pool.Stats(); stats.Pooled != 0 {
		t.Fatalf("invalid-size external changed pool stats: %+v", stats)
	}
	if got := warmer.snapshot(); len(got) != 0 {
		t.Fatalf("invalid-size external prewarmed accounts: %+v", got)
	}

	if err := service.OnExternalMessage(context.Background(), externalHookEvent(root, false)); err != nil {
		t.Fatal(err)
	}
	if err := service.OnExternalMessage(context.Background(), externalHookEvent(root, false)); err != nil {
		t.Fatal(err)
	}
	rejectedAccount := testAddr(0x55)
	if err := service.OnExternalMessage(
		context.Background(),
		externalHookEvent(extMsgCell(t, rejectedAccount, 2), false),
	); err != nil {
		t.Fatal(err)
	}

	if pooled := pool.Stats().Pooled; pooled != 1 {
		t.Fatalf("pooled external messages = %d, want 1", pooled)
	}
	if overflow := pool.Stats().OverflowMempool; overflow != 1 {
		t.Fatalf("external mempool overflow = %d, want 1", overflow)
	}
	got := warmer.snapshot()
	want := pooledAccountPrewarmKey{workchain: 0, account: account}
	if len(got) != 1 || got[0] != want {
		t.Fatalf("prewarmed accounts = %+v, want %+v", got, want)
	}
}
