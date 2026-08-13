package p2p

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/liteclient"
)

func TestNodeStartIsIdempotentWhileRunning(t *testing.T) {
	node := newTestNode(t)
	node.lifecycleState = nodeLifecycleRunning

	const callers = 32
	start := make(chan struct{})
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	wg.Add(callers)
	for range callers {
		go func() {
			defer wg.Done()
			<-start
			errs <- node.Start(context.Background())
		}()
	}
	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent Start returned %v", err)
		}
	}
	if err := node.Start(context.Background()); err != nil {
		t.Fatalf("idempotent Start returned %v", err)
	}
}

func TestNodeStartEndToEndIsConcurrentAndIdempotent(t *testing.T) {
	node := newTestNode(t)
	node.globalConfig = lifecycleTestGlobalConfig()

	const callers = 16
	start := make(chan struct{})
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	wg.Add(callers)
	for range callers {
		go func() {
			defer wg.Done()
			<-start
			errs <- node.Start(context.Background())
		}()
	}
	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent Start returned %v", err)
		}
	}
	if err := node.Start(context.Background()); err != nil {
		t.Fatalf("idempotent Start returned %v", err)
	}

	node.EnterOffline("test complete")
	node.Wait()
	if err := node.Start(context.Background()); !errors.Is(err, ErrOffline) {
		t.Fatalf("Start after stop error = %v, want ErrOffline", err)
	}
}

func TestNodeConcurrentStartJoinsOwnerResult(t *testing.T) {
	node := newTestNode(t)
	ownerDone, owner, err := node.beginStart()
	if err != nil || !owner {
		t.Fatalf("first beginStart = done %p, owner %v, err %v", ownerDone, owner, err)
	}
	waitDone, owner, err := node.beginStart()
	if err != nil || owner || waitDone != ownerDone {
		t.Fatalf("second beginStart = done %p, owner %v, err %v", waitDone, owner, err)
	}

	wantErr := errors.New("startup failed")
	node.lifecycleMu.Lock()
	node.lifecycleState = nodeLifecycleFailed
	node.startErr = wantErr
	close(node.startDone)
	node.startDone = nil
	node.lifecycleMu.Unlock()

	<-waitDone
	node.lifecycleMu.Lock()
	gotErr := node.startErr
	node.lifecycleMu.Unlock()
	if !errors.Is(gotErr, wantErr) {
		t.Fatalf("joined Start result = %v, want %v", gotErr, wantErr)
	}
}

func TestNodeStartAfterStopIsOffline(t *testing.T) {
	node := newTestNode(t)

	node.EnterOffline("test complete")
	node.Wait()
	if err := node.Start(context.Background()); !errors.Is(err, ErrOffline) {
		t.Fatalf("Start after stop error = %v, want ErrOffline", err)
	}
}

func TestNodeStartFailureIsTerminalAndUnblocksWait(t *testing.T) {
	node := newTestNode(t)

	firstErr := node.Start(context.Background())
	if firstErr == nil || !strings.Contains(firstErr.Error(), "global config is required") {
		t.Fatalf("Start error = %v, want missing global config", firstErr)
	}
	waitForNodeStop(t, node)

	node.globalConfig = lifecycleTestGlobalConfig()
	if err := node.Start(context.Background()); err == nil || err.Error() != firstErr.Error() {
		t.Fatalf("Start after failure error = %v, want terminal %v", err, firstErr)
	}
}

func TestNodePartialStartFailureIsTerminal(t *testing.T) {
	node := newTestNode(t)
	node.globalConfig = lifecycleTestGlobalConfig()
	node.listenAddr = "not-an-address"

	firstErr := node.Start(context.Background())
	if firstErr == nil || !strings.Contains(firstErr.Error(), "start network gateways") {
		t.Fatalf("Start error = %v, want gateway failure", firstErr)
	}
	waitForNodeStop(t, node)

	if err := node.Start(context.Background()); err == nil || err.Error() != firstErr.Error() {
		t.Fatalf("Start after partial failure error = %v, want terminal %v", err, firstErr)
	}
}

func TestNodeStartAfterPreStartStopIsOffline(t *testing.T) {
	node := newTestNode(t)
	node.EnterOffline("stopped before start")
	node.Wait()
	node.globalConfig = lifecycleTestGlobalConfig()

	if err := node.Start(context.Background()); !errors.Is(err, ErrOffline) {
		t.Fatalf("Start after pre-start stop error = %v, want ErrOffline", err)
	}
}

func waitForNodeStop(t *testing.T, node *Node) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		node.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Node.Wait did not return")
	}
}

func lifecycleTestGlobalConfig() *liteclient.GlobalConfig {
	return &liteclient.GlobalConfig{
		Validator: liteclient.ValidatorConfig{
			ZeroState: liteclient.ConfigBlock{
				Workchain: -1,
				Shard:     topShard,
				RootHash:  bytes.Repeat([]byte{0x11}, 32),
				FileHash:  bytes.Repeat([]byte{0x22}, 32),
			},
		},
	}
}
