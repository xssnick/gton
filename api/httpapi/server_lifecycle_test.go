package httpapi

import (
	"context"
	"testing"
	"time"
)

type lifecycleStore struct {
	Store
}

func TestServerCloseStopsOwnedLifecycle(t *testing.T) {
	server, err := New(Options{
		Store:      lifecycleStore{},
		ListenAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("new http api: %v", err)
	}
	if err = server.Start(context.Background()); err != nil {
		t.Fatalf("start http api: %v", err)
	}

	closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err = server.Close(closeCtx); err != nil {
		t.Fatalf("close http api: %v", err)
	}

	done := make(chan struct{})
	go func() {
		server.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("http api Wait did not return after Close")
	}
}
