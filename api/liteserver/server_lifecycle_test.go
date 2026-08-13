package liteserver

import (
	"context"
	"crypto/ed25519"
	"net"
	"testing"
	"time"
)

func TestServerCanCloseAndWaitAfterStartFailure(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve listen address: %v", err)
	}
	defer listener.Close()

	_, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate liteserver key: %v", err)
	}
	server, err := New(Options{
		Store:            NewLiveStore(&fakeStore{}),
		PrivateKey:       privateKey,
		ListenAddr:       listener.Addr().String(),
		QueryConcurrency: 1,
	})
	if err != nil {
		t.Fatalf("new liteserver: %v", err)
	}
	if err = server.Start(context.Background()); err == nil {
		t.Fatal("expected occupied listen address to fail")
	}
	if err = server.Close(); err != nil {
		t.Fatalf("close liteserver after start failure: %v", err)
	}

	done := make(chan struct{})
	go func() {
		server.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("liteserver Wait did not return after start rollback")
	}
}
