package liteserver

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"net"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

func TestWaitLimiterCapsPerIP(t *testing.T) {
	limiter := newWaitLimiter(2)

	if !limiter.acquire("1.1.1.1") || !limiter.acquire("1.1.1.1") {
		t.Fatal("acquires below the limit failed")
	}
	if limiter.acquire("1.1.1.1") {
		t.Fatal("acquire above the limit succeeded")
	}
	if !limiter.acquire("2.2.2.2") {
		t.Fatal("acquire for another ip failed")
	}

	limiter.release("1.1.1.1")
	if !limiter.acquire("1.1.1.1") {
		t.Fatal("acquire after release failed")
	}
}

func TestQueryExecutorShedsWhenPendingFull(t *testing.T) {
	executor := &queryExecutor{tasks: make(chan executorTask, 1), done: make(chan struct{})}
	executor.wg.Add(1)
	go executor.worker()
	defer func() {
		executor.Close()
		executor.Wait()
	}()

	// Capture the only worker so subsequent runs deterministically queue.
	release, ok := executor.occupy(context.Background())
	if !ok {
		t.Fatal("occupy failed")
	}

	done := make(chan struct{}, 1)
	if !executor.run(context.Background(), func() { done <- struct{}{} }) {
		t.Fatal("pending run was shed below the cap")
	}
	if executor.run(context.Background(), func() { done <- struct{}{} }) {
		t.Fatal("run above the pending cap was accepted")
	}

	release()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("accepted query did not finish")
	}
}

func TestQueryExecutorDropsQueuedOnContextCancel(t *testing.T) {
	executor := &queryExecutor{tasks: make(chan executorTask, 1), done: make(chan struct{})}
	executor.wg.Add(1)
	go executor.worker()
	defer func() {
		executor.Close()
		executor.Wait()
	}()

	// Capture the only worker so the next run deterministically queues.
	release, ok := executor.occupy(context.Background())
	if !ok {
		t.Fatal("occupy failed")
	}

	ctx, cancel := context.WithCancel(context.Background())
	ran := make(chan struct{}, 1)
	if !executor.run(ctx, func() { ran <- struct{}{} }) {
		t.Fatal("queued run was shed")
	}
	cancel()
	release()

	select {
	case <-ran:
		t.Fatal("cancelled queued query still ran")
	case <-time.After(200 * time.Millisecond):
	}
}

func TestQueryExecutorOccupyQueuedCancelDoesNotLeak(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		executor := &queryExecutor{
			tasks: make(chan executorTask, 1),
			done:  make(chan struct{}),
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		type occupyResult struct {
			release func()
			ok      bool
		}
		result := make(chan occupyResult, 1)
		go func() {
			release, ok := executor.occupy(ctx)
			result <- occupyResult{release: release, ok: ok}
		}()

		// Wait until occupy has queued the hold and is parked waiting for a
		// worker to pick it. There is deliberately no worker: cancellation
		// must not leave a cleanup goroutine waiting for one.
		synctest.Wait()
		if len(executor.tasks) != 1 {
			t.Fatalf("queued holds = %d, want 1", len(executor.tasks))
		}
		cancel()

		res := <-result
		if res.ok || res.release != nil {
			t.Fatal("cancelled queued occupy succeeded")
		}
	})
}

func TestQueryExecutorCloseStopsWorkers(t *testing.T) {
	executor := newQueryExecutor(2)
	executor.Close()

	done := make(chan struct{})
	go func() {
		executor.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("query executor workers did not stop")
	}

	if executor.run(context.Background(), func() {}) {
		t.Fatal("closed executor accepted a task")
	}
}

func TestClassifyQuery(t *testing.T) {
	current, root, data := testCurrentStateWithLiveBlock(t, 10)
	live := NewLiveStore(&fakeStore{})
	if err := live.publishLiveBlockData(current.Masterchain.Block, root, data, true); err != nil {
		t.Fatalf("publish live master block: %v", err)
	}
	live.SetLiveCurrentState(current)
	srv := testServer(live)

	cases := []struct {
		name  string
		query tl.Serializable
		want  queryClass
	}{
		{"masterchain-info", ton.GetMasterchainInf{}, queryClassFast},
		{"masterchain-info-ext", ton.GetMasterchainInfoExt{}, queryClassFast},
		{"time", ton.GetTime{}, queryClassFast},
		{"wrapped-fast", liteclient.LiteServerQuery{Data: ton.GetMasterchainInf{}}, queryClassFast},
		{"account-state", ton.GetAccountState{}, queryClassBounded},
		{"ready-wait-fast", []tl.Serializable{ton.WaitMasterchainSeqno{Seqno: 10}, ton.GetMasterchainInf{}}, queryClassFast},
		{"ready-wait-bounded", []tl.Serializable{ton.WaitMasterchainSeqno{Seqno: 10}, ton.GetAccountState{}}, queryClassBounded},
		{"unready-wait", []tl.Serializable{ton.WaitMasterchainSeqno{Seqno: 11}, ton.GetMasterchainInf{}}, queryClassWait},
		{"invalid-wait", []tl.Serializable{ton.WaitMasterchainSeqno{Seqno: -1}, ton.GetAccountState{}}, queryClassBounded},
	}
	for _, tc := range cases {
		if got := srv.classifyQuery(tc.query); got != tc.want {
			t.Fatalf("%s: class = %d, want %d", tc.name, got, tc.want)
		}
	}
}

// TestDispatchIntegration runs the full client-server flow over the new
// dispatch: inline fast queries, a parked wait that wakes up on the next
// published block, the per-IP parked wait cap, and a bounded executor query.
func TestDispatchIntegration(t *testing.T) {
	current, root, data := testCurrentStateWithLiveBlock(t, 10)
	live := NewLiveStore(&fakeStore{})
	if err := live.publishLiveBlockData(current.Masterchain.Block, root, data, true); err != nil {
		t.Fatalf("publish live master block: %v", err)
	}
	live.SetLiveCurrentState(current)

	_, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}

	addr := freeListenAddr(t)
	srv, err := New(Options{
		Store:         live,
		MessageSender: nil,
		PrivateKey:    key,
		ListenAddr:    addr,
		RequestLimits: RequestLimitOptions{MaxWaitsPerIP: 1},
	})
	if err != nil {
		t.Fatalf("new liteserver: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err = srv.Start(ctx); err != nil {
		t.Fatalf("start liteserver: %v", err)
	}
	defer func() {
		cancel()
		_ = srv.Close()
		srv.Wait()
	}()

	pool := liteclient.NewConnectionPool()
	defer pool.Stop()

	connectCtx, connectCancel := context.WithTimeout(context.Background(), 5*time.Second)
	err = pool.AddConnection(connectCtx, addr, base64.StdEncoding.EncodeToString(key.Public().(ed25519.PublicKey)))
	connectCancel()
	if err != nil {
		t.Fatalf("connect to liteserver: %v", err)
	}
	api := ton.NewAPIClient(pool)

	queryCtx, queryCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer queryCancel()

	// Fast path: snapshot query answered inline from the read loop.
	master, err := api.GetMasterchainInfo(queryCtx)
	if err != nil {
		t.Fatalf("get masterchain info: %v", err)
	}
	if master.SeqNo != 10 {
		t.Fatalf("master seqno = %d, want 10", master.SeqNo)
	}

	// Bounded path: block data goes through the executor. The stored block is a
	// synthetic BOC, so query raw instead of parsing it as a real block.
	var rawResp tl.Serializable
	if err = pool.QueryLiteserver(queryCtx, ton.GetBlockData{ID: master}, &rawResp); err != nil {
		t.Fatalf("get block data: %v", err)
	}
	blockResp, ok := rawResp.(ton.BlockData)
	if !ok {
		t.Fatalf("unexpected block data response %T", rawResp)
	}
	if !bytes.Equal(blockResp.Payload, data) {
		t.Fatal("block data payload mismatch")
	}

	// Parked waits: with MaxWaitsPerIP=1 the second concurrent wait is shed.
	type waitResult struct {
		seqno uint32
		err   error
	}
	results := make(chan waitResult, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			waited, err := api.WaitForBlock(11).GetMasterchainInfo(queryCtx)
			if err != nil {
				results <- waitResult{err: err}
				return
			}
			results <- waitResult{seqno: waited.SeqNo}
		}()
	}

	waitForParkedWait(t, srv, "1 parked wait")

	// Publish the next master block: the parked wait must wake up and answer.
	next, nextRoot, nextData := testCurrentStateWithLiveBlock(t, 11)
	if err := live.publishLiveBlockData(next.Masterchain.Block, nextRoot, nextData, true); err != nil {
		t.Fatalf("publish next live master block: %v", err)
	}
	live.SetLiveCurrentState(next)

	wg.Wait()
	close(results)

	var completed, shed int
	for result := range results {
		if result.err == nil {
			if result.seqno != 11 {
				t.Fatalf("waited master seqno = %d, want 11", result.seqno)
			}
			completed++
			continue
		}

		var lsErr ton.LSError
		if !errors.As(result.err, &lsErr) || lsErr.Code != errCodeTooManyRequests {
			t.Fatalf("unexpected wait error: %v", result.err)
		}
		shed++
	}
	if completed != 1 || shed != 1 {
		t.Fatalf("waits completed=%d shed=%d, want 1/1", completed, shed)
	}

	// The freed wait slot must be reusable.
	waited, err := api.WaitForBlock(11).GetMasterchainInfo(queryCtx)
	if err != nil {
		t.Fatalf("ready wait after park: %v", err)
	}
	if waited.SeqNo != 11 {
		t.Fatalf("ready waited seqno = %d, want 11", waited.SeqNo)
	}
}

// BenchmarkDispatchSingleConnection measures pipelined fast-path throughput
// over one real client connection — the typical backend usage pattern.
func BenchmarkDispatchSingleConnection(b *testing.B) {
	current, root, data := testCurrentStateWithLiveBlock(b, 10)
	live := NewLiveStore(&fakeStore{})
	if err := live.publishLiveBlockData(current.Masterchain.Block, root, data, true); err != nil {
		b.Fatalf("publish live master block: %v", err)
	}
	live.SetLiveCurrentState(current)

	_, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		b.Fatal(err)
	}

	addr := freeListenAddr(b)
	srv, err := New(Options{
		Store:      live,
		PrivateKey: key,
		ListenAddr: addr,
	})
	if err != nil {
		b.Fatalf("new liteserver: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err = srv.Start(ctx); err != nil {
		b.Fatalf("start liteserver: %v", err)
	}
	defer func() {
		cancel()
		_ = srv.Close()
		srv.Wait()
	}()

	pool := liteclient.NewConnectionPool()
	defer pool.Stop()

	connectCtx, connectCancel := context.WithTimeout(context.Background(), 5*time.Second)
	err = pool.AddConnection(connectCtx, addr, base64.StdEncoding.EncodeToString(key.Public().(ed25519.PublicKey)))
	connectCancel()
	if err != nil {
		b.Fatalf("connect to liteserver: %v", err)
	}
	api := ton.NewAPIClient(pool)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := api.GetMasterchainInfo(context.Background()); err != nil {
				b.Fatalf("get masterchain info: %v", err)
			}
		}
	})
}

func waitForParkedWait(t *testing.T, srv *Server, what string) {
	t.Helper()

	deadline := time.After(5 * time.Second)
	for {
		srv.waitLimiter.mu.Lock()
		parked := 0
		for _, n := range srv.waitLimiter.perIP {
			parked += n
		}
		srv.waitLimiter.mu.Unlock()
		if parked >= 1 {
			return
		}

		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %s", what)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func freeListenAddr(t testing.TB) string {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err = ln.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}
