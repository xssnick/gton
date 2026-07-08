package liteserver

import (
	"context"
	"runtime"
	"sync"

	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

// DefaultMaxWaitsPerIP is the parked waitMasterchainSeqno cap per client
// address applied when RequestLimitOptions.MaxWaitsPerIP is zero.
const DefaultMaxWaitsPerIP = 256

const (
	// queryExecutorPendingFactor sizes the shed threshold: how many queries may
	// wait for an execution slot per unit of concurrency before new ones are
	// rejected instead of queued.
	queryExecutorPendingFactor = 32

	queryErrorReasonWaitLimited = "wait_limited"
	queryErrorReasonOverloaded  = "overloaded"
)

type queryClass uint8

const (
	queryClassFast queryClass = iota
	queryClassWait
	queryClassBounded
)

// dispatchQueryRequest is called synchronously from the connection read loop,
// so every path here must be quick: snapshot queries are answered inline, not
// yet satisfiable waitMasterchainSeqno sequences park in their own goroutine,
// and everything else runs through the bounded executor.
func (s *Server) dispatchQueryRequest(ctx context.Context, client *liteclient.ServerClient, queryID []byte, data tl.Serializable) {
	if s.connectionTracker != nil && s.connectionTracker.keepAliveEnabled() && s.requestLimiter == nil {
		s.connectionTracker.beginRequest(client, s.now())
	}

	switch s.classifyQuery(data) {
	case queryClassFast:
		s.finishQuery(ctx, client, queryID, data, nil)
	case queryClassWait:
		s.dispatchWaitQuery(ctx, client, queryID, data)
	default:
		accepted := s.queryExecutor.run(ctx, func() {
			s.finishQuery(ctx, client, queryID, data, nil)
		})
		if !accepted {
			s.shedQuery(client, queryID, data, queryErrorReasonOverloaded, "too many requests in flight")
		}
	}
}

func (s *Server) dispatchWaitQuery(ctx context.Context, client *liteclient.ServerClient, queryID []byte, data tl.Serializable) {
	ip := client.IP()
	if !s.waitLimiter.acquire(ip) {
		s.shedQuery(client, queryID, data, queryErrorReasonWaitLimited, "too many parked waits")
		return
	}

	// The wait itself parks on the store notify channel without holding an
	// executor slot; the continuation after the wait acquires a slot through
	// the gate, bypassing the pending cap because the query is already admitted.
	go func() {
		defer s.waitLimiter.release(ip)
		s.finishQuery(ctx, client, queryID, data, &executorGate{executor: s.queryExecutor})
	}()
}

func (s *Server) finishQuery(ctx context.Context, client *liteclient.ServerClient, queryID []byte, data tl.Serializable, gate *executorGate) {
	defer gate.leave()

	if s.queryObserver != nil {
		s.queryObserver.AddLiteserverInflight(1)
		defer s.queryObserver.AddLiteserverInflight(-1)
	}

	// The timing variant is always used; when neither debug logging nor the
	// query observer consumes the result, its residual cost is two clock reads
	// because the debug names resolve lazily inside queryLogTiming.
	event := s.log.Debug()
	resp, timing := s.handleQueryDataWithTiming(ctx, data, gate)
	if resp == nil {
		// The client disconnected while the query waited for an executor slot.
		s.endQueryRequest(client)
		return
	}

	client.Answer(queryID, resp)
	if s.queryObserver != nil {
		s.queryObserver.ObserveLiteserverQuery(queryObservationFromResponse(resp, timing))
	}
	if event.Enabled() {
		queryName := timing.queryName()
		event = event.
			Str("query", queryName).
			Str("response", liteserverTypeName(resp)).
			Dur("duration", timing.duration).
			Str("remote_ip", client.IP()).
			Uint16("remote_port", client.Port())
		if sequence := timing.sequenceName(); sequence != "" && sequence != queryName {
			event = event.Str("sequence", sequence)
		}
		if timing.waitDuration > 0 {
			event = event.Dur("wait_duration", timing.waitDuration)
		}
		if lsErr, ok := resp.(ton.LSError); ok {
			event = event.Int32("error_code", lsErr.Code)
		}
		if timing.errorReason != "" {
			event = event.Str("error_reason", timing.errorReason)
		}
		event.Msg("handled liteserver query")
	}
	s.endQueryRequest(client)
}

func (s *Server) shedQuery(client *liteclient.ServerClient, queryID []byte, data tl.Serializable, reason string, text string) {
	resp := ton.LSError{Code: errCodeTooManyRequests, Text: text}
	client.Answer(queryID, resp)

	timing := queryLogTiming{query: data, errorReason: reason}
	if s.queryObserver != nil {
		s.queryObserver.ObserveLiteserverQuery(queryObservationFromResponse(resp, timing))
	}
	if event := s.log.Debug(); event.Enabled() {
		event.
			Str("query", timing.queryName()).
			Str("response", liteserverTypeName(resp)).
			Str("remote_ip", client.IP()).
			Uint16("remote_port", client.Port()).
			Int32("error_code", resp.Code).
			Str("error_reason", reason).
			Msg("shed liteserver query")
	}
	s.endQueryRequest(client)
}

func (s *Server) endQueryRequest(client *liteclient.ServerClient) {
	if s.connectionTracker != nil && s.connectionTracker.keepAliveEnabled() {
		s.connectionTracker.endRequest(client, s.now())
	}
}

// classifyQuery decides the dispatch path. Snapshot queries (and sequences
// ending in one whose waits are already satisfied) are fast; sequences with an
// unsatisfied waitMasterchainSeqno park; the rest is bounded CPU work.
func (s *Server) classifyQuery(data tl.Serializable) queryClass {
	value := unwrapLiteServerQuery(data)
	items, ok := value.([]tl.Serializable)
	if !ok {
		if isFastLiteserverQuery(value) {
			return queryClassFast
		}
		return queryClassBounded
	}

	fast := false
	for _, item := range items {
		switch q := unwrapLiteServerQuery(item).(type) {
		case liteclient.LiteServerQueryPrefix:
		case ton.WaitMasterchainSeqno:
			if q.Seqno >= 0 && !s.store.MasterchainSeqnoReady(uint32(q.Seqno)) {
				return queryClassWait
			}
		default:
			fast = isFastLiteserverQuery(q)
		}
	}
	if fast {
		return queryClassFast
	}
	return queryClassBounded
}

func unwrapLiteServerQuery(data any) any {
	for {
		q, ok := data.(liteclient.LiteServerQuery)
		if !ok {
			return data
		}
		data = q.Data
	}
}

func isFastLiteserverQuery(query any) bool {
	switch query.(type) {
	case ton.GetTime, ton.GetVersion, ton.GetMasterchainInf, ton.GetMasterchainInfoExt:
		return true
	default:
		return false
	}
}

// queryExecutor bounds concurrent query processing with a pool of live
// workers on a buffered queue: enqueueing is a couple of channel operations
// with no per-query goroutine, tasks whose client disconnected are skipped at
// dequeue, and a full backlog is shed explicitly instead of stalling the
// connection read loops.
type queryExecutor struct {
	tasks     chan executorTask
	done      chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

type executorTask struct {
	ctx context.Context
	fn  func()
}

func newQueryExecutor(concurrency int) *queryExecutor {
	if concurrency <= 0 {
		concurrency = runtime.GOMAXPROCS(0) * 4
	}

	executor := &queryExecutor{
		tasks: make(chan executorTask, concurrency*queryExecutorPendingFactor),
		done:  make(chan struct{}),
	}
	for i := 0; i < concurrency; i++ {
		executor.wg.Add(1)
		go executor.worker()
	}
	return executor
}

func (e *queryExecutor) worker() {
	defer e.wg.Done()
	for {
		select {
		case <-e.done:
			return
		case task := <-e.tasks:
			if task.ctx.Err() != nil {
				// The client disconnected while the task waited in the queue.
				continue
			}
			task.fn()
		}
	}
}

// run enqueues fn for the worker pool. It returns false when the backlog is
// full and the query must be shed.
func (e *queryExecutor) run(ctx context.Context, fn func()) bool {
	select {
	case <-e.done:
		return false
	default:
	}

	select {
	case e.tasks <- executorTask{ctx: ctx, fn: fn}:
		return true
	default:
		return false
	}
}

// occupy blocks until a worker is captured and returns its release func; used
// by continuations of parked waits which must not be shed. The worker stays
// parked until release is called, while the continuation runs in the caller's
// goroutine, so wake-up bursts stay bounded by the pool size.
func (e *queryExecutor) occupy(ctx context.Context) (func(), bool) {
	picked := make(chan struct{})
	done := make(chan struct{})
	hold := executorTask{
		// Background: the hold must never be skipped at dequeue, otherwise the
		// picked worker would park forever below.
		ctx: context.Background(),
		fn: func() {
			close(picked)
			select {
			case <-done:
			case <-e.done:
			}
		},
	}

	select {
	case <-e.done:
		return nil, false
	case e.tasks <- hold:
	case <-ctx.Done():
		return nil, false
	}

	select {
	case <-picked:
		return func() { close(done) }, true
	case <-ctx.Done():
		// The hold is already queued; unpark the worker once it takes it.
		go func() {
			select {
			case <-picked:
				close(done)
			case <-e.done:
			}
		}()
		return nil, false
	case <-e.done:
		return nil, false
	}
}

func (e *queryExecutor) Close() {
	e.closeOnce.Do(func() {
		close(e.done)
	})
}

func (e *queryExecutor) Wait() {
	e.wg.Wait()
}

// executorGate defers capturing a worker until the query part of a parked
// wait sequence actually starts, so the wait itself never occupies one. A nil
// gate means the caller already runs on a worker.
type executorGate struct {
	executor *queryExecutor
	release  func()
}

func (g *executorGate) enter(ctx context.Context) bool {
	if g == nil || g.release != nil {
		return true
	}

	release, ok := g.executor.occupy(ctx)
	if !ok {
		return false
	}
	g.release = release
	return true
}

func (g *executorGate) leave() {
	if g != nil && g.release != nil {
		g.release()
		g.release = nil
	}
}

// waitLimiter caps parked waitMasterchainSeqno goroutines per client address.
type waitLimiter struct {
	max   int
	mu    sync.Mutex
	perIP map[string]int
}

func newWaitLimiter(max int) *waitLimiter {
	if max <= 0 {
		max = DefaultMaxWaitsPerIP
	}
	return &waitLimiter{max: max, perIP: map[string]int{}}
}

func (l *waitLimiter) acquire(ip string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.perIP[ip] >= l.max {
		return false
	}
	l.perIP[ip]++
	return true
}

func (l *waitLimiter) release(ip string) {
	l.mu.Lock()
	if n := l.perIP[ip]; n <= 1 {
		delete(l.perIP, ip)
	} else {
		l.perIP[ip] = n - 1
	}
	l.mu.Unlock()
}
