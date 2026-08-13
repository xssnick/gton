package simplex

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/ton"
)

// pipelineHooks completes validation and store inline, like a zero-cost
// embedder; the benchmark then measures pure engine cost.
type pipelineHooks struct {
	acceptingHooks
}

func (*pipelineHooks) ValidateCandidate(_ *Candidate, done func(error)) { done(nil) }
func (*pipelineHooks) StoreCandidate(_ *Candidate, done func(error))    { done(nil) }
func (*pipelineHooks) HandleWindow(Window)                              {}

type pipelineSlot struct {
	cand  *Candidate
	notar [][]byte
	final [][]byte
}

func buildPipeline(b *testing.B, n, slots int) (*Engine, []pipelineSlot) {
	b.Helper()
	return buildPipelineWith(b, n, slots, &pipelineHooks{}, newFakeClock(), nil)
}

// buildPipelineWith is buildPipeline with the embedder pieces the Runner
// variant has to replace: it needs the wall clock the Runner arms its timer
// off, timeouts long enough that no alarm fires mid-benchmark, and hooks that
// report finalizations back to the benchmark goroutine.
func buildPipelineWith(
	b *testing.B,
	n, slots int,
	hooks Hooks,
	clock Clock,
	params *Params,
) (*Engine, []pipelineSlot) {
	b.Helper()
	vals, keys := testValidators(n)
	var session [32]byte
	session[0] = 0x42
	eng, err := NewEngine(Config{
		SessionID:            session,
		ProtocolVersion:      3,
		Validators:           vals,
		LocalIndex:           0,
		SlotsPerLeaderWindow: 4,
		Journal:              NewMemoryJournal(n),
		Transport:            &fakeTransport{},
		Hooks:                hooks,
		Signer:               newTestSigner(keys[0]),
		Clock:                clock,
		Params:               params,
	})
	if err != nil {
		b.Fatal(err)
	}
	out := make([]pipelineSlot, slots)
	parent := Genesis()
	for s := 0; s < slots; s++ {
		leader := eng.schedule.ExpectedLeader(uint32(s))
		c := &Candidate{
			ID:     CandidateID{Slot: uint32(s)},
			Parent: parent,
			Leader: leader,
			Block: ton.BlockIDExt{
				Workchain: 0, Shard: -0x8000000000000000, SeqNo: uint32(s + 1),
				RootHash: make([]byte, 32), FileHash: make([]byte, 32),
			},
		}
		for i := 0; i < 32; i++ {
			c.Block.RootHash[i] = byte(s)
			c.Block.FileHash[i] = byte(s) ^ 0xff
		}
		c.ID = c.ComputeID(uint32(s))
		sig, err := SignCandidate(newTestSigner(keys[leader]), session, c.ID)
		if err != nil {
			b.Fatal(err)
		}
		c.Signature = sig
		parent = Parent(c.ID)

		d := pipelineSlot{cand: c}
		for i := 1; i < n; i++ {
			nv := NotarizeVote(c.ID)
			sv := SignedVote{ValidatorIndex: uint32(i), Vote: nv,
				Signature: ed25519.Sign(keys[i], DataToSign(session, VoteBytes(nv)))}
			d.notar = append(d.notar, sv.Serialize())
			fv := FinalizeVote(c.ID)
			sf := SignedVote{ValidatorIndex: uint32(i), Vote: fv,
				Signature: ed25519.Sign(keys[i], DataToSign(session, VoteBytes(fv)))}
			d.final = append(d.final, sf.Serialize())
		}
		out[s] = d
	}
	return eng, out
}

// BenchmarkSteadySlotPipeline drives full slots through a validator engine:
// candidate submission, every remote vote, own votes and both certificates.
// One iteration = one finalized slot; ~90% of the cost is raw ed25519.
func BenchmarkSteadySlotPipeline(b *testing.B) {
	for _, n := range []int{25, 100, 300} {
		b.Run(fmt.Sprintf("validators=%d", n), func(b *testing.B) {
			eng, slots := buildPipeline(b, n, b.N)
			if err := eng.Start(); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for s := 0; s < b.N; s++ {
				d := slots[s]
				if err := eng.SubmitCandidate(d.cand); err != nil {
					b.Fatal(err)
				}
				for i, w := range d.notar {
					eng.HandleMessage(peer(i+1), i+1, w)
				}
				for i, w := range d.final {
					eng.HandleMessage(peer(i+1), i+1, w)
				}
				if eng.slots.firstNonFinalized != uint32(s+1) {
					b.Fatalf("slot %d not finalized", s)
				}
			}
			if err := eng.Err(); err != nil {
				b.Fatal(err)
			}
		})
	}
}

// runnerPipelineHooks reports finalizations to the benchmark goroutine; the
// channel is buffered past one slot so the loop never blocks on it.
type runnerPipelineHooks struct {
	pipelineHooks
	finalized chan uint32
}

func (h *runnerPipelineHooks) OnFinalized(id CandidateID, _ *Certificate) {
	h.finalized <- id.Slot
}

// BenchmarkRunnerSlotPipeline is BenchmarkSteadySlotPipeline driven through the
// production Runner. It exists because the engine-level benchmark cannot
// measure batched verification at all: it calls Engine.HandleMessage, which
// admits and verifies one message per call, while here a whole vote round is
// posted at once and the loop drains it as one batch.
func BenchmarkRunnerSlotPipeline(b *testing.B) {
	for _, n := range []int{25, 100, 300} {
		b.Run(fmt.Sprintf("validators=%d", n), func(b *testing.B) {
			// Wall-clock timers: the Runner arms its own timer off time.Now,
			// so a frozen clock would make it spin instead of sleep. The
			// consensus timeouts are pushed out of reach — this measures
			// ingestion, and a fired alarm would derail the pipeline.
			params := DefaultParams()
			params.FirstBlockTimeout = time.Hour
			params.StandstillTimeout = time.Hour
			hooks := &runnerPipelineHooks{finalized: make(chan uint32, 64)}
			eng, slots := buildPipelineWith(b, n, b.N, hooks, nil, &params)

			r := NewRunner(eng)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			startup := make(chan error, 1)
			exited := make(chan error, 1)
			go func() { exited <- r.RunWithStartup(ctx, startup) }()
			if err := <-startup; err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for s := 0; s < b.N; s++ {
				d := slots[s]
				if err := r.SubmitCandidate(d.cand); err != nil {
					b.Fatal(err)
				}
				for i, w := range d.notar {
					r.HandleMessage(peer(i+1), i+1, w)
				}
				for i, w := range d.final {
					r.HandleMessage(peer(i+1), i+1, w)
				}
				for done := false; !done; {
					select {
					case slot := <-hooks.finalized:
						done = slot == uint32(s)
					case err := <-exited:
						b.Fatalf("runner exited at slot %d: %v", s, err)
					}
				}
			}
			b.StopTimer()
			r.Stop()
		})
	}
}

// BenchmarkCatchupCertificates models standstill catch-up on an observer: per
// slot a notarization and a finalization certificate arrive from the network
// and must be fully verified, persisted and applied.
func BenchmarkCatchupCertificates(b *testing.B) {
	const n = 100
	vals, keys := testValidators(n)
	var session [32]byte
	session[0] = 0x77
	eng, err := NewEngine(Config{
		SessionID:            session,
		ProtocolVersion:      3,
		Validators:           vals,
		LocalIndex:           ObserverIndex,
		SlotsPerLeaderWindow: 4,
		Journal:              NewMemoryJournal(n),
		Transport:            &fakeTransport{},
		Hooks:                acceptingHooks{},
		Clock:                newFakeClock(),
	})
	if err != nil {
		b.Fatal(err)
	}
	if err = eng.Start(); err != nil {
		b.Fatal(err)
	}
	threshold := 2*n/3 + 1

	type pair struct{ notar, final []byte }
	pairs := make([]pair, b.N)
	for s := 0; s < b.N; s++ {
		id := candID(uint32(s), byte(s))
		var p pair
		for kind, dst := range map[VoteKind]*[]byte{VoteNotarize: &p.notar, VoteFinalize: &p.final} {
			v := Vote{Kind: kind, ID: id}
			cert := &Certificate{Vote: v}
			payload := DataToSign(session, VoteBytes(v))
			for i := 0; i < threshold; i++ {
				cert.Signatures = append(cert.Signatures, VoteSignature{
					ValidatorIndex: uint32(i), Signature: ed25519.Sign(keys[i], payload)})
			}
			*dst = cert.Serialize()
		}
		pairs[s] = p
	}

	b.ReportAllocs()
	b.ResetTimer()
	for s := 0; s < b.N; s++ {
		eng.HandleMessage(peer(1), 1, pairs[s].notar)
		eng.HandleMessage(peer(1), 1, pairs[s].final)
		if eng.slots.firstNonFinalized != uint32(s+1) {
			b.Fatalf("slot %d not finalized", s)
		}
	}
}

// BenchmarkFinalizePrune isolates slotMap.notifyFinalized with many tracked
// slots (a node that was stuck while live certificates kept arriving); the
// range-delete keeps a long finalization cascade linear.
func BenchmarkFinalizePrune(b *testing.B) {
	const tracked = 5000
	b.ReportAllocs()
	var total time.Duration
	for it := 0; it < b.N; it++ {
		m := newSlotMap(func() *struct{} { return &struct{}{} })
		for i := uint32(0); i < tracked; i++ {
			m.at(i)
		}
		start := time.Now()
		for i := uint32(0); i < tracked; i++ {
			m.notifyFinalized(i)
		}
		total += time.Since(start)
	}
	b.ReportMetric(float64(total.Nanoseconds())/float64(b.N)/tracked, "ns/finalization")
}
