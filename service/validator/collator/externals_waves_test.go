package collator

import (
	"bytes"
	"context"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
	funcsop "github.com/xssnick/tonutils-go/tvm/op/funcs"
	stackop "github.com/xssnick/tonutils-go/tvm/op/stack"
)

// externalWaveFixture is a shard whose inbound work is external messages and
// nothing else, shaped to reach every branch the external wave has that the
// internal one does not: accepted transactions, rejected ones, and the attempt
// budget running out part-way through a wave.
//
// Rejection is the one that matters. An external is accepted only when the
// contract calls ACCEPT, so an account whose code returns without it rejects
// every message sent to it — at the cost of one TVM attempt, exactly as a
// wallet with a stale seqno would. The sequential loop records that outcome
// without committing anything, and the wave has to end up in precisely the
// same place from a transaction it ran ahead of time on another goroutine.
type externalWaveFixture struct {
	request   ShardRequest
	accepting int
	rejecting int
	externals int
}

func newExternalWaveFixture(tb testing.TB, accepting, rejecting, perAccount int) externalWaveFixture {
	tb.Helper()

	req := emptyCandidateRequest(tb)
	accounts, err := tlb.NewShardAccountsAugDict()
	if err != nil {
		tb.Fatal(err)
	}
	accept := externalWaveCode(tb, true)
	reject := externalWaveCode(tb, false)

	var addrs []*address.Address
	install := func(index int, code *cell.Cell) {
		var raw [32]byte
		raw[0] = 0xA0
		raw[1] = byte(index >> 8)
		raw[2] = byte(index)
		addr := address.NewAddress(0, 0, raw[:])
		key := cell.BeginCell().MustStoreSlice(addr.Data(), 256).EndCell()
		data := cell.BeginCell().MustStoreUInt(uint64(index), 32).EndCell()
		if err := accounts.Set(key, executionReadShardAccount(tb.(*testing.T), addr, code, data, req.Header.GenUtime)); err != nil {
			tb.Fatal(err)
		}
		addrs = append(addrs, addr)
	}
	for i := range accepting {
		install(i, accept)
	}
	for i := range rejecting {
		install(accepting+i, reject)
	}
	req.Previous.State = stateWithAccounts(tb, req.Previous.State, accounts)
	req = advanceCandidateRequest(tb, req)

	// Round-robin over the accounts, so consecutive messages hit different
	// destinations — which is what lets a wave form at all — and so the same
	// account is hit again a few messages later, which is what makes the second
	// message of a pair depend on the first's committed state.
	var externals []ExternalInput
	for round := range perAccount {
		for i, addr := range addrs {
			msg, err := tlb.ToCell(&tlb.ExternalMessage{
				DstAddr: addr,
				Body:    cell.BeginCell().MustStoreUInt(uint64(round<<16|i), 32).EndCell(),
			})
			if err != nil {
				tb.Fatal(err)
			}
			externals = append(externals, externalInput(tb, msg))
		}
	}
	req.Externals = externals
	req.MaxExternalAttempts = len(externals)

	return externalWaveFixture{
		request:   req,
		accepting: accepting,
		rejecting: rejecting,
		externals: len(externals),
	}
}

// externalWaveCode is a contract that either accepts every external — and
// bumps a counter in its data, so consecutive messages to one account produce
// distinct states and the order they were committed in is visible in the bytes
// — or returns without accepting, which rejects the message after the attempt
// has been paid for.
func externalWaveCode(tb testing.TB, accepts bool) *cell.Cell {
	tb.Helper()

	code := cell.BeginCell()
	ops := []*cell.Builder{stackop.DROP().Serialize()}
	if accepts {
		ops = append(ops, funcsop.ACCEPT().Serialize())
	}
	for _, op := range ops {
		if err := code.StoreBuilder(op); err != nil {
			tb.Fatal(err)
		}
	}

	return code.EndCell()
}

// The three arms of the external phase must produce one block, and one set of
// pool feedback. The feedback is the part the internal gate cannot stand in
// for: which externals were included, which rejected, which skipped, and how
// many attempts were spent, all go back to the pool and decide what it offers
// next — a wave that miscounted an attempt or recorded a rejection for a
// message the sequential loop would have skipped is a divergence no byte of the
// block shows.
func TestExternalWavesProduceTheSequentialBlock(t *testing.T) {
	arms := []struct {
		name    string
		workers int
	}{
		{"sequential", -1},
		{"waves-inline", 1},
		{"waves-16", 16},
	}
	fixtures := []struct {
		name     string
		fixture  func(testing.TB) externalWaveFixture
		rejects  bool
		budgeted bool
	}{
		{"all accepted", func(tb testing.TB) externalWaveFixture {
			return newExternalWaveFixture(tb, 40, 0, 3)
		}, false, false},
		{"accepted and rejected interleaved", func(tb testing.TB) externalWaveFixture {
			return newExternalWaveFixture(tb, 24, 24, 3)
		}, true, false},
		{"attempt budget runs out inside a wave", func(tb testing.TB) externalWaveFixture {
			f := newExternalWaveFixture(tb, 24, 24, 3)
			// The budget is reserved at planning, so the wave stops planning
			// exactly where the sequential loop stops executing, and the
			// messages past the budget are skipped unexecuted on every arm.
			f.request.MaxExternalAttempts = 41
			return f
		}, true, true},
		// The synthetic fixtures above run with no internal messages and no
		// collated proof, and that leaves two of the wave's obligations
		// unobservable. The external lt floor is c.lastProcLT, which only the
		// import phase moves — with nothing imported it is zero on every arm and
		// a wave that forgot it would still match. And a lane's buffered reads
		// are replayed into the record that selects the collated proof — with
		// the capability off nothing selects anything, and a wave that dropped
		// the replay would still match. The mainnet fixture has 257 queued
		// internals ahead of its 12 externals and builds the full proof, so on
		// it both omissions change the bytes.
		{"mainnet: floored behind imported internals, full collated proof", func(tb testing.TB) externalWaveFixture {
			req := fullCollatedMainnetRequest(tb)
			return externalWaveFixture{request: req, externals: len(req.Externals)}
		}, false, false},
	}
	for _, fixture := range fixtures {
		t.Run(fixture.name, func(t *testing.T) {
			var reference *Candidate
			for _, arm := range arms {
				f := fixture.fixture(t)
				req := f.request
				req.internalWaveWorkers = arm.workers
				candidate, err := testBuilder().BuildShard(context.Background(), req)
				if err != nil {
					t.Fatalf("%s: %v", arm.name, err)
				}
				stats := candidate.Stats
				if stats.ExternalIncluded == 0 {
					t.Fatalf("%s: no external was included, so the fixture exercises nothing", arm.name)
				}
				if fixture.rejects && stats.ExternalNotAccepted == 0 {
					t.Fatalf("%s: no external was rejected, so the rejection path went untested", arm.name)
				}
				if fixture.budgeted {
					// ExternalStop belongs to the live-stream path; BuildShard runs
					// the plain batch, where the stop is visible only as the budget
					// being exactly spent and the tail being skipped.
					if int(stats.ExternalAttempts) != f.request.MaxExternalAttempts {
						t.Fatalf("%s: spent %d attempts against a budget of %d", arm.name,
							stats.ExternalAttempts, f.request.MaxExternalAttempts)
					}
					if stats.ExternalSkippedLimit == 0 {
						t.Fatalf("%s: nothing was skipped after the budget ran out", arm.name)
					}
				}
				if reference == nil {
					reference = candidate
					t.Logf("%s: included %d, rejected %d, skipped %d, attempts %d, block %d B",
						arm.name, stats.ExternalIncluded, stats.ExternalNotAccepted,
						stats.ExternalSkippedLimit, stats.ExternalAttempts, len(candidate.BlockBOC))
					continue
				}
				if !bytes.Equal(candidate.BlockBOC, reference.BlockBOC) {
					t.Fatalf("%s produced a different block (%d B against %d B)", arm.name,
						len(candidate.BlockBOC), len(reference.BlockBOC))
				}
				if !bytes.Equal(candidate.CollatedData, reference.CollatedData) {
					t.Fatalf("%s produced different collated data", arm.name)
				}
				got, want := stats, reference.Stats
				got.InternalsSpeculated, got.InternalsDiscarded = 0, 0
				want.InternalsSpeculated, want.InternalsDiscarded = 0, 0
				if got != want {
					t.Fatalf("%s produced different stats:\n got  %+v\n want %+v", arm.name, got, want)
				}
				// The pool feedback, entry by entry and in order. This is what
				// the pool acts on, and the block does not carry it.
				if len(candidate.Externals) != len(reference.Externals) {
					t.Fatalf("%s reported %d feedback entries, want %d", arm.name,
						len(candidate.Externals), len(reference.Externals))
				}
				for i := range reference.Externals {
					if candidate.Externals[i] != reference.Externals[i] {
						t.Fatalf("%s: feedback %d is %+v, want %+v", arm.name, i,
							candidate.Externals[i], reference.Externals[i])
					}
				}
			}
		})
	}
}
