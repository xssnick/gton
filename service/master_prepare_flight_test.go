package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	tnstore "github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
)

func testFlightStateCellRecords(fill byte) tnstore.StateCellRecords {
	return tnstore.NewStateCellRecords([]tnstore.EncodedCellRecord{{Data: []byte{fill}}})
}

func TestMasterPrepareFlightCollapsesConcurrentCalls(t *testing.T) {
	var flight masterPrepareFlight
	key := tnstore.BlockKey(testMasterBlockID(41))

	release := make(chan struct{})
	entered := make(chan struct{})
	var callsMu sync.Mutex
	calls := 0
	produced := map[byte]tnstore.StateCellRecords{}

	//nolint:unparam // masterPrepareFlight.do requires an error result; this fixture is intentionally successful.
	work := func() (tnstore.StateCellRecords, error) {
		callsMu.Lock()
		calls++
		fill := byte(calls)
		records := testFlightStateCellRecords(fill)
		produced[fill] = records
		first := calls == 1
		callsMu.Unlock()
		if first {
			close(entered)
			<-release
		}
		return records, nil
	}

	// The owner starts and blocks inside the work function.
	results := make(chan tnstore.StateCellRecords, 4)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		cells, _, err := flight.do(context.Background(), key, work)
		if err != nil {
			t.Errorf("owner flight returned error: %v", err)
			return
		}
		results <- cells
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first flight call never started")
	}

	var waiting sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		waiting.Add(1)
		go func() {
			defer wg.Done()
			waiting.Done()
			cells, _, err := flight.do(context.Background(), key, work)
			if err != nil {
				t.Errorf("waiter flight returned error: %v", err)
				return
			}
			results <- cells
		}()
	}
	waiting.Wait()
	// The owner cannot finish while release is open, so this only gives the
	// waiters time to reach the join.
	time.Sleep(20 * time.Millisecond)

	close(release)
	wg.Wait()
	close(results)

	callsMu.Lock()
	got := calls
	shared := produced[1]
	callsMu.Unlock()
	if got != 1 {
		t.Fatalf("state cell preparations = %d, want 1 for four concurrent callers", got)
	}
	count := 0
	for cells := range results {
		count++
		if &cells.Records[0] != &shared.Records[0] {
			t.Fatal("a caller received a different records slice than the owner produced")
		}
	}
	if count != 4 {
		t.Fatalf("callers that received a result = %d, want 4", count)
	}

	// Nothing is retained: the next call runs the work again.
	if _, _, err := flight.do(context.Background(), key, work); err != nil {
		t.Fatalf("flight after completion: %v", err)
	}
	callsMu.Lock()
	got = calls
	callsMu.Unlock()
	if got != 2 {
		t.Fatalf("state cell preparations after a completed flight = %d, want 2", got)
	}
}

func TestMasterPrepareFlightDoesNotCacheErrors(t *testing.T) {
	var flight masterPrepareFlight
	key := tnstore.BlockKey(testMasterBlockID(41))
	wantErr := errors.New("prepare failed")

	if _, _, err := flight.do(context.Background(), key, func() (tnstore.StateCellRecords, error) {
		return tnstore.StateCellRecords{}, wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("flight error = %v, want %v", err, wantErr)
	}

	cells, _, err := flight.do(context.Background(), key, func() (tnstore.StateCellRecords, error) {
		return testFlightStateCellRecords(0x22), nil
	})
	if err != nil {
		t.Fatalf("retry after a failed flight: %v", err)
	}
	if cells.Empty() {
		t.Fatal("retry after a failed flight returned no cells")
	}
}

func TestMasterPrepareFlightWaiterRespectsItsOwnContext(t *testing.T) {
	var flight masterPrepareFlight
	key := tnstore.BlockKey(testMasterBlockID(41))

	release := make(chan struct{})
	entered := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _, _ = flight.do(context.Background(), key, func() (tnstore.StateCellRecords, error) {
			close(entered)
			<-release
			return testFlightStateCellRecords(0x33), nil
		})
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("flight work never started")
	}

	// A waiter whose own context dies must not abandon the work for the owner.
	waiterCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := flight.do(waiterCtx, key, func() (tnstore.StateCellRecords, error) {
		t.Fatal("waiter ran the work instead of joining")
		return tnstore.StateCellRecords{}, nil
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled waiter error = %v, want context.Canceled", err)
	}

	close(release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("owner did not finish after a waiter cancelled")
	}
}

// The shared preparation may alias the state-update cells, but never the two
// fields a consumer mutates: the metadata (stamped by the shard apply path) and
// the consensus proof (whose signature-check memoization is written in place).
func TestPrepareVerifiedMasterchainBlockSharedGivesEachConsumerItsOwnMeta(t *testing.T) {
	downloaded := mustLoadFixtureDownloadedBlock(t)
	svc := &SyncCoordinator{log: zerolog.Nop()}
	verified, err := svc.verifyDownloadedBlock(*downloaded)
	if err != nil {
		t.Fatalf("verify fixture block: %v", err)
	}
	first, err := svc.prepareVerifiedMasterchainBlockShared(context.Background(), verified)
	if err != nil {
		t.Fatalf("prepare fixture block: %v", err)
	}
	second, err := svc.prepareVerifiedMasterchainBlockShared(context.Background(), verified)
	if err != nil {
		t.Fatalf("prepare fixture block again: %v", err)
	}

	if first.Meta == second.Meta {
		t.Fatal("consumers share one metadata pointer")
	}
	if first.Meta == verified.Meta || second.Meta == verified.Meta {
		t.Fatal("a consumer aliases the verified block metadata")
	}
	if !first.ID.Equals(&second.ID) {
		t.Fatal("consumers received different blocks")
	}
	if first.StateUpdateToCells.Empty() {
		t.Fatal("prepared block has no state update cells")
	}
}
