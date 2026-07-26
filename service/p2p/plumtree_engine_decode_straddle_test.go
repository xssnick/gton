package p2p

import (
	"bytes"
	"crypto/sha256"
	"runtime"
	"testing"
	"time"

	"github.com/xssnick/raptorq"
	"github.com/xssnick/tonutils-go/adnl/overlay"
)

// The lazy FEC decoder runs DecodeInto+sha256 under state.decodeMu with e.mu
// released (plumtree_engine_payload.go), then re-acquires e.mu to deliver. The
// tests below park a completing decode on state.decodeMu after recheckFECLocked
// has recorded its part but before the decode section re-takes e.mu, then drive
// Close (variant A) or eviction (variant B) to completion while the decode is
// parked. Releasing decodeMu lets the decode finish and re-acquire e.mu, which
// is the exact straddle the e.closed guard and the removeFECStateLocked
// accounting-zeroing guard protect. This is as close to the real "decode
// completes WHILE shutdown/eviction runs" window as the test seams reach:
// there is no hook between DecodeInto and the e.mu re-acquire, so decodeMu is
// used to hold the decode at the entrance of that section.

type plumtreeDecodeStraddleResult struct {
	actions   plumtreeActions
	err       error
	recovered any
}

// plumtreeDecodeStraddleFixture builds an engine holding a FEC broadcast that
// is one source symbol short of decoding, and returns the envelope for the
// completing part. Feeding that envelope drives a full decode.
type plumtreeDecodeStraddleFixture struct {
	engine      *plumtreeEngine
	from        PeerID
	now         time.Time
	broadcastID [sha256.Size]byte
	completing  plumtreeFECEnvelope
	data        []byte
}

func newPlumtreeDecodeStraddleFixture(t *testing.T) plumtreeDecodeStraddleFixture {
	t.Helper()

	now := time.Unix(1_800_002_000, 0)
	from := plumtreeEngineTestPeer(0xc1)
	engine := plumtreeEngineTestNew(
		t,
		plumtreeEngineTestPeer(0xc2),
		false,
		&plumtreeEngineTestPeers{receives: map[PeerID]bool{from: true}},
		&plumtreeEngineTestVerifier{},
		func() time.Time { return now },
	)

	engine.mu.Lock()
	for partIndex := int32(0); partIndex < plumtreeFECDataParts; partIndex++ {
		engine.promoteEagerLocked(
			&engine.slots[partIndex+plumtreeFECTreeOffset],
			from,
			true,
			now,
		)
	}
	engine.mu.Unlock()

	data := make([]byte, 300)
	for index := range data {
		data[index] = byte(index)
	}
	partSize := plumtreeFECPartSize(int32(len(data)))
	encoder, err := raptorq.NewRaptorQ(uint32(partSize)).CreateEncoder(data)
	if err != nil {
		t.Fatalf("create encoder: %v", err)
	}
	fullHash := sha256.Sum256(data)
	broadcastID := computeLocalPlumtreeFECID(0, fullHash, int32(len(data)), partSize)

	newFECMessage := func(partIndex int32) BroadcastPlumtreeFEC {
		return BroadcastPlumtreeFEC{
			Timestamp:    plumtreeUnixSeconds(now),
			Source:       plumtreeEngineTestSource(0xc3),
			Certificate:  overlay.CertificateEmpty{},
			FullDataHash: fullHash[:],
			FullDataSize: int32(len(data)),
			PartIndex:    partIndex,
			TreeIndex:    partIndex + plumtreeFECTreeOffset,
			Data:         encoder.GenSymbol(uint32(partIndex)),
			Signature:    []byte{0xc4},
		}
	}

	// Feed every source symbol but the last: RaptorQ cannot reconstruct 300
	// bytes (K=30 source symbols) from 29 symbols, so none of these deliver and
	// the decoder is retained.
	for partIndex := int32(0); partIndex < plumtreeFECDataParts-1; partIndex++ {
		envelope := plumtreeEngineTestFECEnvelope(t, newFECMessage(partIndex), byte(partIndex))
		actions, handleErr := engine.HandleFEC(t.Context(), now, from, envelope)
		if handleErr != nil {
			t.Fatalf("handle FEC part %d: %v", partIndex, handleErr)
		}
		if len(actions.Deliveries) != 0 {
			t.Fatalf("FEC part %d delivered before the final source symbol", partIndex)
		}
	}

	completing := plumtreeEngineTestFECEnvelope(
		t,
		newFECMessage(plumtreeFECDataParts-1),
		byte(plumtreeFECDataParts-1),
	)

	return plumtreeDecodeStraddleFixture{
		engine:      engine,
		from:        from,
		now:         now,
		broadcastID: broadcastID,
		completing:  completing,
		data:        data,
	}
}

// parkCompletingDecode launches the completing FEC handler and blocks it just
// inside the decode section (on state.decodeMu, which the caller must already
// hold) after recheckFECLocked has recorded the part and released e.mu. It
// returns a channel that yields the handler's result once decodeMu is released.
func (f plumtreeDecodeStraddleFixture) parkCompletingDecode(t *testing.T) (*plumtreeFECState, chan plumtreeDecodeStraddleResult) {
	t.Helper()

	f.engine.mu.Lock()
	state := f.engine.fecStates[f.broadcastID]
	f.engine.mu.Unlock()
	if state == nil {
		t.Fatal("FEC state missing before the completing decode")
	}

	// Hold decodeMu so the completing handler parks at the top of the decode
	// section (payload.go, state.decodeMu.Lock()).
	state.decodeMu.Lock()

	done := make(chan plumtreeDecodeStraddleResult, 1)
	go func() {
		var res plumtreeDecodeStraddleResult
		defer func() {
			res.recovered = recover()
			done <- res
		}()
		res.actions, res.err = f.engine.HandleFEC(t.Context(), f.now, f.from, f.completing)
	}()

	// Wait until the parked handler has passed recheckFECLocked (part recorded,
	// wire bytes reserved, e.mu released) and is therefore blocked on decodeMu.
	plumtreeWaitForFECPartRecorded(t, f.engine, f.broadcastID, plumtreeFECDataParts-1)
	return state, done
}

func plumtreeWaitForFECPartRecorded(
	t *testing.T,
	engine *plumtreeEngine,
	broadcastID [sha256.Size]byte,
	partIndex int32,
) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		engine.mu.Lock()
		state := engine.fecStates[broadcastID]
		recorded := state != nil && state.parts[partIndex] != nil
		engine.mu.Unlock()
		if recorded {
			return
		}
		runtime.Gosched()
	}
	t.Fatal("completing FEC part was not recorded before the decode section")
}

// TestPlumtreeEngineCloseWinsConcurrentFECDecode guards the e.closed re-check
// after DecodeInto (plumtree_engine_payload.go). A decode that straddles Close
// must observe e.closed and skip delivery+releaseFECDecoderLocked, so it never
// touches the zeroed e.delivered map or double-releases budget.
func TestPlumtreeEngineCloseWinsConcurrentFECDecode(t *testing.T) {
	fixture := newPlumtreeDecodeStraddleFixture(t)
	state, done := fixture.parkCompletingDecode(t)

	// Close runs to completion while the decode is parked: it sets e.closed,
	// evicts every state and nils e.delivered.
	fixture.engine.Close()
	plumtreeMemoryBudgetRequireCounters(t, fixture.engine.budget, 0, 0)

	// Let the parked decode finish and re-acquire e.mu.
	state.decodeMu.Unlock()

	result := <-done
	if result.recovered != nil {
		t.Fatalf("post-close FEC decode panicked: %v", result.recovered)
	}
	if result.err != nil {
		t.Fatalf("post-close FEC decode returned error: %v", result.err)
	}
	if len(result.actions.Deliveries) != 0 {
		t.Fatalf("post-close FEC decode emitted %d deliveries, want 0", len(result.actions.Deliveries))
	}
	plumtreeMemoryBudgetRequireCounters(t, fixture.engine.budget, 0, 0)
}

// TestPlumtreeEngineEvictionWinsConcurrentFECDecode guards the accounting
// zeroing in removeFECStateLocked (plumtree_engine_repair.go). Eviction and a
// straddling decode both reach releaseFECDecoderLocked; without the zeroing the
// decoder estimate would be released twice and budget.release would panic on
// underflow.
func TestPlumtreeEngineEvictionWinsConcurrentFECDecode(t *testing.T) {
	fixture := newPlumtreeDecodeStraddleFixture(t)
	state, done := fixture.parkCompletingDecode(t)

	// Age the alarm past the broadcast lifetime so it evicts the FEC state,
	// releasing its full budget and zeroing the retained/decoder accounting.
	agedNow := fixture.now.Add(plumtreeBroadcastLifetime + time.Second)
	fixture.engine.Alarm(agedNow)
	plumtreeMemoryBudgetRequireCounters(t, fixture.engine.budget, 0, 0)

	// Let the parked decode finish; its releaseFECDecoderLocked must be a no-op
	// on the zeroed accounting rather than a double release.
	state.decodeMu.Unlock()

	result := <-done
	if result.recovered != nil {
		t.Fatalf("post-eviction FEC decode panicked (double budget release): %v", result.recovered)
	}
	if result.err != nil {
		t.Fatalf("post-eviction FEC decode returned error: %v", result.err)
	}
	if len(result.actions.Deliveries) == 1 &&
		!bytes.Equal(result.actions.Deliveries[0].Data, fixture.data) {
		t.Fatalf("post-eviction FEC decode delivered corrupt data")
	}
	plumtreeMemoryBudgetRequireCounters(t, fixture.engine.budget, 0, 0)
}
