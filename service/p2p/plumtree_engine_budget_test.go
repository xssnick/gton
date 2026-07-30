package p2p

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/xssnick/raptorq"
	"github.com/xssnick/tonutils-go/adnl/overlay"
)

type plumtreeMemoryBudgetCounters struct {
	states uint64
	bytes  uint64
}

type plumtreeMemoryBudgetCapacityTestCase struct {
	name          string
	reserveStates uint64
	reserveBytes  uint64
}

type plumtreeFECDecoderBufferEstimateTestCase struct {
	name         string
	fullDataSize int32
	partSize     int32
	want         uint64
}

func TestPlumtreeMemoryBudgetReserveReleaseExact(t *testing.T) {
	t.Parallel()

	budget := &plumtreeMemoryBudget{}
	maxStates := plumtreeMaxActiveStates
	maxBytes := plumtreeMaxActiveBytes

	if !budget.reserve(1, 17) {
		t.Fatal("initial reservation was rejected")
	}
	if !budget.reserve(maxStates-1, maxBytes-17) {
		t.Fatal("remaining capacity reservation was rejected")
	}
	if budget.reserve(1, 0) {
		t.Fatal("state capacity overflow was accepted")
	}
	if budget.reserve(0, 1) {
		t.Fatal("byte capacity overflow was accepted")
	}
	plumtreeMemoryBudgetRequireCounters(t, budget, maxStates, maxBytes)

	budget.release(maxStates-1, maxBytes-17)
	plumtreeMemoryBudgetRequireCounters(t, budget, 1, 17)
	budget.release(1, 17)
	plumtreeMemoryBudgetRequireCounters(t, budget, 0, 0)
}

func TestPlumtreeFECDecoderBufferEstimateFormula(t *testing.T) {
	t.Parallel()

	tests := []plumtreeFECDecoderBufferEstimateTestCase{
		{
			name:         "one data part",
			fullDataSize: 1,
			partSize:     1,
			want:         4188,
		},
		{
			name:         "thirty data parts",
			fullDataSize: 300,
			partSize:     10,
			want:         5596,
		},
		{
			name:         "partial tail",
			fullDataSize: 31,
			partSize:     2,
			want:         4340,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got := plumtreeFECDecoderBufferEstimate(
				test.fullDataSize,
				test.partSize,
			)
			if got != test.want {
				t.Fatalf("decoder buffer estimate = %d, want %d", got, test.want)
			}
		})
	}
}

func TestPlumtreeMemoryBudgetRejectsWithoutEviction(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_001_000, 0)
	from := plumtreeEngineTestPeer(0xd1)
	peers := &plumtreeEngineTestPeers{
		receives: map[PeerID]bool{from: true},
	}

	tests := []plumtreeMemoryBudgetCapacityTestCase{
		{
			name:          "states",
			reserveStates: plumtreeMaxActiveStates,
		},
		{
			name:         "bytes",
			reserveBytes: plumtreeMaxActiveBytes,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			budget := &plumtreeMemoryBudget{}
			if !budget.reserve(test.reserveStates, test.reserveBytes) {
				t.Fatal("prefill reservation was rejected")
			}
			engine, err := newPlumtreeEngine(
				plumtreeEngineConfig{
					LocalID: plumtreeEngineTestPeer(0xd2),
					Now:     func() time.Time { return now },
				},
				budget,
				peers,
				&plumtreeEngineTestVerifier{},
				&plumtreeStats{},
			)
			if err != nil {
				t.Fatalf("create engine: %v", err)
			}
			engine.mu.Lock()
			engine.promoteEagerLocked(
				&engine.slots[plumtreeSimpleTree],
				from,
				true,
				now,
			)
			engine.mu.Unlock()

			message := plumtreeEngineTestSimple(
				now,
				plumtreeEngineTestID(0xd3),
				plumtreeEngineTestSource(0xd4),
				[]byte("capacity rejection"),
			)
			envelope := plumtreeEngineTestSimpleEnvelope(t, message, 0xd5)
			_, err = engine.HandleSimple(
				t.Context(),
				now,
				from,
				envelope,
			)
			if !errors.Is(err, errPlumtreeMemoryCapacity) {
				t.Fatalf("capacity result = %v", err)
			}
			if len(engine.simpleStates) != 0 {
				t.Fatal("capacity-rejected state was retained")
			}
			plumtreeMemoryBudgetRequireCounters(
				t,
				budget,
				test.reserveStates,
				test.reserveBytes,
			)

			budget.release(test.reserveStates, test.reserveBytes)
		})
	}
}

func TestPlumtreeMemoryBudgetCountsWireOnceAndReleasesOnGC(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_001_100, 0)
	from := plumtreeEngineTestPeer(0xe1)
	engine := plumtreeEngineTestNew(
		t,
		plumtreeEngineTestPeer(0xe2),
		false,
		&plumtreeEngineTestPeers{
			receives: map[PeerID]bool{from: true},
		},
		&plumtreeEngineTestVerifier{},
		func() time.Time { return now },
	)
	engine.mu.Lock()
	engine.promoteEagerLocked(
		&engine.slots[plumtreeSimpleTree],
		from,
		true,
		now,
	)
	engine.mu.Unlock()

	message := plumtreeEngineTestSimple(
		now,
		plumtreeEngineTestID(0xe3),
		plumtreeEngineTestSource(0xe4),
		[]byte("one retained wire"),
	)
	envelope := plumtreeEngineTestSimpleEnvelope(t, message, 0xe5)
	if len(envelope.Wire.Body) == 0 ||
		&envelope.Wire.Body[0] != &envelope.Wire.Message[1] {
		t.Fatal("test envelope body does not alias its retained message")
	}
	if !plumtreeMemoryBudgetWireContainsView(
		envelope.Wire.Message,
		envelope.Message.Data,
	) {
		t.Fatal("decoded payload does not alias its retained message")
	}

	if _, err := engine.HandleSimple(
		t.Context(),
		now,
		from,
		envelope,
	); err != nil {
		t.Fatalf("handle simple payload: %v", err)
	}
	wantBytes := uint64(len(envelope.Wire.Message))
	plumtreeMemoryBudgetRequireCounters(t, engine.budget, 1, wantBytes)

	if _, err := engine.HandleSimple(
		t.Context(),
		now,
		from,
		envelope,
	); err == nil {
		t.Fatal("duplicate simple payload was accepted")
	}
	plumtreeMemoryBudgetRequireCounters(t, engine.budget, 1, wantBytes)

	engine.Alarm(now.Add(plumtreeBroadcastLifetime))
	plumtreeMemoryBudgetRequireCounters(t, engine.budget, 0, 0)
}

func TestPlumtreeMemoryBudgetIsSharedAcrossEngines(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_001_150, 0)
	from := plumtreeEngineTestPeer(0xe6)
	peers := &plumtreeEngineTestPeers{
		receives: map[PeerID]bool{from: true},
	}
	budget := &plumtreeMemoryBudget{}
	first, err := newPlumtreeEngine(
		plumtreeEngineConfig{
			LocalID: plumtreeEngineTestPeer(0xe7),
			Now:     func() time.Time { return now },
		},
		budget,
		peers,
		&plumtreeEngineTestVerifier{},
		&plumtreeStats{},
	)
	if err != nil {
		t.Fatalf("create first engine: %v", err)
	}
	second, err := newPlumtreeEngine(
		plumtreeEngineConfig{
			LocalID: plumtreeEngineTestPeer(0xe8),
			Now:     func() time.Time { return now },
		},
		budget,
		peers,
		&plumtreeEngineTestVerifier{},
		&plumtreeStats{},
	)
	if err != nil {
		t.Fatalf("create second engine: %v", err)
	}

	engines := []*plumtreeEngine{first, second}
	var retainedByEngine [2]uint64
	var retainedBytes uint64
	for index, engine := range engines {
		engine.mu.Lock()
		engine.promoteEagerLocked(
			&engine.slots[plumtreeSimpleTree],
			from,
			true,
			now,
		)
		engine.mu.Unlock()

		message := plumtreeEngineTestSimple(
			now,
			plumtreeEngineTestID(uint32(0xe9+index)),
			plumtreeEngineTestSource(byte(0xea+index)),
			[]byte("node-global budget"),
		)
		envelope := plumtreeEngineTestSimpleEnvelope(
			t,
			message,
			byte(0xeb+index),
		)
		if _, handleErr := engine.HandleSimple(
			t.Context(),
			now,
			from,
			envelope,
		); handleErr != nil {
			t.Fatalf("engine %d handle simple: %v", index, handleErr)
		}
		retainedByEngine[index] = uint64(len(envelope.Wire.Message))
		retainedBytes += retainedByEngine[index]
	}
	plumtreeMemoryBudgetRequireCounters(t, budget, 2, retainedBytes)

	first.Close()
	if first.simpleOldest != nil {
		t.Fatal("first closed engine retained its state list")
	}
	plumtreeMemoryBudgetRequireCounters(t, budget, 1, retainedByEngine[1])

	second.Close()
	plumtreeMemoryBudgetRequireCounters(t, budget, 0, 0)
}

func TestPlumtreeRuntimeUsesNodeMemoryBudget(t *testing.T) {
	t.Parallel()

	node := newTestNode(t)
	for index := range 2 {
		overlayID := plumtreeEngineTestID(uint32(0xec + index))
		sub, err := node.newOverlaySubscription(overlaySpec{
			Name:      "budget-runtime",
			Kind:      overlayKindPublicShard,
			Workchain: 0,
			ShortID:   overlayID[:],
		})
		if err != nil {
			t.Fatalf("create subscription %d: %v", index, err)
		}
		t.Cleanup(sub.close)

		if sub.plumtree.engine.budget != node.plumtreeBudget {
			t.Fatalf("subscription %d does not use the node budget", index)
		}
	}
}

func TestPlumtreeMemoryBudgetReleasesRemoteFECDecoderAtTerminal(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_001_200, 0)
	from := plumtreeEngineTestPeer(0xf1)
	engine := plumtreeEngineTestNew(
		t,
		plumtreeEngineTestPeer(0xf2),
		false,
		&plumtreeEngineTestPeers{
			receives: map[PeerID]bool{from: true},
		},
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
	broadcastID := computeLocalPlumtreeFECID(
		0,
		fullHash,
		int32(len(data)),
		partSize,
	)
	decoderEstimatedBytes := plumtreeFECDecoderBufferEstimate(
		int32(len(data)),
		partSize,
	)

	var retainedWireBytes uint64
	var delivered []byte
	for partIndex := int32(0); partIndex < plumtreeFECDataParts; partIndex++ {
		message := BroadcastPlumtreeFEC{
			Timestamp:    plumtreeUnixSeconds(now),
			Source:       plumtreeEngineTestSource(0xf3),
			Certificate:  overlay.CertificateEmpty{},
			FullDataHash: fullHash[:],
			FullDataSize: int32(len(data)),
			PartIndex:    partIndex,
			TreeIndex:    partIndex + plumtreeFECTreeOffset,
			Data:         encoder.GenSymbol(uint32(partIndex)),
			Signature:    bytes.Repeat([]byte{0xf4}, ed25519.SignatureSize),
		}
		envelope := plumtreeEngineTestFECEnvelope(
			t,
			message,
			byte(partIndex),
		)
		retainedWireBytes += uint64(len(envelope.Wire.Message))

		actions, handleErr := engine.HandleFEC(
			t.Context(),
			now,
			from,
			envelope,
		)
		if handleErr != nil {
			t.Fatalf("handle FEC part %d: %v", partIndex, handleErr)
		}
		if partIndex == 0 {
			plumtreeMemoryBudgetRequireCounters(
				t,
				engine.budget,
				1,
				retainedWireBytes+decoderEstimatedBytes,
			)
			if _, duplicateErr := engine.HandleFEC(
				t.Context(),
				now,
				from,
				envelope,
			); duplicateErr == nil {
				t.Fatal("duplicate FEC part was accepted")
			}
			plumtreeMemoryBudgetRequireCounters(
				t,
				engine.budget,
				1,
				retainedWireBytes+decoderEstimatedBytes,
			)
		}
		if len(actions.Deliveries) == 1 {
			delivered = actions.Deliveries[0].Data
			break
		}
	}

	if !bytes.Equal(delivered, data) {
		t.Fatal("remote FEC payload was not delivered")
	}
	plumtreeMemoryBudgetRequireCounters(
		t,
		engine.budget,
		1,
		retainedWireBytes,
	)

	engine.mu.Lock()
	state := engine.fecStates[broadcastID]
	if state == nil {
		engine.mu.Unlock()
		t.Fatal("terminal FEC state is missing")
	}
	if state.decoder != nil ||
		state.decodeBuffer != nil ||
		state.decoderEstimatedBytes != 0 ||
		state.retainedWireBytes != retainedWireBytes {
		engine.mu.Unlock()
		t.Fatalf("unexpected terminal FEC accounting: %+v", state)
	}
	engine.mu.Unlock()

	engine.Close()
	plumtreeMemoryBudgetRequireCounters(t, engine.budget, 0, 0)
}

func TestPlumtreeMemoryBudgetCloseReleasesState(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_001_300, 0)
	from := plumtreeEngineTestPeer(0xa1)
	engine := plumtreeEngineTestNew(
		t,
		plumtreeEngineTestPeer(0xa2),
		false,
		&plumtreeEngineTestPeers{
			receives: map[PeerID]bool{from: true},
		},
		&plumtreeEngineTestVerifier{},
		func() time.Time { return now },
	)
	engine.mu.Lock()
	engine.promoteEagerLocked(
		&engine.slots[plumtreeFECTreeOffset],
		from,
		true,
		now,
	)
	engine.mu.Unlock()

	data := bytes.Repeat([]byte{0xa3}, 300)
	partSize := plumtreeFECPartSize(int32(len(data)))
	encoder, err := raptorq.NewRaptorQ(uint32(partSize)).CreateEncoder(data)
	if err != nil {
		t.Fatalf("create encoder: %v", err)
	}
	fullHash := sha256.Sum256(data)
	message := BroadcastPlumtreeFEC{
		Timestamp:    plumtreeUnixSeconds(now),
		Source:       plumtreeEngineTestSource(0xa4),
		Certificate:  overlay.CertificateEmpty{},
		FullDataHash: fullHash[:],
		FullDataSize: int32(len(data)),
		PartIndex:    0,
		TreeIndex:    plumtreeFECTreeOffset,
		Data:         encoder.GenSymbol(0),
		Signature:    bytes.Repeat([]byte{0xa5}, ed25519.SignatureSize),
	}
	envelope := plumtreeEngineTestFECEnvelope(t, message, 0xa6)
	if _, err = engine.HandleFEC(
		t.Context(),
		now,
		from,
		envelope,
	); err != nil {
		t.Fatalf("handle FEC payload: %v", err)
	}
	plumtreeMemoryBudgetRequireCounters(
		t,
		engine.budget,
		1,
		uint64(len(envelope.Wire.Message))+
			plumtreeFECDecoderBufferEstimate(int32(len(data)), partSize),
	)

	overlayID := plumtreeEngineTestID(0xa7)
	receiver, err := overlay.NewBroadcastReceiver(
		overlayID[:],
		maxOverlayPayloadSize,
		true,
		false,
	)
	if err != nil {
		t.Fatalf("create broadcast receiver: %v", err)
	}
	sub := &overlaySubscription{
		peers:             map[PeerID]*overlayPeer{},
		peerNotify:        make(chan struct{}, 1),
		broadcastReceiver: receiver,
		plumtree: &plumtreeRuntime{
			engine: engine,
		},
	}
	sub.close()
	engine.Close()
	plumtreeMemoryBudgetRequireCounters(t, engine.budget, 0, 0)
	if len(engine.simpleStates) != 0 || len(engine.fecStates) != 0 {
		t.Fatal("closed engine retained broadcast states")
	}
}

func TestPlumtreeEngineCloseWinsConcurrentVerification(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_001_400, 0)
	from := plumtreeEngineTestPeer(0xb1)
	release := make(chan struct{})
	verifier := &plumtreeEngineTestVerifier{
		entered: make(chan struct{}, 1),
		release: release,
	}
	engine := plumtreeEngineTestNew(
		t,
		plumtreeEngineTestPeer(0xb2),
		false,
		&plumtreeEngineTestPeers{
			receives: map[PeerID]bool{from: true},
		},
		verifier,
		func() time.Time { return now },
	)
	engine.mu.Lock()
	engine.promoteEagerLocked(
		&engine.slots[plumtreeSimpleTree],
		from,
		true,
		now,
	)
	engine.mu.Unlock()

	message := plumtreeEngineTestSimple(
		now,
		plumtreeEngineTestID(0xb3),
		plumtreeEngineTestSource(0xb4),
		[]byte("verification finishes after close"),
	)
	envelope := plumtreeEngineTestSimpleEnvelope(t, message, 0xb5)
	result := make(chan error, 1)
	go func() {
		_, err := engine.HandleSimple(
			t.Context(),
			now,
			from,
			envelope,
		)
		result <- err
	}()

	<-verifier.entered
	engine.Close()
	close(release)

	if err := <-result; !errors.Is(err, errPlumtreeClosed) {
		t.Fatalf("post-close verification result = %v", err)
	}
	plumtreeMemoryBudgetRequireCounters(t, engine.budget, 0, 0)
	if len(engine.simpleStates) != 0 {
		t.Fatal("verification committed state after close")
	}
}

func BenchmarkPlumtreeMemoryBudgetAccounting(b *testing.B) {
	budget := &plumtreeMemoryBudget{}

	b.ReportAllocs()
	for b.Loop() {
		if !budget.reserve(1, 1) {
			b.Fatal("reserve")
		}
		budget.release(1, 1)
	}
}

func plumtreeMemoryBudgetSnapshot(
	budget *plumtreeMemoryBudget,
) plumtreeMemoryBudgetCounters {
	budget.mu.Lock()
	defer budget.mu.Unlock()

	return plumtreeMemoryBudgetCounters{
		states: budget.activeStates,
		bytes:  budget.activeBytes,
	}
}

func plumtreeMemoryBudgetRequireCounters(
	t testing.TB,
	budget *plumtreeMemoryBudget,
	states uint64,
	bytes uint64,
) {
	t.Helper()

	counters := plumtreeMemoryBudgetSnapshot(budget)
	if counters.states != states || counters.bytes != bytes {
		t.Fatalf(
			"Plumtree memory counters = states %d bytes %d, want %d/%d",
			counters.states,
			counters.bytes,
			states,
			bytes,
		)
	}
}

func plumtreeMemoryBudgetWireContainsView(owner []byte, view []byte) bool {
	if len(view) == 0 || len(view) > len(owner) {
		return false
	}

	for index := 0; index <= len(owner)-len(view); index++ {
		if &owner[index] == &view[0] {
			return true
		}
	}
	return false
}
