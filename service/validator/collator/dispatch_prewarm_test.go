package collator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/msgpool"
)

func TestDispatchPrewarmUsesCanonicalPhaseZeroWithoutTracingOrMutation(t *testing.T) {
	priority := benchmarkDispatchAccount(3)
	first := benchmarkDispatchAccount(1)
	second := benchmarkDispatchAccount(2)
	queue := makeDispatchQueue(t,
		dispatchFixtureAccount{accountID: priority, lts: []uint64{300, 301}},
		dispatchFixtureAccount{accountID: second, lts: []uint64{200}},
		dispatchFixtureAccount{accountID: first, lts: []uint64{100}},
	)
	queueHash := queue.RootCell().HashKey()
	state := previousStateWithDispatchQueue(t, emptyCandidateRequest(t).Previous.State, queue)
	readSet := cell.NewReadSet(state)

	warmer := &recordedAccountPrewarmer{}
	acquisition := &LocalAcquisition{
		accountPrewarmer: warmer,
		dispatch: DispatchPolicy{PriorityList: []DispatchAccount{{
			Workchain: 0,
			AccountID: priority,
		}}},
	}
	shard := msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll}
	if err := acquisition.prewarmDispatchFront(t.Context(), shard, readSet.Root()); err != nil {
		t.Fatal(err)
	}

	wantSources := [][32]byte{priority, first, second}
	if len(warmer.accounts) != len(wantSources) {
		t.Fatalf("prewarmed destinations = %d, want %d", len(warmer.accounts), len(wantSources))
	}
	if len(warmer.roots) != len(wantSources) {
		t.Fatalf("prewarmed envelopes = %d, want %d", len(warmer.roots), len(wantSources))
	}
	for i, source := range wantSources {
		wantDestination := dispatchFixtureDestination(source)
		if warmer.accounts[i] != (prewarmAccountKey{workchain: 0, account: wantDestination}) {
			t.Fatalf("prewarmed destination %d = %+v, want %x", i, warmer.accounts[i], wantDestination)
		}
		lt := []uint64{300, 100, 200}[i]
		if warmer.roots[i] != dispatchFixtureEnvelopeRoot(t, queue, source, lt) {
			t.Fatalf("prewarmed envelope %d differs from canonical phase-zero message", i)
		}
	}
	if readSet.Size() != 0 {
		t.Fatalf("dispatch prewarm added %d cells to the candidate proof trace", readSet.Size())
	}
	if queue.RootCell().HashKey() != queueHash {
		t.Fatal("dispatch prewarm mutated the source queue root")
	}
	assertDispatchLTs(t, queue, priority, []uint64{300, 301})
	assertDispatchLTs(t, queue, first, []uint64{100})
	assertDispatchLTs(t, queue, second, []uint64{200})
}

func TestDispatchPrewarmBoundsPhaseZeroAtFiveHundredTwelve(t *testing.T) {
	fixtures := make([]dispatchFixtureAccount, dispatchPrewarmFrontLimit+1)
	for i := range fixtures {
		fixtures[i] = dispatchFixtureAccount{
			accountID: benchmarkDispatchAccount(i),
			lts:       []uint64{uint64(i + 1)},
		}
	}
	queue := makeDispatchQueue(t, fixtures...)
	state := previousStateWithDispatchQueue(t, emptyCandidateRequest(t).Previous.State, queue)
	warmer := &recordedAccountPrewarmer{}
	acquisition := &LocalAcquisition{accountPrewarmer: warmer}

	if err := acquisition.prewarmDispatchFront(t.Context(), msgpool.ShardIdent{
		Workchain: 0,
		Shard:     msgpool.ShardAll,
	}, state); err != nil {
		t.Fatal(err)
	}

	if len(warmer.accounts) != dispatchPrewarmFrontLimit {
		t.Fatalf("prewarmed destinations = %d, want %d", len(warmer.accounts), dispatchPrewarmFrontLimit)
	}
	if len(warmer.roots) != dispatchPrewarmFrontLimit {
		t.Fatalf("prewarmed envelope roots = %d, want %d", len(warmer.roots), dispatchPrewarmFrontLimit)
	}
	for i := range dispatchPrewarmFrontLimit {
		want := dispatchFixtureDestination(benchmarkDispatchAccount(i))
		if warmer.accounts[i].account != want {
			t.Fatalf("prewarmed destination %d = %x, want %x", i, warmer.accounts[i].account, want)
		}
	}
}

func TestDispatchPrewarmUsesRewrittenLocalDestinationAndAlwaysWarmsEnvelope(t *testing.T) {
	localSource := benchmarkDispatchAccount(1)
	duplicateSource := benchmarkDispatchAccount(5)
	remoteSource := benchmarkDispatchAccount(2)
	variableSource := benchmarkDispatchAccount(3)
	rawLocal := [32]byte{0x44, 0x55}
	anycast := address.NewAddress(0, 0, rawLocal[:]).WithAnycast(address.NewAnycast(13, []byte{0xab, 0xc8}))
	remoteAccount := benchmarkDispatchAccount(4)
	remote := address.NewAddress(0, 1, remoteAccount[:])
	variableData := make([]byte, 38)
	variableData[0] = 0x77
	variable := address.NewAddressVar(0, 0x2000, 300, variableData)

	queue := dispatchPrewarmQueue(t,
		dispatchPrewarmFixture{source: localSource, destination: anycast, lt: 100},
		dispatchPrewarmFixture{source: duplicateSource, destination: anycast, lt: 150},
		dispatchPrewarmFixture{source: remoteSource, destination: remote, lt: 200},
		dispatchPrewarmFixture{source: variableSource, destination: variable, lt: 300},
	)
	state := previousStateWithDispatchQueue(t, emptyCandidateRequest(t).Previous.State, queue)
	warmer := &recordedAccountPrewarmer{}
	acquisition := &LocalAcquisition{accountPrewarmer: warmer}

	if err := acquisition.prewarmDispatchFront(t.Context(), msgpool.ShardIdent{
		Workchain: 0,
		Shard:     msgpool.ShardAll,
	}, state); err != nil {
		t.Fatal(err)
	}

	wantLocal := rawLocal
	if err := msgpool.RewriteAnycast(wantLocal[:], anycast); err != nil {
		t.Fatal(err)
	}
	if len(warmer.accounts) != 1 || warmer.accounts[0] != (prewarmAccountKey{account: wantLocal}) {
		t.Fatalf("prewarmed accounts = %+v, want only rewritten local destination %x", warmer.accounts, wantLocal)
	}
	if warmer.accounts[0].account == localSource {
		t.Fatal("dispatch outer sender key was prewarmed as a destination")
	}
	if len(warmer.roots) != 4 {
		t.Fatalf("prewarmed envelope roots = %d, want both local, remote, and variable envelopes", len(warmer.roots))
	}
}

func TestDispatchPrewarmSchedulerKeepsLatestStatePerShard(t *testing.T) {
	shard := msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll}
	cancels := 0
	acquisition := &LocalAcquisition{accountPrewarmer: &recordedAccountPrewarmer{}}
	acquisition.dispatchPrewarm = dispatchPrewarmScheduler{
		running:   true,
		hasActive: true,
		active: dispatchPrewarmSource{
			deadline: time.Now().Add(time.Hour),
			shard:    shard,
			seqno:    10,
			key:      dispatchPrewarmTestKey(10),
		},
		activeCancel: func() { cancels++ },
	}

	if acquisition.enqueueDispatchPrewarm(dispatchPrewarmSource{
		shard: shard,
		seqno: 9,
		key:   dispatchPrewarmTestKey(9),
	}) {
		t.Fatal("older state displaced the active dispatch scan")
	}
	if !acquisition.enqueueDispatchPrewarm(dispatchPrewarmSource{
		shard: shard,
		seqno: 11,
		key:   dispatchPrewarmTestKey(11),
	}) {
		t.Fatal("newer state was not queued")
	}
	if !acquisition.enqueueDispatchPrewarm(dispatchPrewarmSource{
		shard: shard,
		seqno: 12,
		key:   dispatchPrewarmTestKey(12),
	}) {
		t.Fatal("newest state did not replace the pending state")
	}
	if acquisition.enqueueDispatchPrewarm(dispatchPrewarmSource{
		shard: shard,
		seqno: 12,
		key:   dispatchPrewarmTestKey(12),
	}) {
		t.Fatal("duplicate pending state was queued")
	}

	acquisition.dispatchPrewarm.mu.Lock()
	pending := append([]dispatchPrewarmSource(nil), acquisition.dispatchPrewarm.pending...)
	acquisition.dispatchPrewarm.mu.Unlock()
	if cancels != 2 {
		t.Fatalf("active scan cancellations = %d, want 2 newer states", cancels)
	}
	if len(pending) != 1 || pending[0].seqno != 12 || pending[0].key != dispatchPrewarmTestKey(12) {
		t.Fatalf("pending dispatch states = %+v, want only seqno 12", pending)
	}
}

func TestDispatchPrewarmSchedulerKeepsActiveScanWhenPendingCapacityIsFull(t *testing.T) {
	activeShard := msgpool.ShardIdent{Workchain: 0, Shard: 1000}
	cancels := 0
	pending := make([]dispatchPrewarmSource, maxPendingDispatchPrewarms)
	for i := range pending {
		pending[i] = dispatchPrewarmSource{
			deadline: time.Now().Add(time.Hour),
			shard:    msgpool.ShardIdent{Workchain: 0, Shard: uint64(i + 1)},
			seqno:    10,
			key:      dispatchPrewarmTestKey(byte(i + 1)),
		}
	}
	acquisition := &LocalAcquisition{accountPrewarmer: &recordedAccountPrewarmer{}}
	acquisition.dispatchPrewarm = dispatchPrewarmScheduler{
		running:   true,
		hasActive: true,
		active: dispatchPrewarmSource{
			deadline: time.Now().Add(time.Hour),
			shard:    activeShard,
			seqno:    10,
			key:      dispatchPrewarmTestKey(10),
		},
		activeCancel: func() { cancels++ },
		pending:      pending,
	}

	if acquisition.enqueueDispatchPrewarm(dispatchPrewarmSource{
		shard: activeShard,
		seqno: 11,
		key:   dispatchPrewarmTestKey(11),
	}) {
		t.Fatal("dispatch prewarm was queued past the pending capacity")
	}
	if cancels != 0 {
		t.Fatalf("active scan was cancelled %d times even though its replacement was rejected", cancels)
	}
}

func TestDispatchPrewarmRetiredSessionCancelsActiveAndPurgesPending(t *testing.T) {
	retired := [32]byte{0x11}
	kept := [32]byte{0x22}
	retiredOwner := dispatchPrewarmOwner{session: retired, generation: 1}
	keptOwner := dispatchPrewarmOwner{session: kept, generation: 1}
	activeShard := msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll}
	activeKey := dispatchPrewarmTestKey(10)
	activeKey.owner = retiredOwner
	cancels := 0
	acquisition := &LocalAcquisition{}
	acquisition.dispatchPrewarm = dispatchPrewarmScheduler{
		running:   true,
		hasActive: true,
		active: dispatchPrewarmSource{
			owner: retiredOwner,
			shard: activeShard,
			seqno: 10,
			key:   activeKey,
		},
		activeCancel: func() {
			cancels++
		},
		pending: []dispatchPrewarmSource{
			{owner: retiredOwner, state: cell.BeginCell().MustStoreUInt(1, 1).EndCell()},
			{owner: keptOwner, state: cell.BeginCell().MustStoreUInt(0, 1).EndCell()},
			{owner: retiredOwner, state: cell.BeginCell().MustStoreUInt(3, 2).EndCell()},
		},
	}

	acquisition.cancelSessionDispatchPrewarms(retiredOwner)

	if cancels != 1 {
		t.Fatalf("active retired-session cancellations = %d, want 1", cancels)
	}
	if acquisition.dispatchPrewarm.hasActive || acquisition.dispatchPrewarm.activeCancel != nil ||
		acquisition.dispatchPrewarm.active.owner != (dispatchPrewarmOwner{}) {
		t.Fatal("cancelled retired-session scan remained registered as active")
	}
	if len(acquisition.dispatchPrewarm.pending) != 1 ||
		acquisition.dispatchPrewarm.pending[0].owner != keptOwner {
		t.Fatalf("pending dispatch prewarms = %+v, want only live session", acquisition.dispatchPrewarm.pending)
	}
	if cap(acquisition.dispatchPrewarm.pending) > len(acquisition.dispatchPrewarm.pending) {
		stale := acquisition.dispatchPrewarm.pending[:cap(acquisition.dispatchPrewarm.pending)]
		for i := len(acquisition.dispatchPrewarm.pending); i < len(stale); i++ {
			if stale[i].state != nil {
				t.Fatalf("purged dispatch prewarm %d retained a state root", i)
			}
		}
	}
	reusedOwner := dispatchPrewarmOwner{session: retired, generation: 2}
	reusedKey := activeKey
	reusedKey.owner = reusedOwner
	if !acquisition.enqueueDispatchPrewarm(dispatchPrewarmSource{
		owner: reusedOwner,
		ctx:   context.Background(),
		shard: activeShard,
		seqno: 10,
		key:   reusedKey,
	}) {
		t.Fatal("reused session ID was deduplicated against its cancelled active scan")
	}
}

func TestDispatchPrewarmExpiredTaskDoesNoWorkAndReleasesActiveState(t *testing.T) {
	retained := cell.BeginCell().MustStoreUInt(1, 1).EndCell()
	acquisition := &LocalAcquisition{
		accountPrewarmer: &recordedAccountPrewarmer{},
		dispatchPrewarm: dispatchPrewarmScheduler{
			running: true,
			pending: []dispatchPrewarmSource{{
				kind:     dispatchPrewarmSourceState,
				ctx:      context.Background(),
				deadline: time.Now().Add(-time.Second),
				state:    retained,
			}},
		},
	}

	acquisition.runDispatchPrewarms()

	if acquisition.dispatchPrewarm.running || acquisition.dispatchPrewarm.hasActive ||
		acquisition.dispatchPrewarm.active.state != nil || acquisition.dispatchPrewarm.active.ctx != nil ||
		len(acquisition.dispatchPrewarm.pending) != 0 {
		t.Fatalf("completed scheduler retained state: %+v", acquisition.dispatchPrewarm)
	}
}

func TestDispatchPrewarmNewSessionReplacesSameShardTask(t *testing.T) {
	oldSession := [32]byte{0x11}
	newSession := [32]byte{0x22}
	oldOwner := dispatchPrewarmOwner{session: oldSession, generation: 1}
	newOwner := dispatchPrewarmOwner{session: newSession, generation: 1}
	shard := msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll}
	oldKey := dispatchPrewarmTestKey(10)
	oldKey.owner = oldOwner
	newKey := dispatchPrewarmTestKey(10)
	newKey.owner = newOwner
	acquisition := &LocalAcquisition{dispatchPrewarm: dispatchPrewarmScheduler{
		running: true,
		pending: []dispatchPrewarmSource{{
			owner:    oldOwner,
			ctx:      context.Background(),
			deadline: time.Now().Add(time.Hour),
			shard:    shard,
			seqno:    10,
			key:      oldKey,
		}},
	}}

	if !acquisition.enqueueDispatchPrewarm(dispatchPrewarmSource{
		owner: newOwner,
		ctx:   context.Background(),
		shard: shard,
		seqno: 10,
		key:   newKey,
	}) {
		t.Fatal("new session did not replace the old session's identical shard state")
	}
	if len(acquisition.dispatchPrewarm.pending) != 1 ||
		acquisition.dispatchPrewarm.pending[0].owner != newOwner {
		t.Fatalf("pending dispatch prewarms = %+v, want only new session", acquisition.dispatchPrewarm.pending)
	}
}

func TestDispatchPrewarmStopsSubmittingHintsAfterCancellation(t *testing.T) {
	queue := makeDispatchQueue(t,
		dispatchFixtureAccount{accountID: benchmarkDispatchAccount(1), lts: []uint64{100}},
		dispatchFixtureAccount{accountID: benchmarkDispatchAccount(2), lts: []uint64{200}},
		dispatchFixtureAccount{accountID: benchmarkDispatchAccount(3), lts: []uint64{300}},
	)
	ctx, cancel := context.WithCancel(t.Context())
	warmer := &cancelDispatchPrewarmer{cancel: cancel}
	acquisition := &LocalAcquisition{accountPrewarmer: warmer}

	err := acquisition.prewarmDispatchQueue(ctx, msgpool.ShardIdent{
		Workchain: 0,
		Shard:     msgpool.ShardAll,
	}, queue)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("dispatch prewarm error = %v, want context cancellation", err)
	}
	if warmer.accounts != 1 || warmer.roots != 0 {
		t.Fatalf("cancelled dispatch prewarm submitted accounts=%d roots=%d, want one account and no roots",
			warmer.accounts, warmer.roots)
	}
}

func TestDispatchPrewarmValidatesEffectiveRegisteredTopology(t *testing.T) {
	target := groups.ShardID{Workchain: 0, Shard: -1 << 63}
	left := groups.ShardID{Workchain: 0, Shard: mustPredecessorChild(t, target.Shard, true)}
	right := groups.ShardID{Workchain: 0, Shard: mustPredecessorChild(t, target.Shard, false)}
	leftChild := groups.ShardID{Workchain: 0, Shard: mustPredecessorChild(t, left.Shard, true)}
	description := func(shard groups.ShardID, seed byte) groups.ShardDescription {
		return groups.ShardDescription{
			Shard: shard,
			Block: testBlockID(shard.Workchain, shard.Shard, 10, seed),
		}
	}

	tests := []struct {
		name       string
		target     groups.ShardID
		registered []groups.ShardDescription
		want       bool
	}{
		{name: "linear", target: target, registered: []groups.ShardDescription{description(target, 1)}, want: true},
		{name: "split", target: left, registered: []groups.ShardDescription{description(target, 2)}, want: true},
		{name: "merge", target: target, registered: []groups.ShardDescription{description(left, 3), description(right, 4)}, want: true},
		{name: "reversed merge", target: target, registered: []groups.ShardDescription{description(right, 4), description(left, 3)}},
		{name: "non-parent split", target: leftChild, registered: []groups.ShardDescription{description(target, 2)}},
		{name: "missing", target: target},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := validDispatchPrewarmPredecessors(test.target, test.registered); got != test.want {
				t.Fatalf("valid topology = %t, want %t", got, test.want)
			}
		})
	}
}

func TestDispatchPrewarmSchedulesOneRegisteredTaskForEffectiveTarget(t *testing.T) {
	root := groups.ShardID{Workchain: 0, Shard: -1 << 63}
	left := groups.ShardID{Workchain: 0, Shard: mustPredecessorChild(t, root.Shard, true)}
	right := groups.ShardID{Workchain: 0, Shard: mustPredecessorChild(t, root.Shard, false)}
	description := func(shard groups.ShardID, seqno uint32, seed byte) groups.ShardDescription {
		return groups.ShardDescription{
			Shard: shard,
			Block: testBlockID(shard.Workchain, shard.Shard, seqno, seed),
		}
	}
	tests := []struct {
		name         string
		target       groups.ShardID
		registered   []groups.ShardDescription
		wantCount    uint8
		wantMaxSeqno uint32
	}{
		{
			name:         "linear",
			target:       root,
			registered:   []groups.ShardDescription{description(root, 10, 1)},
			wantCount:    1,
			wantMaxSeqno: 10,
		},
		{
			name:         "split",
			target:       left,
			registered:   []groups.ShardDescription{description(root, 11, 2)},
			wantCount:    1,
			wantMaxSeqno: 11,
		},
		{
			name:         "merge",
			target:       root,
			registered:   []groups.ShardDescription{description(left, 12, 3), description(right, 15, 4)},
			wantCount:    2,
			wantMaxSeqno: 15,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			acquisition := &LocalAcquisition{accountPrewarmer: &recordedAccountPrewarmer{}}
			acquisition.dispatchPrewarm.running = true
			owner := dispatchPrewarmOwner{session: [32]byte{0x71}, generation: 1}
			if !acquisition.enqueueRegisteredDispatchPrewarm(t.Context(), owner, test.target, test.registered) {
				t.Fatal("registered dispatch prewarm was not queued")
			}

			pending := acquisition.dispatchPrewarm.pending
			if len(pending) != 1 {
				t.Fatalf("pending dispatch prewarms = %d, want one effective target", len(pending))
			}
			task := pending[0]
			if task.owner != owner || task.key.owner != owner || task.deadline.IsZero() ||
				task.shard != targetShardIdent(test.target) || task.predecessorCount != test.wantCount ||
				task.seqno != test.wantMaxSeqno {
				t.Fatalf("effective target task = %+v", task)
			}
			for i := range test.registered {
				if !task.predecessors[i].Equals(&test.registered[i].Block) {
					t.Fatalf("predecessor %d differs from registered block", i)
				}
			}
		})
	}
}

func TestDispatchPrewarmBuildsEffectiveSplitQueue(t *testing.T) {
	request := emptyCandidateRequest(t)
	parent := request.Shard
	left := groups.ShardID{Workchain: parent.Workchain, Shard: mustPredecessorChild(t, parent.Shard, true)}
	keptSource := [32]byte{0x11}
	removedSource := [32]byte{0x91}
	physical := dispatchPrewarmQueue(t,
		dispatchPrewarmFixture{
			source:      keptSource,
			destination: dispatchPrewarmDestination(keptSource),
			lt:          100,
		},
		dispatchPrewarmFixture{
			source:      removedSource,
			destination: dispatchPrewarmDestination(removedSource),
			lt:          200,
		},
	)
	physicalHash := physical.RootCell().HashKey()
	source := dispatchPrewarmBlockSource(t, request.Previous.State, parent, 10, true, 0x31, physical)

	effective, err := effectiveDispatchPrewarmQueue(left, source, nil)
	if err != nil {
		t.Fatal(err)
	}
	warmer := &recordedAccountPrewarmer{}
	acquisition := &LocalAcquisition{accountPrewarmer: warmer}
	if err = acquisition.prewarmDispatchQueue(t.Context(), targetShardIdent(left), effective); err != nil {
		t.Fatal(err)
	}

	if len(warmer.accounts) != 1 || warmer.accounts[0].account != dispatchFixtureDestination(keptSource) {
		t.Fatalf("split prewarmed destinations = %+v, want only left-child message", warmer.accounts)
	}
	if len(warmer.roots) != 1 {
		t.Fatalf("split prewarmed envelope roots = %d, want 1", len(warmer.roots))
	}
	if _, err = loadAccountDispatchQueue(physical, keptSource); err != nil {
		t.Fatalf("effective split mutated kept physical queue entry: %v", err)
	}
	if _, err = loadAccountDispatchQueue(physical, removedSource); err != nil {
		t.Fatalf("effective split mutated removed physical queue entry: %v", err)
	}
	if physical.RootCell().HashKey() != physicalHash {
		t.Fatal("effective split changed the physical parent queue hash")
	}
}

func TestDispatchPrewarmBuildsOneEffectiveMergedQueue(t *testing.T) {
	request := emptyCandidateRequest(t)
	target := request.Shard
	left := groups.ShardID{Workchain: target.Workchain, Shard: mustPredecessorChild(t, target.Shard, true)}
	right := groups.ShardID{Workchain: target.Workchain, Shard: mustPredecessorChild(t, target.Shard, false)}
	leftSource := [32]byte{0x11}
	rightSource := [32]byte{0x91}
	leftQueue := dispatchPrewarmQueue(t, dispatchPrewarmFixture{
		source:      leftSource,
		destination: dispatchPrewarmDestination(leftSource),
		lt:          100,
	})
	rightQueue := dispatchPrewarmQueue(t, dispatchPrewarmFixture{
		source:      rightSource,
		destination: dispatchPrewarmDestination(rightSource),
		lt:          200,
	})
	leftHash := leftQueue.RootCell().HashKey()
	rightHash := rightQueue.RootCell().HashKey()
	leftBlock := dispatchPrewarmBlockSource(t, request.Previous.State, left, 10, false, 0x41, leftQueue)
	rightBlock := dispatchPrewarmBlockSource(t, request.Previous.State, right, 11, false, 0x51, rightQueue)

	effective, err := effectiveDispatchPrewarmQueue(target, leftBlock, rightBlock)
	if err != nil {
		t.Fatal(err)
	}
	warmer := &recordedAccountPrewarmer{}
	acquisition := &LocalAcquisition{accountPrewarmer: warmer}
	if err = acquisition.prewarmDispatchQueue(t.Context(), targetShardIdent(target), effective); err != nil {
		t.Fatal(err)
	}

	if len(warmer.accounts) != 2 || len(warmer.roots) != 2 {
		t.Fatalf("merge prewarmed accounts=%d roots=%d, want one canonical scan over both children",
			len(warmer.accounts), len(warmer.roots))
	}
	if _, err = loadAccountDispatchQueue(leftQueue, leftSource); err != nil {
		t.Fatalf("effective merge mutated left physical queue: %v", err)
	}
	if _, err = loadAccountDispatchQueue(rightQueue, rightSource); err != nil {
		t.Fatalf("effective merge mutated right physical queue: %v", err)
	}
	if leftQueue.RootCell().HashKey() != leftHash || rightQueue.RootCell().HashKey() != rightHash {
		t.Fatal("effective merge changed a physical child queue hash")
	}
}

type cancelDispatchPrewarmer struct {
	cancel   context.CancelFunc
	accounts int
	roots    int
}

func (w *cancelDispatchPrewarmer) EnqueueRoot(cell.Hash) bool {
	w.roots++
	return true
}

func (w *cancelDispatchPrewarmer) EnqueueAccount(int32, [32]byte) bool {
	w.accounts++
	if w.accounts == 1 {
		w.cancel()
	}
	return true
}

func (w *cancelDispatchPrewarmer) PrewarmAccountNow(int32, [32]byte) bool {
	return false
}

func dispatchPrewarmDestination(source [32]byte) *address.Address {
	destination := dispatchFixtureDestination(source)
	return address.NewAddress(0, 0, destination[:])
}

func dispatchPrewarmBlockSource(
	t *testing.T,
	base *cell.Cell,
	shard groups.ShardID,
	seqno uint32,
	beforeSplit bool,
	seed byte,
	queue *tlb.DispatchQueueAugDict,
) *localBlockSource {
	t.Helper()

	var state tlb.ShardStateUnsplit
	if err := parseExact(&state, base); err != nil {
		t.Fatal(err)
	}
	state.ShardIdent = mustPredecessorIdent(t, shard)
	state.Seqno = seqno
	state.BeforeSplit = beforeSplit
	var queueInfo tlb.OutMsgQueueInfo
	if err := parseExact(&queueInfo, state.OutMsgQueueInfo); err != nil {
		t.Fatal(err)
	}
	queueInfo.Extra = &tlb.OutMsgQueueExtra{DispatchQueue: queue}
	var err error
	state.OutMsgQueueInfo, err = queueInfo.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	root, err := tlb.ToCell(&state)
	if err != nil {
		t.Fatal(err)
	}
	previous := PreviousBlock{
		ID:    testBlockID(shard.Workchain, shard.Shard, seqno, seed),
		State: root,
	}

	return &localBlockSource{previous: previous, state: state}
}

func dispatchPrewarmTestKey(value byte) dispatchPrewarmSourceKey {
	return dispatchPrewarmSourceKey{
		kind:  dispatchPrewarmSourceState,
		count: 1,
		roots: [2]cell.Hash{{value}},
	}
}

type dispatchPrewarmFixture struct {
	source      [32]byte
	destination *address.Address
	lt          uint64
}

func dispatchPrewarmQueue(t *testing.T, fixtures ...dispatchPrewarmFixture) *tlb.DispatchQueueAugDict {
	t.Helper()
	queue, err := tlb.NewDispatchQueueAugDict()
	if err != nil {
		t.Fatal(err)
	}
	for _, fixture := range fixtures {
		source := address.NewAddress(0, 0, fixture.source[:])
		message, serializeErr := tlb.ToCell(&tlb.InternalMessage{
			IHRDisabled: true,
			SrcAddr:     source,
			DstAddr:     fixture.destination,
			Amount:      tlb.FromNanoTONU(1_000_000),
			FwdFee:      tlb.FromNanoTONU(1_000),
			CreatedLT:   fixture.lt,
			CreatedAt:   1_900_000_000,
			Body:        cell.BeginCell().EndCell(),
		})
		if serializeErr != nil {
			t.Fatal(serializeErr)
		}
		envelope, serializeErr := (tlb.MsgEnvelope{
			CurAddr:         tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular},
			NextAddr:        tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular},
			FwdFeeRemaining: tlb.FromNanoTONU(1_000),
			Msg:             message,
		}).ToCell()
		if serializeErr != nil {
			t.Fatal(serializeErr)
		}
		enqueued, serializeErr := (tlb.EnqueuedMsg{EnqueuedLT: fixture.lt, Msg: envelope}).ToCell()
		if serializeErr != nil {
			t.Fatal(serializeErr)
		}
		messages := cell.NewDict(64)
		if err = messages.Set(dispatchLTKey(fixture.lt), enqueued); err != nil {
			t.Fatal(err)
		}
		accountQueue, serializeErr := (tlb.AccountDispatchQueue{Messages: messages, Count: 1}).ToCell()
		if serializeErr != nil {
			t.Fatal(serializeErr)
		}
		if err = queue.Set(dispatchAccountKey(fixture.source), accountQueue); err != nil {
			t.Fatal(err)
		}
	}
	return queue
}

func dispatchFixtureDestination(source [32]byte) [32]byte {
	destination := source
	destination[31] ^= 0xff
	return destination
}

func dispatchFixtureEnvelopeRoot(
	t *testing.T,
	queue *tlb.DispatchQueueAugDict,
	account [32]byte,
	lt uint64,
) cell.Hash {
	t.Helper()
	accountQueue, err := loadAccountDispatchQueue(queue, account)
	if err != nil {
		t.Fatal(err)
	}
	value, err := accountQueue.Messages.LoadValue(dispatchLTKey(lt))
	if err != nil {
		t.Fatal(err)
	}
	var enqueued tlb.EnqueuedMsg
	if err = loadExactSlice(&enqueued, value); err != nil {
		t.Fatal(err)
	}
	return enqueued.Msg.HashKey()
}
