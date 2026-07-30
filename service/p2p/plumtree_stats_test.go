package p2p

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPlumtreeStatsJitterAndSchedule(t *testing.T) {
	t.Parallel()

	zeroJitter := plumtreeStatsTestJitter(t, 0)
	if zeroJitter.delay != 0 {
		t.Fatalf("zero jitter delay = %v, want 0", zeroJitter.delay)
	}

	maximumJitter := plumtreeStatsTestJitter(t, plumtreeStatsMaximumJitterStep)
	if maximumJitter.delay != 5*time.Minute {
		t.Fatalf("maximum jitter delay = %v, want 5m", maximumJitter.delay)
	}

	if _, err := newPlumtreeStatsJitter(plumtreeStatsMaximumJitterStep + 1); err == nil {
		t.Fatal("oversized jitter step accepted")
	}

	stats := &plumtreeStats{}
	start := time.Unix(44*int64(time.Hour/time.Second), 0)
	jitter := plumtreeStatsTestJitter(t, 500)
	schedule := stats.Schedule(start, jitter)

	if schedule.epoch != 44 {
		t.Fatalf("epoch = %d, want 44", schedule.epoch)
	}
	if schedule.rotatingTree != 45 {
		t.Fatalf("rotating tree = %d, want 45", schedule.rotatingTree)
	}
	if !schedule.snapshotAt.Equal(start.Add(5 * time.Minute)) {
		t.Fatalf("snapshot time = %v", schedule.snapshotAt)
	}
	if !schedule.sendAt.Equal(start.Add(7*time.Minute + 30*time.Second)) {
		t.Fatalf("send time = %v", schedule.sendAt)
	}
	if !schedule.nextEpochAt.Equal(start.Add(time.Hour)) {
		t.Fatalf("next epoch time = %v", schedule.nextEpochAt)
	}
	if schedule.snapshotDone || schedule.sendDone || schedule.skipped {
		t.Fatalf("fresh schedule has completed flags: %+v", schedule)
	}
	if next := stats.NextAt(start, zeroJitter); !next.Equal(schedule.snapshotAt) {
		t.Fatalf("next time = %v, want snapshot %v", next, schedule.snapshotAt)
	}

	nextSchedule := stats.Schedule(start.Add(time.Hour), zeroJitter)
	if nextSchedule.epoch != 45 || nextSchedule.rotatingTree != 1 {
		t.Fatalf(
			"next schedule epoch/tree = %d/%d, want 45/1",
			nextSchedule.epoch,
			nextSchedule.rotatingTree,
		)
	}
}

func TestPlumtreeStatsIgnoresStaleEpoch(t *testing.T) {
	t.Parallel()

	stats := &plumtreeStats{}
	jitter := plumtreeStatsTestJitter(t, 0)
	current := time.Unix(80*int64(time.Hour/time.Second), 0)
	currentSchedule := stats.Schedule(current, jitter)
	staleSchedule := stats.Schedule(current.Add(-time.Hour), jitter)

	if staleSchedule != currentSchedule {
		t.Fatalf(
			"stale epoch changed schedule: got %+v, want %+v",
			staleSchedule,
			currentSchedule,
		)
	}
}

func TestPlumtreeStatsSnapshotBucketsAndForwarding(t *testing.T) {
	t.Parallel()

	stats := &plumtreeStats{}
	start := time.Unix(90*int64(time.Hour/time.Second), 0)
	jitter := plumtreeStatsTestJitter(t, plumtreeStatsMaximumJitterStep)
	schedule := stats.Schedule(start, jitter)

	for range 7 {
		stats.NoteDeliveredBroadcast()
	}
	for range 2 {
		stats.NoteFECPartsCollected(3)
	}
	for range 98 {
		stats.NoteFECPartsCollected(45)
	}

	latencyBase := time.Unix(1_000, 0)
	for range 98 {
		stats.NoteUsefulDelivery(latencyBase, 1_000)
	}
	for range 2 {
		stats.NoteUsefulDelivery(latencyBase.Add(999*time.Millisecond), 1_000)
	}
	stats.NoteUsefulDelivery(latencyBase, math.NaN())
	stats.NoteUsefulDelivery(latencyBase, math.Inf(1))

	localID := plumtreeStatsTestPeer(250)
	simpleFirst := plumtreeStatsTestPeer(10)
	simpleSecond := plumtreeStatsTestPeer(11)
	rotating := plumtreeStatsTestPeer(12)
	eager := plumtreeStatsTestEagerSnapshot(
		t,
		[]PeerID{simpleFirst, simpleSecond},
		[]PeerID{rotating},
	)

	before := stats.Tick(
		schedule.snapshotAt.Add(-time.Nanosecond),
		jitter,
		localID,
		&eager,
	)
	if before.snapshotCreated || before.forward.ready {
		t.Fatalf("stats acted before snapshot: %+v", before)
	}

	result := stats.Tick(schedule.snapshotAt, jitter, localID, &eager)
	if !result.snapshotCreated {
		t.Fatal("snapshot was not created at its deadline")
	}
	if result.forward.ready {
		t.Fatal("snapshot forwarded before jittered send deadline")
	}

	record := result.snapshot
	if !bytes.Equal(record.Source, localID[:]) {
		t.Fatalf("snapshot source = %x, want %x", record.Source, localID)
	}
	if record.Epoch != int32(schedule.epoch) || record.TreeIndex != schedule.rotatingTree {
		t.Fatalf("snapshot epoch/tree = %d/%d", record.Epoch, record.TreeIndex)
	}
	if record.DeliveredBroadcast != 7 {
		t.Fatalf("delivered broadcasts = %d, want 7", record.DeliveredBroadcast)
	}
	if record.PartsInP99 != 3 {
		t.Fatalf("FEC parts p99 = %d, want 3", record.PartsInP99)
	}
	if record.UsefulAverageMS != 19 {
		t.Fatalf("useful average = %dms, want 19ms", record.UsefulAverageMS)
	}
	if record.UsefulMaximumMS != 999 {
		t.Fatalf("useful maximum = %dms, want 999ms", record.UsefulMaximumMS)
	}
	if record.UsefulP99MS != 1000 {
		t.Fatalf("useful p99 = %dms, want 1000ms", record.UsefulP99MS)
	}

	wantSimplePrefixes := []int64{
		int64(binary.LittleEndian.Uint64(simpleFirst[:8])),
		int64(binary.LittleEndian.Uint64(simpleSecond[:8])),
	}
	wantRotatingPrefixes := []int64{
		int64(binary.LittleEndian.Uint64(rotating[:8])),
	}
	if !reflect.DeepEqual(record.EagerSimple, wantSimplePrefixes) {
		t.Fatalf("simple eager prefixes = %v, want %v", record.EagerSimple, wantSimplePrefixes)
	}
	if !reflect.DeepEqual(record.EagerRotating, wantRotatingPrefixes) {
		t.Fatalf(
			"rotating eager prefixes = %v, want %v",
			record.EagerRotating,
			wantRotatingPrefixes,
		)
	}
	if next := stats.NextAt(schedule.snapshotAt, jitter); !next.Equal(schedule.sendAt) {
		t.Fatalf("next time after snapshot = %v, want %v", next, schedule.sendAt)
	}

	send := stats.Tick(schedule.sendAt, jitter, localID, &eager)
	if send.snapshotCreated {
		t.Fatal("snapshot repeated at send deadline")
	}
	if !send.forward.ready {
		t.Fatal("local snapshot was not forwarded at send deadline")
	}
	if send.forward.count != 2 ||
		send.forward.to[0] != simpleFirst ||
		send.forward.to[1] != simpleSecond {
		t.Fatalf(
			"forward targets = %v/%d",
			send.forward.to,
			send.forward.count,
		)
	}
	if !bytes.Equal(send.forward.record.Source, localID[:]) {
		t.Fatalf("forwarded source = %x, want %x", send.forward.record.Source, localID)
	}
	if next := stats.NextAt(schedule.sendAt, jitter); !next.Equal(schedule.nextEpochAt) {
		t.Fatalf("next time after send = %v, want %v", next, schedule.nextEpochAt)
	}
}

func TestPlumtreeStatsCounterApproximationAndReset(t *testing.T) {
	t.Parallel()

	var counters plumtreeStatsCounters
	for range 99 {
		counters.noteFECPartsCollected(45)
	}
	counters.noteFECPartsCollected(0)
	for range 99 {
		counters.noteUsefulDelivery(0)
	}
	counters.noteUsefulDelivery(999)
	counters.deliveredBroadcast = uint64(math.MaxInt32) + 1

	snapshot := counters.snapshotAndReset()
	if snapshot.deliveredBroadcast != math.MaxInt32 {
		t.Fatalf("saturated delivered = %d", snapshot.deliveredBroadcast)
	}
	if snapshot.partsInP99 != 45 {
		t.Fatalf("lower-tail FEC p99 = %d, want 45", snapshot.partsInP99)
	}
	if snapshot.usefulAverageMS != 9 {
		t.Fatalf("useful average = %d, want 9", snapshot.usefulAverageMS)
	}
	if snapshot.usefulMaximumMS != 999 {
		t.Fatalf("useful maximum = %d, want 999", snapshot.usefulMaximumMS)
	}
	if snapshot.usefulP99MS != 10 {
		t.Fatalf("useful p99 = %d, want 10", snapshot.usefulP99MS)
	}
	if counters != (plumtreeStatsCounters{}) {
		t.Fatalf("counters were not reset: %+v", counters)
	}

	counters.noteFECPartsCollected(math.MaxUint32)
	counters.noteUsefulDelivery(math.MaxUint32)
	saturated := counters.snapshotAndReset()
	if saturated.partsInP99 != plumtreeFECTotalParts {
		t.Fatalf("clamped FEC parts = %d", saturated.partsInP99)
	}
	if saturated.usefulAverageMS != math.MaxInt32 ||
		saturated.usefulMaximumMS != math.MaxInt32 ||
		saturated.usefulP99MS != 1000 {
		t.Fatalf("saturated useful snapshot = %+v", saturated)
	}
}

func TestPlumtreeStatsUsefulTimestampRules(t *testing.T) {
	t.Parallel()

	stats := &plumtreeStats{}
	now := time.Unix(4_500_000, 0)
	stats.NoteUsefulDelivery(now, math.NaN())
	stats.NoteUsefulDelivery(now, math.Inf(-1))
	stats.NoteUsefulDelivery(now, float64(now.Unix()+1))
	stats.NoteUsefulDelivery(now, 0)

	stats.mu.Lock()
	snapshot := stats.counters.snapshotAndReset()
	stats.mu.Unlock()

	if snapshot.usefulAverageMS != math.MaxInt32 {
		t.Fatalf("useful average = %d, want saturation", snapshot.usefulAverageMS)
	}
	if snapshot.usefulMaximumMS != math.MaxInt32 {
		t.Fatalf("useful maximum = %d, want saturation", snapshot.usefulMaximumMS)
	}
	if snapshot.usefulP99MS != 1000 {
		t.Fatalf("useful p99 = %d, want 1000", snapshot.usefulP99MS)
	}
}

func TestPlumtreeStatsLateEpochCarriesCounters(t *testing.T) {
	t.Parallel()

	stats := &plumtreeStats{}
	stats.NoteDeliveredBroadcast()

	start := time.Unix(120*int64(time.Hour/time.Second), 0)
	jitter := plumtreeStatsTestJitter(t, 0)
	eager := plumtreeStatsTestEagerSnapshot(t, nil, nil)
	localID := plumtreeStatsTestPeer(250)

	late := stats.Schedule(start.Add(plumtreeStatsSnapshotDelay), jitter)
	if !late.skipped || !late.snapshotDone || !late.sendDone {
		t.Fatalf("late epoch was not skipped: %+v", late)
	}
	if next := stats.NextAt(start.Add(plumtreeStatsSnapshotDelay), jitter); !next.Equal(start.Add(time.Hour)) {
		t.Fatalf("late epoch next time = %v", next)
	}
	if result := stats.Tick(
		start.Add(10*time.Minute),
		jitter,
		localID,
		&eager,
	); result.snapshotCreated || result.forward.ready {
		t.Fatalf("skipped epoch produced actions: %+v", result)
	}

	nextStart := start.Add(time.Hour)
	nextSchedule := stats.Schedule(nextStart, jitter)
	result := stats.Tick(nextSchedule.snapshotAt, jitter, localID, &eager)
	if !result.snapshotCreated {
		t.Fatal("next epoch snapshot was not created")
	}
	if result.forward.ready {
		t.Fatal("zero-target send produced a forwarding action")
	}
	if result.snapshot.DeliveredBroadcast != 1 {
		t.Fatalf(
			"carried delivered broadcasts = %d, want 1",
			result.snapshot.DeliveredBroadcast,
		)
	}
}

func TestPlumtreeStatsHandlePushAndTreeZeroForwarding(t *testing.T) {
	t.Parallel()

	stats := &plumtreeStats{}
	start := time.Unix(180*int64(time.Hour/time.Second), 0)
	jitter := plumtreeStatsTestJitter(t, 0)
	schedule := stats.Schedule(start, jitter)

	localID := plumtreeStatsTestPeer(250)
	from := plumtreeStatsTestPeer(10)
	next := plumtreeStatsTestPeer(11)
	last := plumtreeStatsTestPeer(12)
	simple := plumtreeStatsTestEagerPeers(t, []PeerID{next, from, last})
	push := plumtreeStatsTestPush(
		plumtreeStatsTestPeer(1),
		int32(schedule.epoch),
		schedule.rotatingTree,
	)

	result, err := stats.HandlePush(
		start.Add(time.Minute),
		jitter,
		localID,
		from,
		&simple,
		push,
	)
	if err != nil {
		t.Fatalf("handle valid push: %v", err)
	}
	if !result.accepted || !result.forward.ready {
		t.Fatalf("valid push result = %+v", result)
	}
	if result.forward.count != 2 ||
		result.forward.to[0] != next ||
		result.forward.to[1] != last {
		t.Fatalf(
			"tree-zero targets = %v/%d",
			result.forward.to,
			result.forward.count,
		)
	}
	if !reflect.DeepEqual(result.forward.record, push.Record) {
		t.Fatalf("forwarded record changed: %+v", result.forward.record)
	}

	duplicate, err := stats.HandlePush(
		start.Add(time.Minute),
		jitter,
		localID,
		from,
		&simple,
		push,
	)
	if err != nil {
		t.Fatalf("handle duplicate push: %v", err)
	}
	if duplicate.accepted || duplicate.forward.ready {
		t.Fatalf("duplicate push was accepted: %+v", duplicate)
	}
	if records := stats.Records(); len(records) != 1 {
		t.Fatalf("stored record count = %d, want 1", len(records))
	}

	onlySenderStats := &plumtreeStats{}
	onlySenderSchedule := onlySenderStats.Schedule(start, jitter)
	onlySender := plumtreeStatsTestEagerPeers(t, []PeerID{from})
	onlySenderPush := plumtreeStatsTestPush(
		plumtreeStatsTestPeer(2),
		int32(onlySenderSchedule.epoch),
		onlySenderSchedule.rotatingTree,
	)
	onlySenderResult, err := onlySenderStats.HandlePush(
		start.Add(time.Minute),
		jitter,
		localID,
		from,
		&onlySender,
		onlySenderPush,
	)
	if err != nil {
		t.Fatalf("handle only-sender push: %v", err)
	}
	if !onlySenderResult.accepted || onlySenderResult.forward.ready {
		t.Fatalf("only-sender forwarding result = %+v", onlySenderResult)
	}
}

func TestPlumtreeStatsPushValidation(t *testing.T) {
	t.Parallel()

	start := time.Unix(225*int64(time.Hour/time.Second), 0)
	jitter := plumtreeStatsTestJitter(t, 0)
	localID := plumtreeStatsTestPeer(250)
	from := plumtreeStatsTestPeer(10)
	source := plumtreeStatsTestPeer(1)

	type invalidCase struct {
		name   string
		from   PeerID
		eager  []PeerID
		change func(*PlumtreeStatsRecord)
	}

	cases := []invalidCase{
		{
			name:  "wrong tree",
			from:  from,
			eager: []PeerID{from},
			change: func(record *PlumtreeStatsRecord) {
				record.TreeIndex++
			},
		},
		{
			name:   "sender is not eager on tree zero",
			from:   from,
			eager:  []PeerID{plumtreeStatsTestPeer(11)},
			change: func(*PlumtreeStatsRecord) {},
		},
		{
			name:  "malformed source",
			from:  from,
			eager: []PeerID{from},
			change: func(record *PlumtreeStatsRecord) {
				record.Source = record.Source[:PeerIDSize-1]
			},
		},
		{
			name:  "zero source",
			from:  from,
			eager: []PeerID{from},
			change: func(record *PlumtreeStatsRecord) {
				record.Source = make([]byte, PeerIDSize)
			},
		},
		{
			name:  "local source",
			from:  from,
			eager: []PeerID{from},
			change: func(record *PlumtreeStatsRecord) {
				record.Source = localID.Bytes()
			},
		},
		{
			name:  "wrong epoch",
			from:  from,
			eager: []PeerID{from},
			change: func(record *PlumtreeStatsRecord) {
				record.Epoch++
			},
		},
		{
			name:  "too many simple eager prefixes",
			from:  from,
			eager: []PeerID{from},
			change: func(record *PlumtreeStatsRecord) {
				record.EagerSimple = make([]int64, plumtreeRegularEagerLimit+1)
			},
		},
		{
			name:  "too many rotating eager prefixes",
			from:  from,
			eager: []PeerID{from},
			change: func(record *PlumtreeStatsRecord) {
				record.EagerRotating = make([]int64, plumtreeRegularEagerLimit+1)
			},
		},
		{
			name:  "negative delivered",
			from:  from,
			eager: []PeerID{from},
			change: func(record *PlumtreeStatsRecord) {
				record.DeliveredBroadcast = -1
			},
		},
		{
			name:  "negative parts",
			from:  from,
			eager: []PeerID{from},
			change: func(record *PlumtreeStatsRecord) {
				record.PartsInP99 = -1
			},
		},
		{
			name:  "negative average",
			from:  from,
			eager: []PeerID{from},
			change: func(record *PlumtreeStatsRecord) {
				record.UsefulAverageMS = -1
			},
		},
		{
			name:  "negative maximum",
			from:  from,
			eager: []PeerID{from},
			change: func(record *PlumtreeStatsRecord) {
				record.UsefulMaximumMS = -1
			},
		},
		{
			name:  "negative p99",
			from:  from,
			eager: []PeerID{from},
			change: func(record *PlumtreeStatsRecord) {
				record.UsefulP99MS = -1
			},
		},
		{
			name:  "parts exceed FEC total",
			from:  from,
			eager: []PeerID{from},
			change: func(record *PlumtreeStatsRecord) {
				record.PartsInP99 = plumtreeFECTotalParts + 1
			},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			stats := &plumtreeStats{}
			schedule := stats.Schedule(start, jitter)
			simple := plumtreeStatsTestEagerPeers(t, testCase.eager)
			push := plumtreeStatsTestPush(
				source,
				int32(schedule.epoch),
				schedule.rotatingTree,
			)
			testCase.change(&push.Record)

			result, err := stats.HandlePush(
				start.Add(time.Minute),
				jitter,
				localID,
				testCase.from,
				&simple,
				push,
			)
			if err != nil {
				t.Fatalf("handle invalid push: %v", err)
			}
			if result.accepted || result.forward.ready {
				t.Fatalf("invalid push accepted: %+v", result)
			}
			if records := stats.Records(); len(records) != 0 {
				t.Fatalf("invalid push stored %d records", len(records))
			}
		})
	}

	t.Run("missing immediate sender is an error", func(t *testing.T) {
		t.Parallel()

		stats := &plumtreeStats{}
		schedule := stats.Schedule(start, jitter)
		simple := plumtreeStatsTestEagerPeers(t, []PeerID{from})
		push := plumtreeStatsTestPush(
			source,
			int32(schedule.epoch),
			schedule.rotatingTree,
		)

		_, err := stats.HandlePush(
			start.Add(time.Minute),
			jitter,
			localID,
			PeerID{},
			&simple,
			push,
		)
		if err == nil || !strings.Contains(err.Error(), "missing Plumtree immediate sender") {
			t.Fatalf("missing sender error = %v", err)
		}
	})

	t.Run("skipped epoch ignores push", func(t *testing.T) {
		t.Parallel()

		stats := &plumtreeStats{}
		tree := plumtreeFECTreeOffset + int32(225%int64(plumtreeTreeSlots-1))
		simple := plumtreeStatsTestEagerPeers(t, []PeerID{from})
		push := plumtreeStatsTestPush(source, 225, tree)

		result, err := stats.HandlePush(
			start.Add(plumtreeStatsSnapshotDelay),
			jitter,
			localID,
			from,
			&simple,
			push,
		)
		if err != nil {
			t.Fatalf("handle skipped push: %v", err)
		}
		if result.accepted || len(stats.Records()) != 0 {
			t.Fatalf("skipped push accepted: %+v", result)
		}
	})
}

func TestPlumtreeStatsStoreLimitMatchesReference(t *testing.T) {
	t.Parallel()

	start := time.Unix(270*int64(time.Hour/time.Second), 0)
	jitter := plumtreeStatsTestJitter(t, plumtreeStatsMaximumJitterStep)
	localID := plumtreeStatsTestPeer(250)
	from := plumtreeStatsTestPeer(200)
	simple := plumtreeStatsTestEagerPeers(t, []PeerID{from})

	stats := &plumtreeStats{}
	schedule := stats.Schedule(start, jitter)
	for index := 1; index <= plumtreeStatsStoreLimit; index++ {
		push := plumtreeStatsTestPush(
			plumtreeStatsTestPeer(byte(index)),
			int32(schedule.epoch),
			schedule.rotatingTree,
		)
		result, err := stats.HandlePush(
			start.Add(time.Minute),
			jitter,
			localID,
			from,
			&simple,
			push,
		)
		if err != nil {
			t.Fatalf("handle push %d: %v", index, err)
		}
		if !result.accepted {
			t.Fatalf("push %d was not accepted", index)
		}
	}

	overflow := plumtreeStatsTestPush(
		plumtreeStatsTestPeer(101),
		int32(schedule.epoch),
		schedule.rotatingTree,
	)
	result, err := stats.HandlePush(
		start.Add(time.Minute),
		jitter,
		localID,
		from,
		&simple,
		overflow,
	)
	if err != nil {
		t.Fatalf("handle overflow push: %v", err)
	}
	if result.accepted {
		t.Fatal("101st remote record was accepted")
	}

	eager := plumtreeStatsTestEagerSnapshot(t, []PeerID{from}, nil)
	snapshot := stats.Tick(schedule.snapshotAt, jitter, localID, &eager)
	if !snapshot.snapshotCreated {
		t.Fatal("local snapshot was not created after full remote store")
	}
	if records := stats.Records(); len(records) != plumtreeStatsStoreLimit+1 {
		t.Fatalf(
			"records after local snapshot = %d, want %d",
			len(records),
			plumtreeStatsStoreLimit+1,
		)
	}

	afterSnapshot := &plumtreeStats{}
	afterSchedule := afterSnapshot.Schedule(start, jitter)
	afterSnapshot.Tick(afterSchedule.snapshotAt, jitter, localID, &eager)
	for index := 1; index < plumtreeStatsStoreLimit; index++ {
		push := plumtreeStatsTestPush(
			plumtreeStatsTestPeer(byte(index)),
			int32(afterSchedule.epoch),
			afterSchedule.rotatingTree,
		)
		result, err = afterSnapshot.HandlePush(
			start.Add(6*time.Minute),
			jitter,
			localID,
			from,
			&simple,
			push,
		)
		if err != nil || !result.accepted {
			t.Fatalf("post-snapshot push %d = %+v, %v", index, result, err)
		}
	}

	push100 := plumtreeStatsTestPush(
		plumtreeStatsTestPeer(100),
		int32(afterSchedule.epoch),
		afterSchedule.rotatingTree,
	)
	result, err = afterSnapshot.HandlePush(
		start.Add(6*time.Minute),
		jitter,
		localID,
		from,
		&simple,
		push100,
	)
	if err != nil {
		t.Fatalf("post-snapshot overflow push: %v", err)
	}
	if result.accepted {
		t.Fatal("100th remote record after local snapshot was accepted")
	}
	if records := afterSnapshot.Records(); len(records) != plumtreeStatsStoreLimit {
		t.Fatalf("post-snapshot record count = %d", len(records))
	}
}

func TestPlumtreeStatsRecordsAreSortedAndCloned(t *testing.T) {
	t.Parallel()

	stats := &plumtreeStats{}
	start := time.Unix(315*int64(time.Hour/time.Second), 0)
	jitter := plumtreeStatsTestJitter(t, 0)
	schedule := stats.Schedule(start, jitter)
	localID := plumtreeStatsTestPeer(250)
	from := plumtreeStatsTestPeer(200)
	simple := plumtreeStatsTestEagerPeers(t, []PeerID{from})

	for _, marker := range []byte{3, 1, 2} {
		push := plumtreeStatsTestPush(
			plumtreeStatsTestPeer(marker),
			int32(schedule.epoch),
			schedule.rotatingTree,
		)
		result, err := stats.HandlePush(
			start.Add(time.Minute),
			jitter,
			localID,
			from,
			&simple,
			push,
		)
		if err != nil || !result.accepted {
			t.Fatalf("handle source %d = %+v, %v", marker, result, err)
		}
	}

	records := stats.Records()
	if len(records) != 3 {
		t.Fatalf("record count = %d, want 3", len(records))
	}
	for index, marker := range []byte{1, 2, 3} {
		if records[index].Source[0] != marker {
			t.Fatalf("record %d source marker = %d, want %d", index, records[index].Source[0], marker)
		}
	}

	records[0].Source[0] = 99
	records[0].EagerSimple[0] = 99
	again := stats.Records()
	if again[0].Source[0] != 1 || again[0].EagerSimple[0] != 1 {
		t.Fatalf("record collection aliases store: %+v", again[0])
	}

	stats.Schedule(start.Add(time.Hour), jitter)
	if records := stats.Records(); records != nil {
		t.Fatalf("epoch transition retained records: %+v", records)
	}
}

func TestPlumtreeStatsConcurrentCounters(t *testing.T) {
	t.Parallel()

	const (
		workers    = 8
		iterations = 1_000
	)

	stats := &plumtreeStats{}
	start := time.Unix(360*int64(time.Hour/time.Second), 0)
	jitter := plumtreeStatsTestJitter(t, 0)
	schedule := stats.Schedule(start, jitter)

	var wait sync.WaitGroup
	wait.Add(workers + 1)
	for range workers {
		go func() {
			defer wait.Done()

			now := time.Unix(1_000, 0)
			for index := range iterations {
				stats.NoteDeliveredBroadcast()
				stats.NoteFECPartsCollected(uint32(index % int(plumtreeFECTotalParts+1)))
				stats.NoteUsefulDelivery(now, 1_000)
			}
		}()
	}
	go func() {
		defer wait.Done()

		for range iterations {
			stats.Schedule(start, jitter)
			stats.NextAt(start, jitter)
			stats.Records()
		}
	}()
	wait.Wait()

	eager := plumtreeStatsTestEagerSnapshot(t, nil, nil)
	result := stats.Tick(
		schedule.snapshotAt,
		jitter,
		plumtreeStatsTestPeer(250),
		&eager,
	)
	if !result.snapshotCreated {
		t.Fatal("concurrent counter snapshot was not created")
	}
	if result.snapshot.DeliveredBroadcast != workers*iterations {
		t.Fatalf(
			"delivered broadcasts = %d, want %d",
			result.snapshot.DeliveredBroadcast,
			workers*iterations,
		)
	}
	if result.snapshot.PartsInP99 != 0 {
		t.Fatalf("concurrent FEC p99 = %d, want 0", result.snapshot.PartsInP99)
	}
	if result.snapshot.UsefulAverageMS != 0 ||
		result.snapshot.UsefulMaximumMS != 0 ||
		result.snapshot.UsefulP99MS != 10 {
		t.Fatalf("concurrent useful snapshot = %+v", result.snapshot)
	}
}

func BenchmarkPlumtreeStatsNoteDeliveredBroadcast(b *testing.B) {
	stats := &plumtreeStats{}
	b.ReportAllocs()

	for b.Loop() {
		stats.NoteDeliveredBroadcast()
	}
}

func BenchmarkPlumtreeStatsNoteFECPartsCollected(b *testing.B) {
	stats := &plumtreeStats{}
	var parts uint32
	b.ReportAllocs()

	for b.Loop() {
		stats.NoteFECPartsCollected(parts)
		parts++
		if parts > uint32(plumtreeFECTotalParts) {
			parts = 0
		}
	}
}

func BenchmarkPlumtreeStatsNoteUsefulDelivery(b *testing.B) {
	stats := &plumtreeStats{}
	now := time.Unix(1_000, 20_000_000)
	b.ReportAllocs()

	for b.Loop() {
		stats.NoteUsefulDelivery(now, 1_000)
	}
}

func plumtreeStatsTestPeer(marker byte) PeerID {
	var peer PeerID
	for index := range peer {
		peer[index] = marker
	}
	return peer
}

func plumtreeStatsTestJitter(t *testing.T, step uint32) plumtreeStatsJitter {
	t.Helper()

	jitter, err := newPlumtreeStatsJitter(step)
	if err != nil {
		t.Fatalf("make stats jitter: %v", err)
	}
	return jitter
}

func plumtreeStatsTestEagerPeers(
	t *testing.T,
	peers []PeerID,
) plumtreeStatsEagerPeers {
	t.Helper()

	result, err := newPlumtreeStatsEagerPeers(peers)
	if err != nil {
		t.Fatalf("make stats eager peers: %v", err)
	}
	return result
}

func plumtreeStatsTestEagerSnapshot(
	t *testing.T,
	simple []PeerID,
	rotating []PeerID,
) plumtreeStatsEagerSnapshot {
	t.Helper()

	result, err := newPlumtreeStatsEagerSnapshot(simple, rotating)
	if err != nil {
		t.Fatalf("make stats eager snapshot: %v", err)
	}
	return result
}

func plumtreeStatsTestPush(
	source PeerID,
	epoch int32,
	tree int32,
) PlumtreeStatsPush {
	return PlumtreeStatsPush{
		Record: PlumtreeStatsRecord{
			Source:             source.Bytes(),
			Epoch:              epoch,
			TreeIndex:          tree,
			EagerSimple:        []int64{1, 2, 3, 4},
			EagerRotating:      []int64{5, 6, 7, 8},
			DeliveredBroadcast: 10,
			PartsInP99:         40,
			UsefulAverageMS:    20,
			UsefulMaximumMS:    30,
			UsefulP99MS:        40,
		},
	}
}

func newPlumtreeStatsJitter(step uint32) (plumtreeStatsJitter, error) {
	if step > plumtreeStatsMaximumJitterStep {
		return plumtreeStatsJitter{}, fmt.Errorf("invalid Plumtree stats jitter step %d", step)
	}

	return plumtreeStatsJitter{
		delay: time.Duration(step) * plumtreeStatsJitterStepDuration,
	}, nil
}

func newPlumtreeStatsEagerPeers(peers []PeerID) (plumtreeStatsEagerPeers, error) {
	if len(peers) > plumtreeRegularEagerLimit {
		return plumtreeStatsEagerPeers{}, fmt.Errorf(
			"too many Plumtree stats eager peers: %d",
			len(peers),
		)
	}

	var result plumtreeStatsEagerPeers
	result.count = uint8(copy(result.peers[:], peers))
	return result, nil
}

func (s *plumtreeStats) Records() []PlumtreeStatsRecord {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.epoch.store) == 0 {
		return nil
	}

	records := make([]PlumtreeStatsRecord, 0, len(s.epoch.store))
	for _, record := range s.epoch.store {
		records = append(records, clonePlumtreeStatsRecord(record))
	}
	slices.SortFunc(records, func(left, right PlumtreeStatsRecord) int {
		return bytes.Compare(left.Source, right.Source)
	})

	return records
}

func clonePlumtreeStatsRecord(record PlumtreeStatsRecord) PlumtreeStatsRecord {
	record.Source = append([]byte(nil), record.Source...)
	record.EagerSimple = append([]int64(nil), record.EagerSimple...)
	record.EagerRotating = append([]int64(nil), record.EagerRotating...)
	return record
}

func newPlumtreeStatsEagerSnapshot(
	simple []PeerID,
	rotating []PeerID,
) (plumtreeStatsEagerSnapshot, error) {
	simplePeers, err := newPlumtreeStatsEagerPeers(simple)
	if err != nil {
		return plumtreeStatsEagerSnapshot{}, err
	}

	rotatingPeers, err := newPlumtreeStatsEagerPeers(rotating)
	if err != nil {
		return plumtreeStatsEagerSnapshot{}, err
	}

	return plumtreeStatsEagerSnapshot{
		simple:   simplePeers,
		rotating: rotatingPeers,
	}, nil
}
