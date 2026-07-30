package p2p

import (
	"encoding/binary"
	"fmt"
	"math"
	"sync"
	"time"
)

const (
	plumtreeStatsStoreLimit         = 100
	plumtreeStatsLatencyBucketCount = 100
	plumtreeStatsLatencyBucketMS    = uint32(10)
	plumtreeStatsEpochDuration      = time.Hour
	plumtreeStatsSnapshotDelay      = plumtreeStatsEpochDuration / 12
	plumtreeStatsMaximumSendJitter  = plumtreeStatsEpochDuration / 12
	plumtreeStatsMaximumJitterStep  = uint32(1000)
	plumtreeStatsJitterStepDuration = plumtreeStatsMaximumSendJitter / time.Duration(plumtreeStatsMaximumJitterStep)
	plumtreeStatsRecordMapCapacity  = plumtreeStatsStoreLimit + 1
)

type plumtreeStatsJitter struct {
	delay time.Duration
}

type plumtreeStatsEagerPeers struct {
	peers [plumtreeRegularEagerLimit]PeerID
	count uint8
}

func (p *plumtreeStatsEagerPeers) contains(peer PeerID) bool {
	for i := range int(p.count) {
		if p.peers[i] == peer {
			return true
		}
	}

	return false
}

type plumtreeStatsEagerSnapshot struct {
	simple   plumtreeStatsEagerPeers
	rotating plumtreeStatsEagerPeers
}

type plumtreeStatsForward struct {
	// record aliases the immutable epoch store so the runtime can serialize it
	// once for every destination without per-peer clones.
	ready  bool
	record PlumtreeStatsRecord
	to     [plumtreeRegularEagerLimit]PeerID
	count  uint8
}

type plumtreeStatsTickResult struct {
	snapshotCreated bool
	snapshot        PlumtreeStatsRecord
	forward         plumtreeStatsForward
}

type plumtreeStatsPushResult struct {
	accepted bool
	forward  plumtreeStatsForward
}

type plumtreeStatsSchedule struct {
	epoch        int64
	rotatingTree int32
	snapshotAt   time.Time
	sendAt       time.Time
	nextEpochAt  time.Time
	snapshotDone bool
	sendDone     bool
	skipped      bool
}

type plumtreeStatsSnapshot struct {
	deliveredBroadcast int32
	partsInP99         int32
	usefulAverageMS    int32
	usefulMaximumMS    int32
	usefulP99MS        int32
}

type plumtreeStatsCounters struct {
	deliveredBroadcast uint64
	fecPartsBuckets    [plumtreeFECTotalParts + 1]uint64
	usefulCount        uint64
	usefulSumMS        uint64
	usefulMaximumMS    uint32
	usefulBuckets      [plumtreeStatsLatencyBucketCount]uint64
}

func (c *plumtreeStatsCounters) noteDeliveredBroadcast() {
	c.deliveredBroadcast++
}

func (c *plumtreeStatsCounters) noteFECPartsCollected(parts uint32) {
	bucket := min(parts, uint32(plumtreeFECTotalParts))
	c.fecPartsBuckets[bucket]++
}

func (c *plumtreeStatsCounters) noteUsefulDelivery(valueMS uint32) {
	c.usefulCount++
	c.usefulSumMS += uint64(valueMS)
	c.usefulMaximumMS = max(c.usefulMaximumMS, valueMS)

	bucket := min(
		valueMS/plumtreeStatsLatencyBucketMS,
		uint32(plumtreeStatsLatencyBucketCount-1),
	)
	c.usefulBuckets[bucket]++
}

func (c *plumtreeStatsCounters) snapshotAndReset() plumtreeStatsSnapshot {
	snapshot := plumtreeStatsSnapshot{
		deliveredBroadcast: plumtreeStatsToInt32(c.deliveredBroadcast),
		partsInP99:         c.partsInP99(),
		usefulAverageMS:    c.usefulAverage(),
		usefulMaximumMS:    plumtreeStatsToInt32(uint64(c.usefulMaximumMS)),
		usefulP99MS:        c.usefulP99(),
	}

	*c = plumtreeStatsCounters{}
	return snapshot
}

func (c *plumtreeStatsCounters) partsInP99() int32 {
	var total uint64
	for _, count := range c.fecPartsBuckets {
		total += count
	}
	if total == 0 {
		return 0
	}

	target := total/100 + 1
	var seen uint64
	for index, count := range c.fecPartsBuckets {
		seen += count
		if seen >= target {
			return int32(index)
		}
	}

	return plumtreeFECTotalParts
}

func (c *plumtreeStatsCounters) usefulAverage() int32 {
	if c.usefulCount == 0 {
		return 0
	}

	return plumtreeStatsToInt32(c.usefulSumMS / c.usefulCount)
}

func (c *plumtreeStatsCounters) usefulP99() int32 {
	if c.usefulCount == 0 {
		return 0
	}

	target := c.usefulCount - c.usefulCount/100
	var seen uint64
	for index, count := range c.usefulBuckets {
		seen += count
		if seen >= target {
			return int32(index+1) * int32(plumtreeStatsLatencyBucketMS)
		}
	}

	return int32(plumtreeStatsLatencyBucketCount) * int32(plumtreeStatsLatencyBucketMS)
}

func plumtreeStatsToInt32(value uint64) int32 {
	if value > math.MaxInt32 {
		return math.MaxInt32
	}

	return int32(value)
}

type plumtreeStatsEpoch struct {
	initialized  bool
	epoch        int64
	rotatingTree int32
	snapshotAt   time.Time
	sendAt       time.Time
	nextEpochAt  time.Time
	snapshotDone bool
	sendDone     bool
	skipped      bool
	store        map[PeerID]PlumtreeStatsRecord
}

type plumtreeStats struct {
	mu sync.Mutex

	counters plumtreeStatsCounters
	epoch    plumtreeStatsEpoch
}

func (s *plumtreeStats) NoteDeliveredBroadcast() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.counters.noteDeliveredBroadcast()
}

func (s *plumtreeStats) NoteFECPartsCollected(parts uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.counters.noteFECPartsCollected(parts)
}

func (s *plumtreeStats) NoteUsefulDelivery(now time.Time, timestamp float64) {
	nowSeconds := float64(now.Unix()) +
		float64(now.Nanosecond())/float64(time.Second)
	durationMS := (nowSeconds - timestamp) * float64(time.Second/time.Millisecond)
	if math.IsNaN(durationMS) || math.IsInf(durationMS, 0) {
		return
	}
	if durationMS < 0 {
		durationMS = 0
	}

	var valueMS uint32
	if durationMS > math.MaxUint32 {
		valueMS = math.MaxUint32
	} else {
		valueMS = uint32(durationMS + 0.5)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.counters.noteUsefulDelivery(valueMS)
}

func (s *plumtreeStats) Schedule(
	now time.Time,
	jitter plumtreeStatsJitter,
) plumtreeStatsSchedule {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.syncEpochLocked(now, jitter)
	return s.scheduleLocked()
}

func (s *plumtreeStats) NextAt(now time.Time, jitter plumtreeStatsJitter) time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.syncEpochLocked(now, jitter)
	if !s.epoch.snapshotDone {
		return s.epoch.snapshotAt
	}
	if !s.epoch.sendDone {
		return s.epoch.sendAt
	}

	return s.epoch.nextEpochAt
}

func (s *plumtreeStats) Tick(
	now time.Time,
	jitter plumtreeStatsJitter,
	localID PeerID,
	eager *plumtreeStatsEagerSnapshot,
) plumtreeStatsTickResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.syncEpochLocked(now, jitter)

	var result plumtreeStatsTickResult
	if !s.epoch.snapshotDone && !now.Before(s.epoch.snapshotAt) {
		s.epoch.snapshotDone = true

		values := s.counters.snapshotAndReset()
		record := PlumtreeStatsRecord{
			Source:             localID.Bytes(),
			Epoch:              int32(s.epoch.epoch),
			TreeIndex:          s.epoch.rotatingTree,
			EagerSimple:        plumtreeStatsEagerPrefixes(&eager.simple),
			EagerRotating:      plumtreeStatsEagerPrefixes(&eager.rotating),
			DeliveredBroadcast: values.deliveredBroadcast,
			PartsInP99:         values.partsInP99,
			UsefulAverageMS:    values.usefulAverageMS,
			UsefulMaximumMS:    values.usefulMaximumMS,
			UsefulP99MS:        values.usefulP99MS,
		}

		s.ensureStoreLocked()
		s.epoch.store[localID] = record
		result.snapshotCreated = true
		result.snapshot = record
	}

	if s.epoch.snapshotDone && !s.epoch.sendDone && !now.Before(s.epoch.sendAt) {
		s.epoch.sendDone = true

		record, exists := s.epoch.store[localID]
		if exists {
			result.forward = plumtreeStatsForwardRecord(
				record,
				&eager.simple,
				PeerID{},
			)
		}
	}

	return result
}

// HandlePush takes ownership of the slices in an accepted push. The stored
// record and returned forwarding view are immutable after this method returns.
func (s *plumtreeStats) HandlePush(
	now time.Time,
	jitter plumtreeStatsJitter,
	localID PeerID,
	from PeerID,
	simpleEager *plumtreeStatsEagerPeers,
	push PlumtreeStatsPush,
) (plumtreeStatsPushResult, error) {
	if from.IsZero() {
		return plumtreeStatsPushResult{}, fmt.Errorf("missing Plumtree immediate sender")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.syncEpochLocked(now, jitter)

	record := push.Record
	if record.TreeIndex != s.epoch.rotatingTree {
		return plumtreeStatsPushResult{}, nil
	}
	if s.epoch.skipped {
		return plumtreeStatsPushResult{}, nil
	}
	if !simpleEager.contains(from) {
		return plumtreeStatsPushResult{}, nil
	}

	source, err := NewPeerID(record.Source)
	if err != nil || source.IsZero() || source == localID {
		return plumtreeStatsPushResult{}, nil
	}
	if record.Epoch != int32(s.epoch.epoch) {
		return plumtreeStatsPushResult{}, nil
	}
	if len(record.EagerSimple) > plumtreeRegularEagerLimit ||
		len(record.EagerRotating) > plumtreeRegularEagerLimit {
		return plumtreeStatsPushResult{}, nil
	}
	if record.DeliveredBroadcast < 0 ||
		record.PartsInP99 < 0 ||
		record.UsefulAverageMS < 0 ||
		record.UsefulMaximumMS < 0 ||
		record.UsefulP99MS < 0 {
		return plumtreeStatsPushResult{}, nil
	}
	if record.PartsInP99 > plumtreeFECTotalParts {
		return plumtreeStatsPushResult{}, nil
	}
	if len(s.epoch.store) >= plumtreeStatsStoreLimit {
		return plumtreeStatsPushResult{}, nil
	}

	if _, exists := s.epoch.store[source]; exists {
		return plumtreeStatsPushResult{}, nil
	}

	s.ensureStoreLocked()
	s.epoch.store[source] = record

	return plumtreeStatsPushResult{
		accepted: true,
		forward:  plumtreeStatsForwardRecord(record, simpleEager, from),
	}, nil
}

func (s *plumtreeStats) syncEpochLocked(now time.Time, jitter plumtreeStatsJitter) {
	epoch := now.Unix() / int64(plumtreeStatsEpochDuration/time.Second)
	if s.epoch.initialized && epoch <= s.epoch.epoch {
		return
	}

	epochStart := time.Unix(
		epoch*int64(plumtreeStatsEpochDuration/time.Second),
		0,
	)
	s.epoch = plumtreeStatsEpoch{
		initialized:  true,
		epoch:        epoch,
		rotatingTree: plumtreeFECTreeOffset + int32(epoch%int64(plumtreeTreeSlots-1)),
		snapshotAt:   epochStart.Add(plumtreeStatsSnapshotDelay),
		sendAt:       epochStart.Add(plumtreeStatsSnapshotDelay + jitter.delay),
		nextEpochAt:  epochStart.Add(plumtreeStatsEpochDuration),
	}
	if !now.Before(s.epoch.snapshotAt) {
		s.epoch.snapshotDone = true
		s.epoch.sendDone = true
		s.epoch.skipped = true
	}
}

func (s *plumtreeStats) scheduleLocked() plumtreeStatsSchedule {
	return plumtreeStatsSchedule{
		epoch:        s.epoch.epoch,
		rotatingTree: s.epoch.rotatingTree,
		snapshotAt:   s.epoch.snapshotAt,
		sendAt:       s.epoch.sendAt,
		nextEpochAt:  s.epoch.nextEpochAt,
		snapshotDone: s.epoch.snapshotDone,
		sendDone:     s.epoch.sendDone,
		skipped:      s.epoch.skipped,
	}
}

func (s *plumtreeStats) ensureStoreLocked() {
	if s.epoch.store == nil {
		s.epoch.store = make(
			map[PeerID]PlumtreeStatsRecord,
			plumtreeStatsRecordMapCapacity,
		)
	}
}

func plumtreeStatsEagerPrefixes(peers *plumtreeStatsEagerPeers) []int64 {
	if peers.count == 0 {
		return nil
	}

	prefixes := make([]int64, peers.count)
	for i := range int(peers.count) {
		prefixes[i] = int64(binary.LittleEndian.Uint64(peers.peers[i][:8]))
	}

	return prefixes
}

func plumtreeStatsForwardRecord(
	record PlumtreeStatsRecord,
	eager *plumtreeStatsEagerPeers,
	except PeerID,
) plumtreeStatsForward {
	result := plumtreeStatsForward{
		record: record,
	}
	for i := range int(eager.count) {
		peer := eager.peers[i]
		if peer == except {
			continue
		}

		result.to[result.count] = peer
		result.count++
	}
	result.ready = result.count != 0

	return result
}
