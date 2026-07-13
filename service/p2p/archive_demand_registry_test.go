package p2p

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/gton/service/archive"
)

func TestArchiveDemandSchedulerVisitsEveryActiveDemand(t *testing.T) {
	const (
		demandCount = 9
		batchSize   = 4
	)

	shard := archive.ShardID{Workchain: 0, Shard: topShard}
	sub := testOverlaySubscription(&overlaySubscription{log: discardLogger()})
	pool := testArchivePool(t, sub)

	releases := make([]func(), 0, demandCount)
	active := make(map[uint64]struct{}, demandCount)
	for seqno := range demandCount {
		probe, release, err := pool.beginArchiveRequest(shard, uint32(seqno+1))
		if err != nil {
			t.Fatalf("begin demand %d: %v", seqno, err)
		}
		releases = append(releases, release)
		active[probe.demandID] = struct{}{}
	}
	defer func() {
		for _, release := range releases {
			release()
		}
	}()

	seen := make(map[uint64]struct{}, demandCount)
	batches := (demandCount + batchSize - 1) / batchSize
	for batch := 0; batch < batches; batch++ {
		probes := pool.probeSnapshots(batchSize)
		if len(probes) != batchSize {
			t.Fatalf("batch %d contains %d probes, want %d", batch, len(probes), batchSize)
		}

		inBatch := make(map[uint64]struct{}, batchSize)
		for _, probe := range probes {
			if _, ok := active[probe.demandID]; !ok {
				t.Fatalf("batch %d returned inactive demand %d", batch, probe.demandID)
			}
			if _, duplicate := inBatch[probe.demandID]; duplicate {
				t.Fatalf("batch %d returned demand %d twice", batch, probe.demandID)
			}
			inBatch[probe.demandID] = struct{}{}
			seen[probe.demandID] = struct{}{}
		}
	}

	if len(seen) != demandCount {
		t.Fatalf("scheduler visited %d of %d active demands in %d batches", len(seen), demandCount, batches)
	}
}

func TestArchiveDemandConcurrentReferenceAndEvidenceLifecycle(t *testing.T) {
	const (
		workers    = 64
		uniqueKeys = 8
	)

	shard := archive.ShardID{Workchain: 0, Shard: topShard}
	sub := testOverlaySubscription(&overlaySubscription{log: discardLogger()})
	pool := testArchivePool(t, sub)
	peer := testArchiveOnlyPoolPeer(t, pool, "concurrent-demand-evidence")

	type demandHandle struct {
		probe   archivePeerProbe
		release func()
	}
	handles := make([]demandHandle, workers)
	start := make(chan struct{})
	var beginWG sync.WaitGroup
	for worker := range workers {
		beginWG.Go(func() {
			<-start
			probe, release, err := pool.beginArchiveRequest(shard, uint32(worker%uniqueKeys))
			if err != nil {
				t.Errorf("worker %d begin demand: %v", worker, err)
				return
			}
			handles[worker] = demandHandle{probe: probe, release: release}
		})
	}
	close(start)
	beginWG.Wait()
	if t.Failed() {
		return
	}

	pool.mx.Lock()
	demandCount := len(pool.demands)
	demandKeyCount := len(pool.demandKeys)
	if demandCount != uniqueKeys || demandKeyCount != uniqueKeys {
		pool.mx.Unlock()
		t.Fatalf("active demand registry sizes = demands:%d keys:%d, want %d each", demandCount, demandKeyCount, uniqueKeys)
	}
	for demandID, demand := range pool.demands {
		if demand.refs != workers/uniqueKeys {
			pool.mx.Unlock()
			t.Fatalf("demand %d refs = %d, want %d", demandID, demand.refs, workers/uniqueKeys)
		}
		if pool.demandKeys[demand.key] != demandID {
			pool.mx.Unlock()
			t.Fatalf("demand %d key index points to %d", demandID, pool.demandKeys[demand.key])
		}
	}
	pool.mx.Unlock()

	var applyWG sync.WaitGroup
	for worker := range workers {
		applyWG.Go(func() {
			handle := handles[worker]
			result := archivePeerProbeResult{
				probe:          handle.probe,
				evidence:       archivePeerEvidenceProven,
				at:             time.Now(),
				bytes:          archiveSliceProbeSize,
				elapsed:        time.Second,
				bytesPerSecond: float64(worker+1) * 1024,
			}
			if !pool.applyArchivePeerEvidence(peer, result) {
				t.Errorf("worker %d could not apply evidence to its active demand", worker)
			}
			handle.release()
			handle.release()
		})
	}
	applyWG.Wait()

	pool.mx.Lock()
	defer pool.mx.Unlock()
	if len(pool.demands) != 0 || len(pool.demandKeys) != 0 {
		t.Fatalf("released demand registry sizes = demands:%d keys:%d, want zero", len(pool.demands), len(pool.demandKeys))
	}
	if pool.archiveOnlySizeLocked() > archivePeerRosterLimit {
		t.Fatalf("archive roster grew to %d peers, limit %d", pool.archiveOnlySizeLocked(), archivePeerRosterLimit)
	}
}

func TestArchiveDemandConcurrentAdmissionsRespectRosterLimit(t *testing.T) {
	const (
		candidates = archivePeerRosterLimit * 2
		uniqueKeys = 8
	)

	shard := archive.ShardID{Workchain: 0, Shard: topShard}
	sub := testOverlaySubscription(&overlaySubscription{log: discardLogger()})
	pool := testArchivePool(t, sub)

	ready := make(chan struct{}, candidates)
	start := make(chan struct{})
	var admissions sync.WaitGroup
	for candidateIndex := range candidates {
		admissions.Go(func() {
			probe, release, err := pool.beginArchiveRequest(shard, uint32(candidateIndex%uniqueKeys))
			if err != nil {
				t.Errorf("candidate %d begin demand: %v", candidateIndex, err)
				ready <- struct{}{}
				return
			}
			defer release()

			overlayWrapper, _ := newTestOverlayWrapper()
			peer := testArchiveCandidate(fmt.Sprintf("demand-admission-%d", candidateIndex))
			peer.overlay = overlayWrapper
			result := archivePeerProbeResult{
				probe:          probe,
				evidence:       archivePeerEvidenceProven,
				at:             time.Now(),
				bytes:          archiveSliceProbeSize,
				elapsed:        time.Second,
				bytesPerSecond: float64(candidateIndex+1) * 1024,
			}
			ready <- struct{}{}
			<-start

			admission := pool.admitArchiveOnlyPeer(peer, result)
			if admission.evicted != nil {
				closeArchiveOnlyPeer(admission.evicted)
			}
			if !admission.admitted {
				closeArchiveOnlyPeer(peer)
			}
		})
	}
	for range candidates {
		<-ready
	}
	close(start)
	admissions.Wait()
	if t.Failed() {
		return
	}

	pool.mx.Lock()
	roster := pool.archiveOnlySizeLocked()
	demands := len(pool.demands)
	demandKeys := len(pool.demandKeys)
	pool.mx.Unlock()
	if roster != archivePeerRosterLimit {
		t.Fatalf("archive roster after concurrent admissions = %d, want %d", roster, archivePeerRosterLimit)
	}
	if demands != 0 || demandKeys != 0 {
		t.Fatalf("released admission demands = demands:%d keys:%d, want zero", demands, demandKeys)
	}
}

func TestArchiveDemandOperationsRacingCloseLeaveNoState(t *testing.T) {
	const workers = 32

	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	sub := testOverlaySubscription(&overlaySubscription{log: discardLogger()})
	pool := testArchivePool(t, sub)
	peer := testArchiveOnlyPoolPeer(t, pool, "demand-close-race")

	type demandHandle struct {
		probe   archivePeerProbe
		release func()
	}
	handles := make([]demandHandle, workers)
	for worker := range workers {
		probe, release, err := pool.beginArchiveRequest(shard, uint32(worker%4))
		if err != nil {
			t.Fatalf("begin demand %d: %v", worker, err)
		}
		handles[worker] = demandHandle{probe: probe, release: release}
	}

	start := make(chan struct{})
	started := make(chan struct{}, workers)
	var operations sync.WaitGroup
	for worker := range workers {
		operations.Go(func() {
			handle := handles[worker]
			result := archivePeerProbeResult{
				probe:    handle.probe,
				evidence: archivePeerEvidenceAvailable,
				at:       time.Now(),
			}
			started <- struct{}{}
			<-start

			for range 32 {
				pool.applyArchivePeerEvidence(peer, result)
				pool.probeSnapshots(4)
			}
			handle.release()
			handle.release()

			probe, release, err := pool.beginArchiveRequest(shard, uint32(1000+worker))
			if err == nil {
				pool.applyArchivePeerEvidence(peer, archivePeerProbeResult{
					probe:    probe,
					evidence: archivePeerEvidenceAvailable,
					at:       time.Now(),
				})
				release()
				return
			}
			if !errors.Is(err, errArchiveSessionClosed) {
				t.Errorf("worker %d begin after close race: %v", worker, err)
			}
		})
	}
	for range workers {
		<-started
	}

	close(start)
	pool.Close()
	operations.Wait()

	pool.mx.Lock()
	closed := pool.closed
	demands := len(pool.demands)
	demandKeys := len(pool.demandKeys)
	peers := len(pool.peers)
	scouting := len(pool.scouting)
	pool.mx.Unlock()
	if !closed || demands != 0 || demandKeys != 0 || peers != 0 || scouting != 0 {
		t.Fatalf("closed pool retained state: closed=%v demands=%d keys=%d peers=%d scouting=%d", closed, demands, demandKeys, peers, scouting)
	}

	probe, release, err := pool.beginArchiveRequest(shard, 9999)
	if release != nil || probe.demandID != 0 || !errors.Is(err, errArchiveSessionClosed) {
		t.Fatalf("begin after Close = probe:%+v release:%v err:%v", probe, release != nil, err)
	}
	if pool.applyArchivePeerEvidence(peer, archivePeerProbeResult{
		probe:    handles[0].probe,
		evidence: archivePeerEvidenceAvailable,
		at:       time.Now(),
	}) {
		t.Fatal("stale evidence mutated a closed pool")
	}
	if got := pool.archiveOnlySize(); got > archivePeerRosterLimit {
		t.Fatalf("closed archive roster contains %d peers, limit %d", got, archivePeerRosterLimit)
	}
}
