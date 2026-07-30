package service

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

// shardApplyAheadPendingLimit bounds how many applied masters wait for the
// stage. The stage keeps the LOWEST pending seqnos, because those are the ones
// the commit stage reaches next: keeping the newest instead would leave a gap
// the contiguity gate below has to reject, which is exactly the regime (a
// master-apply stage running ahead during catch-up) where the stage is worth
// having. It stays small because every master it resolves applies shard states
// into the shared checkpoint window before its own master is committed.
const shardApplyAheadPendingLimit = 4

// shardAheadRecorderEntry keeps the master seqno alongside the recorder so the
// bounded map evicts consumed masters instead of the one in flight.
type shardAheadRecorderEntry struct {
	recorder    *shardObtainRecorder
	masterSeqno uint32
}

type shardApplyAheadJob struct {
	master  *storage.BlockState
	targets []ton.BlockIDExt
}

// startShardApplyAhead runs the shard resolution of an already-applied master
// block ahead of the commit stage. The commit stage resolves the very same
// targets through the same resolver, so this stage adds no work of its own: its
// results are picked up from the resolver cache, or the commit joins the task
// mid-flight. Joining also means a failure here is delivered to the commit
// stage verbatim — which is the same failure it would have produced itself,
// since it is the same resolution.
//
// It returns the stop func the run must call before it returns: the stage
// applies shard states into the run's cell window and collects their deferred
// checkpoint staging, so it must not outlive the run that owns them.
//
// Exactly one master is resolved at a time, so at most one master's shard
// states are applied before their own master block is committed. Applying
// ahead is durable-safe because only caches and live artifacts are published
// at apply time: the checkpoint staging of each shard block is deferred until
// the commit of its inclusion master (afterApplyShardState), so a checkpoint
// taken in that window cannot persist a shard block whose MasterchainRefSeqno
// points to a master absent from storage after an unclean restart.
func (r *nextSyncRunner) startShardApplyAhead() func() {
	r.shardAheadPending = map[uint32]shardApplyAheadJob{}
	r.shardAheadWake = make(chan struct{}, 1)

	done := make(chan struct{})
	go func() {
		defer close(done)
		r.runShardApplyAhead()
	}()

	return func() {
		r.cancel()
		r.wakeShardApplyAhead()
		<-done
	}
}

// scheduleShardApplyAhead never blocks the master apply loop: when the stage is
// already behind, the hint is dropped and the commit stage resolves the master
// itself, exactly as it did before this stage existed.
func (r *nextSyncRunner) scheduleShardApplyAhead(master *storage.BlockState, targets []ton.BlockIDExt) {
	if master == nil || len(targets) == 0 {
		return
	}

	seqno := master.Block.SeqNo
	committed := r.committedMasterSeqno.Load()

	r.shardAheadMu.Lock()
	if r.shardAheadPending == nil || seqno <= committed {
		r.shardAheadMu.Unlock()
		return
	}
	for pending := range r.shardAheadPending {
		if pending <= committed {
			delete(r.shardAheadPending, pending)
		}
	}
	r.shardAheadPending[seqno] = shardApplyAheadJob{master: master, targets: targets}
	for len(r.shardAheadPending) > shardApplyAheadPendingLimit {
		highest := uint32(0)
		for pending := range r.shardAheadPending {
			if pending > highest {
				highest = pending
			}
		}
		delete(r.shardAheadPending, highest)
	}
	r.shardAheadMu.Unlock()

	r.wakeShardApplyAhead()
}

func (r *nextSyncRunner) wakeShardApplyAhead() {
	if r.shardAheadWake == nil {
		return
	}
	select {
	case r.shardAheadWake <- struct{}{}:
	default:
	}
}

// takeShardApplyAheadJob returns the pending master this stage may resolve
// without becoming the owner of an earlier master's shard blocks.
func (r *nextSyncRunner) takeShardApplyAheadJob(lastAhead uint32) (shardApplyAheadJob, bool) {
	committed := r.committedMasterSeqno.Load()

	r.shardAheadMu.Lock()
	defer r.shardAheadMu.Unlock()

	for pending := range r.shardAheadPending {
		if pending <= committed {
			delete(r.shardAheadPending, pending)
		}
	}
	for _, seqno := range []uint32{lastAhead + 1, committed + 1} {
		if !shardApplyAheadContiguous(seqno, lastAhead, committed) {
			continue
		}
		if job, ok := r.shardAheadPending[seqno]; ok {
			delete(r.shardAheadPending, seqno)
			return job, true
		}
	}
	return shardApplyAheadJob{}, false
}

func (r *nextSyncRunner) runShardApplyAhead() {
	// lastAhead is the last masterchain seqno this stage resolved; it is only
	// touched by this goroutine.
	var lastAhead uint32

	for {
		job, ok := r.takeShardApplyAheadJob(lastAhead)
		if !ok {
			select {
			case <-r.ctx.Done():
				return
			case <-r.shardAheadWake:
				continue
			}
		}
		if r.ctx.Err() != nil {
			return
		}

		if !r.applyShardTargetsAhead(job) {
			// A target this master includes stayed unresolved. Admitting the
			// next master would let its prev chain reach that hole and make this
			// stage its owner, stamping it with the later master.
			lastAhead = 0
			continue
		}
		lastAhead = job.master.Block.SeqNo
	}
}

// shardApplyAheadContiguous reports that resolving this master cannot make the
// stage the owner of a shard block belonging to an earlier master: either it
// directly follows the master this stage just resolved, or it directly follows
// the committed head (whose shard blocks are all resolved and stamped).
func shardApplyAheadContiguous(seqno, lastAhead, committed uint32) bool {
	if lastAhead != 0 && seqno == lastAhead+1 {
		return true
	}
	return committed != 0 && seqno == committed+1
}

// applyShardTargetsAhead returns false when any of the master's targets was
// left unresolved, so the caller can keep the contiguity gate honest.
func (r *nextSyncRunner) applyShardTargetsAhead(job shardApplyAheadJob) bool {
	// r.ctx unmodified except for values: the resolver hands one task's error to
	// every waiter, so a cancellable context here would let this stage fail the
	// commit stage's resolution of the same block.
	ctx := contextWithShardObtainRecorder(r.ctx, r.shardAheadRecorder(job.master.Block))
	ctx = contextWithShardApplyMaster(ctx, job.master)

	// Bounded by GOMAXPROCS as well as by the target count: these workers run
	// alongside the commit stage's own pool, the shard prefetchers and the
	// decode pool, and this stage is the one that may yield to the others.
	workers := min(archiveShardApplyParallelism, runtime.GOMAXPROCS(0))
	if len(job.targets) < workers {
		workers = len(job.targets)
	}
	if workers < 1 {
		workers = 1
	}

	targets := make(chan ton.BlockIDExt)
	var resolved atomic.Int32
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for target := range targets {
				if r.ctx.Err() != nil {
					continue
				}
				if r.shardResolver.hasCurrentBlock(target) {
					// The commit stage carries shard tips over without touching
					// the resolver, which is what keeps their original inclusion
					// master; resolving them here would re-stamp them.
					resolved.Add(1)
					continue
				}
				if _, err := r.shardResolver.resolveWithContext(ctx, target); err != nil {
					if r.ctx.Err() == nil {
						r.service.log.Debug().
							Err(err).
							Stringer("masterchain", storage.BlockRef(job.master.Block)).
							Stringer("target", storage.BlockRef(target)).
							Msg("shard apply-ahead failed, leaving the target to the commit stage")
					}
					continue
				}
				resolved.Add(1)
			}
		}()
	}

feed:
	for _, target := range job.targets {
		select {
		case targets <- target:
		case <-r.ctx.Done():
			break feed
		}
	}
	close(targets)
	wg.Wait()

	return int(resolved.Load()) == len(job.targets)
}

// shardAheadRecorder returns the obtain recorder for a master block, creating
// it on first use. The commit stage takes the same recorder so downloads paid
// for by this stage still show up in that master block's obtain stats.
func (r *nextSyncRunner) shardAheadRecorder(master ton.BlockIDExt) *shardObtainRecorder {
	key := storage.BlockKey(master)

	r.shardAheadMu.Lock()
	defer r.shardAheadMu.Unlock()

	if r.shardAheadRecorders == nil {
		r.shardAheadRecorders = map[storage.BlockRootHash]shardAheadRecorderEntry{}
	}
	if entry, ok := r.shardAheadRecorders[key]; ok {
		return entry.recorder
	}

	// The commit stage removes each entry as it consumes it, so this map holds
	// at most the masters in flight. Entries at or below the committed head can
	// no longer be consumed (the pipeline yielded or failed before committing
	// them), so they are the only safe ones to drop — evicting by age would
	// throw away exactly the entry the commit stage is about to take, since the
	// commit runs behind this stage.
	committed := r.committedMasterSeqno.Load()
	for key, entry := range r.shardAheadRecorders {
		if entry.masterSeqno <= committed {
			delete(r.shardAheadRecorders, key)
		}
	}

	recorder := newShardObtainRecorder()
	r.shardAheadRecorders[key] = shardAheadRecorderEntry{
		recorder:    recorder,
		masterSeqno: master.SeqNo,
	}
	return recorder
}

// shardApplyAheadContext is the context a caller must hand to
// currentStateForNextMasterState so a commit and the ahead stage share one
// obtain recorder for this master block. It creates the recorder when the
// commit gets there first (the job may still be pending), so the two stages
// cannot end up with one recorder each and lose half the downloads.
func (r *nextSyncRunner) shardApplyAheadContext(ctx context.Context, master ton.BlockIDExt) context.Context {
	return contextWithShardObtainRecorder(ctx, r.shardAheadRecorder(master))
}
