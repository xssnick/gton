package collator

import (
	"context"
	"sync"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

// accountSiblingPrefetch resolves the untouched siblings of the predecessor
// ShardAccounts spines while execution is still running.
//
// Every fork node on a touched account's path has one child the update descends
// into and one it does not. The bulk write at the end of the collation still has
// to read that untouched child, because it re-hashes the fork from both
// branches, and on a store-backed predecessor each one of them is a cell load.
// Measured on the mainnet fixture, those loads are the whole store traffic of
// the final account stage — 1848 reads, every one inside the ShardAccounts bulk
// write, none in the per-lane build and none in the account-block write — and
// they are almost pure latency: 0.33 us of decode each. Paying them at the end
// serializes them behind the block; resolving them here overlaps them with the
// machine, which is the only place in the collation with time to hide them in.
//
// The prefetch is an optimization and never a precondition. A sibling that was
// not resolved — the job has not run yet, the loader failed, the context was
// cancelled — is simply absent from the handoff, and the bulk write loads it
// itself exactly as it does today. Nothing about the produced block depends on
// which siblings made it.
type accountSiblingPrefetch struct {
	ctx context.Context
	// limit bounds concurrent STORE READS rather than CPU: the decode behind one
	// read is 0.33 us, so the whole set is under a millisecond of work and the
	// budget only has to be wide enough to keep the store busy.
	limit chan struct{}
	wg    sync.WaitGroup

	// seen holds both the path nodes and the siblings already scheduled. A node
	// that lies on some lane's path is never a sibling worth loading — the walk
	// descends into it — and the top of the tree is shared by every lane, so
	// without the cross-lane half of this set the same handful of cells near the
	// root would be scheduled once per account.
	//
	// It is written only by submit, which needs no lock for the same reason
	// collation.lanes needs none: account() is its only caller and runs on the
	// collation goroutine.
	seen map[cell.Hash]struct{}

	mu    sync.Mutex
	cells []*cell.Cell
}

func newAccountSiblingPrefetch(ctx context.Context) *accountSiblingPrefetch {
	return &accountSiblingPrefetch{
		ctx:   ctx,
		limit: make(chan struct{}, collationParallelism),
		seen:  make(map[cell.Hash]struct{}),
	}
}

// submit schedules the untouched siblings of one account's predecessor spine.
// It must be given the lane's own clone of the recorded path: the recorder
// reuses its buffer across lookups, so handing that buffer to a worker would
// race the next lookup.
func (p *accountSiblingPrefetch) submit(path []*cell.Cell) {
	if p == nil || len(path) == 0 {
		return
	}
	for _, node := range path {
		p.seen[node.HashKey()] = struct{}{}
	}

	var stubs []*cell.Cell
	for _, node := range path {
		// Only a Patricia fork has two references; a leaf carries the account
		// value inline and has nothing beside it to load.
		if node.RefsNum() != 2 {
			continue
		}
		for i := 0; i < 2; i++ {
			ref, err := node.PeekRef(i)
			if err != nil || ref == nil || !ref.IsLazy() {
				continue
			}
			hash := ref.HashKey()
			if _, ok := p.seen[hash]; ok {
				continue
			}
			p.seen[hash] = struct{}{}
			// Untraced on purpose. These loads must not enter the read set, or
			// the collated proof — and with it the block the candidate carries —
			// would move depending on how far the prefetch happened to get.
			// Nothing is stripped in practice today, for two reasons that are
			// both somebody else's: a path node resolved through the lazy loader
			// comes back without a trace, and the resident nodes that do carry
			// one have no lazy reference to schedule. Neither is a property of
			// this walk, so the strip is unconditional.
			stubs = append(stubs, ref.WithTrace(nil))
		}
	}
	if len(stubs) == 0 {
		return
	}

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for _, stub := range stubs {
			if p.ctx != nil && p.ctx.Err() != nil {
				return
			}
			p.limit <- struct{}{}
			resolved, err := stub.Prewarm()
			<-p.limit
			if err != nil || resolved == nil || resolved.IsLazy() {
				continue
			}
			p.mu.Lock()
			p.cells = append(p.cells, resolved)
			p.mu.Unlock()
		}
	}()
}

// join waits for the scheduled loads and returns what they resolved. The result
// is free of nil and of still-lazy cells because the bulk resolver refuses a
// loaded-path entry containing either, which would end the collation rather
// than slow it down.
func (p *accountSiblingPrefetch) join() []*cell.Cell {
	if p == nil {
		return nil
	}
	p.wg.Wait()
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cells
}
