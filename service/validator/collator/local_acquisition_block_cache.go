package collator

import (
	"context"
	"fmt"
	"sync"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
)

const (
	// maxLocalBlockSources bounds the cache. Entries are only ever evicted,
	// never invalidated, so the bound is about memory rather than freshness.
	maxLocalBlockSources = 256
	// localBlockSourceGenerations keeps an entry alive across this many
	// masterchain view installations. Two is enough to survive the window where
	// the collating slot still references the previous view.
	localBlockSourceGenerations = 2
)

// localBlockSource is one verified read of a block and its state, shared by
// every collation and validation that references that block.
//
// It deliberately holds no cell.MerkleProofBuilder: a read set only ever
// accumulates reads, so a shared builder would emit the union of everything any
// slot ever opened. The traced view is therefore rebuilt per collation, and only
// the untraced read plus the derivations that do not depend on the imported cut
// are cached.
//
// It deliberately holds no decoded outbound queue either: the queue and the
// processed frontier are only ever consumed through a proof-carrying
// localNeighborView, which localViewFromPrevious builds per collation from the
// state root cached here.
type localBlockSource struct {
	previous PreviousBlock
	state    tlb.ShardStateUnsplit

	masterOnce     sync.Once
	masterGenLT    uint64
	masterRegistry *ShardRegistry
	masterErr      error

	generation uint64
}

// masterShardRegistry memoises the masterchain shard configuration of this
// block. historicalShardEndLT asks for it once per distinct frontier
// masterchain block, on every collation and every candidate validation.
func (s *localBlockSource) masterShardRegistry() (uint64, *ShardRegistry, error) {
	s.masterOnce.Do(func() {
		if s.state.McStateExtra == nil {
			s.masterErr = fmt.Errorf("%w: historical masterchain state extra is absent", ErrInvalidInput)
			return
		}
		var extra tlb.McStateExtra
		if s.masterErr = parseExact(&extra, s.state.McStateExtra); s.masterErr != nil {
			s.masterErr = fmt.Errorf("%w: decode historical masterchain extra: %v", ErrInvalidInput, s.masterErr)
			return
		}
		s.masterRegistry, s.masterErr = ParseShardRegistry(extra.ShardHashes)
		s.masterGenLT = s.state.GenLT
	})
	if s.masterErr != nil {
		return 0, nil, s.masterErr
	}

	return s.masterGenLT, s.masterRegistry, nil
}

// localBlockCache is content-addressed by block root hash. A block root hash
// can never map to two different states — verifyPredecessor binds the block's
// state update to exactly this state root — so a hit is always valid and the
// cache needs eviction but never invalidation.
type localBlockCache struct {
	mu         sync.Mutex
	generation uint64
	entries    map[[32]byte]*localBlockSource
	// loads holds the reads currently in flight, so that the collation, the
	// validation and the neighbour warm-up asking for the same block at the
	// same time read it once. A read is a state-tree root load, a block root
	// load and a full predecessor verification, and the same neighbour is asked
	// for by every one of them at the start of a slot.
	loads map[[32]byte]*localBlockLoad
}

// localBlockLoad is one in-flight read. source and err are written before done
// is closed and only read after it, so the close is their publication.
type localBlockLoad struct {
	done   chan struct{}
	source *localBlockSource
	err    error
}

// loadOnce returns the cached read of id, performing load at most once across
// concurrent callers. A failed read is never cached: the flight is dropped
// either way, so the next caller retries.
//
// The result still goes through store, which is what keeps content addressing
// authoritative — a caller that joined a flight gets the same entry a caller
// that missed it would have got.
func (c *localBlockCache) loadOnce(
	ctx context.Context,
	id ton.BlockIDExt,
	load func() (*localBlockSource, error),
) (*localBlockSource, error) {
	key, err := blockRootKey(id)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	if flight, joined := c.loads[key]; joined {
		c.mu.Unlock()
		select {
		case <-flight.done:
		case <-ctx.Done():
			// Only this caller leaves; the read itself is shared work and runs
			// to completion for whoever is still waiting on it.
			return nil, ctx.Err()
		}
		if flight.err != nil {
			return nil, flight.err
		}

		return flight.source, nil
	}
	flight := &localBlockLoad{done: make(chan struct{})}
	if c.loads == nil {
		c.loads = make(map[[32]byte]*localBlockLoad)
	}
	c.loads[key] = flight
	c.mu.Unlock()

	source, err := load()
	if err == nil {
		source, err = c.store(source)
	}
	flight.source, flight.err = source, err

	c.mu.Lock()
	delete(c.loads, key)
	c.mu.Unlock()
	close(flight.done)

	return source, err
}

func (c *localBlockCache) lookup(id ton.BlockIDExt) (*localBlockSource, error) {
	key, err := blockRootKey(id)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	entry := c.entries[key]
	if entry == nil {
		return nil, ErrNotFound
	}
	if !entry.previous.ID.Equals(&id) {
		return nil, fmt.Errorf("%w: cached block %x belongs to another block id", ErrInvalidInput, key)
	}
	entry.generation = c.generation

	return entry, nil
}

func (c *localBlockCache) store(source *localBlockSource) (*localBlockSource, error) {
	key, err := blockRootKey(source.previous.ID)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if current, exists := c.entries[key]; exists {
		// The same guard lookup applies: content addressing is what makes a hit
		// safe, so the incumbent must be the same block, not merely the same
		// root hash.
		if !current.previous.ID.Equals(&source.previous.ID) {
			return nil, fmt.Errorf("%w: cached block %x belongs to another block id", ErrInvalidInput, key)
		}
		current.generation = c.generation
		return current, nil
	}
	if c.entries == nil {
		c.entries = make(map[[32]byte]*localBlockSource)
	}
	source.generation = c.generation
	c.entries[key] = source
	for len(c.entries) > maxLocalBlockSources {
		oldest := key
		oldestGeneration := source.generation
		for candidate, entry := range c.entries {
			if entry.generation <= oldestGeneration {
				oldest, oldestGeneration = candidate, entry.generation
			}
		}
		delete(c.entries, oldest)
	}

	return source, nil
}

// advance retires entries that no live masterchain view references any more.
// A registered neighbor top can only change when the masterchain view moves, so
// view installation is the exact event-driven bound on entry lifetime.
func (c *localBlockCache) advance() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.generation++
	for key, entry := range c.entries {
		if entry.generation+localBlockSourceGenerations <= c.generation {
			delete(c.entries, key)
		}
	}
}
