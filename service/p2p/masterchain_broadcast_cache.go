package p2p

import (
	"container/list"
	"fmt"
	"sync"
	"time"

	tnstore "github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	masterchainNextBroadcastCacheTTL      = 3 * time.Minute
	masterchainNextBroadcastCacheMaxBytes = 256 << 20
	masterchainNextBroadcastCacheMaxItems = 4096
	masterchainNextBroadcastCacheOverhead = 256
)

type masterchainNextBroadcastCache struct {
	mu sync.Mutex

	ttl      time.Duration
	maxBytes int64
	maxItems int

	entries map[tnstore.BlockRootHash]*masterchainNextBroadcastCacheEntry
	order   *list.List
	bytes   int64
}

type masterchainNextBroadcastCacheEntry struct {
	key          tnstore.BlockRootHash
	element      *list.Element
	prev         ton.BlockIDExt
	block        ton.BlockIDExt
	kind         string
	blockRoot    *cell.Cell
	proofRoot    *cell.Cell
	stateUpdate  *cell.Cell
	blockBOC     []byte
	proofBOC     []byte
	isLink       bool
	meta         *tnstore.BlockMeta
	sourcePeerID PeerID
	expiresAt    time.Time
	bytes        int64
}

func newMasterchainNextBroadcastCache(ttl time.Duration, maxBytes int64, maxItems int) *masterchainNextBroadcastCache {
	return &masterchainNextBroadcastCache{
		ttl:      ttl,
		maxBytes: maxBytes,
		maxItems: maxItems,
		entries:  map[tnstore.BlockRootHash]*masterchainNextBroadcastCacheEntry{},
		order:    list.New(),
	}
}

func (c *masterchainNextBroadcastCache) Store(downloaded DownloadedBlock) error {
	return c.storeAt(downloaded, time.Now())
}

func (c *masterchainNextBroadcastCache) storeAt(downloaded DownloadedBlock, now time.Time) error {
	if c == nil {
		return tnstore.ErrNotFound
	}
	if !isMasterchainBlock(downloaded.ID) {
		return fmt.Errorf("block %s is not a masterchain next broadcast cache candidate", formatBlockRef(downloaded.ID))
	}
	if !downloaded.VerifiedRootHash {
		return fmt.Errorf("block %s is not hash verified", formatBlockRef(downloaded.ID))
	}
	if len(downloaded.BlockBOC) == 0 {
		return fmt.Errorf("block %s has empty block data", formatBlockRef(downloaded.ID))
	}
	if len(downloaded.ProofBOC) == 0 {
		return fmt.Errorf("block %s has empty proof", formatBlockRef(downloaded.ID))
	}
	if c.maxItems <= 0 || c.maxBytes <= 0 {
		return fmt.Errorf("masterchain next broadcast cache is disabled")
	}
	if downloaded.Meta == nil {
		return fmt.Errorf("block %s has no metadata", formatBlockRef(downloaded.ID))
	}
	if len(downloaded.Meta.PrevRefs) != 1 {
		return fmt.Errorf("block %s has %d previous refs", formatBlockRef(downloaded.ID), len(downloaded.Meta.PrevRefs))
	}
	prev := downloaded.Meta.PrevRefs[0]
	if !isMasterchainBlock(prev) {
		return fmt.Errorf("block %s previous ref %s is not masterchain", formatBlockRef(downloaded.ID), formatBlockRef(prev))
	}
	if downloaded.Block == nil {
		return fmt.Errorf("block %s has no parsed block root", formatBlockRef(downloaded.ID))
	}
	if downloaded.Proof == nil {
		return fmt.Errorf("block %s has no parsed proof root", formatBlockRef(downloaded.ID))
	}
	if downloaded.StateUpdate == nil {
		return fmt.Errorf("block %s has no state update", formatBlockRef(downloaded.ID))
	}

	size := masterchainNextBroadcastBlockCacheSize(downloaded.BlockBOC, downloaded.ProofBOC)
	if size > c.maxBytes {
		return fmt.Errorf("block %s is too large for masterchain next broadcast cache: %d > %d", formatBlockRef(downloaded.ID), size, c.maxBytes)
	}

	key := tnstore.BlockKey(prev)
	entry := &masterchainNextBroadcastCacheEntry{
		key:          key,
		prev:         cloneBlockID(prev),
		block:        cloneBlockID(downloaded.ID),
		kind:         downloaded.Kind,
		blockRoot:    downloaded.Block,
		proofRoot:    downloaded.Proof,
		stateUpdate:  downloaded.StateUpdate,
		blockBOC:     downloaded.BlockBOC,
		proofBOC:     downloaded.ProofBOC,
		isLink:       downloaded.IsLink,
		meta:         downloaded.Meta.Clone(),
		sourcePeerID: downloaded.SourcePeerID,
		expiresAt:    now.Add(c.ttl),
		bytes:        size,
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if old := c.entries[key]; old != nil {
		c.deleteEntryLocked(old)
	}

	entry.element = c.order.PushBack(entry)
	c.entries[key] = entry
	c.bytes += size

	c.pruneExpiredLocked(now)
	c.pruneOverflowLocked()
	return nil
}

func (c *masterchainNextBroadcastCache) BlockAfter(prev ton.BlockIDExt) (*DownloadedBlock, error) {
	return c.blockAfterAt(prev, time.Now())
}

func (c *masterchainNextBroadcastCache) blockAfterAt(prev ton.BlockIDExt, now time.Time) (*DownloadedBlock, error) {
	if c == nil || !isMasterchainBlock(prev) {
		return nil, tnstore.ErrNotFound
	}

	c.mu.Lock()
	key := tnstore.BlockKey(prev)
	entry := c.entries[key]
	if entry == nil {
		c.mu.Unlock()
		return nil, tnstore.ErrNotFound
	}
	if !entry.expiresAt.After(now) {
		c.deleteEntryLocked(entry)
		c.mu.Unlock()
		return nil, tnstore.ErrNotFound
	}

	kind := entry.kind
	if kind == "" {
		kind = "masterchain next broadcast cache"
	}
	downloaded := &DownloadedBlock{
		ID:               cloneBlockID(entry.block),
		Kind:             kind,
		Block:            entry.blockRoot,
		Proof:            entry.proofRoot,
		BlockBOC:         entry.blockBOC,
		ProofBOC:         entry.proofBOC,
		Meta:             entry.meta.Clone(),
		StateUpdate:      entry.stateUpdate,
		SourcePeerID:     entry.sourcePeerID,
		IsLink:           entry.isLink,
		VerifiedRootHash: true,
	}
	c.mu.Unlock()
	return downloaded, nil
}

func (c *masterchainNextBroadcastCache) pruneExpiredLocked(now time.Time) {
	for elem := c.order.Front(); elem != nil; {
		entry := elem.Value.(*masterchainNextBroadcastCacheEntry)
		if !entry.expiresAt.After(now) {
			next := elem.Next()
			c.deleteEntryLocked(entry)
			elem = next
			continue
		}
		return
	}
}

func (c *masterchainNextBroadcastCache) pruneOverflowLocked() {
	for len(c.entries) > c.maxItems || c.bytes > c.maxBytes {
		elem := c.order.Front()
		if elem == nil {
			return
		}
		c.deleteEntryLocked(elem.Value.(*masterchainNextBroadcastCacheEntry))
	}
}

func (c *masterchainNextBroadcastCache) deleteEntryLocked(entry *masterchainNextBroadcastCacheEntry) {
	if entry == nil {
		return
	}
	delete(c.entries, entry.key)
	if entry.element != nil {
		c.order.Remove(entry.element)
		entry.element = nil
	}
	c.bytes -= entry.bytes
	if c.bytes < 0 {
		c.bytes = 0
	}
}

func masterchainNextBroadcastBlockCacheSize(blockBOC []byte, proofBOC []byte) int64 {
	return int64(len(blockBOC)*2 + len(proofBOC)*2 + masterchainNextBroadcastCacheOverhead)
}

func (n *Node) rememberMasterchainNextBroadcastBlock(downloaded *DownloadedBlock) bool {
	if downloaded == nil || n.masterchainNextBroadcastCache == nil {
		return false
	}
	if !isMasterchainBlock(downloaded.ID) {
		return false
	}
	if err := n.masterchainNextBroadcastCache.Store(*downloaded); err != nil {
		n.log.Debug().
			Err(err).
			Str("block", formatBlockRef(downloaded.ID)).
			Msg("dropping masterchain block broadcast from next cache")
		return false
	}

	prev := downloaded.Meta.PrevRefs[0]
	n.log.Debug().
		Str("block", formatBlockRef(downloaded.ID)).
		Str("prev", formatBlockRef(prev)).
		Msg("cached masterchain block broadcast")
	n.notifyMasterchainNextBroadcastBlock(prev)
	return true
}

func (n *Node) masterchainNextBroadcastBlock(prev ton.BlockIDExt) (*DownloadedBlock, error) {
	if n == nil || n.masterchainNextBroadcastCache == nil {
		return nil, tnstore.ErrNotFound
	}
	return n.masterchainNextBroadcastCache.BlockAfter(prev)
}

func (n *Node) watchMasterchainNextBroadcastBlock(prev ton.BlockIDExt) (<-chan struct{}, func()) {
	if n == nil || n.masterchainNextBroadcastCache == nil || !isMasterchainBlock(prev) {
		return nil, func() {}
	}

	key := tnstore.BlockKey(prev)
	ch := make(chan struct{})

	n.masterchainNextBroadcastWaitMx.Lock()
	if n.masterchainNextBroadcastWaiters == nil {
		n.masterchainNextBroadcastWaiters = map[tnstore.BlockRootHash][]chan struct{}{}
	}
	n.masterchainNextBroadcastWaiters[key] = append(n.masterchainNextBroadcastWaiters[key], ch)
	n.masterchainNextBroadcastWaitMx.Unlock()

	var once sync.Once
	cancel := func() {
		once.Do(func() {
			n.masterchainNextBroadcastWaitMx.Lock()
			waiters := n.masterchainNextBroadcastWaiters[key]
			for i, waiter := range waiters {
				if waiter == ch {
					waiters = append(waiters[:i], waiters[i+1:]...)
					break
				}
			}
			if len(waiters) == 0 {
				delete(n.masterchainNextBroadcastWaiters, key)
			} else {
				n.masterchainNextBroadcastWaiters[key] = waiters
			}
			n.masterchainNextBroadcastWaitMx.Unlock()
		})
	}
	return ch, cancel
}

func (n *Node) notifyMasterchainNextBroadcastBlock(prev ton.BlockIDExt) {
	if n == nil || !isMasterchainBlock(prev) {
		return
	}

	key := tnstore.BlockKey(prev)

	n.masterchainNextBroadcastWaitMx.Lock()
	waiters := n.masterchainNextBroadcastWaiters[key]
	delete(n.masterchainNextBroadcastWaiters, key)
	n.masterchainNextBroadcastWaitMx.Unlock()

	for _, waiter := range waiters {
		close(waiter)
	}
}
