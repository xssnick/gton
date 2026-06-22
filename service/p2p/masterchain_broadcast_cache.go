package p2p

import (
	"fmt"
	"sync"
	"time"

	tnstore "github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

const (
	masterchainNextBroadcastCacheTTL      = 3 * time.Minute
	masterchainNextBroadcastCacheMaxBytes = 256 << 20
	masterchainNextBroadcastCacheMaxItems = 4096
	masterchainNextBroadcastCacheOverhead = 256
)

type masterchainNextBroadcastCache struct {
	broadcastBlockCache
}

func newMasterchainNextBroadcastCache(ttl time.Duration, maxBytes int64, maxItems int) *masterchainNextBroadcastCache {
	return &masterchainNextBroadcastCache{
		broadcastBlockCache: newBroadcastBlockCache(ttl, maxBytes, maxItems, "masterchain next broadcast cache"),
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
	entry := &broadcastBlockCacheEntry{
		key:          key,
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
		bytes:        size,
	}

	c.storeEntry(entry, now)
	return nil
}

func (c *masterchainNextBroadcastCache) BlockAfter(prev ton.BlockIDExt) (*DownloadedBlock, error) {
	return c.blockAfterAt(prev, time.Now())
}

func (c *masterchainNextBroadcastCache) blockAfterAt(prev ton.BlockIDExt, now time.Time) (*DownloadedBlock, error) {
	if c == nil || !isMasterchainBlock(prev) {
		return nil, tnstore.ErrNotFound
	}
	return c.broadcastBlockCache.blockAt(tnstore.BlockKey(prev), now)
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
