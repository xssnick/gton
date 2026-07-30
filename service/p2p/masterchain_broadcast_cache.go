package p2p

import (
	"fmt"
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

func (c *masterchainNextBroadcastCache) storeAt(downloaded DownloadedBlock, now time.Time) error {
	if c.maxItems <= 0 || c.maxBytes <= 0 {
		return fmt.Errorf("masterchain next broadcast cache is disabled")
	}
	if len(downloaded.Meta.PrevRefs) != 1 {
		return fmt.Errorf("block %s has %d previous refs", tnstore.FormatBlockRef(downloaded.ID), len(downloaded.Meta.PrevRefs))
	}
	prev := downloaded.Meta.PrevRefs[0]
	if !isMasterchainBlock(prev) {
		return fmt.Errorf("block %s previous ref %s is not masterchain", tnstore.FormatBlockRef(downloaded.ID), tnstore.FormatBlockRef(prev))
	}

	size := masterchainNextBroadcastBlockCacheSize(downloaded.BlockBOC, downloaded.ProofBOC)
	if size > c.maxBytes {
		return fmt.Errorf("block %s is too large for masterchain next broadcast cache: %d > %d", tnstore.FormatBlockRef(downloaded.ID), size, c.maxBytes)
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

		signaturesVerifiedKey: append([]byte(nil), downloaded.SignaturesVerifiedKey...),
	}

	c.storeEntry(entry, now)
	return nil
}

func (c *masterchainNextBroadcastCache) BlockAfter(prev ton.BlockIDExt) (*DownloadedBlock, error) {
	if !isMasterchainBlock(prev) {
		return nil, tnstore.ErrNotFound
	}
	return c.blockAt(tnstore.BlockKey(prev), time.Now())
}

func masterchainNextBroadcastBlockCacheSize(blockBOC []byte, proofBOC []byte) int64 {
	return int64(len(blockBOC)*2 + len(proofBOC)*2 + masterchainNextBroadcastCacheOverhead)
}

func (n *Node) rememberMasterchainNextBroadcastBlock(downloaded *DownloadedBlock) bool {
	if !isMasterchainBlock(downloaded.ID) {
		return false
	}
	if err := n.masterchainNextBroadcastCache.storeAt(*downloaded, time.Now()); err != nil {
		n.log.Debug().
			Err(err).
			Stringer("block", tnstore.BlockRef(downloaded.ID)).
			Msg("dropping masterchain block broadcast from next cache")
		return false
	}

	prev := downloaded.Meta.PrevRefs[0]
	n.log.Debug().
		Stringer("block", tnstore.BlockRef(downloaded.ID)).
		Stringer("prev", tnstore.BlockRef(prev)).
		Msg("cached masterchain block broadcast")
	n.notifyMasterchainNextBroadcastBlock(prev)
	return true
}

// WatchMasterchainNextBroadcastBlock returns a channel that is closed when a
// decoded masterchain broadcast following prev lands in the next-broadcast
// cache, plus an unwatch func. A nil channel means prev is not masterchain.
func (n *Node) WatchMasterchainNextBroadcastBlock(prev ton.BlockIDExt) (<-chan struct{}, func()) {
	if !isMasterchainBlock(prev) {
		return nil, func() {}
	}

	key := tnstore.BlockKey(prev)
	return n.masterchainNextBroadcastWaiters.watch(key)
}

// HasMasterchainNextBroadcastBlock reports whether a decoded masterchain
// broadcast following prev is already in the next-broadcast cache.
func (n *Node) HasMasterchainNextBroadcastBlock(prev ton.BlockIDExt) bool {
	if !isMasterchainBlock(prev) {
		return false
	}
	return n.masterchainNextBroadcastCache.has(tnstore.BlockKey(prev), time.Now())
}

func (n *Node) notifyMasterchainNextBroadcastBlock(prev ton.BlockIDExt) {
	if !isMasterchainBlock(prev) {
		return
	}

	key := tnstore.BlockKey(prev)
	n.masterchainNextBroadcastWaiters.notify(key)
}
