package p2p

import (
	"context"
	"errors"
	"fmt"
	"time"

	sharddomain "github.com/xssnick/gton/service/shard"
	storage2 "github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
)

func (n *Node) startSubscription(sub *overlaySubscription) {
	ctx, cancel := context.WithCancel(n.runCtx)
	if err := ctx.Err(); err != nil {
		cancel()
		return
	}
	token, ok := sub.setRunCancel(cancel)
	if !ok {
		cancel()
		return
	}

	sub.prewarmQUICPeers()
	sub.startTwoStepRebroadcastWorker(ctx)
	n.runAsync(func() {
		defer sub.clearRunCancel(token)
		sub.run(ctx)
	})
}

func (n *Node) closeSubscriptions() {
	n.subscriptionsMx.Lock()
	if n.privateOverlays != nil {
		n.privateOverlays.sealed = true
	}
	subscriptions := make([]*overlaySubscription, 0, len(n.subscriptions))
	for _, sub := range n.subscriptions {
		subscriptions = append(subscriptions, sub)
	}
	n.subscriptionsMx.Unlock()

	for _, sub := range subscriptions {
		sub.close()
	}
}

func (n *Node) getOrCreateSubscription(spec overlaySpec) (*overlaySubscription, error) {
	key := overlaySpecKey(spec)

	n.subscriptionsMx.Lock()
	if sub := n.subscriptions[key]; sub != nil {
		n.subscriptionsMx.Unlock()
		return sub, nil
	}

	sub, err := n.newOverlaySubscription(spec)
	if err != nil {
		n.subscriptionsMx.Unlock()
		return nil, err
	}
	n.subscriptions[key] = sub
	n.publishPublicBroadcastReceiversLocked()
	n.subscriptionsMx.Unlock()

	n.blockBroadcasts.applyProducerRole(sub)
	n.attachSubscriptionPeers(sub)
	return sub, nil
}

func (n *Node) getOrActivateSubscription(spec overlaySpec) (*overlaySubscription, bool, error) {
	key := overlaySpecKey(spec)
	for {
		sub, err := n.getOrCreateSubscription(spec)
		if err != nil {
			return nil, false, err
		}

		n.subscriptionsMx.Lock()
		if n.subscriptions[key] != sub {
			n.subscriptionsMx.Unlock()
			continue
		}
		sub.mx.Lock()
		reactivated := sub.setActiveLocked(true, time.Time{})
		sub.mx.Unlock()
		n.subscriptionsMx.Unlock()
		return sub, reactivated, nil
	}
}

func (n *Node) subscriptionForBlock(block ton.BlockIDExt) (*overlaySubscription, error) {
	overlayBlock, err := n.overlayBlockForDownload(block)
	if err != nil {
		return nil, err
	}
	return n.subscriptionForOverlayBlock(overlayBlock)
}

func (n *Node) subscriptionForOverlayBlock(block ton.BlockIDExt) (*overlaySubscription, error) {
	if len(n.zeroStateFileHash) == 0 {
		return nil, errors.New("node is not started")
	}

	spec, err := buildOverlaySpec(n.zeroStateFileHash, block.Workchain, block.Shard, overlayName(block.Workchain, block.Shard))
	if err != nil {
		return nil, fmt.Errorf("build overlay for %s: %w", storage2.FormatBlockRef(block), err)
	}

	sub, _, err := n.getOrActivateSubscription(spec)
	if err != nil {
		return nil, err
	}
	n.startSubscription(sub)
	return sub, nil
}

func (n *Node) SetMonitorMinSplitDepth(workchain int32, depth uint32) {
	n.monitorSplitMx.Lock()
	defer n.monitorSplitMx.Unlock()

	n.monitorMinSplitDepth[workchain] = depth
}

func (n *Node) SetActiveShardOverlays(blocks []ton.BlockIDExt) error {
	if len(n.zeroStateFileHash) == 0 {
		return nil
	}

	specs, err := n.activeShardOverlaySpecs(blocks)
	if err != nil {
		return err
	}

	active := make(map[string]struct{}, len(specs))
	for _, spec := range specs {
		key := overlaySpecKey(spec)
		active[key] = struct{}{}

		sub, reactivated, err := n.getOrActivateSubscription(spec)
		if err != nil {
			return err
		}
		if reactivated {
			n.log.Debug().
				Str("overlay", spec.Name).
				Msg("reactivated shard overlay")
		}
		n.startSubscription(sub)
	}

	deleteAt := time.Now().Add(inactiveShardOverlayTTL)
	for _, entry := range n.subscriptionEntriesSnapshot() {
		if _, ok := active[entry.key]; ok {
			continue
		}
		if !entry.sub.spec.followsShardLifecycle() {
			continue
		}
		if entry.sub.spec.Workchain == -1 && entry.sub.spec.Shard == topShard {
			continue
		}
		if entry.sub.setActive(false, deleteAt) {
			n.log.Debug().
				Str("overlay", entry.sub.spec.Name).
				Dur("ttl", inactiveShardOverlayTTL).
				Msg("marked shard overlay inactive")
		}
	}

	return nil
}

func (n *Node) activeShardOverlaySpecs(blocks []ton.BlockIDExt) ([]overlaySpec, error) {
	specs := make([]overlaySpec, 0, len(blocks)+2)
	seen := map[string]struct{}{}

	add := func(workchain int32, shard int64) error {
		spec, err := buildOverlaySpec(n.zeroStateFileHash, workchain, shard, overlayName(workchain, shard))
		if err != nil {
			return err
		}
		key := overlaySpecKey(spec)
		if _, ok := seen[key]; ok {
			return nil
		}
		seen[key] = struct{}{}
		specs = append(specs, spec)
		return nil
	}

	if err := add(-1, topShard); err != nil {
		return nil, fmt.Errorf("build active masterchain overlay: %w", err)
	}
	if err := add(0, topShard); err != nil {
		return nil, fmt.Errorf("build active basechain overlay: %w", err)
	}

	for _, block := range blocks {
		if block.Workchain == -1 {
			continue
		}

		depth := n.monitorMinSplitDepthForWorkchain(block.Workchain)
		prefixLen, err := sharddomain.PrefixLength(block.Shard)
		if err != nil {
			return nil, fmt.Errorf("invalid active shard %d:%016x: %w", block.Workchain, uint64(block.Shard), err)
		}
		shardID := block.Shard
		if prefixLen > depth {
			shardID, err = sharddomain.Ancestor(shardID, depth)
			if err != nil {
				return nil, fmt.Errorf("normalize active shard %d:%016x: %w", block.Workchain, uint64(block.Shard), err)
			}
		}

		for {
			if err := add(block.Workchain, shardID); err != nil {
				return nil, fmt.Errorf("build active shard overlay %d:%016x: %w", block.Workchain, uint64(shardID), err)
			}
			if shardID == topShard {
				break
			}
			parent, err := sharddomain.Parent(shardID)
			if err != nil {
				return nil, fmt.Errorf("parent of active shard %d:%016x: %w", block.Workchain, uint64(shardID), err)
			}
			shardID = parent
		}
	}

	return specs, nil
}

func (n *Node) runSubscriptionLifecycleLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			if closed := n.pool.pruneIdle(now); closed > 0 {
				n.log.Debug().
					Int("closed", closed).
					Int("pooled", n.pool.size()).
					Msg("closed idle pooled peers")
			}
			n.stopExpiredInactiveSubscriptions(now)
		}
	}
}

func (n *Node) stopExpiredInactiveSubscriptions(now time.Time) {
	for _, entry := range n.subscriptionEntriesSnapshot() {
		if !entry.sub.inactiveExpired(now) {
			continue
		}
		if n.deleteInactiveSubscription(entry.key, entry.sub, now) {
			n.log.Debug().
				Str("overlay", entry.sub.spec.Name).
				Msg("deleted inactive shard overlay")
		}
	}
}

func (n *Node) deleteInactiveSubscription(key string, sub *overlaySubscription, now time.Time) bool {
	n.subscriptionsMx.Lock()
	if n.subscriptions[key] != sub {
		n.subscriptionsMx.Unlock()
		return false
	}

	sub.mx.Lock()
	if !sub.inactiveExpiredLocked(now) {
		sub.mx.Unlock()
		n.subscriptionsMx.Unlock()
		return false
	}
	delete(n.subscriptions, key)
	sub.removed = true
	n.publishPublicBroadcastReceiversLocked()
	sub.mx.Unlock()
	// Keep subscription creation serialized until the old receiver generation
	// is closed. AttachOverlay may then replace its still-attached wrappers
	// without losing every pooled peer during shard reactivation.
	sub.broadcastReceiver.Close()
	n.subscriptionsMx.Unlock()

	sub.close()
	return true
}

func (n *Node) overlayBlockForDownload(block ton.BlockIDExt) (ton.BlockIDExt, error) {
	if block.Workchain == -1 {
		block.Shard = topShard
		return block, nil
	}

	depth := n.monitorMinSplitDepthForWorkchain(block.Workchain)
	prefixLen, err := sharddomain.PrefixLength(block.Shard)
	if err != nil {
		return ton.BlockIDExt{}, fmt.Errorf("invalid block shard %d:%016x: %w", block.Workchain, uint64(block.Shard), err)
	}
	if prefixLen <= depth {
		return block, nil
	}

	ancestor, err := sharddomain.Ancestor(block.Shard, depth)
	if err != nil {
		return ton.BlockIDExt{}, fmt.Errorf("select overlay shard for %d:%016x: %w", block.Workchain, uint64(block.Shard), err)
	}
	block.Shard = ancestor
	return block, nil
}

func (n *Node) monitorMinSplitDepthForWorkchain(workchain int32) uint32 {
	n.monitorSplitMx.RLock()
	defer n.monitorSplitMx.RUnlock()

	return n.monitorMinSplitDepth[workchain]
}

type subscriptionEntry struct {
	key string
	sub *overlaySubscription
}

func (n *Node) subscriptionEntriesSnapshot() []subscriptionEntry {
	n.subscriptionsMx.RLock()
	defer n.subscriptionsMx.RUnlock()

	list := make([]subscriptionEntry, 0, len(n.subscriptions))
	for key, sub := range n.subscriptions {
		list = append(list, subscriptionEntry{
			key: key,
			sub: sub,
		})
	}
	return list
}

// subscriptionByOverlayShortID resolves an overlay by its wire id. Used by the
// detached query path, which sees the overlay envelope but has no attachment.
func (n *Node) subscriptionByOverlayShortID(id []byte) *overlaySubscription {
	n.subscriptionsMx.RLock()
	defer n.subscriptionsMx.RUnlock()

	return n.subscriptions[string(id)]
}

// subscriptionByName resolves an overlay by its display name without
// allocating; the subscription map holds a handful of entries.
func (n *Node) subscriptionByName(name string) *overlaySubscription {
	n.subscriptionsMx.RLock()
	defer n.subscriptionsMx.RUnlock()

	for _, sub := range n.subscriptions {
		if sub.spec.Name == name {
			return sub
		}
	}
	return nil
}

func (n *Node) subscriptionsSnapshot() []*overlaySubscription {
	entries := n.subscriptionEntriesSnapshot()
	list := make([]*overlaySubscription, 0, len(entries))
	for _, entry := range entries {
		list = append(list, entry.sub)
	}
	return list
}
