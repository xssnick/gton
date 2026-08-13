package p2p

import (
	storage2 "github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

func (n *Node) trackUnverifiedBroadcastBlock(event BroadcastEvent) {
	if event.Block.Workchain == -1 && event.Block.Shard == topShard {
		n.trackRawMasterchainBroadcast(event.Block)
		return
	}

	// Masterchain observed/seen heads are advanced only after consensus
	// signature validation through RememberSeenMasterchainBlock.
	if event.Block.Workchain == 0 {
		n.trackLatestBasechain(event.Block)
	}
}

func (n *Node) trackRawMasterchainBroadcast(block ton.BlockIDExt) {
	if block.Workchain != -1 || block.Shard != topShard {
		return
	}

	n.latestBlocksMx.Lock()
	if n.rawMasterchainBroadcast == nil || n.rawMasterchainBroadcast.SeqNo < block.SeqNo {
		n.rawMasterchainBroadcast = &block
		close(n.rawMasterchainNotify)
		n.rawMasterchainNotify = make(chan struct{})
	}
	n.latestBlocksMx.Unlock()
}

// MasterchainBroadcastAfter returns the latest raw masterchain broadcast seqno
// above seqno, or zero when none was received, and a channel that is closed
// when a newer raw masterchain broadcast arrives.
func (n *Node) MasterchainBroadcastAfter(seqno uint32) (uint32, <-chan struct{}) {
	n.latestBlocksMx.RLock()
	defer n.latestBlocksMx.RUnlock()

	if n.rawMasterchainBroadcast != nil && n.rawMasterchainBroadcast.SeqNo > seqno {
		return n.rawMasterchainBroadcast.SeqNo, n.rawMasterchainNotify
	}
	return 0, n.rawMasterchainNotify
}

func (n *Node) RememberSeenMasterchainBlock(block ton.BlockIDExt) {
	n.observeMasterchainBlock(block)
	n.observeSeenMasterchainBlock(block)
}

func (n *Node) observeMasterchainBlock(block ton.BlockIDExt) bool {
	if block.Workchain != -1 || block.Shard != topShard {
		return false
	}

	updated := false

	n.latestBlocksMx.Lock()
	if n.observedMasterchain == nil || n.observedMasterchain.SeqNo < block.SeqNo {
		n.observedMasterchain = &block
		close(n.observedMasterchainNotify)
		n.observedMasterchainNotify = make(chan struct{})
		updated = true
	}
	n.latestBlocksMx.Unlock()

	return updated
}

func (n *Node) observeSeenMasterchainBlock(block ton.BlockIDExt) bool {
	if block.Workchain != -1 || block.Shard != topShard {
		return false
	}

	updated := false

	n.latestBlocksMx.Lock()
	if n.seenMasterchain == nil || n.seenMasterchain.SeqNo < block.SeqNo {
		n.seenMasterchain = &block
		close(n.seenMasterchainNotify)
		n.seenMasterchainNotify = make(chan struct{})
		updated = true
	}
	n.latestBlocksMx.Unlock()

	return updated
}

func (n *Node) SeenMasterchainBlock() (ton.BlockIDExt, error) {
	n.latestBlocksMx.RLock()
	defer n.latestBlocksMx.RUnlock()

	if n.seenMasterchain == nil {
		return ton.BlockIDExt{}, storage2.ErrNotFound
	}
	return *n.seenMasterchain, nil
}

func (n *Node) ObservedMasterchainBlock() (ton.BlockIDExt, error) {
	n.latestBlocksMx.RLock()
	defer n.latestBlocksMx.RUnlock()

	if n.observedMasterchain == nil {
		return ton.BlockIDExt{}, storage2.ErrNotFound
	}
	return *n.observedMasterchain, nil
}

func (n *Node) trackLatestBasechain(block ton.BlockIDExt) {
	n.latestBlocksMx.Lock()
	defer n.latestBlocksMx.Unlock()

	key := storage2.ShardKeyFromBlock(block)
	current, ok := n.latestBasechainShards[key]
	if ok && current.SeqNo >= block.SeqNo {
		return
	}

	n.latestBasechainShards[key] = block
	if n.latestBasechain == nil || n.latestBasechain.SeqNo < block.SeqNo {
		n.latestBasechain = &block
	}
	close(n.latestBasechainNotify)
	n.latestBasechainNotify = make(chan struct{})
}
