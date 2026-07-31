package p2p

import (
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

// appliedShardHeadTable is the highest applied seqno per shard, keyed by the
// exact shard prefix. A split or merge changes the prefix, so a broadcast for a
// shard the table does not know is simply not gated: the table is rebuilt from
// every committed masterchain state, and the transition costs at most one
// ungated broadcast per new shard.
type appliedShardHeadTable map[storage.ShardKey]uint32

// NoteAppliedMasterchainSeqno records the highest applied masterchain seqno.
// Classify drops masterchain block broadcasts at or below it before any proof
// parsing or signature verification, mirroring the reference node's
// validate-broadcast early exit for already-applied blocks.
func (n *Node) NoteAppliedMasterchainSeqno(seqno uint32) {
	for {
		current := n.appliedMasterchainSeqno.Load()
		if seqno <= current {
			return
		}
		if n.appliedMasterchainSeqno.CompareAndSwap(current, seqno) {
			return
		}
	}
}

// NoteAppliedShardHeads records the applied top block of every shard the
// service currently follows. The reference node has no per-shard counterpart of
// its masterchain "block is too old" exit — it gates shards on need_monitor
// alone — but the same argument holds: a shard block at or below the applied
// head was already committed by the masterchain, so a broadcast of it can only
// be a redelivery or a replay. Dropping it here is what keeps a multi-MB
// payload from buying an ed25519 pass and a decode worker.
func (n *Node) NoteAppliedShardHeads(heads []ton.BlockIDExt) {
	if len(heads) == 0 {
		return
	}

	previous := n.appliedShardHeads.Load()
	next := make(appliedShardHeadTable, len(heads))
	for _, head := range heads {
		if isMasterchainBlock(head) {
			continue
		}

		key := storage.ShardKeyFromBlock(head)
		seqno := head.SeqNo
		// publishes of two committed states can race, so keep the table
		// monotonic per shard; shards missing from this state fall out
		// instead, so a split or a merge does not leave dead prefixes behind
		if previous != nil {
			if known, ok := (*previous)[key]; ok && known > seqno {
				seqno = known
			}
		}
		if known, ok := next[key]; ok && known > seqno {
			continue
		}
		next[key] = seqno
	}
	if len(next) == 0 {
		return
	}
	n.appliedShardHeads.Store(&next)
}

// alreadyAppliedBroadcast reports a block the service has already applied, so
// classify can drop its broadcast before parsing a proof, verifying signatures
// or decoding a payload.
func (n *Node) alreadyAppliedBroadcast(block ton.BlockIDExt) bool {
	if isMasterchainBlock(block) {
		applied := n.appliedMasterchainSeqno.Load()
		return applied > 0 && block.SeqNo <= applied
	}

	heads := n.appliedShardHeads.Load()
	if heads == nil {
		return false
	}
	applied, ok := (*heads)[storage.ShardKeyFromBlock(block)]
	return ok && applied > 0 && block.SeqNo <= applied
}
