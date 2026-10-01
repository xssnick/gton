package collator

import (
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

// prewarmHints is what an acquisition decided to warm, collected while it holds
// the session mutex and issued once it does not.
//
// The split exists because issuing is the expensive half and the mutex is the
// wrong place to spend it. A single acquisition can name up to
// accountPrewarmCapacity destinations, and every one of them takes the
// prewarmer's own global mutex — twice, when PrewarmAccountNow declines and the
// hint falls back to the queue — while the prewarm workers are taking that same
// mutex from the other side. Behind the session mutex, meanwhile, stands
// AdvanceConsensusBase, which is how a leader window opens, and CommitCandidate,
// which is what a finished block goes through between its signature and the
// wire.
//
// Only value types are collected. The messages and the cut belong to the message
// branch, which AdvanceConsensusBase re-roots under the very mutex being
// released, so a hint that kept a pointer into them would be reading discarded
// lineage by the time it was issued.
//
// The two account buckets are what the prewarmer is asked for, and they are kept
// apart rather than merged: a destination the block is about to execute is worth
// a worker now, while one that merely entered the branch is background work.
// Each bucket dedupes on its own, so a destination that is both stays both.
type prewarmHints struct {
	queued []prewarmAccountKey
	urgent []prewarmAccountKey
	roots  []cell.Hash

	seenQueued map[prewarmAccountKey]struct{}
	seenUrgent map[prewarmAccountKey]struct{}
	seenRoots  map[cell.Hash]struct{}
}

func (h *prewarmHints) empty() bool {
	return h == nil || (len(h.queued) == 0 && len(h.urgent) == 0 && len(h.roots) == 0)
}

func addHint[K comparable](seen *map[K]struct{}, list *[]K, key K) bool {
	if *seen == nil {
		*seen = make(map[K]struct{}, 64)
	}
	if _, exists := (*seen)[key]; exists {
		return false
	}
	(*seen)[key] = struct{}{}
	*list = append(*list, key)

	return true
}

// collectPooledInternals is prewarmPooledInternals without the issuing: the
// destination accounts and exact envelopes of messages that have just become
// part of the session branch.
//
// The prefix bound is the same one and for the same measured reason: hinting
// every destination of a queue thousands deep enqueued most of them into a
// prewarmer that drops past its capacity anyway, and spent the first build of a
// leader window — the one with no slack — on the enqueue attempts, measured at
// ~17 ms of CPU per seed on an 8,000-entry queue.
func (a *LocalAcquisition) collectPooledInternals(hints *prewarmHints, messages []*msgpool.InternalMessage) {
	if a.accountPrewarmer == nil || hints == nil {
		return
	}
	if limit := a.pooledPrewarmLimit(); len(messages) > limit {
		messages = messages[:limit]
	}
	for _, message := range messages {
		if !message.DestinationPrewarmable {
			continue
		}
		addHint(&hints.seenQueued, &hints.queued, prewarmAccountKey{
			workchain: message.DestinationWorkchain,
			account:   message.DestinationAccount,
		})
	}
	for _, message := range messages {
		addHint(&hints.seenRoots, &hints.roots, cell.Hash(message.EnvHash))
	}
}

// collectCurrentInternals is prewarmCurrentInternals without the issuing: the
// bounded look-ahead over the canonical front of the exact cut. Its horizon
// follows the configured worker and queue capacity rather than a message
// constant — a large block keeps warming later destinations while its earlier
// transactions execute, while a deeper backlog cannot create unbounded work.
func (a *LocalAcquisition) collectCurrentInternals(hints *prewarmHints, cut *msgpool.Cut) {
	if a.accountPrewarmer == nil || hints == nil || cut == nil {
		return
	}
	capacity := a.accountPrewarmCapacity
	if capacity <= 0 {
		return
	}
	taken := 0
	for _, message := range cut.Messages {
		if !message.DestinationPrewarmable {
			continue
		}
		if !addHint(&hints.seenUrgent, &hints.urgent, prewarmAccountKey{
			workchain: message.DestinationWorkchain,
			account:   message.DestinationAccount,
		}) {
			continue
		}
		if taken++; taken == capacity {
			return
		}
	}
}

// issuePrewarmHints hands everything collected to the prewarmer. It must run
// with the session mutex released; every caller registers it as the defer above
// the unlock, so it runs after it.
func (a *LocalAcquisition) issuePrewarmHints(hints *prewarmHints) {
	if a.accountPrewarmer == nil || hints.empty() {
		return
	}
	for _, key := range hints.urgent {
		if !a.accountPrewarmer.PrewarmAccountNow(key.workchain, key.account) {
			a.accountPrewarmer.EnqueueAccount(key.workchain, key.account)
		}
	}
	for _, key := range hints.queued {
		a.accountPrewarmer.EnqueueAccount(key.workchain, key.account)
	}
	for _, root := range hints.roots {
		a.accountPrewarmer.EnqueueRoot(root)
	}
}
