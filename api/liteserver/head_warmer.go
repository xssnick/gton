package liteserver

import (
	"context"
	"time"

	"github.com/xssnick/tonutils-go/ton"
)

const (
	// headWarmMaxLag bounds how stale a published masterchain head may be for
	// its response artifacts to still be worth pre-building. A node replaying
	// history publishes heads far faster than any client consumes them, so
	// warming during catch-up only burns CPU and ages the response cache.
	headWarmMaxLag = 30 * time.Second

	// headWarmWaitTimeout re-arms the store wait. The wait parks on the store
	// notify channel, so this only bounds how long the warmer sleeps while the
	// head does not move; an expiry re-reads the head and warms nothing when it
	// is unchanged. The store caps a single wait at 10s regardless.
	headWarmWaitTimeout = 10 * time.Second

	// headWarmIdleBackoff throttles the loop in the degenerate case where the
	// store reports a ready masterchain seqno but exposes no usable head. The
	// wait would return immediately every time, so without the backoff the loop
	// would spin. A healthy store never takes this path.
	headWarmIdleBackoff = time.Second
)

// runHeadWarmer pre-builds the deterministic per-head artifacts that the
// canonical live-head client asks for the moment its waitMasterchainSeqno
// returns: the block header proof, allShardsInfo, getConfigAll and the
// getBlockProof reference base. Each of them is memoized per block identity, so
// without a warmer the first (latency sensitive) client of every new head pays
// the full build inside its own query.
//
// The loop is event driven: it parks on the same store wait the client-facing
// wait path uses and wakes only when a newer head is published. It runs on its
// own goroutine outside the query executor, so a warm never occupies a slot a
// client query could have used, and it never runs ahead of publication.
//
// Cache pressure is bounded by construction. A warm adds three entries to the
// response cache, whose eviction is FIFO over 512 entries: the entries a warm
// displaces are the oldest inserted, which belong to blocks older than the ones
// still being queried, so warm entries age out client entries only after the
// client entries are already stale. The getBlockProof base cache is smaller (8
// entries, also FIFO). On a node serving mode-default getBlockProof traffic the
// head base a warm inserts is the entry those queries would have inserted
// anyway; on a node without such traffic it is a net-new insert, and what it can
// displace is a client-pinned base (mode 0x1000), which is rare and rebuilt on
// its next use.
//
// There is no config knob because the package has no comparable performance
// toggle, and the warmer is inherently scoped to the liteserver: it only runs
// between Start and Close, so a node without the liteserver extension never
// starts it.
func (s *Server) runHeadWarmer(ctx context.Context) {
	var warmed liteBlockKey
	// unreadable counts consecutive iterations in which the store exposed no
	// usable head. The wait cannot park below an already-ready seqno, so the
	// second such iteration in a row backs off instead of spinning.
	unreadable := 0
	for {
		if ctx.Err() != nil {
			return
		}

		head := s.headWarmTarget(ctx)
		if head.live {
			if key := liteBlockKeyFromBlock(head.block); key != warmed {
				s.warmHead(ctx, head.block)
				warmed = key
			}
		}
		if head.readable {
			unreadable = 0
		} else {
			unreadable++
		}

		// Waiting for head+1 parks on the store notify channel: an unchanged
		// head costs nothing and a new head wakes the warmer without polling.
		// Before the first publish head is the zero id, and waiting for seqno 1
		// parks just the same.
		waitErr := s.store.WaitMasterchainSeqno(ctx, head.block.SeqNo+1, headWarmWaitTimeout)
		if ctx.Err() != nil {
			return
		}
		if unreadable > 1 && waitErr == nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(headWarmIdleBackoff):
			}
		}
	}
}

// headWarmState is the published masterchain head as the warmer sees it.
type headWarmState struct {
	// block is the published head, or the zero id when the store has none. Its
	// seqno is what the warmer parks on even when the head is not warmable.
	block ton.BlockIDExt
	// readable is set when the store exposes a usable masterchain head id.
	readable bool
	// live is set when that head is recent enough to be the one clients are
	// waiting on, which is the only case worth pre-building.
	live bool
}

func (s *Server) headWarmTarget(ctx context.Context) headWarmState {
	head, _, genUTime, err := s.store.CurrentMasterchainInfo(ctx)
	if err != nil || !isFullBlockID(&head) || head.Workchain != masterchainID {
		return headWarmState{block: head}
	}
	// genUTime == 0 means the store could not date the head. Treat it as not at
	// the tip: an undatable head is exactly the case where warming may be waste.
	if genUTime == 0 {
		return headWarmState{block: head, readable: true}
	}
	return headWarmState{
		block:    head,
		readable: true,
		live:     s.now().Sub(time.Unix(int64(genUTime), 0)) <= headWarmMaxLag,
	}
}

// warmHead pre-builds the head artifacts in the order a live-head client asks
// for them. Every step runs through the same entry point the query handler
// uses, so a warmed entry necessarily lands under the key a real query looks
// up, and a query arriving mid-warm joins the in-flight build through the
// response cache singleflight instead of starting a second one.
//
// The warm is abandoned as soon as a newer head is ready: the next head's
// artifacts are worth more than this one's, and the caller keeps a single warm
// in flight so it can never queue up behind itself. Individual builds are not
// interruptible - the response cache deliberately detaches them from the
// initiating context - so the abandon check sits between steps, where the
// granularity is one artifact.
//
// Failures are diagnostic only. Nothing here is on a serving path: a failed
// warm just leaves the artifact cold for the first client, exactly as if the
// warmer had not run.
func (s *Server) warmHead(ctx context.Context, head ton.BlockIDExt) {
	steps := [...]struct {
		name string
		warm func() error
	}{
		// Mode 0 is what every mainstream client sends for getBlockHeader and
		// what lookupBlock's mode>>4 resolves to; other mode variants keep
		// their own cache entries and stay cold on purpose rather than
		// multiplying builds per head.
		{"block_header", func() error {
			_, err := s.blockHeader(ctx, head, 0)
			return err
		}},
		{"all_shards_info", func() error {
			return warmResponseError(s.handleAllShardsInfo(ctx, &head))
		}},
		// getConfigAll with mode 0 - the canonical request. Non-zero modes are
		// separate cache entries and are rare enough not to be worth warming;
		// the previous-key-block mode (0x8000) is not cached at all.
		{"config_all", func() error {
			return warmResponseError(s.handleConfig(ctx, &head, 0, true, nil))
		}},
		// Every getBlockProof that does not pin its own reference block reads
		// the base from the current head, so this is the one base worth having.
		{"block_proof_base", func() error {
			_, err := s.loadBlockProofBase(ctx, head)
			return err
		}},
	}

	for _, step := range steps {
		if s.store.MasterchainSeqnoReady(head.SeqNo + 1) {
			return
		}
		if err := step.warm(); err != nil {
			if event := s.log.Debug(); event.Enabled() {
				event.Err(err).
					Str("artifact", step.name).
					Uint32("seqno", head.SeqNo).
					Msg("liteserver head warm failed")
			}
		}
	}
}

// warmResponseError turns a handler's error response into an error so the
// warmer can log it. Handlers answer with a value, never with a Go error.
func warmResponseError(resp any) error {
	if lsErr, ok := resp.(ton.LSError); ok {
		return queryErrorFromResponse(lsErr)
	}
	return nil
}
