package blocksync

import (
	"context"
	"encoding/hex"
	"flexserver/internal/logutil"
	"flexserver/service/p2p"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/ton"
)

const (
	defaultOutputBuffer = 256
	defaultRetryCount   = 3
	defaultRetryDelay   = 500 * time.Millisecond
)

type Source interface {
	Events() <-chan p2p.BroadcastEvent
}

type Fetcher interface {
	DownloadBlockFull(ctx context.Context, block ton.BlockIDExt) (p2p.DownloadedBlock, error)
	DownloadNextBlockFull(ctx context.Context, prev ton.BlockIDExt) (p2p.DownloadedBlock, error)
}

type SyncedBlock struct {
	Trigger    p2p.BroadcastEvent
	Downloaded p2p.DownloadedBlock
	CatchUp    bool
}

type Service struct {
	log        zerolog.Logger
	source     Source
	fetcher    Fetcher
	out        chan SyncedBlock
	retryCount int
	retryDelay time.Duration

	chains map[string]*chain
}

type chain struct {
	service *Service
	key     string
	notify  chan struct{}

	mx      sync.Mutex
	current *p2p.BroadcastEvent
	future  *p2p.BroadcastEvent
	busy    bool
	closed  bool
	last    *ton.BlockIDExt
}

func New(logger *zerolog.Logger, source Source, fetcher Fetcher) *Service {
	return &Service{
		log:        logutil.WithComponent(logger, "blocksync"),
		source:     source,
		fetcher:    fetcher,
		out:        make(chan SyncedBlock, defaultOutputBuffer),
		retryCount: defaultRetryCount,
		retryDelay: defaultRetryDelay,
		chains:     map[string]*chain{},
	}
}

type NodeFetcher struct {
	node *p2p.Node
}

func NewNodeFetcher(node *p2p.Node) NodeFetcher {
	return NodeFetcher{node: node}
}

func (f NodeFetcher) DownloadBlockFull(ctx context.Context, block ton.BlockIDExt) (p2p.DownloadedBlock, error) {
	downloaded, err := f.node.DownloadBlockFull(ctx, block)
	if err != nil {
		return p2p.DownloadedBlock{}, err
	}
	if downloaded == nil {
		return p2p.DownloadedBlock{}, fmt.Errorf("download block %s: empty response", formatBlockRef(block))
	}

	return *downloaded, nil
}

func (f NodeFetcher) DownloadNextBlockFull(ctx context.Context, prev ton.BlockIDExt) (p2p.DownloadedBlock, error) {
	downloaded, err := f.node.DownloadNextBlockFull(ctx, prev)
	if err != nil {
		return p2p.DownloadedBlock{}, err
	}
	if downloaded == nil {
		return p2p.DownloadedBlock{}, fmt.Errorf("download next block after %s: empty response", formatBlockRef(prev))
	}

	return *downloaded, nil
}

func (s *Service) Blocks() <-chan SyncedBlock {
	return s.out
}

func (s *Service) Run(ctx context.Context) {
	var wg sync.WaitGroup
	defer func() {
		for _, chain := range s.chains {
			chain.close()
		}
		wg.Wait()
		close(s.out)
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-s.source.Events():
			if !ok {
				return
			}

			chain := s.getOrCreateChain(ctx, &wg, ev.Block)
			if !chain.enqueue(ev) {
				return
			}
		}
	}
}

func (s *Service) getOrCreateChain(ctx context.Context, wg *sync.WaitGroup, block ton.BlockIDExt) *chain {
	key := chainKey(block)
	if chain := s.chains[key]; chain != nil {
		return chain
	}

	chain := &chain{
		service: s,
		key:     key,
		notify:  make(chan struct{}, 1),
	}
	s.chains[key] = chain

	wg.Add(1)
	go func() {
		defer wg.Done()
		chain.run(ctx)
	}()

	return chain
}

func (c *chain) run(ctx context.Context) {
	for {
		ev, ok := c.next(ctx)
		if !ok {
			return
		}
		c.handleBroadcast(ctx, ev)
		c.done()
	}
}

func (c *chain) enqueue(ev p2p.BroadcastEvent) bool {
	c.mx.Lock()
	defer c.mx.Unlock()

	if c.closed {
		return false
	}

	if !c.busy && c.current == nil {
		copyEv := ev
		c.current = &copyEv
	} else if c.future == nil || shouldReplacePending(*c.future, ev) {
		copyEv := ev
		c.future = &copyEv
	}

	select {
	case c.notify <- struct{}{}:
	default:
	}
	return true
}

func (c *chain) close() {
	c.mx.Lock()
	c.closed = true
	c.mx.Unlock()

	select {
	case c.notify <- struct{}{}:
	default:
	}
}

func (c *chain) next(ctx context.Context) (p2p.BroadcastEvent, bool) {
	for {
		c.mx.Lock()
		if c.current != nil {
			ev := *c.current
			c.current = nil
			c.busy = true
			c.mx.Unlock()
			return ev, true
		}
		closed := c.closed
		c.mx.Unlock()

		if closed {
			return p2p.BroadcastEvent{}, false
		}

		select {
		case <-ctx.Done():
			return p2p.BroadcastEvent{}, false
		case <-c.notify:
		}
	}
}

func (c *chain) done() {
	c.mx.Lock()
	defer c.mx.Unlock()

	c.busy = false
	if c.current == nil && c.future != nil {
		c.current = c.future
		c.future = nil
	}
}

func shouldReplacePending(current, next p2p.BroadcastEvent) bool {
	if next.Block.SeqNo != current.Block.SeqNo {
		return next.Block.SeqNo > current.Block.SeqNo
	}
	if next.ReceivedAt.After(current.ReceivedAt) {
		return true
	}
	if next.Trusted && !current.Trusted {
		return true
	}
	return false
}

func (c *chain) handleBroadcast(ctx context.Context, ev p2p.BroadcastEvent) {
	if c.last == nil {
		downloaded, ok := c.waitForBlock(ctx, ev)
		if !ok {
			return
		}

		if !c.service.emit(ctx, SyncedBlock{
			Trigger:    ev,
			Downloaded: downloaded,
			CatchUp:    false,
		}) {
			return
		}

		last := downloaded.ID
		c.last = &last
		return
	}

	if ev.Block.SeqNo < c.last.SeqNo {
		return
	}

	if ev.Block.SeqNo == c.last.SeqNo {
		if !c.last.Equals(&ev.Block) {
			c.service.log.Debug().
				Str("expected", formatBlockRef(*c.last)).
				Str("got", ev.BlockRef()).
				Str("expected_root_hash", formatHashPrefix(c.last.RootHash)).
				Str("expected_file_hash", formatHashPrefix(c.last.FileHash)).
				Str("got_root_hash", formatHashPrefix(ev.Block.RootHash)).
				Str("got_file_hash", formatHashPrefix(ev.Block.FileHash)).
				Str("overlay", ev.Overlay).
				Str("kind", ev.Kind).
				Msg("received different broadcast hash for already synced seqno")
		}
		return
	}

	for c.last.SeqNo < ev.Block.SeqNo {
		prev := *c.last

		downloaded, ok := c.waitForNextBlock(ctx, ev, prev)
		if !ok {
			return
		}

		if downloaded.ID.SeqNo <= prev.SeqNo {
			c.service.log.Warn().
				Str("from", formatBlockRef(prev)).
				Str("got", downloaded.BlockRef()).
				Str("overlay", ev.Overlay).
				Str("kind", ev.Kind).
				Msg("peer returned non-advancing next block")
			return
		}

		if chainKey(downloaded.ID) != c.key {
			c.service.log.Warn().
				Str("from", formatBlockRef(prev)).
				Str("got", downloaded.BlockRef()).
				Str("target", ev.BlockRef()).
				Str("overlay", ev.Overlay).
				Str("kind", ev.Kind).
				Msg("next block moved to another shard chain, catch-up is not implemented for this transition")
			return
		}

		if !c.service.emit(ctx, SyncedBlock{
			Trigger:    ev,
			Downloaded: downloaded,
			CatchUp:    true,
		}) {
			return
		}

		last := downloaded.ID
		c.last = &last
	}

	if !c.last.Equals(&ev.Block) {
		if sameBlockPosition(*c.last, ev.Block) {
			c.service.log.Debug().
				Str("downloaded", formatBlockRef(*c.last)).
				Str("broadcast", ev.BlockRef()).
				Str("downloaded_root_hash", formatHashPrefix(c.last.RootHash)).
				Str("downloaded_file_hash", formatHashPrefix(c.last.FileHash)).
				Str("broadcast_root_hash", formatHashPrefix(ev.Block.RootHash)).
				Str("broadcast_file_hash", formatHashPrefix(ev.Block.FileHash)).
				Str("overlay", ev.Overlay).
				Str("kind", ev.Kind).
				Msg("broadcast hash differs from downloaded shard chain head")
			return
		}

		c.service.log.Warn().
			Str("downloaded", formatBlockRef(*c.last)).
			Str("broadcast", ev.BlockRef()).
			Str("overlay", ev.Overlay).
			Str("kind", ev.Kind).
			Msg("broadcast head does not match the downloaded shard chain head")
	}
}

func (c *chain) waitForBlock(ctx context.Context, ev p2p.BroadcastEvent) (p2p.DownloadedBlock, bool) {
	for attempt := 1; ; attempt++ {
		downloaded, err := c.service.downloadBlockWithRetry(ctx, ev.Block)
		if err == nil {
			return downloaded, true
		}
		if ctx.Err() != nil {
			return p2p.DownloadedBlock{}, false
		}

		c.service.log.Debug().
			Err(err).
			Str("block", ev.BlockRef()).
			Str("overlay", ev.Overlay).
			Str("kind", ev.Kind).
			Int("attempt", attempt).
			Msg("failed to anchor shard chain from broadcast, will retry in order")
	}
}

func (c *chain) waitForNextBlock(ctx context.Context, ev p2p.BroadcastEvent, prev ton.BlockIDExt) (p2p.DownloadedBlock, bool) {
	for attempt := 1; ; attempt++ {
		downloaded, err := c.service.downloadNextBlockWithRetry(ctx, prev)
		if err == nil {
			return downloaded, true
		}
		if ctx.Err() != nil {
			return p2p.DownloadedBlock{}, false
		}

		c.service.log.Debug().
			Err(err).
			Str("from", formatBlockRef(prev)).
			Str("target", ev.BlockRef()).
			Str("overlay", ev.Overlay).
			Str("kind", ev.Kind).
			Int("attempt", attempt).
			Msg("failed to catch up shard chain after broadcast gap, will retry in order")
	}
}

func (s *Service) emit(ctx context.Context, block SyncedBlock) bool {
	select {
	case <-ctx.Done():
		return false
	case s.out <- block:
		return true
	}
}

func (s *Service) downloadBlockWithRetry(ctx context.Context, block ton.BlockIDExt) (p2p.DownloadedBlock, error) {
	var lastErr error

	for attempt := 1; attempt <= s.retryCount; attempt++ {
		downloaded, err := s.fetcher.DownloadBlockFull(ctx, block)
		if err == nil {
			return downloaded, nil
		}
		lastErr = err

		if attempt == s.retryCount {
			break
		}
		if err = waitOrDone(ctx, s.retryDelay); err != nil {
			return p2p.DownloadedBlock{}, err
		}
	}

	return p2p.DownloadedBlock{}, fmt.Errorf("download block %s: %w", formatBlockRef(block), lastErr)
}

func (s *Service) downloadNextBlockWithRetry(ctx context.Context, prev ton.BlockIDExt) (p2p.DownloadedBlock, error) {
	var lastErr error

	for attempt := 1; attempt <= s.retryCount; attempt++ {
		downloaded, err := s.fetcher.DownloadNextBlockFull(ctx, prev)
		if err == nil {
			return downloaded, nil
		}
		lastErr = err

		if attempt == s.retryCount {
			break
		}
		if err = waitOrDone(ctx, s.retryDelay); err != nil {
			return p2p.DownloadedBlock{}, err
		}
	}

	return p2p.DownloadedBlock{}, fmt.Errorf("download next block after %s: %w", formatBlockRef(prev), lastErr)
}

func waitOrDone(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func chainKey(block ton.BlockIDExt) string {
	return fmt.Sprintf("%d:%016x", block.Workchain, uint64(block.Shard))
}

func sameBlockPosition(a, b ton.BlockIDExt) bool {
	return a.Workchain == b.Workchain && a.Shard == b.Shard && a.SeqNo == b.SeqNo
}

func formatBlockRef(block ton.BlockIDExt) string {
	return fmt.Sprintf("wc=%d shard=%016x seqno=%d", block.Workchain, uint64(block.Shard), block.SeqNo)
}

func formatHashPrefix(hash []byte) string {
	if len(hash) > 8 {
		hash = hash[:8]
	}
	return hex.EncodeToString(hash)
}
