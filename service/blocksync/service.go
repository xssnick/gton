package blocksync

import (
	"context"
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xssnick/gton/internal/logutil"
	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/ton"
)

const (
	defaultOutputBuffer           = 256
	defaultShardDescriptionBuffer = 1024
	defaultRetryCount             = 3
	defaultRetryDelay             = 500 * time.Millisecond
	masterchainShard              = int64(-1 << 63)
)

type Node interface {
	Events() <-chan p2p.BroadcastEvent
	DownloadBlockFull(ctx context.Context, block ton.BlockIDExt) (*p2p.DownloadedBlock, error)
	DownloadNextBlockFull(ctx context.Context, prev ton.BlockIDExt) (*p2p.DownloadedBlock, error)
}

type SyncedBlock struct {
	Trigger         p2p.BroadcastEvent
	Downloaded      p2p.DownloadedBlock
	CatchUp         bool
	Priority        bool
	DownloadElapsed time.Duration
	ack             chan bool
}

func (b SyncedBlock) Accept() {
	b.ackResult(true)
}

func (b SyncedBlock) Reject() {
	b.ackResult(false)
}

func (b SyncedBlock) ackResult(accepted bool) {
	if b.ack == nil {
		return
	}

	select {
	case b.ack <- accepted:
	default:
	}
}

type Service struct {
	log                   zerolog.Logger
	node                  Node
	out                   chan SyncedBlock
	shardDescriptions     chan p2p.BroadcastEvent
	shardDescriptionDrops atomic.Uint64
	retryCount            int
	retryDelay            time.Duration

	chainsMu sync.Mutex
	chains   map[string]*chain
}

type StatusSnapshot struct {
	OutputQueueItems              int
	OutputQueueCapacity           int
	ShardDescriptionQueueItems    int
	ShardDescriptionQueueCapacity int
	ShardDescriptionDropped       uint64
	Chains                        int
	BusyChains                    int
	PendingChains                 int
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

func New(logger *zerolog.Logger, node Node) *Service {
	return &Service{
		log:               logutil.WithComponent(logger, "blocksync"),
		node:              node,
		out:               make(chan SyncedBlock, defaultOutputBuffer),
		shardDescriptions: make(chan p2p.BroadcastEvent, defaultShardDescriptionBuffer),
		retryCount:        defaultRetryCount,
		retryDelay:        defaultRetryDelay,
		chains:            map[string]*chain{},
	}
}

func (s *Service) Blocks() <-chan SyncedBlock {
	return s.out
}

func (s *Service) ShardDescriptions() <-chan p2p.BroadcastEvent {
	return s.shardDescriptions
}

func (s *Service) StatusSnapshot() StatusSnapshot {
	if s == nil {
		return StatusSnapshot{}
	}

	snapshot := StatusSnapshot{
		OutputQueueItems:              len(s.out),
		OutputQueueCapacity:           cap(s.out),
		ShardDescriptionQueueItems:    len(s.shardDescriptions),
		ShardDescriptionQueueCapacity: cap(s.shardDescriptions),
		ShardDescriptionDropped:       s.shardDescriptionDrops.Load(),
	}
	s.chainsMu.Lock()
	snapshot.Chains = len(s.chains)
	for _, chain := range s.chains {
		chain.mx.Lock()
		if chain.busy {
			snapshot.BusyChains++
		}
		if chain.current != nil || chain.future != nil {
			snapshot.PendingChains++
		}
		chain.mx.Unlock()
	}
	s.chainsMu.Unlock()
	return snapshot
}

func (s *Service) Run(ctx context.Context) {
	var wg sync.WaitGroup
	defer func() {
		s.chainsMu.Lock()
		for _, chain := range s.chains {
			chain.close()
		}
		s.chainsMu.Unlock()
		wg.Wait()
		close(s.out)
		close(s.shardDescriptions)
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-s.node.Events():
			if !ok {
				return
			}

			if s.emitDirectMasterchainBroadcast(ctx, ev) {
				continue
			}
			if s.emitShardDescriptionBroadcast(ev) {
				continue
			}
			if !isMasterchainBlock(ev.Block) {
				s.log.Debug().
					Str("block", ev.BlockRef()).
					Str("overlay", ev.Overlay).
					Str("kind", ev.Kind).
					Msg("ignoring non-masterchain broadcast in block sync")
				continue
			}

			chain := s.getOrCreateChain(ctx, &wg, ev.Block)
			if !chain.enqueue(ev) {
				return
			}
		}
	}
}

func (s *Service) emitDirectMasterchainBroadcast(ctx context.Context, ev p2p.BroadcastEvent) bool {
	if !isMasterchainBlock(ev.Block) {
		return false
	}
	if ev.Downloaded == nil || !ev.Downloaded.ID.Equals(&ev.Block) {
		return false
	}
	if !fullBlockBroadcastKind(ev.Kind) {
		return false
	}

	downloaded := *ev.Downloaded
	s.emitAsync(ctx, &SyncedBlock{
		Trigger:    ev,
		Downloaded: downloaded,
		CatchUp:    false,
		Priority:   true,
	})
	return true
}

func (s *Service) emitShardDescriptionBroadcast(ev p2p.BroadcastEvent) bool {
	if ev.Kind != "tonNode.newShardBlockBroadcast" || ev.ShardDescription == nil || isMasterchainBlock(ev.Block) {
		return false
	}

	select {
	case s.shardDescriptions <- ev:
	default:
		s.shardDescriptionDrops.Add(1)
		s.log.Debug().
			Str("block", ev.BlockRef()).
			Str("overlay", ev.Overlay).
			Msg("dropping shard description broadcast because queue is full")
	}
	return true
}

func isMasterchainBlock(block ton.BlockIDExt) bool {
	return block.Workchain == -1 && block.Shard == masterchainShard
}

func (s *Service) getOrCreateChain(ctx context.Context, wg *sync.WaitGroup, block ton.BlockIDExt) *chain {
	key := chainKey(block)
	s.chainsMu.Lock()
	defer s.chainsMu.Unlock()
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
		downloadStarted := time.Now()
		downloaded, ok := c.waitForBlock(ctx, ev)
		downloadElapsed := time.Since(downloadStarted)
		if !ok {
			return
		}

		if !c.service.emit(ctx, &SyncedBlock{
			Trigger:         ev,
			Downloaded:      downloaded,
			CatchUp:         false,
			DownloadElapsed: downloadElapsed,
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
				Str("expected", storage.FormatBlockRef(*c.last)).
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

		downloadStarted := time.Now()
		downloaded, ok := c.waitForNextBlock(ctx, ev, prev)
		downloadElapsed := time.Since(downloadStarted)
		if !ok {
			return
		}

		if downloaded.ID.SeqNo <= prev.SeqNo {
			c.service.log.Warn().
				Str("from", storage.FormatBlockRef(prev)).
				Str("got", downloaded.BlockRef()).
				Str("overlay", ev.Overlay).
				Str("kind", ev.Kind).
				Msg("peer returned non-advancing next block")
			return
		}

		if chainKey(downloaded.ID) != c.key {
			c.service.log.Warn().
				Str("from", storage.FormatBlockRef(prev)).
				Str("got", downloaded.BlockRef()).
				Str("target", ev.BlockRef()).
				Str("overlay", ev.Overlay).
				Str("kind", ev.Kind).
				Msg("next block moved to another shard chain, catch-up is not implemented for this transition")
			return
		}

		if !c.service.emit(ctx, &SyncedBlock{
			Trigger:         ev,
			Downloaded:      downloaded,
			CatchUp:         true,
			DownloadElapsed: downloadElapsed,
		}) {
			return
		}

		last := downloaded.ID
		c.last = &last
	}

	if !c.last.Equals(&ev.Block) {
		if sameBlockPosition(*c.last, ev.Block) {
			c.service.log.Debug().
				Str("downloaded", storage.FormatBlockRef(*c.last)).
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
			Str("downloaded", storage.FormatBlockRef(*c.last)).
			Str("broadcast", ev.BlockRef()).
			Str("overlay", ev.Overlay).
			Str("kind", ev.Kind).
			Msg("broadcast head does not match the downloaded shard chain head")
	}
}

func (c *chain) waitForBlock(ctx context.Context, ev p2p.BroadcastEvent) (p2p.DownloadedBlock, bool) {
	if ev.Downloaded != nil && ev.Downloaded.ID.Equals(&ev.Block) {
		return *ev.Downloaded, true
	}
	if fullBlockBroadcastKind(ev.Kind) {
		c.service.log.Debug().
			Str("block", ev.BlockRef()).
			Str("overlay", ev.Overlay).
			Str("kind", ev.Kind).
			Msg("dropping full block broadcast without decoded payload")
		return p2p.DownloadedBlock{}, false
	}

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
	if ev.Downloaded != nil && ev.Downloaded.ID.Equals(&ev.Block) && ev.Downloaded.ID.SeqNo == prev.SeqNo+1 {
		return *ev.Downloaded, true
	}
	if fullBlockBroadcastKind(ev.Kind) && ev.Block.SeqNo == prev.SeqNo+1 {
		c.service.log.Debug().
			Str("from", storage.FormatBlockRef(prev)).
			Str("target", ev.BlockRef()).
			Str("overlay", ev.Overlay).
			Str("kind", ev.Kind).
			Msg("dropping full block broadcast without decoded next payload")
		return p2p.DownloadedBlock{}, false
	}

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
			Str("from", storage.FormatBlockRef(prev)).
			Str("target", ev.BlockRef()).
			Str("overlay", ev.Overlay).
			Str("kind", ev.Kind).
			Int("attempt", attempt).
			Msg("failed to catch up shard chain after broadcast gap, will retry in order")
	}
}

func fullBlockBroadcastKind(kind string) bool {
	switch kind {
	case "tonNode.blockBroadcast", "tonNode.blockBroadcastCompressed", "tonNode.blockBroadcastCompressedV2":
		return true
	default:
		return false
	}
}

func (s *Service) emit(ctx context.Context, block *SyncedBlock) bool {
	block.ack = make(chan bool, 1)

	select {
	case <-ctx.Done():
		return false
	case s.out <- *block:
	}

	select {
	case <-ctx.Done():
		return false
	case accepted := <-block.ack:
		return accepted
	}
}

func (s *Service) emitAsync(ctx context.Context, block *SyncedBlock) bool {
	block.ack = nil

	select {
	case <-ctx.Done():
		return false
	case s.out <- *block:
		return true
	}
}

func (s *Service) downloadBlockWithRetry(ctx context.Context, block ton.BlockIDExt) (p2p.DownloadedBlock, error) {
	return s.downloadWithRetry(ctx, fmt.Sprintf("download block %s", storage.FormatBlockRef(block)), func(ctx context.Context) (p2p.DownloadedBlock, error) {
		downloaded, err := s.node.DownloadBlockFull(ctx, block)
		if err != nil {
			return p2p.DownloadedBlock{}, err
		}
		if downloaded == nil {
			return p2p.DownloadedBlock{}, fmt.Errorf("download block %s: empty response", storage.FormatBlockRef(block))
		}
		return *downloaded, nil
	})
}

func (s *Service) downloadNextBlockWithRetry(ctx context.Context, prev ton.BlockIDExt) (p2p.DownloadedBlock, error) {
	return s.downloadWithRetry(ctx, fmt.Sprintf("download next block after %s", storage.FormatBlockRef(prev)), func(ctx context.Context) (p2p.DownloadedBlock, error) {
		downloaded, err := s.node.DownloadNextBlockFull(ctx, prev)
		if err != nil {
			return p2p.DownloadedBlock{}, err
		}
		if downloaded == nil {
			return p2p.DownloadedBlock{}, fmt.Errorf("download next block after %s: empty response", storage.FormatBlockRef(prev))
		}
		return *downloaded, nil
	})
}

func (s *Service) downloadWithRetry(ctx context.Context, label string, download func(context.Context) (p2p.DownloadedBlock, error)) (p2p.DownloadedBlock, error) {
	var lastErr error

	for attempt := 1; attempt <= s.retryCount; attempt++ {
		downloaded, err := download(ctx)
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

	return p2p.DownloadedBlock{}, fmt.Errorf("%s: %w", label, lastErr)
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

func formatHashPrefix(hash []byte) string {
	if len(hash) > 8 {
		hash = hash[:8]
	}
	return hex.EncodeToString(hash)
}
