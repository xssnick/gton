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
	defaultInternalQueueCapacity  = 16
	maxRetainedInternalQueue      = 1024
	defaultShardDescriptionBuffer = 1024
	defaultRetryCount             = 3
	defaultRetryDelay             = 500 * time.Millisecond
	defaultPendingBroadcasts      = 64
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
	Internal        bool
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
	internalDrops         atomic.Uint64
	shardDescriptionDrops atomic.Uint64

	internalMu     sync.Mutex
	internalQueue  []p2p.DownloadedBlock
	internalHead   int
	internalLen    int
	internalWake   chan struct{}
	internalClosed bool

	chainsMu sync.Mutex
	chains   map[string]*chain

	startOnce sync.Once
	wg        sync.WaitGroup
}

type StatusSnapshot struct {
	OutputQueueItems              int
	OutputQueueCapacity           int
	InternalQueueItems            int
	InternalQueueCapacity         int
	InternalQueueDropped          uint64
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
	pending map[uint32]p2p.BroadcastEvent
	busy    bool
	closed  bool
	last    *ton.BlockIDExt
}

func New(logger *zerolog.Logger, node Node) *Service {
	return &Service{
		log:               logutil.WithComponent(logger, "blocksync"),
		node:              node,
		out:               make(chan SyncedBlock, defaultOutputBuffer),
		internalQueue:     make([]p2p.DownloadedBlock, defaultInternalQueueCapacity),
		internalWake:      make(chan struct{}, 1),
		shardDescriptions: make(chan p2p.BroadcastEvent, defaultShardDescriptionBuffer),
		chains:            map[string]*chain{},
	}
}

func (s *Service) Blocks() <-chan SyncedBlock {
	return s.out
}

// SubmitBlockLocally queues a locally produced block without waiting for its
// validation or application. The caller transfers immutable ownership of the
// block and its backing buffers. This trusted ingress is lossless while the
// service is running: finalization recovery may be the only remaining source
// of a block after every validator restarts together.
func (s *Service) SubmitBlockLocally(block p2p.DownloadedBlock) {
	s.internalMu.Lock()
	if s.internalClosed {
		s.internalMu.Unlock()
		s.internalDrops.Add(1)

		return
	}
	if s.internalLen == len(s.internalQueue) {
		s.growInternalQueueLocked()
	}
	tail := (s.internalHead + s.internalLen) % len(s.internalQueue)
	s.internalQueue[tail] = block
	s.internalLen++
	s.internalMu.Unlock()

	select {
	case s.internalWake <- struct{}{}:
	default:
	}
}

func (s *Service) ShardDescriptions() <-chan p2p.BroadcastEvent {
	return s.shardDescriptions
}

func (s *Service) StatusSnapshot() StatusSnapshot {
	s.internalMu.Lock()
	internalItems := s.internalLen
	s.internalMu.Unlock()

	snapshot := StatusSnapshot{
		OutputQueueItems:              len(s.out),
		OutputQueueCapacity:           cap(s.out),
		InternalQueueItems:            internalItems,
		InternalQueueDropped:          s.internalDrops.Load(),
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
		if chain.current != nil || len(chain.pending) > 0 {
			snapshot.PendingChains++
		}
		chain.mx.Unlock()
	}
	s.chainsMu.Unlock()
	return snapshot
}

// Start begins the block-stream workflow. The service owns and joins the
// goroutine so callers do not have to add it to another component's lifecycle.
func (s *Service) Start(ctx context.Context) {
	s.startOnce.Do(func() {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.Run(ctx)
		}()
	})
}

// Wait joins the block-stream workflow started by Start. Call it after Start.
func (s *Service) Wait() {
	s.wg.Wait()
}

func (s *Service) Run(ctx context.Context) {
	var wg sync.WaitGroup
	defer func() {
		s.closeInternalQueue()
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
		if block, ok := s.takeInternalBlock(); ok {
			s.emitAsync(ctx, SyncedBlock{
				Downloaded: block,
				Priority:   true,
				Internal:   true,
			})

			continue
		}

		select {
		case <-ctx.Done():
			return
		case <-s.internalWake:
			continue
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

func (s *Service) takeInternalBlock() (p2p.DownloadedBlock, bool) {
	s.internalMu.Lock()
	defer s.internalMu.Unlock()

	if s.internalLen == 0 {
		return p2p.DownloadedBlock{}, false
	}

	block := s.internalQueue[s.internalHead]
	s.internalQueue[s.internalHead] = p2p.DownloadedBlock{}
	s.internalHead = (s.internalHead + 1) % len(s.internalQueue)
	s.internalLen--
	if s.internalLen == 0 {
		s.internalHead = 0
		if len(s.internalQueue) > maxRetainedInternalQueue {
			s.internalQueue = make([]p2p.DownloadedBlock, defaultInternalQueueCapacity)
		}
	}

	return block, true
}

func (s *Service) growInternalQueueLocked() {
	next := make([]p2p.DownloadedBlock, max(defaultInternalQueueCapacity, len(s.internalQueue)*2))
	if s.internalLen != 0 {
		first := copy(next, s.internalQueue[s.internalHead:])
		copy(next[first:], s.internalQueue[:s.internalHead])
	}
	s.internalQueue = next
	s.internalHead = 0
}

func (s *Service) closeInternalQueue() {
	s.internalMu.Lock()
	s.internalClosed = true
	for s.internalLen != 0 {
		s.internalQueue[s.internalHead] = p2p.DownloadedBlock{}
		s.internalHead = (s.internalHead + 1) % len(s.internalQueue)
		s.internalLen--
	}
	s.internalQueue = nil
	s.internalHead = 0
	s.internalMu.Unlock()
}

func (s *Service) emitDirectMasterchainBroadcast(ctx context.Context, ev p2p.BroadcastEvent) bool {
	if !isMasterchainBlock(ev.Block) {
		return false
	}
	if ev.Downloaded == nil || !ev.Downloaded.ID.Equals(&ev.Block) {
		return false
	}
	if !directDownloadedBroadcastKind(ev.Kind) {
		return false
	}

	downloaded := *ev.Downloaded
	s.emitAsync(ctx, SyncedBlock{
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
		if len(c.pending) == 0 {
			copyEv := ev
			c.current = &copyEv
		} else {
			c.storePendingLocked(ev)
			c.promotePendingLocked()
		}
	} else {
		c.storePendingLocked(ev)
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
	if c.last != nil {
		c.prunePendingAtOrBeforeLocked(c.last.SeqNo)
	}
	c.promotePendingLocked()
}

func (c *chain) storePendingLocked(ev p2p.BroadcastEvent) {
	if c.pending == nil {
		c.pending = make(map[uint32]p2p.BroadcastEvent)
	}

	if current, ok := c.pending[ev.Block.SeqNo]; ok {
		if shouldReplacePendingAtSeqno(current, ev) {
			c.pending[ev.Block.SeqNo] = ev
		}
		return
	}

	if len(c.pending) >= defaultPendingBroadcasts {
		highest := c.highestPendingSeqnoLocked()
		if ev.Block.SeqNo >= highest {
			return
		}
		delete(c.pending, highest)
	}

	c.pending[ev.Block.SeqNo] = ev
}

func (c *chain) promotePendingLocked() {
	if c.busy || c.current != nil || len(c.pending) == 0 {
		return
	}

	seqno := c.lowestPendingSeqnoLocked()
	ev := c.pending[seqno]
	delete(c.pending, seqno)
	c.current = &ev
}

func (c *chain) prunePendingAtOrBeforeLocked(seqno uint32) {
	for pendingSeqno := range c.pending {
		if pendingSeqno <= seqno {
			delete(c.pending, pendingSeqno)
		}
	}
}

func (c *chain) lowestPendingSeqnoLocked() uint32 {
	var lowest uint32
	first := true
	for seqno := range c.pending {
		if first || seqno < lowest {
			lowest = seqno
			first = false
		}
	}
	return lowest
}

func (c *chain) highestPendingSeqnoLocked() uint32 {
	var highest uint32
	for seqno := range c.pending {
		if seqno > highest {
			highest = seqno
		}
	}
	return highest
}

func shouldReplacePendingAtSeqno(current, next p2p.BroadcastEvent) bool {
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

		if !c.service.emit(ctx, SyncedBlock{
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
		downloaded, trigger, ok := c.waitForNextBlock(ctx, ev, prev)
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

		if !c.service.emit(ctx, SyncedBlock{
			Trigger:         trigger,
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

func (c *chain) waitForNextBlock(ctx context.Context, ev p2p.BroadcastEvent, prev ton.BlockIDExt) (p2p.DownloadedBlock, p2p.BroadcastEvent, bool) {
	if ev.Downloaded != nil && ev.Downloaded.ID.Equals(&ev.Block) && ev.Downloaded.ID.SeqNo == prev.SeqNo+1 {
		return *ev.Downloaded, ev, true
	}
	if fullBlockBroadcastKind(ev.Kind) && ev.Block.SeqNo == prev.SeqNo+1 {
		c.service.log.Debug().
			Str("from", storage.FormatBlockRef(prev)).
			Str("target", ev.BlockRef()).
			Str("overlay", ev.Overlay).
			Str("kind", ev.Kind).
			Msg("dropping full block broadcast without decoded next payload")
		return p2p.DownloadedBlock{}, ev, false
	}

retry:
	for attempt := 1; ; attempt++ {
		if downloaded, trigger, ok := c.takePendingDownloadedNext(prev); ok {
			return downloaded, trigger, true
		}

		downloadCtx, cancel := context.WithCancel(ctx)
		result := make(chan nextBlockDownloadResult, 1)
		go func() {
			downloaded, err := c.service.downloadNextBlockWithRetry(downloadCtx, prev)
			result <- nextBlockDownloadResult{downloaded: downloaded, err: err}
		}()

		for {
			select {
			case res := <-result:
				cancel()
				if res.err == nil {
					return res.downloaded, ev, true
				}
				if ctx.Err() != nil {
					return p2p.DownloadedBlock{}, ev, false
				}

				c.service.log.Debug().
					Err(res.err).
					Str("from", storage.FormatBlockRef(prev)).
					Str("target", ev.BlockRef()).
					Str("overlay", ev.Overlay).
					Str("kind", ev.Kind).
					Int("attempt", attempt).
					Msg("failed to catch up shard chain after broadcast gap, will retry in order")
				continue retry
			case <-c.notify:
				if downloaded, trigger, ok := c.takePendingDownloadedNext(prev); ok {
					cancel()
					return downloaded, trigger, true
				}
			case <-ctx.Done():
				cancel()
				return p2p.DownloadedBlock{}, ev, false
			}
		}
	}
}

type nextBlockDownloadResult struct {
	downloaded p2p.DownloadedBlock
	err        error
}

func (c *chain) takePendingDownloadedNext(prev ton.BlockIDExt) (p2p.DownloadedBlock, p2p.BroadcastEvent, bool) {
	nextSeqno := prev.SeqNo + 1

	c.mx.Lock()
	defer c.mx.Unlock()

	ev, ok := c.pending[nextSeqno]
	if !ok || ev.Downloaded == nil || !ev.Downloaded.ID.Equals(&ev.Block) || ev.Downloaded.ID.SeqNo != nextSeqno {
		return p2p.DownloadedBlock{}, p2p.BroadcastEvent{}, false
	}

	delete(c.pending, nextSeqno)
	return *ev.Downloaded, ev, true
}

func fullBlockBroadcastKind(kind string) bool {
	switch kind {
	case "tonNode.blockBroadcast", "tonNode.blockBroadcastCompressed", "tonNode.blockBroadcastCompressedV2":
		return true
	default:
		return false
	}
}

func directDownloadedBroadcastKind(kind string) bool {
	return fullBlockBroadcastKind(kind) || kind == "tonNode.blockFinalityBroadcast"
}

func (s *Service) emit(ctx context.Context, block SyncedBlock) bool {
	block.ack = make(chan bool, 1)

	select {
	case <-ctx.Done():
		return false
	case s.out <- block:
	}

	select {
	case <-ctx.Done():
		return false
	case accepted := <-block.ack:
		return accepted
	}
}

func (s *Service) emitAsync(ctx context.Context, block SyncedBlock) {
	block.ack = nil

	select {
	case <-ctx.Done():
	case s.out <- block:
	}
}

func (s *Service) downloadBlockWithRetry(ctx context.Context, block ton.BlockIDExt) (p2p.DownloadedBlock, error) {
	return s.downloadWithRetry(ctx, func() string {
		return "download block " + storage.FormatBlockRef(block)
	}, func(ctx context.Context) (p2p.DownloadedBlock, error) {
		downloaded, err := s.node.DownloadBlockFull(ctx, block)
		if err != nil {
			return p2p.DownloadedBlock{}, err
		}
		return *downloaded, nil
	})
}

func (s *Service) downloadNextBlockWithRetry(ctx context.Context, prev ton.BlockIDExt) (p2p.DownloadedBlock, error) {
	return s.downloadWithRetry(ctx, func() string {
		return "download next block after " + storage.FormatBlockRef(prev)
	}, func(ctx context.Context) (p2p.DownloadedBlock, error) {
		downloaded, err := s.node.DownloadNextBlockFull(ctx, prev)
		if err != nil {
			return p2p.DownloadedBlock{}, err
		}
		return *downloaded, nil
	})
}

// downloadWithRetry takes the label lazily: it is only read when every attempt
// failed, and formatting a block ref per download is pure waste on the hot path.
func (s *Service) downloadWithRetry(ctx context.Context, label func() string, download func(context.Context) (p2p.DownloadedBlock, error)) (p2p.DownloadedBlock, error) {
	var lastErr error

	for attempt := 1; attempt <= defaultRetryCount; attempt++ {
		downloaded, err := download(ctx)
		if err == nil {
			return downloaded, nil
		}
		lastErr = err

		if attempt == defaultRetryCount {
			break
		}
		if err = waitOrDone(ctx, defaultRetryDelay); err != nil {
			return p2p.DownloadedBlock{}, err
		}
	}

	return p2p.DownloadedBlock{}, fmt.Errorf("%s: %w", label(), lastErr)
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
