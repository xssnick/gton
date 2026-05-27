package p2p

import (
	"container/list"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/gton/internal/extmsg"
	"github.com/xssnick/gton/internal/logutil"
	tnstate "github.com/xssnick/gton/service/state"
	storage2 "github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/adnl"
	adnladdr "github.com/xssnick/tonutils-go/adnl/address"
	"github.com/xssnick/tonutils-go/adnl/dht"
	"github.com/xssnick/tonutils-go/adnl/keys"
	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/adnl/rldp"
	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

type dhtBackend interface {
	Close()
	FindOverlayNodes(ctx context.Context, overlayKey []byte, continuation ...*dht.Continuation) (*overlay.NodesList, *dht.Continuation, error)
	FindAddresses(ctx context.Context, key []byte) (*adnladdr.List, ed25519.PublicKey, error)
	FindValue(ctx context.Context, key *dht.Key, continuation ...*dht.Continuation) (*dht.Value, *dht.Continuation, error)
	StoreAddress(ctx context.Context, addresses adnladdr.List, ttl time.Duration, ownerKey ed25519.PrivateKey) (storedCount int, idKey []byte, err error)
	StoreOverlayNodes(ctx context.Context, overlayKey []byte, nodes *overlay.NodesList, ttl time.Duration) (storedCount int, idKey []byte, err error)
}

var _ dhtBackend = (*dht.Client)(nil)

type Node struct {
	log        zerolog.Logger
	cfgPath    string
	listenAddr string
	externalIP net.IP
	privKey    ed25519.PrivateKey
	dhtPrivKey ed25519.PrivateKey
	gateway    *adnl.Gateway
	dhtGateway *adnl.Gateway
	dhtServer  *dht.Server
	dht        dhtBackend
	pool       *peerPool
	events     chan BroadcastEvent
	eventQueue *boundedQueue[BroadcastEvent]
	deduper    *eventDeduper

	myExternalMessages        *eventDeduper
	processedExternalMessages *eventDeduper
	externalMessageLimiter    *extmsg.AddressLimiter

	onceStop sync.Once
	stopped  chan struct{}
	wg       sync.WaitGroup

	inboundMx       sync.Mutex
	inboundStopping bool
	inboundWG       sync.WaitGroup

	runCtx                context.Context
	zeroStateFileHash     []byte
	zeroStateBlock        ton.BlockIDExt
	initBlock             ton.BlockIDExt
	externalPort          uint16
	dhtListenAddr         string
	storage               storage2.Storage
	peerStorage           storage2.PeerServingStorage
	compressedState       CompressedBlockStateProvider
	syncLag               SyncLagProvider
	signatureVerifierMx   sync.RWMutex
	signatureVerifier     MasterchainBroadcastSignatureVerifier
	stateReadyMx          sync.Mutex
	stateReadyNotify      chan struct{}
	shardBroadcastCache   *shardBroadcastBlockCache
	shardBroadcastWaitMx  sync.Mutex
	shardBroadcastWaiters map[string][]chan struct{}
	blockCacheObserver    BlockCacheObserver
	blockCacheSlots       chan struct{}
	rebroadcastQuiet      atomic.Bool

	rebroadcastThrottleMu   sync.Mutex
	rebroadcastThrottleLast map[string]time.Time
	rebroadcastFECSlots     map[rebroadcastFECLimiterClass]chan struct{}
	rebroadcastFECMu        sync.Mutex
	rebroadcastFECPeers     map[rebroadcastFECLimiterClass]map[string]struct{}

	pendingBroadcastMx sync.Mutex
	pendingBroadcasts  map[string]struct{}

	broadcastStatsMx sync.Mutex
	broadcastStats   map[broadcastStatKey]uint64

	fecReceiverStatsMx sync.Mutex
	fecReceiverLast    map[fecReceiverPeerKey]fecReceiverCounterSnapshot
	fecReceiverTotals  map[string]fecReceiverCounterSnapshot

	localRebroadcastSent    atomic.Uint64
	localRebroadcastDropped atomic.Uint64
	peerRebroadcastSent     atomic.Uint64
	peerRebroadcastDropped  atomic.Uint64

	subscriptionsMx sync.RWMutex
	subscriptions   map[string]*overlaySubscription

	monitorSplitMx       sync.RWMutex
	monitorMinSplitDepth map[int32]uint32

	latestBlocksMx            sync.RWMutex
	observedMasterchain       *ton.BlockIDExt
	seenMasterchain           *ton.BlockIDExt
	latestBasechain           *ton.BlockIDExt
	latestBasechainShards     map[storage2.ShardKey]ton.BlockIDExt
	rawMasterchainBroadcast   *ton.BlockIDExt
	observedMasterchainNotify chan struct{}
	seenMasterchainNotify     chan struct{}
	latestBasechainNotify     chan struct{}
	rawMasterchainNotify      chan struct{}
	stateFilesDir             string
	downloadPeerMx            sync.RWMutex
	downloadPeerLeases        map[string]int
	stateCellImportSlot       chan struct{}
	stateSplitPartDecodeSlot  chan struct{}
	zeroStateBootstrapMu      sync.Mutex
}

func New(opts Options) (*Node, error) {
	logger := logutil.WithComponent(opts.Logger, "p2p")
	if rldp.MaxFECDataSize < persistentStateChunkAnswerMax {
		rldp.MaxFECDataSize = persistentStateChunkAnswerMax
	}
	rldp.Logger = func(args ...any) {
		msg := strings.TrimSpace(fmt.Sprintln(args...))
		if strings.Contains(msg, "received out of order part") ||
			strings.Contains(msg, "unsupported peer query") {
			return
		}
		if strings.Contains(msg, "error") ||
			strings.Contains(msg, "failed") ||
			strings.Contains(msg, "invalid") ||
			strings.Contains(msg, "too big") {
			if len(msg) > 2000 {
				msg = msg[:2000] + "...(truncated)"
			}
			logger.Debug().Str("rldp", msg).Msg("rldp diagnostic")
		}
	}

	cfgPath := opts.GlobalConfigPath
	if cfgPath == "" {
		cfgPath = DefaultGlobalConfigPath
	}

	priv, err := privateKeyOrGenerate(opts.PrivateKey)
	if err != nil {
		return nil, fmt.Errorf("load ADNL key: %w", err)
	}

	var dhtPriv ed25519.PrivateKey
	if len(opts.DHTPrivateKey) > 0 {
		dhtPriv, err = privateKeyOrGenerate(opts.DHTPrivateKey)
		if err != nil {
			return nil, fmt.Errorf("load DHT ADNL key: %w", err)
		}
	}
	if opts.DHTListenAddr != "" {
		if len(dhtPriv) == 0 {
			dhtPriv, err = privateKeyOrGenerate(nil)
			if err != nil {
				return nil, fmt.Errorf("generate DHT ADNL key: %w", err)
			}
		}
	}

	gateway := adnl.NewGateway(priv)

	peerStorage := opts.PeerServingStorage
	storage := opts.Storage
	if storage != nil {
		peerStorage = storage
	}
	if peerStorage == nil {
		return nil, fmt.Errorf("peer serving storage is required")
	}

	stateFilesDir, err := prepareStateFilesDir(opts.StateFilesDir)
	if err != nil {
		return nil, fmt.Errorf("prepare state files dir: %w", err)
	}

	return &Node{
		log:                       logger,
		cfgPath:                   cfgPath,
		listenAddr:                opts.ListenAddr,
		externalIP:                append(net.IP(nil), opts.ExternalIP...),
		externalPort:              opts.ExternalPort,
		privKey:                   priv,
		dhtPrivKey:                dhtPriv,
		gateway:                   gateway,
		dhtListenAddr:             opts.DHTListenAddr,
		pool:                      newPeerPool(gateway),
		events:                    make(chan BroadcastEvent, broadcastEventBuffer),
		eventQueue:                newBoundedQueue(broadcastQueueMaxItems, broadcastQueueMaxBytes, broadcastEventBytes),
		deduper:                   newEventDeduper(10*time.Minute, broadcastDeduperMaxEntries),
		myExternalMessages:        newEventDeduper(externalMessageCacheTTL, externalMessageCacheMax),
		processedExternalMessages: newEventDeduper(externalMessageCacheTTL, externalMessageCacheMax),
		externalMessageLimiter:    extmsg.NewDefaultAddressLimiter(),
		stopped:                   make(chan struct{}),
		subscriptions:             map[string]*overlaySubscription{},
		monitorMinSplitDepth:      map[int32]uint32{},
		latestBasechainShards:     map[storage2.ShardKey]ton.BlockIDExt{},
		observedMasterchainNotify: make(chan struct{}),
		seenMasterchainNotify:     make(chan struct{}),
		latestBasechainNotify:     make(chan struct{}),
		rawMasterchainNotify:      make(chan struct{}),
		storage:                   storage,
		peerStorage:               peerStorage,
		compressedState:           opts.CompressedState,
		syncLag:                   opts.SyncLag,
		stateReadyNotify:          make(chan struct{}),
		shardBroadcastCache:       newShardBroadcastBlockCache(shardBroadcastBlockCacheTTL, shardBroadcastBlockCacheMaxBytes, shardBroadcastBlockCacheMaxItems),
		shardBroadcastWaiters:     map[string][]chan struct{}{},
		blockCacheSlots:           make(chan struct{}, 2),
		rebroadcastThrottleLast:   map[string]time.Time{},
		rebroadcastFECSlots:       newRebroadcastFECSlotLimits(),
		rebroadcastFECPeers:       newRebroadcastFECPeerLimits(),
		pendingBroadcasts:         map[string]struct{}{},
		broadcastStats:            map[broadcastStatKey]uint64{},
		fecReceiverLast:           map[fecReceiverPeerKey]fecReceiverCounterSnapshot{},
		fecReceiverTotals:         map[string]fecReceiverCounterSnapshot{},
		stateFilesDir:             stateFilesDir,
		downloadPeerLeases:        map[string]int{},
		stateCellImportSlot:       make(chan struct{}, 1),
		stateSplitPartDecodeSlot:  make(chan struct{}, 1),
	}, nil
}

func prepareStateFilesDir(dir string) (string, error) {
	if strings.TrimSpace(dir) == "" {
		return "", fmt.Errorf("state files dir is empty")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	if err := removeIncompleteStateFiles(dir); err != nil {
		return "", err
	}
	return dir, nil
}

func privateKeyOrGenerate(key ed25519.PrivateKey) (ed25519.PrivateKey, error) {
	if len(key) == 0 {
		_, priv, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			return nil, err
		}
		return priv, nil
	}

	if len(key) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("expected %d bytes, got %d", ed25519.PrivateKeySize, len(key))
	}

	return append(ed25519.PrivateKey(nil), key...), nil
}

func (n *Node) Events() <-chan BroadcastEvent {
	return n.events
}

func (n *Node) Start(ctx context.Context) error {
	cfg, err := liteclient.GetConfigFromFile(n.cfgPath)
	if err != nil {
		return fmt.Errorf("load TON config: %w", err)
	}

	zeroBlock := blockIDFromConfig(cfg.Validator.ZeroState)
	if zeroBlock.Workchain != -1 || zeroBlock.Shard != topShard || zeroBlock.SeqNo != 0 || !validBlockID(zeroBlock) {
		return fmt.Errorf("global config contains invalid zero_state")
	}

	initBlock, err := initBlockFromConfig(cfg.Validator.InitBlock, zeroBlock)
	if err != nil {
		return err
	}

	n.runCtx = ctx
	n.zeroStateFileHash = append([]byte(nil), zeroBlock.FileHash...)
	n.zeroStateBlock = zeroBlock
	n.initBlock = initBlock
	specs, err := buildOverlaySpecs(n.zeroStateFileHash)
	if err != nil {
		return err
	}
	for _, spec := range specs {
		n.getOrCreateSubscription(spec)
	}

	n.gateway.SetConnectionHandler(n.handleInboundPeer)
	if err = n.startGateway(); err != nil {
		return fmt.Errorf("start ADNL gateway: %w", err)
	}

	if err = n.startDHT(ctx, cfg); err != nil {
		_ = n.gateway.Close()
		return err
	}

	for _, spec := range specs {
		sub, _ := n.getOrCreateSubscription(spec)
		n.startSubscription(sub)
	}
	n.runAsync(func() {
		n.runEventLoop(ctx)
	})
	n.runAsync(func() {
		n.runShardBroadcastCacheJanitor(ctx)
	})
	n.runAsync(func() {
		n.runSubscriptionLifecycleLoop(ctx)
	})
	if n.isPublicServer() {
		n.runAsync(func() {
			n.runAnnounceLoop(ctx)
		})
	}

	go func() {
		<-ctx.Done()
		n.stop()
	}()

	return nil
}

func (n *Node) runAsync(fn func()) {
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		fn()
	}()
}

func (n *Node) startSubscription(sub *overlaySubscription) {
	if sub == nil || n.runCtx == nil {
		return
	}

	ctx, cancel := context.WithCancel(n.runCtx)
	token, ok := sub.setRunCancel(cancel)
	if !ok {
		cancel()
		return
	}

	n.runAsync(func() {
		defer sub.clearRunCancel(token)
		sub.run(ctx)
	})
}

func (n *Node) Wait() {
	<-n.stopped
}

func (n *Node) stop() {
	n.onceStop.Do(func() {
		defer close(n.stopped)

		n.stopAcceptingInbound()

		if n.dhtServer != nil {
			_ = n.dhtServer.Close()
		}
		if n.dhtServer == nil && n.dht != nil {
			n.dht.Close()
		}
		if n.dhtGateway != nil {
			_ = n.dhtGateway.Close()
		}
		_ = n.gateway.Close()

		n.eventQueue.Close()
		n.closePeerRebroadcastQueues()

		n.wg.Wait()
		n.inboundWG.Wait()

		if n.stateFilesDir != "" {
			_ = removeIncompleteStateFiles(n.stateFilesDir)
		}
	})
}

func (n *Node) stopAcceptingInbound() {
	n.inboundMx.Lock()
	n.inboundStopping = true
	n.inboundMx.Unlock()
}

func (n *Node) beginInbound() bool {
	n.inboundMx.Lock()
	defer n.inboundMx.Unlock()

	if n.inboundStopping {
		return false
	}
	n.inboundWG.Add(1)
	return true
}

func (n *Node) finishInbound() {
	n.inboundWG.Done()
}

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

func (n *Node) MasterchainBroadcastAfter(seqno uint32) (<-chan struct{}, bool) {
	n.latestBlocksMx.RLock()
	defer n.latestBlocksMx.RUnlock()

	if n.rawMasterchainBroadcast != nil && n.rawMasterchainBroadcast.SeqNo > seqno {
		return nil, true
	}
	return n.rawMasterchainNotify, false
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
	if n.latestBasechainShards == nil {
		n.latestBasechainShards = map[storage2.ShardKey]ton.BlockIDExt{}
	}
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

func (n *Node) WaitObservedMasterchainBlock(ctx context.Context) (ton.BlockIDExt, error) {
	startedAt := time.Now()
	progressTicker := time.NewTicker(masterchainWaitLogEvery)
	defer progressTicker.Stop()

	for {
		n.latestBlocksMx.RLock()
		current := n.observedMasterchain
		wait := n.observedMasterchainNotify
		n.latestBlocksMx.RUnlock()

		if current != nil {
			return *current, nil
		}

		select {
		case <-ctx.Done():
			return ton.BlockIDExt{}, ctx.Err()
		case <-wait:
		case <-progressTicker.C:
			n.logMasterchainWaitProgress(startedAt)
		}
	}
}

func (n *Node) WaitBasechainBlock(ctx context.Context) (ton.BlockIDExt, error) {
	startedAt := time.Now()
	progressTicker := time.NewTicker(masterchainWaitLogEvery)
	defer progressTicker.Stop()

	for {
		n.latestBlocksMx.RLock()
		current := n.latestBasechain
		wait := n.latestBasechainNotify
		n.latestBlocksMx.RUnlock()

		if current != nil {
			return *current, nil
		}

		select {
		case <-ctx.Done():
			return ton.BlockIDExt{}, ctx.Err()
		case <-wait:
		case <-progressTicker.C:
			n.logBasechainWaitProgress(startedAt)
		}
	}
}

func (n *Node) logMasterchainWaitProgress(startedAt time.Time) {
	n.logLatestBlockWaitProgress(startedAt, "masterchain")
}

func (n *Node) logBasechainWaitProgress(startedAt time.Time) {
	n.logLatestBlockWaitProgress(startedAt, "basechain")
}

func (n *Node) logLatestBlockWaitProgress(startedAt time.Time, chain string) {
	subscriptions := n.subscriptionsSnapshot()

	var knownPeers, alivePeers, activeNeighbours, aliveNeighbours int
	for _, sub := range subscriptions {
		status := sub.statusSnapshot()
		knownPeers += status.KnownPeers
		alivePeers += status.AliveKnownPeers
		activeNeighbours += status.ActiveNeighbours
		aliveNeighbours += status.AliveNeighbours
	}

	n.log.Info().
		Dur("elapsed", time.Since(startedAt)).
		Int("overlays", len(subscriptions)).
		Int("known_peers", knownPeers).
		Int("alive_peers", alivePeers).
		Int("active_neighbours", activeNeighbours).
		Int("alive_neighbours", aliveNeighbours).
		Msgf("waiting for latest %s block broadcast", chain)
}

func (n *Node) ZeroStateBlock() (ton.BlockIDExt, error) {
	block, ok := n.configuredZeroStateBlock()
	if !ok {
		return ton.BlockIDExt{}, storage2.ErrNotFound
	}
	return block, nil
}

func (n *Node) InitBlock() (ton.BlockIDExt, error) {
	block, ok := n.configuredInitBlock()
	if !ok {
		return ton.BlockIDExt{}, storage2.ErrNotFound
	}
	return block, nil
}

func (n *Node) configuredZeroStateBlock() (ton.BlockIDExt, bool) {
	if n.zeroStateBlock.Workchain != -1 || n.zeroStateBlock.Shard != topShard || n.zeroStateBlock.SeqNo != 0 || !validBlockID(n.zeroStateBlock) {
		return ton.BlockIDExt{}, false
	}
	return n.zeroStateBlock, true
}

func (n *Node) configuredInitBlock() (ton.BlockIDExt, bool) {
	if n.initBlock.Workchain != -1 || n.initBlock.Shard != topShard || !validBlockID(n.initBlock) {
		return ton.BlockIDExt{}, false
	}
	return n.initBlock, true
}

func blockIDFromConfig(block liteclient.ConfigBlock) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: block.Workchain,
		Shard:     block.Shard,
		SeqNo:     block.SeqNo,
		RootHash:  append([]byte(nil), block.RootHash...),
		FileHash:  append([]byte(nil), block.FileHash...),
	}
}

func initBlockFromConfig(block liteclient.ConfigBlock, zeroBlock ton.BlockIDExt) (ton.BlockIDExt, error) {
	if emptyConfigBlock(block) {
		return zeroBlock, nil
	}

	initBlock := blockIDFromConfig(block)
	if initBlock.Workchain != -1 || initBlock.Shard != topShard || !validBlockID(initBlock) {
		return ton.BlockIDExt{}, fmt.Errorf("global config contains invalid init_block")
	}
	return initBlock, nil
}

func emptyConfigBlock(block liteclient.ConfigBlock) bool {
	return block.Workchain == 0 &&
		block.Shard == 0 &&
		block.SeqNo == 0 &&
		len(block.RootHash) == 0 &&
		len(block.FileHash) == 0
}

func (n *Node) EnsureZeroState(ctx context.Context) error {
	block, ok := n.configuredZeroStateBlock()
	if !ok {
		return storage2.ErrNotFound
	}

	writer, _ := n.peerStorage.(storage2.PeerServingStorageWriter)
	if writer == nil {
		return storage2.ErrNotFound
	}

	n.zeroStateBootstrapMu.Lock()
	defer n.zeroStateBootstrapMu.Unlock()

	return n.ensureZeroState(ctx, block, writer)
}

func (n *Node) ensureZeroState(ctx context.Context, block ton.BlockIDExt, writer storage2.PeerServingStorageWriter) error {
	if data, err := n.peerStorage.ZeroState(ctx, block); err == nil && len(data) > 0 {
		return nil
	} else if err != nil && !errors.Is(err, storage2.ErrNotFound) {
		return err
	}

	n.log.Info().
		Str("block", formatBlockRef(block)).
		Msg("zero state is missing, downloading zero state")

	artifact, err := n.DownloadState(ctx, block, block, 0, nil)
	if err != nil {
		return err
	}
	defer func() {
		if cleanupErr := artifact.Cleanup(); cleanupErr != nil {
			n.log.Debug().
				Err(cleanupErr).
				Str("block", formatBlockRef(block)).
				Msg("failed to cleanup zero state artifact")
		}
	}()

	zero, ok := artifact.(*zeroStateSnapshotArtifact)
	if !ok {
		return fmt.Errorf("unexpected zero state artifact %T", artifact)
	}
	zero.writer = writer

	if _, err = artifact.Decode(ctx); err != nil {
		return fmt.Errorf("verify zero state: %w", err)
	}

	logEvent := n.log.Info().
		Str("block", formatBlockRef(block)).
		Int("bytes", len(zero.data))
	logEvent.Msg("zero state downloaded and stored")
	return nil
}

func (n *Node) startDHT(ctx context.Context, cfg *liteclient.GlobalConfig) error {
	if n.dhtListenAddr != "" {
		return n.startDHTServer(ctx, cfg)
	}
	return n.startDHTClient(cfg)
}

func (n *Node) startDHTClient(cfg *liteclient.GlobalConfig) error {
	dhtGateway := adnl.NewGateway(n.privKey)

	if err := dhtGateway.StartClient(); err != nil {
		return fmt.Errorf("start DHT gateway: %w", err)
	}

	client, err := dht.NewClientFromConfig(dhtGateway, cfg)
	if err != nil {
		_ = dhtGateway.Close()
		return fmt.Errorf("init DHT client: %w", err)
	}

	n.dhtGateway = dhtGateway
	n.dht = client

	return nil
}

type peerPool struct {
	gateway *adnl.Gateway
	mx      sync.RWMutex
	peers   map[string]*pooledPeer
}

type pooledPeer struct {
	id      string
	shortID []byte
	addr    string
	pub     ed25519.PublicKey
	adnl    *overlay.ADNLWrapper
	rldp    *overlay.RLDPWrapper
}

func newPeerPool(gateway *adnl.Gateway) *peerPool {
	return &peerPool{
		gateway: gateway,
		peers:   map[string]*pooledPeer{},
	}
}

func (p *peerPool) Get(addr string, key ed25519.PublicKey) (*pooledPeer, error) {
	peer, err := p.gateway.RegisterClient(addr, key)
	if err != nil {
		return nil, err
	}
	pooled, _, err := p.wrap(peer)
	if err != nil {
		return nil, err
	}
	return pooled, nil
}

func (p *peerPool) wrap(peer adnl.Peer) (*pooledPeer, bool, error) {
	peerID := peer.GetID()
	id := hex.EncodeToString(peerID)

	p.mx.Lock()
	defer p.mx.Unlock()

	if pooled := p.peers[id]; pooled != nil {
		return pooled, false, nil
	}

	wrapper := overlay.CreateExtendedADNL(peer)
	rldpClient := overlay.CreateExtendedRLDP(rldp.NewClientV2(wrapper))

	pooled := &pooledPeer{
		id:      id,
		shortID: append([]byte(nil), peerID...),
		addr:    peer.RemoteAddr(),
		pub:     append(ed25519.PublicKey(nil), peer.GetPubKey()...),
		adnl:    wrapper,
		rldp:    rldpClient,
	}
	rldpClient.SetOnDisconnect(func() {
		p.mx.Lock()
		delete(p.peers, id)
		p.mx.Unlock()
	})
	p.peers[id] = pooled
	return pooled, true, nil
}

func (p *peerPool) snapshot() []*pooledPeer {
	p.mx.RLock()
	defer p.mx.RUnlock()

	list := make([]*pooledPeer, 0, len(p.peers))
	for _, peer := range p.peers {
		list = append(list, peer)
	}
	return list
}

type eventDeduper struct {
	ttl        time.Duration
	maxEntries int
	mx         sync.Mutex
	seen       map[string]*eventDeduperEntry
	order      *list.List
}

type eventDeduperEntry struct {
	key     string
	seenAt  time.Time
	element *list.Element
}

func newEventDeduper(ttl time.Duration, maxEntries int) *eventDeduper {
	return &eventDeduper{
		ttl:        ttl,
		maxEntries: maxEntries,
		seen:       map[string]*eventDeduperEntry{},
		order:      list.New(),
	}
}

func (d *eventDeduper) Mark(key string, now time.Time) bool {
	d.mx.Lock()
	defer d.mx.Unlock()

	if entry := d.seen[key]; entry != nil {
		if now.Sub(entry.seenAt) < d.ttl {
			return false
		}
		entry.seenAt = now
		d.order.MoveToBack(entry.element)
		d.pruneExpiredLocked(now)
		d.pruneOverflowLocked()
		return true
	}

	entry := &eventDeduperEntry{
		key:    key,
		seenAt: now,
	}
	entry.element = d.order.PushBack(entry)
	d.seen[key] = entry

	d.pruneExpiredLocked(now)
	d.pruneOverflowLocked()
	return true
}

func (d *eventDeduper) Seen(key string, now time.Time) bool {
	d.mx.Lock()
	defer d.mx.Unlock()

	entry := d.seen[key]
	if entry == nil {
		return false
	}
	if now.Sub(entry.seenAt) >= d.ttl {
		d.deleteEntryLocked(entry)
		return false
	}
	return true
}

func (d *eventDeduper) pruneExpiredLocked(now time.Time) {
	for elem := d.order.Front(); elem != nil; {
		entry := elem.Value.(*eventDeduperEntry)
		if now.Sub(entry.seenAt) < d.ttl {
			return
		}
		next := elem.Next()
		d.deleteEntryLocked(entry)
		elem = next
	}
}

func (d *eventDeduper) pruneOverflowLocked() {
	if d.maxEntries <= 0 {
		return
	}
	for len(d.seen) > d.maxEntries {
		elem := d.order.Front()
		if elem == nil {
			return
		}
		d.deleteEntryLocked(elem.Value.(*eventDeduperEntry))
	}
}

func (d *eventDeduper) deleteEntryLocked(entry *eventDeduperEntry) {
	if entry == nil {
		return
	}
	delete(d.seen, entry.key)
	if elem := entry.element; elem != nil {
		d.order.Remove(elem)
		entry.element = nil
	}
}

func buildOverlaySpecs(zeroStateFileHash []byte) ([]overlaySpec, error) {
	masterSpec, err := buildOverlaySpec(zeroStateFileHash, -1, topShard, "masterchain")
	if err != nil {
		return nil, fmt.Errorf("build masterchain overlay: %w", err)
	}
	baseSpec, err := buildOverlaySpec(zeroStateFileHash, 0, topShard, "basechain")
	if err != nil {
		return nil, fmt.Errorf("build basechain overlay: %w", err)
	}

	return []overlaySpec{masterSpec, baseSpec}, nil
}

func buildOverlaySpec(zeroStateFileHash []byte, workchain int32, shard int64, name string) (overlaySpec, error) {
	fullID, err := tl.Hash(tonnodeapi.ShardPublicOverlayID{
		Workchain:         workchain,
		Shard:             shard,
		ZeroStateFileHash: zeroStateFileHash,
	})
	if err != nil {
		return overlaySpec{}, err
	}

	shortID, err := tl.Hash(keys.PublicKeyOverlay{Key: fullID})
	if err != nil {
		return overlaySpec{}, err
	}

	protoMajor := int32(shardchainProtoVersionMajor)
	protoMinor := int32(shardchainProtoVersionMinor)
	if workchain == -1 {
		protoMajor = int32(masterchainProtoVersionMajor)
		protoMinor = int32(masterchainProtoVersionMinor)
	}

	return overlaySpec{
		Name:              name,
		Workchain:         workchain,
		Shard:             shard,
		FullID:            fullID,
		ShortID:           shortID,
		ProtoVersionMajor: protoMajor,
		ProtoVersionMinor: protoMinor,
	}, nil
}

func overlaySpecKey(spec overlaySpec) string {
	return hex.EncodeToString(spec.ShortID)
}

func (n *Node) getOrCreateSubscription(spec overlaySpec) (*overlaySubscription, bool) {
	key := overlaySpecKey(spec)

	n.subscriptionsMx.Lock()
	if sub := n.subscriptions[key]; sub != nil {
		n.subscriptionsMx.Unlock()
		return sub, false
	}

	sub := &overlaySubscription{
		node:         n,
		spec:         spec,
		log:          n.log.With().Str("overlay", spec.Name).Logger(),
		peers:        map[string]*overlayPeer{},
		archivePeers: map[string]*archivePeerState{},
		peerNotify:   make(chan struct{}, 1),
	}
	n.subscriptions[key] = sub
	n.subscriptionsMx.Unlock()

	n.attachSubscriptionPeers(sub)
	return sub, true
}

func (n *Node) subscriptionForBlock(block ton.BlockIDExt) (*overlaySubscription, error) {
	if n.runCtx == nil || len(n.zeroStateFileHash) == 0 {
		return nil, errors.New("node is not started")
	}

	overlayBlock := n.overlayBlockForDownload(block)
	spec, err := buildOverlaySpec(n.zeroStateFileHash, overlayBlock.Workchain, overlayBlock.Shard, overlayName(overlayBlock.Workchain, overlayBlock.Shard))
	if err != nil {
		return nil, fmt.Errorf("build overlay for %s: %w", formatBlockRef(block), err)
	}

	sub, _ := n.getOrCreateSubscription(spec)
	sub.setActive(true, time.Time{})
	n.startSubscription(sub)
	return sub, nil
}

func (n *Node) SetMonitorMinSplitDepth(workchain int32, depth uint32) {
	n.monitorSplitMx.Lock()
	defer n.monitorSplitMx.Unlock()

	if n.monitorMinSplitDepth == nil {
		n.monitorMinSplitDepth = map[int32]uint32{}
	}
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

		sub, _ := n.getOrCreateSubscription(spec)
		if sub.setActive(true, time.Time{}) {
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
		if block.Workchain == -1 || block.Shard == 0 {
			continue
		}

		depth := n.monitorMinSplitDepthForWorkchain(block.Workchain)
		shard := block.Shard
		if uint32(tnstate.ShardPrefixLength(shard)) > depth {
			shard = shardPrefix(shard, depth)
		}

		for {
			if err := add(block.Workchain, shard); err != nil {
				return nil, fmt.Errorf("build active shard overlay %d:%016x: %w", block.Workchain, uint64(shard), err)
			}
			if shard == topShard {
				break
			}
			shard = shardParent(shard)
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
	if !sub.inactiveExpired(now) {
		return false
	}

	n.subscriptionsMx.Lock()
	if n.subscriptions[key] != sub {
		n.subscriptionsMx.Unlock()
		return false
	}
	if !sub.inactiveExpired(now) {
		n.subscriptionsMx.Unlock()
		return false
	}
	delete(n.subscriptions, key)
	n.subscriptionsMx.Unlock()

	sub.close()
	return true
}

func (n *Node) overlayBlockForDownload(block ton.BlockIDExt) ton.BlockIDExt {
	if block.Workchain == -1 {
		block.Shard = topShard
		return block
	}

	depth := n.monitorMinSplitDepthForWorkchain(block.Workchain)
	prefixLen := tnstate.ShardPrefixLength(block.Shard)
	if uint32(prefixLen) <= depth {
		return block
	}

	block.Shard = shardPrefix(block.Shard, depth)
	return block
}

func (n *Node) monitorMinSplitDepthForWorkchain(workchain int32) uint32 {
	n.monitorSplitMx.RLock()
	defer n.monitorSplitMx.RUnlock()

	return n.monitorMinSplitDepth[workchain]
}

func shardPrefix(shard int64, depth uint32) int64 {
	if depth == 0 {
		return topShard
	}
	if depth > 63 {
		depth = 63
	}

	prefixBits := uint64(shard) >> (64 - depth)
	return int64((prefixBits << (64 - depth)) | (uint64(1) << (63 - depth)))
}

func shardParent(shard int64) int64 {
	depth := tnstate.ShardPrefixLength(shard)
	if depth <= 0 {
		return topShard
	}
	return shardPrefix(shard, uint32(depth-1))
}

func overlayName(workchain int32, shard int64) string {
	switch {
	case workchain == -1 && shard == topShard:
		return "masterchain"
	case workchain == 0 && shard == topShard:
		return "basechain"
	default:
		return fmt.Sprintf("wc=%d shard=%016x", workchain, uint64(shard))
	}
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

func (n *Node) subscriptionsSnapshot() []*overlaySubscription {
	entries := n.subscriptionEntriesSnapshot()
	list := make([]*overlaySubscription, 0, len(entries))
	for _, entry := range entries {
		list = append(list, entry.sub)
	}
	return list
}
