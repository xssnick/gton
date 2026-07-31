package p2p

import (
	"bytes"
	"cmp"
	"container/list"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"os"
	"slices"
	"sort"
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
	adnlquic "github.com/xssnick/tonutils-go/adnl/quic"
	"github.com/xssnick/tonutils-go/adnl/rldp"
	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

type dhtBackend interface {
	FindOverlayNodes(ctx context.Context, overlayKey []byte, continuation ...*dht.Continuation) (*overlay.NodesList, *dht.Continuation, error)
	FindAddresses(ctx context.Context, key []byte) (*adnladdr.List, ed25519.PublicKey, error)
	FindValue(ctx context.Context, key *dht.Key, continuation ...*dht.Continuation) (*dht.Value, *dht.Continuation, error)
	StoreAddress(ctx context.Context, addresses adnladdr.List, ttl time.Duration, ownerKey ed25519.PrivateKey) (storedCount int, idKey []byte, err error)
	StoreOverlayNodes(ctx context.Context, overlayKey []byte, nodes *overlay.NodesList, ttl time.Duration) (storedCount int, idKey []byte, err error)
}

var _ dhtBackend = (*dht.Client)(nil)
var _ dhtBackend = (*dht.Server)(nil)

var ErrOffline = errors.New("p2p node is offline")

type Node struct {
	log                      zerolog.Logger
	globalConfig             *liteclient.GlobalConfig
	listenAddr               string
	externalIP               net.IP
	privKey                  ed25519.PrivateKey
	quicPublicKey            ed25519.PublicKey
	localID                  PeerID
	dhtPrivKey               ed25519.PrivateKey
	gateway                  *adnl.Gateway
	quicGateway              *adnlquic.Gateway
	quicPacketConn           net.PacketConn
	quicServeDone            chan struct{}
	quicServeErr             error
	quicQuerySlots           chan struct{}
	fastSyncBroadcastFECPace time.Duration
	quicPeersMx              sync.RWMutex
	quicPeers                map[PeerID]*authenticatedQUICPeer
	quicPeersAccepted        atomic.Uint64
	dhtGateway               *adnl.Gateway
	dhtClient                *dht.Client
	dhtServer                *dht.Server
	dht                      dhtBackend
	peerAddresses            peerAddressResolver
	pool                     *peerPool
	peerRoutes               *peerRouteTable
	events                   chan BroadcastEvent
	eventQueue               *boundedQueue[BroadcastEvent]
	deduper                  *eventDeduper
	overlayFanoutDeduper     *eventDeduper
	decodedBroadcasts        decodedBroadcastCache
	decodeQueue              chan offloadedBroadcastDecode
	masterDecodeQueue        chan offloadedBroadcastDecode
	decodeWorkersOnce        sync.Once

	myExternalMessages        *eventDeduper
	processedExternalMessages *eventDeduper
	externalMessageLimiter    *extmsg.AddressLimiter
	externalBroadcastPacer    *externalBroadcastPacer
	allowDuplicateExternals   bool
	localExternalFanout       int

	onceStop sync.Once
	stopped  chan struct{}
	wg       sync.WaitGroup
	// asyncMx orders runAsync's wg.Add against stop's wg.Wait: inbound
	// handlers are only drained after wg.Wait, so without the gate they could
	// Add to a WaitGroup a concurrent Wait already saw at zero.
	asyncMx      sync.Mutex
	asyncStopped bool
	offline      atomic.Bool
	// offlineFailed separates "stopped because a subsystem died" from the
	// deliberate offline of ton.sync_until. Both stop the node, but only the
	// former is an incident: the service must not mistake it for a completed
	// sync, and the process must exit non-zero so a supervisor restarts it.
	offlineFailed atomic.Bool
	onceFailed    sync.Once
	failed        chan struct{}

	networkStarted atomic.Bool

	// appliedMasterchainSeqno is the highest masterchain seqno the service has
	// applied; broadcasts at or below it are dropped before signature work.
	appliedMasterchainSeqno atomic.Uint32
	// appliedShardHeads is the same gate for shards, rebuilt from every
	// committed masterchain state; nil until the first one is published.
	appliedShardHeads atomic.Pointer[appliedShardHeadTable]

	offlineReasonMu sync.RWMutex
	offlineReason   string

	inboundMx       sync.Mutex
	inboundStopping bool
	inboundWG       sync.WaitGroup

	runCtx                          context.Context
	runCancel                       context.CancelFunc
	zeroStateFileHash               []byte
	zeroStateBlock                  ton.BlockIDExt
	initBlock                       ton.BlockIDExt
	hardforkSet                     map[blockIDFullKey]struct{}
	externalPort                    uint16
	dhtListenAddr                   string
	storage                         storage2.Storage
	peerCache                       storage2.OverlayPeerCache
	fastSyncCertificateStorage      storage2.FastSyncCertificateStorage
	peerStorage                     storage2.PeerServingStorage
	liveBlockCache                  *storage2.LiveBlockCache
	compressedState                 CompressedBlockStateProvider
	syncLag                         SyncLagProvider
	signatureVerifier               BroadcastSignatureVerifier
	broadcastAdmission              BroadcastAdmission
	externalMessageAdmission        ExternalMessageAdmission
	blockReceivedObserver           BlockReceivedObserver
	blockReceivedHooks              bool
	broadcastPipelineObserver       BroadcastPipelineObserver
	shardBroadcastCache             *shardBroadcastBlockCache
	shardCandidateCache             *shardBlockCandidateCache
	blockFinalityCache              *blockFinalityCache
	shardBroadcastWaiters           keyedBroadcastWaiters
	masterchainNextBroadcastCache   *masterchainNextBroadcastCache
	masterchainNextBroadcastWaiters keyedBroadcastWaiters
	blockCacheObserver              BlockCacheObserver
	rebroadcastQuiet                atomic.Bool

	rebroadcastThrottleMu   sync.Mutex
	rebroadcastThrottleLast map[string]time.Time
	rebroadcastFECSlots     map[rebroadcastFECLimiterClass]chan struct{}
	rebroadcastFECMu        sync.Mutex
	rebroadcastFECPeers     map[rebroadcastFECLimiterClass]map[PeerID]struct{}

	pendingBroadcastMx         sync.Mutex
	pendingBroadcasts          map[string]pendingBlockBroadcastDecode
	pendingBroadcastByPrev     map[storage2.BlockRootHash]map[string]struct{}
	pendingBroadcastReadyPrev  map[storage2.BlockRootHash]ton.BlockIDExt
	pendingBroadcastBytes      int64
	pendingBroadcastProcessing bool

	broadcastStatsMx sync.Mutex
	broadcastStats   map[broadcastStatKey]uint64

	fecReceiverStatsMx sync.Mutex
	fecReceiverLast    map[string]fecReceiverCounterSnapshot
	fecReceiverTotals  map[string]fecReceiverCounterSnapshot

	localRebroadcastSent    atomic.Uint64
	localRebroadcastDropped atomic.Uint64
	peerRebroadcastSent     atomic.Uint64
	peerRebroadcastDropped  atomic.Uint64

	subscriptionsMx          sync.RWMutex
	subscriptions            map[string]*overlaySubscription
	fastSyncSubscriptions    map[FastSyncShard]*overlaySubscription
	publicBroadcastReceivers atomic.Pointer[publicBroadcastReceiverSnapshot]
	plumtreePolicy           atomic.Pointer[PlumtreePolicy]
	plumtreePolicyLogOnce    sync.Once
	plumtreeBroadcastLogOnce sync.Once
	plumtreeBudget           *plumtreeMemoryBudget

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
	stateDownloadMx           sync.Mutex
	stateDownloadBudget       uint64
	stateDownloadReserved     uint64
	stateDownloadActive       int
	peerUseMx                 sync.RWMutex
	peerUse                   map[PeerID]peerUse
	stateCellImportSlot       chan struct{}
	stateSplitPartDecodeSlot  chan struct{}
	customOverlays            []CustomOverlayConfig

	// Certificates load from storage and are replaced by the ones validators
	// push. The slice is copy-on-write: readers take it under the mutex and
	// then use it without one.
	fastSyncCertificatesMx sync.Mutex
	fastSyncCertificates   []overlay.MemberCertificate

	// Last applied FastSync state, kept so an imported certificate can
	// reconcile overlays without waiting for the next masterchain block.
	fastSyncStateMx sync.Mutex
	fastSyncState   *FastSyncState
}

func New(opts Options) (*Node, error) {
	logger := logutil.WithComponent(opts.Logger, "p2p")
	if rldp.MaxFECDataSize < persistentStateChunkAnswerMax {
		rldp.MaxFECDataSize = persistentStateChunkAnswerMax
	}
	if logger.GetLevel() == zerolog.DebugLevel {
		adnl.Logger = func(args ...any) {
			msg := strings.TrimSpace(fmt.Sprintln(args...))
			if !protocolDiagnosticLoggable(msg) {
				return
			}
			if len(msg) > 2000 {
				msg = msg[:2000] + "...(truncated)"
			}
			logger.Debug().Str("adnl", msg).Msg("adnl diagnostic")
		}
		rldp.Logger = func(args ...any) {
			msg := strings.TrimSpace(fmt.Sprintln(args...))
			if strings.Contains(msg, "received out of order part") ||
				strings.Contains(msg, "unsupported peer query") {
				return
			}
			if protocolDiagnosticLoggable(msg) {
				if len(msg) > 2000 {
					msg = msg[:2000] + "...(truncated)"
				}
				logger.Debug().Str("rldp", msg).Msg("rldp diagnostic")
			}
		}
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
	quicGateway, err := adnlquic.NewGatewayWithLimits(nodeQUICLimits(), priv)
	if err != nil {
		return nil, fmt.Errorf("create QUIC gateway: %w", err)
	}
	localPub := priv.Public().(ed25519.PublicKey)
	localIDRaw, err := tl.Hash(keys.PublicKeyED25519{Key: localPub})
	if err != nil {
		return nil, fmt.Errorf("compute local ADNL id: %w", err)
	}
	localID, err := NewPeerID(localIDRaw)
	if err != nil {
		return nil, fmt.Errorf("parse local ADNL id: %w", err)
	}
	peerStorage := opts.PeerServingStorage
	storage := opts.Storage
	if storage != nil {
		peerStorage = storage
	}
	if peerStorage == nil {
		return nil, fmt.Errorf("peer serving storage is required")
	}
	fastSyncCertificates, err := loadFastSyncCertificates(
		context.Background(),
		opts.FastSyncCertificateStorage,
		localID,
		time.Now(),
	)
	if err != nil {
		return nil, err
	}
	liveBlockCache := opts.LiveBlockCache
	if liveBlockCache == nil {
		liveBlockCache = storage2.NewLiveBlockCache(storage2.DefaultLiveBlockCacheMaxBlocks)
	}

	externalBroadcastPacer, err := newExternalBroadcastPacer(opts.ExternalBroadcastCapacity)
	if err != nil {
		return nil, err
	}
	localExternalFanout, err := normalizeLocalExternalFanout(opts.LocalExternalFanout)
	if err != nil {
		return nil, err
	}
	fastSyncBroadcastFECPace, err := normalizeFastSyncBroadcastFECPace(
		fastSyncBroadcastSpeedMultiplier,
	)
	if err != nil {
		return nil, err
	}

	stateFilesDir, err := prepareStateFilesDir(opts.StateFilesDir)
	if err != nil {
		return nil, fmt.Errorf("prepare state files dir: %w", err)
	}

	node := &Node{
		log:                           logger,
		globalConfig:                  opts.GlobalConfig,
		listenAddr:                    opts.ListenAddr,
		externalIP:                    append(net.IP(nil), opts.ExternalIP...),
		externalPort:                  opts.ExternalPort,
		privKey:                       priv,
		quicPublicKey:                 localPub,
		localID:                       localID,
		dhtPrivKey:                    dhtPriv,
		gateway:                       gateway,
		quicGateway:                   quicGateway,
		quicQuerySlots:                make(chan struct{}, inboundQUICQueryParallelism),
		fastSyncBroadcastFECPace:      fastSyncBroadcastFECPace,
		quicPeers:                     make(map[PeerID]*authenticatedQUICPeer),
		dhtListenAddr:                 opts.DHTListenAddr,
		events:                        make(chan BroadcastEvent, broadcastEventBuffer),
		eventQueue:                    newBoundedQueue(broadcastQueueMaxItems, broadcastQueueMaxBytes, broadcastEventBytes),
		deduper:                       newEventDeduper(10*time.Minute, broadcastDeduperMaxEntries),
		overlayFanoutDeduper:          newEventDeduper(10*time.Minute, broadcastDeduperMaxEntries),
		myExternalMessages:            newEventDeduper(externalMessageCacheTTL, externalMessageCacheMax),
		processedExternalMessages:     newEventDeduper(externalMessageCacheTTL, externalMessageCacheMax),
		externalMessageLimiter:        extmsg.NewDefaultAddressLimiter(),
		externalBroadcastPacer:        externalBroadcastPacer,
		allowDuplicateExternals:       opts.AllowDuplicateExternals,
		localExternalFanout:           localExternalFanout,
		runCtx:                        context.Background(),
		stopped:                       make(chan struct{}),
		failed:                        make(chan struct{}),
		subscriptions:                 map[string]*overlaySubscription{},
		fastSyncSubscriptions:         map[FastSyncShard]*overlaySubscription{},
		plumtreeBudget:                &plumtreeMemoryBudget{},
		monitorMinSplitDepth:          map[int32]uint32{},
		latestBasechainShards:         map[storage2.ShardKey]ton.BlockIDExt{},
		observedMasterchainNotify:     make(chan struct{}),
		seenMasterchainNotify:         make(chan struct{}),
		latestBasechainNotify:         make(chan struct{}),
		rawMasterchainNotify:          make(chan struct{}),
		storage:                       storage,
		peerCache:                     opts.PeerCache,
		fastSyncCertificateStorage:    opts.FastSyncCertificateStorage,
		peerStorage:                   peerStorage,
		liveBlockCache:                liveBlockCache,
		compressedState:               opts.CompressedState,
		syncLag:                       opts.SyncLag,
		signatureVerifier:             opts.SignatureVerifier,
		broadcastAdmission:            opts.BroadcastAdmission,
		externalMessageAdmission:      opts.ExternalMessageAdmission,
		blockReceivedObserver:         opts.BlockReceivedObserver,
		blockReceivedHooks:            blockReceivedHooksEnabled(opts.BlockReceivedObserver),
		shardBroadcastCache:           newShardBroadcastBlockCache(shardBroadcastBlockCacheTTL, shardBroadcastBlockCacheMaxBytes, shardBroadcastBlockCacheMaxItems),
		shardCandidateCache:           newShardBlockCandidateCache(shardBlockCandidateCacheTTL, shardBlockCandidateCacheMaxBytes, shardBlockCandidateCacheMaxItems),
		blockFinalityCache:            newBlockFinalityCache(blockFinalityCacheTTL, blockFinalityCacheMaxBytes, blockFinalityCacheMaxItems),
		masterchainNextBroadcastCache: newMasterchainNextBroadcastCache(masterchainNextBroadcastCacheTTL, masterchainNextBroadcastCacheMaxBytes, masterchainNextBroadcastCacheMaxItems),
		rebroadcastThrottleLast:       map[string]time.Time{},
		rebroadcastFECSlots:           newRebroadcastFECSlotLimits(),
		rebroadcastFECPeers:           newRebroadcastFECPeerLimits(),
		pendingBroadcasts:             map[string]pendingBlockBroadcastDecode{},
		pendingBroadcastByPrev:        map[storage2.BlockRootHash]map[string]struct{}{},
		broadcastStats:                map[broadcastStatKey]uint64{},
		fecReceiverLast:               map[string]fecReceiverCounterSnapshot{},
		fecReceiverTotals:             map[string]fecReceiverCounterSnapshot{},
		stateFilesDir:                 stateFilesDir,
		peerUse:                       map[PeerID]peerUse{},
		stateCellImportSlot:           make(chan struct{}, 1),
		stateSplitPartDecodeSlot:      make(chan struct{}, 1),
		customOverlays:                append([]CustomOverlayConfig(nil), opts.CustomOverlays...),
		fastSyncCertificates:          fastSyncCertificates,
	}
	node.SetPlumtreePolicy(PlumtreePolicy{})
	node.peerRoutes = newPeerRouteTable()
	node.pool = newPeerPool(
		gateway,
		node.resolvePublicBroadcastReceiver,
		node.handlePeerCustomMessage,
		node.peerRoutes,
	)
	node.pool.detachedQuery = &detachedQueryHandlers{
		adnl: node.serveDetachedADNLQuery,
		rldp: node.serveDetachedRLDPQuery,
	}
	return node, nil
}

func (n *Node) SetRuntimeCallbacks(callbacks RuntimeCallbacks) {
	n.compressedState = callbacks
	n.syncLag = callbacks
	n.signatureVerifier = callbacks
	n.broadcastAdmission = callbacks
	n.externalMessageAdmission = callbacks
	n.blockReceivedObserver = callbacks
	n.blockReceivedHooks = blockReceivedHooksEnabled(callbacks)
}

func (n *Node) SetPlumtreePolicy(policy PlumtreePolicy) {
	n.plumtreePolicy.Store(&policy)

	if len(policy.authorizedKeys) > 0 {
		n.plumtreePolicyLogOnce.Do(func() {
			n.log.Info().
				Uint8("masterchain_protocol_version", policy.masterchainProtocolVersion).
				Uint8("shard_protocol_version", policy.shardProtocolVersion).
				Int("authorized_keys", len(policy.authorizedKeys)).
				Msg("loaded Plumtree broadcast policy")
		})
	}

	for _, entry := range n.subscriptionEntriesSnapshot() {
		if entry.sub.plumtree != nil {
			entry.sub.plumtree.notifyAlarmChanged()
		}
	}
}

func protocolDiagnosticLoggable(msg string) bool {
	return strings.Contains(msg, "error") ||
		strings.Contains(msg, "failed") ||
		strings.Contains(msg, "invalid") ||
		strings.Contains(msg, "too big")
}

func (n *Node) LocalID() PeerID {
	return n.localID
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
	cfg := n.globalConfig
	if cfg == nil {
		return fmt.Errorf("global config is required")
	}

	zeroBlock := blockIDFromConfig(cfg.Validator.ZeroState)
	if zeroBlock.Workchain != -1 || zeroBlock.Shard != topShard || zeroBlock.SeqNo != 0 || !storage2.BlockIDHashesKnown(zeroBlock) {
		return fmt.Errorf("global config contains invalid zero_state")
	}

	hardforks, hardforkSet, err := hardforksFromConfig(cfg.Validator.Hardforks)
	if err != nil {
		return err
	}

	initBlock, err := initBlockFromConfig(cfg.Validator.InitBlock, zeroBlock, hardforks)
	if err != nil {
		return err
	}

	runCtx, runCancel := context.WithCancel(ctx)
	n.runCtx = runCtx
	n.runCancel = runCancel
	if err = runCtx.Err(); err != nil {
		runCancel()
		return err
	}
	n.zeroStateFileHash = append([]byte(nil), zeroBlock.FileHash...)
	n.zeroStateBlock = zeroBlock
	n.initBlock = initBlock
	n.hardforkSet = hardforkSet
	if len(hardforks) > 0 {
		n.log.Info().
			Int("hardforks", len(hardforks)).
			Str("init_block", storage2.FormatBlockRef(initBlock)).
			Msg("loaded hardforks from global config")
	}
	specs, err := buildOverlaySpecs(n.zeroStateFileHash)
	if err != nil {
		return err
	}
	customSpecs, err := buildCustomOverlaySpecs(n.zeroStateFileHash, n.customOverlays, n.localID)
	if err != nil {
		return err
	}
	if len(n.customOverlays) > 0 {
		n.log.Info().
			Int("configured", len(n.customOverlays)).
			Int("local", len(customSpecs)).
			Msg("loaded custom overlay config")
	}
	specs = append(specs, customSpecs...)
	fail := func(err error) error {
		runCancel()
		n.closeSubscriptions()
		return err
	}
	failNetwork := func(err error) error {
		runCancel()
		n.stopAcceptingInbound()
		if n.dhtClient != nil {
			n.dhtClient.Close()
		}
		if n.dhtServer != nil {
			_ = n.dhtServer.Close()
		}
		if n.dhtGateway != nil {
			_ = n.dhtGateway.Close()
		}
		_ = n.gateway.Close()
		_ = n.closeQUICGateway()
		n.networkStarted.Store(false)
		n.closeSubscriptions()
		n.wg.Wait()
		n.inboundWG.Wait()
		return err
	}
	subscriptions := make([]*overlaySubscription, 0, len(specs))
	for _, spec := range specs {
		sub, err := n.getOrCreateSubscription(spec)
		if err != nil {
			return fail(err)
		}
		subscriptions = append(subscriptions, sub)
	}

	n.gateway.SetConnectionHandler(n.handleInboundPeer)
	n.quicGateway.SetConnectionHandler(n.handleInboundQUICPeer)
	if err = n.startGateway(); err != nil {
		return failNetwork(fmt.Errorf("start network gateways: %w", err))
	}

	if err = n.startDHT(runCtx, cfg); err != nil {
		return failNetwork(err)
	}
	if err = runCtx.Err(); err != nil {
		return failNetwork(err)
	}
	if err = n.checkQUICServer(); err != nil {
		return failNetwork(err)
	}
	n.networkStarted.Store(true)

	for _, sub := range subscriptions {
		n.startSubscription(sub)
	}
	n.runAsync(func() {
		n.runEventLoop(runCtx)
	})
	n.runAsync(func() {
		n.runShardBroadcastCacheJanitor(runCtx)
	})
	n.runAsync(func() {
		n.runSubscriptionLifecycleLoop(runCtx)
	})
	n.runAsync(func() {
		n.runQUICIdleSweepLoop(runCtx)
	})
	n.startQUICMonitor(runCtx)
	if n.isPublicServer() {
		n.runAsync(func() {
			n.runAnnounceLoop(runCtx)
		})
	}

	go func() {
		<-runCtx.Done()
		n.stop()
	}()

	return nil
}

func (n *Node) runAsync(fn func()) bool {
	n.asyncMx.Lock()
	if n.asyncStopped {
		n.asyncMx.Unlock()
		return false
	}
	n.wg.Add(1)
	n.asyncMx.Unlock()
	go func() {
		defer n.wg.Done()
		fn()
	}()
	return true
}

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

	sub.startTwoStepRebroadcastWorker(ctx)
	n.runAsync(func() {
		defer sub.clearRunCancel(token)
		sub.run(ctx)
	})
}

func (n *Node) Wait() {
	<-n.stopped
}

func (n *Node) EnterOffline(reason string) {
	n.enterOffline(reason, false)
}

// enterOfflineFailure is EnterOffline for an unexpected subsystem death. The
// failure flag is raised before the offline transition, and the failure signal
// is published only after its reason is ready for observers.
func (n *Node) enterOfflineFailure(reason string) {
	n.enterOffline(reason, true)
}

func (n *Node) enterOffline(reason string, failed bool) {
	if strings.TrimSpace(reason) == "" {
		reason = "offline mode requested"
	}
	if failed {
		n.offlineFailed.Store(true)
	}
	if !n.offline.CompareAndSwap(false, true) {
		return
	}

	n.offlineReasonMu.Lock()
	n.offlineReason = reason
	n.offlineReasonMu.Unlock()

	if failed {
		n.onceFailed.Do(func() {
			close(n.failed)
		})
	}

	n.log.Info().
		Str("reason", reason).
		Msg("entering p2p offline mode")
	n.stop()
}

func (n *Node) IsOffline() bool {
	return n.offline.Load()
}

// OfflineFailed reports whether the node went offline because something broke,
// as opposed to the deliberate stop at ton.sync_until.
func (n *Node) OfflineFailed() bool {
	return n.offlineFailed.Load()
}

// Failed is closed when the node stops because a subsystem died. The deliberate
// ton.sync_until stop never closes it: there the process is meant to keep
// serving the frozen state.
func (n *Node) Failed() <-chan struct{} {
	return n.failed
}

func (n *Node) OfflineReason() string {
	n.offlineReasonMu.RLock()
	defer n.offlineReasonMu.RUnlock()
	return n.offlineReason
}

func (n *Node) stop() {
	n.onceStop.Do(func() {
		defer close(n.stopped)

		networkStarted := n.networkStarted.Load()
		if n.runCancel != nil {
			n.runCancel()
		}
		n.stopAcceptingInbound()

		if networkStarted {
			if n.dhtClient != nil {
				n.dhtClient.Close()
			}
			if n.dhtServer != nil {
				_ = n.dhtServer.Close()
			}
			if n.dhtGateway != nil {
				_ = n.dhtGateway.Close()
			}
			_ = n.gateway.Close()
			_ = n.closeQUICGateway()
		}

		n.eventQueue.Close()
		n.closeSubscriptions()

		n.asyncMx.Lock()
		n.asyncStopped = true
		n.asyncMx.Unlock()
		n.wg.Wait()
		n.inboundWG.Wait()

		if n.stateFilesDir != "" {
			_ = removeIncompleteStateFiles(n.stateFilesDir)
		}
	})
}

func (n *Node) closeSubscriptions() {
	for _, sub := range n.subscriptionsSnapshot() {
		sub.close()
	}
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

func (n *Node) ZeroStateBlock() (ton.BlockIDExt, error) {
	if n.zeroStateBlock.Workchain != -1 || n.zeroStateBlock.Shard != topShard || n.zeroStateBlock.SeqNo != 0 || !storage2.BlockIDHashesKnown(n.zeroStateBlock) {
		return ton.BlockIDExt{}, storage2.ErrNotFound
	}
	return n.zeroStateBlock, nil
}

func (n *Node) InitBlock() (ton.BlockIDExt, error) {
	if n.initBlock.Workchain != -1 || n.initBlock.Shard != topShard || !storage2.BlockIDHashesKnown(n.initBlock) {
		return ton.BlockIDExt{}, storage2.ErrNotFound
	}
	return n.initBlock, nil
}

func (n *Node) IsHardfork(block ton.BlockIDExt) bool {
	key, ok := blockIDFullKeyFromBlock(block)
	if !ok {
		return false
	}
	_, ok = n.hardforkSet[key]
	return ok
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

type blockIDFullKey struct {
	workchain int32
	shard     int64
	seqno     uint32
	rootHash  [32]byte
	fileHash  [32]byte
}

func blockIDFullKeyFromBlock(block ton.BlockIDExt) (blockIDFullKey, bool) {
	if !storage2.BlockIDHashesKnown(block) {
		return blockIDFullKey{}, false
	}

	var key blockIDFullKey
	key.workchain = block.Workchain
	key.shard = block.Shard
	key.seqno = block.SeqNo
	copy(key.rootHash[:], block.RootHash)
	copy(key.fileHash[:], block.FileHash)
	return key, true
}

func hardforksFromConfig(blocks []liteclient.ConfigBlock) ([]ton.BlockIDExt, map[blockIDFullKey]struct{}, error) {
	if len(blocks) == 0 {
		return nil, nil, nil
	}

	hardforks := make([]ton.BlockIDExt, 0, len(blocks))
	active := make([]bool, 0, len(blocks))
	for _, block := range blocks {
		hardfork := blockIDFromConfig(block)
		if hardfork.Workchain != -1 || hardfork.Shard != topShard {
			return nil, nil, fmt.Errorf("global config contains non-masterchain hardfork")
		}
		if !storage2.BlockIDHashesKnown(hardfork) {
			return nil, nil, fmt.Errorf("global config contains invalid hardfork")
		}

		for i := range hardforks {
			if active[i] && hardforks[i].SeqNo >= hardfork.SeqNo {
				active[i] = false
			}
		}
		hardforks = append(hardforks, hardfork)
		active = append(active, true)
	}

	filtered := hardforks[:0]
	for i := range hardforks {
		if active[i] {
			filtered = append(filtered, hardforks[i])
		}
	}
	hardforks = filtered
	if len(hardforks) == 0 {
		return nil, nil, nil
	}

	set := make(map[blockIDFullKey]struct{}, len(hardforks))
	for _, hardfork := range hardforks {
		key, _ := blockIDFullKeyFromBlock(hardfork)
		set[key] = struct{}{}
	}
	return hardforks, set, nil
}

func initBlockFromConfig(block liteclient.ConfigBlock, zeroBlock ton.BlockIDExt, hardforks []ton.BlockIDExt) (ton.BlockIDExt, error) {
	initBlock := zeroBlock
	if emptyConfigBlock(block) {
		return latestInitBlockForHardforks(initBlock, hardforks), nil
	}

	initBlock = blockIDFromConfig(block)
	if initBlock.Workchain != -1 || initBlock.Shard != topShard || !storage2.BlockIDHashesKnown(initBlock) {
		return ton.BlockIDExt{}, fmt.Errorf("global config contains invalid init_block")
	}
	return latestInitBlockForHardforks(initBlock, hardforks), nil
}

func latestInitBlockForHardforks(initBlock ton.BlockIDExt, hardforks []ton.BlockIDExt) ton.BlockIDExt {
	for _, hardfork := range hardforks {
		if hardfork.SeqNo > initBlock.SeqNo {
			initBlock = hardfork
		}
	}
	return initBlock
}

func emptyConfigBlock(block liteclient.ConfigBlock) bool {
	return block.Workchain == 0 &&
		block.Shard == 0 &&
		block.SeqNo == 0 &&
		len(block.RootHash) == 0 &&
		len(block.FileHash) == 0
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
	n.dhtClient = client
	n.dht = client

	return nil
}

type peerPool struct {
	gateway                   *adnl.Gateway
	broadcastReceiverResolver overlay.BroadcastReceiverResolver
	// customMessage receives ADNL messages that carry no overlay envelope.
	customMessage func(msg *adnl.MessageCustom) error
	detachedQuery *detachedQueryHandlers
	routes        *peerRouteTable
	mx            sync.RWMutex
	peers         map[PeerID]*pooledPeer
}

// detachedQueryHandlers serve overlay queries arriving on a transport that has
// no attachment for the target overlay.
type detachedQueryHandlers struct {
	adnl func(pooled *pooledPeer, msg *adnl.MessageQuery) error
	rldp func(pooled *pooledPeer, transferID []byte, query *rldp.Query) error
}

type peerEndpoint struct {
	adnlAddr string
	quicAddr string
}

type peerRoute struct {
	quic                atomic.Pointer[string]
	quicRefreshRequired atomic.Bool
	quicRetryState      atomic.Int64
	quicRetryMx         sync.Mutex
	quicDialFailures    uint32
	// quicDialSpawned bounds background dials to one goroutine per route: the
	// retry gate alone cannot, because its claim happens inside the dial's
	// address resolver, behind the gateway's per-peer dial mutex — a slow
	// ungated dial there would let every relayed part spawn another waiter.
	quicDialSpawned atomic.Bool
	// lastUsedNanos is when a transport last asked for this route. Only the
	// sweep reads it, and only for routes no transport holds any more.
	lastUsedNanos atomic.Int64
}

// peerRouteTable keeps one peerRoute per peer id for the whole node. The route
// carries the learned QUIC endpoint and the single-dial gate, so it must
// outlive any individual transport: a pooled peer pruned and re-wrapped, or the
// same peer reached over both the pool and an inbound QUIC path, previously got
// separate routes — which lost the learned address on every prune and left the
// dial gate claiming nothing.
//
// Lock order: pool.mx -> peerRoutes.mx. The table is a leaf; it calls nothing.
type peerRouteTable struct {
	mx     sync.RWMutex
	routes map[PeerID]*peerRoute
}

func newPeerRouteTable() *peerRouteTable {
	return &peerRouteTable{routes: map[PeerID]*peerRoute{}}
}

// get returns the route of a peer, creating it on first use. The pointer stays
// valid for as long as any transport holds it, so every holder observes the same
// gate; sweepPeerRoutes only ever drops routes nobody holds.
func (t *peerRouteTable) get(id PeerID) *peerRoute {
	nowNanos := time.Now().UnixNano()

	t.mx.RLock()
	route := t.routes[id]
	t.mx.RUnlock()
	if route != nil {
		route.lastUsedNanos.Store(nowNanos)
		return route
	}

	t.mx.Lock()
	defer t.mx.Unlock()
	if route = t.routes[id]; route != nil {
		route.lastUsedNanos.Store(nowNanos)
		return route
	}
	route = newPeerRoute("")
	route.lastUsedNanos.Store(nowNanos)
	t.routes[id] = route
	return route
}

// sweep bounds the table to maxRoutes.
//
// Routes any live transport still holds are never dropped: the route carries the
// learned QUIC endpoint and the single-dial gate, and taking it away from a peer
// we are talking to right now would lose the address and leave the gate claiming
// nothing. Among the rest, routes that never learned an address go first - those
// are what an inbound connection leaves behind, and they hold nothing worth
// keeping - and only then the oldest of the useful ones.
func (t *peerRouteTable) sweep(
	held map[PeerID]struct{},
	maxRoutes int,
	now time.Time,
) int {
	t.mx.Lock()
	defer t.mx.Unlock()

	over := len(t.routes) - maxRoutes
	if over <= 0 {
		return 0
	}

	type victim struct {
		id       PeerID
		useful   bool
		lastUsed int64
	}
	candidates := make([]victim, 0, len(t.routes))
	for id, route := range t.routes {
		if _, live := held[id]; live {
			continue
		}
		candidates = append(candidates, victim{
			id:       id,
			useful:   route.quicAddr() != "",
			lastUsed: route.lastUsedNanos.Load(),
		})
	}

	slices.SortFunc(candidates, func(a, b victim) int {
		if a.useful != b.useful {
			if a.useful {
				return 1
			}
			return -1
		}
		return cmp.Compare(a.lastUsed, b.lastUsed)
	})

	if over > len(candidates) {
		over = len(candidates)
	}
	for _, candidate := range candidates[:over] {
		delete(t.routes, candidate.id)
	}
	return over
}

func (t *peerRouteTable) size() int {
	t.mx.RLock()
	defer t.mx.RUnlock()

	return len(t.routes)
}

func newPeerRoute(quicAddr string) *peerRoute {
	route := &peerRoute{}
	route.setQUICAddr(quicAddr)
	return route
}

func (r *peerRoute) setQUICAddr(addr string) {
	if addr == "" {
		return
	}

	if r.quic.CompareAndSwap(nil, &addr) {
		r.quicRefreshRequired.Store(false)
	}
}

func (r *peerRoute) refreshQUICAddr(addr string) {
	r.quic.Store(&addr)
	r.quicRefreshRequired.Store(false)
}

func (r *peerRoute) quicAddr() string {
	addr := r.quic.Load()
	if addr == nil {
		return ""
	}
	return *addr
}

func (r *peerRoute) markQUICAddrStale() {
	r.quicRefreshRequired.Store(true)
}

func (r *peerRoute) quicAddrStale() bool {
	return r.quicRefreshRequired.Load()
}

// QUIC retries start at the eager-peer inactivity window, then back off while
// the route keeps failing. A successful connection resets the delay.
const (
	quicDialRetryMinDelay = plumtreeEagerInactivityTTL
	quicDialRetryMaxDelay = 30 * time.Second
)

// quicReady reports whether the retry gate permits a dial attempt. Callers
// must check it explicitly before dialing: the gate cannot live only in the
// dial's address resolver because the gateway's live-outbound fast path never
// invokes the resolver.
func (r *peerRoute) quicReady(now time.Time) bool {
	retryState := r.quicRetryState.Load()
	return retryState <= 0 || now.UnixNano() >= retryState
}

// quicDialPermitted mirrors beginQUICDial's claim test without claiming: it
// filters out routes with a dial already in flight or inside the retry window
// before any dial goroutine is spawned.
func (r *peerRoute) quicDialPermitted(now time.Time) bool {
	retryState := r.quicRetryState.Load()
	return retryState >= 0 && retryState <= now.UnixNano()
}

func (r *peerRoute) beginQUICDial(now time.Time) bool {
	nowUnix := now.UnixNano()
	for {
		retryState := r.quicRetryState.Load()
		if retryState < 0 || retryState > nowUnix {
			return false
		}
		if r.quicRetryState.CompareAndSwap(retryState, -1) {
			return true
		}
	}
}

func (r *peerRoute) finishQUICDial() {
	r.quicRetryMx.Lock()
	if r.quicRetryState.CompareAndSwap(-1, 0) {
		r.quicDialFailures = 0
	}
	r.quicRetryMx.Unlock()
}

func (r *peerRoute) succeedQUICDial(claimed bool) {
	r.quicRetryMx.Lock()
	defer r.quicRetryMx.Unlock()

	if claimed {
		if !r.quicRetryState.CompareAndSwap(-1, 0) {
			return
		}
	} else {
		for {
			retryState := r.quicRetryState.Load()
			if retryState < 0 {
				return
			}
			if r.quicRetryState.CompareAndSwap(retryState, 0) {
				break
			}
		}
	}
	r.quicDialFailures = 0
	r.quicRefreshRequired.Store(false)
}

func (r *peerRoute) failQUICDial(now time.Time) {
	r.quicRetryMx.Lock()
	defer r.quicRetryMx.Unlock()

	failures := r.quicDialFailures + 1
	retryAt := now.Add(quicDialRetryDelay(failures)).UnixNano()
	for current := r.quicRetryState.Load(); current < retryAt; {
		if r.quicRetryState.CompareAndSwap(current, retryAt) {
			r.quicDialFailures = failures
			return
		}
		current = r.quicRetryState.Load()
	}
}

func (r *peerRoute) deferQUICDial(now time.Time) {
	r.quicRetryMx.Lock()
	defer r.quicRetryMx.Unlock()

	nowUnix := now.UnixNano()
	for {
		current := r.quicRetryState.Load()
		if current < 0 || current > nowUnix {
			return
		}
		failures := r.quicDialFailures + 1
		retryAt := now.Add(quicDialRetryDelay(failures)).UnixNano()
		if r.quicRetryState.CompareAndSwap(current, retryAt) {
			r.quicDialFailures = failures
			return
		}
	}
}

func quicDialRetryDelay(failures uint32) time.Duration {
	delay := quicDialRetryMinDelay
	for failure := uint32(1); failure < failures && delay < quicDialRetryMaxDelay; failure++ {
		delay *= 2
	}
	if delay > quicDialRetryMaxDelay {
		return quicDialRetryMaxDelay
	}
	return delay
}

type pooledPeer struct {
	id              PeerID
	addr            string
	route           *peerRoute
	pub             ed25519.PublicKey
	adnl            *overlay.ADNLWrapper
	baseRLDP        *rldp.RLDP
	rldp            *overlay.RLDPWrapper
	refs            int
	fixedMemberRefs int
	adnlOverlayRefs map[*overlay.ADNLOverlayWrapper]int
	rldpOverlayRefs map[*overlay.RLDPOverlayWrapper]int
	lastUsedAt      atomic.Int64
}

type idlePooledPeer struct {
	peer     *pooledPeer
	lastUsed time.Time
}

var errPeerEndpointBusy = errors.New("peer endpoint is in active use")
var errPooledPeerUnavailable = errors.New("pooled peer is no longer available")

// acquirePeerEndpoint treats the first ADNL connection for a PeerID as canonical.
func (n *Node) acquirePeerEndpoint(peerID PeerID, endpoint peerEndpoint, key ed25519.PublicKey) (*pooledPeer, func(), error) {
	if err := validatePeerEndpointIdentity(peerID, key); err != nil {
		return nil, nil, err
	}

	n.peerUseMx.Lock()
	use := n.peerUse[peerID]
	if use.endpointPending != nil {
		n.peerUseMx.Unlock()
		return nil, nil, errPeerEndpointBusy
	}

	n.pool.mx.Lock()
	pooled := n.pool.peers[peerID]
	if pooled != nil && pooledPeerClosed(pooled) {
		// The transport is already closed but the asynchronous disconnect
		// cascade has not pruned the pool entry yet; reusing it would attach
		// a dead connection. Prune like removeDisconnected and reconnect.
		delete(n.pool.peers, peerID)
		pooled = nil
	}
	if pooled != nil {
		pooled.route.setQUICAddr(endpoint.quicAddr)
		pooled.refs++
		n.pool.mx.Unlock()

		use.queries++
		n.peerUse[peerID] = use
		n.peerUseMx.Unlock()
		return pooled, n.releasePeerEndpoint(peerID, pooled, nil), nil
	}
	n.pool.mx.Unlock()

	if use.downloads > 0 || use.queries > 0 {
		n.peerUseMx.Unlock()
		return nil, nil, errPeerEndpointBusy
	}

	pending := make(chan struct{})
	use.endpointPending = pending
	n.peerUse[peerID] = use
	n.peerUseMx.Unlock()

	connected, err := n.pool.Get(endpoint, key)
	if err != nil {
		n.cancelPeerEndpointPending(peerID, pending)
		return nil, nil, err
	}
	pooled = connected
	if pooled.id != peerID {
		n.pool.closeIfUnused(pooled)
		n.cancelPeerEndpointPending(peerID, pending)
		return nil, nil, fmt.Errorf("peer endpoint identity mismatch: got %s want %s", pooled.id.String(), peerID.String())
	}
	if !n.pool.retain(pooled) {
		n.cancelPeerEndpointPending(peerID, pending)
		return nil, nil, errors.New("peer endpoint disconnected during acquisition")
	}

	n.peerUseMx.Lock()
	use = n.peerUse[peerID]
	use.queries++
	n.peerUse[peerID] = use
	n.peerUseMx.Unlock()
	return pooled, n.releasePeerEndpoint(peerID, pooled, pending), nil
}

// acquireArchivePeerEndpoint retains the canonical transport without entering
// ordinary live query/download accounting. The returned overlay handle has its
// own lifetime and can therefore be rotated by the archive pool independently
// from the live overlay roster.
func (n *Node) acquireArchivePeerEndpoint(peerID PeerID, addr string, key ed25519.PublicKey) (*pooledPeer, func(), error) {
	if err := validatePeerEndpointIdentity(peerID, key); err != nil {
		return nil, nil, err
	}
	if pooled := n.pool.retainByID(peerID); pooled != nil {
		return pooled, releaseRetainedPeer(n.pool, pooled), nil
	}

	pooled, err := n.pool.getADNL(addr, key)
	if err != nil {
		return nil, nil, err
	}
	if pooled.id != peerID {
		n.pool.closeIfUnused(pooled)
		return nil, nil, fmt.Errorf("peer endpoint identity mismatch: got %s want %s", pooled.id.String(), peerID.String())
	}
	if !n.pool.retain(pooled) {
		return nil, nil, errors.New("peer endpoint disconnected during acquisition")
	}
	return pooled, releaseRetainedPeer(n.pool, pooled), nil
}

func validatePeerEndpointIdentity(peerID PeerID, key ed25519.PublicKey) error {
	if peerID.IsZero() {
		return errors.New("peer endpoint identity is empty")
	}
	if len(key) != ed25519.PublicKeySize {
		return fmt.Errorf("invalid peer endpoint public key size %d", len(key))
	}
	rawID, err := tl.Hash(keys.PublicKeyED25519{Key: key})
	if err != nil {
		return fmt.Errorf("hash peer endpoint public key: %w", err)
	}
	keyID, err := NewPeerID(rawID)
	if err != nil {
		return fmt.Errorf("parse peer endpoint identity: %w", err)
	}
	if keyID != peerID {
		return fmt.Errorf("peer endpoint public key does not match identity %s", peerID.String())
	}
	return nil
}

func releaseRetainedPeer(pool *peerPool, peer *pooledPeer) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			pool.releaseRetained(peer)
		})
	}
}

func (n *Node) releasePeerEndpoint(peerID PeerID, pooled *pooledPeer, pending chan struct{}) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			n.peerUseMx.Lock()
			use := n.peerUse[peerID]
			if use.queries > 0 {
				use.queries--
			}
			if pending != nil && use.endpointPending == pending {
				use.endpointPending = nil
				close(pending)
			}
			if use.downloads == 0 && use.queries == 0 && use.endpointPending == nil {
				delete(n.peerUse, peerID)
			} else {
				n.peerUse[peerID] = use
			}
			n.peerUseMx.Unlock()

			n.pool.releaseRetained(pooled)
		})
	}
}

func (n *Node) cancelPeerEndpointPending(peerID PeerID, pending chan struct{}) {
	n.peerUseMx.Lock()
	defer n.peerUseMx.Unlock()

	use := n.peerUse[peerID]
	if use.endpointPending != pending {
		return
	}
	use.endpointPending = nil
	close(pending)
	if use.downloads == 0 && use.queries == 0 {
		delete(n.peerUse, peerID)
		return
	}
	n.peerUse[peerID] = use
}

func newPeerPool(
	gateway *adnl.Gateway,
	resolver overlay.BroadcastReceiverResolver,
	customMessage func(msg *adnl.MessageCustom) error,
	routes *peerRouteTable,
) *peerPool {
	if routes == nil {
		routes = newPeerRouteTable()
	}
	return &peerPool{
		gateway:                   gateway,
		broadcastReceiverResolver: resolver,
		customMessage:             customMessage,
		routes:                    routes,
		peers:                     map[PeerID]*pooledPeer{},
	}
}

func (p *peerPool) Get(endpoint peerEndpoint, key ed25519.PublicKey) (*pooledPeer, error) {
	peer, err := p.gateway.RegisterClient(endpoint.adnlAddr, key)
	if err != nil {
		return nil, err
	}
	pooled, _, err := p.wrapEndpoint(peer, endpoint)
	if err != nil {
		peer.Close()
		return nil, err
	}
	return pooled, nil
}

func (p *peerPool) getADNL(addr string, key ed25519.PublicKey) (*pooledPeer, error) {
	peer, err := p.gateway.RegisterClient(addr, key)
	if err != nil {
		return nil, err
	}
	pooled, _, err := p.wrap(peer)
	if err != nil {
		peer.Close()
		return nil, err
	}
	return pooled, nil
}

func (p *peerPool) wrap(peer adnl.Peer) (*pooledPeer, bool, error) {
	id, err := NewPeerID(peer.GetID())
	if err != nil {
		return nil, false, err
	}

	p.mx.Lock()
	pooled, fresh, stale := p.wrapPeerLocked(peer, id)
	p.mx.Unlock()

	for _, idle := range stale {
		idle.close()
	}
	return pooled, fresh, nil
}

func (p *peerPool) wrapEndpoint(peer adnl.Peer, endpoint peerEndpoint) (*pooledPeer, bool, error) {
	id, err := NewPeerID(peer.GetID())
	if err != nil {
		return nil, false, err
	}

	p.mx.Lock()
	pooled, fresh, stale := p.wrapPeerLocked(peer, id)
	// Inbound ADNL peers have no discovered QUIC route. The first outbound
	// address-list route learned for that live generation becomes canonical.
	pooled.route.setQUICAddr(endpoint.quicAddr)
	p.mx.Unlock()

	for _, idle := range stale {
		idle.close()
	}
	return pooled, fresh, nil
}

func (p *peerPool) wrapPeerLocked(peer adnl.Peer, id PeerID) (*pooledPeer, bool, []*pooledPeer) {
	if pooled := p.peers[id]; pooled != nil {
		if !pooledPeerClosed(pooled) {
			pooled.touch(time.Now())
			return pooled, false, nil
		}
		delete(p.peers, id)
	}

	wrapper := overlay.CreateExtendedADNL(peer)
	wrapper.SetBroadcastReceiverResolver(p.broadcastReceiverResolver)
	if p.customMessage != nil {
		// Non-overlay ADNL messages fall through to this handler; without it
		// they are dropped before anything sees them.
		wrapper.SetCustomMessageHandler(p.customMessage)
	}
	baseRLDP := rldp.NewClientV2(wrapper)
	rldpClient := overlay.CreateExtendedRLDP(baseRLDP)

	pooled := &pooledPeer{
		id:              id,
		addr:            peer.RemoteAddr(),
		route:           p.routes.get(id),
		pub:             append(ed25519.PublicKey(nil), peer.GetPubKey()...),
		adnl:            wrapper,
		baseRLDP:        baseRLDP,
		rldp:            rldpClient,
		adnlOverlayRefs: map[*overlay.ADNLOverlayWrapper]int{},
		rldpOverlayRefs: map[*overlay.RLDPOverlayWrapper]int{},
	}
	pooled.touch(time.Now())
	// Serve overlay queries from peers we hold no attachment for. Without this
	// the wrappers drop them as "unregistered overlay", so an unattached peer
	// gets neither blocks nor Pong and evicts us as unreliable.
	if p.detachedQuery != nil {
		wrapper.SetOnUnknownOverlayQuery(func(msg *adnl.MessageQuery) error {
			return p.detachedQuery.adnl(pooled, msg)
		})
		rldpClient.SetOnUnknownOverlayQuery(func(transferID []byte, query *rldp.Query) error {
			return p.detachedQuery.rldp(pooled, transferID, query)
		})
	}
	rldpClient.SetOnDisconnect(func() {
		p.removeDisconnected(id, pooled)
	})
	p.peers[id] = pooled
	stale := p.pruneOverCapLocked(time.Now())
	return pooled, true, stale
}

func (p *pooledPeer) touch(now time.Time) {
	p.lastUsedAt.Store(now.UnixNano())
}

func (p *pooledPeer) lastUsed() time.Time {
	lastUsed := time.Unix(0, p.lastUsedAt.Load())
	stats := p.adnl.Stats()
	return latestTime(lastUsed, stats.Inbound.LastPacketAt, stats.Outbound.LastPacketAt)
}

func (p *peerPool) touchByID(id PeerID, now time.Time) {
	p.mx.RLock()
	if pooled := p.peers[id]; pooled != nil {
		pooled.touch(now)
	}
	p.mx.RUnlock()
}

func (p *peerPool) removeDisconnected(id PeerID, disconnected *pooledPeer) {
	p.mx.Lock()
	if p.peers[id] == disconnected {
		delete(p.peers, id)
	}
	p.mx.Unlock()
}

func (p *peerPool) acquireOverlay(pooled *pooledPeer, receiver *overlay.BroadcastReceiver, fixedMember bool) (*overlay.ADNLOverlayWrapper, *overlay.RLDPOverlayWrapper, func(), error) {
	p.mx.Lock()
	if p.peers[pooled.id] != pooled || pooledPeerClosed(pooled) {
		p.mx.Unlock()
		return nil, nil, nil, errPooledPeerUnavailable
	}

	adnlOverlay, err := pooled.adnl.AttachOverlay(receiver)
	if err != nil {
		p.mx.Unlock()
		return nil, nil, nil, err
	}
	rldpOverlay := pooled.rldp.CreateOverlay(receiver.OverlayID())

	pooled.refs++
	pooled.adnlOverlayRefs[adnlOverlay]++
	pooled.rldpOverlayRefs[rldpOverlay]++
	if fixedMember {
		pooled.fixedMemberRefs++
		if pooled.fixedMemberRefs == 1 {
			// C++ raises the unexpected RLDP receive MTU only for private-overlay
			// peers. Public and unlisted peers keep tonutils-go's bounded default;
			// expected query answers use their per-request limits independently.
			pooled.baseRLDP.SetMaxUnexpectedTransferSize(maxRLDPTwoStepTransferSize)
		}
	}
	p.mx.Unlock()

	var once sync.Once
	return adnlOverlay, rldpOverlay, func() {
		once.Do(func() {
			p.releaseOverlay(pooled, adnlOverlay, rldpOverlay, fixedMember)
		})
	}, nil
}

func (p *peerPool) releaseOverlay(pooled *pooledPeer, adnlOverlay *overlay.ADNLOverlayWrapper, rldpOverlay *overlay.RLDPOverlayWrapper, fixedMember bool) {
	p.mx.Lock()
	// Detach zero-ref wrappers before releasing the pool lock. Otherwise a
	// concurrent acquire can reuse the same wrapper in the gap and a stale
	// Close would detach the live generation.
	if refs := pooled.adnlOverlayRefs[adnlOverlay]; refs <= 1 {
		delete(pooled.adnlOverlayRefs, adnlOverlay)
		adnlOverlay.Close()
	} else {
		pooled.adnlOverlayRefs[adnlOverlay] = refs - 1
	}
	if refs := pooled.rldpOverlayRefs[rldpOverlay]; refs <= 1 {
		delete(pooled.rldpOverlayRefs, rldpOverlay)
		rldpOverlay.Close()
	} else {
		pooled.rldpOverlayRefs[rldpOverlay] = refs - 1
	}

	if pooled.refs > 0 {
		pooled.refs--
	}
	if fixedMember {
		pooled.fixedMemberRefs--
		if pooled.fixedMemberRefs == 0 {
			// The transport stays pooled for reuse by public overlays, so the
			// raised private-overlay MTU is restored to the bounded default.
			pooled.baseRLDP.SetMaxUnexpectedTransferSize(0)
		}
	}
	var stale []*pooledPeer
	if pooledPeerIdle(pooled) && p.peers[pooled.id] == pooled {
		now := time.Now()
		pooled.touch(now)
		stale = p.pruneOverCapLocked(now)
	}
	p.mx.Unlock()

	for _, idle := range stale {
		idle.close()
	}
}

func (p *peerPool) closeIfUnused(pooled *pooledPeer) {
	var closeBase *pooledPeer

	p.mx.Lock()
	if pooled.refs == 0 && len(pooled.adnlOverlayRefs) == 0 && len(pooled.rldpOverlayRefs) == 0 && p.peers[pooled.id] == pooled {
		delete(p.peers, pooled.id)
		closeBase = pooled
	}
	p.mx.Unlock()

	if closeBase != nil {
		closeBase.close()
	}
}

func (p *peerPool) retain(pooled *pooledPeer) bool {
	p.mx.Lock()
	defer p.mx.Unlock()

	if p.peers[pooled.id] != pooled {
		return false
	}
	pooled.refs++
	return true
}

func (p *peerPool) retainByID(peerID PeerID) *pooledPeer {
	p.mx.Lock()
	defer p.mx.Unlock()

	pooled := p.peers[peerID]
	if pooled == nil {
		return nil
	}
	if pooledPeerClosed(pooled) {
		delete(p.peers, peerID)
		return nil
	}
	pooled.refs++
	return pooled
}

// pooledPeerClosed reports whether the pooled transport has already been
// closed underneath (the disconnect cascade prunes the pool asynchronously,
// so a closed entry can linger in the map for a moment).
func pooledPeerClosed(pooled *pooledPeer) bool {
	select {
	case <-pooled.adnl.GetCloserCtx().Done():
		return true
	default:
		return false
	}
}

func (p *peerPool) releaseRetained(pooled *pooledPeer) {
	p.mx.Lock()
	if pooled.refs > 0 {
		pooled.refs--
	}
	var stale []*pooledPeer
	if pooledPeerIdle(pooled) && p.peers[pooled.id] == pooled {
		now := time.Now()
		pooled.touch(now)
		stale = p.pruneOverCapLocked(now)
	}
	p.mx.Unlock()

	for _, idle := range stale {
		idle.close()
	}
}

func (p *peerPool) size() int {
	p.mx.RLock()
	defer p.mx.RUnlock()
	return len(p.peers)
}

func (p *peerPool) pruneIdle(now time.Time) int {
	p.mx.Lock()
	stale := p.sweepIdleLocked(now)
	p.mx.Unlock()

	for _, idle := range stale {
		idle.close()
	}
	return len(stale)
}

// pruneOverCapLocked is the release-path variant: it only enforces the hard
// cap, and only once the pool is over it, so releasing a peer never pays for a
// per-peer Stats() scan. Expiring by TTL is the periodic sweep's job.
func (p *peerPool) pruneOverCapLocked(now time.Time) []*pooledPeer {
	if len(p.peers) <= peerPoolMaxIdle {
		return nil
	}
	return p.sweepIdleLocked(now)
}

// sweepIdleLocked drops pooled transports nothing references any more: first
// everything idle past the TTL, then the least recently used above the cap.
//
// The TTL used to be gated behind "pool larger than the cap", which made it
// inert in practice: the pool only had to stay under peerPoolMaxIdle, so
// unreferenced peers accumulated for as long as the node ran. Re-dialling a
// peer when it is needed again is cheaper than holding its ADNL channel, its
// RLDP client and that client's two goroutines indefinitely. This mirrors
// sweepIdleQUICPaths, so both transports of a peer now decay together the way
// quicIdleTTL already promises.
//
// Only peers with no refs and no overlay attachments are candidates, and
// lastUsed also reflects real ADNL packet activity, so a transport still
// carrying traffic is kept even when nothing holds a reference to it.
func (p *peerPool) sweepIdleLocked(now time.Time) []*pooledPeer {
	cutoff := now.Add(-peerPoolIdleTTL)
	var stale []*pooledPeer
	idle := make([]idlePooledPeer, 0, len(p.peers))

	for id, pooled := range p.peers {
		if pooledPeerClosed(pooled) {
			delete(p.peers, id)
			continue
		}
		if !pooledPeerIdle(pooled) {
			continue
		}
		lastUsed := pooled.lastUsed()
		if !lastUsed.After(cutoff) {
			delete(p.peers, id)
			stale = append(stale, pooled)
			continue
		}
		idle = append(idle, idlePooledPeer{peer: pooled, lastUsed: lastUsed})
	}

	if excess := len(idle) - peerPoolMaxIdle; excess > 0 {
		sort.Slice(idle, func(i, j int) bool {
			return idle[i].lastUsed.Before(idle[j].lastUsed)
		})
		for _, candidate := range idle[:excess] {
			delete(p.peers, candidate.peer.id)
			stale = append(stale, candidate.peer)
		}
	}
	return stale
}

func pooledPeerIdle(pooled *pooledPeer) bool {
	return pooled.refs == 0 && len(pooled.adnlOverlayRefs) == 0 && len(pooled.rldpOverlayRefs) == 0
}

func (p *pooledPeer) close() {
	if p.rldp != nil {
		p.rldp.Close()
		return
	}
	if p.adnl != nil {
		p.adnl.Close()
	}
}

func (p *peerPool) overlaySnapshot() []*pooledPeer {
	p.mx.RLock()
	defer p.mx.RUnlock()

	list := make([]*pooledPeer, 0, len(p.peers))
	for _, peer := range p.peers {
		if len(peer.adnlOverlayRefs) == 0 {
			continue
		}
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

func (d *eventDeduper) Forget(key string) {
	d.mx.Lock()
	defer d.mx.Unlock()

	d.deleteEntryLocked(d.seen[key])
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
		d.deleteEntryLocked(elem.Value.(*eventDeduperEntry))
	}
}

func (d *eventDeduper) deleteEntryLocked(entry *eventDeduperEntry) {
	if entry == nil {
		return
	}
	delete(d.seen, entry.key)
	d.order.Remove(entry.element)
	entry.element = nil
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
		Kind:              overlayKindPublicShard,
		Workchain:         workchain,
		Shard:             shard,
		FullID:            fullID,
		ShortID:           shortID,
		ProtoVersionMajor: protoMajor,
		ProtoVersionMinor: protoMinor,
		Announce:          true,
		RandomPeers:       true,
		QueryCapabilities: true,
	}, nil
}

func buildCustomOverlaySpecs(zeroStateFileHash []byte, overlays []CustomOverlayConfig, localID PeerID) ([]overlaySpec, error) {
	if len(overlays) == 0 {
		return nil, nil
	}

	specs := make([]overlaySpec, 0, len(overlays))
	for _, cfg := range overlays {
		spec, localMember, err := buildCustomOverlaySpec(zeroStateFileHash, cfg, localID)
		if err != nil {
			return nil, err
		}
		if !localMember {
			continue
		}
		specs = append(specs, spec)
	}
	return specs, nil
}

func buildCustomOverlaySpec(zeroStateFileHash []byte, cfg CustomOverlayConfig, localID PeerID) (overlaySpec, bool, error) {
	nodes := make([][]byte, 0, len(cfg.Nodes))
	fixedNodes := make([]PeerID, 0, len(cfg.Nodes))
	fixedIDs := make(map[PeerID]struct{}, len(cfg.Nodes))
	queryAcceptorIDs := make(map[PeerID]struct{}, len(cfg.Nodes))
	msgSenders := map[PeerID]int{}
	blockSenders := map[PeerID]struct{}{}
	authorizedKeys := map[string]uint32{}
	acceptQueries := false

	for _, node := range cfg.Nodes {
		if node.ADNLID.IsZero() {
			return overlaySpec{}, false, fmt.Errorf("custom overlay %q has empty adnl_id", cfg.Name)
		}
		id := node.ADNLID.Bytes()
		if _, ok := fixedIDs[node.ADNLID]; !ok {
			nodes = append(nodes, id)
			fixedNodes = append(fixedNodes, node.ADNLID)
			fixedIDs[node.ADNLID] = struct{}{}
		}
		if node.MsgSender {
			msgSenders[node.ADNLID] = node.MsgSenderPriority
			authorizedKeys[string(id)] = maxOverlayPayloadSize
		}
		if node.BlockSender {
			blockSenders[node.ADNLID] = struct{}{}
			authorizedKeys[string(id)] = maxOverlayPayloadSize
		}
		if node.AcceptQueries {
			queryAcceptorIDs[node.ADNLID] = struct{}{}
		}
		if node.ADNLID == localID && node.AcceptQueries {
			acceptQueries = true
		}
	}
	if len(nodes) == 0 {
		return overlaySpec{}, false, fmt.Errorf("custom overlay %q has no nodes", cfg.Name)
	}

	sort.Slice(nodes, func(i, j int) bool {
		return bytes.Compare(nodes[i], nodes[j]) < 0
	})
	queryAcceptors := make([]PeerID, 0, len(queryAcceptorIDs))
	for id := range queryAcceptorIDs {
		queryAcceptors = append(queryAcceptors, id)
	}
	sort.Slice(queryAcceptors, func(i, j int) bool {
		return bytes.Compare(queryAcceptors[i][:], queryAcceptors[j][:]) < 0
	})

	fullID, err := tl.Hash(tonnodeapi.CustomOverlayID{
		ZeroStateFileHash: zeroStateFileHash,
		Name:              cfg.Name,
		Nodes:             nodes,
	})
	if err != nil {
		return overlaySpec{}, false, fmt.Errorf("build custom overlay %q id: %w", cfg.Name, err)
	}

	shortID, err := tl.Hash(keys.PublicKeyOverlay{Key: fullID})
	if err != nil {
		return overlaySpec{}, false, fmt.Errorf("build custom overlay %q short id: %w", cfg.Name, err)
	}

	_, localMember := fixedIDs[localID]
	return overlaySpec{
		Name:              "custom." + cfg.Name,
		Kind:              overlayKindCustomFixed,
		Workchain:         0,
		Shard:             topShard,
		FullID:            fullID,
		ShortID:           shortID,
		ProtoVersionMajor: shardchainProtoVersionMajor,
		ProtoVersionMinor: shardchainProtoVersionMinor,
		FixedNodes:        fixedNodes,
		FixedNodeIDs:      fixedIDs,
		QueryAcceptors:    queryAcceptors,
		MsgSenders:        msgSenders,
		BlockSenders:      blockSenders,
		AuthorizedKeys:    authorizedKeys,
		SenderShards:      append([]CustomOverlayShard(nil), cfg.SenderShards...),
		SkipPublicMsgSend: cfg.SkipPublicMsgSend,
		UseQUIC:           cfg.UseQUIC,
		SendQueries:       cfg.SendQueries,
		AcceptQueries:     acceptQueries,
		Announce:          false,
		RandomPeers:       false,
		QueryCapabilities: false,
	}, localMember, nil
}

func overlaySpecKey(spec overlaySpec) string {
	return string(spec.ShortID)
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
	return n.subscriptionForOverlayBlock(n.overlayBlockForDownload(block))
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
			if closed := n.pool.pruneIdle(now); closed > 0 {
				n.log.Info().
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
