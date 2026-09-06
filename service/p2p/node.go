package p2p

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/gton/internal/extmsg"
	"github.com/xssnick/gton/internal/logutil"
	"github.com/xssnick/gton/service/p2p/internal/eventdedupe"
	"github.com/xssnick/gton/service/p2p/internal/peerroute"
	storage2 "github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/adnl/dht"
	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	adnlquic "github.com/xssnick/tonutils-go/adnl/quic"
	"github.com/xssnick/tonutils-go/adnl/rldp"
	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

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
	quicOutboundDialSlots    chan struct{}
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
	peerRoutes               *peerroute.Table[PeerID]
	events                   chan BroadcastEvent
	eventQueue               *boundedQueue[BroadcastEvent]
	blockBroadcasts          *BlockBroadcasts
	deduper                  *eventdedupe.Cache
	overlayFanoutDeduper     *eventdedupe.Cache
	decodedBroadcasts        decodedBroadcastCache
	decodeQueue              chan offloadedBroadcastDecode
	masterDecodeQueue        chan offloadedBroadcastDecode
	decodeWorkersOnce        sync.Once

	myExternalMessages        *eventdedupe.Cache
	processedExternalMessages *eventdedupe.Cache
	externalMessageLimiter    *extmsg.AddressLimiter
	externalBroadcastPacer    *externalBroadcastPacer
	allowDuplicateExternals   bool
	localExternalFanout       int
	// relayEgress meters the FEC parts this node forwards on public overlays;
	// nil disables the metering. See relay_budget.go.
	relayEgress *relayEgressBudget

	onceStop sync.Once
	stopped  chan struct{}
	wg       sync.WaitGroup
	// lifecycleWG owns the context watcher separately because it may be the
	// goroutine that initiates shutdown and therefore cannot join through wg.
	lifecycleWG sync.WaitGroup

	lifecycleMu    sync.Mutex
	lifecycleState nodeLifecycleState
	startDone      chan struct{}
	startErr       error
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
	stateArtifacts                  StateArtifactStorage
	peerCache                       storage2.OverlayPeerCache
	fastSyncCertificateStorage      storage2.FastSyncCertificateStorage
	peerStorage                     PeerStorage
	liveBlockCache                  *storage2.LiveBlockCache
	compressedState                 CompressedBlockStateProvider
	syncLag                         SyncLagProvider
	signatureVerifier               BroadcastSignatureVerifier
	broadcastAdmission              BroadcastAdmission
	externalMessageAdmission        ExternalMessageAdmission
	blockReceivedObserver           BlockReceivedObserver
	broadcastPipelineObserver       BroadcastPipelineObserver
	runtimeCallbacksMu              sync.Mutex
	runtimeCallbacksBound           bool
	runtimeCallbacksSealed          bool
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
	privateOverlays          *PrivateOverlayRegistry
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
	if opts.PeerStorage == nil {
		return nil, fmt.Errorf("peer storage is required")
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
	relayEgressBits := opts.RelayEgressBitsPerSecond
	if relayEgressBits == 0 {
		relayEgressBits = defaultRelayEgressBitsPerSecond
	}
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
		quicOutboundDialSlots:         make(chan struct{}, outboundQUICDialParallelism),
		fastSyncBroadcastFECPace:      fastSyncBroadcastFECPace,
		quicPeers:                     make(map[PeerID]*authenticatedQUICPeer),
		dhtListenAddr:                 opts.DHTListenAddr,
		events:                        make(chan BroadcastEvent, broadcastEventBuffer),
		eventQueue:                    newBoundedQueue(broadcastQueueMaxItems, broadcastQueueMaxBytes, broadcastEventBytes),
		deduper:                       eventdedupe.New(10*time.Minute, broadcastDeduperMaxEntries),
		overlayFanoutDeduper:          eventdedupe.New(10*time.Minute, broadcastDeduperMaxEntries),
		myExternalMessages:            eventdedupe.New(externalMessageCacheTTL, externalMessageCacheMax),
		processedExternalMessages:     eventdedupe.New(externalMessageCacheTTL, externalMessageCacheMax),
		externalMessageLimiter:        extmsg.NewDefaultAddressLimiter(),
		externalBroadcastPacer:        externalBroadcastPacer,
		allowDuplicateExternals:       opts.AllowDuplicateExternals,
		localExternalFanout:           localExternalFanout,
		relayEgress:                   newRelayEgressBudget(relayEgressBits, time.Now()),
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
		stateArtifacts:                opts.StateArtifactStorage,
		peerCache:                     opts.PeerCache,
		fastSyncCertificateStorage:    opts.FastSyncCertificateStorage,
		peerStorage:                   opts.PeerStorage,
		liveBlockCache:                liveBlockCache,
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
	node.privateOverlays = newPrivateOverlayRegistry(node)
	node.blockBroadcasts = newBlockBroadcasts(node)
	node.peerRoutes = peerroute.NewTable[PeerID](peerRouteRetryPolicy)
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

func protocolDiagnosticLoggable(msg string) bool {
	return strings.Contains(msg, "error") ||
		strings.Contains(msg, "failed") ||
		strings.Contains(msg, "invalid") ||
		strings.Contains(msg, "too big")
}

func (n *Node) LocalID() PeerID {
	return n.localID
}

func (n *Node) Events() <-chan BroadcastEvent {
	return n.events
}
