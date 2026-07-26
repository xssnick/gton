package p2p

import (
	"context"
	"crypto/ed25519"
	"net"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	topShard              = int64(-1 << 63)
	maxPeersPerOverlay    = 20
	maxQueryNeighbours    = 16
	maxOverlayPayloadSize = 16 << 20
	peerPoolIdleTTL       = 130 * time.Second
	peerPoolMaxIdle       = 2048
	// C++ private overlays set the RLDP2 peer MTU to max broadcast size + 1024.
	maxRLDPTwoStepTransferSize = maxOverlayPayloadSize + 1024
	dhtRefreshInterval         = 90 * time.Second
	peerRefreshMinDelay        = time.Second
	peerRefreshJitter          = 0
	peerRefreshFanout          = 1
	neighbourReloadMinDelay    = 10 * time.Second
	neighbourReloadJitter      = 20 * time.Second
	peerPingMinDelay           = 500 * time.Millisecond
	peerPingJitter             = 500 * time.Millisecond
	peerPingFanout             = 6
	overlayPeerTTL             = 10 * time.Minute
	overlayFutureSkew          = 60 * time.Second
	downloadQueryTimeout       = 15 * time.Second
	downloadNextQueryTimeout   = 5 * time.Second
	downloadNextDescTimeout    = 1500 * time.Millisecond
	downloadRetryDelay         = 1500 * time.Millisecond
	downloadQueryParallelism   = 4
	downloadQueryHedgeDelay    = 350 * time.Millisecond
	proofPrepareTimeout        = time.Second
	proofDownloadTimeout       = 3 * time.Second
	proofDownloadParallelism   = 6
	proofDownloadPeerLimit     = 24
	proofDownloadHedgeDelay    = 250 * time.Millisecond
	proofDownloadWaves         = 3
	proofPeerDiscoveryDelay    = time.Second
	keyBlockLookupTimeout      = time.Second
	keyBlockLookupRoundTimeout = 5 * time.Second
	keyBlockLookupParallelism  = 6
	keyBlockLookupPeerLimit    = 24
	keyBlockLookupHedgeDelay   = 150 * time.Millisecond
	bootstrapDiscoveryTarget   = 64
	dhtSeedConnectParallelism  = 8
	dhtSeedPeerTimeout         = 5 * time.Second
	dhtSeedCooldownMinDelay    = 6 * time.Second
	dhtSeedCooldownJitter      = 4 * time.Second
	dhtRefreshReplacementLimit = 2
	inactiveShardOverlayTTL    = overlayPeerTTL + 60*time.Second
	attachWarmupTimeout        = 3 * time.Second
	broadcastEventBuffer       = 4096
	broadcastQueueMaxItems     = 1024
	broadcastQueueMaxBytes     = int64(512 << 20)
	broadcastDeduperMaxEntries = 4096
	externalMessageCacheTTL    = time.Minute
	externalMessageCacheMax    = 1 << 17
	publicAnnounceTTL          = time.Hour
	publicAnnounceEvery        = 90 * time.Second
	publicAnnounceRetryDelay   = 15 * time.Second
	dhtStoreTimeout            = 45 * time.Second
	dhtFindTimeout             = 30 * time.Second
	peerQueryTimeout           = 10 * time.Second
	broadcastSignatureTimeout  = 2 * time.Second
	peerRebroadcastTimeout     = 5 * time.Second
	peerRebroadcastQueueItems  = 2048
	peerRebroadcastQueueBytes  = int64(1024 << 20)
	externalRebroadcastFanout  = 5
	laggedExternalFanout       = 3
	minLocalExternalFanout     = 3
	maxLocalExternalFanout     = maxPeersPerOverlay
	localRebroadcastAttempts   = 3
	dhtServerStoreMaxKeys      = 300000

	maxBlockDownloadAnswerSize  = 32 << 20
	maxKeyBlockLookupAnswerSize = 1 << 20
	maxDecompressedBlockSize    = 32 << 20
	maxRandomPeerReply          = 4

	masterchainProtoVersionMajor   = 3
	masterchainProtoVersionMinor   = 2
	shardchainProtoVersionMajor    = 3
	shardchainProtoVersionMinor    = 2
	ordinarySimpleBroadcastMaxSize = 768

	peerStopUnreliability = 5.0
	peerFailUnreliability = 10.0
	peerSlowEvictionScore = peerStopUnreliability + 1

	largeDownloadSlowPeerPenalty  = 3 * time.Minute
	blockUnknownPeerSpeed         = float64(256 << 10)
	blockSlowPeerSpeed            = float64(64 << 10)
	blockSlowPeerPenalty          = 30 * time.Second
	blockUnavailablePeerPenalty   = 10 * time.Second
	blockUnavailableConfirmWindow = 3 * time.Second
	blockSpeedSampleMin           = int64(64 << 10)

	customQueryProbeMinDelay  = time.Second
	customQueryProbeJitter    = time.Second
	customQueryProbeTimeout   = time.Second
	customQueryProbeMaxPeers  = 3
	customQueryProbeMaxAnswer = 1024
	fastSyncPingMinDelay      = time.Second
	fastSyncPingJitter        = time.Second
	fastSyncPingFanout        = 5
	fastSyncPingMaxAnswer     = 1024
)

// inboundQUICQueryParallelism bounds project-local concurrent QUIC query work.
const inboundQUICQueryParallelism = 64

const (
	prefetchShardBlockTimeout    = 2500 * time.Millisecond
	prefetchShardBlockPeers      = 3
	prefetchShardBlockWidePeers  = 8
	prefetchShardBlockStageDelay = 250 * time.Millisecond
)

type Delivery string

const (
	DeliverySimple   Delivery = "simple"
	DeliveryFEC      Delivery = "fec"
	DeliveryTwoStep  Delivery = "two_step"
	DeliveryPlumtree Delivery = "plumtree"
)

type BroadcastEvent struct {
	Overlay          string
	Kind             string
	Delivery         Delivery
	Trusted          bool
	Block            ton.BlockIDExt
	Downloaded       *DownloadedBlock
	ShardDescription *ShardBlockDescription
	SourcePeerID     PeerID
	ReceivedAt       time.Time
}

type BroadcastPipelineObserver interface {
	ObserveBroadcastPipelineStage(BroadcastPipelineStageObservation)
}

type BroadcastPipelineStageObservation struct {
	Stage    string
	Kind     string
	Delivery Delivery
	Result   string
	Duration time.Duration
}

type BroadcastAdmissionRequest struct {
	Kind  string
	Local bool
}

type BroadcastAdmission interface {
	CanAcceptBroadcast(req BroadcastAdmissionRequest) bool
}

func (e BroadcastEvent) BlockRef() string {
	return storage.FormatBlockRef(e.Block)
}

type Options struct {
	Logger                           *zerolog.Logger
	GlobalConfig                     *liteclient.GlobalConfig
	PrivateKey                       ed25519.PrivateKey
	ListenAddr                       string
	ExternalIP                       net.IP
	ExternalPort                     uint16
	DHTPrivateKey                    ed25519.PrivateKey
	DHTListenAddr                    string
	StateFilesDir                    string
	Storage                          storage.Storage
	PeerServingStorage               storage.PeerServingStorage
	LiveBlockCache                   *storage.LiveBlockCache
	CompressedState                  CompressedBlockStateProvider
	SyncLag                          SyncLagProvider
	SignatureVerifier                BroadcastSignatureVerifier
	BroadcastAdmission               BroadcastAdmission
	ExternalMessageAdmission         ExternalMessageAdmission
	BlockReceivedObserver            BlockReceivedObserver
	ExternalBroadcastCapacity        ExternalBroadcastCapacityOptions
	AllowDuplicateExternals          bool
	LocalExternalFanout              int
	CustomOverlays                   []CustomOverlayConfig
	FastSyncCertificates             []overlay.MemberCertificate
	FastSyncBroadcastSpeedMultiplier float64
}

type ExternalBroadcastCapacityOptions struct {
	BytesPerSecond int64
	MaxDelay       time.Duration
}

type BroadcastSignatureVerifier interface {
	CheckBlockBroadcastSignatures(ctx context.Context, req BlockBroadcastSignatureCheck) error
	CheckBlockFinalitySignatures(ctx context.Context, req BlockFinalitySignatureCheck) (*BlockFinalitySignatureCheckResult, error)
	ValidateShardDescriptionBroadcast(ctx context.Context, req ShardDescriptionSignatureCheck) (*ShardBlockDescription, error)
}

type BlockBroadcastSignatureCheck struct {
	Kind       string
	Block      ton.BlockIDExt
	Proof      *cell.Cell
	Signatures *blockproof.ValidatorSignatureSet
}

type BlockFinalitySignatureCheck struct {
	Kind       string
	Block      ton.BlockIDExt
	Signatures *blockproof.ValidatorSignatureSet
}

type BlockFinalitySignatureCheckResult struct {
	SignaturesCell        *cell.Cell
	SignaturesVerifiedKey []byte
}

type ShardDescriptionSignatureCheck struct {
	Block         ton.BlockIDExt
	CatchainSeqno int32
	Data          []byte
}

type ShardBlockDescription struct {
	Block            ton.BlockIDExt
	CatchainSeqno    uint32
	ValidatorSetHash uint32
	Data             []byte
	Chain            []ShardDescriptionLink
}

type ShardDescriptionLink struct {
	Block          ton.BlockIDExt
	PrevRefs       []ton.BlockIDExt
	MasterchainRef *ton.BlockIDExt
	ProofRoot      *cell.Cell
	ProofBOC       []byte
}

type BlockCacheObserver interface {
	NonfinalBlockCacheEnabled() bool
	PublishNonfinalBlockArtifacts(artifacts storage.LiveBlockArtifacts, kind storage.LiveBlockNonfinalKind) error
}

type SyncLagProvider interface {
	SyncLagSeconds() (int64, error)
}

type ExternalMessageEvent struct {
	// IsLocal is true when the message originated from the local API path.
	IsLocal bool
	// Body is the raw external message BOC.
	Body []byte
	// Root is the parsed external message root when already available.
	Root *cell.Cell
	// Message is the decoded external message when already available.
	Message *tlb.ExternalMessage
}

type ExternalMessageAdmission interface {
	AcceptExternalMessage(ctx context.Context, event ExternalMessageEvent) error
	AcceptCheckedExternalMessage(ctx context.Context, event ExternalMessageEvent) error
}

type BlockReceivedEvent struct {
	// IsSigned is true for signed block broadcasts and downloaded full blocks.
	IsSigned bool
	// Downloaded contains the received block artifacts.
	Downloaded *DownloadedBlock
}

type BlockReceivedObserver interface {
	ObserveBlockReceived(ctx context.Context, event BlockReceivedEvent)
	BlockReceivedHooksEnabled() bool
}

type RuntimeCallbacks interface {
	CompressedBlockStateProvider
	SyncLagProvider
	BroadcastSignatureVerifier
	BroadcastAdmission
	ExternalMessageAdmission
	BlockReceivedObserver
}

type CustomOverlayConfig struct {
	Name              string
	Nodes             []CustomOverlayNodeConfig
	SenderShards      []CustomOverlayShard
	SkipPublicMsgSend bool
	UseQUIC           bool
	SendQueries       bool
}

type CustomOverlayNodeConfig struct {
	ADNLID            PeerID
	MsgSender         bool
	MsgSenderPriority int
	BlockSender       bool
	AcceptQueries     bool
}

type CustomOverlayShard struct {
	Workchain int32
	Shard     int64
}

type overlayKind uint8

const (
	overlayKindPublicShard overlayKind = iota
	overlayKindCustomFixed
	overlayKindFastSync
)

type overlaySpec struct {
	Name              string
	Kind              overlayKind
	Workchain         int32
	Shard             int64
	FullID            []byte
	ShortID           []byte
	ProtoVersionMajor int32
	ProtoVersionMinor int32
	FixedNodes        []PeerID
	FixedNodeIDs      map[PeerID]struct{}
	QueryAcceptors    []PeerID
	MsgSenders        map[PeerID]int
	BlockSenders      map[PeerID]struct{}
	AuthorizedKeys    map[string]uint32
	SenderShards      []CustomOverlayShard
	SkipPublicMsgSend bool
	UseQUIC           bool
	SendQueries       bool
	AcceptQueries     bool
	Announce          bool
	DHTDiscovery      bool
	RandomPeers       bool
	QueryCapabilities bool
	FastSync          *fastSyncOverlaySpec
}

type DownloadedBlock struct {
	ID ton.BlockIDExt
	// Kind is the TL type returned by the peer, for example tonNode.dataFull.
	Kind     string
	Block    *cell.Cell
	Proof    *cell.Cell
	BlockBOC []byte
	ProofBOC []byte
	Meta     *storage.BlockMeta

	StateUpdate  *cell.Cell
	SourcePeerID PeerID

	IsLink bool

	VerifiedRootHash bool
	// SignaturesVerifiedKey is the blockproof.ValidatorSignatureSet.ContentKey
	// of a validator signature set that already passed the broadcast-level
	// signature check for this block; nil when no such check happened.
	// Consumers may skip re-verifying a signature set whose content key
	// equals this one.
	SignaturesVerifiedKey []byte
}

func (b DownloadedBlock) BlockRef() string {
	return storage.FormatBlockRef(b.ID)
}

func blockWithEffectiveShard(block ton.BlockIDExt, effectiveShard int64) ton.BlockIDExt {
	if effectiveShard != 0 {
		block.Shard = effectiveShard
	}
	return block
}

func formatPersistentStateBlockRef(block ton.BlockIDExt, effectiveShard int64) string {
	return storage.FormatBlockRef(blockWithEffectiveShard(block, effectiveShard))
}
