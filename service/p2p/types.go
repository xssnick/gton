package p2p

import (
	"context"
	"crypto/ed25519"
	"net"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	DefaultGlobalConfigPath = "global.config.json"

	topShard              = int64(-1 << 63)
	maxPeersPerOverlay    = 20
	maxQueryNeighbours    = 16
	maxOverlayPayloadSize = 16 << 20
	// C++ private overlays set the RLDP2 peer MTU to max broadcast size + 1024.
	maxRLDPTwoStepTransferSize = maxOverlayPayloadSize + 1024
	simpleBroadcastSkew        = 20 * time.Second
	dhtRefreshInterval         = 90 * time.Second
	peerRefreshMinDelay        = time.Second
	peerRefreshJitter          = 0
	peerRefreshFanout          = 1
	neighbourReloadMinDelay    = 10 * time.Second
	neighbourReloadJitter      = 20 * time.Second
	peerPingMinDelay           = 500 * time.Millisecond
	peerPingJitter             = 500 * time.Millisecond
	peerPingFanout             = 6
	adnlPingMinDelay           = 30 * time.Second
	adnlPingJitter             = 20 * time.Second
	adnlPingFanout             = 5
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
	broadcastQueueMaxBytes     = int64(128 << 20)
	broadcastDeduperMaxEntries = 4096
	externalMessageCacheTTL    = time.Minute
	externalMessageCacheMax    = 1 << 17
	publicAnnounceTTL          = time.Hour
	publicAnnounceEvery        = 90 * time.Second
	publicAnnounceRetryDelay   = 15 * time.Second
	dhtStoreTimeout            = 45 * time.Second
	dhtFindTimeout             = 30 * time.Second
	masterchainWaitLogEvery    = 5 * time.Second
	peerQueryTimeout           = 10 * time.Second
	broadcastSignatureTimeout  = 2 * time.Second
	peerRebroadcastTimeout     = 5 * time.Second
	peerRebroadcastQueueItems  = 2048
	peerRebroadcastQueueBytes  = int64(256 << 20)
	externalRebroadcastFanout  = 5
	localRebroadcastAttempts   = 3
	dhtServerStoreMaxKeys      = 300000

	maxBlockDownloadAnswerSize  = 32 << 20
	maxKeyBlockLookupAnswerSize = 1 << 20
	maxDecompressedBlockSize    = 32 << 20
	maxRandomPeerReply          = 4

	masterchainProtoVersionMajor   = 1
	masterchainProtoVersionMinor   = 0
	shardchainProtoVersionMajor    = 3
	shardchainProtoVersionMinor    = 0
	ordinarySimpleBroadcastMaxSize = 768

	peerStopUnreliability = 5.0
	peerFailUnreliability = 10.0
	peerSlowEvictionScore = peerStopUnreliability + 1

	blockUnknownPeerSpeed         = float64(256 << 10)
	blockSlowPeerSpeed            = float64(64 << 10)
	blockSlowPeerPenalty          = 30 * time.Second
	blockUnavailablePeerPenalty   = 10 * time.Second
	blockUnavailableConfirmWindow = 3 * time.Second
	blockSpeedSampleMin           = int64(64 << 10)
)

type Delivery string

const (
	DeliverySimple  Delivery = "simple"
	DeliveryFEC     Delivery = "fec"
	DeliveryTwoStep Delivery = "two_step"
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

func (e BroadcastEvent) BlockRef() string {
	return formatBlockRef(e.Block)
}

type ShardBlockDescription struct {
	CatchainSeqno int32
	Data          []byte
}

type Options struct {
	Logger             *zerolog.Logger
	GlobalConfigPath   string
	PrivateKey         ed25519.PrivateKey
	ListenAddr         string
	ExternalIP         net.IP
	ExternalPort       uint16
	DHTPrivateKey      ed25519.PrivateKey
	DHTListenAddr      string
	StateFilesDir      string
	Storage            storage.Storage
	PeerServingStorage storage.PeerServingStorage
	CompressedState    CompressedBlockStateProvider
	SyncLag            SyncLagProvider
	SignatureVerifier  BroadcastSignatureVerifier
	CustomOverlays     []CustomOverlayConfig
}

type BroadcastSignatureVerifier interface {
	CheckBlockBroadcastSignatures(ctx context.Context, req BlockBroadcastSignatureCheck) error
	CheckShardDescriptionSignatures(ctx context.Context, req ShardDescriptionSignatureCheck) error
}

type BlockBroadcastSignatureCheck struct {
	Kind       string
	Block      ton.BlockIDExt
	Proof      *cell.Cell
	Signatures *blockproof.ValidatorSignatureSet
}

type ShardDescriptionSignatureCheck struct {
	Block         ton.BlockIDExt
	CatchainSeqno int32
	Data          []byte
}

type BlockCacheObserver interface {
	MarkLiveBlockFlushed(block ton.BlockIDExt)
	NonfinalBlockCacheEnabled() bool
	PublishNonfinalBlockArtifacts(artifacts storage.LiveBlockArtifacts, kind storage.LiveBlockNonfinalKind) error
}

type SyncLagProvider interface {
	SyncLagSeconds() (int64, bool)
}

type SyncLagProviderFunc func() (int64, bool)

func (f SyncLagProviderFunc) SyncLagSeconds() (int64, bool) {
	return f()
}

type CustomOverlayConfig struct {
	Name              string
	Nodes             []CustomOverlayNodeConfig
	SenderShards      []CustomOverlayShard
	SkipPublicMsgSend bool
}

type CustomOverlayNodeConfig struct {
	ADNLID            PeerID
	MsgSender         bool
	MsgSenderPriority int
	BlockSender       bool
}

type CustomOverlayShard struct {
	Workchain int32
	Shard     int64
}

type overlayKind uint8

const (
	overlayKindPublicShard overlayKind = iota
	overlayKindCustomFixed
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
	MsgSenders        map[PeerID]int
	BlockSenders      map[PeerID]struct{}
	AuthorizedKeys    map[string]uint32
	SenderShards      []CustomOverlayShard
	SkipPublicMsgSend bool
	Announce          bool
	DHTDiscovery      bool
	RandomPeers       bool
	QueryCapabilities bool
}

type DownloadedBlock struct {
	ID ton.BlockIDExt
	// Kind is the TL type returned by the peer, for example tonNode.dataFull.
	Kind     string
	Block    *cell.Cell
	Proof    *cell.Cell
	BlockBOC []byte
	ProofBOC []byte
	// BroadcastSignatures is the validator signature set received with block broadcasts.
	// It is not proof of validity by itself; service consensus validation checks it
	// against the validator set before the block is used as verified.
	BroadcastSignatures *cell.Cell
	Meta                *storage.BlockMeta
	StateUpdate         *cell.Cell
	SourcePeerID        PeerID

	IsLink bool

	VerifiedRootHash bool
}

func (b DownloadedBlock) BlockRef() string {
	return formatBlockRef(b.ID)
}

func formatBlockRef(block ton.BlockIDExt) string {
	return storage.FormatBlockRef(block)
}

func blockWithEffectiveShard(block ton.BlockIDExt, effectiveShard int64) ton.BlockIDExt {
	if effectiveShard != 0 {
		block.Shard = effectiveShard
	}
	return block
}

func formatPersistentStateBlockRef(block ton.BlockIDExt, effectiveShard int64) string {
	return formatBlockRef(blockWithEffectiveShard(block, effectiveShard))
}
