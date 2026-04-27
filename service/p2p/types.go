package p2p

import (
	"crypto/ed25519"
	"flexserver/service/storage"
	"fmt"
	"net"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	DefaultGlobalConfigPath = "global.config.json"

	topShard                  = int64(-1 << 63)
	maxPeersPerOverlay        = 20
	maxQueryNeighbours        = 16
	maxOverlayPayloadSize     = 16 << 20
	simpleBroadcastSkew       = 20 * time.Second
	dhtRefreshInterval        = 45 * time.Second
	peerRefreshMinDelay       = time.Second
	peerRefreshJitter         = 0
	peerRefreshFanout         = 1
	neighbourReloadMinDelay   = 10 * time.Second
	neighbourReloadJitter     = 20 * time.Second
	peerPingMinDelay          = 500 * time.Millisecond
	peerPingJitter            = 500 * time.Millisecond
	peerPingFanout            = 6
	overlayPeerTTL            = 10 * time.Minute
	overlayFutureSkew         = 60 * time.Second
	downloadQueryTimeout      = 15 * time.Second
	downloadNextQueryTimeout  = 5 * time.Second
	downloadRetryDelay        = 1500 * time.Millisecond
	downloadQueryParallelism  = 4
	downloadQueryHedgeDelay   = 350 * time.Millisecond
	keyBlockLookupTimeout     = time.Second
	keyBlockLookupParallelism = 2
	keyBlockLookupHedgeDelay  = 250 * time.Millisecond
	broadcastEventBuffer      = 4096
	publicAnnounceTTL         = 12 * time.Minute
	publicAnnounceEvery       = 4 * time.Minute
	publicAnnounceRetryDelay  = 15 * time.Second
	dhtStoreTimeout           = 45 * time.Second
	dhtFindTimeout            = 30 * time.Second
	masterchainWaitLogEvery   = 5 * time.Second
	peerQueryTimeout          = 10 * time.Second
	rebroadcastWorkerCount    = 4
	inboundIngestWorkerCount  = 4
	dhtServerStoreMaxKeys     = 300000

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
)

type Delivery string

const (
	DeliverySimple Delivery = "simple"
	DeliveryFEC    Delivery = "fec"
)

type BroadcastEvent struct {
	Overlay    string
	Kind       string
	Delivery   Delivery
	Trusted    bool
	Block      ton.BlockIDExt
	SourceKey  string
	ReceivedAt time.Time
}

func (e BroadcastEvent) BlockRef() string {
	return formatBlockRef(e.Block)
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
	StateDownloadDir   string
	Storage            storage.Storage
	PeerServingStorage storage.PeerServingStorage
}

type overlaySpec struct {
	Name              string
	FullID            []byte
	ShortID           []byte
	ProtoVersionMajor int32
	ProtoVersionMinor int32
}

type DownloadedBlock struct {
	ID ton.BlockIDExt
	// Kind is the TL type returned by the peer, for example tonNode.dataFull.
	Kind     string
	Block    *cell.Cell
	Proof    *cell.Cell
	BlockBOC []byte
	ProofBOC []byte
	Parsed   *tlb.Block
	Meta     *storage.BlockMeta

	IsLink bool

	VerifiedRootHash bool
	VerifiedFileHash bool
}

func (b DownloadedBlock) BlockRef() string {
	return formatBlockRef(b.ID)
}

func formatBlockRef(block ton.BlockIDExt) string {
	return fmt.Sprintf("wc=%d shard=%016x seqno=%d", block.Workchain, uint64(block.Shard), block.SeqNo)
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
