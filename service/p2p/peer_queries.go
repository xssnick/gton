package p2p

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"time"

	"github.com/xssnick/gton/service/blockproof"
	tnstate "github.com/xssnick/gton/service/state"
	tnstore "github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/adnl/keys"
	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/adnl/rldp"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

const maxPeerSliceRequestSize = 1 << 24

func init() {
	tl.Register(PreparedProofEmpty{}, "tonNode.preparedProofEmpty = tonNode.PreparedProof")
	tl.Register(PreparedProof{}, "tonNode.preparedProof = tonNode.PreparedProof")
	tl.Register(PreparedProofLink{}, "tonNode.preparedProofLink = tonNode.PreparedProof")
	tl.Register(Prepared{}, "tonNode.prepared = tonNode.Prepared")
	tl.Register(NotFound{}, "tonNode.notFound = tonNode.Prepared")
	tl.Register(Capabilities{}, "tonNode.capabilities#f5bf60c0 version_major:int version_minor:int flags:# = tonNode.Capabilities")
	tl.Register(ArchiveNotFound{}, "tonNode.archiveNotFound = tonNode.ArchiveInfo")
	tl.Register(ArchiveInfo{}, "tonNode.archiveInfo id:long = tonNode.ArchiveInfo")
	tl.Register(ForgetPeer{}, "tonNode.forgetPeer = tonNode.ForgetPeer")
	tl.Register(GetCapabilities{}, "tonNode.getCapabilities = tonNode.Capabilities")
	tl.Register(GetArchiveInfo{}, "tonNode.getArchiveInfo masterchain_seqno:int = tonNode.ArchiveInfo")
	tl.Register(GetShardArchiveInfo{}, "tonNode.getShardArchiveInfo masterchain_seqno:int shard_prefix:tonNode.shardId = tonNode.ArchiveInfo")
	tl.Register(GetArchiveSlice{}, "tonNode.getArchiveSlice archive_id:long offset:long max_size:int = tonNode.Data")
	tl.Register(PrepareBlock{}, "tonNode.prepareBlock block:tonNode.blockIdExt = tonNode.Prepared")
	tl.Register(PrepareBlockProof{}, "tonNode.prepareBlockProof block:tonNode.blockIdExt allow_partial:Bool = tonNode.PreparedProof")
	tl.Register(PrepareKeyBlockProof{}, "tonNode.prepareKeyBlockProof block:tonNode.blockIdExt allow_partial:Bool = tonNode.PreparedProof")
	tl.Register(DownloadBlockProof{}, "tonNode.downloadBlockProof block:tonNode.blockIdExt = tonNode.Data")
	tl.Register(DownloadKeyBlockProof{}, "tonNode.downloadKeyBlockProof block:tonNode.blockIdExt = tonNode.Data")
	tl.Register(DownloadBlockProofLink{}, "tonNode.downloadBlockProofLink block:tonNode.blockIdExt = tonNode.Data")
	tl.Register(DownloadKeyBlockProofLink{}, "tonNode.downloadKeyBlockProofLink block:tonNode.blockIdExt = tonNode.Data")
	tl.Register(IhrMessage{}, "tonNode.ihrMessage data:bytes = tonNode.IhrMessage")
	tl.Register(IhrMessageBroadcast{}, "tonNode.ihrMessageBroadcast message:tonNode.ihrMessage = tonNode.Broadcast")
}

type PreparedProofEmpty struct{}
type PreparedProof struct{}
type PreparedProofLink struct{}
type Prepared struct{}
type NotFound struct{}
type ArchiveNotFound struct{}
type ForgetPeer struct{}
type GetCapabilities struct{}

type Capabilities struct {
	VersionMajor int32  `tl:"int"`
	VersionMinor int32  `tl:"int"`
	Flags        uint32 `tl:"flags"`
}

type ArchiveInfo struct {
	ID int64 `tl:"long"`
}

type GetArchiveInfo struct {
	MasterchainSeqno int32 `tl:"int"`
}

type GetShardArchiveInfo struct {
	MasterchainSeqno int32              `tl:"int"`
	ShardPrefix      tonnodeapi.ShardID `tl:"struct"`
}

type GetArchiveSlice struct {
	ArchiveID int64 `tl:"long"`
	Offset    int64 `tl:"long"`
	MaxSize   int32 `tl:"int"`
}

type PrepareBlock struct {
	Block ton.BlockIDExt `tl:"struct"`
}

type PrepareBlockProof struct {
	Block        ton.BlockIDExt `tl:"struct"`
	AllowPartial bool           `tl:"bool"`
}

type PrepareKeyBlockProof struct {
	Block        ton.BlockIDExt `tl:"struct"`
	AllowPartial bool           `tl:"bool"`
}

type DownloadBlockProof struct {
	Block ton.BlockIDExt `tl:"struct"`
}

type DownloadKeyBlockProof struct {
	Block ton.BlockIDExt `tl:"struct"`
}

type DownloadBlockProofLink struct {
	Block ton.BlockIDExt `tl:"struct"`
}

type DownloadKeyBlockProofLink struct {
	Block ton.BlockIDExt `tl:"struct"`
}

type IhrMessage struct {
	Data []byte `tl:"bytes"`
}

type IhrMessageBroadcast struct {
	Message IhrMessage `tl:"struct"`
}

type advertisedPeerCandidate struct {
	publicKey [ed25519.PublicKeySize]byte
	overlayID [PeerIDSize]byte
	signature [ed25519.SignatureSize]byte
	version   int32
}

var errOverlayInactive = errors.New("overlay is inactive")

func (s *overlaySubscription) answerADNLQuery(peer *overlayPeer, msg *adnl.MessageQuery) error {
	if !s.node.beginInbound() {
		return nil
	}
	defer s.node.finishInbound()

	if peer.noteReceive() {
		s.peerPromoted(peer)
	}

	if err := s.admitInboundQuery(msg.Data, time.Now()); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(s.node.runCtx, peerQueryTimeout)
	defer cancel()

	resp, err := s.handlePeerQuery(ctx, peer.addr, msg.Data)
	if errors.Is(err, errOverlayInactive) {
		s.sendForgetPeer(ctx, peer)
	}
	if errors.Is(err, context.Canceled) {
		return nil
	}
	if err != nil {
		return err
	}
	if err = peer.overlay.Answer(ctx, msg.ID, resp); errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func (s *overlaySubscription) answerRLDPQuery(peer *overlayPeer, transferID []byte, query *rldp.Query) error {
	if !s.node.beginInbound() {
		return nil
	}
	defer s.node.finishInbound()

	if peer.noteReceive() {
		s.peerPromoted(peer)
	}

	if err := s.admitInboundQuery(query.Data, time.Now()); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(s.node.runCtx, peerQueryTimeout)
	defer cancel()

	resp, err := s.handlePeerQuery(ctx, peer.addr, query.Data)
	if errors.Is(err, errOverlayInactive) {
		s.sendForgetPeer(ctx, peer)
	}
	if errors.Is(err, context.Canceled) {
		return nil
	}
	if err != nil {
		return err
	}
	err = peer.rldpOverlay.SendAnswer(
		ctx,
		query.MaxAnswerSize,
		query.Timeout,
		query.ID,
		transferID,
		resp,
	)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func (s *overlaySubscription) admitInboundQuery(req tl.Serializable, now time.Time) error {
	if s.spec.Kind != overlayKindFastSync {
		return nil
	}

	if !s.node.fastSyncQueryLimiter.Allow(req, now) {
		return errFastSyncQueryRateLimited
	}
	return nil
}

func (s *overlaySubscription) handlePeerQuery(
	ctx context.Context,
	peerAddr string,
	req any,
) (tl.Serializable, error) {
	startedAt := time.Now()
	// Event.Type only evaluates the %T reflection when the event is enabled,
	// keeping the disabled trace/debug paths free of per-query fmt.Sprintf.
	queryLog := s.log.Trace().
		Type("kind", req).
		Str("peer", peerAddr)
	queryLog.Msg("received overlay query")

	if !s.isActive() {
		return nil, errOverlayInactive
	}

	resp, err := s.dispatchPeerQuery(ctx, req)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			logEvt := s.log.Debug().
				Err(err).
				Type("kind", req).
				Str("peer", peerAddr)
			logEvt.Msg("failed to answer overlay query")
		}
		return nil, err
	}
	answerLog := s.log.Trace().
		Type("kind", req).
		Type("response", resp).
		Dur("elapsed", time.Since(startedAt)).
		Str("peer", peerAddr)
	answerLog.Msg("answering overlay query")
	return resp, nil
}

func (s *overlaySubscription) sendForgetPeer(ctx context.Context, peer *overlayPeer) {
	_ = peer.broadcastPeer.SendCustomMessage(ctx, ForgetPeer{})
	s.removePeerIfCurrent(peer)
}

func (s *overlaySubscription) dispatchPeerQuery(ctx context.Context, req any) (tl.Serializable, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	switch req.(type) {
	case overlay.Ping:
		return overlay.Pong{}, nil
	}

	if s.spec.Kind == overlayKindCustomFixed {
		if _, ok := req.(overlay.GetRandomPeers); ok {
			return nil, errors.New("overlay is private")
		}
		if !s.spec.AcceptQueries {
			return nil, errors.New("this node does not accept queries")
		}
	}

	switch query := req.(type) {
	case overlay.GetRandomPeers:
		return s.handleGetRandomPeers(ctx, query), nil
	case overlay.GetRandomPeersV2:
		if s.fastSync == nil {
			return nil, errors.New("overlay.getRandomPeersV2 requires a FastSync overlay")
		}
		return s.handleFastSyncRandomPeers(query)
	case GetCapabilities:
		return Capabilities{
			VersionMajor: s.spec.ProtoVersionMajor,
			VersionMinor: s.spec.ProtoVersionMinor,
			Flags:        0,
		}, nil
	case tonnodeapi.DownloadBlockFull:
		return s.serveBlockFull(ctx, query.Block)
	case GetNextBlockDescription:
		return s.serveNextBlockDescription(ctx, query.PrevBlock)
	case DownloadNextBlockFull:
		return s.serveNextBlockFull(ctx, query.PrevBlock)
	case DownloadNextBlocksFull:
		return s.serveNextBlocksFull(ctx, query.PrevBlock, query.MaxBlocks)
	case tonnodeapi.DownloadBlock:
		return s.serveBlockData(ctx, query.Block)
	case PrepareBlock:
		return s.servePrepareBlock(ctx, query.Block)
	case PrepareBlockProof:
		return s.servePrepareBlockProof(ctx, query.Block, query.AllowPartial)
	case PrepareKeyBlockProof:
		return s.servePrepareKeyBlockProof(ctx, query.Block, query.AllowPartial)
	case DownloadBlockProof:
		return s.serveProofData(ctx, tnstore.ServedProofBlock, query.Block)
	case DownloadKeyBlockProof:
		return s.serveProofData(ctx, tnstore.ServedProofKeyBlock, query.Block)
	case DownloadBlockProofLink:
		return s.serveProofData(ctx, tnstore.ServedProofBlockLink, query.Block)
	case DownloadKeyBlockProofLink:
		return s.serveKeyBlockProofLink(ctx, query.Block)
	case PrepareZeroState:
		return s.servePrepareZeroState(ctx, query.Block)
	case DownloadZeroState:
		return s.serveZeroStateData(ctx, query.Block)
	case GetNextKeyBlockIDs:
		return s.serveNextKeyBlockIDs(ctx, query.Block, query.MaxSize)
	case SendExtMessage:
		return nil, errors.New("query not from full-node master")
	case GetOutMsgQueueProof:
		return nil, errors.New("not supported yet")
	case PreparePersistentState:
		return s.servePreparePersistentState(ctx, query.Block, query.MasterchainBlock)
	case DownloadPersistentStateSliceV2:
		return s.servePersistentStateSlice(ctx, query.State, query.Offset, query.MaxSize)
	case GetPersistentStateSizeV2:
		return s.servePersistentStateSize(ctx, query.State)
	case GetArchiveInfo:
		return s.serveArchiveInfo(ctx, query.MasterchainSeqno, -1, topShard)
	case GetShardArchiveInfo:
		return s.serveArchiveInfo(ctx, query.MasterchainSeqno, query.ShardPrefix.Workchain, query.ShardPrefix.Shard)
	case GetArchiveSlice:
		return s.serveArchiveSlice(ctx, query.ArchiveID, query.Offset, query.MaxSize)
	default:
		return nil, fmt.Errorf("unsupported peer query %T", req)
	}
}

func (s *overlaySubscription) serveBlockFull(ctx context.Context, block ton.BlockIDExt) (tl.Serializable, error) {
	full, err := s.node.localServedBlockFull(ctx, block)
	if errors.Is(err, tnstore.ErrNotFound) {
		return tonnodeapi.DataFullEmpty{}, nil
	}
	if err != nil {
		return nil, err
	}

	return tonnodeapi.DataFull{
		ID:     full.ID,
		Proof:  full.Proof,
		Block:  full.Block,
		IsLink: full.IsLink,
	}, nil
}

func (s *overlaySubscription) serveNextBlockDescription(ctx context.Context, prev ton.BlockIDExt) (tl.Serializable, error) {
	if !isMasterchainBlock(prev) {
		return nil, errors.New("next block allowed only for masterchain")
	}

	meta, err := s.localNextBlockMeta(ctx, prev)
	if errors.Is(err, tnstore.ErrNotFound) {
		return BlockDescriptionEmpty{}, nil
	}
	if err != nil {
		return nil, err
	}

	return BlockDescription{ID: meta.ID}, nil
}

func (s *overlaySubscription) serveNextBlockFull(ctx context.Context, prev ton.BlockIDExt) (tl.Serializable, error) {
	full, err := s.node.localNextServedBlockFull(ctx, prev)
	if errors.Is(err, tnstore.ErrNotFound) {
		return tonnodeapi.DataFullEmpty{}, nil
	}
	if err != nil {
		return nil, err
	}

	return tonnodeapi.DataFull{
		ID:     full.ID,
		Proof:  full.Proof,
		Block:  full.Block,
		IsLink: full.IsLink,
	}, nil
}

func (s *overlaySubscription) serveBlockData(ctx context.Context, block ton.BlockIDExt) (tl.Serializable, error) {
	data, err := s.node.localBlockData(ctx, block)
	if errors.Is(err, tnstore.ErrNotFound) {
		return nil, errors.New("unknown block")
	}
	if err != nil {
		return nil, err
	}
	return tl.Raw(data), nil
}

func (s *overlaySubscription) servePrepareBlock(ctx context.Context, block ton.BlockIDExt) (tl.Serializable, error) {
	meta, err := s.localBlockMeta(ctx, block, tnstore.BlockMetaHasBlockData)
	if err != nil {
		if errors.Is(err, tnstore.ErrNotFound) {
			return NotFound{}, nil
		}
		return nil, err
	}
	if !meta.Has(tnstore.BlockMetaHasBlockData) {
		return NotFound{}, nil
	}

	return Prepared{}, nil
}

func (s *overlaySubscription) servePrepareBlockProof(ctx context.Context, block ton.BlockIDExt, allowPartial bool) (tl.Serializable, error) {
	if block.SeqNo == 0 {
		return nil, errors.New("cannot download proof for zero state")
	}

	meta, err := s.localBlockMeta(ctx, block, tnstore.BlockMetaHasProofBlock)
	if errors.Is(err, tnstore.ErrNotFound) {
		return PreparedProofEmpty{}, nil
	}
	if err != nil {
		return nil, err
	}

	if meta.Has(tnstore.BlockMetaHasProofBlock) {
		if isMasterchainBlock(block) {
			return PreparedProof{}, nil
		}
		return PreparedProofLink{}, nil
	}

	if !allowPartial {
		return PreparedProofEmpty{}, nil
	}
	if !meta.Has(tnstore.BlockMetaHasProofBlockLink) {
		return PreparedProofEmpty{}, nil
	}

	return PreparedProofLink{}, nil
}

func (s *overlaySubscription) servePrepareKeyBlockProof(ctx context.Context, block ton.BlockIDExt, allowPartial bool) (tl.Serializable, error) {
	if block.SeqNo == 0 {
		return nil, errors.New("cannot download proof for zero state")
	}

	meta, err := s.localBlockMeta(ctx, block, tnstore.BlockMetaHasProofKeyBlock)
	if errors.Is(err, tnstore.ErrNotFound) {
		return PreparedProofEmpty{}, nil
	}
	if err != nil {
		return nil, err
	}

	if allowPartial && (meta.Has(tnstore.BlockMetaHasProofKeyBlock) ||
		meta.Has(tnstore.BlockMetaHasProofKeyBlockLink)) {
		return PreparedProofLink{}, nil
	}
	if meta.Has(tnstore.BlockMetaHasProofKeyBlock) {
		return PreparedProof{}, nil
	}

	return PreparedProofEmpty{}, nil
}

func (s *overlaySubscription) serveKeyBlockProofLink(ctx context.Context, block ton.BlockIDExt) (tl.Serializable, error) {
	if block.SeqNo == 0 {
		return nil, errors.New("cannot download proof for zero state")
	}

	link, err := s.node.localBlockProof(ctx, tnstore.ServedProofKeyBlockLink, block)
	if err == nil {
		return tl.Raw(link), nil
	}
	if !errors.Is(err, tnstore.ErrNotFound) {
		return nil, err
	}

	proof, err := s.node.localBlockProof(ctx, tnstore.ServedProofKeyBlock, block)
	if errors.Is(err, tnstore.ErrNotFound) {
		return nil, errors.New("unknown block proof")
	}
	if err != nil {
		return nil, err
	}

	link, err = blockproof.LinkBOC(block, proof)
	if err != nil {
		return nil, err
	}
	return tl.Raw(link), nil
}

func (s *overlaySubscription) serveProofData(ctx context.Context, kind tnstore.ServedProofKind, block ton.BlockIDExt) (tl.Serializable, error) {
	if block.SeqNo == 0 && isKeyBlockProofKind(kind) {
		return nil, errors.New("cannot download proof for zero state")
	}

	proof, err := s.node.localBlockProof(ctx, kind, block)
	if errors.Is(err, tnstore.ErrNotFound) {
		return nil, errors.New("unknown block proof")
	}
	if err != nil {
		return nil, err
	}
	return tl.Raw(proof), nil
}

func (s *overlaySubscription) servePrepareZeroState(ctx context.Context, block ton.BlockIDExt) (tl.Serializable, error) {
	_, err := s.node.peerStorage.ZeroStateSize(ctx, block)
	if errors.Is(err, tnstore.ErrNotFound) {
		return NotFoundState{}, nil
	}
	if err != nil {
		return nil, err
	}

	return PreparedState{}, nil
}

func (s *overlaySubscription) serveZeroStateData(ctx context.Context, block ton.BlockIDExt) (tl.Serializable, error) {
	data, err := s.node.peerStorage.ZeroState(ctx, block)
	if err != nil {
		if errors.Is(err, tnstore.ErrNotFound) {
			return nil, errors.New("failed to get state from db")
		}
		return nil, err
	}
	if len(data) == 0 {
		return nil, errors.New("failed to get state from db")
	}
	return tl.Raw(data), nil
}

func (s *overlaySubscription) servePreparePersistentState(ctx context.Context, block ton.BlockIDExt, master ton.BlockIDExt) (tl.Serializable, error) {
	_, err := s.node.peerStorage.PersistentStateSize(ctx, block, master, 0)
	if err == nil {
		return PreparedState{}, nil
	}
	if errors.Is(err, tnstore.ErrNotFound) {
		return NotFoundState{}, nil
	}
	return nil, err
}

func (s *overlaySubscription) servePersistentStateSize(ctx context.Context, state PersistentStateIDV2) (tl.Serializable, error) {
	effectiveShard := persistentStateEffectiveShardForQuery(state.Block, state.EffectiveShard)
	size, err := s.node.peerStorage.PersistentStateSize(ctx, state.Block, state.MasterchainBlock, effectiveShard)
	if errors.Is(err, tnstore.ErrNotFound) {
		return PersistentStateSizeNotFound{}, nil
	}
	if err != nil {
		return nil, err
	}
	return PersistentStateSize{Size: size}, nil
}

func (s *overlaySubscription) servePersistentStateSlice(ctx context.Context, state PersistentStateIDV2, offset int64, maxSize int64) (tl.Serializable, error) {
	if maxSize < 0 || maxSize > maxPeerSliceRequestSize {
		return nil, fmt.Errorf("invalid max_size %d", maxSize)
	}
	effectiveShard := persistentStateEffectiveShardForQuery(state.Block, state.EffectiveShard)
	data, err := s.node.peerStorage.PersistentStateSlice(ctx, state.Block, state.MasterchainBlock, effectiveShard, offset, maxSize)
	if errors.Is(err, tnstore.ErrNotFound) {
		return nil, errors.New("failed to get state from db")
	}
	if err != nil {
		return nil, err
	}
	return tl.Raw(data), nil
}

func persistentStateEffectiveShardForQuery(block ton.BlockIDExt, effectiveShard int64) int64 {
	if effectiveShard == 0 || !shardIsAncestor(block.Shard, effectiveShard) {
		return 0
	}
	return effectiveShard
}

func shardIsAncestor(shard int64, child int64) bool {
	shardPrefixLen := tnstate.ShardPrefixLength(shard)
	childPrefixLen := tnstate.ShardPrefixLength(child)
	if shardPrefixLen > childPrefixLen {
		return false
	}
	if shardPrefixLen == 0 {
		return true
	}
	mask := ^uint64(0) << (64 - shardPrefixLen)
	return uint64(shard)&mask == uint64(child)&mask
}

func (s *overlaySubscription) serveNextKeyBlockIDs(ctx context.Context, block ton.BlockIDExt, maxSize int32) (tl.Serializable, error) {
	if s.node.storage == nil || block.Workchain != -1 || block.Shard != topShard {
		return KeyBlocks{Error: true}, nil
	}

	if block.SeqNo > 0 {
		meta, err := s.node.storage.BlockMeta(ctx, block)
		if err != nil || !meta.Has(tnstore.BlockMetaIsKeyBlock) {
			return KeyBlocks{Error: true}, nil
		}
	}

	limit := int(maxSize)
	if maxSize < 0 || limit > 8 {
		limit = 8
	}
	if limit == 0 {
		return KeyBlocks{}, nil
	}

	latestSeqno, err := s.seenMasterchainSeqnoForKeyBlockScan(ctx)
	if err != nil {
		return KeyBlocks{Error: true}, nil
	}
	if latestSeqno <= block.SeqNo || block.SeqNo == ^uint32(0) {
		return KeyBlocks{Incomplete: true}, nil
	}

	blocks := make([]ton.BlockIDExt, 0, limit)
	next, err := s.node.storage.NextKeyBlocks(ctx, block.SeqNo, limit)
	if errors.Is(err, tnstore.ErrNotFound) {
		return KeyBlocks{Incomplete: true}, nil
	}
	if err != nil {
		return KeyBlocks{Error: true}, nil
	}
	for _, nextBlock := range next {
		if nextBlock.SeqNo > latestSeqno {
			break
		}
		blocks = append(blocks, nextBlock)
	}

	return KeyBlocks{
		Blocks:     blocks,
		Incomplete: len(blocks) < limit,
	}, nil
}

func (s *overlaySubscription) seenMasterchainSeqnoForKeyBlockScan(ctx context.Context) (uint32, error) {
	latest, err := s.node.SeenMasterchainBlock()
	if err == nil {
		return latest.SeqNo, nil
	}
	if err != nil && !errors.Is(err, tnstore.ErrNotFound) {
		return 0, err
	}

	current, err := s.node.storage.CurrentState(ctx)
	if err == nil && tnstore.BlockIDHashesKnown(current.Masterchain.Block) {
		return current.Masterchain.Block.SeqNo, nil
	}
	if err != nil && !errors.Is(err, tnstore.ErrNotFound) {
		return 0, err
	}
	return 0, tnstore.ErrNotFound
}

func (s *overlaySubscription) serveArchiveInfo(ctx context.Context, masterchainSeqno int32, workchain int32, shard int64) (tl.Serializable, error) {
	id, err := s.node.peerStorage.ArchiveInfo(ctx, masterchainSeqno, workchain, shard)
	if errors.Is(err, tnstore.ErrNotFound) {
		return ArchiveNotFound{}, nil
	}
	if err != nil {
		return nil, err
	}
	return ArchiveInfo{ID: id}, nil
}

func (s *overlaySubscription) serveArchiveSlice(ctx context.Context, archiveID, offset int64, maxSize int32) (tl.Serializable, error) {
	if maxSize < 0 || maxSize > maxPeerSliceRequestSize {
		return nil, fmt.Errorf("invalid archive slice max_size %d", maxSize)
	}
	data, err := s.node.peerStorage.ArchiveSlice(ctx, archiveID, offset, maxSize)
	if errors.Is(err, tnstore.ErrNotFound) {
		return nil, errors.New("unknown archive")
	}
	if err != nil {
		return nil, err
	}
	return tl.Raw(data), nil
}

func (s *overlaySubscription) handleGetRandomPeers(_ context.Context, query overlay.GetRandomPeers) overlay.NodesList {
	if len(query.List.List) > 0 && s.advertisedPeerLearning.CompareAndSwap(false, true) {
		peers := copyAdvertisedPeerCandidates(s.spec.ShortID, query.List.List)
		if len(peers) == 0 {
			s.advertisedPeerLearning.Store(false)
		} else {
			s.node.runAsync(func() {
				defer s.advertisedPeerLearning.Store(false)
				s.learnAdvertisedPeers(s.node.runCtx, peers)
			})
		}
	}

	reply, err := s.randomPeerAdvertisement()
	if err != nil {
		s.log.Debug().Err(err).Msg("failed to create getRandomPeers response")
		return overlay.NodesList{}
	}
	return reply
}

func copyAdvertisedPeerCandidates(overlayID []byte, nodes []overlay.Node) []advertisedPeerCandidate {
	count := min(len(nodes), maxPeersPerOverlay)
	peers := make([]advertisedPeerCandidate, 0, count)
	for i := 0; i < count; i++ {
		node := nodes[i]
		publicKey, ok := node.ID.(keys.PublicKeyED25519)
		if !ok ||
			len(publicKey.Key) != ed25519.PublicKeySize ||
			len(node.Overlay) != PeerIDSize ||
			!bytes.Equal(node.Overlay, overlayID) ||
			len(node.Signature) != ed25519.SignatureSize {
			continue
		}

		var peer advertisedPeerCandidate
		copy(peer.publicKey[:], publicKey.Key)
		copy(peer.overlayID[:], node.Overlay)
		copy(peer.signature[:], node.Signature)
		peer.version = node.Version
		peers = append(peers, peer)
	}
	return peers
}

func (s *overlaySubscription) learnAdvertisedPeers(ctx context.Context, peers []advertisedPeerCandidate) {
	for _, peer := range peers {
		if ctx.Err() != nil {
			return
		}
		if s.aliveKnownPeerCount() >= maxPeersPerOverlay && !s.hasPeerPublicKey(peer.publicKey) {
			continue
		}

		node := overlay.Node{
			ID:        keys.PublicKeyED25519{Key: peer.publicKey[:]},
			Overlay:   peer.overlayID[:],
			Version:   peer.version,
			Signature: peer.signature[:],
		}

		// connectOverlayNodeV1 owns the signature check; admission above uses
		// only the structurally checked public key.
		connectCtx, cancel := context.WithTimeout(ctx, peerQueryTimeout)
		_, err := s.connectOverlayNodeV1(connectCtx, node)
		cancel()
		if err != nil {
			s.log.Debug().Err(err).Msg("failed to connect peer learned from overlay query")
		}
	}
}

func (s *overlaySubscription) hasPeerPublicKey(key [ed25519.PublicKeySize]byte) bool {
	s.mx.Lock()
	defer s.mx.Unlock()

	for _, peer := range s.peers {
		if bytes.Equal(peer.pub, key[:]) {
			return true
		}
	}
	return false
}

func (s *overlaySubscription) localBlockMeta(
	ctx context.Context,
	block ton.BlockIDExt,
	required tnstore.BlockMetaFlags,
) (*tnstore.BlockMeta, error) {
	live, liveErr := s.node.liveBlockCache.BlockMeta(ctx, block)
	if liveErr == nil && live.Flags&required == required {
		return live, nil
	}
	if liveErr != nil && !errors.Is(liveErr, tnstore.ErrNotFound) {
		return nil, liveErr
	}

	stored, err := s.node.peerStorage.BlockMeta(ctx, block)
	if err == nil {
		return tnstore.MergeBlockMeta(stored, live), nil
	}
	if !errors.Is(err, tnstore.ErrNotFound) {
		return nil, err
	}
	if liveErr == nil {
		return live, nil
	}

	return nil, tnstore.ErrNotFound
}

func (s *overlaySubscription) localNextBlockMeta(
	ctx context.Context,
	prev ton.BlockIDExt,
) (*tnstore.BlockMeta, error) {
	livePrev, err := s.node.liveBlockCache.BlockMeta(ctx, prev)
	if err == nil && len(livePrev.NextRefs) > 0 {
		liveNext, nextErr := s.node.liveBlockCache.BlockMeta(ctx, livePrev.NextRefs[0])
		if nextErr == nil &&
			liveNext.Has(tnstore.BlockMetaHasBlockData) &&
			liveNext.Has(tnstore.BlockMetaHasProofBlock) {
			return liveNext, nil
		}
		if nextErr != nil && !errors.Is(nextErr, tnstore.ErrNotFound) {
			return nil, nextErr
		}
	}
	if err != nil && !errors.Is(err, tnstore.ErrNotFound) {
		return nil, err
	}

	storedPrev, err := s.node.peerStorage.BlockMeta(ctx, prev)
	if err != nil {
		return nil, err
	}
	if len(storedPrev.NextRefs) == 0 {
		return nil, tnstore.ErrNotFound
	}

	storedNext, err := s.node.peerStorage.BlockMeta(ctx, storedPrev.NextRefs[0])
	if err != nil {
		return nil, err
	}
	if !storedNext.Has(tnstore.BlockMetaHasBlockData) ||
		!storedNext.Has(tnstore.BlockMetaHasProofBlock) {
		return nil, tnstore.ErrNotFound
	}

	return storedNext, nil
}

func isMasterchainBlock(block ton.BlockIDExt) bool {
	return block.Workchain == -1 && block.Shard == topShard
}

func isKeyBlockProofKind(kind tnstore.ServedProofKind) bool {
	return kind == tnstore.ServedProofKeyBlock || kind == tnstore.ServedProofKeyBlockLink
}
