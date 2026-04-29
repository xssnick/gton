package p2p

import (
	"context"
	"errors"
	"flexserver/service/archive"
	tnstore "flexserver/service/storage"
	"fmt"
	"time"

	"github.com/xssnick/tonutils-go/adnl"
	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/adnl/rldp"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

func init() {
	tl.Register(PreparedProofEmpty{}, "tonNode.preparedProofEmpty = tonNode.PreparedProof")
	tl.Register(PreparedProof{}, "tonNode.preparedProof = tonNode.PreparedProof")
	tl.Register(PreparedProofLink{}, "tonNode.preparedProofLink = tonNode.PreparedProof")
	tl.Register(Prepared{}, "tonNode.prepared = tonNode.Prepared")
	tl.Register(NotFound{}, "tonNode.notFound = tonNode.Prepared")
	tl.Register(Capabilities{}, "tonNode.capabilities#f5bf60c0 version_major:int version_minor:int flags:# = tonNode.Capabilities")
	tl.Register(ArchiveNotFound{}, "tonNode.archiveNotFound = tonNode.ArchiveInfo")
	tl.Register(ArchiveInfo{}, "tonNode.archiveInfo id:long = tonNode.ArchiveInfo")
	tl.Register(archive.ShardID{}, "tonNode.shardId workchain:int shard:long = tonNode.ShardId")
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
	MasterchainSeqno int32           `tl:"int"`
	ShardPrefix      archive.ShardID `tl:"struct"`
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

func (s *overlaySubscription) answerADNLQuery(peer *overlayPeer, msg *adnl.MessageQuery) error {
	return s.answerPeerQuery(peer, msg.Data, func(ctx context.Context, resp tl.Serializable) error {
		return peer.overlay.Answer(ctx, msg.ID, resp)
	})
}

func (s *overlaySubscription) answerRLDPQuery(peer *overlayPeer, transferID []byte, query *rldp.Query) error {
	return s.answerPeerQuery(peer, query.Data, func(ctx context.Context, resp tl.Serializable) error {
		return peer.rldpOverlay.SendAnswer(ctx, query.MaxAnswerSize, query.Timeout, query.ID, transferID, resp)
	})
}

func (s *overlaySubscription) answerPeerQuery(peer *overlayPeer, req any, answer func(context.Context, tl.Serializable) error) error {
	if !s.node.beginInbound() {
		return nil
	}
	defer s.node.finishInbound()

	if peer != nil {
		peer.noteReceive()
	}
	parent := context.Background()
	if s.node.runCtx != nil {
		parent = s.node.runCtx
	}
	ctx, cancel := context.WithTimeout(parent, peerQueryTimeout)
	defer cancel()

	resp, err := s.dispatchPeerQuery(ctx, peer, req)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		logEvt := s.log.Debug().
			Err(err).
			Str("kind", queryTypeName(req))
		if peer != nil {
			logEvt = logEvt.Str("peer", peer.addr)
		}
		logEvt.Msg("failed to answer overlay query")
		return err
	}
	if err = answer(ctx, resp); errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func (s *overlaySubscription) dispatchPeerQuery(ctx context.Context, peer *overlayPeer, req any) (tl.Serializable, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	switch query := req.(type) {
	case overlay.GetRandomPeers:
		return s.handleGetRandomPeers(ctx, query), nil
	case GetCapabilities:
		return Capabilities{
			VersionMajor: s.spec.ProtoVersionMajor,
			VersionMinor: s.spec.ProtoVersionMinor,
			Flags:        0,
		}, nil
	case tonnodeapi.DownloadBlockFull:
		return s.serveBlockFull(ctx, query.Block)
	case DownloadNextBlockFull:
		return s.serveNextBlockFull(ctx, query.PrevBlock)
	case tonnodeapi.DownloadBlock:
		return s.serveBlockData(ctx, query.Block)
	case PrepareBlock:
		return s.servePrepareBlock(ctx, query.Block)
	case PrepareBlockProof:
		return s.servePrepareProof(ctx, query.Block, query.AllowPartial, tnstore.ServedProofBlock, tnstore.ServedProofBlockLink)
	case PrepareKeyBlockProof:
		return s.servePrepareProof(ctx, query.Block, query.AllowPartial, tnstore.ServedProofKeyBlock, tnstore.ServedProofKeyBlockLink)
	case DownloadBlockProof:
		return s.serveProofData(ctx, tnstore.ServedProofBlock, query.Block)
	case DownloadKeyBlockProof:
		return s.serveProofData(ctx, tnstore.ServedProofKeyBlock, query.Block)
	case DownloadBlockProofLink:
		return s.serveProofData(ctx, tnstore.ServedProofBlockLink, query.Block)
	case DownloadKeyBlockProofLink:
		return s.serveProofData(ctx, tnstore.ServedProofKeyBlockLink, query.Block)
	case PrepareZeroState, PreparePersistentState:
		return NotFoundState{}, nil
	case DownloadZeroState, DownloadPersistentStateSliceV2:
		return TonNodeData{}, nil
	case GetPersistentStateSizeV2:
		return PersistentStateSizeNotFound{}, nil
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
	full, err := s.node.peerStorage.BlockFull(ctx, block)
	if errors.Is(err, tnstore.ErrNotFound) {
		return tonnodeapi.DataFullEmpty{}, nil
	}
	if err != nil {
		return nil, err
	}

	return tonnodeapi.DataFull{
		ID:     full.ID,
		Proof:  append([]byte(nil), full.Proof...),
		Block:  append([]byte(nil), full.Block...),
		IsLink: full.IsLink,
	}, nil
}

func (s *overlaySubscription) serveNextBlockFull(ctx context.Context, prev ton.BlockIDExt) (tl.Serializable, error) {
	full, err := s.node.peerStorage.NextBlockFull(ctx, prev)
	if errors.Is(err, tnstore.ErrNotFound) {
		return tonnodeapi.DataFullEmpty{}, nil
	}
	if err != nil {
		return nil, err
	}

	return tonnodeapi.DataFull{
		ID:     full.ID,
		Proof:  append([]byte(nil), full.Proof...),
		Block:  append([]byte(nil), full.Block...),
		IsLink: full.IsLink,
	}, nil
}

func (s *overlaySubscription) serveBlockData(ctx context.Context, block ton.BlockIDExt) (tl.Serializable, error) {
	data, err := s.loadStoredBlockData(ctx, block)
	if errors.Is(err, tnstore.ErrNotFound) {
		return TonNodeData{}, nil
	}
	if err != nil {
		return nil, err
	}
	return TonNodeData{Data: data}, nil
}

func (s *overlaySubscription) servePrepareBlock(ctx context.Context, block ton.BlockIDExt) (tl.Serializable, error) {
	_, err := s.loadStoredBlockData(ctx, block)
	if err == nil {
		return Prepared{}, nil
	}

	if !errors.Is(err, tnstore.ErrNotFound) {
		return nil, err
	}

	return NotFound{}, nil
}

func (s *overlaySubscription) servePrepareProof(ctx context.Context, block ton.BlockIDExt, allowPartial bool, fullKind tnstore.ServedProofKind, linkKind tnstore.ServedProofKind) (tl.Serializable, error) {
	_, err := s.node.peerStorage.BlockProof(ctx, fullKind, block)
	if err == nil {
		return PreparedProof{}, nil
	}

	if !errors.Is(err, tnstore.ErrNotFound) {
		return nil, err
	}

	if allowPartial {
		_, err = s.node.peerStorage.BlockProof(ctx, linkKind, block)
		if err == nil {
			return PreparedProofLink{}, nil
		}

		if !errors.Is(err, tnstore.ErrNotFound) {
			return nil, err
		}
	}

	return PreparedProofEmpty{}, nil
}

func (s *overlaySubscription) serveProofData(ctx context.Context, kind tnstore.ServedProofKind, block ton.BlockIDExt) (tl.Serializable, error) {
	proof, err := s.node.peerStorage.BlockProof(ctx, kind, block)
	if errors.Is(err, tnstore.ErrNotFound) {
		return TonNodeData{}, nil
	}
	if err != nil {
		return nil, err
	}
	return TonNodeData{Data: proof}, nil
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
	if maxSize < 0 || maxSize > 1<<24 {
		return nil, fmt.Errorf("invalid archive slice max_size %d", maxSize)
	}
	data, err := s.node.peerStorage.ArchiveSlice(ctx, archiveID, offset, maxSize)
	if errors.Is(err, tnstore.ErrNotFound) {
		return TonNodeData{}, nil
	}
	if err != nil {
		return nil, err
	}
	if maxSize > 0 && len(data) > int(maxSize) {
		data = data[:maxSize]
	}
	return TonNodeData{Data: data}, nil
}

func (s *overlaySubscription) handleGetRandomPeers(_ context.Context, query overlay.GetRandomPeers) overlay.NodesList {
	if len(query.List.List) > 0 && s.node.runCtx != nil {
		go s.learnAdvertisedPeers(query.List.List)
	}

	reply := make([]overlay.Node, 0, maxRandomPeerReply)
	if self, err := s.node.selfOverlayNode(s.spec); err == nil {
		reply = append(reply, *self)
	}

	for _, node := range s.overlayNodesSnapshot() {
		if len(reply) >= maxRandomPeerReply {
			break
		}
		reply = append(reply, node)
	}

	return overlay.NodesList{List: reply}
}

func (s *overlaySubscription) learnAdvertisedPeers(peers []overlay.Node) {
	if s.node.runCtx == nil {
		return
	}

	for _, peer := range peers {
		if s.aliveKnownPeerCount() >= maxPeersPerOverlay {
			return
		}

		connectCtx, cancel := context.WithTimeout(s.node.runCtx, 10*time.Second)
		_, err := s.connectOverlayNodeV1(connectCtx, peer)
		cancel()
		if err != nil {
			s.log.Debug().Err(err).Msg("failed to connect peer learned from overlay query")
		}
	}
}

func queryTypeName(query any) string {
	return fmt.Sprintf("%T", query)
}

func (s *overlaySubscription) loadStoredBlockData(ctx context.Context, block ton.BlockIDExt) ([]byte, error) {
	data, err := s.node.peerStorage.BlockData(ctx, block)
	if err == nil {
		return data, nil
	}
	if !errors.Is(err, tnstore.ErrNotFound) {
		return nil, err
	}

	full, err := s.node.peerStorage.BlockFull(ctx, block)
	if err != nil {
		return nil, err
	}
	return full.Block, nil
}
