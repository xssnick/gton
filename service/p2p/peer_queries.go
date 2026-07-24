package p2p

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xssnick/gton/service/archive"
	"github.com/xssnick/gton/service/blockproof"
	tnstate "github.com/xssnick/gton/service/state"
	tnstore "github.com/xssnick/gton/service/storage"
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

	if peer != nil && peer.noteReceive() {
		s.peerPromoted(peer)
	}
	ctx, cancel := context.WithTimeout(s.node.runCtx, peerQueryTimeout)
	defer cancel()

	startedAt := time.Now()
	// Event.Type only evaluates the %T reflection when the event is enabled,
	// keeping the disabled trace/debug paths free of per-query fmt.Sprintf.
	queryLog := s.log.Trace().
		Type("kind", req)
	if peer != nil {
		queryLog = queryLog.Str("peer", peer.addr)
	}
	queryLog.Msg("received overlay query")

	if !s.isActive() {
		s.sendForgetPeer(ctx, peer)
		return errors.New("shard is inactive")
	}

	resp, err := s.dispatchPeerQuery(ctx, req)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		logEvt := s.log.Debug().
			Err(err).
			Type("kind", req)
		if peer != nil {
			logEvt = logEvt.Str("peer", peer.addr)
		}
		logEvt.Msg("failed to answer overlay query")
		return err
	}
	answerLog := s.log.Trace().
		Type("kind", req).
		Type("response", resp).
		Dur("elapsed", time.Since(startedAt))
	if peer != nil {
		answerLog = answerLog.Str("peer", peer.addr)
	}
	answerLog.Msg("answering overlay query")

	if err = answer(ctx, resp); errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func (s *overlaySubscription) sendForgetPeer(ctx context.Context, peer *overlayPeer) {
	if peer == nil || peer.overlay == nil {
		return
	}
	_ = peer.overlay.SendCustomMessage(ctx, ForgetPeer{})
	s.removePeerIfCurrent(peer)
}

func (s *overlaySubscription) dispatchPeerQuery(ctx context.Context, req any) (tl.Serializable, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if s.spec.Kind == overlayKindCustomFixed {
		switch req.(type) {
		case overlay.GetRandomPeers:
			return nil, errors.New("overlay is private")
		default:
			return nil, errors.New("overlay query is not supported in private overlay")
		}
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
	case GetNextBlockDescription:
		return s.serveNextBlockDescription(ctx, query.PrevBlock)
	case DownloadNextBlockFull:
		return s.serveNextBlockFull(ctx, query.PrevBlock)
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
		return s.serveBlockProofLink(ctx, query.Block)
	case DownloadKeyBlockProofLink:
		return s.serveKeyBlockProofLink(ctx, query.Block)
	case PrepareZeroState:
		return s.servePrepareZeroState(ctx, query.Block)
	case DownloadZeroState:
		return s.serveZeroStateData(ctx, query.Block)
	case GetNextKeyBlockIDs:
		return s.serveNextKeyBlockIDs(ctx, query.Block, query.MaxSize)
	case SendExtMessage:
		return s.serveSendExtMessage(ctx, query.Message)
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
	if !s.isMasterchainOverlay() && !isMasterchainBlock(prev) {
		return nil, errors.New("next block allowed only for masterchain")
	}

	full, err := s.node.localNextServedBlockFull(ctx, prev)
	if errors.Is(err, tnstore.ErrNotFound) {
		return BlockDescriptionEmpty{}, nil
	}
	if err != nil {
		return nil, err
	}
	return BlockDescription{ID: full.ID}, nil
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
	_, err := s.node.localBlockData(ctx, block)
	if err == nil {
		return Prepared{}, nil
	}

	if !errors.Is(err, tnstore.ErrNotFound) {
		return nil, err
	}

	return NotFound{}, nil
}

func (s *overlaySubscription) servePrepareBlockProof(ctx context.Context, block ton.BlockIDExt, allowPartial bool) (tl.Serializable, error) {
	if block.SeqNo == 0 {
		return nil, errors.New("cannot download proof for zero state")
	}

	hasFull, err := s.hasStoredProof(ctx, tnstore.ServedProofBlock, block)
	if err != nil {
		return nil, err
	}
	if isMasterchainBlock(block) {
		if hasFull {
			return PreparedProof{}, nil
		}
		if allowPartial {
			hasLink, err := s.hasStoredProof(ctx, tnstore.ServedProofBlockLink, block)
			if err != nil {
				return nil, err
			}
			if hasLink {
				return PreparedProofLink{}, nil
			}
		}
		return PreparedProofEmpty{}, nil
	}

	if !allowPartial {
		return PreparedProofEmpty{}, nil
	}

	hasLink, err := s.hasStoredProof(ctx, tnstore.ServedProofBlockLink, block)
	if err != nil {
		return nil, err
	}
	if hasLink {
		return PreparedProofLink{}, nil
	}

	return PreparedProofEmpty{}, nil
}

func (s *overlaySubscription) servePrepareKeyBlockProof(ctx context.Context, block ton.BlockIDExt, allowPartial bool) (tl.Serializable, error) {
	if block.SeqNo == 0 {
		return nil, errors.New("cannot download proof for zero state")
	}

	hasFull, err := s.hasStoredProof(ctx, tnstore.ServedProofKeyBlock, block)
	if err != nil {
		return nil, err
	}
	if !hasFull {
		if allowPartial {
			hasLink, err := s.hasStoredProof(ctx, tnstore.ServedProofKeyBlockLink, block)
			if err != nil {
				return nil, err
			}
			if hasLink {
				return PreparedProofLink{}, nil
			}
		}
		return PreparedProofEmpty{}, nil
	}
	if allowPartial {
		return PreparedProofLink{}, nil
	}
	return PreparedProof{}, nil
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

func (s *overlaySubscription) serveBlockProofLink(ctx context.Context, block ton.BlockIDExt) (tl.Serializable, error) {
	if isMasterchainBlock(block) {
		link, err := s.node.localBlockProof(ctx, tnstore.ServedProofBlockLink, block)
		if err == nil {
			return tl.Raw(link), nil
		}
		if !errors.Is(err, tnstore.ErrNotFound) {
			return nil, err
		}

		proof, err := s.node.localBlockProof(ctx, tnstore.ServedProofBlock, block)
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

	return s.serveProofData(ctx, tnstore.ServedProofBlockLink, block)
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
	data, err := s.node.peerStorage.ZeroState(ctx, block)
	if errors.Is(err, tnstore.ErrNotFound) {
		return NotFoundState{}, nil
	}
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return NotFoundState{}, nil
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
	size, err := s.node.peerStorage.PersistentStateSize(ctx, block, master, 0)
	if err == nil && size > 0 {
		return PreparedState{}, nil
	}
	if err == nil {
		return NotFoundState{}, nil
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
	if maxSize < 0 || maxSize > 1<<24 {
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
	if maxSize < 0 || maxSize > 1<<24 {
		return nil, fmt.Errorf("invalid archive slice max_size %d", maxSize)
	}
	data, err := s.node.peerStorage.ArchiveSlice(ctx, archiveID, offset, maxSize)
	if errors.Is(err, tnstore.ErrNotFound) {
		return nil, errors.New("unknown archive")
	}
	if err != nil {
		return nil, err
	}
	if maxSize > 0 && len(data) > int(maxSize) {
		data = data[:maxSize]
	}
	return tl.Raw(data), nil
}

func (s *overlaySubscription) handleGetRandomPeers(_ context.Context, query overlay.GetRandomPeers) overlay.NodesList {
	if len(query.List.List) > 0 {
		go s.learnAdvertisedPeers(query.List.List)
	}

	reply, err := s.randomPeerAdvertisement()
	if err != nil {
		s.log.Debug().Err(err).Msg("failed to create getRandomPeers response")
		return overlay.NodesList{}
	}
	return reply
}

func (s *overlaySubscription) learnAdvertisedPeers(peers []overlay.Node) {
	for _, peer := range peers {
		if !s.canLearnAdvertisedPeer(peer) {
			continue
		}

		connectCtx, cancel := context.WithTimeout(s.node.runCtx, 10*time.Second)
		_, err := s.connectOverlayNodeV1(connectCtx, peer)
		cancel()
		if err != nil {
			s.log.Debug().Err(err).Msg("failed to connect peer learned from overlay query")
		}
	}
}

func (s *overlaySubscription) hasStoredProof(ctx context.Context, kind tnstore.ServedProofKind, block ton.BlockIDExt) (bool, error) {
	_, err := s.node.localBlockProof(ctx, kind, block)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, tnstore.ErrNotFound) {
		return false, nil
	}
	return false, err
}

func (s *overlaySubscription) isMasterchainOverlay() bool {
	return s.spec.Name == "masterchain"
}

func isMasterchainBlock(block ton.BlockIDExt) bool {
	return block.Workchain == -1 && block.Shard == topShard
}

func isKeyBlockProofKind(kind tnstore.ServedProofKind) bool {
	return kind == tnstore.ServedProofKeyBlock || kind == tnstore.ServedProofKeyBlockLink
}
