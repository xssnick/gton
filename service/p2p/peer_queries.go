package p2p

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"time"

	"github.com/xssnick/gton/service/blockproof"
	sharddomain "github.com/xssnick/gton/service/shard"
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
	forgetPeerConstructorID = tl.Register(ForgetPeer{}, "tonNode.forgetPeer = tonNode.ForgetPeer")
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

var errOverlayInactive = errors.New("overlay is inactive")
var forgetPeerConstructorID uint32

// inboundQuery is one served query, independent of whether the sender is
// attached to this overlay. A detached sender carries no wrappers, so the
// liveness row and the answer transport are supplied separately.
type inboundQuery struct {
	source PeerID
	addr   string
	row    *overlayPeer              // liveness row; nil when unknown
	forget func(ctx context.Context) // told the overlay is inactive
	answer func(ctx context.Context, resp tl.Serializable) error
}

// serveInboundQuery is the shared body of every inbound query path: admission,
// liveness, dispatch and the answer. Keeping detached senders on exactly this
// path is what makes it safe to stop attaching every peer — an unattached peer
// must still be served, or it marks us unreliable and drops us.
func (s *overlaySubscription) serveInboundQuery(query inboundQuery, data any) error {
	if !s.node.beginInbound() {
		return nil
	}
	defer s.node.finishInbound()

	if query.row != nil && query.row.noteReceive() {
		s.peerPromoted(query.row)
	}

	ctx, cancel := context.WithTimeout(s.node.runCtx, peerQueryTimeout)
	defer cancel()

	resp, err := s.handlePeerQueryFrom(ctx, query.source, query.addr, data)
	if errors.Is(err, errOverlayInactive) && query.forget != nil {
		query.forget(ctx)
	}
	if errors.Is(err, context.Canceled) {
		return nil
	}
	if err != nil {
		return err
	}
	if err = query.answer(ctx, resp); errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

// serveDetachedADNLQuery answers a query from a peer that holds no attachment to
// this overlay. Without it, a peer we are not attached to is silently ignored:
// tonutils drops the query with "got query for unregistered overlay", the peer
// never gets its blocks, proofs or Pong, marks us unreliable and evicts us.
func (s *overlaySubscription) serveDetachedADNLQuery(pooled *pooledPeer, msg *adnl.MessageQuery, unwrapped tl.Serializable) error {
	s.noteDirectoryActivity(pooled.id, pooled.addr)
	return s.serveInboundQuery(inboundQuery{
		source: pooled.id,
		addr:   pooled.addr,
		row:    s.rosterRow(pooled.id),
		forget: func(ctx context.Context) { s.sendDetachedForgetPeer(ctx, pooled) },
		answer: func(ctx context.Context, resp tl.Serializable) error {
			return pooled.adnl.Answer(ctx, msg.ID, resp)
		},
	}, unwrapped)
}

func (s *overlaySubscription) serveDetachedRLDPQuery(pooled *pooledPeer, transferID []byte, query *rldp.Query, unwrapped tl.Serializable) error {
	s.noteDirectoryActivity(pooled.id, pooled.addr)
	return s.serveInboundQuery(inboundQuery{
		source: pooled.id,
		addr:   pooled.addr,
		row:    s.rosterRow(pooled.id),
		forget: func(ctx context.Context) { s.sendDetachedForgetPeer(ctx, pooled) },
		answer: func(ctx context.Context, resp tl.Serializable) error {
			return pooled.rldp.SendAnswer(
				ctx,
				query.MaxAnswerSize,
				query.Timeout,
				query.ID,
				transferID,
				resp,
			)
		},
	}, unwrapped)
}

// sendDetachedForgetPeer tells an unattached peer to drop us, over its pooled
// transport: the attached path goes through the overlay broadcast peer, which a
// detached sender does not have.
func (s *overlaySubscription) sendDetachedForgetPeer(ctx context.Context, pooled *pooledPeer) {
	_ = pooled.adnl.SendCustomMessage(ctx, overlay.WrapMessage(s.spec.ShortID, ForgetPeer{}))
}

func (s *overlaySubscription) rosterRow(id PeerID) *overlayPeer {
	s.mx.Lock()
	defer s.mx.Unlock()

	return s.peers[id]
}

func (s *overlaySubscription) answerADNLQuery(peer *overlayPeer, msg *adnl.MessageQuery) error {
	return s.serveInboundQuery(inboundQuery{
		source: peer.id,
		addr:   peer.addr,
		row:    peer,
		forget: func(ctx context.Context) { s.sendForgetPeer(ctx, peer) },
		answer: func(ctx context.Context, resp tl.Serializable) error {
			return peer.overlay.Answer(ctx, msg.ID, resp)
		},
	}, msg.Data)
}

func (s *overlaySubscription) answerRLDPQuery(peer *overlayPeer, transferID []byte, query *rldp.Query) error {
	return s.serveInboundQuery(inboundQuery{
		source: peer.id,
		addr:   peer.addr,
		row:    peer,
		forget: func(ctx context.Context) { s.sendForgetPeer(ctx, peer) },
		answer: func(ctx context.Context, resp tl.Serializable) error {
			return peer.rldpOverlay.SendAnswer(
				ctx,
				query.MaxAnswerSize,
				query.Timeout,
				query.ID,
				transferID,
				resp,
			)
		},
	}, query.Data)
}

func (s *overlaySubscription) handlePeerQuery(
	ctx context.Context,
	peerAddr string,
	req any,
) (tl.Serializable, error) {
	return s.handlePeerQueryFrom(ctx, PeerID{}, peerAddr, req)
}

func (s *overlaySubscription) handlePeerQueryFrom(
	ctx context.Context,
	source PeerID,
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

	resp, err := s.dispatchPeerQueryFrom(ctx, source, peerAddr, req)
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
	return s.dispatchPeerQueryFrom(ctx, PeerID{}, "", req)
}

func (s *overlaySubscription) dispatchPeerQueryFrom(
	ctx context.Context,
	source PeerID,
	peerAddr string,
	req any,
) (tl.Serializable, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	switch req.(type) {
	case overlay.Ping:
		return overlay.Pong{}, nil
	}

	if s.spec.privatePeerRoster() {
		if _, ok := req.(overlay.GetRandomPeers); ok {
			return nil, errors.New("overlay is private")
		}
	}
	if s.private != nil {
		if !s.private.begin() {
			return nil, ErrPrivateOverlayClosed
		}
		defer s.private.done()

		if s.private.callbacks.Query == nil {
			return nil, ErrPrivateOverlayHandlerUnavailable
		}
		return s.private.callbacks.Query(ctx, source, req)
	}
	if s.spec.enforcesAcceptQueries() && !s.spec.AcceptQueries {
		return nil, errors.New("this node does not accept queries")
	}

	switch query := req.(type) {
	case overlay.GetRandomPeers:
		return s.handleGetRandomPeers(ctx, source, peerAddr, query), nil
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
	if effectiveShard == 0 || !sharddomain.Contains(block.Shard, effectiveShard) {
		return 0
	}
	return effectiveShard
}

func (s *overlaySubscription) serveNextKeyBlockIDs(ctx context.Context, block ton.BlockIDExt, maxSize int32) (tl.Serializable, error) {
	if block.Workchain != -1 || block.Shard != topShard {
		return KeyBlocks{Error: true}, nil
	}

	if block.SeqNo > 0 {
		meta, err := s.node.peerStorage.BlockMeta(ctx, block)
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
	next, err := s.node.peerStorage.NextKeyBlocks(ctx, block.SeqNo, limit)
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

	current, err := s.node.peerStorage.CurrentState(ctx)
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

func (s *overlaySubscription) handleGetRandomPeers(
	_ context.Context,
	source PeerID,
	peerAddr string,
	query overlay.GetRandomPeers,
) overlay.NodesList {
	advertised := boundedAdvertisedNodes(query.List.List)
	s.learnQuerySource(source, peerAddr, advertised)

	if len(advertised) > 0 && s.advertisedPeerLearning.CompareAndSwap(false, true) {
		// The query's slices may alias a transport buffer the caller reuses once
		// this answer is written, so the async job gets copies. Records that
		// cannot be a node of this overlay are dropped before the copy: rejecting
		// them costs no signature check and no goroutine.
		peers := make([]overlay.Node, 0, len(advertised))
		for i := range advertised {
			if !advertisedNodeHasShape(&advertised[i], s.spec.ShortID) {
				continue
			}
			peers = append(peers, *cloneOverlayNode(&advertised[i]))
		}
		if len(peers) == 0 {
			s.advertisedPeerLearning.Store(false)
		} else {
			s.node.runAsync(func() {
				defer s.advertisedPeerLearning.Store(false)
				s.learnAdvertisedNodes(s.node.runCtx, peers)
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

// learnQuerySource files the sender's own announcement together with the address
// its query arrived from.
//
// C++ puts that record first in every getRandomPeers it sends or answers
// (send_random_peers_cont, gated by announce_self_), and the transport already
// proved the sender holds the matching key. It is therefore the one address in
// the exchange we know is real, and the only one that costs no DHT lookup -
// overlay.node carries no address, so a row learned from gossip alone can never
// be dialled. Without this a peer that talks to us every few minutes stays an
// address-less, unverified row: never a gossip target, never a promotion
// candidate, and first in line for eviction.
func (s *overlaySubscription) learnQuerySource(source PeerID, peerAddr string, nodes []overlay.Node) {
	if source.IsZero() || peerAddr == "" || !s.spec.hasDirectoryTier() {
		return
	}

	for i := range nodes {
		key, ok := nodes[i].ID.(keys.PublicKeyED25519)
		if !ok || !advertisedNodeHasShape(&nodes[i], s.spec.ShortID) {
			continue
		}
		// The id derives from the key, so matching the sender first keeps the
		// signature check to exactly one per query however long the list is.
		id, err := peerIDFromPublicKey(key.Key)
		if err != nil || id != source {
			continue
		}

		// overlayNodeIdentity owns the signature, freshness and overlay-id
		// checks. A record that fails them ends the scan rather than sending us
		// looking for another one claiming the same id.
		identity, err := s.overlayNodeIdentity(nodes[i])
		if err != nil || identity.self {
			return
		}

		announced := cloneOverlayNode(&nodes[i])
		s.mx.Lock()
		s.rememberDirectoryPeerLocked(source, identity.pub, peerAddr, "", announced, time.Now(), directoryContacted)
		s.mx.Unlock()
		return
	}
}

// boundedAdvertisedNodes caps what one exchange may teach us. Honest peers send
// four records; the cap is what keeps a hostile peer from buying hundreds of
// signature checks and directory writes with a single query.
func boundedAdvertisedNodes(nodes []overlay.Node) []overlay.Node {
	if len(nodes) > maxAdvertisedPeersPerQuery {
		return nodes[:maxAdvertisedPeersPerQuery]
	}
	return nodes
}

// advertisedNodeHasShape is the free half of validating an advertised record:
// it can only be a node of this overlay if the fields are the right kind and
// size. The signature check that follows costs real work, so nothing that fails
// here should reach it.
func advertisedNodeHasShape(node *overlay.Node, overlayID []byte) bool {
	key, ok := node.ID.(keys.PublicKeyED25519)
	return ok &&
		len(key.Key) == ed25519.PublicKeySize &&
		len(node.Signature) == ed25519.SignatureSize &&
		bytes.Equal(node.Overlay, overlayID)
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
