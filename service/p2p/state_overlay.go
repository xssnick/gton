package p2p

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/xssnick/gton/internal/logutil"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
)

const (
	persistentStateChunkSize              = 1 << 21
	persistentStateChunkAnswerMax         = persistentStateChunkSize + 1024
	persistentStateSmallAnswerMax         = 1024
	persistentStatePrepareTimeout         = time.Second
	persistentStateSizeTimeout            = 3 * time.Second
	persistentStateChunkTimeout           = 20 * time.Second
	persistentStateChunkWorkers           = 10
	persistentStateChunkRetries           = 3
	persistentStatePeerProbeCandidates    = 3
	persistentStatePeerProbeChunks        = 4
	persistentStatePeerProbeGrace         = 2 * time.Second
	persistentStateSplitPartBacklog       = 4
	persistentStateSplitPartImportWorkers = 2
	persistentStateSplitChunkParallelism  = 32
	persistentStateSplitRetryDelay        = time.Second
)

var (
	ErrStateNotAvailable    = errors.New("state snapshot is not available from fullnode")
	errStateSnapshotInvalid = errors.New("downloaded state snapshot is invalid")
)

func init() {
	tl.Register(PreparedState{}, "tonNode.preparedState = tonNode.PreparedState")
	tl.Register(NotFoundState{}, "tonNode.notFoundState = tonNode.PreparedState")
	tl.Register(PersistentStateSize{}, "tonNode.persistentStateSize size:long = tonNode.PersistentStateSize")
	tl.Register(PersistentStateSizeNotFound{}, "tonNode.persistentStateSizeNotFound = tonNode.PersistentStateSize")
	tl.Register(TonNodeData{}, "tonNode.data data:bytes = tonNode.Data")

	tl.Register(PrepareZeroState{}, "tonNode.prepareZeroState block:tonNode.blockIdExt = tonNode.PreparedState")
	tl.Register(DownloadZeroState{}, "tonNode.downloadZeroState block:tonNode.blockIdExt = tonNode.Data")
	tl.Register(PreparePersistentState{}, "tonNode.preparePersistentState block:tonNode.blockIdExt masterchain_block:tonNode.blockIdExt = tonNode.PreparedState")
	tl.Register(DownloadPersistentStateSliceV2{}, "tonNode.downloadPersistentStateSliceV2 state:tonNode.persistentStateIdV2 offset:long max_size:long = tonNode.Data")
	tl.Register(GetPersistentStateSizeV2{}, "tonNode.getPersistentStateSizeV2 state:tonNode.persistentStateIdV2 = tonNode.PersistentStateSize")
	tl.Register(PersistentStateIDV2{}, "tonNode.persistentStateIdV2 block:tonNode.blockIdExt masterchain_block:tonNode.blockIdExt effective_shard:long = tonNode.PersistentStateIdV2")

	tl.Register(DbFileDBKeyPersistentStateFile{}, "db.filedb.key.persistentStateFile block_id:tonNode.blockIdExt masterchain_block_id:tonNode.blockIdExt = db.filedb.Key")
	tl.Register(DbFileDBKeySplitAccountStateFile{}, "db.filedb.key.splitAccountStateFile block_id:tonNode.blockIdExt masterchain_block_id:tonNode.blockIdExt effective_shard:long = db.filedb.Key")
	tl.Register(DbFileDBKeySplitPersistentStateFile{}, "db.filedb.key.splitPersistentStateFile block_id:tonNode.blockIdExt masterchain_block_id:tonNode.blockIdExt = db.filedb.Key")
}

type PreparedState struct{}

type NotFoundState struct{}

type PersistentStateSize struct {
	Size int64 `tl:"long"`
}

type PersistentStateSizeNotFound struct{}

type TonNodeData struct {
	Data []byte `tl:"bytes"`
}

type PrepareZeroState struct {
	Block ton.BlockIDExt `tl:"struct"`
}

type DownloadZeroState struct {
	Block ton.BlockIDExt `tl:"struct"`
}

type PreparePersistentState struct {
	Block            ton.BlockIDExt `tl:"struct"`
	MasterchainBlock ton.BlockIDExt `tl:"struct"`
}

type DownloadPersistentStateSliceV2 struct {
	State   PersistentStateIDV2 `tl:"struct"`
	Offset  int64               `tl:"long"`
	MaxSize int64               `tl:"long"`
}

type GetPersistentStateSizeV2 struct {
	State PersistentStateIDV2 `tl:"struct"`
}

type PersistentStateIDV2 struct {
	Block            ton.BlockIDExt `tl:"struct"`
	MasterchainBlock ton.BlockIDExt `tl:"struct"`
	EffectiveShard   int64          `tl:"long"`
}

type DbFileDBKeyPersistentStateFile struct {
	BlockID            ton.BlockIDExt `tl:"struct"`
	MasterchainBlockID ton.BlockIDExt `tl:"struct"`
}

type DbFileDBKeySplitAccountStateFile struct {
	BlockID            ton.BlockIDExt `tl:"struct"`
	MasterchainBlockID ton.BlockIDExt `tl:"struct"`
	EffectiveShard     int64          `tl:"long"`
}

type DbFileDBKeySplitPersistentStateFile struct {
	BlockID            ton.BlockIDExt `tl:"struct"`
	MasterchainBlockID ton.BlockIDExt `tl:"struct"`
}

func (n *Node) DownloadState(ctx context.Context, block ton.BlockIDExt, master ton.BlockIDExt, splitDepth uint32, stateRootHash []byte) (storage.DownloadedState, error) {
	if block.SeqNo == 0 {
		return n.downloadZeroStateSnapshot(ctx, block)
	}
	if len(stateRootHash) != 32 {
		return nil, fmt.Errorf("state root hash is required for %s", formatBlockRef(block))
	}
	return n.downloadPersistentStateSnapshot(ctx, block, master, splitDepth, stateRootHash)
}

func (n *Node) downloadZeroStateSnapshot(ctx context.Context, block ton.BlockIDExt) (storage.DownloadedState, error) {
	if data, err := n.peerStorage.ZeroState(ctx, block); err == nil && len(data) > 0 {
		return &zeroStateSnapshotArtifact{
			block: block,
			data:  bytes.Clone(data),
		}, nil
	} else if err != nil && !errors.Is(err, storage.ErrNotFound) {
		n.log.Debug().
			Err(err).
			Str("block", formatBlockRef(block)).
			Msg("failed to load cached zero state")
	}

	sub, err := n.subscriptionForBlock(block)
	if err != nil {
		return nil, err
	}
	if err = sub.ensurePeers(ctx); err != nil {
		return nil, fmt.Errorf("bootstrap overlay peers: %w", err)
	}

	peers := sub.queryCandidates(0, 0)
	if len(peers) == 0 {
		return nil, errors.New("overlay has no connected peers")
	}

	n.log.Info().
		Str("block", formatBlockRef(block)).
		Int("peers", len(peers)).
		Msg("requesting zero state from overlay peers")

	var errs []error
	for _, peer := range peers {
		n.log.Info().
			Str("peer", peer.addr).
			Str("block", formatBlockRef(block)).
			Msg("requesting zero state from peer")

		artifact, err := n.downloadZeroStateSnapshotFromPeer(ctx, sub, peer, block)
		if err == nil {
			if zero, ok := artifact.(*zeroStateSnapshotArtifact); ok {
				zero.writer, _ = n.peerStorage.(storage.PeerServingStorageWriter)
			}
			return artifact, nil
		}
		if errors.Is(err, context.Canceled) {
			return nil, err
		}
		errs = append(errs, fmt.Errorf("%s: %w", peer.addr, err))
	}

	if len(errs) == 0 {
		return nil, ErrStateNotAvailable
	}
	return nil, errors.Join(errs...)
}

func (n *Node) downloadZeroStateSnapshotFromPeer(ctx context.Context, sub *overlaySubscription, peer *overlayPeer, block ton.BlockIDExt) (storage.DownloadedState, error) {
	resp, err := sub.queryFromPeer(ctx, peer, PrepareZeroState{Block: block})
	if err != nil {
		return nil, err
	}
	switch resp.(type) {
	case PreparedState:
	case NotFoundState:
		return nil, ErrStateNotAvailable
	default:
		return nil, fmt.Errorf("unexpected prepareZeroState response %T", resp)
	}

	data, err := sub.queryRawFromPeerWithLimits(ctx, peer, DownloadZeroState{Block: block}, downloadQueryTimeout, maxBlockDownloadAnswerSize)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("state boc is empty")
	}

	return &zeroStateSnapshotArtifact{
		block: block,
		data:  bytes.Clone(data),
	}, nil
}

func (n *Node) downloadPersistentStateSnapshot(ctx context.Context, block ton.BlockIDExt, master ton.BlockIDExt, splitDepth uint32, stateRootHash []byte) (storage.DownloadedState, error) {
	sub, err := n.subscriptionForBlock(block)
	if err != nil {
		return nil, err
	}

	return persistentStateSnapshotDownloader{
		node:          n,
		sub:           sub,
		block:         block,
		master:        master,
		stateRootHash: bytes.Clone(stateRootHash),
	}.download(ctx, splitDepth)
}

func (d persistentStateSnapshotDownloader) download(ctx context.Context, splitDepth uint32) (storage.DownloadedState, error) {
	n := d.node

	if n.storage != nil && (d.block.Workchain == -1 || splitDepth <= uint32(shardPrefixLength(d.block.Shard))) {
		staged, lazyRoot, err := n.tryImportReusableStagedStateFile(ctx, d.block, d.master, 0, d.stateRootHash)
		if err == nil {
			if err = n.cacheImportedStagedBlockState(d.block, staged, lazyRoot, d.stateRootHash); err != nil {
				return nil, fmt.Errorf("prepare imported reusable staged state: %w", err)
			}
			if err = n.savePersistentStateFile(d.block, d.master, staged, d.stateRootHash); err != nil {
				return nil, fmt.Errorf("store reusable staged state file: %w", err)
			}
			return &persistentStateSnapshotArtifact{
				node:          n,
				block:         d.block,
				master:        d.master,
				stateRootHash: d.stateRootHash,
				staged:        staged,
			}, nil
		}
		if !errors.Is(err, storage.ErrNotFound) {
			return nil, fmt.Errorf("import reusable staged state: %w", err)
		}
	}

	if err := d.sub.ensurePeers(ctx); err != nil {
		return nil, fmt.Errorf("bootstrap overlay peers: %w", err)
	}

	peers := d.sub.queryCandidates(0, 0)
	if len(peers) == 0 {
		return nil, errors.New("overlay has no connected peers")
	}

	n.log.Info().
		Str("block", formatBlockRef(d.block)).
		Str("masterchain", formatBlockRef(d.master)).
		Uint32("split_depth", splitDepth).
		Int("peers", len(peers)).
		Msg("trying peers for persistent state snapshot")

	d.peers = peers
	if d.block.Workchain != -1 && splitDepth > uint32(shardPrefixLength(d.block.Shard)) {
		return d.downloadSplit(ctx, splitDepth)
	}

	staged, err := d.downloadStagedValidated(ctx, 0, nil)
	if err != nil {
		return nil, err
	}
	if n.storage != nil {
		n.log.Info().
			Str("peer", staged.peerAddr).
			Str("block", formatPersistentStateBlockRef(d.block, staged.effectiveShard)).
			Str("size", formatByteSize(staged.size)).
			Str("path", staged.path).
			Msg("importing staged state snapshot cells")

		lazyRoot, err := n.decodeAndImportStagedStateCellTree(ctx, d.block, staged, d.stateRootHash)
		if err != nil {
			return nil, fmt.Errorf("import staged state cells: %w", err)
		}
		if err = n.cacheImportedStagedBlockState(d.block, staged, lazyRoot, d.stateRootHash); err != nil {
			return nil, fmt.Errorf("prepare imported staged state: %w", err)
		}
		if err = n.savePersistentStateFile(d.block, d.master, staged, d.stateRootHash); err != nil {
			return nil, fmt.Errorf("store staged state file: %w", err)
		}

		n.log.Info().
			Str("peer", staged.peerAddr).
			Str("block", formatPersistentStateBlockRef(d.block, staged.effectiveShard)).
			Msg("staged state snapshot switched to lazy celldb root")
	}
	return &persistentStateSnapshotArtifact{
		node:          n,
		block:         d.block,
		master:        d.master,
		stateRootHash: d.stateRootHash,
		staged:        staged,
	}, nil
}

func (d persistentStateSnapshotDownloader) preparePersistentState(ctx context.Context, peer *overlayPeer) error {
	n := d.node

	startedAt := time.Now()
	resp, err := d.sub.queryFromPeerWithLimits(ctx, peer, PreparePersistentState{
		Block:            d.block,
		MasterchainBlock: d.master,
	}, persistentStatePrepareTimeout, persistentStateSmallAnswerMax)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			n.log.Debug().
				Err(err).
				Str("peer", peer.addr).
				Str("block", formatBlockRef(d.block)).
				Str("masterchain", formatBlockRef(d.master)).
				Dur("elapsed", time.Since(startedAt)).
				Msg("failed to prepare persistent state snapshot")
		}
		return err
	}

	switch resp.(type) {
	case PreparedState:
	case NotFoundState:
		return ErrStateNotAvailable
	default:
		return fmt.Errorf("unexpected preparePersistentState response %T", resp)
	}
	n.log.Info().
		Str("peer", peer.addr).
		Str("block", formatBlockRef(d.block)).
		Str("masterchain", formatBlockRef(d.master)).
		Dur("elapsed", time.Since(startedAt)).
		Msg("persistent state snapshot is prepared")
	return nil
}

type persistentStateCandidate struct {
	peer       *overlayPeer
	id         PersistentStateIDV2
	size       int64
	workers    int
	chunkCount int
}

type persistentStateChunkSeed struct {
	chunks  []stateChunkResult
	elapsed time.Duration
}

type persistentStatePeerProbe struct {
	candidate persistentStateCandidate
	seed      persistentStateChunkSeed
	bytes     int64
	elapsed   time.Duration
}

type persistentStateSnapshotDownloader struct {
	node          *Node
	sub           *overlaySubscription
	block         ton.BlockIDExt
	master        ton.BlockIDExt
	stateRootHash []byte
	peers         []*overlayPeer
}

func (d persistentStateSnapshotDownloader) withPeers(peers []*overlayPeer) persistentStateSnapshotDownloader {
	d.peers = peers
	return d
}

func (d persistentStateSnapshotDownloader) preparePersistentStateCandidate(ctx context.Context, peer *overlayPeer, effectiveShard int64) (*persistentStateCandidate, error) {
	if !peer.hasOpenConnection() {
		return nil, adnl.ErrPeerConnClosed
	}

	if effectiveShard == 0 {
		d.node.log.Info().
			Str("peer", peer.addr).
			Str("block", formatBlockRef(d.block)).
			Str("masterchain", formatBlockRef(d.master)).
			Msg("preparing persistent state snapshot")
		if err := d.preparePersistentState(ctx, peer); err != nil {
			return nil, err
		}
	}

	id := PersistentStateIDV2{
		Block:            d.block,
		MasterchainBlock: d.master,
		EffectiveShard:   effectiveShard,
	}

	startedAt := time.Now()
	sizeResp, err := d.sub.queryFromPeerWithLimits(ctx, peer, GetPersistentStateSizeV2{State: id}, persistentStateSizeTimeout, persistentStateSmallAnswerMax)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			d.node.log.Debug().
				Err(err).
				Str("peer", peer.addr).
				Str("block", formatPersistentStateBlockRef(d.block, id.EffectiveShard)).
				Str("masterchain", formatBlockRef(d.master)).
				Int64("effective_shard", id.EffectiveShard).
				Dur("elapsed", time.Since(startedAt)).
				Msg("failed to load persistent state snapshot size")
		}
		return nil, err
	}

	var size int64
	switch t := sizeResp.(type) {
	case PersistentStateSize:
		size = t.Size
	case PersistentStateSizeNotFound:
		d.node.log.Debug().
			Str("peer", peer.addr).
			Str("block", formatPersistentStateBlockRef(d.block, id.EffectiveShard)).
			Str("masterchain", formatBlockRef(d.master)).
			Int64("effective_shard", id.EffectiveShard).
			Msg("persistent state snapshot size is not found")
		return nil, ErrStateNotAvailable
	default:
		return nil, fmt.Errorf("unexpected getPersistentStateSizeV2 response %T", sizeResp)
	}
	if size <= 0 {
		return nil, fmt.Errorf("invalid persistent state size %d", size)
	}

	workers := persistentStateChunkWorkers
	chunkCount := int((size + persistentStateChunkSize - 1) / persistentStateChunkSize)
	if chunkCount < workers {
		workers = chunkCount
	}

	d.node.log.Info().
		Str("peer", peer.addr).
		Str("block", formatPersistentStateBlockRef(d.block, id.EffectiveShard)).
		Str("masterchain", formatBlockRef(d.master)).
		Int64("effective_shard", id.EffectiveShard).
		Str("size", formatByteSize(size)).
		Int("workers", workers).
		Msg("state snapshot size received")

	return &persistentStateCandidate{
		peer:       peer,
		id:         id,
		size:       size,
		workers:    workers,
		chunkCount: chunkCount,
	}, nil
}

func (d persistentStateSnapshotDownloader) downloadToStagedFile(ctx context.Context, candidate persistentStateCandidate, seed *persistentStateChunkSeed, chunkLimiter chan struct{}) (*stagedStateFile, error) {
	return d.stagePersistentStateFile(ctx, candidate, seed, chunkLimiter)
}

func persistentStateCandidateProbeChunks(candidates []persistentStateCandidate) int {
	probeChunks := persistentStatePeerProbeChunks
	for _, candidate := range candidates {
		if candidate.chunkCount < probeChunks {
			probeChunks = candidate.chunkCount
		}
	}
	return probeChunks
}

func (d persistentStateSnapshotDownloader) downloadStagedFromCandidates(ctx context.Context, candidates []persistentStateCandidate, chunkLimiter chan struct{}) (*stagedStateFile, error) {
	if len(candidates) == 0 {
		return nil, errors.New("no persistent state candidates")
	}
	if len(candidates) == 1 {
		started := time.Now()
		releasePeer := d.node.acquireDownloadPeer(candidates[0].peer)
		staged, err := d.downloadToStagedFile(ctx, candidates[0], nil, chunkLimiter)
		releasePeer()
		if err != nil {
			candidates[0].peer.downloadFailed(stateSlowPeerPenalty)
			return nil, fmt.Errorf("%s: %w", candidates[0].peer.addr, err)
		}
		candidates[0].peer.downloadSuccess(staged.size, time.Since(started), stateSlowPeerSpeed, stateSlowPeerPenalty)
		return staged, nil
	}

	probeChunks := persistentStateCandidateProbeChunks(candidates)

	d.node.log.Info().
		Str("block", formatPersistentStateBlockRef(d.block, candidates[0].id.EffectiveShard)).
		Str("masterchain", formatBlockRef(d.master)).
		Int64("effective_shard", candidates[0].id.EffectiveShard).
		Int("peers", len(candidates)).
		Int("probe_chunks", probeChunks).
		Str("probe_bytes_per_peer", formatByteSize(int64(probeChunks)*persistentStateChunkSize)).
		Int64("probe_bytes_per_peer_bytes", int64(probeChunks)*persistentStateChunkSize).
		Msg("selecting fastest persistent state snapshot peer")

	probes, errs := d.probePersistentStateCandidates(ctx, candidates, probeChunks, chunkLimiter)
	if len(probes) == 0 {
		return nil, errors.Join(errs...)
	}

	sort.Slice(probes, func(i, j int) bool {
		return probeBytesPerSecond(probes[i]) > probeBytesPerSecond(probes[j])
	})

	for _, probe := range probes {
		d.node.log.Info().
			Str("peer", probe.candidate.peer.addr).
			Str("block", formatPersistentStateBlockRef(d.block, probe.candidate.id.EffectiveShard)).
			Int64("effective_shard", probe.candidate.id.EffectiveShard).
			Int("probe_chunks", len(probe.seed.chunks)).
			Str("probe_bytes", formatByteSize(probe.bytes)).
			Int64("probe_bytes_bytes", probe.bytes).
			Dur("elapsed", probe.elapsed).
			Int("download_leases", d.node.downloadPeerLeaseCount(probe.candidate.peer)).
			Str("speed", logutil.FormatByteRate(probe.bytes, probe.elapsed)).
			Msg("persistent state snapshot peer probe result")
	}

	var attemptErrs []error
	remaining := append([]persistentStatePeerProbe(nil), probes...)
	for i := 0; len(remaining) > 0; i++ {
		probe, releasePeer := d.node.acquirePreferredStateSnapshotProbe(remaining)

		d.node.log.Info().
			Str("peer", probe.candidate.peer.addr).
			Str("block", formatPersistentStateBlockRef(d.block, probe.candidate.id.EffectiveShard)).
			Int64("effective_shard", probe.candidate.id.EffectiveShard).
			Int("rank", i+1).
			Int("peers", len(remaining)).
			Int("download_leases", d.node.downloadPeerLeaseCount(probe.candidate.peer)).
			Str("speed", logutil.FormatByteRate(probe.bytes, probe.elapsed)).
			Msg("selected persistent state snapshot peer")

		started := time.Now()
		staged, err := d.downloadToStagedFile(ctx, probe.candidate, &probe.seed, chunkLimiter)
		releasePeer()
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil, err
			}
			probe.candidate.peer.downloadFailed(stateSlowPeerPenalty)
			attemptErrs = append(attemptErrs, fmt.Errorf("%s: %w", probe.candidate.peer.addr, err))
			d.node.log.Warn().
				Err(err).
				Str("peer", probe.candidate.peer.addr).
				Str("block", formatPersistentStateBlockRef(d.block, probe.candidate.id.EffectiveShard)).
				Int64("effective_shard", probe.candidate.id.EffectiveShard).
				Msg("selected persistent state snapshot peer failed, trying next probed peer")
			for j := range remaining {
				if remaining[j].candidate.peer == probe.candidate.peer {
					remaining = append(remaining[:j], remaining[j+1:]...)
					break
				}
			}
			continue
		}
		probe.candidate.peer.downloadSuccess(staged.size, time.Since(started), stateSlowPeerSpeed, stateSlowPeerPenalty)
		return staged, nil
	}

	return nil, errors.Join(append(errs, attemptErrs...)...)
}

func (d persistentStateSnapshotDownloader) downloadStagedValidated(ctx context.Context, effectiveShard int64, chunkLimiter chan struct{}) (*stagedStateFile, error) {
	var unavailableErrs []error
	var failedErrs []error

	recordPeerError := func(peer *overlayPeer, err error) {
		peerErr := fmt.Errorf("%s: %w", peer.addr, err)
		if errors.Is(err, ErrStateNotAvailable) {
			unavailableErrs = append(unavailableErrs, peerErr)
			d.node.log.Debug().
				Err(err).
				Str("peer", peer.addr).
				Str("block", formatPersistentStateBlockRef(d.block, effectiveShard)).
				Str("masterchain", formatBlockRef(d.master)).
				Int64("effective_shard", effectiveShard).
				Msg("peer does not have persistent state snapshot")
			return
		}
		if errors.Is(err, context.Canceled) {
			failedErrs = append(failedErrs, peerErr)
			return
		}

		failedErrs = append(failedErrs, peerErr)
		d.node.log.Debug().
			Err(err).
			Str("peer", peer.addr).
			Str("block", formatPersistentStateBlockRef(d.block, effectiveShard)).
			Str("masterchain", formatBlockRef(d.master)).
			Msg("persistent state snapshot peer failed, trying next peer")
	}

	recordBatchError := func(err error) {
		if errors.Is(err, context.Canceled) {
			failedErrs = append(failedErrs, err)
			return
		}
		failedErrs = append(failedErrs, err)
	}

	var candidates []persistentStateCandidate
	flushCandidates := func() (*stagedStateFile, bool) {
		if len(candidates) == 0 {
			return nil, false
		}

		staged, err := d.downloadStagedFromCandidates(ctx, candidates, chunkLimiter)
		candidates = nil
		if err == nil {
			return staged, true
		}
		recordBatchError(err)
		return nil, false
	}

	for _, peer := range d.node.prioritizeStateSnapshotPeers(d.peers) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		candidate, err := d.preparePersistentStateCandidate(ctx, peer, effectiveShard)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return nil, ctxErr
				}
				return nil, err
			}
			recordPeerError(peer, err)
			continue
		}

		candidates = append(candidates, *candidate)
		if len(candidates) < persistentStatePeerProbeCandidates {
			continue
		}

		if staged, ok := flushCandidates(); ok {
			return staged, nil
		}
	}

	if staged, ok := flushCandidates(); ok {
		return staged, nil
	}

	return nil, fmt.Errorf("%w: %w", ErrStateNotAvailable, errors.Join(append(unavailableErrs, failedErrs...)...))
}

func probeBytesPerSecond(probe persistentStatePeerProbe) float64 {
	if probe.elapsed <= 0 {
		return 0
	}
	return float64(probe.bytes) / probe.elapsed.Seconds()
}

func (d persistentStateSnapshotDownloader) probePersistentStateCandidates(ctx context.Context, candidates []persistentStateCandidate, probeChunks int, chunkLimiter chan struct{}) ([]persistentStatePeerProbe, []error) {
	peers := make([]*overlayPeer, 0, len(candidates))
	candidateByPeer := make(map[string]persistentStateCandidate, len(candidates))
	for _, candidate := range candidates {
		peers = append(peers, candidate.peer)
		candidateByPeer[downloadPeerKey(candidate.peer)] = candidate
	}

	results, errs := runPeerRequests(ctx, peers, peerRequestOptions{
		parallelism:         len(peers),
		collectAfterSuccess: persistentStatePeerProbeGrace,
		onCollectElapsed: func(ready int, pending int) {
			d.node.log.Info().
				Str("block", formatPersistentStateBlockRef(d.block, candidates[0].id.EffectiveShard)).
				Str("masterchain", formatBlockRef(d.master)).
				Int("ready_peers", ready).
				Int("pending_peers", pending).
				Dur("grace", persistentStatePeerProbeGrace).
				Msg("persistent state peer probe grace elapsed, selecting from ready peers")
		},
	}, func(queryCtx context.Context, peer *overlayPeer) (persistentStatePeerProbe, error) {
		candidate := candidateByPeer[downloadPeerKey(peer)]
		return d.probePersistentStatePeer(queryCtx, candidate, probeChunks, chunkLimiter)
	})

	probes := make([]persistentStatePeerProbe, 0, len(results))
	for _, res := range results {
		probes = append(probes, res.value)
	}
	return probes, errs
}

type downloadedSplitStateHeader struct {
	state  *tlb.ShardStateUnsplit
	parts  []splitStatePart
	staged *stagedStateFile
}

type downloadedSplitStatePart struct {
	staged *stagedStateFile
}

type splitStatePartResult struct {
	index int
	part  *downloadedSplitStatePart
	err   error
}

func (d persistentStateSnapshotDownloader) downloadSplit(ctx context.Context, splitDepth uint32) (storage.DownloadedState, error) {
	n := d.node

	header, err := d.downloadSplitHeader(ctx, splitDepth)
	if err != nil {
		return nil, err
	}

	limiter := make(chan struct{}, persistentStateSplitChunkParallelism)
	parts, err := d.downloadSplitParts(ctx, header.parts, limiter)
	if err != nil {
		return nil, err
	}

	partArtifacts := make([]splitPersistentStatePartArtifact, len(parts))
	for i, part := range parts {
		partArtifacts[i] = splitPersistentStatePartArtifact{
			part:   header.parts[i],
			staged: part.staged,
		}
	}

	n.log.Info().
		Str("block", formatBlockRef(d.block)).
		Str("masterchain", formatBlockRef(d.master)).
		Int("parts", len(parts)).
		Msg("split persistent state parts staged for import")

	return &splitPersistentStateSnapshotArtifact{
		node:          n,
		block:         d.block,
		master:        d.master,
		stateRootHash: d.stateRootHash,
		header:        header,
		parts:         partArtifacts,
	}, nil
}

func splitStatePartStorageBlock(block ton.BlockIDExt, part splitStatePart) ton.BlockIDExt {
	block.Shard = int64(part.effectiveShard)
	block.RootHash = append([]byte(nil), part.rootHash...)
	return block
}

func (d persistentStateSnapshotDownloader) downloadSplitHeader(ctx context.Context, splitDepth uint32) (*downloadedSplitStateHeader, error) {
	n := d.node

	header, err := d.tryLoadReusableSplitHeader(ctx, splitDepth)
	if err == nil {
		return header, nil
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return nil, err
	}

	n.log.Info().
		Str("block", formatBlockRef(d.block)).
		Str("masterchain", formatBlockRef(d.master)).
		Uint32("split_depth", splitDepth).
		Int64("effective_shard", d.block.Shard).
		Msg("downloading split persistent state header")

	staged, err := d.downloadStagedValidated(ctx, d.block.Shard, nil)
	if err != nil {
		return nil, err
	}
	header, err = d.parseStagedSplitHeader(splitDepth, staged)
	if err != nil {
		runtime.GC()
		if errors.Is(err, errStateSnapshotInvalid) {
			if cleanupErr := staged.cleanup(); cleanupErr != nil {
				n.log.Warn().
					Err(cleanupErr).
					Str("block", formatPersistentStateBlockRef(d.block, staged.effectiveShard)).
					Str("path", staged.path).
					Msg("failed to remove invalid split persistent state header file")
			}
		}
		return nil, err
	}

	if err = n.savePersistentStateFile(d.block, d.master, staged, nil); err != nil {
		runtime.GC()
		return nil, fmt.Errorf("store split persistent state header file: %w", err)
	}
	runtime.GC()
	return header, nil
}

func (d persistentStateSnapshotDownloader) tryLoadReusableSplitHeader(ctx context.Context, splitDepth uint32) (*downloadedSplitStateHeader, error) {
	n := d.node

	paths, err := n.reusableStagedStateFiles(d.block, d.master, d.block.Shard)
	if err != nil {
		return nil, err
	}

	for _, path := range paths {
		if err = ctx.Err(); err != nil {
			return nil, err
		}

		staged, err := n.loadReusableStagedStateFile(path, d.block.Shard)
		if err != nil {
			if errors.Is(err, errStateSnapshotInvalid) {
				n.removeInvalidReusableStateFile(d.block, d.block.Shard, path, err)
				continue
			}
			return nil, err
		}

		header, err := d.parseStagedSplitHeader(splitDepth, staged)
		if err != nil {
			runtime.GC()
			if errors.Is(err, errStateSnapshotInvalid) {
				staged.keep = false
				if cleanupErr := staged.cleanup(); cleanupErr != nil {
					n.log.Warn().
						Err(cleanupErr).
						Str("block", formatPersistentStateBlockRef(d.block, staged.effectiveShard)).
						Str("path", staged.path).
						Msg("failed to remove invalid split persistent state header file")
				}
				continue
			}
			return nil, err
		}

		n.log.Info().
			Str("block", formatPersistentStateBlockRef(d.block, staged.effectiveShard)).
			Str("masterchain", formatBlockRef(d.master)).
			Str("size", formatByteSize(staged.size)).
			Str("path", staged.path).
			Msg("using reusable staged split persistent state header")

		if err = n.savePersistentStateFile(d.block, d.master, staged, nil); err != nil {
			runtime.GC()
			return nil, fmt.Errorf("store reusable split persistent state header file: %w", err)
		}
		runtime.GC()
		return header, nil
	}

	return nil, storage.ErrNotFound
}

func (d persistentStateSnapshotDownloader) parseStagedSplitHeader(splitDepth uint32, staged *stagedStateFile) (*downloadedSplitStateHeader, error) {
	n := d.node

	root, _, err := staged.decodeRootCells()
	if err != nil {
		return nil, fmt.Errorf("%w: parse staged split persistent state header boc: %w", errStateSnapshotInvalid, err)
	}

	header, parts, err := splitStateParts(d.block, root, splitDepth, d.stateRootHash)
	if err != nil {
		n.log.Error().
			Err(err).
			Str("peer", staged.peerAddr).
			Str("block", formatPersistentStateBlockRef(d.block, staged.effectiveShard)).
			Int64("size", staged.size).
			Str("state_file_hash", hex.EncodeToString(staged.fileHash)).
			Str("prefix", firstBytesHex(staged.prefix, 16)).
			Msg("failed to parse split persistent state header")
		return nil, fmt.Errorf("%w: %w", errStateSnapshotInvalid, err)
	}

	n.log.Info().
		Str("peer", staged.peerAddr).
		Str("block", formatPersistentStateBlockRef(d.block, staged.effectiveShard)).
		Str("masterchain", formatBlockRef(d.master)).
		Uint32("split_depth", splitDepth).
		Int("parts", len(parts)).
		Int64("header_size", staged.size).
		Str("header_file_hash", hex.EncodeToString(staged.fileHash)).
		Msg("split persistent state header parsed")

	return &downloadedSplitStateHeader{
		state:  header,
		parts:  parts,
		staged: staged,
	}, nil
}

func (d persistentStateSnapshotDownloader) downloadSplitParts(ctx context.Context, parts []splitStatePart, chunkLimiter chan struct{}) ([]*downloadedSplitStatePart, error) {
	n := d.node
	downloaded := make([]*downloadedSplitStatePart, len(parts))
	for {
		missing := missingSplitStateParts(downloaded)
		if missing == 0 {
			return downloaded, nil
		}

		currentPeers := d.sub.queryCandidates(0, 0)
		if len(currentPeers) == 0 {
			if err := d.sub.ensurePeers(ctx); err != nil {
				return nil, fmt.Errorf("bootstrap overlay peers: %w", err)
			}
			currentPeers = d.sub.queryCandidates(0, 0)
		}
		if len(currentPeers) == 0 {
			currentPeers = d.peers
		}
		if len(currentPeers) == 0 {
			return nil, errors.New("overlay has no connected peers")
		}

		backlog := persistentStateSplitPartBacklog
		if missing < backlog {
			backlog = missing
		}
		importWorkers := persistentStateSplitPartImportWorkers
		if missing < importWorkers {
			importWorkers = missing
		}

		n.log.Info().
			Str("block", formatBlockRef(d.block)).
			Str("masterchain", formatBlockRef(d.master)).
			Int("missing_parts", missing).
			Int("parts", len(parts)).
			Int("download_streams", 1).
			Int("staged_backlog", backlog).
			Int("import_workers", importWorkers).
			Int("chunk_parallelism", cap(chunkLimiter)).
			Int("peers", len(currentPeers)).
			Msg("downloading split persistent state parts")

		progress, err := d.withPeers(currentPeers).downloadSplitPartsPass(ctx, parts, downloaded, backlog, importWorkers, chunkLimiter)
		if missingSplitStateParts(downloaded) == 0 {
			continue
		}

		event := n.log.Debug()
		if err != nil && errors.Is(err, errStateSnapshotInvalid) {
			event = n.log.Warn()
		}
		event = event.
			Str("block", formatBlockRef(d.block)).
			Str("masterchain", formatBlockRef(d.master)).
			Int("downloaded_parts", len(parts)-missingSplitStateParts(downloaded)).
			Int("missing_parts", missingSplitStateParts(downloaded)).
			Int("parts", len(parts)).
			Int("progress_parts", progress).
			Dur("retry_in", persistentStateSplitRetryDelay)
		if err != nil {
			event.Err(err)
		}
		event.Msg("split persistent state parts are not complete, retrying missing parts")

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(persistentStateSplitRetryDelay):
		}
	}
}

func missingSplitStateParts(parts []*downloadedSplitStatePart) int {
	var missing int
	for _, part := range parts {
		if part == nil {
			missing++
		}
	}
	return missing
}

func (d persistentStateSnapshotDownloader) downloadSplitPartsPass(ctx context.Context, parts []splitStatePart, downloaded []*downloadedSplitStatePart, backlog int, importWorkers int, chunkLimiter chan struct{}) (int, error) {
	missing := make([]int, 0, len(parts))
	for i := range parts {
		if downloaded[i] == nil {
			missing = append(missing, i)
		}
	}
	if len(missing) == 0 {
		return 0, nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if backlog <= 0 {
		backlog = 1
	}
	if importWorkers <= 0 {
		importWorkers = 1
	}

	staged := make(chan splitStatePartResult, backlog)
	results := make(chan splitStatePartResult, importWorkers)
	var wg sync.WaitGroup
	for i := 0; i < importWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for res := range staged {
				if res.err == nil {
					res.err = d.importSplitPart(ctx, res.index, len(parts), parts[res.index], res.part)
				}

				select {
				case results <- res:
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	go func() {
		defer close(staged)
		for _, idx := range missing {
			res := d.stageSplitPart(ctx, idx, len(parts), parts[idx], chunkLimiter)
			select {
			case staged <- res:
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	var progress int
	var errs []error
	for res := range results {
		if res.err != nil {
			errs = append(errs, res.err)
			continue
		}
		if downloaded[res.index] == nil {
			downloaded[res.index] = res.part
			progress++
		}
	}
	return progress, errors.Join(errs...)
}

func (d persistentStateSnapshotDownloader) stageSplitPart(ctx context.Context, idx int, partsCount int, part splitStatePart, chunkLimiter chan struct{}) splitStatePartResult {
	n := d.node

	if reusable, err := d.loadReusableSplitPart(ctx, idx, partsCount, part); err == nil {
		return splitStatePartResult{index: idx, part: reusable}
	} else if errors.Is(err, context.Canceled) {
		return splitStatePartResult{index: idx, err: err}
	} else if !errors.Is(err, storage.ErrNotFound) {
		return splitStatePartResult{
			index: idx,
			err:   fmt.Errorf("load reusable split state part %d file: %w", idx+1, err),
		}
	}

	n.log.Info().
		Str("block", formatPersistentStateBlockRef(d.block, int64(part.effectiveShard))).
		Str("masterchain", formatBlockRef(d.master)).
		Int("part", idx+1).
		Int("parts", partsCount).
		Int64("effective_shard", int64(part.effectiveShard)).
		Msg("downloading split persistent state part")

	staged, err := d.downloadStagedValidated(ctx, int64(part.effectiveShard), chunkLimiter)
	if err != nil {
		return splitStatePartResult{
			index: idx,
			err:   fmt.Errorf("split state part %d: %w", idx+1, err),
		}
	}

	n.log.Info().
		Str("peer", staged.peerAddr).
		Str("block", formatPersistentStateBlockRef(d.block, int64(part.effectiveShard))).
		Int("part", idx+1).
		Int("parts", partsCount).
		Str("size", formatByteSize(staged.size)).
		Int64("effective_shard", int64(part.effectiveShard)).
		Str("state_file_hash", hex.EncodeToString(staged.fileHash)).
		Str("path", staged.path).
		Msg("split persistent state part staged")

	return splitStatePartResult{
		index: idx,
		part:  &downloadedSplitStatePart{staged: staged},
	}
}

func (d persistentStateSnapshotDownloader) importSplitPart(ctx context.Context, idx int, partsCount int, part splitStatePart, downloaded *downloadedSplitStatePart) error {
	n := d.node
	if n.storage == nil || downloaded == nil || downloaded.staged == nil || downloaded.staged.lazyRoot != nil {
		return nil
	}
	staged := downloaded.staged

	n.log.Info().
		Str("peer", staged.peerAddr).
		Str("block", formatPersistentStateBlockRef(d.block, int64(part.effectiveShard))).
		Int("part", idx+1).
		Int("parts", partsCount).
		Str("size", formatByteSize(staged.size)).
		Str("path", staged.path).
		Msg("importing staged split persistent state part cells")

	partBlock := splitStatePartStorageBlock(d.block, part)
	if _, err := n.decodeAndImportSplitPartStagedStateCellTree(ctx, d.block, partBlock, staged, part.rootHash); err != nil {
		if errors.Is(err, errStateSnapshotInvalid) {
			path := staged.path
			staged.keep = false
			if cleanupErr := staged.cleanup(); cleanupErr != nil {
				n.log.Warn().
					Err(cleanupErr).
					Str("block", formatPersistentStateBlockRef(d.block, int64(part.effectiveShard))).
					Str("path", path).
					Msg("failed to remove invalid split persistent state part file")
			} else {
				n.log.Warn().
					Err(err).
					Str("peer", staged.peerAddr).
					Str("block", formatPersistentStateBlockRef(d.block, int64(part.effectiveShard))).
					Int("part", idx+1).
					Int("parts", partsCount).
					Str("path", path).
					Msg("removed invalid split persistent state part file")
			}
		}
		return fmt.Errorf("import split state part %d cells: %w", idx+1, err)
	}
	if err := n.savePersistentStateFile(d.block, d.master, staged, nil); err != nil {
		return fmt.Errorf("store split state part %d file: %w", idx+1, err)
	}

	n.log.Info().
		Str("peer", staged.peerAddr).
		Str("block", formatPersistentStateBlockRef(d.block, int64(part.effectiveShard))).
		Int("part", idx+1).
		Int("parts", partsCount).
		Msg("staged split persistent state part switched to lazy celldb root")
	return nil
}

func (d persistentStateSnapshotDownloader) loadReusableSplitPart(ctx context.Context, idx int, partsCount int, part splitStatePart) (*downloadedSplitStatePart, error) {
	n := d.node
	paths, err := n.reusableStagedStateFiles(d.block, d.master, int64(part.effectiveShard))
	if err != nil {
		return nil, err
	}

	for _, path := range paths {
		if err = ctx.Err(); err != nil {
			return nil, err
		}

		staged, err := n.loadReusableStagedStateFile(path, int64(part.effectiveShard))
		if err != nil {
			if errors.Is(err, errStateSnapshotInvalid) {
				n.removeInvalidReusableStateFile(d.block, int64(part.effectiveShard), path, err)
				continue
			}
			return nil, err
		}

		n.log.Info().
			Str("block", formatPersistentStateBlockRef(d.block, int64(part.effectiveShard))).
			Str("masterchain", formatBlockRef(d.master)).
			Int("part", idx+1).
			Int("parts", partsCount).
			Str("size", formatByteSize(staged.size)).
			Str("path", staged.path).
			Msg("using reusable staged split persistent state part")
		return &downloadedSplitStatePart{staged: staged}, nil
	}

	return nil, storage.ErrNotFound
}

type stateChunkResult struct {
	offset    int64
	chunkSize int64
	data      []byte
	elapsed   time.Duration
	attempts  int
	err       error
}

func (d persistentStateSnapshotDownloader) probePersistentStatePeer(ctx context.Context, candidate persistentStateCandidate, probeChunks int, chunkLimiter chan struct{}) (persistentStatePeerProbe, error) {
	if probeChunks > candidate.chunkCount {
		probeChunks = candidate.chunkCount
	}
	if probeChunks <= 0 {
		return persistentStatePeerProbe{}, fmt.Errorf("invalid persistent state probe chunks %d", probeChunks)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	startedAt := time.Now()
	workers := candidate.workers
	if probeChunks < workers {
		workers = probeChunks
	}

	jobs := make(chan int)
	results := make(chan stateChunkResult, probeChunks)
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				res := d.downloadPersistentStateChunk(ctx, candidate.peer, candidate.id, idx, candidate.size, chunkLimiter)
				select {
				case results <- res:
				case <-ctx.Done():
					return
				}
				if res.err != nil {
					return
				}
			}
		}()
	}

	go func() {
		defer close(jobs)
		for idx := 0; idx < probeChunks; idx++ {
			select {
			case jobs <- idx:
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	chunks := make([]stateChunkResult, 0, probeChunks)
	var downloaded int64
	var firstErr error
	for res := range results {
		if res.err != nil {
			if firstErr == nil {
				firstErr = res.err
				cancel()
			}
			continue
		}

		chunks = append(chunks, res)
		downloaded += int64(len(res.data))
	}

	elapsed := time.Since(startedAt)
	if firstErr != nil {
		if !errors.Is(firstErr, context.Canceled) {
			candidate.peer.downloadFailed(stateSlowPeerPenalty)
		}
		return persistentStatePeerProbe{}, firstErr
	}
	if len(chunks) != probeChunks {
		candidate.peer.downloadFailed(stateSlowPeerPenalty)
		return persistentStatePeerProbe{}, fmt.Errorf("persistent state peer probe incomplete: got=%d want=%d", len(chunks), probeChunks)
	}

	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].offset < chunks[j].offset
	})

	candidate.peer.downloadSuccess(downloaded, elapsed, stateSlowPeerSpeed, stateSlowPeerPenalty)

	return persistentStatePeerProbe{
		candidate: candidate,
		seed: persistentStateChunkSeed{
			chunks:  chunks,
			elapsed: elapsed,
		},
		bytes:   downloaded,
		elapsed: elapsed,
	}, nil
}

func (n *Node) logDownloadStateProgress(peer *overlayPeer, blockRef string, downloaded, size int64, workers int, lastProgress time.Time, lastProgressDownloaded int64) {
	now := time.Now()
	progress := float64(downloaded) * 100 / float64(size)
	speed := logutil.FormatByteRate(downloaded-lastProgressDownloaded, now.Sub(lastProgress))

	n.log.Info().
		Str("peer", peer.addr).
		Str("block", blockRef).
		Str("downloaded", formatByteSize(downloaded)).
		Str("size", formatByteSize(size)).
		Int("workers", workers).
		Str("progress", fmt.Sprintf("%.1f%%", progress)).
		Str("speed", speed).
		Msg("state snapshot download progress")
}

func formatByteSize(size int64) string {
	const unit = 1024
	units := []string{"B", "KB", "MB", "GB", "TB"}
	value := float64(size)
	i := 0
	for value >= unit && i+1 < len(units) {
		value /= unit
		i++
	}

	if i == 0 {
		return fmt.Sprintf("%.0f %s", value, units[i])
	}
	if i == 1 || i == 2 || i == 3 || i == 4 {
		return fmt.Sprintf("%.2f %s", value, units[i])
	}
	return fmt.Sprintf("%.2f %s", value, units[i])
}

func firstBytesHex(data []byte, limit int) string {
	if len(data) > limit {
		data = data[:limit]
	}
	return hex.EncodeToString(data)
}

func (d persistentStateSnapshotDownloader) downloadPersistentStateChunk(ctx context.Context, peer *overlayPeer, id PersistentStateIDV2, idx int, size int64, chunkLimiter chan struct{}) stateChunkResult {
	n := d.node

	offset := int64(idx) * persistentStateChunkSize
	chunkSize := int64(persistentStateChunkSize)
	if left := size - offset; left < chunkSize {
		chunkSize = left
	}

	var last stateChunkResult
	for attempt := 1; attempt <= persistentStateChunkRetries; attempt++ {
		startedAt := time.Now()
		data, err := queryPersistentStateChunk(ctx, d.sub, peer, DownloadPersistentStateSliceV2{
			State:   id,
			Offset:  offset,
			MaxSize: chunkSize,
		}, persistentStateChunkTimeout, uint64(chunkSize+1024), chunkLimiter)

		last = stateChunkResult{
			offset:    offset,
			chunkSize: chunkSize,
			data:      data,
			elapsed:   time.Since(startedAt),
			attempts:  attempt,
			err:       err,
		}
		if err == nil && int64(len(data)) != chunkSize {
			last.err = fmt.Errorf("persistent state chunk size mismatch at offset %d: got=%d want=%d", offset, len(data), chunkSize)
		}
		if last.err == nil || errors.Is(last.err, context.Canceled) || attempt == persistentStateChunkRetries {
			return last
		}

		n.log.Debug().
			Err(last.err).
			Str("peer", peer.addr).
			Str("block", formatPersistentStateBlockRef(d.block, id.EffectiveShard)).
			Int64("offset", offset).
			Int64("chunk_size", chunkSize).
			Int("attempt", attempt).
			Int("max_attempts", persistentStateChunkRetries).
			Dur("elapsed", last.elapsed).
			Msg("retrying persistent state snapshot chunk")
	}

	return last
}

func queryPersistentStateChunk(ctx context.Context, sub *overlaySubscription, peer *overlayPeer, req DownloadPersistentStateSliceV2, timeout time.Duration, maxAnswerSize uint64, chunkLimiter chan struct{}) ([]byte, error) {
	release, err := acquirePersistentStateChunkSlot(ctx, chunkLimiter)
	if err != nil {
		return nil, err
	}
	defer release()

	return sub.queryRawFromPeerWithLimits(ctx, peer, req, timeout, maxAnswerSize)
}

func acquirePersistentStateChunkSlot(ctx context.Context, chunkLimiter chan struct{}) (func(), error) {
	if chunkLimiter == nil {
		return func() {}, nil
	}

	select {
	case chunkLimiter <- struct{}{}:
		return func() { <-chunkLimiter }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
