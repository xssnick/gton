package p2p

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"flexserver/service/storage"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	persistentStateChunkSize             = 1 << 21
	persistentStateChunkAnswerMax        = persistentStateChunkSize + 1024
	persistentStateSmallAnswerMax        = 1024
	persistentStatePrepareTimeout        = time.Second
	persistentStateSizeTimeout           = 3 * time.Second
	persistentStateChunkTimeout          = 20 * time.Second
	persistentStateChunkWorkers          = 10
	persistentStateChunkRetries          = 3
	persistentStatePeerProbeCandidates   = 3
	persistentStatePeerProbeChunks       = 4
	persistentStatePeerProbeGrace        = 2 * time.Second
	persistentStateSplitPartWorkers      = 4
	persistentStateSplitChunkParallelism = 32
	persistentStateSplitRetryDelay       = time.Second
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

func (n *Node) DownloadState(ctx context.Context, block ton.BlockIDExt, master ton.BlockIDExt, splitDepth uint32, stateRootHash []byte) (StateSnapshotArtifact, error) {
	if block.SeqNo == 0 {
		return n.downloadZeroStateSnapshot(ctx, block)
	}
	if len(stateRootHash) != 32 {
		return nil, fmt.Errorf("state root hash is required for %s", formatBlockRef(block))
	}
	return n.downloadPersistentStateSnapshot(ctx, block, master, splitDepth, stateRootHash)
}

func (n *Node) queryBlockOverlay(ctx context.Context, block ton.BlockIDExt, req tl.Serializable) (tl.Serializable, error) {
	sub, err := n.subscriptionForBlock(block)
	if err != nil {
		return nil, err
	}
	if err = sub.ensurePeers(ctx); err != nil {
		return nil, fmt.Errorf("bootstrap overlay peers: %w", err)
	}
	return sub.query(ctx, req)
}

func (n *Node) downloadZeroStateSnapshot(ctx context.Context, block ton.BlockIDExt) (StateSnapshotArtifact, error) {
	resp, err := n.queryBlockOverlay(ctx, block, PrepareZeroState{Block: block})
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

	dataResp, err := n.queryBlockOverlay(ctx, block, DownloadZeroState{Block: block})
	if err != nil {
		return nil, err
	}

	data, ok := dataResp.(TonNodeData)
	if !ok {
		return nil, fmt.Errorf("unexpected downloadZeroState response %T", dataResp)
	}
	if len(data.Data) == 0 {
		return nil, fmt.Errorf("state boc is empty")
	}
	return &zeroStateSnapshotArtifact{
		block: block,
		data:  bytes.Clone(data.Data),
	}, nil
}

func (n *Node) downloadPersistentStateSnapshot(ctx context.Context, block ton.BlockIDExt, master ton.BlockIDExt, splitDepth uint32, stateRootHash []byte) (StateSnapshotArtifact, error) {
	sub, err := n.subscriptionForBlock(block)
	if err != nil {
		return nil, err
	}

	downloader := persistentStateSnapshotDownloader{
		node:          n,
		sub:           sub,
		block:         block,
		master:        master,
		stateRootHash: bytes.Clone(stateRootHash),
	}
	if n.storage != nil && (block.Workchain == -1 || splitDepth <= uint32(shardPrefixLength(block.Shard))) {
		staged, lazyRoot, ok, err := n.tryImportReusableStagedStateFile(ctx, block, 0, downloader.stateRootHash)
		if err != nil {
			return nil, fmt.Errorf("import reusable staged state: %w", err)
		}
		if ok {
			if err = n.persistImportedStagedBlockState(ctx, block, staged, lazyRoot, downloader.stateRootHash); err != nil {
				return nil, fmt.Errorf("persist imported reusable staged state: %w", err)
			}
			return &persistentStateSnapshotArtifact{
				node:          n,
				block:         block,
				stateRootHash: downloader.stateRootHash,
				staged:        staged,
			}, nil
		}
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
		Str("masterchain", formatBlockRef(master)).
		Uint32("split_depth", splitDepth).
		Int("peers", len(peers)).
		Msg("trying peers for persistent state snapshot")

	downloader.peers = peers
	if block.Workchain != -1 && splitDepth > uint32(shardPrefixLength(block.Shard)) {
		return n.downloadSplitPersistentStateSnapshot(ctx, downloader, splitDepth)
	}

	staged, err := downloader.downloadStagedValidated(ctx, 0, nil)
	if err != nil {
		return nil, err
	}
	if n.storage != nil {
		n.log.Info().
			Str("peer", staged.peerAddr).
			Str("block", formatPersistentStateBlockRef(block, staged.effectiveShard)).
			Str("size", formatByteSize(staged.size)).
			Uint64("cells", staged.cells).
			Str("path", staged.path).
			Msg("importing staged state snapshot cells")

		lazyRoot, err := n.decodeAndImportStagedStateCellTree(ctx, block, staged, downloader.stateRootHash)
		if err != nil {
			return nil, fmt.Errorf("import staged state cells: %w", err)
		}
		if err = n.persistImportedStagedBlockState(ctx, block, staged, lazyRoot, downloader.stateRootHash); err != nil {
			return nil, fmt.Errorf("persist imported staged state: %w", err)
		}

		n.log.Info().
			Str("peer", staged.peerAddr).
			Str("block", formatPersistentStateBlockRef(block, staged.effectiveShard)).
			Uint64("cells", staged.cells).
			Msg("staged state snapshot switched to lazy celldb root")
	}
	return &persistentStateSnapshotArtifact{
		node:          n,
		block:         block,
		stateRootHash: downloader.stateRootHash,
		staged:        staged,
	}, nil
}

func (n *Node) preparePersistentState(ctx context.Context, sub *overlaySubscription, peer *overlayPeer, block ton.BlockIDExt, master ton.BlockIDExt) error {
	startedAt := time.Now()
	resp, err := sub.queryFromPeerWithLimits(ctx, peer, PreparePersistentState{
		Block:            block,
		MasterchainBlock: master,
	}, persistentStatePrepareTimeout, persistentStateSmallAnswerMax)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			n.log.Debug().
				Err(err).
				Str("peer", peer.addr).
				Str("block", formatBlockRef(block)).
				Str("masterchain", formatBlockRef(master)).
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
		Str("block", formatBlockRef(block)).
		Str("masterchain", formatBlockRef(master)).
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
		if err := d.node.preparePersistentState(ctx, d.sub, peer, d.block, d.master); err != nil {
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
	return d.node.stagePersistentStateFile(ctx, d.sub, candidate, d.block, d.master, seed, chunkLimiter)
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
		releasePeer := d.node.acquireStateSnapshotPeer(candidates[0].peer)
		staged, err := d.downloadToStagedFile(ctx, candidates[0], nil, chunkLimiter)
		releasePeer()
		if err != nil {
			return nil, fmt.Errorf("%s: %w", candidates[0].peer.addr, err)
		}
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
			Int("state_downloads", d.node.stateSnapshotPeerLeases(probe.candidate.peer)).
			Str("speed", formatByteRate(probe.bytes, probe.elapsed)).
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
			Int("state_downloads", d.node.stateSnapshotPeerLeases(probe.candidate.peer)).
			Str("speed", formatByteRate(probe.bytes, probe.elapsed)).
			Msg("selected persistent state snapshot peer")

		staged, err := d.downloadToStagedFile(ctx, probe.candidate, &probe.seed, chunkLimiter)
		releasePeer()
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil, err
			}
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
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make(chan persistentStatePeerProbeResult, len(candidates))
	var wg sync.WaitGroup

	for _, candidate := range candidates {
		candidate := candidate
		wg.Add(1)
		go func() {
			defer wg.Done()
			probe, err := d.node.probePersistentStatePeer(ctx, d.sub, candidate, d.block, probeChunks, chunkLimiter)
			select {
			case results <- persistentStatePeerProbeResult{probe: probe, err: err}:
			case <-ctx.Done():
			}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var probes []persistentStatePeerProbe
	var errs []error
	var grace <-chan time.Time
	var graceTimer *time.Timer
	defer func() {
		if graceTimer != nil {
			graceTimer.Stop()
		}
	}()

	pending := len(candidates)
	for pending > 0 {
		select {
		case res, ok := <-results:
			if !ok {
				return probes, errs
			}
			pending--
			if res.err != nil {
				errs = append(errs, res.err)
				continue
			}

			probes = append(probes, res.probe)
			if graceTimer == nil && pending > 0 {
				graceTimer = time.NewTimer(persistentStatePeerProbeGrace)
				grace = graceTimer.C
			}
		case <-grace:
			cancel()
			d.node.log.Info().
				Str("block", formatPersistentStateBlockRef(d.block, candidates[0].id.EffectiveShard)).
				Str("masterchain", formatBlockRef(d.master)).
				Int("ready_peers", len(probes)).
				Int("pending_peers", pending).
				Dur("grace", persistentStatePeerProbeGrace).
				Msg("persistent state peer probe grace elapsed, selecting from ready peers")
			return probes, errs
		case <-ctx.Done():
			if len(probes) > 0 {
				return probes, errs
			}
			return nil, append(errs, ctx.Err())
		}
	}
	return probes, errs
}

type persistentStatePeerProbeResult struct {
	probe persistentStatePeerProbe
	err   error
}

type downloadedSplitStateHeader struct {
	state  *tlb.ShardStateUnsplit
	parts  []splitStatePart
	cells  uint64
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

func (n *Node) downloadSplitPersistentStateSnapshot(ctx context.Context, downloader persistentStateSnapshotDownloader, splitDepth uint32) (StateSnapshotArtifact, error) {
	header, err := n.loadImportedSplitPersistentStateHeader(ctx, downloader, splitDepth)
	if err != nil {
		if !errors.Is(err, storage.ErrNotFound) {
			return nil, err
		}

		header, err = n.downloadSplitPersistentStateHeader(ctx, downloader, splitDepth)
		if err != nil {
			return nil, err
		}
	}

	limiter := make(chan struct{}, persistentStateSplitChunkParallelism)
	parts, err := n.downloadSplitPersistentStateParts(ctx, downloader, header.parts, limiter)
	if err != nil {
		return nil, err
	}

	partArtifacts := make([]splitPersistentStatePartArtifact, len(parts))
	var partCells uint64
	for i, part := range parts {
		partArtifacts[i] = splitPersistentStatePartArtifact{
			part:   header.parts[i],
			staged: part.staged,
		}
		partCells += part.staged.cells
	}

	n.log.Info().
		Str("block", formatBlockRef(downloader.block)).
		Str("masterchain", formatBlockRef(downloader.master)).
		Int("parts", len(parts)).
		Uint64("cells", header.cells+partCells).
		Msg("split persistent state parts staged for import")

	return &splitPersistentStateSnapshotArtifact{
		node:          n,
		block:         downloader.block,
		master:        downloader.master,
		stateRootHash: downloader.stateRootHash,
		header:        header,
		parts:         partArtifacts,
	}, nil
}

func splitStateHeaderStorageBlock(block ton.BlockIDExt) ton.BlockIDExt {
	block.Shard ^= 1
	return block
}

func splitStatePartStorageBlock(block ton.BlockIDExt, part splitStatePart) ton.BlockIDExt {
	block.Shard = int64(part.effectiveShard)
	block.RootHash = append([]byte(nil), part.rootHash...)
	return block
}

func (n *Node) loadImportedSplitPersistentStateHeader(ctx context.Context, downloader persistentStateSnapshotDownloader, splitDepth uint32) (*downloadedSplitStateHeader, error) {
	if n.storage == nil {
		return nil, storage.ErrNotFound
	}

	headerBlock := splitStateHeaderStorageBlock(downloader.block)
	root, cellsCount, err := n.storage.LoadStateCellTree(ctx, headerBlock, nil)
	if err != nil {
		return nil, err
	}

	header, parts, err := splitStateParts(downloader.block, root, splitDepth, downloader.stateRootHash)
	if err != nil {
		return nil, fmt.Errorf("%w: parse imported split persistent state header: %w", errStateSnapshotInvalid, err)
	}

	n.log.Info().
		Str("block", formatBlockRef(downloader.block)).
		Str("masterchain", formatBlockRef(downloader.master)).
		Str("header_storage_block", formatBlockRef(headerBlock)).
		Uint32("split_depth", splitDepth).
		Int("parts", len(parts)).
		Uint64("cells", cellsCount).
		Msg("using imported split persistent state header")

	return &downloadedSplitStateHeader{
		state: header,
		parts: parts,
		cells: cellsCount,
	}, nil
}

func (n *Node) downloadSplitPersistentStateHeader(ctx context.Context, downloader persistentStateSnapshotDownloader, splitDepth uint32) (*downloadedSplitStateHeader, error) {
	header, ok, err := n.tryLoadReusableSplitPersistentStateHeader(ctx, downloader, splitDepth)
	if err != nil {
		return nil, err
	}
	if ok {
		return header, nil
	}

	n.log.Info().
		Str("block", formatBlockRef(downloader.block)).
		Str("masterchain", formatBlockRef(downloader.master)).
		Uint32("split_depth", splitDepth).
		Int64("effective_shard", downloader.block.Shard).
		Msg("downloading split persistent state header")

	staged, err := downloader.downloadStagedValidated(ctx, downloader.block.Shard, nil)
	if err != nil {
		return nil, err
	}
	header, root, parsedCells, err := n.parseStagedSplitPersistentStateHeader(downloader, splitDepth, staged)
	if err != nil {
		root = nil
		parsedCells = nil
		runtime.GC()
		if errors.Is(err, errStateSnapshotInvalid) {
			if cleanupErr := staged.cleanup(); cleanupErr != nil {
				n.log.Warn().
					Err(cleanupErr).
					Str("block", formatPersistentStateBlockRef(downloader.block, staged.effectiveShard)).
					Str("path", staged.path).
					Msg("failed to remove invalid split persistent state header file")
			}
		}
		return nil, err
	}

	if n.storage != nil {
		if err = n.importSplitPersistentStateHeaderCells(ctx, downloader, header, root, parsedCells); err != nil {
			root = nil
			parsedCells = nil
			runtime.GC()
			return nil, fmt.Errorf("import split persistent state header cells: %w", err)
		}
	}
	root = nil
	parsedCells = nil
	runtime.GC()
	return header, nil
}

func (n *Node) tryLoadReusableSplitPersistentStateHeader(ctx context.Context, downloader persistentStateSnapshotDownloader, splitDepth uint32) (*downloadedSplitStateHeader, bool, error) {
	paths, err := n.reusableStagedStateFiles(downloader.block, downloader.block.Shard)
	if err != nil {
		return nil, false, err
	}

	for _, path := range paths {
		if err = ctx.Err(); err != nil {
			return nil, false, err
		}

		staged, err := n.loadReusableStagedStateFile(path, downloader.block.Shard)
		if err != nil {
			if errors.Is(err, errStateSnapshotInvalid) {
				n.removeInvalidReusableStateFile(downloader.block, downloader.block.Shard, path, err)
				continue
			}
			return nil, false, err
		}

		header, root, parsedCells, err := n.parseStagedSplitPersistentStateHeader(downloader, splitDepth, staged)
		if err != nil {
			root = nil
			parsedCells = nil
			runtime.GC()
			if errors.Is(err, errStateSnapshotInvalid) {
				if cleanupErr := staged.cleanup(); cleanupErr != nil {
					n.log.Warn().
						Err(cleanupErr).
						Str("block", formatPersistentStateBlockRef(downloader.block, staged.effectiveShard)).
						Str("path", staged.path).
						Msg("failed to remove invalid split persistent state header file")
				}
				continue
			}
			return nil, false, err
		}

		n.log.Info().
			Str("block", formatPersistentStateBlockRef(downloader.block, staged.effectiveShard)).
			Str("masterchain", formatBlockRef(downloader.master)).
			Str("size", formatByteSize(staged.size)).
			Uint64("cells", staged.cells).
			Str("path", staged.path).
			Msg("using reusable staged split persistent state header")

		if n.storage != nil {
			if err = n.importSplitPersistentStateHeaderCells(ctx, downloader, header, root, parsedCells); err != nil {
				root = nil
				parsedCells = nil
				runtime.GC()
				return nil, false, fmt.Errorf("import reusable split persistent state header cells: %w", err)
			}
		}
		root = nil
		parsedCells = nil
		runtime.GC()
		return header, true, nil
	}

	return nil, false, nil
}

func (n *Node) parseStagedSplitPersistentStateHeader(downloader persistentStateSnapshotDownloader, splitDepth uint32, staged *stagedStateFile) (*downloadedSplitStateHeader, *cell.Cell, []cell.Cell, error) {
	root, parsedCells, err := staged.decodeRootCells()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("%w: parse staged split persistent state header boc: %w", errStateSnapshotInvalid, err)
	}

	header, parts, err := splitStateParts(downloader.block, root, splitDepth, downloader.stateRootHash)
	if err != nil {
		n.log.Error().
			Err(err).
			Str("peer", staged.peerAddr).
			Str("block", formatPersistentStateBlockRef(downloader.block, staged.effectiveShard)).
			Int64("size", staged.size).
			Str("state_file_hash", hex.EncodeToString(staged.fileHash)).
			Str("prefix", firstBytesHex(staged.prefix, 16)).
			Msg("failed to parse split persistent state header")
		return nil, root, parsedCells, fmt.Errorf("%w: %w", errStateSnapshotInvalid, err)
	}

	n.log.Info().
		Str("peer", staged.peerAddr).
		Str("block", formatPersistentStateBlockRef(downloader.block, staged.effectiveShard)).
		Str("masterchain", formatBlockRef(downloader.master)).
		Uint32("split_depth", splitDepth).
		Int("parts", len(parts)).
		Int64("header_size", staged.size).
		Uint64("cells", staged.cells).
		Str("header_file_hash", hex.EncodeToString(staged.fileHash)).
		Msg("split persistent state header parsed")

	return &downloadedSplitStateHeader{
		state:  header,
		parts:  parts,
		cells:  staged.cells,
		staged: staged,
	}, root, parsedCells, nil
}

func (n *Node) importSplitPersistentStateHeaderCells(ctx context.Context, downloader persistentStateSnapshotDownloader, header *downloadedSplitStateHeader, root *cell.Cell, parsedCells []cell.Cell) error {
	headerBlock := splitStateHeaderStorageBlock(downloader.block)
	if _, err := n.storage.ImportStateCellTree(ctx, headerBlock, root, parsedCells, header.cells); err != nil {
		return err
	}

	n.log.Info().
		Str("block", formatBlockRef(downloader.block)).
		Str("masterchain", formatBlockRef(downloader.master)).
		Str("header_storage_block", formatBlockRef(headerBlock)).
		Int("parts", len(header.parts)).
		Uint64("cells", header.cells).
		Msg("split persistent state header imported")
	return nil
}

func (n *Node) downloadSplitPersistentStateParts(ctx context.Context, downloader persistentStateSnapshotDownloader, parts []splitStatePart, chunkLimiter chan struct{}) ([]*downloadedSplitStatePart, error) {
	downloaded := make([]*downloadedSplitStatePart, len(parts))
	for {
		missing := missingSplitStateParts(downloaded)
		if missing == 0 {
			return downloaded, nil
		}

		currentPeers := downloader.sub.queryCandidates(0, 0)
		if len(currentPeers) == 0 {
			if err := downloader.sub.ensurePeers(ctx); err != nil {
				return nil, fmt.Errorf("bootstrap overlay peers: %w", err)
			}
			currentPeers = downloader.sub.queryCandidates(0, 0)
		}
		if len(currentPeers) == 0 {
			currentPeers = downloader.peers
		}
		if len(currentPeers) == 0 {
			return nil, errors.New("overlay has no connected peers")
		}

		workers := persistentStateSplitPartWorkers
		if missing < workers {
			workers = missing
		}

		n.log.Info().
			Str("block", formatBlockRef(downloader.block)).
			Str("masterchain", formatBlockRef(downloader.master)).
			Int("missing_parts", missing).
			Int("parts", len(parts)).
			Int("part_workers", workers).
			Int("chunk_parallelism", cap(chunkLimiter)).
			Int("peers", len(currentPeers)).
			Msg("downloading split persistent state parts")

		progress, err := n.downloadSplitPersistentStatePartsPass(ctx, downloader.withPeers(currentPeers), parts, downloaded, workers, chunkLimiter)
		if err != nil && errors.Is(err, errStateSnapshotInvalid) {
			return nil, err
		}
		if missingSplitStateParts(downloaded) == 0 {
			continue
		}

		event := n.log.Debug().
			Str("block", formatBlockRef(downloader.block)).
			Str("masterchain", formatBlockRef(downloader.master)).
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

func (n *Node) downloadSplitPersistentStatePartsPass(ctx context.Context, downloader persistentStateSnapshotDownloader, parts []splitStatePart, downloaded []*downloadedSplitStatePart, workers int, chunkLimiter chan struct{}) (int, error) {
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

	jobs := make(chan int)
	results := make(chan splitStatePartResult, workers)
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		workerSlot := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				res := n.downloadSplitPersistentStatePart(ctx, downloader, idx, len(parts), parts[idx], workerSlot, chunkLimiter)
				select {
				case results <- res:
				case <-ctx.Done():
					return
				}
				if errors.Is(res.err, errStateSnapshotInvalid) {
					return
				}
			}
		}()
	}

	go func() {
		defer close(jobs)
		for _, idx := range missing {
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

	var progress int
	var errs []error
	for res := range results {
		if res.err != nil {
			errs = append(errs, res.err)
			if errors.Is(res.err, errStateSnapshotInvalid) {
				cancel()
			}
			continue
		}
		if downloaded[res.index] == nil {
			downloaded[res.index] = res.part
			progress++
		}
	}
	return progress, errors.Join(errs...)
}

func (n *Node) downloadSplitPersistentStatePart(ctx context.Context, downloader persistentStateSnapshotDownloader, idx int, partsCount int, part splitStatePart, workerSlot int, chunkLimiter chan struct{}) splitStatePartResult {
	if imported, err := n.loadImportedSplitPersistentStatePart(ctx, downloader, idx, partsCount, part); err == nil {
		return splitStatePartResult{index: idx, part: imported}
	} else if errors.Is(err, context.Canceled) {
		return splitStatePartResult{index: idx, err: err}
	} else if !errors.Is(err, storage.ErrNotFound) {
		n.log.Warn().
			Err(err).
			Str("block", formatPersistentStateBlockRef(downloader.block, int64(part.effectiveShard))).
			Str("masterchain", formatBlockRef(downloader.master)).
			Int("part", idx+1).
			Int("parts", partsCount).
			Msg("failed to load imported split persistent state part, downloading it again")
	}

	if n.storage != nil {
		partBlock := splitStatePartStorageBlock(downloader.block, part)
		staged, _, ok, err := n.tryImportReusableStagedStateFileAs(ctx, downloader.block, partBlock, int64(part.effectiveShard), part.rootHash, stagedStateCellHash)
		if err != nil {
			return splitStatePartResult{
				index: idx,
				err:   fmt.Errorf("import reusable split state part %d cells: %w", idx+1, err),
			}
		}
		if ok {
			return splitStatePartResult{
				index: idx,
				part:  &downloadedSplitStatePart{staged: staged},
			}
		}
	}

	var result *downloadedSplitStatePart
	peerOffset := 0
	if len(downloader.peers) > 0 {
		peerOffset = (workerSlot * persistentStatePeerProbeCandidates) % len(downloader.peers)
	}
	partDownloader := downloader.withPeers(rotatedPeers(downloader.peers, peerOffset))

	n.log.Info().
		Str("block", formatPersistentStateBlockRef(downloader.block, int64(part.effectiveShard))).
		Str("masterchain", formatBlockRef(downloader.master)).
		Int("part", idx+1).
		Int("parts", partsCount).
		Int("peer_group", workerSlot+1).
		Int("peer_offset", peerOffset).
		Int64("effective_shard", int64(part.effectiveShard)).
		Msg("downloading split persistent state part")

	staged, err := partDownloader.downloadStagedValidated(ctx, int64(part.effectiveShard), chunkLimiter)
	if err != nil {
		return splitStatePartResult{
			index: idx,
			err:   fmt.Errorf("split state part %d: %w", idx+1, err),
		}
	}

	n.log.Info().
		Str("peer", staged.peerAddr).
		Str("block", formatPersistentStateBlockRef(downloader.block, int64(part.effectiveShard))).
		Int("part", idx+1).
		Int("parts", partsCount).
		Str("size", formatByteSize(staged.size)).
		Uint64("cells", staged.cells).
		Int64("effective_shard", int64(part.effectiveShard)).
		Str("state_file_hash", hex.EncodeToString(staged.fileHash)).
		Str("path", staged.path).
		Msg("split persistent state part staged")

	if n.storage != nil {
		n.log.Info().
			Str("peer", staged.peerAddr).
			Str("block", formatPersistentStateBlockRef(downloader.block, int64(part.effectiveShard))).
			Int("part", idx+1).
			Int("parts", partsCount).
			Str("size", formatByteSize(staged.size)).
			Uint64("cells", staged.cells).
			Str("path", staged.path).
			Msg("importing staged split persistent state part cells")

		partBlock := splitStatePartStorageBlock(downloader.block, part)
		if _, err = n.decodeAndImportStagedStateCellTreeAs(ctx, downloader.block, partBlock, staged, part.rootHash, stagedStateCellHash); err != nil {
			return splitStatePartResult{
				index: idx,
				err:   fmt.Errorf("import split state part %d cells: %w", idx+1, err),
			}
		}

		n.log.Info().
			Str("peer", staged.peerAddr).
			Str("block", formatPersistentStateBlockRef(downloader.block, int64(part.effectiveShard))).
			Int("part", idx+1).
			Int("parts", partsCount).
			Uint64("cells", staged.cells).
			Msg("staged split persistent state part switched to lazy celldb root")
	}

	result = &downloadedSplitStatePart{staged: staged}

	return splitStatePartResult{index: idx, part: result}
}

func (n *Node) loadImportedSplitPersistentStatePart(ctx context.Context, downloader persistentStateSnapshotDownloader, idx int, partsCount int, part splitStatePart) (*downloadedSplitStatePart, error) {
	if n.storage == nil {
		return nil, storage.ErrNotFound
	}

	partBlock := splitStatePartStorageBlock(downloader.block, part)
	root, cellsCount, err := n.storage.LoadStateCellTree(ctx, partBlock, nil)
	if err != nil {
		return nil, err
	}
	rootHash := root.HashKey()
	if !bytes.Equal(rootHash[:], part.rootHash) {
		return nil, storage.ErrNotFound
	}

	n.log.Info().
		Str("block", formatPersistentStateBlockRef(downloader.block, int64(part.effectiveShard))).
		Str("masterchain", formatBlockRef(downloader.master)).
		Int("part", idx+1).
		Int("parts", partsCount).
		Uint64("cells", cellsCount).
		Int64("effective_shard", int64(part.effectiveShard)).
		Msg("using imported split persistent state part")

	return &downloadedSplitStatePart{
		staged: &stagedStateFile{
			effectiveShard: int64(part.effectiveShard),
			peerAddr:       "celldb",
			cells:          cellsCount,
			lazyRoot:       root,
		},
	}, nil
}

func rotatedPeers(peers []*overlayPeer, offset int) []*overlayPeer {
	if len(peers) == 0 {
		return peers
	}

	offset %= len(peers)
	if offset == 0 {
		return peers
	}

	rotated := make([]*overlayPeer, 0, len(peers))
	rotated = append(rotated, peers[offset:]...)
	return append(rotated, peers[:offset]...)
}

type stateChunkResult struct {
	offset    int64
	chunkSize int64
	data      []byte
	elapsed   time.Duration
	attempts  int
	err       error
}

func (n *Node) probePersistentStatePeer(ctx context.Context, sub *overlaySubscription, candidate persistentStateCandidate, block ton.BlockIDExt, probeChunks int, chunkLimiter chan struct{}) (persistentStatePeerProbe, error) {
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
				res := n.downloadPersistentStateChunk(ctx, sub, candidate.peer, candidate.id, block, idx, candidate.size, chunkLimiter)
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
		return persistentStatePeerProbe{}, fmt.Errorf("%s: %w", candidate.peer.addr, firstErr)
	}
	if len(chunks) != probeChunks {
		return persistentStatePeerProbe{}, fmt.Errorf("%s: persistent state peer probe incomplete: got=%d want=%d", candidate.peer.addr, len(chunks), probeChunks)
	}

	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].offset < chunks[j].offset
	})

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
	speed := formatByteRate(downloaded-lastProgressDownloaded, now.Sub(lastProgress))

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

func formatByteRate(bytes int64, elapsed time.Duration) string {
	if bytes <= 0 || elapsed <= 0 {
		return "0 B/s"
	}

	rate := float64(bytes) / elapsed.Seconds()
	units := []string{"B/s", "KB/s", "MB/s", "GB/s"}
	unit := 0
	for rate >= 1024 && unit < len(units)-1 {
		rate /= 1024
		unit++
	}

	if unit == 0 {
		return fmt.Sprintf("%.0f %s", rate, units[unit])
	}
	return fmt.Sprintf("%.2f %s", rate, units[unit])
}

func firstBytesHex(data []byte, limit int) string {
	if len(data) > limit {
		data = data[:limit]
	}
	return hex.EncodeToString(data)
}

func (n *Node) downloadPersistentStateChunk(ctx context.Context, sub *overlaySubscription, peer *overlayPeer, id PersistentStateIDV2, block ton.BlockIDExt, idx int, size int64, chunkLimiter chan struct{}) stateChunkResult {
	offset := int64(idx) * persistentStateChunkSize
	chunkSize := int64(persistentStateChunkSize)
	if left := size - offset; left < chunkSize {
		chunkSize = left
	}

	var last stateChunkResult
	for attempt := 1; attempt <= persistentStateChunkRetries; attempt++ {
		startedAt := time.Now()
		data, err := queryPersistentStateChunk(ctx, sub, peer, DownloadPersistentStateSliceV2{
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
			Str("block", formatPersistentStateBlockRef(block, id.EffectiveShard)).
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
