package p2p

import (
	"context"
	"crypto/sha256"
	"errors"
	"hash"
	"io"
	"sync"
	"time"
)

const persistentStateReadPrefixLimit = 64

type persistentStateChunkReader struct {
	ctx    context.Context
	cancel context.CancelFunc

	downloader   persistentStateSnapshotDownloader
	node         *Node
	peer         *overlayPeer
	id           PersistentStateIDV2
	blockRef     string
	size         int64
	workers      int
	chunkLimiter chan struct{}

	chunks     [][]byte
	current    []byte
	nextChunk  int
	readOffset int64
	downloaded int64

	results chan stateChunkResult
	err     error

	hash   hash.Hash
	prefix []byte

	lastProgress           time.Time
	lastProgressDownloaded int64
}

func (d persistentStateSnapshotDownloader) newPersistentStateChunkReader(ctx context.Context, peer *overlayPeer, id PersistentStateIDV2, size int64, workers int, chunkLimiter chan struct{}, seed *persistentStateChunkSeed) (*persistentStateChunkReader, error) {
	n := d.node

	ctx, cancel := context.WithCancel(ctx)
	chunkCount := int((size + persistentStateChunkSize - 1) / persistentStateChunkSize)
	now := time.Now()
	startedAt := now
	if seed != nil && seed.elapsed > 0 {
		startedAt = startedAt.Add(-seed.elapsed)
	}

	r := &persistentStateChunkReader{
		ctx:          ctx,
		cancel:       cancel,
		downloader:   d,
		node:         n,
		peer:         peer,
		id:           id,
		blockRef:     formatPersistentStateBlockRef(d.block, id.EffectiveShard),
		size:         size,
		workers:      workers,
		chunkLimiter: chunkLimiter,
		chunks:       make([][]byte, chunkCount),
		hash:         sha256.New(),
		lastProgress: startedAt,
	}

	if seed != nil {
		for _, chunk := range seed.chunks {
			r.addDownloadedChunk(chunk)
		}

		n.log.Info().
			Str("peer", peer.addr).
			Str("block", r.blockRef).
			Str("downloaded", formatByteSize(r.downloaded)).
			Str("size", formatByteSize(size)).
			Int("seed_chunks", len(seed.chunks)).
			Int("workers", workers).
			Msg("continuing state snapshot stream from selected peer")
	} else {
		probeChunkSize := int64(persistentStateChunkSize)
		if size < probeChunkSize {
			probeChunkSize = size
		}

		n.log.Info().
			Str("peer", peer.addr).
			Str("block", r.blockRef).
			Str("size", formatByteSize(size)).
			Str("chunk_size", formatByteSize(probeChunkSize)).
			Int64("chunk_size_bytes", probeChunkSize).
			Msg("checking state snapshot availability")

		first := d.downloadPersistentStateChunk(ctx, peer, id, 0, size, chunkLimiter)
		if first.err != nil {
			cancel()
			if !errors.Is(first.err, context.Canceled) {
				n.log.Error().
					Err(first.err).
					Str("peer", peer.addr).
					Str("block", r.blockRef).
					Int64("offset", first.offset).
					Int64("chunk_size", first.chunkSize).
					Int("attempts", first.attempts).
					Dur("elapsed", first.elapsed).
					Msg("failed to download persistent state snapshot probe chunk")
			}
			return nil, first.err
		}

		r.addDownloadedChunk(first)

		n.log.Info().
			Str("peer", peer.addr).
			Str("block", r.blockRef).
			Str("downloaded", formatByteSize(r.downloaded)).
			Str("size", formatByteSize(size)).
			Int("workers", workers).
			Msg("state snapshot availability confirmed")
	}

	if r.downloaded == size {
		n.logDownloadStateProgress(peer, r.blockRef, r.downloaded, size, workers, r.lastProgress, r.lastProgressDownloaded)
		return r, nil
	}

	r.lastProgress = time.Now()
	r.lastProgressDownloaded = r.downloaded
	r.start()
	return r, nil
}

func (r *persistentStateChunkReader) Read(dst []byte) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	if r.readOffset >= r.size {
		return 0, io.EOF
	}

	for len(r.current) == 0 {
		if r.nextChunk < len(r.chunks) && r.chunks[r.nextChunk] != nil {
			chunk := r.chunks[r.nextChunk]
			r.current = chunk
			r.chunks[r.nextChunk] = nil
			r.nextChunk++
			break
		}

		if r.results == nil {
			if r.err != nil {
				return 0, r.err
			}
			return 0, io.ErrUnexpectedEOF
		}

		res, ok := <-r.results
		if !ok {
			r.results = nil
			continue
		}
		if res.err != nil {
			r.fail(res)
			return 0, r.err
		}

		r.addDownloadedChunk(res)
		r.logProgress()
	}

	n := copy(dst, r.current)
	r.recordRead(r.current[:n])
	r.current = r.current[n:]
	if len(r.current) == 0 {
		r.current = nil
	}
	r.readOffset += int64(n)
	return n, nil
}

func (r *persistentStateChunkReader) Len() int {
	left := r.size - r.readOffset
	if left <= 0 {
		return 0
	}
	return int(left)
}

func (r *persistentStateChunkReader) Close() {
	r.cancel()
	r.current = nil
	for i := range r.chunks {
		r.chunks[i] = nil
	}
	r.chunks = nil
}

func (r *persistentStateChunkReader) DownloadErr() error {
	return r.err
}

func (r *persistentStateChunkReader) FileHash() []byte {
	return r.hash.Sum(nil)
}

func (r *persistentStateChunkReader) Prefix() []byte {
	return append([]byte(nil), r.prefix...)
}

func (r *persistentStateChunkReader) start() {
	jobs := make(chan int)
	results := make(chan stateChunkResult, r.workers)
	r.results = results
	downloaded := make([]bool, len(r.chunks))
	for idx, chunk := range r.chunks {
		downloaded[idx] = chunk != nil
	}

	var wg sync.WaitGroup
	for i := 0; i < r.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				res := r.downloader.downloadPersistentStateChunk(r.ctx, r.peer, r.id, idx, r.size, r.chunkLimiter)
				select {
				case results <- res:
				case <-r.ctx.Done():
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
		for idx := range downloaded {
			if downloaded[idx] {
				continue
			}
			select {
			case jobs <- idx:
			case <-r.ctx.Done():
				return
			}
		}
	}()

	go func() {
		wg.Wait()
		close(results)
	}()
}

func (r *persistentStateChunkReader) addDownloadedChunk(chunk stateChunkResult) {
	idx := int(chunk.offset / persistentStateChunkSize)
	if idx < 0 || idx >= len(r.chunks) || r.chunks[idx] != nil {
		return
	}

	r.chunks[idx] = chunk.data
	r.downloaded += int64(len(chunk.data))
}

func (r *persistentStateChunkReader) fail(res stateChunkResult) {
	if r.err != nil {
		return
	}
	r.err = res.err
	r.cancel()
	if errors.Is(res.err, context.Canceled) {
		return
	}

	r.node.log.Error().
		Err(res.err).
		Str("peer", r.peer.addr).
		Str("block", r.blockRef).
		Int64("offset", res.offset).
		Int64("chunk_size", res.chunkSize).
		Int("attempts", res.attempts).
		Dur("elapsed", res.elapsed).
		Msg("failed to download persistent state snapshot chunk")
}

func (r *persistentStateChunkReader) logProgress() {
	now := time.Now()
	if r.downloaded != r.size && now.Sub(r.lastProgress) < 5*time.Second {
		return
	}

	r.node.logDownloadStateProgress(r.peer, r.blockRef, r.downloaded, r.size, r.workers, r.lastProgress, r.lastProgressDownloaded)
	r.lastProgress = now
	r.lastProgressDownloaded = r.downloaded
}

func (r *persistentStateChunkReader) recordRead(data []byte) {
	if len(data) == 0 {
		return
	}

	_, _ = r.hash.Write(data)
	if len(r.prefix) >= persistentStateReadPrefixLimit {
		return
	}

	left := persistentStateReadPrefixLimit - len(r.prefix)
	if len(data) > left {
		data = data[:left]
	}
	r.prefix = append(r.prefix, data...)
}
