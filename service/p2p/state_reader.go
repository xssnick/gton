package p2p

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"sync"
	"time"
)

const (
	persistentStateReadPrefixLimit  = 64
	persistentStateChunkWindowScale = 2
)

type bufferedPersistentStateChunk struct {
	data       []byte
	windowSlot bool
}

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
	chunkCount   int64
	chunkLimiter chan struct{}

	chunks            map[int64]bufferedPersistentStateChunk
	current           []byte
	currentWindowSlot bool
	nextChunk         int64
	readOffset        int64
	downloaded        int64
	window            chan struct{}

	results chan stateChunkResult
	done    chan struct{}
	err     error

	hash   hash.Hash
	prefix []byte

	lastProgress           time.Time
	lastProgressDownloaded int64
}

func (d persistentStateSnapshotDownloader) newPersistentStateChunkReader(ctx context.Context, peer *overlayPeer, id PersistentStateIDV2, size int64, workers int, chunkLimiter chan struct{}, seed *persistentStateChunkSeed) (*persistentStateChunkReader, error) {
	n := d.node

	chunkCount, err := persistentStateChunkCount(size)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)
	now := time.Now()
	startedAt := now
	if seed != nil && seed.elapsed > 0 {
		startedAt = startedAt.Add(-seed.elapsed)
	}
	bufferedChunks := workers * persistentStateChunkWindowScale
	if seed != nil {
		bufferedChunks += len(seed.chunks)
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
		chunkCount:   chunkCount,
		chunkLimiter: chunkLimiter,
		chunks:       make(map[int64]bufferedPersistentStateChunk, bufferedChunks),
		hash:         sha256.New(),
		lastProgress: startedAt,
	}

	if seed != nil {
		for _, chunk := range seed.chunks {
			if err = r.addDownloadedChunk(chunk, false); err != nil {
				cancel()
				return nil, err
			}
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

		if err = r.addDownloadedChunk(first, false); err != nil {
			cancel()
			return nil, err
		}

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
	if err := r.waitCurrentChunk(); err != nil {
		return 0, err
	}

	n := copy(dst, r.current)
	r.recordRead(r.current[:n])
	r.current = r.current[n:]
	if len(r.current) == 0 {
		r.current = nil
		r.releaseCurrentWindowSlot()
	}
	r.readOffset += int64(n)
	return n, nil
}

// WriteTo streams every remaining chunk to w with a single Write per chunk,
// letting io.Copy skip its generic 32KB shuttle buffer. Output, hashing and
// prefix capture are byte-identical to draining the reader via Read.
func (r *persistentStateChunkReader) WriteTo(w io.Writer) (int64, error) {
	var written int64
	for r.readOffset < r.size {
		if err := r.waitCurrentChunk(); err != nil {
			return written, err
		}

		chunk := r.current
		n, err := w.Write(chunk)
		if n > 0 {
			r.recordRead(chunk[:n])
			r.readOffset += int64(n)
			written += int64(n)
		}
		if n == len(chunk) {
			r.current = nil
			r.releaseCurrentWindowSlot()
		} else if n > 0 {
			r.current = chunk[n:]
		}
		if err != nil {
			return written, err
		}
		if n < len(chunk) {
			return written, io.ErrShortWrite
		}
	}
	return written, nil
}

// waitCurrentChunk fills r.current with the next sequential chunk, consuming
// download results until it becomes available.
func (r *persistentStateChunkReader) waitCurrentChunk() error {
	for len(r.current) == 0 {
		if chunk, ok := r.chunks[r.nextChunk]; ok {
			r.current = chunk.data
			r.currentWindowSlot = chunk.windowSlot
			delete(r.chunks, r.nextChunk)
			r.nextChunk++
			break
		}

		if r.results == nil {
			if r.err != nil {
				return r.err
			}
			return io.ErrUnexpectedEOF
		}

		res, ok := <-r.results
		if !ok {
			r.results = nil
			continue
		}
		if res.err != nil {
			r.fail(res)
			return r.err
		}

		if err := r.addDownloadedChunk(res, true); err != nil {
			res.err = err
			r.fail(res)
			return r.err
		}
		r.logProgress()
	}
	return nil
}

func (r *persistentStateChunkReader) Close() {
	r.cancel()
	if r.done != nil {
		<-r.done
	}

	r.current = nil
	r.currentWindowSlot = false
	r.chunks = nil
	r.results = nil
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
	jobs := make(chan int64)
	results := make(chan stateChunkResult, r.workers)
	r.results = results
	r.done = make(chan struct{})
	r.window = make(chan struct{}, r.workers*persistentStateChunkWindowScale)
	seeded := make(map[int64]struct{}, len(r.chunks))
	for idx := range r.chunks {
		seeded[idx] = struct{}{}
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

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(jobs)
		for idx := int64(0); idx < r.chunkCount; idx++ {
			if _, ok := seeded[idx]; ok {
				continue
			}

			select {
			case r.window <- struct{}{}:
			case <-r.ctx.Done():
				return
			}

			select {
			case jobs <- idx:
			case <-r.ctx.Done():
				<-r.window
				return
			}
		}
	}()

	go func() {
		wg.Wait()
		close(results)
		close(r.done)
	}()
}

func (r *persistentStateChunkReader) addDownloadedChunk(chunk stateChunkResult, windowSlot bool) error {
	if chunk.offset < 0 || chunk.offset%persistentStateChunkSize != 0 {
		return fmt.Errorf("invalid persistent state chunk offset %d", chunk.offset)
	}

	idx := chunk.offset / persistentStateChunkSize
	if idx >= r.chunkCount {
		return fmt.Errorf("persistent state chunk index %d exceeds count %d", idx, r.chunkCount)
	}
	if _, ok := r.chunks[idx]; ok {
		return fmt.Errorf("duplicate persistent state chunk at offset %d", chunk.offset)
	}

	r.chunks[idx] = bufferedPersistentStateChunk{
		data:       chunk.data,
		windowSlot: windowSlot,
	}
	r.downloaded += int64(len(chunk.data))
	return nil
}

func (r *persistentStateChunkReader) releaseCurrentWindowSlot() {
	if !r.currentWindowSlot {
		return
	}

	<-r.window
	r.currentWindowSlot = false
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
