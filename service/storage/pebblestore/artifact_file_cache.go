package pebblestore

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/xssnick/gton/service/storage"
)

type artifactFileCache struct {
	mu        sync.Mutex
	notify    chan struct{}
	maxOpen   int
	openCount int
	entries   map[string]*artifactFileEntry
	opening   map[string]*artifactFileOpen
	order     *list.List
	closed    bool
}

type artifactFileEntry struct {
	path           string
	file           *os.File
	size           int64
	refs           int
	closeOnRelease bool
	elem           *list.Element
}

type artifactFileOpen struct {
	done chan struct{}
}

type artifactFileHandle struct {
	cache *artifactFileCache
	entry *artifactFileEntry
}

func newArtifactFileCache(maxOpen int) *artifactFileCache {
	if maxOpen < 1 {
		maxOpen = 1
	}
	return &artifactFileCache{
		notify:  make(chan struct{}),
		maxOpen: maxOpen,
		entries: map[string]*artifactFileEntry{},
		opening: map[string]*artifactFileOpen{},
		order:   list.New(),
	}
}

func (c *artifactFileCache) readRange(ctx context.Context, path string, offset int64, size int64, minFileSize int64) ([]byte, error) {
	handle, err := c.acquire(ctx, path)
	if err != nil {
		if isMissingArtifactError(err) {
			return nil, storage.ErrNotFound
		}
		return nil, err
	}
	defer handle.release()

	end := offset + size
	fileSize := handle.size()
	if fileSize < minFileSize || end > fileSize {
		fileSize, err = handle.refreshSize()
		if err != nil {
			if isMissingArtifactError(err) {
				return nil, storage.ErrNotFound
			}
			return nil, err
		}
	}
	if fileSize < minFileSize || end > fileSize {
		return nil, storage.ErrNotFound
	}

	data := make([]byte, size)
	if _, err = handle.entry.file.ReadAt(data, offset); err != nil {
		if isMissingArtifactError(err) {
			return nil, storage.ErrNotFound
		}
		return nil, err
	}
	return data, nil
}

func (c *artifactFileCache) acquire(ctx context.Context, path string) (*artifactFileHandle, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return nil, errPebbleClosed
		}
		if entry := c.entries[path]; entry != nil {
			entry.refs++
			c.order.MoveToFront(entry.elem)
			c.mu.Unlock()
			return &artifactFileHandle{cache: c, entry: entry}, nil
		}
		if opening := c.opening[path]; opening != nil {
			done := opening.done
			c.mu.Unlock()
			select {
			case <-done:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if c.reserveSlotLocked() {
			opening := &artifactFileOpen{done: make(chan struct{})}
			c.opening[path] = opening
			c.mu.Unlock()

			return c.openReserved(ctx, path, opening)
		}
		notify := c.notify
		c.mu.Unlock()

		select {
		case <-notify:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (c *artifactFileCache) openReserved(ctx context.Context, path string, opening *artifactFileOpen) (*artifactFileHandle, error) {
	file, err := os.Open(path)
	var fileSize int64
	if err == nil {
		var stat os.FileInfo
		stat, err = file.Stat()
		if err != nil {
			_ = file.Close()
		} else {
			fileSize = stat.Size()
		}
	}

	c.mu.Lock()
	delete(c.opening, path)
	defer func() {
		close(opening.done)
		c.broadcastLocked()
		c.mu.Unlock()
	}()

	if err != nil {
		c.openCount--
		return nil, err
	}
	if c.closed {
		c.openCount--
		c.mu.Unlock()
		closeErr := file.Close()
		c.mu.Lock()
		if closeErr != nil {
			return nil, closeErr
		}
		return nil, errPebbleClosed
	}
	select {
	case <-ctx.Done():
		c.openCount--
		c.mu.Unlock()
		closeErr := file.Close()
		c.mu.Lock()
		if closeErr != nil {
			return nil, closeErr
		}
		return nil, ctx.Err()
	default:
	}

	entry := &artifactFileEntry{
		path: path,
		file: file,
		size: fileSize,
		refs: 1,
	}
	entry.elem = c.order.PushFront(entry)
	c.entries[path] = entry
	return &artifactFileHandle{cache: c, entry: entry}, nil
}

func (c *artifactFileCache) reserveSlotLocked() bool {
	if c.openCount < c.maxOpen {
		c.openCount++
		return true
	}
	if !c.evictIdleLocked() {
		return false
	}
	c.openCount++
	return true
}

func (c *artifactFileCache) evictIdleLocked() bool {
	for elem := c.order.Back(); elem != nil; elem = elem.Prev() {
		entry := elem.Value.(*artifactFileEntry)
		if entry.refs != 0 {
			continue
		}
		c.removeEntryLocked(entry)
		_ = entry.file.Close()
		return true
	}
	return false
}

func (h *artifactFileHandle) release() {
	h.cache.release(h.entry)
}

func (h *artifactFileHandle) size() int64 {
	h.cache.mu.Lock()
	defer h.cache.mu.Unlock()
	return h.entry.size
}

func (h *artifactFileHandle) refreshSize() (int64, error) {
	stat, err := h.entry.file.Stat()
	if err != nil {
		return 0, err
	}

	h.cache.mu.Lock()
	h.entry.size = stat.Size()
	h.cache.mu.Unlock()
	return stat.Size(), nil
}

func (c *artifactFileCache) release(entry *artifactFileEntry) {
	c.mu.Lock()
	if entry.refs > 0 {
		entry.refs--
	}
	shouldClose := entry.refs == 0 && (c.closed || entry.closeOnRelease)
	if shouldClose {
		c.removeEntryLocked(entry)
	}
	c.broadcastLocked()
	c.mu.Unlock()

	if shouldClose {
		_ = entry.file.Close()
	}
}

func (c *artifactFileCache) close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true

	var files []*os.File
	for elem := c.order.Front(); elem != nil; {
		next := elem.Next()
		entry := elem.Value.(*artifactFileEntry)
		if entry.refs == 0 {
			c.removeEntryLocked(entry)
			files = append(files, entry.file)
		} else {
			entry.closeOnRelease = true
		}
		elem = next
	}
	c.broadcastLocked()
	c.mu.Unlock()

	var err error
	for _, file := range files {
		if closeErr := file.Close(); closeErr != nil {
			err = errors.Join(err, artifactFileCloseError(file.Name(), closeErr))
		}
	}
	return err
}

func (c *artifactFileCache) removeEntryLocked(entry *artifactFileEntry) {
	if entry.elem != nil {
		c.order.Remove(entry.elem)
		entry.elem = nil
	}
	if c.entries[entry.path] == entry {
		delete(c.entries, entry.path)
		c.openCount--
	}
}

func (c *artifactFileCache) broadcastLocked() {
	close(c.notify)
	c.notify = make(chan struct{})
}

func (s *Store) readArtifactFileRange(ctx context.Context, path string, offset int64, size int64, minFileSize int64) ([]byte, error) {
	if s.artifactFiles != nil {
		return s.artifactFiles.readRange(ctx, path, offset, size, minFileSize)
	}

	file, err := os.Open(path)
	if err != nil {
		if isMissingArtifactError(err) {
			return nil, storage.ErrNotFound
		}
		return nil, err
	}
	defer func() { _ = file.Close() }()

	stat, err := file.Stat()
	if err != nil {
		if isMissingArtifactError(err) {
			return nil, storage.ErrNotFound
		}
		return nil, err
	}
	if stat.Size() < minFileSize || offset+size > stat.Size() {
		return nil, storage.ErrNotFound
	}

	data := make([]byte, size)
	if _, err = file.ReadAt(data, offset); err != nil {
		if isMissingArtifactError(err) {
			return nil, storage.ErrNotFound
		}
		return nil, err
	}
	return data, nil
}

func artifactRangeOverflow(offset int64, size int64) bool {
	const maxInt64 = int64(^uint64(0) >> 1)
	return offset > maxInt64-size
}

func artifactFileCloseError(path string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("close artifact file %s: %w", path, err)
}
