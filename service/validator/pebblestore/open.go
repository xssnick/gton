package pebblestore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"

	"github.com/cockroachdb/pebble/v2"
)

const (
	schemaVersion    = uint32(1)
	defaultCacheSize = int64(8 << 20)
	defaultQueueSize = 1024
)

var (
	// ErrUnversionedDatabase reports a nonempty database without the required
	// schema marker.
	ErrUnversionedDatabase = errors.New("validator pebblestore: nonempty unversioned database")
	// ErrUnsupportedSchema reports a schema marker this implementation cannot
	// safely read.
	ErrUnsupportedSchema = errors.New("validator pebblestore: unsupported schema version")
)

// Options configures the validator store rooted directly at Dir.
type Options struct {
	Dir       string
	CacheSize int64
	QueueSize int
}

// Open opens the WAL-backed metadata database, candidate packs and their
// bounded writer.
func Open(opts Options) (*Store, error) {
	if opts.Dir == "" {
		return nil, errors.New("validator pebblestore: dir is empty")
	}
	if opts.CacheSize < 0 {
		return nil, errors.New("validator pebblestore: cache size is negative")
	}
	if opts.QueueSize < 0 {
		return nil, errors.New("validator pebblestore: queue size is negative")
	}
	if opts.CacheSize == 0 {
		opts.CacheSize = defaultCacheSize
	}
	if opts.QueueSize == 0 {
		opts.QueueSize = defaultQueueSize
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("validator pebblestore: create dir: %w", err)
	}

	cache := pebble.NewCache(opts.CacheSize)
	db, err := pebble.Open(opts.Dir, &pebble.Options{
		Cache:      cache,
		DisableWAL: false,
		CompactionConcurrencyRange: func() (int, int) {
			return 1, 1
		},
	})
	if err != nil {
		cache.Unref()

		return nil, fmt.Errorf("validator pebblestore: open: %w", err)
	}
	if err = ensureSchema(db); err != nil {
		closeErr := db.Close()
		cache.Unref()

		return nil, errors.Join(err, closeErr)
	}

	store := &Store{
		db:         db,
		cache:      cache,
		queue:      make(chan writeRequest, opts.QueueSize),
		writerDone: make(chan struct{}),
		closeDone:  make(chan struct{}),
	}
	store.validator = &ValidatorStore{
		store:          store,
		candidatePacks: newCandidatePackStore(opts.Dir),
		deleting:       make(map[storageNamespace]struct{}),
		deleted:        make(map[storageNamespace]struct{}),
		journals:       make(map[storageNamespace]*journal),
	}
	store.collator = &CollatorStore{
		store:    store,
		deleting: make(map[[32]byte]struct{}),
		deleted:  make(map[[32]byte]struct{}),
	}
	go store.runWriter()

	return store, nil
}

func ensureSchema(db *pebble.DB) error {
	value, closer, err := db.Get(schemaKey)
	if err == nil {
		defer closer.Close()

		if len(value) != 4 {
			return fmt.Errorf("%w: malformed marker", ErrUnsupportedSchema)
		}
		version := binary.LittleEndian.Uint32(value)
		if version != schemaVersion {
			return fmt.Errorf("%w: got %d, want %d", ErrUnsupportedSchema, version, schemaVersion)
		}

		return nil
	}
	if !errors.Is(err, pebble.ErrNotFound) {
		return fmt.Errorf("validator pebblestore: read schema: %w", err)
	}

	iter, err := db.NewIter(nil)
	if err != nil {
		return fmt.Errorf("validator pebblestore: inspect schema: %w", err)
	}
	isNonempty := iter.First()
	iterErr := errors.Join(iter.Error(), iter.Close())
	if iterErr != nil {
		return fmt.Errorf("validator pebblestore: inspect schema: %w", iterErr)
	}
	if isNonempty {
		return ErrUnversionedDatabase
	}

	version := binary.LittleEndian.AppendUint32(nil, schemaVersion)
	if err = db.Set(schemaKey, version, pebble.Sync); err != nil {
		return fmt.Errorf("validator pebblestore: initialize schema: %w", err)
	}

	return nil
}
