package pebblestore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"

	"github.com/cockroachdb/pebble/v2"
	"github.com/xssnick/gton/service/shard"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
)

type blockHistoryIndexKind uint8

const (
	blockHistoryLogicalTime blockHistoryIndexKind = iota
	blockHistoryUnixTime
)

// blockHistoryLookup owns the index-specific key encoding and direct-hit
// policy. The prefix traversal remains concrete and allocation-free inside
// the loop; no interface or callback is invoked per history.
type blockHistoryLookup struct {
	kind  blockHistoryIndexKind
	value uint64
}

func logicalTimeHistoryLookup(lt uint64) blockHistoryLookup {
	return blockHistoryLookup{kind: blockHistoryLogicalTime, value: lt}
}

func unixTimeHistoryLookup(utime uint32) blockHistoryLookup {
	return blockHistoryLookup{kind: blockHistoryUnixTime, value: uint64(utime)}
}

func (l blockHistoryLookup) workchainPrefix(workchain int32) []byte {
	if l.kind == blockHistoryLogicalTime {
		return hotKeyBlockLTWorkchainPrefix(workchain)
	}
	return hotKeyBlockUTimeWorkchainPrefix(workchain)
}

func (l blockHistoryLookup) appendPrefix(buf []byte, key storage.BlockHistoryKey) []byte {
	if l.kind == blockHistoryLogicalTime {
		return appendHotKeyBlockLTPrefix(buf, key)
	}
	return appendHotKeyBlockUTimePrefix(buf, key)
}

func (l blockHistoryLookup) appendSeek(buf []byte, key storage.BlockHistoryKey) []byte {
	if l.kind == blockHistoryLogicalTime {
		return appendHotKeyBlockLTSeekGE(buf, key, l.value)
	}
	return appendHotKeyBlockUTimeSeekGE(buf, key, uint32(l.value))
}

func (l blockHistoryLookup) decodeRef(key []byte) (storage.BlockSeqRef, error) {
	if l.kind == blockHistoryLogicalTime {
		return decodeBlockLTIndexKey(key)
	}
	return decodeBlockUTimeIndexKey(key)
}

func (l blockHistoryLookup) indexedValue(key []byte, prefixLength int) uint64 {
	if l.kind == blockHistoryLogicalTime {
		return binary.BigEndian.Uint64(key[prefixLength : prefixLength+8])
	}
	return uint64(binary.BigEndian.Uint32(key[prefixLength : prefixLength+4]))
}

func (l blockHistoryLookup) lookupInHistory(
	iter *pebble.Iterator,
	key storage.BlockHistoryKey,
	prefixBuf, seekBuf *[32]byte,
) (blockHistoryIndexRef, error) {
	prefix := l.appendPrefix(prefixBuf[:0], key)
	seek := l.appendSeek(seekBuf[:0], key)
	ok := iter.SeekGE(seek)
	if ok && bytes.HasPrefix(iter.Key(), prefix) {
		indexedValue := l.indexedValue(iter.Key(), len(prefix))
		ref, err := l.decodeRef(iter.Key())
		if err != nil {
			return blockHistoryIndexRef{}, err
		}
		block, err := decodeBlockIDFromHashes(ref.Workchain, ref.Shard, ref.SeqNo, iter.Value())
		if err != nil {
			return blockHistoryIndexRef{}, err
		}
		return blockHistoryIndexRef{block: block, seqno: ref.SeqNo, exact: indexedValue == l.value}, nil
	}
	if err := iter.Error(); err != nil {
		return blockHistoryIndexRef{}, err
	}

	var previous bool
	if ok {
		previous = iter.Prev()
	} else {
		previous = iter.SeekLT(seek)
	}
	if previous && bytes.HasPrefix(iter.Key(), prefix) {
		return blockHistoryIndexRef{}, storage.ErrNotFound
	}
	if err := iter.Error(); err != nil {
		return blockHistoryIndexRef{}, err
	}
	return blockHistoryIndexRef{}, errBlockHistoryMissing
}

func (l blockHistoryLookup) directResult(
	snap *pebble.Snapshot,
	iter *pebble.Iterator,
	key storage.BlockHistoryKey,
	direct blockHistoryIndexRef,
	boundBuf *[32]byte,
) (ton.BlockIDExt, bool, error) {
	if l.kind == blockHistoryLogicalTime {
		meta, err := blockLookupMetaFromReader(snap, direct.block)
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			return ton.BlockIDExt{}, false, err
		}

		safe := key.Workchain == -1
		if err == nil && meta.startLT != 0 && meta.startLT < l.value {
			safe = true
		}
		if !safe {
			return ton.BlockIDExt{}, false, nil
		}
		if err != nil {
			return ton.BlockIDExt{}, false, err
		}
		if meta.flags&storage.BlockMetaHasServedFull == 0 {
			return ton.BlockIDExt{}, false, storage.ErrNotFound
		}
		return direct.block, true, nil
	}

	safe := key.Workchain == -1
	if !safe {
		var err error
		safe, err = unixTimeHitIsSeqnoBracketed(iter, key, direct.seqno, boundBuf)
		if err != nil {
			return ton.BlockIDExt{}, false, err
		}
	}
	if !safe {
		return ton.BlockIDExt{}, false, nil
	}
	if err := requireServedFullBlock(snap, direct.block); err != nil {
		return ton.BlockIDExt{}, false, err
	}
	return direct.block, true, nil
}

func (s *Store) lookupBlockByHistoryForPrefix(ctx context.Context, key storage.BlockHistoryKey, lookup blockHistoryLookup) (ton.BlockIDExt, error) {
	prefix := lookup.workchainPrefix(key.Workchain)

	db, err := s.acquireHotDB(ctx)
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	defer s.releaseHotDB()

	snap := db.NewSnapshot()
	defer func() { _ = snap.Close() }()

	iter, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	defer func() { _ = iter.Close() }()

	var prefixBuf, seekBuf, boundBuf [32]byte
	direct, directErr := lookup.lookupInHistory(iter, key, &prefixBuf, &seekBuf)
	if directErr == nil {
		block, safe, err := lookup.directResult(snap, iter, key, direct, &boundBuf)
		if err != nil {
			return ton.BlockIDExt{}, err
		}
		if safe {
			return block, nil
		}
	} else if !errors.Is(directErr, storage.ErrNotFound) && !errors.Is(directErr, errBlockHistoryMissing) {
		return ton.BlockIDExt{}, directErr
	}

	var best blockHistoryIndexRef
	found := false
	historyFound := false
	maxDepth := uint32(60)
	if key.Workchain == -1 {
		maxDepth = 0
	}
	for depth := uint32(0); depth <= maxDepth; depth++ {
		select {
		case <-ctx.Done():
			return ton.BlockIDExt{}, ctx.Err()
		default:
		}

		candidateShard, err := shard.FromAccountPrefix(uint64(key.Shard), depth)
		if err != nil {
			return ton.BlockIDExt{}, err
		}
		candidateKey := storage.BlockHistoryKey{
			Workchain: key.Workchain,
			Shard:     candidateShard,
		}
		ref, err := lookup.lookupInHistory(iter, candidateKey, &prefixBuf, &seekBuf)
		if errors.Is(err, errBlockHistoryMissing) {
			if historyFound {
				break
			}
			continue
		}
		historyFound = true
		if errors.Is(err, storage.ErrNotFound) {
			if iterErr := iter.Error(); iterErr != nil {
				return ton.BlockIDExt{}, iterErr
			}
			continue
		}
		if err != nil {
			return ton.BlockIDExt{}, err
		}
		if ref.exact {
			if err = requireServedFullBlock(snap, ref.block); err != nil {
				return ton.BlockIDExt{}, err
			}
			return ref.block, nil
		}

		if !found || best.seqno > ref.seqno {
			best = ref
			found = true
		}
	}
	if err = iter.Error(); err != nil {
		return ton.BlockIDExt{}, err
	}
	if !found {
		return ton.BlockIDExt{}, storage.ErrNotFound
	}
	if err = requireServedFullBlock(snap, best.block); err != nil {
		return ton.BlockIDExt{}, err
	}
	return best.block, nil
}
