package liveview

import (
	"context"
	"errors"
	"sort"

	"github.com/xssnick/gton/service/shard"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

type historyIndexKind uint8

const (
	historyIndexLogicalTime historyIndexKind = iota
	historyIndexUnixTime
)

// historyIndexLookup is the concrete lookup strategy shared by the LT and
// unix-time indexes. Its switch-based methods keep the binary-search path on
// concrete slices; there is no interface dispatch or callback in the loop.
type historyIndexLookup struct {
	kind  historyIndexKind
	value uint64
}

func logicalTimeIndexLookup(lt uint64) historyIndexLookup {
	return historyIndexLookup{kind: historyIndexLogicalTime, value: lt}
}

func unixTimeIndexLookup(utime uint32) historyIndexLookup {
	return historyIndexLookup{kind: historyIndexUnixTime, value: uint64(utime)}
}

func (l historyIndexLookup) metaIsExact(meta *storage.BlockMeta) bool {
	if l.kind == historyIndexLogicalTime {
		return meta.EndLT == l.value
	}
	return meta.GenUTime == uint32(l.value)
}

func (l historyIndexLookup) backingBlock(ctx context.Context, backing Backing, key storage.BlockHistoryKey) (ton.BlockIDExt, error) {
	if l.kind == historyIndexLogicalTime {
		return backing.LookupBlockByLT(ctx, key, l.value)
	}
	return backing.LookupBlockByUnixTime(ctx, key, uint32(l.value))
}

func (l historyIndexLookup) backingBlockForPrefix(ctx context.Context, backing Backing, key storage.BlockHistoryKey) (ton.BlockIDExt, error) {
	prefix, ok := backing.(prefixBacking)
	if !ok {
		return l.backingBlock(ctx, backing, key)
	}
	if l.kind == historyIndexLogicalTime {
		return prefix.LookupBlockByLTForPrefix(ctx, key, l.value)
	}
	return prefix.LookupBlockByUnixTimeForPrefix(ctx, key, uint32(l.value))
}

func (s *Store) lookupBackingBlockByHistory(ctx context.Context, key storage.BlockHistoryKey, lookup historyIndexLookup) (ton.BlockIDExt, error) {
	block, err := lookup.backingBlock(ctx, s.backing, key)
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	if !s.backingBlockAllowed(block) {
		return ton.BlockIDExt{}, storage.ErrNotFound
	}
	return block, nil
}

func (s *Store) lookupBlockByHistoryForPrefixAfterCache(ctx context.Context, key storage.BlockHistoryKey, lookup historyIndexLookup) (ton.BlockIDExt, error) {
	best, bestErr := s.cachedBlockByHistoryForPrefix(key, lookup)
	block, err := lookup.backingBlockForPrefix(ctx, s.backing, key)
	if err == nil && s.backingBlockAllowed(block) {
		if errors.Is(bestErr, storage.ErrNotFound) || best.artifactFlushed {
			return block, nil
		}

		meta, metaErr := s.backing.BlockMeta(ctx, block)
		if metaErr != nil {
			return ton.BlockIDExt{}, metaErr
		}
		candidate := blockPrefixCandidate{block: block, exact: lookup.metaIsExact(meta)}
		if prefixCandidateBetter(candidate, best) {
			best = candidate
		}
	} else if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return ton.BlockIDExt{}, err
	}
	if bestErr != nil {
		return ton.BlockIDExt{}, bestErr
	}
	return best.block, nil
}

// The four direct cache probes stay specialized by index type. These methods
// are the common cache-hit path, and selecting a strategy before each binary
// search is measurable there. The shared strategy below owns the slower
// backing-store and cross-prefix workflows.
func (s *Store) cachedBlockByLT(key storage.BlockHistoryKey, lt uint64) (ton.BlockIDExt, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entries := s.ltIndex[liveHistoryKey{workchain: key.Workchain, shard: key.Shard}]
	idx := sort.Search(len(entries), func(i int) bool {
		return entries[i].endLT >= lt
	})
	if idx < len(entries) && liveLTIndexCovers(entries, idx, lt) {
		return cloneBlockID(entries[idx].block), nil
	}
	return ton.BlockIDExt{}, storage.ErrNotFound
}

func (s *Store) cachedBlockByUnixTime(key storage.BlockHistoryKey, utime uint32) (ton.BlockIDExt, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entries := s.unixIndex[liveHistoryKey{workchain: key.Workchain, shard: key.Shard}]
	idx := sort.Search(len(entries), func(i int) bool {
		return entries[i].genUTime >= utime
	})
	if idx >= len(entries) || idx == 0 && utime < entries[idx].genUTime {
		return ton.BlockIDExt{}, storage.ErrNotFound
	}
	return cloneBlockID(entries[idx].block), nil
}

// cachedDirectBlockByLTForPrefix answers from the live index only when the
// sliding window covers lt. Otherwise only the backing store can know the
// preceding history.
func (s *Store) cachedDirectBlockByLTForPrefix(key storage.BlockHistoryKey, lt uint64) (ton.BlockIDExt, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entries := s.ltIndex[liveHistoryKey{workchain: key.Workchain, shard: key.Shard}]
	idx := sort.Search(len(entries), func(i int) bool {
		return entries[i].endLT >= lt
	})
	if idx >= len(entries) || idx == 0 && (entries[idx].startLT == 0 || entries[idx].startLT >= lt) {
		return ton.BlockIDExt{}, storage.ErrNotFound
	}
	return cloneBlockID(entries[idx].block), nil
}

// cachedDirectBlockByUnixTimeForPrefix carries the same coverage requirement
// over to the unix-time index.
func (s *Store) cachedDirectBlockByUnixTimeForPrefix(key storage.BlockHistoryKey, utime uint32) (ton.BlockIDExt, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entries := s.unixIndex[liveHistoryKey{workchain: key.Workchain, shard: key.Shard}]
	idx := sort.Search(len(entries), func(i int) bool {
		return entries[i].genUTime >= utime
	})
	if idx >= len(entries) || idx == 0 && entries[0].genUTime >= utime {
		return ton.BlockIDExt{}, storage.ErrNotFound
	}
	return cloneBlockID(entries[idx].block), nil
}

func (s *Store) cachedBlockByHistoryForPrefix(key storage.BlockHistoryKey, lookup historyIndexLookup) (blockPrefixCandidate, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var best blockPrefixCandidate
	found := false
	maxDepth := uint32(60)
	if key.Workchain == masterchainID {
		maxDepth = 0
	}
	for depth := uint32(0); depth <= maxDepth; depth++ {
		shardID, err := shard.FromAccountPrefix(uint64(key.Shard), depth)
		if err != nil {
			return blockPrefixCandidate{}, err
		}
		historyKey := liveHistoryKey{workchain: key.Workchain, shard: shardID}

		var candidate blockPrefixCandidate
		if lookup.kind == historyIndexLogicalTime {
			entries := s.ltIndex[historyKey]
			idx := sort.Search(len(entries), func(i int) bool {
				return entries[i].endLT >= lookup.value
			})
			if idx >= len(entries) {
				continue
			}
			candidate = blockPrefixCandidate{block: entries[idx].block, exact: entries[idx].endLT == lookup.value}
		} else {
			utime := uint32(lookup.value)
			entries := s.unixIndex[historyKey]
			idx := sort.Search(len(entries), func(i int) bool {
				return entries[i].genUTime >= utime
			})
			if idx >= len(entries) {
				continue
			}
			candidate = blockPrefixCandidate{block: entries[idx].block, exact: entries[idx].genUTime == utime}
		}
		if !found || prefixCandidateBetter(candidate, best) {
			best = candidate
			found = true
		}
	}
	if !found {
		return blockPrefixCandidate{}, storage.ErrNotFound
	}
	return s.cachedPrefixCandidateLocked(best.block, best.exact), nil
}

func prefixCandidateBetter(candidate, current blockPrefixCandidate) bool {
	if candidate.exact != current.exact {
		return candidate.exact
	}
	if !candidate.exact && candidate.block.SeqNo != current.block.SeqNo {
		return candidate.block.SeqNo < current.block.SeqNo
	}

	candidateDepth, candidateErr := shard.PrefixLength(candidate.block.Shard)
	currentDepth, currentErr := shard.PrefixLength(current.block.Shard)
	if candidateErr != nil {
		return false
	}
	return currentErr != nil || candidateDepth < currentDepth
}
