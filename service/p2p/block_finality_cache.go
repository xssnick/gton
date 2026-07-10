package p2p

import (
	"container/list"
	"fmt"
	"sync"
	"time"

	"github.com/xssnick/gton/service/blockproof"
	tnstore "github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	blockFinalityCacheTTL      = 3 * time.Minute
	blockFinalityCacheMaxBytes = 512 << 20
	blockFinalityCacheMaxItems = 4096
	blockFinalityCacheOverhead = 256
	blockFinalityBroadcastKind = "tonNode.blockFinalityBroadcast"
)

type blockFinalityCache struct {
	mu sync.Mutex

	ttl      time.Duration
	maxBytes int64
	maxItems int

	candidates map[tnstore.BlockRootHash]*blockFinalityCandidateEntry
	finalities map[tnstore.BlockRootHash]*blockFinalityEntry
	assembled  map[tnstore.BlockRootHash]*blockFinalityAssembledEntry
	order      *list.List
	bytes      int64
}

type blockFinalityCacheEntry interface {
	blockFinalityCacheExpiresAt() time.Time
}

type blockFinalityCandidateEntry struct {
	key       tnstore.BlockRootHash
	element   *list.Element
	block     blockFinalityCandidate
	expiresAt time.Time
	bytes     int64
}

type blockFinalityCandidate struct {
	id                    ton.BlockIDExt
	blockRoot             *cell.Cell
	blockBOC              []byte
	meta                  *tnstore.BlockMeta
	stateUpdate           *cell.Cell
	sourcePeerID          PeerID
	signaturesVerifiedKey []byte
}

type blockFinalityEntry struct {
	key                   tnstore.BlockRootHash
	element               *list.Element
	block                 ton.BlockIDExt
	signaturesCell        *cell.Cell
	signaturesVerifiedKey []byte
	sourcePeerID          PeerID
	expiresAt             time.Time
	bytes                 int64
}

type blockFinalityAssembledEntry struct {
	key       tnstore.BlockRootHash
	element   *list.Element
	expiresAt time.Time
	bytes     int64
}

type checkedBlockFinality struct {
	block                 ton.BlockIDExt
	signatures            *blockproof.ValidatorSignatureSet
	signaturesCell        *cell.Cell
	signaturesVerifiedKey []byte
	sourcePeerID          PeerID
	payloadBytes          int
}

func newBlockFinalityCache(ttl time.Duration, maxBytes int64, maxItems int) *blockFinalityCache {
	return &blockFinalityCache{
		ttl:        ttl,
		maxBytes:   maxBytes,
		maxItems:   maxItems,
		candidates: map[tnstore.BlockRootHash]*blockFinalityCandidateEntry{},
		finalities: map[tnstore.BlockRootHash]*blockFinalityEntry{},
		assembled:  map[tnstore.BlockRootHash]*blockFinalityAssembledEntry{},
		order:      list.New(),
	}
}

func (c *blockFinalityCache) StoreCandidate(downloaded DownloadedBlock, now time.Time) ([]DownloadedBlock, error) {
	if err := validateBlockFinalityCandidate(downloaded); err != nil {
		return nil, err
	}
	if c.maxItems <= 0 || c.maxBytes <= 0 {
		return nil, fmt.Errorf("block finality cache is disabled")
	}

	key := tnstore.BlockKey(downloaded.ID)
	entry := &blockFinalityCandidateEntry{
		key:       key,
		block:     blockFinalityCandidateFrom(downloaded),
		expiresAt: now.Add(c.ttl),
		bytes:     blockFinalityCandidateBytes(downloaded),
	}
	if entry.bytes > c.maxBytes {
		return nil, fmt.Errorf("block candidate %s is too large for block finality cache: %d > %d", formatBlockRef(downloaded.ID), entry.bytes, c.maxBytes)
	}

	var finality *blockFinalityEntry

	c.mu.Lock()

	c.pruneExpiredLocked(now)
	if c.assembled[key] != nil {
		c.mu.Unlock()
		return nil, nil
	}
	if old := c.candidates[key]; old != nil {
		c.deleteEntryLocked(old)
	}
	entry.element = c.order.PushBack(entry)
	c.candidates[key] = entry
	c.bytes += entry.bytes

	if cached := c.finalities[key]; cached != nil {
		finality = cached
		c.reserveAssemblyLocked(key, now)
	}
	c.pruneOverflowLocked()
	c.mu.Unlock()

	if finality == nil {
		return nil, nil
	}
	assembled, err := assembleBlockFinality(entry, finality)
	if err != nil {
		c.forgetAssemblyReservation(key)
		return nil, err
	}
	return assembled, nil
}

func (c *blockFinalityCache) StoreFinality(finality checkedBlockFinality, now time.Time) ([]DownloadedBlock, error) {
	if err := validateBlockFinality(finality); err != nil {
		return nil, err
	}
	if c.maxItems <= 0 || c.maxBytes <= 0 {
		return nil, fmt.Errorf("block finality cache is disabled")
	}

	key := tnstore.BlockKey(finality.block)
	entry := &blockFinalityEntry{
		key:                   key,
		block:                 cloneBlockID(finality.block),
		signaturesCell:        finality.signaturesCell,
		signaturesVerifiedKey: append([]byte(nil), finality.signaturesVerifiedKey...),
		sourcePeerID:          finality.sourcePeerID,
		expiresAt:             now.Add(c.ttl),
		bytes:                 blockFinalityBytes(finality),
	}
	if entry.bytes > c.maxBytes {
		return nil, fmt.Errorf("block finality %s is too large for block finality cache: %d > %d", formatBlockRef(finality.block), entry.bytes, c.maxBytes)
	}

	var candidate *blockFinalityCandidateEntry

	c.mu.Lock()

	c.pruneExpiredLocked(now)
	if c.assembled[key] != nil {
		c.mu.Unlock()
		return nil, nil
	}
	if old := c.finalities[key]; old != nil {
		c.deleteEntryLocked(old)
	}
	entry.element = c.order.PushBack(entry)
	c.finalities[key] = entry
	c.bytes += entry.bytes

	if cached := c.candidates[key]; cached != nil {
		candidate = cached
		c.reserveAssemblyLocked(key, now)
	}
	c.pruneOverflowLocked()
	c.mu.Unlock()

	if candidate == nil {
		return nil, nil
	}
	assembled, err := assembleBlockFinality(candidate, entry)
	if err != nil {
		c.forgetAssemblyReservation(key)
		return nil, err
	}
	return assembled, nil
}

func (c *blockFinalityCache) reserveAssemblyLocked(key tnstore.BlockRootHash, now time.Time) {
	if entry := c.candidates[key]; entry != nil {
		c.deleteEntryLocked(entry)
	}
	if entry := c.finalities[key]; entry != nil {
		c.deleteEntryLocked(entry)
	}
	if old := c.assembled[key]; old != nil {
		c.deleteEntryLocked(old)
	}
	entry := &blockFinalityAssembledEntry{
		key:       key,
		expiresAt: now.Add(c.ttl),
		bytes:     blockFinalityCacheOverhead,
	}
	entry.element = c.order.PushBack(entry)
	c.assembled[key] = entry
	c.bytes += entry.bytes
}

func (c *blockFinalityCache) forgetAssemblyReservation(key tnstore.BlockRootHash) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry := c.assembled[key]; entry != nil {
		c.deleteEntryLocked(entry)
	}
}

func assembleBlockFinality(candidate *blockFinalityCandidateEntry, finality *blockFinalityEntry) ([]DownloadedBlock, error) {
	if !candidate.block.id.Equals(&finality.block) {
		return nil, fmt.Errorf("block finality candidate %s finality belongs to %s", formatBlockRef(candidate.block.id), formatBlockRef(finality.block))
	}

	proofRoot, err := blockproof.BroadcastProofRoot(candidate.block.id, candidate.block.blockRoot)
	if err != nil {
		return nil, err
	}

	var proof *cell.Cell
	var proofBOC []byte
	isLink := !isMasterchainBlock(candidate.block.id)
	if isLink {
		proof, proofBOC, err = blockproof.LinkFromRoot(candidate.block.id, proofRoot)
	} else {
		proof, proofBOC, err = blockproof.ProofFromRoot(candidate.block.id, proofRoot, finality.signaturesCell)
	}
	if err != nil {
		return nil, err
	}

	block := candidate.block.downloaded(proof, proofBOC, isLink, finality)
	return []DownloadedBlock{block}, nil
}

func (c *blockFinalityCache) pruneExpiredLocked(now time.Time) {
	for elem := c.order.Front(); elem != nil; {
		entry := elem.Value.(blockFinalityCacheEntry)
		if entry.blockFinalityCacheExpiresAt().After(now) {
			return
		}
		next := elem.Next()
		c.deleteEntryLocked(entry)
		elem = next
	}
}

func (c *blockFinalityCache) pruneOverflowLocked() {
	for c.order.Len() > c.maxItems || c.bytes > c.maxBytes {
		elem := c.order.Front()
		if elem == nil {
			return
		}
		c.deleteEntryLocked(elem.Value.(blockFinalityCacheEntry))
	}
}

func (c *blockFinalityCache) deleteEntryLocked(entry blockFinalityCacheEntry) {
	switch typed := entry.(type) {
	case *blockFinalityCandidateEntry:
		delete(c.candidates, typed.key)
		c.order.Remove(typed.element)
		c.bytes -= typed.bytes
		typed.element = nil
	case *blockFinalityEntry:
		delete(c.finalities, typed.key)
		c.order.Remove(typed.element)
		c.bytes -= typed.bytes
		typed.element = nil
	case *blockFinalityAssembledEntry:
		delete(c.assembled, typed.key)
		c.order.Remove(typed.element)
		c.bytes -= typed.bytes
		typed.element = nil
	default:
		panic(fmt.Sprintf("unknown block finality cache entry %T", entry))
	}
}

func (e *blockFinalityCandidateEntry) blockFinalityCacheExpiresAt() time.Time {
	return e.expiresAt
}

func (e *blockFinalityEntry) blockFinalityCacheExpiresAt() time.Time {
	return e.expiresAt
}

func (e *blockFinalityAssembledEntry) blockFinalityCacheExpiresAt() time.Time {
	return e.expiresAt
}

func validateBlockFinalityCandidate(downloaded DownloadedBlock) error {
	if !downloaded.VerifiedRootHash {
		return fmt.Errorf("block candidate %s is not hash verified", formatBlockRef(downloaded.ID))
	}
	if len(downloaded.BlockBOC) == 0 {
		return fmt.Errorf("block candidate %s has empty block data", formatBlockRef(downloaded.ID))
	}
	if downloaded.Block == nil {
		return fmt.Errorf("block candidate %s has no parsed block root", formatBlockRef(downloaded.ID))
	}
	if downloaded.Meta == nil {
		return fmt.Errorf("block candidate %s has no metadata", formatBlockRef(downloaded.ID))
	}
	if downloaded.StateUpdate == nil {
		return fmt.Errorf("block candidate %s has no state update", formatBlockRef(downloaded.ID))
	}
	return nil
}

func validateBlockFinality(finality checkedBlockFinality) error {
	if isMasterchainBlock(finality.block) && !finality.signatures.Final() {
		return fmt.Errorf("masterchain block finality %s has non-final signatures", formatBlockRef(finality.block))
	}
	if isMasterchainBlock(finality.block) && finality.signaturesCell == nil {
		return fmt.Errorf("masterchain block finality %s has no serialized signatures", formatBlockRef(finality.block))
	}
	if len(finality.signaturesVerifiedKey) == 0 {
		return fmt.Errorf("block finality %s has no verified signature key", formatBlockRef(finality.block))
	}
	return nil
}

func blockFinalityCandidateFrom(downloaded DownloadedBlock) blockFinalityCandidate {
	return blockFinalityCandidate{
		id:                    cloneBlockID(downloaded.ID),
		blockRoot:             downloaded.Block,
		blockBOC:              downloaded.BlockBOC,
		meta:                  downloaded.Meta.Clone(),
		stateUpdate:           downloaded.StateUpdate,
		sourcePeerID:          downloaded.SourcePeerID,
		signaturesVerifiedKey: append([]byte(nil), downloaded.SignaturesVerifiedKey...),
	}
}

func (c blockFinalityCandidate) downloaded(proof *cell.Cell, proofBOC []byte, isLink bool, finality *blockFinalityEntry) DownloadedBlock {
	signaturesVerifiedKey := finality.signaturesVerifiedKey
	if len(signaturesVerifiedKey) == 0 {
		signaturesVerifiedKey = c.signaturesVerifiedKey
	}

	sourcePeerID := finality.sourcePeerID
	if sourcePeerID.IsZero() {
		sourcePeerID = c.sourcePeerID
	}

	return DownloadedBlock{
		ID:                    cloneBlockID(c.id),
		Kind:                  blockFinalityBroadcastKind,
		Block:                 c.blockRoot,
		Proof:                 proof,
		BlockBOC:              c.blockBOC,
		ProofBOC:              proofBOC,
		Meta:                  c.meta.Clone(),
		StateUpdate:           c.stateUpdate,
		SourcePeerID:          sourcePeerID,
		IsLink:                isLink,
		VerifiedRootHash:      true,
		SignaturesVerifiedKey: append([]byte(nil), signaturesVerifiedKey...),
	}
}

func blockFinalityCandidateBytes(downloaded DownloadedBlock) int64 {
	return int64(len(downloaded.BlockBOC) + blockFinalityCacheOverhead)
}

func blockFinalityBytes(finality checkedBlockFinality) int64 {
	bytes := blockFinalityCacheOverhead + len(finality.signaturesVerifiedKey)
	if finality.payloadBytes > 0 {
		bytes += finality.payloadBytes
	}
	return int64(bytes)
}

func (n *Node) rememberBlockFinalityCandidate(downloaded *DownloadedBlock) ([]DownloadedBlock, error) {
	assembled, err := n.blockFinalityCache.StoreCandidate(*downloaded, time.Now())
	if err != nil {
		n.log.Debug().
			Err(err).
			Str("block", formatBlockRef(downloaded.ID)).
			Msg("failed to cache block candidate for finality broadcast")
		return nil, err
	}
	return assembled, nil
}

func (n *Node) rememberBlockFinality(finality checkedBlockFinality) ([]DownloadedBlock, error) {
	assembled, err := n.blockFinalityCache.StoreFinality(finality, time.Now())
	if err != nil {
		n.log.Debug().
			Err(err).
			Str("block", formatBlockRef(finality.block)).
			Msg("failed to cache block finality broadcast")
		return nil, err
	}
	return assembled, nil
}
