package p2p

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/xssnick/gton/service/blockproof"
	tnstore "github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	shardBlockCandidateCacheTTL      = 3 * time.Minute
	shardBlockCandidateCacheMaxBytes = 512 << 20
	shardBlockCandidateCacheMaxItems = 4096
	shardBlockCandidateCacheOverhead = 256
)

type ShardDescriptionProof struct {
	Block    ton.BlockIDExt
	Proof    *cell.Cell
	ProofBOC []byte
}

type shardBlockCandidateCache struct {
	mu sync.Mutex

	ttl      time.Duration
	maxBytes int64
	maxItems int

	candidates map[tnstore.BlockRootHash]shardBlockCandidateEntry
	proofs     map[tnstore.BlockRootHash]shardDescriptionProofEntry
	assembled  map[tnstore.BlockRootHash]shardBlockAssembledEntry
	bytes      int64
}

type shardBlockCandidateEntry struct {
	block      shardBlockCandidate
	receivedAt time.Time
	expiresAt  time.Time
	bytes      int64
}

type shardBlockCandidate struct {
	id                    ton.BlockIDExt
	blockBOC              []byte
	sourcePeerID          PeerID
	signaturesVerifiedKey []byte
}

type shardDescriptionProofEntry struct {
	block      ton.BlockIDExt
	proofBOC   []byte
	receivedAt time.Time
	expiresAt  time.Time
	bytes      int64
}

type shardBlockAssembledEntry struct {
	receivedAt time.Time
	expiresAt  time.Time
	bytes      int64
	assembling bool
}

type shardCandidateAssembly struct {
	key       tnstore.BlockRootHash
	candidate shardBlockCandidateEntry
	proof     shardDescriptionProofEntry
}

func newShardBlockCandidateCache(ttl time.Duration, maxBytes int64, maxItems int) *shardBlockCandidateCache {
	return &shardBlockCandidateCache{
		ttl:        ttl,
		maxBytes:   maxBytes,
		maxItems:   maxItems,
		candidates: map[tnstore.BlockRootHash]shardBlockCandidateEntry{},
		proofs:     map[tnstore.BlockRootHash]shardDescriptionProofEntry{},
		assembled:  map[tnstore.BlockRootHash]shardBlockAssembledEntry{},
	}
}

func (c *shardBlockCandidateCache) StoreCandidate(downloaded DownloadedBlock, now time.Time) ([]DownloadedBlock, error) {
	if c.maxItems <= 0 || c.maxBytes <= 0 {
		return nil, fmt.Errorf("shard block candidate cache is disabled")
	}

	key := tnstore.BlockKey(downloaded.ID)
	entry := shardBlockCandidateEntry{
		block:      shardBlockCandidateFrom(downloaded),
		receivedAt: now,
		expiresAt:  now.Add(c.ttl),
		bytes:      shardBlockCandidateBytes(downloaded),
	}
	if entry.bytes > c.maxBytes {
		return nil, fmt.Errorf("block candidate %s is too large for shard block candidate cache: %d > %d", tnstore.FormatBlockRef(downloaded.ID), entry.bytes, c.maxBytes)
	}

	c.mu.Lock()
	c.pruneExpiredLocked(now)
	if _, ok := c.assembled[key]; ok {
		c.mu.Unlock()
		return nil, nil
	}
	if old, ok := c.candidates[key]; ok {
		c.bytes -= old.bytes
	}
	c.candidates[key] = entry
	c.bytes += entry.bytes

	proof, ok := c.proofs[key]
	if !ok || !proof.expiresAt.After(now) {
		c.pruneOverflowLocked()
		c.mu.Unlock()
		return nil, nil
	}
	c.reserveAssemblyLocked(key, now)
	c.pruneOverflowLocked()
	c.mu.Unlock()

	assembled, err := assembleShardCandidate(entry, proof)
	c.completeAssembly(key, err == nil)

	return assembled, err
}

func (c *shardBlockCandidateCache) StoreProofs(proofs []ShardDescriptionProof, now time.Time) ([]DownloadedBlock, error) {
	if len(proofs) == 0 {
		return nil, nil
	}
	if c.maxItems <= 0 || c.maxBytes <= 0 {
		return nil, fmt.Errorf("shard block candidate cache is disabled")
	}

	entries := make([]shardDescriptionProofEntry, 0, len(proofs))
	for _, proof := range proofs {
		if err := validateShardDescriptionProof(proof); err != nil {
			return nil, err
		}
		entry := shardDescriptionProofEntry{
			block:      cloneBlockID(proof.Block),
			proofBOC:   proof.ProofBOC,
			receivedAt: now,
			expiresAt:  now.Add(c.ttl),
			bytes:      shardDescriptionProofBytes(proof),
		}
		if entry.bytes > c.maxBytes {
			return nil, fmt.Errorf("shard description proof %s is too large for shard block candidate cache: %d > %d", tnstore.FormatBlockRef(proof.Block), entry.bytes, c.maxBytes)
		}
		entries = append(entries, entry)
	}

	c.mu.Lock()
	c.pruneExpiredLocked(now)
	for _, entry := range entries {
		key := tnstore.BlockKey(entry.block)
		if _, ok := c.assembled[key]; ok {
			continue
		}
		if old, ok := c.proofs[key]; ok {
			c.bytes -= old.bytes
		}
		c.proofs[key] = entry
		c.bytes += entry.bytes
	}

	// One unusable link must not discard the rest of the chain. Reserve every
	// ready pair first, then decode them independently after releasing the lock.
	assemblies := make([]shardCandidateAssembly, 0, len(entries))
	for _, entry := range entries {
		key := tnstore.BlockKey(entry.block)
		if _, ok := c.assembled[key]; ok {
			continue
		}
		candidate, ok := c.candidates[key]
		if !ok || !candidate.expiresAt.After(now) {
			continue
		}
		c.reserveAssemblyLocked(key, now)
		assemblies = append(assemblies, shardCandidateAssembly{
			key:       key,
			candidate: candidate,
			proof:     entry,
		})
	}
	c.pruneOverflowLocked()
	c.mu.Unlock()

	assembled := make([]DownloadedBlock, 0, len(assemblies))
	var errs error
	for _, assembly := range assemblies {
		blocks, err := assembleShardCandidate(assembly.candidate, assembly.proof)
		c.completeAssembly(assembly.key, err == nil)
		if err != nil {
			errs = errors.Join(errs, err)
			continue
		}
		assembled = append(assembled, blocks...)
	}

	return assembled, errs
}

func assembleShardCandidate(candidate shardBlockCandidateEntry, proof shardDescriptionProofEntry) ([]DownloadedBlock, error) {
	if !candidate.block.id.Equals(&proof.block) {
		return nil, fmt.Errorf("shard candidate %s proof belongs to %s", tnstore.FormatBlockRef(candidate.block.id), tnstore.FormatBlockRef(proof.block))
	}
	// The proof shape is not re-checked here: c.proofs is only ever populated by
	// StoreProofs, which runs validateShardDescriptionProof (the same
	// CheckProofShape call) on every entry before it reaches the map.

	block, err := candidate.block.downloaded(proof)
	if err != nil {
		return nil, err
	}

	return []DownloadedBlock{*block}, nil
}

func (c *shardBlockCandidateCache) reserveAssemblyLocked(key tnstore.BlockRootHash, now time.Time) {
	assembled := shardBlockAssembledEntry{
		receivedAt: now,
		expiresAt:  now.Add(c.ttl),
		bytes:      shardBlockCandidateCacheOverhead,
		assembling: true,
	}
	c.assembled[key] = assembled
	c.bytes += assembled.bytes
}

func (c *shardBlockCandidateCache) completeAssembly(key tnstore.BlockRootHash, succeeded bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	assembled, ok := c.assembled[key]
	if !ok || !assembled.assembling {
		return
	}
	if !succeeded {
		delete(c.assembled, key)
		c.bytes -= assembled.bytes
		c.pruneOverflowLocked()
		return
	}

	// The returned DownloadedBlock keeps the decoded pair alive for its immediate
	// caller; the cache retains only this marker to suppress duplicate assembly.
	if candidate, found := c.candidates[key]; found {
		delete(c.candidates, key)
		c.bytes -= candidate.bytes
	}
	if proof, found := c.proofs[key]; found {
		delete(c.proofs, key)
		c.bytes -= proof.bytes
	}
	assembled.assembling = false
	c.assembled[key] = assembled
	c.pruneOverflowLocked()
}

func (c *shardBlockCandidateCache) pruneExpiredLocked(now time.Time) {
	for key, entry := range c.candidates {
		if c.assemblyInProgressLocked(key) {
			continue
		}
		if entry.expiresAt.After(now) {
			continue
		}
		delete(c.candidates, key)
		c.bytes -= entry.bytes
	}
	for key, entry := range c.proofs {
		if c.assemblyInProgressLocked(key) {
			continue
		}
		if entry.expiresAt.After(now) {
			continue
		}
		delete(c.proofs, key)
		c.bytes -= entry.bytes
	}
	for key, entry := range c.assembled {
		if entry.assembling {
			continue
		}
		if entry.expiresAt.After(now) {
			continue
		}
		delete(c.assembled, key)
		c.bytes -= entry.bytes
	}
}

func (c *shardBlockCandidateCache) pruneOverflowLocked() {
	for len(c.candidates)+len(c.proofs)+len(c.assembled) > c.maxItems || c.bytes > c.maxBytes {
		var oldestKey tnstore.BlockRootHash
		oldestKind := ""
		var oldestAt time.Time
		for key, entry := range c.candidates {
			if c.assemblyInProgressLocked(key) {
				continue
			}
			if oldestKind == "" || entry.receivedAt.Before(oldestAt) {
				oldestKey = key
				oldestKind = "candidate"
				oldestAt = entry.receivedAt
			}
		}
		for key, entry := range c.proofs {
			if c.assemblyInProgressLocked(key) {
				continue
			}
			if oldestKind == "" || entry.receivedAt.Before(oldestAt) {
				oldestKey = key
				oldestKind = "proof"
				oldestAt = entry.receivedAt
			}
		}
		for key, entry := range c.assembled {
			if entry.assembling {
				continue
			}
			if oldestKind == "" || entry.receivedAt.Before(oldestAt) {
				oldestKey = key
				oldestKind = "assembled"
				oldestAt = entry.receivedAt
			}
		}
		if oldestKind == "" {
			return
		}
		switch oldestKind {
		case "candidate":
			entry := c.candidates[oldestKey]
			delete(c.candidates, oldestKey)
			c.bytes -= entry.bytes
		case "proof":
			entry := c.proofs[oldestKey]
			delete(c.proofs, oldestKey)
			c.bytes -= entry.bytes
		case "assembled":
			entry := c.assembled[oldestKey]
			delete(c.assembled, oldestKey)
			c.bytes -= entry.bytes
		}
	}
}

func (c *shardBlockCandidateCache) assemblyInProgressLocked(key tnstore.BlockRootHash) bool {
	entry, ok := c.assembled[key]

	return ok && entry.assembling
}

func validateShardDescriptionProof(proof ShardDescriptionProof) error {
	if isMasterchainBlock(proof.Block) {
		return fmt.Errorf("masterchain block %s is not a shard description proof candidate", tnstore.FormatBlockRef(proof.Block))
	}
	if proof.Proof == nil {
		return fmt.Errorf("shard description proof %s has no parsed proof", tnstore.FormatBlockRef(proof.Block))
	}
	if len(proof.ProofBOC) == 0 {
		return fmt.Errorf("shard description proof %s has empty proof data", tnstore.FormatBlockRef(proof.Block))
	}
	if err := blockproof.CheckProofShape(proof.Block, proof.Proof, true); err != nil {
		return err
	}
	return nil
}

func shardBlockCandidateFrom(downloaded DownloadedBlock) shardBlockCandidate {
	return shardBlockCandidate{
		id:                    cloneBlockID(downloaded.ID),
		blockBOC:              downloaded.BlockBOC,
		sourcePeerID:          downloaded.SourcePeerID,
		signaturesVerifiedKey: append([]byte(nil), downloaded.SignaturesVerifiedKey...),
	}
}

func (c shardBlockCandidate) downloaded(proof shardDescriptionProofEntry) (*DownloadedBlock, error) {
	downloaded, err := decodeRawBlockCandidateBroadcast(
		shardDescriptionBroadcastKind,
		c.id,
		c.blockBOC,
	)
	if err != nil {
		return nil, fmt.Errorf("decode cached shard candidate %s: %w", tnstore.FormatBlockRef(c.id), err)
	}
	proofRoot, err := parseDownloadedBlockProof(proof.proofBOC)
	if err != nil {
		return nil, fmt.Errorf("decode cached shard candidate proof %s: %w", tnstore.FormatBlockRef(c.id), err)
	}
	downloaded.Proof = proofRoot
	downloaded.ProofBOC = proof.proofBOC
	downloaded.SourcePeerID = c.sourcePeerID
	downloaded.IsLink = true
	downloaded.SignaturesVerifiedKey = append([]byte(nil), c.signaturesVerifiedKey...)

	return downloaded, nil
}

func shardBlockCandidateBytes(downloaded DownloadedBlock) int64 {
	return int64(len(downloaded.BlockBOC) + shardBlockCandidateCacheOverhead)
}

func shardDescriptionProofBytes(proof ShardDescriptionProof) int64 {
	return int64(len(proof.ProofBOC) + shardBlockCandidateCacheOverhead)
}

func (n *Node) rememberShardBlockCandidate(downloaded *DownloadedBlock) {
	if isMasterchainBlock(downloaded.ID) {
		return
	}

	assembled, err := n.shardCandidateCache.StoreCandidate(*downloaded, time.Now())
	if err != nil {
		n.log.Debug().
			Err(err).
			Str("block", tnstore.FormatBlockRef(downloaded.ID)).
			Msg("failed to cache shard block candidate broadcast")
		return
	}
	n.rememberAssembledShardCandidates(assembled)
}

func (n *Node) RememberShardDescriptionProofs(proofs []ShardDescriptionProof) {
	if len(proofs) == 0 {
		return
	}

	// A partial failure still hands back the links that did assemble; deliver
	// them before logging, since their payloads are already released.
	assembled, err := n.shardCandidateCache.StoreProofs(proofs, time.Now())
	n.rememberAssembledShardCandidates(assembled)
	if err != nil {
		n.log.Debug().
			Err(err).
			Msg("failed to cache shard block description proofs")
	}
}

func (n *Node) rememberAssembledShardCandidates(blocks []DownloadedBlock) {
	for i := range blocks {
		block := blocks[i]
		cacheStarted := n.startBroadcastPipelineStage()
		cacheResult := broadcastPipelineResultMiss
		if n.rememberShardBroadcastBlock(&block) {
			cacheResult = broadcastPipelineResultSuccess
		}
		n.observeBroadcastPipelineStageSince(cacheStarted, broadcastPipelineStageHotCacheNotify, block.Kind, "", cacheResult)
	}
}
