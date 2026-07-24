package p2p

import (
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
	blockRoot             *cell.Cell
	blockBOC              []byte
	meta                  *tnstore.BlockMeta
	stateUpdate           *cell.Cell
	sourcePeerID          PeerID
	signaturesVerifiedKey []byte
}

type shardDescriptionProofEntry struct {
	block      ton.BlockIDExt
	proof      *cell.Cell
	proofBOC   []byte
	receivedAt time.Time
	expiresAt  time.Time
	bytes      int64
}

func newShardBlockCandidateCache(ttl time.Duration, maxBytes int64, maxItems int) *shardBlockCandidateCache {
	return &shardBlockCandidateCache{
		ttl:        ttl,
		maxBytes:   maxBytes,
		maxItems:   maxItems,
		candidates: map[tnstore.BlockRootHash]shardBlockCandidateEntry{},
		proofs:     map[tnstore.BlockRootHash]shardDescriptionProofEntry{},
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
	defer c.mu.Unlock()

	c.pruneExpiredLocked(now)
	if old, ok := c.candidates[key]; ok {
		c.bytes -= old.bytes
	}
	c.candidates[key] = entry
	c.bytes += entry.bytes

	proof, ok := c.proofs[key]
	if !ok || !proof.expiresAt.After(now) {
		c.pruneOverflowLocked()
		return nil, nil
	}
	assembled, err := c.assembleLocked(entry, proof)
	if err != nil {
		c.pruneOverflowLocked()
		return nil, err
	}
	c.pruneOverflowLocked()
	return assembled, nil
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
			proof:      proof.Proof,
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
	defer c.mu.Unlock()

	c.pruneExpiredLocked(now)
	assembled := make([]DownloadedBlock, 0, len(entries))
	for _, entry := range entries {
		key := tnstore.BlockKey(entry.block)
		if old, ok := c.proofs[key]; ok {
			c.bytes -= old.bytes
		}
		c.proofs[key] = entry
		c.bytes += entry.bytes
	}

	for _, entry := range entries {
		key := tnstore.BlockKey(entry.block)
		candidate, ok := c.candidates[key]
		if !ok || !candidate.expiresAt.After(now) {
			continue
		}
		blocks, err := c.assembleLocked(candidate, entry)
		if err != nil {
			c.pruneOverflowLocked()
			return nil, err
		}
		assembled = append(assembled, blocks...)
	}
	c.pruneOverflowLocked()
	return assembled, nil
}

func (c *shardBlockCandidateCache) assembleLocked(candidate shardBlockCandidateEntry, proof shardDescriptionProofEntry) ([]DownloadedBlock, error) {
	if !candidate.block.id.Equals(&proof.block) {
		return nil, fmt.Errorf("shard candidate %s proof belongs to %s", tnstore.FormatBlockRef(candidate.block.id), tnstore.FormatBlockRef(proof.block))
	}
	if err := blockproof.CheckProofShape(proof.block, proof.proof, true); err != nil {
		return nil, err
	}

	block := candidate.block.downloaded(proof)
	return []DownloadedBlock{block}, nil
}

func (c *shardBlockCandidateCache) pruneExpiredLocked(now time.Time) {
	for key, entry := range c.candidates {
		if entry.expiresAt.After(now) {
			continue
		}
		delete(c.candidates, key)
		c.bytes -= entry.bytes
	}
	for key, entry := range c.proofs {
		if entry.expiresAt.After(now) {
			continue
		}
		delete(c.proofs, key)
		c.bytes -= entry.bytes
	}
}

func (c *shardBlockCandidateCache) pruneOverflowLocked() {
	for len(c.candidates)+len(c.proofs) > c.maxItems || c.bytes > c.maxBytes {
		var oldestKey tnstore.BlockRootHash
		oldestKind := ""
		var oldestAt time.Time
		for key, entry := range c.candidates {
			if oldestKind == "" || entry.receivedAt.Before(oldestAt) {
				oldestKey = key
				oldestKind = "candidate"
				oldestAt = entry.receivedAt
			}
		}
		for key, entry := range c.proofs {
			if oldestKind == "" || entry.receivedAt.Before(oldestAt) {
				oldestKey = key
				oldestKind = "proof"
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
		}
	}
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
		blockRoot:             downloaded.Block,
		blockBOC:              downloaded.BlockBOC,
		meta:                  downloaded.Meta.Clone(),
		stateUpdate:           downloaded.StateUpdate,
		sourcePeerID:          downloaded.SourcePeerID,
		signaturesVerifiedKey: append([]byte(nil), downloaded.SignaturesVerifiedKey...),
	}
}

func (c shardBlockCandidate) downloaded(proof shardDescriptionProofEntry) DownloadedBlock {
	return DownloadedBlock{
		ID:                    cloneBlockID(c.id),
		Kind:                  shardDescriptionBroadcastKind,
		Block:                 c.blockRoot,
		Proof:                 proof.proof,
		BlockBOC:              c.blockBOC,
		ProofBOC:              proof.proofBOC,
		Meta:                  c.meta.Clone(),
		StateUpdate:           c.stateUpdate,
		SourcePeerID:          c.sourcePeerID,
		IsLink:                true,
		VerifiedRootHash:      true,
		SignaturesVerifiedKey: append([]byte(nil), c.signaturesVerifiedKey...),
	}
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

	assembled, err := n.shardCandidateCache.StoreProofs(proofs, time.Now())
	if err != nil {
		n.log.Debug().
			Err(err).
			Msg("failed to cache shard block description proofs")
		return
	}
	n.rememberAssembledShardCandidates(assembled)
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
