package p2p

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xssnick/gton/service/blockproof"
	tnstore "github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	shardBroadcastBlockCacheTTL      = 3 * time.Minute
	shardBroadcastBlockCacheMaxBytes = 256 << 20
	shardBroadcastBlockCacheMaxItems = 4096
	shardBroadcastBlockCacheOverhead = 256
)

type shardBroadcastBlockCache struct {
	broadcastBlockCache
}

func newShardBroadcastBlockCache(ttl time.Duration, maxBytes int64, maxItems int) *shardBroadcastBlockCache {
	return &shardBroadcastBlockCache{
		broadcastBlockCache: newBroadcastBlockCache(ttl, maxBytes, maxItems, "shard broadcast block cache"),
	}
}

func (c *shardBroadcastBlockCache) Store(downloaded DownloadedBlock, meta *tnstore.BlockMeta) error {
	validated, err := validateShardBroadcastBlock(&downloaded)
	if err != nil {
		return err
	}
	if meta == nil {
		meta = validated.meta
	}
	return c.storeAt(downloaded, meta, validated.blockRoot, validated.proofRoot, validated.stateUpdate, time.Now())
}

func (c *shardBroadcastBlockCache) storeAt(downloaded DownloadedBlock, meta *tnstore.BlockMeta, blockRoot *cell.Cell, proofRoot *cell.Cell, stateUpdate *cell.Cell, now time.Time) error {
	if isMasterchainBlock(downloaded.ID) {
		return fmt.Errorf("masterchain block %s is not a shard broadcast cache candidate", formatBlockRef(downloaded.ID))
	}
	if !downloaded.VerifiedRootHash {
		return fmt.Errorf("block %s is not hash verified", formatBlockRef(downloaded.ID))
	}
	if len(downloaded.BlockBOC) == 0 {
		return fmt.Errorf("block %s has empty block data", formatBlockRef(downloaded.ID))
	}
	if len(downloaded.ProofBOC) == 0 {
		return fmt.Errorf("block %s has empty proof", formatBlockRef(downloaded.ID))
	}
	if c.maxItems <= 0 || c.maxBytes <= 0 {
		return fmt.Errorf("shard broadcast cache is disabled")
	}
	if meta == nil {
		return fmt.Errorf("block %s has no metadata", formatBlockRef(downloaded.ID))
	}
	if blockRoot == nil {
		return fmt.Errorf("block %s has no parsed block root", formatBlockRef(downloaded.ID))
	}
	if proofRoot == nil {
		return fmt.Errorf("block %s has no parsed proof root", formatBlockRef(downloaded.ID))
	}
	if stateUpdate == nil {
		return fmt.Errorf("block %s has no state update", formatBlockRef(downloaded.ID))
	}

	size := shardBroadcastBlockCacheSize(downloaded.BlockBOC, downloaded.ProofBOC)
	if size > c.maxBytes {
		return fmt.Errorf("block %s is too large for shard broadcast cache: %d > %d", formatBlockRef(downloaded.ID), size, c.maxBytes)
	}

	key := tnstore.BlockKey(downloaded.ID)
	entry := &broadcastBlockCacheEntry{
		key:          key,
		block:        cloneBlockID(downloaded.ID),
		kind:         downloaded.Kind,
		blockRoot:    blockRoot,
		proofRoot:    proofRoot,
		stateUpdate:  stateUpdate,
		blockBOC:     downloaded.BlockBOC,
		proofBOC:     downloaded.ProofBOC,
		isLink:       downloaded.IsLink,
		meta:         meta.Clone(),
		sourcePeerID: downloaded.SourcePeerID,
		bytes:        size,

		signaturesVerifiedKey: append([]byte(nil), downloaded.SignaturesVerifiedKey...),
	}

	c.storeEntry(entry, now)
	return nil
}

func (c *shardBroadcastBlockCache) Block(block ton.BlockIDExt) (*DownloadedBlock, error) {
	return c.blockAt(block, time.Now())
}

func (c *shardBroadcastBlockCache) HasBlock(block ton.BlockIDExt) bool {
	return c.hasBlockAt(block, time.Now())
}

func (c *shardBroadcastBlockCache) hasBlockAt(block ton.BlockIDExt, now time.Time) bool {
	return c.has(tnstore.BlockKey(block), now)
}

func (c *shardBroadcastBlockCache) blockAt(block ton.BlockIDExt, now time.Time) (*DownloadedBlock, error) {
	return c.broadcastBlockCache.blockAt(tnstore.BlockKey(block), now)
}

func (c *shardBroadcastBlockCache) Prune(now time.Time) {
	c.prune(now)
}

func shardBroadcastBlockCacheSize(blockBOC []byte, proofBOC []byte) int64 {
	return int64(len(blockBOC)*2 + len(proofBOC)*2 + shardBroadcastBlockCacheOverhead)
}

func cloneBlockID(block ton.BlockIDExt) ton.BlockIDExt {
	block.RootHash = bytes.Clone(block.RootHash)
	block.FileHash = bytes.Clone(block.FileHash)
	return block
}

func (n *Node) rememberShardBroadcastBlock(downloaded *DownloadedBlock) bool {
	if downloaded == nil {
		return false
	}
	if isMasterchainBlock(downloaded.ID) {
		return false
	}

	validated, err := validateShardBroadcastBlock(downloaded)
	if err != nil {
		n.log.Debug().
			Err(err).
			Str("block", formatBlockRef(downloaded.ID)).
			Msg("dropping shard block broadcast from hot cache")
		return false
	}

	if err = n.shardBroadcastCache.storeAt(*downloaded, validated.meta, validated.blockRoot, validated.proofRoot, validated.stateUpdate, time.Now()); err != nil {
		n.log.Debug().
			Err(err).
			Str("block", formatBlockRef(downloaded.ID)).
			Msg("failed to cache shard block broadcast")
		return false
	}

	n.log.Debug().
		Str("block", formatBlockRef(downloaded.ID)).
		Msg("cached shard block broadcast")
	n.notifyShardBroadcastBlock(downloaded.ID)
	return true
}

func (n *Node) shardBroadcastBlock(block ton.BlockIDExt) (*DownloadedBlock, error) {
	popStarted := n.startBroadcastPipelineStage()
	kind := shardDescriptionBroadcastKind
	result := broadcastPipelineResultMiss
	defer func() {
		n.observeBroadcastPipelineStageSince(popStarted, broadcastPipelineStageExactPop, kind, "", result)
	}()

	if isMasterchainBlock(block) {
		return nil, tnstore.ErrNotFound
	}

	downloaded, err := n.shardBroadcastCache.Block(block)
	if err != nil {
		if !errors.Is(err, tnstore.ErrNotFound) {
			result = broadcastPipelineResultError
		}
		return nil, err
	}

	if downloaded.Kind != "" {
		kind = downloaded.Kind
	}
	result = broadcastPipelineResultSuccess
	n.log.Debug().
		Str("block", formatBlockRef(block)).
		Msg("using cached shard block broadcast")
	return downloaded, nil
}

func (n *Node) watchShardBroadcastBlock(block ton.BlockIDExt) (<-chan struct{}, func()) {
	if isMasterchainBlock(block) {
		return nil, func() {}
	}

	key := tnstore.BlockKey(block)
	return n.shardBroadcastWaiters.watch(key)
}

func (n *Node) notifyShardBroadcastBlock(block ton.BlockIDExt) {
	key := tnstore.BlockKey(block)
	n.shardBroadcastWaiters.notify(key)
}

func (n *Node) runShardBroadcastCacheJanitor(ctx context.Context) {
	interval := shardBroadcastBlockCacheTTL / 2
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			n.shardBroadcastCache.Prune(now)
		}
	}
}

type validatedShardBroadcastRoots struct {
	blockRoot *cell.Cell
	proofRoot *cell.Cell
}

type validatedShardBroadcastBlock struct {
	meta        *tnstore.BlockMeta
	blockRoot   *cell.Cell
	proofRoot   *cell.Cell
	stateUpdate *cell.Cell
}

func validateShardBroadcastRoots(downloaded *DownloadedBlock) (validatedShardBroadcastRoots, error) {
	if isMasterchainBlock(downloaded.ID) {
		return validatedShardBroadcastRoots{}, fmt.Errorf("masterchain block %s is not a shard block", formatBlockRef(downloaded.ID))
	}
	if !downloaded.VerifiedRootHash {
		return validatedShardBroadcastRoots{}, fmt.Errorf("block %s is not hash verified", formatBlockRef(downloaded.ID))
	}
	if len(downloaded.BlockBOC) == 0 {
		return validatedShardBroadcastRoots{}, fmt.Errorf("block %s has empty block data", formatBlockRef(downloaded.ID))
	}
	if len(downloaded.ProofBOC) == 0 {
		return validatedShardBroadcastRoots{}, fmt.Errorf("block %s has empty proof", formatBlockRef(downloaded.ID))
	}

	proof := downloaded.Proof
	if proof == nil {
		return validatedShardBroadcastRoots{}, fmt.Errorf("block %s has no parsed proof root", formatBlockRef(downloaded.ID))
	}
	if err := blockproof.CheckProofShape(downloaded.ID, proof, downloaded.IsLink); err != nil {
		return validatedShardBroadcastRoots{}, err
	}

	root := downloaded.Block
	if root == nil {
		return validatedShardBroadcastRoots{}, fmt.Errorf("block %s has no parsed block root", formatBlockRef(downloaded.ID))
	}
	root, err := effectiveDownloadedBlockRoot(downloaded.ID, downloaded.IsLink, root)
	if err != nil {
		return validatedShardBroadcastRoots{}, err
	}
	rootHash := root.HashKey()
	if !bytes.Equal(rootHash[:], downloaded.ID.RootHash) {
		return validatedShardBroadcastRoots{}, fmt.Errorf("block %s root hash mismatch", formatBlockRef(downloaded.ID))
	}

	return validatedShardBroadcastRoots{
		blockRoot: root,
		proofRoot: proof,
	}, nil
}

func validateShardBroadcastBlock(downloaded *DownloadedBlock) (validatedShardBroadcastBlock, error) {
	roots, err := validateShardBroadcastRoots(downloaded)
	if err != nil {
		return validatedShardBroadcastBlock{}, err
	}

	if downloaded.Meta == nil {
		return validatedShardBroadcastBlock{}, fmt.Errorf("block %s has no metadata", formatBlockRef(downloaded.ID))
	}
	if downloaded.StateUpdate == nil {
		return validatedShardBroadcastBlock{}, fmt.Errorf("block %s has no state update", formatBlockRef(downloaded.ID))
	}
	return validatedShardBroadcastBlock{
		meta:        downloaded.Meta.Clone(),
		blockRoot:   roots.blockRoot,
		proofRoot:   roots.proofRoot,
		stateUpdate: downloaded.StateUpdate,
	}, nil
}
