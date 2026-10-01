package service

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

var _ p2p.BroadcastSignatureVerifier = (*SyncCoordinator)(nil)

type broadcastValidatorConfig struct {
	rootHash       cell.Hash
	cfg            *tlb.BlockchainConfig
	plumtreePolicy p2p.PlumtreePolicy
	fastSync       fastSyncBlockchainConfig
}

// broadcastValidatorCacheMaxEntries bounds the validator sets cached for one
// config root. Only sets whose signatures verified are cached, and legitimate
// broadcasts name a few per shard epoch, so the map simply starts over on
// overflow.
const broadcastValidatorCacheMaxEntries = 256

// broadcastFinalityCacheMaxEntries bounds the verified finality evidence kept
// to recognize copies of one broadcast arriving through other overlays. Copies
// land within moments of each other, so starting over on overflow costs at most
// one more verification of a copy racing the reset.
const broadcastFinalityCacheMaxEntries = 1024

type broadcastValidatorCacheKey struct {
	configRootHash   cell.Hash
	workchain        int32
	shard            int64
	catchainSeqno    uint32
	validatorSetHash uint32
}

// broadcastFinalityCacheKey covers every input of a finality signature check:
// the validator set, the block id (root and file hash are part of the content
// key) and the whole signature set content.
type broadcastFinalityCacheKey struct {
	validators broadcastValidatorCacheKey
	seqno      uint32
	content    [32]byte
}

type broadcastValidatorCache struct {
	mu               sync.Mutex
	configBlockSeq   uint32
	config           broadcastValidatorConfig
	configLoaded     bool
	configRootHash   cell.Hash
	initialized      bool
	entries          map[broadcastValidatorCacheKey]*blockproof.PreparedValidatorSet
	verifiedFinality map[broadcastFinalityCacheKey]struct{}
	shardTopView     *shardTopValidationView
}

func (c *broadcastValidatorCache) getConfig() (broadcastValidatorConfig, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.configLoaded {
		return broadcastValidatorConfig{}, storage.ErrNotFound
	}
	return c.config, nil
}

func (c *broadcastValidatorCache) putConfig(block ton.BlockIDExt, config broadcastValidatorConfig) broadcastValidatorConfig {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.configLoaded && block.SeqNo <= c.configBlockSeq {
		return c.config
	}

	c.configBlockSeq = block.SeqNo
	c.config = config
	c.configLoaded = true
	c.cachesConfigRootLocked(config.rootHash)
	return c.config
}

// cachesConfigRootLocked reports whether entries computed under configRootHash
// may be cached, starting the caches over when that root replaces the cached
// one. A root other than the loaded config's belongs to a config this node
// already replaced.
func (c *broadcastValidatorCache) cachesConfigRootLocked(configRootHash cell.Hash) bool {
	if c.configLoaded && c.config.rootHash != configRootHash {
		return false
	}
	if !c.initialized || c.configRootHash != configRootHash {
		c.configRootHash = configRootHash
		c.initialized = true
		c.entries = make(map[broadcastValidatorCacheKey]*blockproof.PreparedValidatorSet)
		c.verifiedFinality = make(map[broadcastFinalityCacheKey]struct{})
	}
	return true
}

func (s *SyncCoordinator) publishBroadcastValidatorConfig(
	block ton.BlockIDExt,
	config broadcastValidatorConfig,
) broadcastValidatorConfig {
	config = s.broadcastValidatorCache.putConfig(block, config)
	s.node.SetPlumtreePolicy(config.plumtreePolicy)
	return config
}

func (c *broadcastValidatorCache) get(key broadcastValidatorCacheKey) (*blockproof.PreparedValidatorSet, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.initialized || c.configRootHash != key.configRootHash {
		return nil, storage.ErrNotFound
	}
	set, ok := c.entries[key]
	if !ok {
		return nil, storage.ErrNotFound
	}
	return set, nil
}

// put caches a validator set only after signatures made with it verified: the
// key comes from an untrusted broadcast, and the set hash is computable offline
// from the public config for any shard and catchain seqno.
func (c *broadcastValidatorCache) put(key broadcastValidatorCacheKey, set *blockproof.PreparedValidatorSet) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.cachesConfigRootLocked(key.configRootHash) {
		return
	}
	if _, ok := c.entries[key]; ok {
		return
	}
	if len(c.entries) >= broadcastValidatorCacheMaxEntries {
		c.entries = make(map[broadcastValidatorCacheKey]*blockproof.PreparedValidatorSet)
	}
	c.entries[key] = set
}

func (c *broadcastValidatorCache) finalityVerified(key broadcastFinalityCacheKey) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	_, ok := c.verifiedFinality[key]
	return ok
}

func (c *broadcastValidatorCache) putVerifiedFinality(key broadcastFinalityCacheKey) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.cachesConfigRootLocked(key.validators.configRootHash) {
		return
	}
	if len(c.verifiedFinality) >= broadcastFinalityCacheMaxEntries {
		c.verifiedFinality = make(map[broadcastFinalityCacheKey]struct{})
	}
	c.verifiedFinality[key] = struct{}{}
}

func (s *SyncCoordinator) CheckBlockBroadcastSignatures(ctx context.Context, req p2p.BlockBroadcastSignatureCheck) error {
	// The same decoded proof cell reaches the verify pipeline again through the
	// p2p hot cache; share the parse with it.
	parsed, err := s.parsedProofs.parse(req.Block, req.Proof)
	if err != nil {
		return err
	}

	signatures, err := blockproof.PrepareValidatorSignatureSet(req.Block, parsed.Block, req.Signatures)
	if err != nil {
		return err
	}

	key, validators, err := s.broadcastValidatorSetForSignatures(
		ctx,
		req.Block,
		signatures.CatchainSeqno(),
		signatures.ValidatorSetHash(),
	)
	if err != nil {
		return err
	}
	if err = blockproof.CheckPreparedSignatures(req.Block, signatures, validators); err != nil {
		return err
	}

	s.broadcastValidatorCache.put(key, validators)
	return nil
}

// CheckBlockFinalitySignatures authenticates a Simplex finality broadcast
// received from an arbitrary network peer. It is the receiving side of what
// validator.BlockAccepter publishes.
//
// It verifies in full, deliberately, and must keep doing so. This is the very
// evidence the block accepter no longer re-verifies: there the certificate came
// out of this node's own consensus engine, which had already checked the whole
// quorum, and the accepter carries that fact in a simplex.VerifiedCertificate.
// Here the certificate came from a peer and no engine of ours ever saw it, so
// blockproof.CheckPreparedSignatures — never CheckPreparedSignatureWeight — is
// what stands between a forged quorum and the block store.
//
// The one thing it does not verify twice is a copy of evidence it already
// verified in full — the same broadcast arriving through another overlay. The
// copy is recognized by broadcastFinalityCacheKey, which covers every input of
// the check, so any difference in the evidence is verified again.
func (s *SyncCoordinator) CheckBlockFinalitySignatures(ctx context.Context, req p2p.BlockFinalitySignatureCheck) (*p2p.BlockFinalitySignatureCheckResult, error) {
	if !req.Signatures.IsSimplex() {
		return nil, fmt.Errorf("block finality broadcast %s has non-simplex validator signatures", storage.FormatBlockRef(req.Block))
	}
	if req.Block.Workchain == -1 && !req.Signatures.Final() {
		return nil, fmt.Errorf("masterchain block %s has non-final validator signatures", storage.FormatBlockRef(req.Block))
	}

	key, validators, err := s.broadcastValidatorSetForSignatures(
		ctx,
		req.Block,
		req.Signatures.CatchainSeqno(),
		req.Signatures.ValidatorSetHash(),
	)
	if err != nil {
		return nil, err
	}

	verifiedKey := req.Signatures.ContentKey(req.Block)
	finalityKey := broadcastFinalityCacheKey{
		validators: key,
		seqno:      req.Block.SeqNo,
		content:    [32]byte(verifiedKey),
	}
	if !s.broadcastValidatorCache.finalityVerified(finalityKey) {
		if err = blockproof.CheckPreparedSignatures(req.Block, req.Signatures, validators); err != nil {
			return nil, err
		}

		s.broadcastValidatorCache.put(key, validators)
		s.broadcastValidatorCache.putVerifiedFinality(finalityKey)
	}

	var signaturesCell *cell.Cell
	if req.Block.Workchain == -1 {
		signaturesCell, err = req.Signatures.FinalitySignaturesCell(validators)
		if err != nil {
			return nil, err
		}
		blockSignatures, parseErr := blockproof.ParseBlockSignatureSetCell(signaturesCell)
		if parseErr != nil {
			return nil, fmt.Errorf("parse serialized finality signatures: %w", parseErr)
		}
		verifiedKey = blockSignatures.ContentKey(req.Block)
	}
	return &p2p.BlockFinalitySignatureCheckResult{
		SignaturesCell:        signaturesCell,
		SignaturesVerifiedKey: verifiedKey,
	}, nil
}

func (s *SyncCoordinator) ValidateShardDescriptionBroadcast(ctx context.Context, req p2p.ShardDescriptionSignatureCheck) (*p2p.ShardBlockDescription, error) {
	parsed, err := p2p.ParseShardTopBlockDescription(req.Block, req.CatchainSeqno, req.Root)
	if err != nil {
		return nil, err
	}
	desc := parsed.Description

	view, err := s.currentShardTopValidationView(ctx)
	if err != nil {
		return nil, err
	}
	if err = s.validateShardDescriptionAgainstView(ctx, view, parsed); err != nil {
		return nil, err
	}

	return desc, nil
}

func (s *SyncCoordinator) validateShardDescriptionAgainstView(
	ctx context.Context,
	view *shardTopValidationView,
	parsed *p2p.ParsedShardTopDescription,
) error {
	desc := parsed.Description
	if err := validateShardTopDescriptionContext(ctx, view, desc); err != nil {
		return err
	}

	validators, err := view.validatorSet(
		desc.Block,
		desc.CatchainSeqno,
		desc.ValidatorSetHash,
	)
	if err != nil {
		if errors.Is(err, blockproof.ErrShardTopValidatorContextNotReady) {
			return fmt.Errorf("%w: shard top validator context is not ready: %v", p2p.ErrBroadcastSignatureRetryable, err)
		}
		return err
	}
	if err = blockproof.CheckPreparedBlockSignatures(desc.Block, parsed.Signatures, validators); err != nil {
		return err
	}

	return nil
}

// broadcastValidatorSetForSignatures returns the validator set named by a
// broadcast together with its cache key. It does not cache a computed set:
// the caller puts it once signatures made with it verified.
func (s *SyncCoordinator) broadcastValidatorSetForSignatures(
	ctx context.Context,
	block ton.BlockIDExt,
	catchainSeqno uint32,
	validatorSetHash uint32,
) (broadcastValidatorCacheKey, *blockproof.PreparedValidatorSet, error) {
	config, err := s.currentBroadcastValidatorConfig(ctx)
	if err != nil {
		return broadcastValidatorCacheKey{}, nil, err
	}

	return s.broadcastValidatorSetForConfig(config, block, catchainSeqno, validatorSetHash)
}

func (s *SyncCoordinator) broadcastValidatorSetForConfig(
	config broadcastValidatorConfig,
	block ton.BlockIDExt,
	catchainSeqno uint32,
	validatorSetHash uint32,
) (broadcastValidatorCacheKey, *blockproof.PreparedValidatorSet, error) {
	key := broadcastValidatorCacheKeyFromBlock(config.rootHash, block, catchainSeqno, validatorSetHash)
	set, err := s.broadcastValidatorCache.get(key)
	if err == nil {
		return key, set, nil
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return broadcastValidatorCacheKey{}, nil, err
	}

	set, err = broadcastValidatorSetFromConfig(config.cfg, block, catchainSeqno, validatorSetHash)
	if err != nil {
		return broadcastValidatorCacheKey{}, nil, err
	}
	return key, set, nil
}

func broadcastValidatorCacheKeyFromBlock(
	configRootHash cell.Hash,
	block ton.BlockIDExt,
	catchainSeqno uint32,
	validatorSetHash uint32,
) broadcastValidatorCacheKey {
	return broadcastValidatorCacheKey{
		configRootHash:   configRootHash,
		workchain:        block.Workchain,
		shard:            block.Shard,
		catchainSeqno:    catchainSeqno,
		validatorSetHash: validatorSetHash,
	}
}

type broadcastValidatorSetCandidate struct {
	name string
	load func() ([]*tlb.ValidatorAddr, error)
}

func broadcastValidatorSetFromConfig(
	cfg *tlb.BlockchainConfig,
	block ton.BlockIDExt,
	catchainSeqno uint32,
	validatorSetHash uint32,
) (*blockproof.PreparedValidatorSet, error) {
	// Key-block boundaries can leave ordinary block and finality broadcasts
	// signed by any validator epoch still named by the current config.
	candidates := []broadcastValidatorSetCandidate{
		{
			name: "current",
			load: func() ([]*tlb.ValidatorAddr, error) {
				return blockproof.CurrentValidatorsForBlock(cfg, &block, catchainSeqno)
			},
		},
		{
			name: "next",
			load: func() ([]*tlb.ValidatorAddr, error) {
				return blockproof.NextValidatorsForBlock(cfg, &block, catchainSeqno)
			},
		},
		{
			name: "previous",
			load: func() ([]*tlb.ValidatorAddr, error) {
				return blockproof.PrevValidatorsForBlock(cfg, &block, catchainSeqno)
			},
		},
	}

	var lastErr error
	for _, candidate := range candidates {
		validators, err := candidate.load()
		if err != nil {
			lastErr = fmt.Errorf("%s validators: %w", candidate.name, err)
			continue
		}

		set, err := blockproof.PrepareValidatorSet(catchainSeqno, validators)
		if err != nil {
			lastErr = fmt.Errorf("%s validators prepare: %w", candidate.name, err)
			continue
		}
		if set.Hash() == validatorSetHash {
			return set, nil
		}
		lastErr = fmt.Errorf("%s validators hash %08x does not match %08x", candidate.name, set.Hash(), validatorSetHash)
	}

	return nil, fmt.Errorf("validator set %08x for %s is not available: %w: %v", validatorSetHash, storage.FormatBlockRef(block), storage.ErrNotFound, lastErr)
}

func (s *SyncCoordinator) currentBroadcastValidatorConfig(ctx context.Context) (broadcastValidatorConfig, error) {
	config, err := s.broadcastValidatorCache.getConfig()
	if err == nil {
		return config, nil
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return broadcastValidatorConfig{}, err
	}

	current, err := s.status.currentStateSnapshot(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return broadcastValidatorConfig{}, fmt.Errorf("%w: load live current state for broadcast signature check: %w", p2p.ErrBroadcastSignatureRetryable, err)
		}
		return broadcastValidatorConfig{}, fmt.Errorf("load live current state for broadcast signature check: %w", err)
	}

	masterState, err := s.loadMasterStateForConsensus(ctx, current.Masterchain.Block)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return broadcastValidatorConfig{}, fmt.Errorf("%w: load masterchain state %s for broadcast signature check: %w", p2p.ErrBroadcastSignatureRetryable, storage.FormatBlockRef(current.Masterchain.Block), err)
		}
		return broadcastValidatorConfig{}, fmt.Errorf("load masterchain state %s for broadcast signature check: %w", storage.FormatBlockRef(current.Masterchain.Block), err)
	}
	config, err = broadcastValidatorConfigFromMasterchainState(masterState)
	if err != nil {
		return broadcastValidatorConfig{}, err
	}
	return s.publishBroadcastValidatorConfig(current.Masterchain.Block, config), nil
}

func broadcastValidatorConfigFromMasterchainState(state *storage.BlockState) (broadcastValidatorConfig, error) {
	cfg, err := blockproof.ConfigFromMasterchainState(state)
	if err != nil {
		return broadcastValidatorConfig{}, err
	}

	return broadcastValidatorConfigForMasterchain(state.Block, cfg)
}

func broadcastValidatorConfigForMasterchain(
	block ton.BlockIDExt,
	cfg *tlb.BlockchainConfig,
) (broadcastValidatorConfig, error) {
	if cfg.Root == nil {
		return broadcastValidatorConfig{}, fmt.Errorf("masterchain state %s has empty validator config root", storage.FormatBlockRef(block))
	}

	fastSync := fastSyncConfigFromConfig(cfg)
	return broadcastValidatorConfig{
		rootHash:       cfg.Root.HashKey(),
		cfg:            cfg,
		plumtreePolicy: fastSync.plumtreePolicy(),
		fastSync:       fastSync,
	}, nil
}
