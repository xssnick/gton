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

type broadcastValidatorCacheKey struct {
	configRootHash   cell.Hash
	workchain        int32
	shard            int64
	catchainSeqno    uint32
	validatorSetHash uint32
}

type broadcastValidatorCache struct {
	mu             sync.Mutex
	configBlockSeq uint32
	config         broadcastValidatorConfig
	configLoaded   bool
	configRootHash cell.Hash
	initialized    bool
	entries        map[broadcastValidatorCacheKey]*blockproof.PreparedValidatorSet
	shardTopView   *shardTopValidationView
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
	if !c.initialized || c.configRootHash != config.rootHash {
		c.configRootHash = config.rootHash
		c.initialized = true
		c.entries = make(map[broadcastValidatorCacheKey]*blockproof.PreparedValidatorSet)
	}
	return c.config
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

func (c *broadcastValidatorCache) put(key broadcastValidatorCacheKey, set *blockproof.PreparedValidatorSet) *blockproof.PreparedValidatorSet {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.configLoaded && c.config.rootHash != key.configRootHash {
		return set
	}
	if !c.initialized || c.configRootHash != key.configRootHash {
		c.configRootHash = key.configRootHash
		c.initialized = true
		c.entries = make(map[broadcastValidatorCacheKey]*blockproof.PreparedValidatorSet)
	}
	if cached, ok := c.entries[key]; ok {
		return cached
	}
	c.entries[key] = set
	return set
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

	validators, err := s.broadcastValidatorSetForSignatures(
		ctx,
		req.Block,
		signatures.CatchainSeqno(),
		signatures.ValidatorSetHash(),
	)
	if err != nil {
		return err
	}
	return blockproof.CheckPreparedSignatures(req.Block, signatures, validators)
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
func (s *SyncCoordinator) CheckBlockFinalitySignatures(ctx context.Context, req p2p.BlockFinalitySignatureCheck) (*p2p.BlockFinalitySignatureCheckResult, error) {
	if !req.Signatures.IsSimplex() {
		return nil, fmt.Errorf("block finality broadcast %s has non-simplex validator signatures", storage.FormatBlockRef(req.Block))
	}
	if req.Block.Workchain == -1 && !req.Signatures.Final() {
		return nil, fmt.Errorf("masterchain block %s has non-final validator signatures", storage.FormatBlockRef(req.Block))
	}

	validators, err := s.broadcastValidatorSetForSignatures(
		ctx,
		req.Block,
		req.Signatures.CatchainSeqno(),
		req.Signatures.ValidatorSetHash(),
	)
	if err != nil {
		return nil, err
	}
	if err = blockproof.CheckPreparedSignatures(req.Block, req.Signatures, validators); err != nil {
		return nil, err
	}

	var signaturesCell *cell.Cell
	verifiedKey := req.Signatures.ContentKey(req.Block)
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

func (s *SyncCoordinator) broadcastValidatorSetForSignatures(
	ctx context.Context,
	block ton.BlockIDExt,
	catchainSeqno uint32,
	validatorSetHash uint32,
) (*blockproof.PreparedValidatorSet, error) {
	config, err := s.currentBroadcastValidatorConfig(ctx)
	if err != nil {
		return nil, err
	}

	return s.broadcastValidatorSetForConfig(config, block, catchainSeqno, validatorSetHash)
}

func (s *SyncCoordinator) broadcastValidatorSetForConfig(
	config broadcastValidatorConfig,
	block ton.BlockIDExt,
	catchainSeqno uint32,
	validatorSetHash uint32,
) (*blockproof.PreparedValidatorSet, error) {
	key := broadcastValidatorCacheKeyFromBlock(config.rootHash, block, catchainSeqno, validatorSetHash)
	set, err := s.broadcastValidatorCache.get(key)
	if err == nil {
		return set, nil
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return nil, err
	}

	set, err = broadcastValidatorSetFromConfig(config.cfg, block, catchainSeqno, validatorSetHash)
	if err != nil {
		return nil, err
	}
	return s.broadcastValidatorCache.put(key, set), nil
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
