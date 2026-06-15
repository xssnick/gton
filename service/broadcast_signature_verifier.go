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

var _ p2p.BroadcastSignatureVerifier = (*Service)(nil)

type broadcastValidatorConfig struct {
	rootHash cell.Hash
	cfg      *tlb.BlockchainConfig
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
	configBlock    storage.BlockRootHash
	configBlockSeq uint32
	config         broadcastValidatorConfig
	configLoaded   bool
	configRootHash cell.Hash
	initialized    bool
	entries        map[broadcastValidatorCacheKey][]*tlb.ValidatorAddr
}

func (c *broadcastValidatorCache) getConfig(block ton.BlockIDExt) (broadcastValidatorConfig, bool) {
	key := storage.BlockKey(block)

	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.configLoaded {
		return broadcastValidatorConfig{}, false
	}
	if block.SeqNo < c.configBlockSeq {
		return broadcastValidatorConfig{}, false
	}
	if block.SeqNo == c.configBlockSeq && c.configBlock != key {
		return broadcastValidatorConfig{}, false
	}
	return c.config, true
}

func (c *broadcastValidatorCache) putConfig(block ton.BlockIDExt, config broadcastValidatorConfig) broadcastValidatorConfig {
	key := storage.BlockKey(block)

	c.mu.Lock()
	defer c.mu.Unlock()

	c.configBlock = key
	c.configBlockSeq = block.SeqNo
	c.config = config
	c.configLoaded = true
	return config
}

func (c *broadcastValidatorCache) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.configBlock = storage.BlockRootHash{}
	c.configBlockSeq = 0
	c.config = broadcastValidatorConfig{}
	c.configLoaded = false
	c.configRootHash = cell.Hash{}
	c.initialized = false
	clear(c.entries)
	c.entries = nil
}

func (c *broadcastValidatorCache) get(key broadcastValidatorCacheKey) ([]*tlb.ValidatorAddr, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.initialized || c.configRootHash != key.configRootHash {
		return nil, false
	}
	validators, ok := c.entries[key]
	return validators, ok
}

func (c *broadcastValidatorCache) put(key broadcastValidatorCacheKey, validators []*tlb.ValidatorAddr) []*tlb.ValidatorAddr {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.initialized || c.configRootHash != key.configRootHash {
		c.configRootHash = key.configRootHash
		c.initialized = true
		c.entries = make(map[broadcastValidatorCacheKey][]*tlb.ValidatorAddr)
	}
	if cached, ok := c.entries[key]; ok {
		return cached
	}
	c.entries[key] = validators
	return validators
}

func (s *Service) CheckBlockBroadcastSignatures(ctx context.Context, req p2p.BlockBroadcastSignatureCheck) error {
	parsed, err := blockproof.ParseCell(req.Block, req.Proof)
	if err != nil {
		return err
	}

	signatures, err := blockproof.PrepareValidatorSignatureSet(req.Block, parsed.Block, req.Signatures)
	if err != nil {
		return err
	}

	validators, err := s.broadcastValidatorsForSignatures(ctx, req.Block, signatures.CatchainSeqno(), signatures.ValidatorSetHash())
	if err != nil {
		return err
	}
	return blockproof.CheckPreparedSignaturesWithValidators(req.Block, signatures, validators)
}

func (s *Service) ValidateShardDescriptionBroadcast(ctx context.Context, req p2p.ShardDescriptionSignatureCheck) (*p2p.ShardBlockDescription, error) {
	desc, err := parseShardTopBlockDescription(req.Block, req.CatchainSeqno, req.Data)
	if err != nil {
		return nil, err
	}

	validators, err := s.broadcastValidatorsForSignatures(ctx, desc.Block, desc.CatchainSeqno, desc.ValidatorSetHash)
	if err != nil {
		return nil, err
	}
	if err = blockproof.CheckPreparedSignaturesWithValidators(desc.Block, desc.Signatures, validators); err != nil {
		return nil, err
	}
	out, err := newP2PShardBlockDescription(desc)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func newP2PShardBlockDescription(desc *shardTopBlockDescription) (*p2p.ShardBlockDescription, error) {
	if desc == nil {
		return nil, fmt.Errorf("shard top block description is empty")
	}

	out := &p2p.ShardBlockDescription{
		Block:            desc.Block,
		CatchainSeqno:    desc.CatchainSeqno,
		ValidatorSetHash: desc.ValidatorSetHash,
		Chain:            make([]p2p.ShardDescriptionLink, 0, len(desc.Chain)),
	}
	for _, link := range desc.Chain {
		proofRoot, proofBOC, err := blockproof.LinkFromRoot(link.Block, link.ProofRoot)
		if err != nil {
			return nil, fmt.Errorf("build shard description proof link for %s: %w", storage.FormatBlockRef(link.Block), err)
		}

		var masterchainRef *ton.BlockIDExt
		if link.MasterchainRef != nil {
			ref := *link.MasterchainRef
			masterchainRef = &ref
		}
		out.Chain = append(out.Chain, p2p.ShardDescriptionLink{
			Block:          link.Block,
			PrevRefs:       append([]ton.BlockIDExt(nil), link.PrevRefs...),
			MasterchainRef: masterchainRef,
			ProofRoot:      proofRoot,
			ProofBOC:       proofBOC,
		})
	}
	return out, nil
}

func (s *Service) broadcastValidatorsForSignatures(ctx context.Context, block ton.BlockIDExt, catchainSeqno uint32, validatorSetHash uint32) ([]*tlb.ValidatorAddr, error) {
	config, err := s.currentBroadcastValidatorConfig(ctx)
	if err != nil {
		return nil, err
	}

	key := broadcastValidatorCacheKeyFromBlock(config.rootHash, block, catchainSeqno, validatorSetHash)
	if validators, ok := s.broadcastValidatorCache.get(key); ok {
		return validators, nil
	}

	validators, err := broadcastValidatorsFromConfig(config.cfg, block, catchainSeqno, validatorSetHash)
	if err != nil {
		return nil, err
	}
	return s.broadcastValidatorCache.put(key, validators), nil
}

func broadcastValidatorCacheKeyFromBlock(configRootHash cell.Hash, block ton.BlockIDExt, catchainSeqno uint32, validatorSetHash uint32) broadcastValidatorCacheKey {
	return broadcastValidatorCacheKey{
		configRootHash:   configRootHash,
		workchain:        block.Workchain,
		shard:            block.Shard,
		catchainSeqno:    catchainSeqno,
		validatorSetHash: validatorSetHash,
	}
}

func broadcastValidatorsFromConfig(cfg *tlb.BlockchainConfig, block ton.BlockIDExt, catchainSeqno uint32, validatorSetHash uint32) ([]*tlb.ValidatorAddr, error) {
	// Key-block boundaries can leave valid broadcasts signed by a known previous,
	// current, or next validator set; the hash is checked before any set is used.
	candidates := []struct {
		name string
		load func() ([]*tlb.ValidatorAddr, error)
	}{
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

		hash, err := blockproof.ValidatorSetHash(catchainSeqno, validators)
		if err != nil {
			lastErr = fmt.Errorf("%s validators hash: %w", candidate.name, err)
			continue
		}
		if hash == validatorSetHash {
			return validators, nil
		}
		lastErr = fmt.Errorf("%s validators hash %08x does not match %08x", candidate.name, hash, validatorSetHash)
	}

	return nil, fmt.Errorf("validator set %08x for %s is not available: %w: %v", validatorSetHash, storage.FormatBlockRef(block), storage.ErrNotFound, lastErr)
}

func (s *Service) currentBroadcastValidatorConfig(ctx context.Context) (broadcastValidatorConfig, error) {
	current, err := s.currentStatusSnapshot(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return broadcastValidatorConfig{}, fmt.Errorf("%w: load live current state for broadcast signature check: %w", p2p.ErrBroadcastSignatureRetryable, err)
		}
		return broadcastValidatorConfig{}, fmt.Errorf("load live current state for broadcast signature check: %w", err)
	}

	if config, ok := s.broadcastValidatorCache.getConfig(current.Masterchain.Block); ok {
		return config, nil
	}

	masterState, err := s.loadMasterStateForConsensus(ctx, current.Masterchain.Block)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return broadcastValidatorConfig{}, fmt.Errorf("%w: load masterchain state %s for broadcast signature check: %w", p2p.ErrBroadcastSignatureRetryable, storage.FormatBlockRef(current.Masterchain.Block), err)
		}
		return broadcastValidatorConfig{}, fmt.Errorf("load masterchain state %s for broadcast signature check: %w", storage.FormatBlockRef(current.Masterchain.Block), err)
	}
	cfg, err := blockproof.ConfigFromMasterchainState(masterState)
	if err != nil {
		return broadcastValidatorConfig{}, err
	}
	if cfg.Root == nil {
		return broadcastValidatorConfig{}, fmt.Errorf("masterchain state %s has empty validator config root", storage.FormatBlockRef(current.Masterchain.Block))
	}
	return s.broadcastValidatorCache.putConfig(current.Masterchain.Block, broadcastValidatorConfig{
		rootHash: cfg.Root.HashKey(),
		cfg:      cfg,
	}), nil
}
