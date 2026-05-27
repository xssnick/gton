package service

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/xssnick/gton/service/blockproof"
	tnstore "github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type masterchainValidatorCacheKey struct {
	// A key block is signed by the previous epoch. The first block that points
	// to the new key block changes this seqno and clears the cache.
	prevKeyBlockSeqno uint32
	catchainSeqno     uint32
	validatorSetHash  uint32
}

type masterchainConsensusProof struct {
	block             ton.BlockIDExt
	parsed            *blockproof.Parsed
	signaturesChecked bool
}

type masterchainValidatorCache struct {
	mu                sync.Mutex
	prevKeyBlockSeqno uint32
	initialized       bool
	entries           map[masterchainValidatorCacheKey][]*tlb.ValidatorAddr
}

func (c *masterchainValidatorCache) get(key masterchainValidatorCacheKey) ([]*tlb.ValidatorAddr, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.initialized || c.prevKeyBlockSeqno != key.prevKeyBlockSeqno {
		return nil, false
	}
	validators, ok := c.entries[key]
	return validators, ok
}

func (c *masterchainValidatorCache) put(key masterchainValidatorCacheKey, validators []*tlb.ValidatorAddr) []*tlb.ValidatorAddr {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.initialized || c.prevKeyBlockSeqno != key.prevKeyBlockSeqno {
		c.prevKeyBlockSeqno = key.prevKeyBlockSeqno
		c.initialized = true
		c.entries = make(map[masterchainValidatorCacheKey][]*tlb.ValidatorAddr)
	}
	if cached, ok := c.entries[key]; ok {
		return cached
	}
	c.entries[key] = validators
	return validators
}

func (s *Service) validateMasterchainBlockConsensusWithProof(current *tnstore.BlockState, downloaded tonBlockForConsensus, proof *masterchainConsensusProof) error {
	if downloaded.block.Workchain != -1 || downloaded.block.Shard != topShard {
		return nil
	}
	if current == nil {
		return fmt.Errorf("current masterchain state is nil")
	}
	if current.Cell == nil {
		return fmt.Errorf("current masterchain state %s is missing cell tree", tnstore.FormatBlockRef(current.Block))
	}
	if len(downloaded.proofBOC) == 0 {
		return fmt.Errorf("masterchain block %s has no proof", tnstore.FormatBlockRef(downloaded.block))
	}

	if proof == nil || !proof.block.Equals(&downloaded.block) {
		var err error
		proof, err = prepareMasterchainConsensusProof(downloaded.block, downloaded.proofBOC)
		if err != nil {
			return err
		}
	}

	parsed := proof.parsed
	if parsed == nil || parsed.Proof == nil || parsed.Block == nil || parsed.Meta == nil {
		return fmt.Errorf("masterchain block proof %s is incomplete", tnstore.FormatBlockRef(downloaded.block))
	}
	if len(parsed.Meta.PrevRefs) != 1 || !parsed.Meta.PrevRefs[0].Equals(&current.Block) {
		return fmt.Errorf("%w: block=%s prev_refs=%d current=%s", errMasterchainPrevMismatch, tnstore.FormatBlockRef(downloaded.block), len(parsed.Meta.PrevRefs), tnstore.FormatBlockRef(current.Block))
	}
	if err := blockproof.ValidateStateUpdateStartsFrom(current, downloaded.block, parsed.Block.StateUpdate); err != nil {
		return err
	}

	if proof.signaturesChecked {
		return nil
	}

	if parsed.Proof.Signatures == nil {
		return fmt.Errorf("masterchain block proof %s has no validator signatures", tnstore.FormatBlockRef(downloaded.block))
	}

	validators, err := s.masterchainValidatorsForConsensus(current, downloaded.block, parsed.Block)
	if err != nil {
		return err
	}
	if downloaded.broadcastSignatures != nil {
		if err = blockproof.CheckMasterchainSignaturesWithValidators(downloaded.block, parsed.Block, downloaded.broadcastSignatures, validators); err != nil {
			return fmt.Errorf("check broadcast signatures for %s: %w", tnstore.FormatBlockRef(downloaded.block), err)
		}
	}
	if err = blockproof.CheckMasterchainSignaturesWithValidators(downloaded.block, parsed.Block, parsed.Proof.Signatures, validators); err != nil {
		return err
	}
	proof.signaturesChecked = true
	return nil
}

func (s *Service) ValidateMasterchainBroadcastSignatures(ctx context.Context, block ton.BlockIDExt, proofBOC []byte, signatures *cell.Cell) error {
	if block.Workchain != -1 || block.Shard != topShard {
		return nil
	}
	if signatures == nil {
		return fmt.Errorf("masterchain broadcast %s has no signatures", tnstore.FormatBlockRef(block))
	}

	proof, err := prepareMasterchainConsensusProof(block, proofBOC)
	if err != nil {
		return err
	}
	if proof.parsed == nil || proof.parsed.Block == nil || proof.parsed.Meta == nil {
		return fmt.Errorf("masterchain broadcast proof %s is incomplete", tnstore.FormatBlockRef(block))
	}

	current, err := s.masterchainStateForBroadcastSignatureValidation(ctx, proof.parsed.Meta)
	if err != nil {
		return err
	}
	validators, err := s.masterchainValidatorsForConsensus(current, block, proof.parsed.Block)
	if err != nil {
		return fmt.Errorf("%w: validator set is not ready for broadcast %s: %v", tnstore.ErrNotFound, tnstore.FormatBlockRef(block), err)
	}
	if err = blockproof.CheckMasterchainSignaturesWithValidators(block, proof.parsed.Block, signatures, validators); err != nil {
		return fmt.Errorf("check broadcast signatures for %s: %w", tnstore.FormatBlockRef(block), err)
	}
	return nil
}

func (s *Service) masterchainStateForBroadcastSignatureValidation(ctx context.Context, meta *tnstore.BlockMeta) (*tnstore.BlockState, error) {
	if meta == nil || len(meta.PrevRefs) != 1 {
		return nil, fmt.Errorf("masterchain broadcast proof has no single previous ref")
	}

	prev := meta.PrevRefs[0]
	if prev.Workchain != -1 || prev.Shard != topShard {
		return nil, fmt.Errorf("masterchain broadcast previous ref is not masterchain: %s", tnstore.FormatBlockRef(prev))
	}

	current, err := s.loadMasterStateForConsensus(ctx, prev)
	if err == nil {
		return current, nil
	}
	if errors.Is(err, tnstore.ErrNotFound) {
		return nil, fmt.Errorf("%w: previous masterchain state %s is not ready", tnstore.ErrNotFound, tnstore.FormatBlockRef(prev))
	}
	return nil, err
}

func prepareMasterchainConsensusProof(block ton.BlockIDExt, proofBOC []byte) (*masterchainConsensusProof, error) {
	parsed, err := blockproof.ParseBOC(block, proofBOC)
	if err != nil {
		return nil, err
	}
	return &masterchainConsensusProof{block: block, parsed: parsed}, nil
}

func (s *Service) masterchainValidatorsForConsensus(current *tnstore.BlockState, blockID ton.BlockIDExt, block *tlb.Block) ([]*tlb.ValidatorAddr, error) {
	key := masterchainValidatorCacheKeyFromBlock(block)
	if validators, ok := s.validatorCache.get(key); ok {
		return validators, nil
	}

	cfg, err := blockproof.ConfigFromMasterchainState(current)
	if err != nil {
		return nil, err
	}
	validators, err := blockproof.MasterchainValidatorsForBlock(cfg, &blockID, block.BlockInfo.GenCatchainSeqno)
	if err != nil {
		return nil, err
	}
	return s.validatorCache.put(key, validators), nil
}

func masterchainValidatorCacheKeyFromBlock(block *tlb.Block) masterchainValidatorCacheKey {
	return masterchainValidatorCacheKey{
		prevKeyBlockSeqno: block.BlockInfo.PrevKeyBlockSeqno,
		catchainSeqno:     block.BlockInfo.GenCatchainSeqno,
		validatorSetHash:  block.BlockInfo.GenValidatorListHashShort,
	}
}

type tonBlockForConsensus struct {
	block               ton.BlockIDExt
	proofBOC            []byte
	broadcastSignatures *cell.Cell
}
