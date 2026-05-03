package service

import (
	"fmt"
	"sync"

	"flexserver/service/blockproof"
	tnstore "flexserver/service/storage"

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

func (s *Service) validateMasterchainBlockConsensus(current *tnstore.BlockState, downloaded tonBlockForConsensus) error {
	return s.validateMasterchainBlockConsensusWithProof(current, downloaded, nil)
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

	signatures := downloaded.broadcastSignatures
	if signatures == nil {
		signatures = parsed.Proof.Signatures
	}
	if signatures == nil {
		return fmt.Errorf("masterchain block proof %s has no validator signatures", tnstore.FormatBlockRef(downloaded.block))
	}
	return s.checkMasterchainProofSignatures(current, downloaded.block, proof, signatures)
}

func prepareMasterchainConsensusProof(block ton.BlockIDExt, proofBOC []byte) (*masterchainConsensusProof, error) {
	parsed, err := blockproof.ParseBOC(block, proofBOC)
	if err != nil {
		return nil, err
	}
	return &masterchainConsensusProof{block: block, parsed: parsed}, nil
}

func (s *Service) checkMasterchainProofSignatures(current *tnstore.BlockState, blockID ton.BlockIDExt, proof *masterchainConsensusProof, signatures *cell.Cell) error {
	validators, err := s.masterchainValidatorsForConsensus(current, blockID, proof.parsed.Block)
	if err != nil {
		return err
	}
	if err = blockproof.CheckMasterchainSignaturesWithValidators(blockID, proof.parsed.Block, signatures, validators); err != nil {
		return err
	}
	proof.signaturesChecked = true
	return nil
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
