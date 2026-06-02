package service

import (
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
	block               ton.BlockIDExt
	prevRef             ton.BlockIDExt
	stateUpdateFromHash cell.Hash
	validatorCacheKey   masterchainValidatorCacheKey
	keyBlock            bool
	proofSignatures     *blockproof.ValidatorSignatureSet
	signaturesChecked   bool
}

type checkedMasterchainConsensus struct {
	block   ton.BlockIDExt
	current ton.BlockIDExt
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

func (s *Service) checkMasterchainBlockConsensusWithProof(current *tnstore.BlockState, proof *masterchainConsensusProof) (*checkedMasterchainConsensus, error) {
	if proof == nil {
		return nil, fmt.Errorf("masterchain block has no prepared consensus proof")
	}
	if proof.block.Workchain != -1 || proof.block.Shard != topShard {
		return nil, fmt.Errorf("consensus proof is not for masterchain block: %s", tnstore.FormatBlockRef(proof.block))
	}
	if current.Cell == nil {
		return nil, fmt.Errorf("current masterchain state %s is missing cell tree", tnstore.FormatBlockRef(current.Block))
	}

	if !proof.prevRef.Equals(&current.Block) {
		return nil, fmt.Errorf("%w: block=%s prev=%s current=%s", errMasterchainPrevMismatch, tnstore.FormatBlockRef(proof.block), tnstore.FormatBlockRef(proof.prevRef), tnstore.FormatBlockRef(current.Block))
	}
	currentHash := current.Cell.Virtualize(0).HashKey(0)
	if proof.stateUpdateFromHash != currentHash {
		return nil, fmt.Errorf("invalid previous state hash in proof %s: expected %x, got %x", tnstore.FormatBlockRef(proof.block), currentHash[:], proof.stateUpdateFromHash[:])
	}

	if proof.signaturesChecked {
		return &checkedMasterchainConsensus{
			block:   proof.block,
			current: current.Block,
		}, nil
	}

	validators, err := s.masterchainValidatorsForConsensus(current, proof.block, proof.validatorCacheKey)
	if err != nil {
		return nil, err
	}
	if err = blockproof.CheckPreparedMasterchainSignaturesWithValidators(proof.block, proof.proofSignatures, validators); err != nil {
		return nil, err
	}
	proof.signaturesChecked = true
	return &checkedMasterchainConsensus{
		block:   proof.block,
		current: current.Block,
	}, nil
}

func (c *checkedMasterchainConsensus) validateFor(current *tnstore.BlockState, block ton.BlockIDExt) error {
	if c == nil {
		return fmt.Errorf("masterchain block %s has no checked consensus", tnstore.FormatBlockRef(block))
	}
	if !c.block.Equals(&block) {
		return fmt.Errorf("checked consensus block mismatch: checked=%s block=%s", tnstore.FormatBlockRef(c.block), tnstore.FormatBlockRef(block))
	}
	if !c.current.Equals(&current.Block) {
		return fmt.Errorf("%w: block=%s checked_current=%s current=%s", errMasterchainPrevMismatch, tnstore.FormatBlockRef(block), tnstore.FormatBlockRef(c.current), tnstore.FormatBlockRef(current.Block))
	}
	return nil
}

func (s *Service) checkedConsensusForPreparedBlock(current *tnstore.BlockState, block PreparedBlock) (*checkedMasterchainConsensus, error) {
	if block.consensusChecked != nil {
		if err := block.consensusChecked.validateFor(current, block.ID); err != nil {
			return nil, err
		}
		return block.consensusChecked, nil
	}
	if block.consensus == nil || !block.consensus.block.Equals(&block.ID) {
		return nil, fmt.Errorf("masterchain block %s has no prepared consensus proof", block.BlockRef())
	}
	return s.checkMasterchainBlockConsensusWithProof(current, block.consensus)
}

func prepareMasterchainConsensusProof(block ton.BlockIDExt, proofRoot *cell.Cell) (*masterchainConsensusProof, *blockproof.Parsed, error) {
	if proofRoot == nil {
		return nil, nil, fmt.Errorf("masterchain block %s has no parsed proof root", tnstore.FormatBlockRef(block))
	}

	parsed, err := blockproof.ParseCell(block, proofRoot)
	if err != nil {
		return nil, nil, err
	}
	proof, err := masterchainConsensusProofFromParsed(block, parsed)
	if err != nil {
		return nil, nil, err
	}
	return proof, parsed, nil
}

func prepareMasterchainConsensusProofBOC(block ton.BlockIDExt, proofBOC []byte) (*masterchainConsensusProof, error) {
	if len(proofBOC) == 0 {
		return nil, fmt.Errorf("masterchain block %s has no proof BOC", tnstore.FormatBlockRef(block))
	}

	parsed, err := blockproof.ParseBOC(block, proofBOC)
	if err != nil {
		return nil, err
	}
	return masterchainConsensusProofFromParsed(block, parsed)
}

func masterchainConsensusProofFromParsed(block ton.BlockIDExt, parsed *blockproof.Parsed) (*masterchainConsensusProof, error) {
	if block.Workchain != -1 || block.Shard != topShard {
		return nil, fmt.Errorf("consensus proof is not for masterchain block: %s", tnstore.FormatBlockRef(block))
	}
	if parsed == nil || parsed.Proof == nil || parsed.Block == nil || parsed.Meta == nil {
		return nil, fmt.Errorf("masterchain block proof %s is incomplete", tnstore.FormatBlockRef(block))
	}
	if len(parsed.Meta.PrevRefs) != 1 {
		return nil, fmt.Errorf("masterchain block proof %s has %d previous refs", tnstore.FormatBlockRef(block), len(parsed.Meta.PrevRefs))
	}
	if parsed.Proof.Signatures == nil {
		return nil, fmt.Errorf("masterchain block proof %s has no validator signatures", tnstore.FormatBlockRef(block))
	}

	fromHash, err := stateUpdateFromHash(block, parsed.Block.StateUpdate)
	if err != nil {
		return nil, err
	}
	proofSignatures, err := blockproof.PrepareMasterchainSignatureSet(block, parsed.Block, parsed.Proof.Signatures)
	if err != nil {
		return nil, err
	}

	return &masterchainConsensusProof{
		block:               block,
		prevRef:             parsed.Meta.PrevRefs[0],
		stateUpdateFromHash: fromHash,
		validatorCacheKey:   masterchainValidatorCacheKeyFromBlock(parsed.Block),
		keyBlock:            parsed.Block.BlockInfo.KeyBlock,
		proofSignatures:     proofSignatures,
	}, nil
}

func stateUpdateFromHash(block ton.BlockIDExt, update *cell.Cell) (cell.Hash, error) {
	if update == nil {
		return cell.Hash{}, fmt.Errorf("block proof %s has no state update", tnstore.FormatBlockRef(block))
	}
	if update.GetType() != cell.MerkleUpdateCellType {
		return cell.Hash{}, fmt.Errorf("block proof %s has non-merkle state update", tnstore.FormatBlockRef(block))
	}

	oldState, err := update.PeekRef(0)
	if err != nil {
		return cell.Hash{}, fmt.Errorf("read old state hash from %s: %w", tnstore.FormatBlockRef(block), err)
	}
	return oldState.HashKey(0), nil
}

func (s *Service) masterchainValidatorsForConsensus(current *tnstore.BlockState, blockID ton.BlockIDExt, key masterchainValidatorCacheKey) ([]*tlb.ValidatorAddr, error) {
	if validators, ok := s.validatorCache.get(key); ok {
		return validators, nil
	}

	cfg, err := blockproof.ConfigFromMasterchainState(current)
	if err != nil {
		return nil, err
	}
	validators, err := blockproof.MasterchainValidatorsForBlock(cfg, &blockID, key.catchainSeqno)
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
