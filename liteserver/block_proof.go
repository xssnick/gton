package liteserver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	maxBlockProofLinks            = 16
	maxBlockProofBaseCacheEntries = 8
)

type blockProofBase struct {
	block      ton.BlockIDExt
	prevBlocks *tlb.OldMcBlocksInfoAugDict
	isKey      bool
}

type blockProofRequest struct {
	from       ton.BlockIDExt
	to         ton.BlockIDExt
	base       ton.BlockIDExt
	prevBlocks *tlb.OldMcBlocksInfoAugDict
	baseIsKey  bool
}

func (s *Server) handleBlockProof(ctx context.Context, query ton.GetBlockProof) tl.Serializable {
	req, err := s.prepareBlockProofRequest(ctx, query)
	if err != nil {
		return errorResponse(err, "cannot build block proof")
	}

	proof, err := s.blockProofChain(ctx, req)
	if err != nil {
		return errorResponse(err, "cannot build block proof")
	}
	return proof
}

func (s *Server) prepareBlockProofRequest(ctx context.Context, query ton.GetBlockProof) (blockProofRequest, error) {
	if !isFullBlockID(query.KnownBlock) || query.KnownBlock.Workchain != masterchainID {
		return blockProofRequest{}, fmt.Errorf("source block must be a valid masterchain block id")
	}

	from := *query.KnownBlock
	to, base, err := s.blockProofTargetAndBase(ctx, from, query)
	if err != nil {
		return blockProofRequest{}, err
	}
	if from.SeqNo > base.SeqNo {
		return blockProofRequest{}, fmt.Errorf("client knows block %s newer than reference masterchain block %s", storage.FormatBlockRef(from), storage.FormatBlockRef(base))
	}
	if to.SeqNo > base.SeqNo {
		return blockProofRequest{}, fmt.Errorf("target block %s is newer than reference masterchain block %s", storage.FormatBlockRef(to), storage.FormatBlockRef(base))
	}
	if query.Mode&0x1000 == 0 && blockIDEqual(from, to) && blockIDEqual(from, base) {
		return blockProofRequest{
			from: from,
			to:   to,
			base: base,
		}, nil
	}

	proofBase, err := s.loadBlockProofBase(ctx, base)
	if err != nil {
		return blockProofRequest{}, err
	}
	if err = s.checkKnownMasterBlock(proofBase, from); err != nil {
		return blockProofRequest{}, fmt.Errorf("proof source masterchain block %s is unknown from the perspective of reference block %s: %w", storage.FormatBlockRef(from), storage.FormatBlockRef(base), err)
	}
	if err = s.checkKnownMasterBlock(proofBase, to); err != nil {
		return blockProofRequest{}, fmt.Errorf("proof destination masterchain block %s is unknown from the perspective of reference block %s: %w", storage.FormatBlockRef(to), storage.FormatBlockRef(base), err)
	}

	return blockProofRequest{
		from:       from,
		to:         to,
		base:       base,
		prevBlocks: proofBase.prevBlocks,
		baseIsKey:  proofBase.isKey,
	}, nil
}

func (s *Server) loadBlockProofBase(ctx context.Context, block ton.BlockIDExt) (*blockProofBase, error) {
	key := storage.BlockKey(block)
	s.blockProofBasesMu.Lock()
	cached := s.blockProofBases[key]
	s.blockProofBasesMu.Unlock()
	if cached != nil {
		return cached, nil
	}

	state, err := s.loadStateRoot(ctx, block)
	if err != nil {
		return nil, err
	}
	prevBlocks, err := blockProofPrevBlocks(state)
	if err != nil {
		return nil, err
	}
	isKey, err := s.blockIsKey(ctx, block, nil)
	if err != nil {
		return nil, err
	}

	base := &blockProofBase{
		block:      block,
		prevBlocks: prevBlocks,
		isKey:      isKey,
	}

	s.blockProofBasesMu.Lock()
	if s.blockProofBases == nil {
		s.blockProofBases = make(map[string]*blockProofBase)
	}
	if existing := s.blockProofBases[key]; existing != nil {
		s.blockProofBasesMu.Unlock()
		return existing, nil
	}
	s.blockProofBases[key] = base
	s.blockProofBaseOrder = append(s.blockProofBaseOrder, key)
	for len(s.blockProofBaseOrder) > maxBlockProofBaseCacheEntries {
		delete(s.blockProofBases, s.blockProofBaseOrder[0])
		s.blockProofBaseOrder = s.blockProofBaseOrder[1:]
	}
	s.blockProofBasesMu.Unlock()

	return base, nil
}

func (s *Server) blockProofTargetAndBase(ctx context.Context, from ton.BlockIDExt, query ton.GetBlockProof) (ton.BlockIDExt, ton.BlockIDExt, error) {
	if query.Mode&1 != 0 {
		if !isFullBlockID(query.TargetBlock) || query.TargetBlock.Workchain != masterchainID {
			return ton.BlockIDExt{}, ton.BlockIDExt{}, fmt.Errorf("destination block must be a valid masterchain block id")
		}
		to := *query.TargetBlock
		if query.Mode&0x1000 != 0 {
			base := from
			if to.SeqNo > base.SeqNo {
				base = to
			}
			return to, base, nil
		}

		current, err := s.store.CurrentState(ctx)
		if err != nil {
			return ton.BlockIDExt{}, ton.BlockIDExt{}, err
		}
		return to, current.Masterchain.Block, nil
	}

	current, err := s.store.CurrentState(ctx)
	if err != nil {
		return ton.BlockIDExt{}, ton.BlockIDExt{}, err
	}
	return current.Masterchain.Block, current.Masterchain.Block, nil
}

func (s *Server) checkKnownMasterBlock(base *blockProofBase, id ton.BlockIDExt) error {
	if blockIDEqual(base.block, id) {
		return nil
	}

	old, err := runMethodOldMasterBlockID(base.prevBlocks, id.SeqNo)
	if err != nil {
		return err
	}
	if !blockIDEqual(old, id) {
		return fmt.Errorf("state contains %s for seqno %d", storage.FormatBlockRef(old), id.SeqNo)
	}
	return nil
}

func (s *Server) blockProofChain(ctx context.Context, req blockProofRequest) (ton.PartialBlockProof, error) {
	current := req.from
	steps := make([]any, 0, 2)

	for len(steps) < maxBlockProofLinks && !blockIDEqual(current, req.to) {
		if current.SeqNo == req.to.SeqNo {
			return ton.PartialBlockProof{}, fmt.Errorf("cannot have two different masterchain blocks %s and %s of the same height", storage.FormatBlockRef(req.to), storage.FormatBlockRef(current))
		}

		if req.to.SeqNo < current.SeqNo {
			step, err := s.blockProofLinkBackward(ctx, current, req.to)
			if err != nil {
				return ton.PartialBlockProof{}, err
			}
			steps = append(steps, step)
			current = req.to
			continue
		}

		prevKey, err := s.prevKeyBlockInState(req, current.SeqNo)
		if err != nil {
			return ton.PartialBlockProof{}, err
		}
		if prevKey.SeqNo > current.SeqNo || (prevKey.SeqNo == current.SeqNo && !blockIDEqual(prevKey, current)) {
			return ton.PartialBlockProof{}, fmt.Errorf("block %s cannot be the previous key block for %s", storage.FormatBlockRef(prevKey), storage.FormatBlockRef(current))
		}
		if prevKey.SeqNo != current.SeqNo {
			step, err := s.blockProofLinkBackward(ctx, current, prevKey)
			if err != nil {
				return ton.PartialBlockProof{}, err
			}
			steps = append(steps, step)
			current = prevKey
			continue
		}

		next, err := s.nextKeyBlockInState(req, current.SeqNo+1)
		if errors.Is(err, storage.ErrNotFound) {
			next = req.to
		} else if err != nil {
			return ton.PartialBlockProof{}, err
		}
		if next.SeqNo <= current.SeqNo {
			return ton.PartialBlockProof{}, fmt.Errorf("cannot construct forward proof link from %s to %s", storage.FormatBlockRef(current), storage.FormatBlockRef(next))
		}

		step, err := s.blockProofLinkForward(ctx, current, next)
		if err != nil {
			return ton.PartialBlockProof{}, err
		}
		steps = append(steps, step)
		current = next
	}

	return ton.PartialBlockProof{
		Complete: blockIDEqual(current, req.to),
		From:     cloneBlockID(req.from),
		To:       cloneBlockID(current),
		Steps:    steps,
	}, nil
}

func (s *Server) blockProofLinkForward(ctx context.Context, from ton.BlockIDExt, to ton.BlockIDExt) (ton.BlockLinkForward, error) {
	fromRoot, err := s.forwardSourceRoot(ctx, from)
	if err != nil {
		return ton.BlockLinkForward{}, fmt.Errorf("load source proof for %s: %w", storage.FormatBlockRef(from), err)
	}

	toRoot, toParsed, err := s.storedMasterProofRoot(ctx, to, false)
	if err != nil {
		return ton.BlockLinkForward{}, fmt.Errorf("load target proof for %s: %w", storage.FormatBlockRef(to), err)
	}

	configProof, err := s.forwardConfigProofBOC(from, fromRoot)
	if err != nil {
		return ton.BlockLinkForward{}, err
	}

	destProof, err := blockHeaderProofBOC(toRoot, to, 0)
	if err != nil {
		return ton.BlockLinkForward{}, err
	}

	signatures, err := blockproof.LiteSignatureSet(toParsed.Proof.Signatures)
	if err != nil {
		return ton.BlockLinkForward{}, fmt.Errorf("extract signatures for %s: %w", storage.FormatBlockRef(to), err)
	}

	return ton.BlockLinkForward{
		ToKeyBlock:   toParsed.Meta.Has(storage.BlockMetaIsKeyBlock),
		From:         cloneBlockID(from),
		To:           cloneBlockID(to),
		DestProof:    destProof,
		ConfigProof:  configProof,
		SignatureSet: signatures,
	}, nil
}

func (s *Server) forwardSourceRoot(ctx context.Context, from ton.BlockIDExt) (*cell.Cell, error) {
	if from.SeqNo != 0 {
		root, _, err := s.storedMasterProofRoot(ctx, from, true)
		return root, err
	}

	data, err := s.store.ZeroState(ctx, from)
	if err != nil {
		return nil, err
	}
	root, err := cell.FromBOC(data)
	if err != nil {
		return nil, fmt.Errorf("parse zerostate: %w", err)
	}
	if !bytes.Equal(root.Hash(0), from.RootHash) {
		return nil, fmt.Errorf("zerostate root hash mismatch")
	}
	return root, nil
}

func (s *Server) forwardConfigProofBOC(from ton.BlockIDExt, fromRoot *cell.Cell) ([]byte, error) {
	var proof *cell.Cell
	var err error
	if from.SeqNo == 0 {
		proof, err = configProof(fromRoot, configModeNeedValidatorSet, false, nil)
	} else {
		proof, err = keyBlockConfigProof(fromRoot, configModeNeedValidatorSet, false, nil)
	}
	if err != nil {
		return nil, err
	}
	return proof.ToBOCWithFlags(false), nil
}

func (s *Server) blockProofLinkBackward(ctx context.Context, from ton.BlockIDExt, to ton.BlockIDExt) (ton.BlockLinkBackward, error) {
	fromRoot, _, err := s.storedMasterProofRoot(ctx, from, true)
	if err != nil {
		return ton.BlockLinkBackward{}, fmt.Errorf("load source proof link for %s: %w", storage.FormatBlockRef(from), err)
	}

	stateRoot, err := s.loadStateRoot(ctx, from)
	if err != nil {
		return ton.BlockLinkBackward{}, err
	}
	proof, err := blockStateRootProof(fromRoot)
	if err != nil {
		return ton.BlockLinkBackward{}, err
	}

	stateProof, err := oldMasterBlockStateProof(stateRoot, to)
	if err != nil {
		return ton.BlockLinkBackward{}, err
	}

	var destProof []byte
	toKeyBlock := to.SeqNo == 0
	if to.SeqNo != 0 {
		toRoot, err := s.loadBlockRoot(ctx, to)
		if err != nil {
			return ton.BlockLinkBackward{}, err
		}
		destProof, err = blockHeaderProofBOC(toRoot, to, 0)
		if err != nil {
			return ton.BlockLinkBackward{}, err
		}
		toKeyBlock, err = s.blockIsKey(ctx, to, toRoot)
		if err != nil {
			return ton.BlockLinkBackward{}, err
		}
	}

	return ton.BlockLinkBackward{
		ToKeyBlock: toKeyBlock,
		From:       cloneBlockID(from),
		To:         cloneBlockID(to),
		DestProof:  destProof,
		Proof:      proof.ToBOCWithFlags(false),
		StateProof: stateProof.ToBOCWithFlags(false),
	}, nil
}

func oldMasterBlockStateProof(stateRoot *cell.Cell, id ton.BlockIDExt) (*cell.Cell, error) {
	return createUsageProof(stateRoot, func(root *cell.Cell) error {
		old, err := oldMasterBlockIDFromState(root, id.SeqNo)
		if err != nil {
			return err
		}
		if !blockIDEqual(old, id) {
			return fmt.Errorf("state contains %s for seqno %d", storage.FormatBlockRef(old), id.SeqNo)
		}
		return nil
	})
}

func (s *Server) storedMasterProofRoot(ctx context.Context, id ton.BlockIDExt, link bool) (*cell.Cell, *blockproof.Parsed, error) {
	data, err := s.storedMasterProof(ctx, id, link)
	if err != nil {
		return nil, nil, err
	}

	proofRoot, err := cell.FromBOC(data)
	if err != nil {
		return nil, nil, fmt.Errorf("parse stored proof: %w", err)
	}
	if err = blockproof.CheckProofShape(id, proofRoot, link); err != nil {
		return nil, nil, err
	}
	parsed, err := blockproof.ParseCell(id, proofRoot)
	if err != nil {
		return nil, nil, err
	}

	root, err := cell.UnwrapProof(parsed.Proof.Root, id.RootHash)
	if err != nil {
		return nil, nil, err
	}
	return root, parsed, nil
}

func (s *Server) storedMasterProof(ctx context.Context, id ton.BlockIDExt, link bool) ([]byte, error) {
	if id.Workchain != masterchainID {
		return nil, fmt.Errorf("block must be a masterchain block")
	}

	meta, err := s.store.BlockMeta(ctx, id)
	if err != nil {
		return nil, err
	}

	if link {
		data, err := s.storedMasterProofByKind(ctx, id, storedMasterProofKinds(meta, true))
		if err == nil {
			return data, nil
		}
		if !errors.Is(err, storage.ErrNotFound) {
			return nil, err
		}

		full, err := s.storedMasterProofByKind(ctx, id, storedMasterProofKinds(meta, false))
		if err != nil {
			return nil, err
		}
		return blockproof.LinkBOC(id, full)
	}

	return s.storedMasterProofByKind(ctx, id, storedMasterProofKinds(meta, false))
}

func (s *Server) storedMasterProofByKind(ctx context.Context, id ton.BlockIDExt, kinds []storage.ServedProofKind) ([]byte, error) {
	for _, kind := range kinds {
		data, err := s.store.BlockProof(ctx, kind, id)
		if err == nil {
			return data, nil
		}
		if !errors.Is(err, storage.ErrNotFound) {
			return nil, err
		}
	}
	return nil, storage.ErrNotFound
}

func storedMasterProofKinds(meta *storage.BlockMeta, link bool) []storage.ServedProofKind {
	var kind storage.ServedProofKind
	if meta.Has(storage.BlockMetaIsKeyBlock) {
		if link {
			kind = storage.ServedProofKeyBlockLink
		} else {
			kind = storage.ServedProofKeyBlock
		}
	} else if link {
		kind = storage.ServedProofBlockLink
	} else {
		kind = storage.ServedProofBlock
	}

	if !meta.HasProof(kind) {
		return nil
	}
	return []storage.ServedProofKind{kind}
}

func (s *Server) blockIsKey(ctx context.Context, id ton.BlockIDExt, root *cell.Cell) (bool, error) {
	meta, err := s.store.BlockMeta(ctx, id)
	if err == nil && meta.Has(storage.BlockMetaIsKeyBlock) {
		return true, nil
	}
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return false, err
	}

	if root == nil {
		root, err = s.loadBlockRoot(ctx, id)
		if err != nil {
			return false, err
		}
	}
	block, err := storage.ParseVerifiedBlockCell(id, root)
	if err != nil {
		return false, err
	}
	return block.BlockInfo.KeyBlock, nil
}

func (s *Server) prevKeyBlockInState(req blockProofRequest, seqno uint32) (ton.BlockIDExt, error) {
	if req.baseIsKey && req.base.SeqNo <= seqno {
		return req.base, nil
	}

	key, err := keyBlockFromPrevBlocks(req.prevBlocks, seqno, false)
	if errors.Is(err, storage.ErrNotFound) {
		return ton.BlockIDExt{}, fmt.Errorf("cannot compute previous key block for seqno %d: %w", seqno, err)
	}
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	return key, nil
}

func (s *Server) nextKeyBlockInState(req blockProofRequest, seqno uint32) (ton.BlockIDExt, error) {
	key, err := keyBlockFromPrevBlocks(req.prevBlocks, seqno, true)
	if err == nil {
		return key, nil
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return ton.BlockIDExt{}, err
	}

	if req.baseIsKey && req.base.SeqNo >= seqno {
		return req.base, nil
	}
	return ton.BlockIDExt{}, storage.ErrNotFound
}

func blockProofPrevBlocks(stateRoot *cell.Cell) (*tlb.OldMcBlocksInfoAugDict, error) {
	prefix, err := loadMcStateExtraPrefix(stateRoot)
	if err != nil {
		return nil, err
	}

	loader, err := prefix.Info.BeginParse()
	if err != nil {
		return nil, err
	}
	if _, err := loader.LoadUInt(16); err != nil {
		return nil, err
	}
	if _, err := loader.LoadUInt(32); err != nil {
		return nil, err
	}
	if _, err := loader.LoadUInt(32); err != nil {
		return nil, err
	}
	if _, err := loader.LoadBoolBit(); err != nil {
		return nil, err
	}

	prevBlocks := &tlb.OldMcBlocksInfoAugDict{}
	if err := prevBlocks.LoadFromCell(loader); err != nil {
		return nil, err
	}
	return prevBlocks, nil
}

func keyBlockFromPrevBlocks(prevBlocks *tlb.OldMcBlocksInfoAugDict, seqno uint32, next bool) (ton.BlockIDExt, error) {
	if prevBlocks == nil || prevBlocks.IsEmpty() {
		return ton.BlockIDExt{}, storage.ErrNotFound
	}

	var found ton.BlockIDExt
	_, _, err := prevBlocks.TraverseExtra(func(keyPrefix *cell.Cell, extra *cell.Slice, value *cell.Slice) (int, error) {
		var maxLT tlb.KeyMaxLt
		if err := tlb.LoadFromCell(&maxLT, extra.Copy()); err != nil {
			return 0, err
		}
		if !maxLT.IsKey {
			return 0, nil
		}

		if value != nil {
			var ref tlb.KeyExtBlkRef
			if err := tlb.LoadFromCell(&ref, value.Copy()); err != nil {
				return 0, err
			}
			if ref.IsKey && ((next && ref.BlkRef.SeqNo >= seqno) || (!next && ref.BlkRef.SeqNo <= seqno)) {
				found = runMethodExtBlkRef(ref.BlkRef)
				return 1, nil
			}
			return 0, nil
		}

		x, bitsCount, err := keyPrefixUint32(keyPrefix)
		if err != nil {
			return 0, err
		}
		d := uint32(32 - bitsCount)
		if d == 0 {
			if (next && x >= seqno) || (!next && x <= seqno) {
				return 1, nil
			}
			return 0, nil
		}

		y := seqno >> (d - 1)
		if next {
			if y > 2*x+1 {
				return 0, nil
			}
			if y == 2*x+1 {
				return 2, nil
			}
			return 6, nil
		}
		if y < 2*x {
			return 0, nil
		}
		if y == 2*x {
			return 1, nil
		}
		return 5, nil
	})
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	if found.RootHash == nil {
		return ton.BlockIDExt{}, storage.ErrNotFound
	}
	return found, nil
}

func keyPrefixUint32(key *cell.Cell) (uint32, uint, error) {
	bitsCount := key.BitsSize()
	if bitsCount > 32 {
		return 0, 0, fmt.Errorf("old mc block key prefix is too large: %d", bitsCount)
	}
	if bitsCount == 0 {
		return 0, 0, nil
	}

	loader, err := key.BeginParse()
	if err != nil {
		return 0, 0, err
	}
	value, err := loader.LoadUInt(bitsCount)
	if err != nil {
		return 0, 0, err
	}
	if value > math.MaxUint32 {
		return 0, 0, fmt.Errorf("old mc block key prefix overflow")
	}
	return uint32(value), bitsCount, nil
}
