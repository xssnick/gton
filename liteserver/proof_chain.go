package liteserver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

	"flexserver/service/storage"

	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

var errInvalidLookupBlockWithProof = errors.New("invalid lookupBlockWithProof request")

func (s *Server) handleShardBlockProof(ctx context.Context, query ton.GetShardBlockProof) tl.Serializable {
	if !isFullBlockID(query.ID) {
		return ton.LSError{Code: errCodeProtoViolation, Text: "invalid BlockIdExt"}
	}

	id := *query.ID
	if id.Workchain == masterchainID {
		return ton.ShardBlockProof{
			MasterchainID: cloneBlockID(id),
			Links:         nil,
		}
	}

	master, err := s.masterRefForBlock(ctx, id, nil)
	if err != nil {
		return errorResponse(err, "cannot load masterchain reference for "+storage.FormatBlockRef(id))
	}

	links, err := s.shardBlockProofLinks(ctx, master, id)
	if err != nil {
		return errorResponse(err, "cannot build shard block proof")
	}

	return ton.ShardBlockProof{
		MasterchainID: cloneBlockID(master),
		Links:         links,
	}
}

func (s *Server) handleLookupBlockWithProof(ctx context.Context, query ton.LookupBlockWithProof) tl.Serializable {
	target, err := s.lookupBlockForProof(ctx, query)
	if err != nil {
		if errors.Is(err, errInvalidLookupBlockWithProof) {
			text := strings.TrimPrefix(err.Error(), errInvalidLookupBlockWithProof.Error()+": ")
			return ton.LSError{Code: errCodeProtoViolation, Text: text}
		}
		return errorResponse(err, "cannot lookup block")
	}

	targetRoot, err := s.loadBlockRoot(ctx, target)
	if err != nil {
		return errorResponse(err, "cannot load block "+storage.FormatBlockRef(target))
	}

	headerSk, err := blockHeaderProofSkeleton(targetRoot, target, 0)
	if err != nil {
		return errorResponse(err, "cannot build block header proof")
	}
	header, err := blockProofBOC(targetRoot, headerSk)
	if err != nil {
		return errorResponse(err, "cannot build block header proof")
	}

	prevHeader, err := s.lookupPrevHeaderProof(ctx, target, targetRoot, query)
	if err != nil {
		return errorResponse(err, "cannot build previous block header proof")
	}

	base := target
	var links []ton.ShardBlockLink
	if target.Workchain != masterchainID {
		base, err = s.masterRefForBlock(ctx, target, targetRoot)
		if err != nil {
			return errorResponse(err, "cannot load masterchain reference for "+storage.FormatBlockRef(target))
		}
		if base.SeqNo > query.MCBlockID.SeqNo {
			return ton.LSError{Code: errCodeProtoViolation, Text: "specified mc block is older than block's masterchain ref"}
		}
		links, err = s.shardBlockProofLinks(ctx, base, target)
		if err != nil {
			return errorResponse(err, "cannot build shard block proof")
		}
	}

	clientMCStateProof, mcBlockProof, err := s.lookupClientMasterProofs(ctx, *query.MCBlockID, base)
	if err != nil {
		return errorResponse(err, "cannot prove masterchain block")
	}

	return ton.LookupBlockResult{
		ID:                 cloneBlockID(target),
		Mode:               query.Mode,
		MCBlockID:          cloneBlockID(base),
		ClientMCStateProof: clientMCStateProof,
		MCBlockProof:       mcBlockProof,
		ShardLinks:         links,
		Header:             header,
		PrevHeader:         prevHeader,
	}
}

func (s *Server) lookupBlockForProof(ctx context.Context, query ton.LookupBlockWithProof) (ton.BlockIDExt, error) {
	if query.ID == nil {
		return ton.BlockIDExt{}, fmt.Errorf("%w: invalid block id requested", errInvalidLookupBlockWithProof)
	}
	if !isFullBlockID(query.MCBlockID) || query.MCBlockID.Workchain != masterchainID {
		return ton.BlockIDExt{}, fmt.Errorf("%w: masterchain block id must be specified", errInvalidLookupBlockWithProof)
	}

	selector := query.Mode & 7
	if selector != 1 && selector != 2 && selector != 4 {
		return ton.BlockIDExt{}, fmt.Errorf("%w: exactly one of mode.0, mode.1 and mode.2 bits must be set", errInvalidLookupBlockWithProof)
	}

	key := storage.BlockHistoryKey{
		Workchain: query.ID.Workchain,
		Shard:     query.ID.Shard,
	}

	switch selector {
	case 1:
		return s.store.LookupBlockBySeqNo(ctx, key, uint32(query.ID.Seqno))
	case 2:
		return s.store.LookupBlockByLT(ctx, key, query.LT)
	default:
		return s.store.LookupBlockByUnixTime(ctx, key, query.UTime)
	}
}

func (s *Server) lookupPrevHeaderProof(ctx context.Context, target ton.BlockIDExt, targetRoot *cell.Cell, query ton.LookupBlockWithProof) ([]byte, error) {
	if query.Mode&6 == 0 {
		return nil, nil
	}

	prev, err := s.previousBlockForPrefix(ctx, target, targetRoot, storage.ShardKey{
		Workchain: query.ID.Workchain,
		Shard:     query.ID.Shard,
	})
	if err != nil {
		return nil, err
	}

	root, err := s.loadBlockRoot(ctx, prev)
	if err != nil {
		return nil, err
	}
	sk, err := blockHeaderProofSkeleton(root, prev, 0)
	if err != nil {
		return nil, err
	}
	return blockProofBOC(root, sk)
}

func (s *Server) lookupClientMasterProofs(ctx context.Context, client ton.BlockIDExt, proved ton.BlockIDExt) ([]byte, []byte, error) {
	if !isFullBlockID(&client) || client.Workchain != masterchainID {
		return nil, nil, fmt.Errorf("masterchain block id must be specified")
	}
	if blockIDEqual(client, proved) {
		return nil, nil, nil
	}

	stateRoot, err := s.loadStateRoot(ctx, client)
	if err != nil {
		return nil, nil, err
	}

	prefix, err := loadMcStateExtraPrefix(stateRoot)
	if err != nil {
		return nil, nil, err
	}
	infoSk, old, err := oldMasterBlockProofSkeleton(prefix.Info, proved.SeqNo)
	if err != nil {
		return nil, nil, err
	}
	if !blockIDEqual(old, proved) {
		return nil, nil, fmt.Errorf("client mc blkid is not in prev_blocks")
	}

	clientMCState, err := s.blockProof(ctx, client)
	if err != nil {
		return nil, nil, err
	}

	stateSk := cell.CreateProofSkeleton()
	stateSk.ProofRef(3).AttachAt(prefix.infoRefIdx, infoSk)
	mcBlock, err := stateRoot.CreateProof(stateSk)
	if err != nil {
		return nil, nil, err
	}

	return clientMCState.ToBOCWithFlags(false), mcBlock.ToBOCWithFlags(false), nil
}

func oldMasterBlockProofSkeleton(info *cell.Cell, seqno uint32) (*cell.ProofSkeleton, ton.BlockIDExt, error) {
	trace := cell.NewProofTrace()
	loader := info.BeginParse().SetObserver(trace)
	if _, err := loader.LoadUInt(16); err != nil {
		return nil, ton.BlockIDExt{}, err
	}
	if _, err := loader.LoadUInt(32); err != nil {
		return nil, ton.BlockIDExt{}, err
	}
	if _, err := loader.LoadUInt(32); err != nil {
		return nil, ton.BlockIDExt{}, err
	}
	if _, err := loader.LoadBoolBit(); err != nil {
		return nil, ton.BlockIDExt{}, err
	}

	prevBlocks := &tlb.OldMcBlocksInfoAugDict{}
	if err := prevBlocks.LoadFromCell(loader); err != nil {
		return nil, ton.BlockIDExt{}, err
	}
	old, err := runMethodOldMasterBlockID(prevBlocks, seqno)
	if err != nil {
		return nil, ton.BlockIDExt{}, err
	}

	sk := trace.Skeleton()
	if prevBlocks.AugmentedDictionary != nil && !prevBlocks.IsEmpty() {
		sk.ProofRef(0).SetRecursive()
	}
	return sk, old, nil
}

func (s *Server) shardBlockProofLinks(ctx context.Context, master ton.BlockIDExt, target ton.BlockIDExt) ([]ton.ShardBlockLink, error) {
	if master.Workchain != masterchainID {
		return nil, fmt.Errorf("masterchain block id must be specified")
	}

	current := master
	links := make([]ton.ShardBlockLink, 0, 2)
	for {
		root, err := s.loadBlockRoot(ctx, current)
		if err != nil {
			return nil, err
		}

		next, err := nextShardProofBlock(current, root, target)
		if err != nil {
			return nil, err
		}

		proof, err := shardLinkProofBOC(current, root)
		if err != nil {
			return nil, err
		}
		links = append(links, ton.ShardBlockLink{
			ID:    cloneBlockID(next),
			Proof: proof,
		})

		if blockIDEqual(next, target) {
			return links, nil
		}
		if len(links) == 8 {
			return nil, fmt.Errorf("proof chain is too long")
		}

		current = next
	}
}

func nextShardProofBlock(current ton.BlockIDExt, root *cell.Cell, target ton.BlockIDExt) (ton.BlockIDExt, error) {
	block, err := storage.ParseVerifiedBlockCell(current, root)
	if err != nil {
		return ton.BlockIDExt{}, err
	}

	if current.Workchain == masterchainID {
		if block.Extra == nil || block.Extra.Custom == nil || block.Extra.Custom.ShardHashes == nil {
			return ton.BlockIDExt{}, fmt.Errorf("masterchain block is missing shard hashes")
		}
		next, _, err := shardInfoFromHashes(block.Extra.Custom.ShardHashes, target.Workchain, shardProofLookupShard(target.Shard), false)
		if err != nil {
			return ton.BlockIDExt{}, err
		}
		return next, nil
	}

	meta, err := storage.BuildBlockMetaFromParsedBlock(current, block)
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	for _, prev := range meta.PrevRefs {
		if shardIntersects(storage.ShardKeyFromBlock(prev), storage.ShardKeyFromBlock(target)) {
			return prev, nil
		}
	}
	return ton.BlockIDExt{}, fmt.Errorf("failed to find block chain")
}

func shardProofLookupShard(shard int64) int64 {
	prefixLen := shardPrefixLen(uint64(shard))
	if prefixLen < 0 {
		return shard
	}
	return int64((uint64(shard) &^ (uint64(1) << (63 - prefixLen))) | 1)
}

func shardLinkProofBOC(id ton.BlockIDExt, root *cell.Cell) ([]byte, error) {
	mode := uint32(0)
	if id.Workchain == masterchainID {
		mode = 16 | 32
	}
	sk, err := blockHeaderProofSkeleton(root, id, mode)
	if err != nil {
		return nil, err
	}
	return blockProofBOC(root, sk)
}

func blockProofBOC(root *cell.Cell, sk *cell.ProofSkeleton) ([]byte, error) {
	proof, err := root.CreateProof(sk)
	if err != nil {
		return nil, err
	}
	return proof.ToBOCWithFlags(false), nil
}

func (s *Server) masterRefForBlock(ctx context.Context, id ton.BlockIDExt, root *cell.Cell) (ton.BlockIDExt, error) {
	if id.Workchain == masterchainID {
		return id, nil
	}

	meta, err := s.store.BlockMeta(ctx, id)
	if err == nil && meta.MasterchainRef != nil {
		return *cloneBlockID(*meta.MasterchainRef), nil
	}
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return ton.BlockIDExt{}, err
	}

	if root == nil {
		root, err = s.loadBlockRoot(ctx, id)
		if err != nil {
			return ton.BlockIDExt{}, err
		}
	}

	parsed, err := storage.ParseVerifiedBlockCell(id, root)
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	if parsed.BlockInfo.MasterRef == nil {
		return ton.BlockIDExt{}, fmt.Errorf("block doesn't have masterchain ref")
	}
	return blockIDFromExtRef(masterchainID, masterchainShard, *parsed.BlockInfo.MasterRef), nil
}

func (s *Server) previousBlockForPrefix(ctx context.Context, id ton.BlockIDExt, root *cell.Cell, prefix storage.ShardKey) (ton.BlockIDExt, error) {
	meta, err := s.store.BlockMeta(ctx, id)
	if err == nil && len(meta.PrevRefs) > 0 {
		for _, prev := range meta.PrevRefs {
			if shardIntersects(storage.ShardKeyFromBlock(prev), prefix) {
				return prev, nil
			}
		}
		return ton.BlockIDExt{}, fmt.Errorf("failed to choose previous block")
	}
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return ton.BlockIDExt{}, err
	}

	if root == nil {
		root, err = s.loadBlockRoot(ctx, id)
		if err != nil {
			return ton.BlockIDExt{}, err
		}
	}

	parsed, err := storage.ParseVerifiedBlockCell(id, root)
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	parsedMeta, err := storage.BuildBlockMetaFromParsedBlock(id, parsed)
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	for _, prev := range parsedMeta.PrevRefs {
		if shardIntersects(storage.ShardKeyFromBlock(prev), prefix) {
			return prev, nil
		}
	}
	return ton.BlockIDExt{}, fmt.Errorf("failed to choose previous block")
}

func blockIDFromExtRef(workchain int32, shard int64, ref tlb.ExtBlkRef) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: workchain,
		Shard:     shard,
		SeqNo:     ref.SeqNo,
		RootHash:  bytes.Clone(ref.RootHash),
		FileHash:  bytes.Clone(ref.FileHash),
	}
}
