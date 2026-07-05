package liteserver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

var errInvalidLookupBlockWithProof = errors.New("invalid lookupBlockWithProof request")

const maxShardBlockProofLinks = 8

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

	master, err := s.masterRefForBlock(ctx, id)
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

	header, err := blockHeaderProofBOC(targetRoot, target, 0)
	if err != nil {
		return errorResponse(err, "cannot build block header proof")
	}

	prevHeader, err := s.lookupPrevHeaderProof(ctx, target, query)
	if err != nil {
		return errorResponse(err, "cannot build previous block header proof")
	}

	base := target
	var links []ton.ShardBlockLink
	if target.Workchain != masterchainID {
		base, err = s.masterRefForBlock(ctx, target)
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
		return s.store.LookupBlockBySeqNo(ctx, storage.BlockSeqRef{Workchain: key.Workchain, Shard: key.Shard, SeqNo: uint32(query.ID.Seqno)})
	case 2:
		return s.store.LookupBlockByLT(ctx, key, query.LT)
	default:
		return s.store.LookupBlockByUnixTime(ctx, key, query.UTime)
	}
}

func (s *Server) lookupPrevHeaderProof(ctx context.Context, target ton.BlockIDExt, query ton.LookupBlockWithProof) ([]byte, error) {
	if query.Mode&6 == 0 {
		return nil, nil
	}

	prev, err := s.previousBlockForPrefix(ctx, target, storage.ShardKey{
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
	return blockHeaderProofBOC(root, prev, 0)
}

func (s *Server) lookupClientMasterProofs(ctx context.Context, client ton.BlockIDExt, proved ton.BlockIDExt) ([]byte, []byte, error) {
	if !isFullBlockID(&client) || client.Workchain != masterchainID {
		return nil, nil, fmt.Errorf("masterchain block id must be specified")
	}
	if blockIDEqual(client, proved) {
		return nil, nil, nil
	}

	fragments, err := s.blockFragments(ctx, client)
	if err != nil {
		return nil, nil, err
	}

	mcBlock, err := oldMasterBlockStateProof(fragments.StateRoot(), proved)
	if err != nil {
		return nil, nil, err
	}

	return fragments.BlockStateRootProof().ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false}), mcBlock.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false}), nil
}

func oldMasterBlockIDFromInfo(info *cell.Cell, seqno uint32) (ton.BlockIDExt, error) {
	loader, err := info.BeginParse()
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	if _, err := loader.LoadUInt(16); err != nil {
		return ton.BlockIDExt{}, err
	}
	if _, err := loader.LoadUInt(32); err != nil {
		return ton.BlockIDExt{}, err
	}
	if _, err := loader.LoadUInt(32); err != nil {
		return ton.BlockIDExt{}, err
	}
	if _, err := loader.LoadBoolBit(); err != nil {
		return ton.BlockIDExt{}, err
	}

	prevBlocks := &tlb.OldMcBlocksInfoAugDict{}
	if err := prevBlocks.LoadFromCell(loader); err != nil {
		return ton.BlockIDExt{}, err
	}
	return runMethodOldMasterBlockID(prevBlocks, seqno)
}

func (s *Server) shardBlockProofLinks(ctx context.Context, master ton.BlockIDExt, target ton.BlockIDExt) ([]ton.ShardBlockLink, error) {
	if master.Workchain != masterchainID {
		return nil, fmt.Errorf("masterchain block id must be specified")
	}

	key := liteResponseKey{kind: liteResponseShardBlockLinks, a: storage.BlockKey(master), b: storage.BlockKey(target)}
	value, err := s.respCache.do(ctx, key, func(ctx context.Context) (any, error) {
		return s.buildShardBlockProofLinks(ctx, master, target)
	})
	if err != nil {
		return nil, err
	}

	links, ok := value.([]ton.ShardBlockLink)
	if !ok {
		return nil, fmt.Errorf("invalid shard block proof links cache value")
	}
	return links, nil
}

func (s *Server) buildShardBlockProofLinks(ctx context.Context, master ton.BlockIDExt, target ton.BlockIDExt) ([]ton.ShardBlockLink, error) {
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
		if len(links) == maxShardBlockProofLinks {
			return nil, fmt.Errorf("proof chain is too long")
		}

		if current.Workchain != masterchainID && next.SeqNo >= current.SeqNo {
			return nil, fmt.Errorf("proof chain does not progress")
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
	return blockHeaderProofBOC(root, id, mode)
}

func blockHeaderProofBOC(root *cell.Cell, id ton.BlockIDExt, mode uint32) ([]byte, error) {
	proof, err := blockHeaderProof(root, id, mode)
	if err != nil {
		return nil, err
	}
	return proof.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false}), nil
}

func (s *Server) masterRefForBlock(ctx context.Context, id ton.BlockIDExt) (ton.BlockIDExt, error) {
	if id.Workchain == masterchainID {
		return id, nil
	}

	meta, err := s.store.BlockMeta(ctx, id)
	if err != nil {
		if !errors.Is(err, storage.ErrNotFound) {
			return ton.BlockIDExt{}, err
		}
		return ton.BlockIDExt{}, fmt.Errorf("%w: block doesn't have masterchain ref", storage.ErrNotFound)
	}
	if !meta.MasterchainRefKnown() {
		return ton.BlockIDExt{}, fmt.Errorf("%w: block doesn't have masterchain ref", storage.ErrNotFound)
	}

	resolved, err := s.store.LookupBlockBySeqNo(ctx, storage.BlockSeqRef{Workchain: masterchainID, Shard: masterchainShard, SeqNo: meta.MasterchainRefSeqno})
	if err != nil {
		return ton.BlockIDExt{}, fmt.Errorf("lookup masterchain ref #%d: %w", meta.MasterchainRefSeqno, err)
	}
	if !isFullBlockID(&resolved) {
		return ton.BlockIDExt{}, fmt.Errorf("resolved masterchain ref is not full: %s", storage.FormatBlockRef(resolved))
	}
	return resolved, nil
}

func (s *Server) previousBlockForPrefix(ctx context.Context, id ton.BlockIDExt, prefix storage.ShardKey) (ton.BlockIDExt, error) {
	meta, err := s.store.BlockMeta(ctx, id)
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	if len(meta.PrevRefs) == 0 {
		return ton.BlockIDExt{}, fmt.Errorf("previous block references are missing")
	}
	for _, prev := range meta.PrevRefs {
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
