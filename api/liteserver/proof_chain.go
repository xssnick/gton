package liteserver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/gton/service/storage"

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

	master, err := blockproof.MasterRefForBlock(ctx, s.store, id)
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

	// blockHeader memoizes the serialized (block, mode=0) header proof in
	// respCache, shared with getBlockHeader responses for the same block.
	targetHeader, err := s.blockHeader(ctx, target, 0)
	if err != nil {
		return errorResponse(err, "cannot build block header proof")
	}
	header := targetHeader.HeaderProof

	prevHeader, err := s.lookupPrevHeaderProof(ctx, target, query)
	if err != nil {
		return errorResponse(err, "cannot build previous block header proof")
	}

	base := target
	var links []ton.ShardBlockLink
	if target.Workchain != masterchainID {
		base, err = blockproof.MasterRefForBlock(ctx, s.store, target)
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

	prevHeader, err := s.blockHeader(ctx, prev, 0)
	if err != nil {
		return nil, err
	}
	return prevHeader.HeaderProof, nil
}

type lookupMasterProofParts struct {
	clientStateProof []byte
	mcBlockProof     []byte
}

func (s *Server) lookupClientMasterProofs(ctx context.Context, client ton.BlockIDExt, proved ton.BlockIDExt) ([]byte, []byte, error) {
	if !isFullBlockID(&client) || client.Workchain != masterchainID {
		return nil, nil, fmt.Errorf("masterchain block id must be specified")
	}
	if blockIDEqual(client, proved) {
		return nil, nil, nil
	}

	key := liteResponseKey{kind: liteResponseLookupMasterProofs, a: storage.BlockKey(client), b: storage.BlockKey(proved)}
	value, err := s.respCache.do(ctx, key, func(ctx context.Context) (any, error) {
		return s.buildLookupClientMasterProofs(ctx, client, proved)
	})
	if err != nil {
		return nil, nil, err
	}

	parts, ok := value.(lookupMasterProofParts)
	if !ok {
		return nil, nil, fmt.Errorf("invalid lookup master proofs cache value")
	}
	return parts.clientStateProof, parts.mcBlockProof, nil
}

func (s *Server) buildLookupClientMasterProofs(ctx context.Context, client ton.BlockIDExt, proved ton.BlockIDExt) (lookupMasterProofParts, error) {
	fragments, err := s.store.BlockFragments(ctx, client)
	if err != nil {
		return lookupMasterProofParts{}, err
	}

	mcBlock, err := blockproof.OldMasterBlockStateProof(fragments.StateRoot(), proved)
	if err != nil {
		return lookupMasterProofParts{}, err
	}

	return lookupMasterProofParts{
		clientStateProof: fragments.BlockStateRootProof().ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false}),
		mcBlockProof:     mcBlock.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false}),
	}, nil
}

func (s *Server) shardBlockProofLinks(ctx context.Context, master ton.BlockIDExt, target ton.BlockIDExt) ([]ton.ShardBlockLink, error) {
	if master.Workchain != masterchainID {
		return nil, fmt.Errorf("masterchain block id must be specified")
	}

	key := liteResponseKey{kind: liteResponseShardBlockLinks, a: storage.BlockKey(master), b: storage.BlockKey(target)}
	value, err := s.respCache.do(ctx, key, func(ctx context.Context) (any, error) {
		return blockproof.ShardBlockProofLinks(ctx, s.store, master, target)
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

func (s *Server) previousBlockForPrefix(ctx context.Context, id ton.BlockIDExt, prefix storage.ShardKey) (ton.BlockIDExt, error) {
	meta, err := s.store.BlockMeta(ctx, id)
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	if len(meta.PrevRefs) == 0 {
		return ton.BlockIDExt{}, fmt.Errorf("previous block references are missing")
	}
	for _, prev := range meta.PrevRefs {
		if blockproof.ShardIntersects(storage.ShardKeyFromBlock(prev), prefix) {
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
