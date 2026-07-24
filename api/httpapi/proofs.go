package httpapi

import (
	"context"
	"encoding/base64"
	"errors"

	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

const (
	shardBlockProofType = "blocks.shardBlockProof"
	shardBlockLinkType  = "blocks.shardBlockLink"
	blockLinkBackType   = "blocks.blockLinkBack"
)

type shardBlockProof struct {
	Type    string           `json:"@type"`
	From    blockIDExt       `json:"from"`
	MCID    blockIDExt       `json:"mc_id"`
	Links   []shardBlockLink `json:"links"`
	MCProof []blockLinkBack  `json:"mc_proof"`
}

type shardBlockLink struct {
	Type  string     `json:"@type"`
	ID    blockIDExt `json:"id"`
	Proof string     `json:"proof"`
}

type blockLinkBack struct {
	Type       string     `json:"@type"`
	ToKeyBlock bool       `json:"to_key_block"`
	From       blockIDExt `json:"from"`
	To         blockIDExt `json:"to"`
	DestProof  string     `json:"dest_proof"`
	Proof      string     `json:"proof"`
	StateProof string     `json:"state_proof"`
}

func (s *Server) handleShardBlockProof(ctx context.Context, params requestParams) (any, *apiError) {
	target, apiErr := s.blockIDFromParams(ctx, params)
	if apiErr != nil {
		return nil, apiErr
	}

	from, apiErr := s.shardProofFromBlock(ctx, params)
	if apiErr != nil {
		return nil, apiErr
	}

	master, err := blockproof.MasterRefForBlock(ctx, s.store, target)
	if err != nil {
		return nil, internalError("cannot load masterchain reference: " + err.Error())
	}
	if master.SeqNo > from.SeqNo {
		return nil, internalError("from mc block is too old")
	}

	links := []shardBlockLink{}
	if target.Workchain != masterchainWorkchain {
		rawLinks, err := blockproof.ShardBlockProofLinks(ctx, s.store, master, target)
		if err != nil {
			return nil, internalError("cannot build shard block proof: " + err.Error())
		}
		links = shardBlockLinksFromTON(rawLinks)
	}

	mcProof := []blockLinkBack{}
	if !from.Equals(&master) {
		link, err := blockproof.BlockLinkBackward(ctx, s.store, from, master)
		if err != nil {
			return nil, internalError(err.Error())
		}
		mcProof = append(mcProof, blockLinkBackFromTON(link))
	}

	return shardBlockProof{
		Type:    shardBlockProofType,
		From:    blockIDExtFromTON(from),
		MCID:    blockIDExtFromTON(master),
		Links:   links,
		MCProof: mcProof,
	}, nil
}

func (s *Server) shardProofFromBlock(ctx context.Context, params requestParams) (ton.BlockIDExt, *apiError) {
	seqno, err := params.optionalUint32("from_seqno")
	hasSeqno := err == nil
	if err != nil && !errors.Is(err, errRequestParamNotFound) {
		return ton.BlockIDExt{}, asAPIError(err)
	}
	if !hasSeqno {
		block, _, _, err := s.store.CurrentMasterchainInfo(ctx)
		if err != nil {
			return ton.BlockIDExt{}, internalError("cannot obtain a valid masterchain state")
		}
		return block, nil
	}

	block, err := s.store.LookupBlockBySeqNo(ctx, storage.BlockSeqRef{
		Workchain: masterchainWorkchain,
		Shard:     masterchainShard,
		SeqNo:     seqno,
	})
	if err != nil {
		return ton.BlockIDExt{}, internalError("cannot lookup masterchain block: " + err.Error())
	}
	return block, nil
}

func shardBlockLinksFromTON(links []ton.ShardBlockLink) []shardBlockLink {
	out := make([]shardBlockLink, 0, len(links))
	for _, link := range links {
		if link.ID == nil {
			continue
		}
		out = append(out, shardBlockLink{
			Type:  shardBlockLinkType,
			ID:    blockIDExtFromTON(*link.ID),
			Proof: bytesBase64(link.Proof),
		})
	}
	return out
}

func blockLinkBackFromTON(link ton.BlockLinkBackward) blockLinkBack {
	return blockLinkBack{
		Type:       blockLinkBackType,
		ToKeyBlock: link.ToKeyBlock,
		From:       blockIDExtFromTON(*link.From),
		To:         blockIDExtFromTON(*link.To),
		DestProof:  bytesBase64(link.DestProof),
		Proof:      bytesBase64(link.Proof),
		StateProof: bytesBase64(link.StateProof),
	}
}

func bytesBase64(data []byte) string {
	return base64.StdEncoding.EncodeToString(data)
}
