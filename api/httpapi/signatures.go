package httpapi

import (
	"context"
	"encoding/base64"

	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	blockSignaturesType        = "blocks.blockSignatures"
	blockSignaturesSimplexType = "blocks.blockSignatures.simplex"
	blockSignatureType         = "blocks.signature"
)

type blockSignatures struct {
	Type       string           `json:"@type"`
	ID         blockIDExt       `json:"id"`
	Signatures []blockSignature `json:"signatures"`
}

type blockSignaturesSimplex struct {
	Type       string           `json:"@type"`
	ID         blockIDExt       `json:"id"`
	Signatures []blockSignature `json:"signatures"`
	SessionID  string           `json:"session_id"`
	Slot       int32            `json:"slot"`
	Candidate  string           `json:"candidate"`
}

type blockSignature struct {
	Type        string `json:"@type"`
	NodeIDShort string `json:"node_id_short"`
	Signature   string `json:"signature"`
}

func (s *Server) handleMasterchainBlockSignatures(ctx context.Context, params requestParams) (any, *apiError) {
	seqno, apiErr := params.requiredUint32("seqno")
	if apiErr != nil {
		return nil, apiErr
	}

	id, err := s.store.LookupBlockBySeqNo(ctx, storage.BlockSeqRef{
		Workchain: masterchainWorkchain,
		Shard:     masterchainShard,
		SeqNo:     seqno,
	})
	if err != nil {
		return nil, internalError("cannot lookup masterchain block: " + err.Error())
	}

	signatures, apiErr := s.masterchainSignatureSet(ctx, id)
	if apiErr != nil {
		return nil, apiErr
	}
	return httpBlockSignatures(id, signatures), nil
}

func (s *Server) masterchainSignatureSet(ctx context.Context, id ton.BlockIDExt) (any, *apiError) {
	meta, err := s.store.BlockMeta(ctx, id)
	if err != nil {
		return nil, internalError("cannot load block metadata: " + err.Error())
	}

	kind := storage.ServedProofBlock
	if meta.Has(storage.BlockMetaIsKeyBlock) {
		kind = storage.ServedProofKeyBlock
	}
	if !meta.HasProof(kind) {
		return nil, internalError("cannot load masterchain block signatures: proof is not available")
	}

	data, err := s.store.BlockProof(ctx, kind, id)
	if err != nil {
		return nil, internalError("cannot load masterchain block proof: " + err.Error())
	}

	proofRoot, err := cell.FromBOC(data)
	if err != nil {
		return nil, internalError("cannot parse masterchain block proof: " + err.Error())
	}
	if err = blockproof.CheckProofShape(id, proofRoot, false); err != nil {
		return nil, internalError("invalid masterchain block proof: " + err.Error())
	}

	parsed, err := blockproof.ParseCell(id, proofRoot)
	if err != nil {
		return nil, internalError("cannot parse masterchain block proof: " + err.Error())
	}
	signatures, err := blockproof.LiteSignatureSet(parsed.Proof.Signatures)
	if err != nil {
		return nil, internalError("cannot extract masterchain block signatures: " + err.Error())
	}
	return signatures, nil
}

func httpBlockSignatures(id ton.BlockIDExt, signatures any) any {
	switch sigs := signatures.(type) {
	case ton.SignatureSetSimplex:
		return blockSignaturesSimplex{
			Type:       blockSignaturesSimplexType,
			ID:         blockIDExtFromTON(id),
			Signatures: httpSignatures(sigs.Signatures),
			SessionID:  tonHash(sigs.SessionID),
			Slot:       sigs.Slot,
			Candidate:  base64.StdEncoding.EncodeToString(sigs.Candidate),
		}
	case ton.SignatureSetOrdinary:
		return blockSignatures{
			Type:       blockSignaturesType,
			ID:         blockIDExtFromTON(id),
			Signatures: httpSignatures(sigs.Signatures),
		}
	case ton.SignatureSet:
		return blockSignatures{
			Type:       blockSignaturesType,
			ID:         blockIDExtFromTON(id),
			Signatures: httpSignatures(sigs.Signatures),
		}
	default:
		return blockSignatures{
			Type:       blockSignaturesType,
			ID:         blockIDExtFromTON(id),
			Signatures: nil,
		}
	}
}

func httpSignatures(signatures []ton.Signature) []blockSignature {
	out := make([]blockSignature, 0, len(signatures))
	for _, sig := range signatures {
		out = append(out, blockSignature{
			Type:        blockSignatureType,
			NodeIDShort: tonHash(sig.NodeIDShort),
			Signature:   base64.StdEncoding.EncodeToString(sig.Signature),
		})
	}
	return out
}
