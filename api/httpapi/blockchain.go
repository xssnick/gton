package httpapi

import (
	"context"
	"encoding/base64"
	"strconv"

	"github.com/xssnick/tonutils-go/ton"
)

const (
	blockIDExtType        = "ton.blockIdExt"
	masterchainInfoType   = "blocks.masterchainInfo"
	masterchainWorkchain  = int32(-1)
	zeroStateHTTPAPIShard = int64(0)
)

type blockIDExt struct {
	Type      string `json:"@type"`
	Workchain int32  `json:"workchain"`
	Shard     string `json:"shard"`
	Seqno     uint32 `json:"seqno"`
	RootHash  string `json:"root_hash"`
	FileHash  string `json:"file_hash"`
}

type masterchainInfo struct {
	Type          string     `json:"@type"`
	Last          blockIDExt `json:"last"`
	StateRootHash string     `json:"state_root_hash"`
	Init          blockIDExt `json:"init"`
}

func (s *Server) handleMasterchainInfo(ctx context.Context, _ requestParams) (any, *apiError) {
	block, stateRootHash, _, err := s.store.CurrentMasterchainInfo(ctx)
	if err != nil {
		return nil, internalError("cannot obtain a valid masterchain state")
	}

	return masterchainInfo{
		Type:          masterchainInfoType,
		Last:          blockIDExtFromTON(block),
		StateRootHash: tonHash(stateRootHash),
		Init:          zeroStateBlockIDExt(s.zeroState),
	}, nil
}

func blockIDExtFromTON(id ton.BlockIDExt) blockIDExt {
	return blockIDExt{
		Type:      blockIDExtType,
		Workchain: id.Workchain,
		Shard:     strconv.FormatInt(id.Shard, 10),
		Seqno:     id.SeqNo,
		RootHash:  tonHash(id.RootHash),
		FileHash:  tonHash(id.FileHash),
	}
}

func zeroStateBlockIDExt(id ton.ZeroStateIDExt) blockIDExt {
	return blockIDExt{
		Type:      blockIDExtType,
		Workchain: id.Workchain,
		Shard:     strconv.FormatInt(zeroStateHTTPAPIShard, 10),
		Seqno:     0,
		RootHash:  tonHash(id.RootHash),
		FileHash:  tonHash(id.FileHash),
	}
}

func tonHash(hash []byte) string {
	return base64.StdEncoding.EncodeToString(hash)
}

func cloneZeroState(id ton.ZeroStateIDExt) ton.ZeroStateIDExt {
	return ton.ZeroStateIDExt{
		Workchain: id.Workchain,
		RootHash:  append([]byte(nil), id.RootHash...),
		FileHash:  append([]byte(nil), id.FileHash...),
	}
}

func internalError(message string) *apiError {
	return &apiError{
		status:  500,
		code:    500,
		message: message,
	}
}
