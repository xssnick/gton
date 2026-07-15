package httpapi

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/xssnick/gton/service/externalmsg"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	resultOKType       = "ok"
	extMessageInfoType = "raw.extMessageInfo"
)

type resultOK struct {
	Type string `json:"@type"`
}

type extMessageInfo struct {
	Type     string `json:"@type"`
	Hash     string `json:"hash"`
	HashNorm string `json:"hash_norm"`
}

func (s *Server) handleSendBoc(ctx context.Context, params requestParams) (any, *apiError) {
	if _, apiErr := s.sendBOC(ctx, params); apiErr != nil {
		return nil, apiErr
	}
	return resultOK{Type: resultOKType}, nil
}

func (s *Server) handleSendBocReturnHash(ctx context.Context, params requestParams) (any, *apiError) {
	root, apiErr := s.sendBOC(ctx, params)
	if apiErr != nil {
		return nil, apiErr
	}

	hash := root.Hash()
	return extMessageInfo{
		Type:     extMessageInfoType,
		Hash:     tonHash(hash[:]),
		HashNorm: tonHash(hash[:]),
	}, nil
}

func (s *Server) sendBOC(ctx context.Context, params requestParams) (*cell.Cell, *apiError) {
	if s.messageSender == nil {
		return nil, internalError("external message sender is not configured")
	}

	raw, apiErr := params.requiredString("boc")
	if apiErr != nil {
		return nil, apiErr
	}
	body, apiErr := parseBytes(raw, "boc")
	if apiErr != nil {
		return nil, apiErr
	}

	root, err := s.messageSender.SendForHash(ctx, body)
	if err != nil {
		if errors.Is(err, externalmsg.ErrParseFailed) {
			return nil, validationError("failed to parse post request: Error at path 'boc': " + err.Error())
		}
		return nil, internalError("cannot send external message: " + err.Error())
	}
	return root, nil
}

func parseBytes(raw string, field string) ([]byte, *apiError) {
	decoders := []*base64.Encoding{
		base64.StdEncoding,
		base64.RawStdEncoding,
		base64.URLEncoding,
		base64.RawURLEncoding,
	}
	for _, decoder := range decoders {
		data, err := decoder.DecodeString(raw)
		if err == nil {
			return data, nil
		}
	}

	return nil, validationError(fmt.Sprintf("failed to parse post request: Error at path '%s': invalid base64 encoded bytes", field))
}
