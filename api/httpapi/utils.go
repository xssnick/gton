package httpapi

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/xssnick/tonutils-go/address"
)

const (
	detectedAddressType        = "ext.utils.detectedAddress"
	detectedAddressVariantType = "ext.utils.detectedAddressVariant"
	detectedHashType           = "ext.utils.detectedHash"
)

type detectedAddress struct {
	Type          string                 `json:"@type"`
	RawForm       string                 `json:"raw_form"`
	Bounceable    detectedAddressVariant `json:"bounceable"`
	NonBounceable detectedAddressVariant `json:"non_bounceable"`
	GivenType     string                 `json:"given_type"`
	TestOnly      bool                   `json:"test_only"`
}

type detectedAddressVariant struct {
	Type   string `json:"@type"`
	B64    string `json:"b64"`
	B64URL string `json:"b64url"`
}

type detectedHash struct {
	Type   string `json:"@type"`
	B64    string `json:"b64"`
	B64URL string `json:"b64url"`
	Hex    string `json:"hex"`
}

func (s *Server) handleDetectAddress(_ context.Context, params requestParams) (any, *apiError) {
	addr, givenType, _, apiErr := parseStdAddressParam(params, "address")
	if apiErr != nil {
		return nil, apiErr
	}

	bounceable, err := addressVariant(addr.Bounce(true))
	if err != nil {
		return nil, validationError(err.Error())
	}
	nonBounceable, err := addressVariant(addr.Bounce(false))
	if err != nil {
		return nil, validationError(err.Error())
	}

	return detectedAddress{
		Type:          detectedAddressType,
		RawForm:       addr.StringRaw(),
		Bounceable:    bounceable,
		NonBounceable: nonBounceable,
		GivenType:     givenType,
		TestOnly:      addr.IsTestnetOnly(),
	}, nil
}

func (s *Server) handlePackAddress(_ context.Context, params requestParams) (any, *apiError) {
	addr, _, _, apiErr := parseAddressParam(params, "address")
	if apiErr != nil {
		return nil, apiErr
	}

	return addr.Bounce(true).String(), nil
}

func (s *Server) handleUnpackAddress(_ context.Context, params requestParams) (any, *apiError) {
	addr, _, _, apiErr := parseAddressParam(params, "address")
	if apiErr != nil {
		return nil, apiErr
	}

	return addr.StringRaw(), nil
}

func (s *Server) handleDetectHash(_ context.Context, params requestParams) (any, *apiError) {
	hash, apiErr := requiredHashParam(params, "hash")
	if apiErr != nil {
		return nil, apiErr
	}

	return detectedHash{
		Type:   detectedHashType,
		B64:    base64.StdEncoding.EncodeToString(hash),
		B64URL: base64.RawURLEncoding.EncodeToString(hash),
		Hex:    hex.EncodeToString(hash),
	}, nil
}

func parseAddressParam(params requestParams, name string) (*address.Address, string, string, *apiError) {
	raw, apiErr := addressParamString(params, name)
	if apiErr != nil {
		return nil, "", "", apiErr
	}

	addr, givenType, err := parseAddress(raw)
	if err != nil {
		return nil, "", "", addressParamError(params, name, raw)
	}
	return addr, givenType, raw, nil
}

func parseStdAddressParam(params requestParams, name string) (*address.Address, string, string, *apiError) {
	addr, givenType, raw, apiErr := parseAddressParam(params, name)
	if apiErr != nil {
		return nil, "", "", apiErr
	}
	if addr.Type() != address.StdAddress || addr.BitsLen() != 256 {
		return nil, "", "", addressParamError(params, name, raw)
	}
	return addr, givenType, raw, nil
}

func addressParamString(params requestParams, name string) (string, *apiError) {
	if params.isPost() {
		return params.requiredString(name)
	}

	raw, _, err := params.stringValue(name)
	if err != nil {
		return "", validationError("failed to parse get request: " + err.Error())
	}
	return raw, nil
}

func addressParamError(params requestParams, name string, raw string) *apiError {
	message := fmt.Sprintf("Failed to parse ton_addr: '%s'", raw)
	if params.isPost() {
		return params.postFieldError(name, message)
	}
	return validationError("failed to parse get request: " + message)
}

func parseAddress(raw string) (*address.Address, string, error) {
	raw = strings.TrimSpace(raw)
	if strings.Contains(raw, ":") {
		addr, err := address.ParseRawAddr(raw)
		return addr, "raw_form", err
	}

	normalized := strings.TrimRight(raw, "=")
	normalized = strings.ReplaceAll(normalized, "+", "-")
	normalized = strings.ReplaceAll(normalized, "/", "_")
	addr, err := address.ParseAddr(normalized)
	if err != nil {
		return nil, "", err
	}

	givenType := "friendly_bounceable"
	if !addr.IsBounceable() {
		givenType = "friendly_non_bounceable"
	}
	return addr, givenType, nil
}

func requiredHashParam(params requestParams, name string) ([]byte, *apiError) {
	raw, ok, apiErr := params.optionalNonEmptyString(name)
	if apiErr != nil {
		return nil, apiErr
	}
	if !ok {
		if params.isPost() {
			return nil, params.postFieldError(name, "Field is missing")
		}
		return nil, validationError("empty " + name)
	}

	hash, err := parseHash(raw)
	if err != nil {
		return nil, hashParamError(params, name, raw)
	}
	return hash, nil
}

func hashParamError(params requestParams, name string, raw string) *apiError {
	message := fmt.Sprintf("invalid hash: '%s'", raw)
	if params.isPost() {
		return params.postFieldError(name, message)
	}
	return validationError("failed to parse get request: " + message)
}

func addressVariant(addr *address.Address) (detectedAddressVariant, error) {
	data, err := base64.RawURLEncoding.DecodeString(addr.String())
	if err != nil {
		return detectedAddressVariant{}, fmt.Errorf("failed to encode address: %w", err)
	}

	return detectedAddressVariant{
		Type:   detectedAddressVariantType,
		B64:    base64.RawStdEncoding.EncodeToString(data),
		B64URL: base64.RawURLEncoding.EncodeToString(data),
	}, nil
}

func parseHash(raw string) ([]byte, error) {
	raw = strings.TrimSpace(raw)
	if len(raw) == 64 {
		hash, err := hex.DecodeString(raw)
		if err == nil && len(hash) == 32 {
			return hash, nil
		}
	}

	decoders := []*base64.Encoding{
		base64.StdEncoding,
		base64.RawStdEncoding,
		base64.URLEncoding,
		base64.RawURLEncoding,
	}
	for _, decoder := range decoders {
		hash, err := decoder.DecodeString(raw)
		if err == nil && len(hash) == 32 {
			return hash, nil
		}
	}

	trimmed := strings.TrimRight(raw, "=")
	for _, decoder := range []*base64.Encoding{base64.RawStdEncoding, base64.RawURLEncoding} {
		hash, err := decoder.DecodeString(trimmed)
		if err == nil && len(hash) == 32 {
			return hash, nil
		}
	}

	return nil, fmt.Errorf("invalid hash")
}
