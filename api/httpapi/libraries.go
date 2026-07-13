package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	libraryResultType = "smc.libraryResult"
	libraryEntryType  = "smc.libraryEntry"
)

type libraryResult struct {
	Type   string         `json:"@type"`
	Result []libraryEntry `json:"result"`
}

type libraryEntry struct {
	Type string `json:"@type"`
	Hash string `json:"hash"`
	Data string `json:"data"`
}

func (s *Server) handleLibraries(ctx context.Context, params requestParams) (any, *apiError) {
	hashes, apiErr := libraryHashes(params)
	if apiErr != nil {
		return nil, apiErr
	}
	if len(hashes) == 0 {
		return libraryResult{Type: libraryResultType, Result: []libraryEntry{}}, nil
	}

	block, _, _, err := s.store.CurrentMasterchainInfo(ctx)
	if err != nil {
		return nil, internalError("cannot obtain current masterchain state: " + err.Error())
	}
	fragments, err := s.store.BlockFragments(ctx, block)
	if err != nil {
		return nil, internalError("cannot load masterchain state: " + err.Error())
	}
	libraries, err := fragments.GlobalLibraries()
	if err != nil {
		return nil, internalError("cannot load libraries: " + err.Error())
	}

	entries, err := libraryEntries(libraries, hashes)
	if err != nil {
		return nil, internalError("cannot load libraries: " + err.Error())
	}
	return libraryResult{Type: libraryResultType, Result: entries}, nil
}

func libraryHashes(params requestParams) ([][]byte, *apiError) {
	rawValues, apiErr := libraryHashStrings(params)
	if apiErr != nil {
		return nil, apiErr
	}

	hashes := make([][]byte, 0, len(rawValues))
	for i, raw := range rawValues {
		if strings.TrimSpace(raw) == "" {
			continue
		}
		hash, err := parseHash(raw)
		if err != nil {
			if params.isPost() {
				return nil, params.postFieldError(fmt.Sprintf("libraries[%d]", i), fmt.Sprintf("invalid hash: '%s'", raw))
			}
			return nil, validationError("failed to parse get request: failed to parse libraries")
		}
		hashes = append(hashes, hash)
	}
	return hashes, nil
}

func libraryHashStrings(params requestParams) ([]string, *apiError) {
	if params.query != nil {
		values := append([]string{}, params.query["libraries"]...)
		values = append(values, params.query["library_list"]...)

		out := make([]string, 0, len(values))
		for _, value := range values {
			for _, part := range strings.Split(value, ",") {
				out = append(out, strings.TrimSpace(part))
			}
		}
		return out, nil
	}

	raw, ok := params.body["libraries"]
	if !ok {
		return nil, nil
	}

	var list []string
	if err := json.Unmarshal(raw, &list); err != nil {
		return nil, params.postFieldError("libraries", "Wrong type. Expected: kArrayValue, actual: "+jsonValueType(raw))
	}
	return list, nil
}

func jsonValueType(raw json.RawMessage) string {
	var value any
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return "kNullValue"
	}

	switch value.(type) {
	case nil:
		return "kNullValue"
	case string:
		return "kStringValue"
	case json.Number:
		return "kNumberValue"
	case bool:
		return "kBoolValue"
	case []any:
		return "kArrayValue"
	case map[string]any:
		return "kObjectValue"
	default:
		return "kNullValue"
	}
}

func libraryEntries(libraries *cell.Dictionary, hashes [][]byte) ([]libraryEntry, error) {
	if libraries == nil {
		return []libraryEntry{}, nil
	}

	out := make([]libraryEntry, 0, len(hashes))
	for _, hash := range hashes {
		value, err := libraries.LoadValue(cell.BeginCell().MustStoreSlice(hash, 256).EndCell())
		if errors.Is(err, cell.ErrNoSuchKeyInDict) {
			continue
		}
		if err != nil {
			return nil, err
		}

		tag, err := value.LoadUInt(2)
		if err != nil {
			return nil, err
		}
		if tag != 0 {
			return nil, fmt.Errorf("invalid shared library descriptor")
		}

		lib, err := value.LoadRefCell()
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(lib.Hash(), hash) {
			continue
		}

		out = append(out, libraryEntry{
			Type: libraryEntryType,
			Hash: tonHash(hash),
			Data: bocBase64(lib),
		})
	}
	return out, nil
}
