package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	gojson "github.com/goccy/go-json"
)

const apiPrefix = "/api/v2/"

type route struct {
	name       string
	allowGet   bool
	allowPost  bool
	handler    methodHandler
	postFields map[string]struct{}
}

type methodHandler func(context.Context, requestParams) (any, *apiError)

type requestSource string

const (
	requestSourceGet  requestSource = "get"
	requestSourcePost requestSource = "post"
)

type requestParams struct {
	query  url.Values
	body   map[string]json.RawMessage
	source requestSource
}

type apiError struct {
	status  int
	code    int
	message string
	result  any
}

func (e *apiError) Error() string {
	return e.message
}

var errRequestParamNotFound = errors.New("request parameter not found")

type successEnvelope struct {
	OK     bool   `json:"ok"`
	Result any    `json:"result"`
	Extra  string `json:"@extra"`
}

type failureEnvelope struct {
	OK     bool   `json:"ok"`
	Error  string `json:"error,omitempty"`
	Result any    `json:"result,omitempty"`
	Code   int    `json:"code"`
	Extra  string `json:"@extra,omitempty"`
}

type jsonRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

func (s *Server) buildRoutes() map[string]route {
	routes := map[string]route{}
	add := func(name string, allowGet bool, handler methodHandler, postFields ...string) {
		fields := make(map[string]struct{}, len(postFields))
		for _, field := range postFields {
			fields[field] = struct{}{}
		}
		routes[name] = route{
			name:       name,
			allowGet:   allowGet,
			allowPost:  true,
			handler:    handler,
			postFields: fields,
		}
	}
	add("detectAddress", true, s.handleDetectAddress, "address")
	add("detectHash", true, s.handleDetectHash, "hash")
	add("packAddress", true, s.handlePackAddress, "address")
	add("unpackAddress", true, s.handleUnpackAddress, "address")
	add("getAddressInformation", true, s.handleAddressInformation, "address", "seqno")
	add("getExtendedAddressInformation", true, s.handleExtendedAddressInformation, "address", "seqno")
	add("getShardAccountCell", true, s.handleShardAccountCell, "address", "seqno")
	add("getWalletInformation", true, s.handleWalletInformation, "address", "seqno")
	add("getAddressBalance", true, s.handleAddressBalance, "address", "seqno")
	add("getAddressState", true, s.handleAddressState, "address", "seqno")
	add("getTokenData", true, s.handleTokenData, "address", "seqno")
	add("getMasterchainInfo", true, s.handleMasterchainInfo)
	add("getMasterchainBlockSignatures", true, s.handleMasterchainBlockSignatures, "seqno")
	add("getShardBlockProof", true, s.handleShardBlockProof, "workchain", "shard", "seqno", "from_seqno")
	add("getConsensusBlock", true, s.handleConsensusBlock)
	add("lookupBlock", true, s.handleLookupBlock, "workchain", "shard", "seqno", "lt", "unixtime")
	add("getShards", true, s.handleShards, "seqno")
	add("getBlockHeader", true, s.handleBlockHeader, "workchain", "shard", "seqno", "root_hash", "file_hash")
	add("getOutMsgQueueSize", true, s.handleOutMsgQueueSize)
	add("getBlockTransactions", true, s.handleBlockTransactions, "workchain", "shard", "seqno", "root_hash", "file_hash", "after_lt", "after_hash", "count")
	add("getBlockTransactionsExt", true, s.handleBlockTransactionsExt, "workchain", "shard", "seqno", "root_hash", "file_hash", "after_lt", "after_hash", "count")
	add("getTransactions", true, s.handleTransactions, "address", "lt", "hash", "to_lt", "archival", "limit")
	add("getTransactionsStd", true, s.handleTransactionsStd, "address", "lt", "hash", "to_lt", "archival", "limit")
	add("tryLocateTx", true, s.handleTryLocateInboundMessageTransaction, "source", "destination", "created_lt")
	add("tryLocateResultTx", true, s.handleTryLocateInboundMessageTransaction, "source", "destination", "created_lt")
	add("tryLocateSourceTx", true, s.handleTryLocateSourceTx, "source", "destination", "created_lt")
	add("getConfigParam", true, s.handleConfigParam, "config_id", "param", "seqno")
	add("getConfigAll", true, s.handleConfigAll, "seqno")
	add("getLibraries", true, s.handleLibraries, "libraries")
	add("runGetMethod", false, s.handleRunGetMethod, "address", "method", "stack", "seqno")
	add("runGetMethodStd", false, s.handleRunGetMethodStd, "address", "method", "stack", "seqno")
	add("sendBoc", false, s.handleSendBoc, "boc")
	add("sendBocReturnHash", false, s.handleSendBocReturnHash, "boc")
	add("estimateFee", false, s.handleEstimateFee, "address", "body", "init_code", "init_data", "ignore_chksig")
	return routes
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	setCommonHeaders(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	started := s.now()
	if r.URL.Path == apiPrefix+"jsonRPC" {
		s.serveJSONRPC(w, r, started)
		return
	}

	name, ok := strings.CutPrefix(r.URL.Path, apiPrefix)
	if !ok || name == "" || strings.Contains(name, "/") {
		http.NotFound(w, r)
		return
	}

	rt, ok := s.routes[name]
	if !ok {
		http.NotFound(w, r)
		return
	}
	if !rt.allowed(r.Method) {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	params, apiErr := s.decodeParams(r, rt)
	if apiErr != nil {
		s.writeAPIError(w, apiErr, started)
		return
	}

	ctx := r.Context()
	if s.requestTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.requestTimeout)
		defer cancel()
	}

	result, apiErr := rt.handler(ctx, params)
	if apiErr != nil {
		s.writeAPIError(w, apiErr, started)
		return
	}
	s.writeSuccess(w, result, started)
}

func (r route) allowed(method string) bool {
	switch method {
	case http.MethodGet:
		return r.allowGet
	case http.MethodPost:
		return r.allowPost
	default:
		return false
	}
}

func (s *Server) serveJSONRPC(w http.ResponseWriter, r *http.Request, started time.Time) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var req jsonRPCRequest
	if err := decodeJSONBody(r, maxBodyBytes, &req); err != nil {
		s.writeAPIError(w, validationError("failed to parse jsonrpc request: "+err.Error()), started)
		return
	}

	rt, ok := s.routes[req.Method]
	if !ok || !rt.allowPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	params, apiErr := jsonRPCParams(req.Params)
	if apiErr != nil {
		s.writeAPIError(w, apiErr, started)
		return
	}
	if apiErr = validatePostFields(rt, params.body); apiErr != nil {
		s.writeAPIError(w, apiErr, started)
		return
	}

	ctx := r.Context()
	if s.requestTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.requestTimeout)
		defer cancel()
	}

	result, apiErr := rt.handler(ctx, params)
	if apiErr != nil {
		s.writeAPIError(w, apiErr, started)
		return
	}
	s.writeSuccess(w, result, started)
}

func (s *Server) decodeParams(r *http.Request, rt route) (requestParams, *apiError) {
	if r.Method == http.MethodGet {
		return requestParams{query: r.URL.Query(), source: requestSourceGet}, nil
	}

	body := map[string]json.RawMessage{}
	if r.Body != nil {
		if err := decodeJSONBody(r, maxBodyBytes, &body); err != nil {
			return requestParams{}, validationError("failed to parse post request: " + err.Error())
		}
	}
	if apiErr := validatePostFields(rt, body); apiErr != nil {
		return requestParams{}, apiErr
	}
	return requestParams{body: body, source: requestSourcePost}, nil
}

func validatePostFields(rt route, body map[string]json.RawMessage) *apiError {
	if body == nil {
		return nil
	}

	var unknown []string
	for name := range body {
		if _, ok := rt.postFields[name]; !ok {
			unknown = append(unknown, name)
		}
	}
	if len(unknown) == 0 {
		return nil
	}
	sort.Strings(unknown)
	return validationError(fmt.Sprintf("failed to parse post request: Unknown property '%s'", unknown[0]))
}

func decodeJSONBody(r *http.Request, maxBodyBytes int64, dst any) error {
	body := r.Body
	if maxBodyBytes > 0 {
		body = http.MaxBytesReader(nil, r.Body, maxBodyBytes)
	}
	defer body.Close()

	initialCapacity := r.ContentLength
	if initialCapacity < 0 {
		initialCapacity = 0
	}
	if maxBodyBytes > 0 && initialCapacity > maxBodyBytes {
		initialCapacity = maxBodyBytes
	}
	const maxInitialCapacity = int64(64 << 10)
	if initialCapacity > maxInitialCapacity {
		initialCapacity = maxInitialCapacity
	}

	var buffer bytes.Buffer
	buffer.Grow(int(initialCapacity) + bytes.MinRead)
	_, err := buffer.ReadFrom(body)
	if err != nil {
		return err
	}
	data := bytes.TrimSpace(buffer.Bytes())
	if len(data) == 0 {
		data = []byte("{}")
	}

	return json.Unmarshal(data, dst)
}

func jsonRPCParams(raw json.RawMessage) (requestParams, *apiError) {
	if len(raw) == 0 || string(raw) == "null" {
		return requestParams{body: map[string]json.RawMessage{}, source: requestSourcePost}, nil
	}

	body := map[string]json.RawMessage{}
	if err := json.Unmarshal(raw, &body); err != nil {
		return requestParams{}, validationError("failed to parse jsonrpc params: " + err.Error())
	}
	return requestParams{body: body, source: requestSourcePost}, nil
}

func (p requestParams) isPost() bool {
	if p.source != "" {
		return p.source == requestSourcePost
	}
	return p.query == nil
}

func (p requestParams) postFieldError(name string, message string) *apiError {
	return validationError(fmt.Sprintf("failed to parse post request: Error at path '%s': %s", name, message))
}

func (p requestParams) requiredFieldError(name string) *apiError {
	if p.isPost() {
		return p.postFieldError(name, "Field is missing")
	}
	return validationError(fmt.Sprintf("failed to parse request: missing required field %q", name))
}

func (p requestParams) stringValue(name string) (string, error) {
	if p.query != nil {
		value, ok := p.query[name]
		if !ok || len(value) == 0 {
			return "", errRequestParamNotFound
		}
		return value[0], nil
	}

	raw, ok := p.body[name]
	if !ok {
		return "", errRequestParamNotFound
	}

	var value any
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return "", err
	}

	switch v := value.(type) {
	case string:
		return v, nil
	case json.Number:
		return v.String(), nil
	case bool:
		if v {
			return "true", nil
		}
		return "false", nil
	default:
		return "", fmt.Errorf("field %s must be a string", name)
	}
}

func (p requestParams) requiredString(name string) (string, *apiError) {
	value, err := p.stringValue(name)
	if err != nil {
		if errors.Is(err, errRequestParamNotFound) {
			return "", p.requiredFieldError(name)
		}
		if p.isPost() {
			return "", p.postFieldError(name, err.Error())
		}
		return "", validationError("failed to parse request: " + err.Error())
	}
	if strings.TrimSpace(value) == "" {
		return "", p.requiredFieldError(name)
	}
	return value, nil
}

func (p requestParams) optionalInt32(name string) (int32, error) {
	raw, err := p.optionalNonEmptyString(name)
	if err != nil {
		return 0, err
	}

	value, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		return 0, validationError(fmt.Sprintf("failed to parse request: field %q must be int32", name))
	}
	return int32(value), nil
}

func (p requestParams) requiredInt32(name string) (int32, *apiError) {
	value, err := p.optionalInt32(name)
	if errors.Is(err, errRequestParamNotFound) {
		return 0, p.requiredFieldError(name)
	}
	if err != nil {
		return 0, asAPIError(err)
	}
	return value, nil
}

func (p requestParams) optionalInt64(name string) (int64, error) {
	raw, err := p.optionalNonEmptyString(name)
	if err != nil {
		return 0, err
	}

	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, validationError(fmt.Sprintf("failed to parse request: field %q must be int64", name))
	}
	return value, nil
}

func (p requestParams) requiredInt64(name string) (int64, *apiError) {
	value, err := p.optionalInt64(name)
	if errors.Is(err, errRequestParamNotFound) {
		return 0, p.requiredFieldError(name)
	}
	if err != nil {
		return 0, asAPIError(err)
	}
	return value, nil
}

func (p requestParams) optionalUint32(name string) (uint32, error) {
	raw, err := p.optionalNonEmptyString(name)
	if err != nil {
		return 0, err
	}

	value, err := strconv.ParseUint(raw, 10, 32)
	if err != nil {
		return 0, validationError(fmt.Sprintf("failed to parse request: field %q must be uint32", name))
	}
	return uint32(value), nil
}

func (p requestParams) requiredUint32(name string) (uint32, *apiError) {
	value, err := p.optionalUint32(name)
	if errors.Is(err, errRequestParamNotFound) {
		return 0, p.requiredFieldError(name)
	}
	if err != nil {
		return 0, asAPIError(err)
	}
	return value, nil
}

func (p requestParams) optionalUint64(name string) (uint64, error) {
	raw, err := p.optionalNonEmptyString(name)
	if err != nil {
		return 0, err
	}

	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, validationError(fmt.Sprintf("failed to parse request: field %q must be uint64", name))
	}
	return value, nil
}

func (p requestParams) optionalBool(name string) (bool, error) {
	raw, err := p.optionalNonEmptyString(name)
	if err != nil {
		return false, err
	}

	value, err := strconv.ParseBool(raw)
	if err != nil {
		return false, validationError(fmt.Sprintf("failed to parse request: field %q must be bool", name))
	}
	return value, nil
}

func (p requestParams) optionalNonEmptyString(name string) (string, error) {
	raw, err := p.stringValue(name)
	if err != nil {
		if errors.Is(err, errRequestParamNotFound) {
			return "", err
		}
		if p.isPost() {
			return "", p.postFieldError(name, err.Error())
		}
		return "", validationError("failed to parse request: " + err.Error())
	}
	if strings.TrimSpace(raw) == "" {
		return "", errRequestParamNotFound
	}
	return raw, nil
}

func asAPIError(err error) *apiError {
	if err == nil {
		return nil
	}

	var apiErr *apiError
	if errors.As(err, &apiErr) {
		return apiErr
	}
	return internalError(err.Error())
}

func setCommonHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-API-Version", DefaultAPIVersion)
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "User-Agent,Keep-Alive,Content-Type,X-API-Key,X-Ton-Client-Version,X-API-Version")
}

func (s *Server) writeSuccess(w http.ResponseWriter, result any, started time.Time) {
	s.writeJSON(w, http.StatusOK, successEnvelope{
		OK:     true,
		Result: result,
		Extra:  s.extra(started),
	})
}

func (s *Server) writeAPIError(w http.ResponseWriter, err *apiError, started time.Time) {
	status := err.status
	if status == 0 {
		status = http.StatusInternalServerError
	}
	code := err.code
	if code == 0 {
		code = status
	}

	s.writeJSON(w, status, failureEnvelope{
		OK:     false,
		Error:  err.message,
		Result: err.result,
		Code:   code,
		Extra:  s.extra(started),
	})
}

func (s *Server) writeJSON(w http.ResponseWriter, status int, value any) {
	w.WriteHeader(status)
	if err := gojson.NewEncoder(w).Encode(value); err != nil {
		s.log.Warn().Err(err).Msg("failed to write http api response")
	}
}

func (s *Server) extra(started time.Time) string {
	return fmt.Sprintf("%d:_:%.3f", s.now().Unix(), s.now().Sub(started).Seconds())
}

func validationError(message string) *apiError {
	return &apiError{
		status:  http.StatusUnprocessableEntity,
		code:    http.StatusUnprocessableEntity,
		message: message,
	}
}
