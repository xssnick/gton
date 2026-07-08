package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
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
	add := func(name string, allowGet bool, allowPost bool, handler methodHandler, postFields ...string) {
		fields := make(map[string]struct{}, len(postFields))
		for _, field := range postFields {
			fields[field] = struct{}{}
		}
		routes[name] = route{
			name:       name,
			allowGet:   allowGet,
			allowPost:  allowPost,
			handler:    handler,
			postFields: fields,
		}
	}
	add("detectAddress", true, true, s.handleDetectAddress, "address")
	add("detectHash", true, true, s.handleDetectHash, "hash")
	add("packAddress", true, true, s.handlePackAddress, "address")
	add("unpackAddress", true, true, s.handleUnpackAddress, "address")
	add("getAddressInformation", true, true, s.handleAddressInformation, "address", "seqno")
	add("getExtendedAddressInformation", true, true, s.handleExtendedAddressInformation, "address", "seqno")
	add("getShardAccountCell", true, true, s.handleShardAccountCell, "address", "seqno")
	add("getWalletInformation", true, true, s.handleWalletInformation, "address", "seqno")
	add("getAddressBalance", true, true, s.handleAddressBalance, "address", "seqno")
	add("getAddressState", true, true, s.handleAddressState, "address", "seqno")
	add("getTokenData", true, true, s.handleTokenData, "address", "seqno")
	add("getMasterchainInfo", true, true, s.handleMasterchainInfo)
	add("getMasterchainBlockSignatures", true, true, s.handleMasterchainBlockSignatures, "seqno")
	add("getShardBlockProof", true, true, s.handleShardBlockProof, "workchain", "shard", "seqno", "from_seqno")
	add("getConsensusBlock", true, true, s.handleConsensusBlock)
	add("lookupBlock", true, true, s.handleLookupBlock, "workchain", "shard", "seqno", "lt", "unixtime")
	add("getShards", true, true, s.handleShards, "seqno")
	add("getBlockHeader", true, true, s.handleBlockHeader, "workchain", "shard", "seqno", "root_hash", "file_hash")
	add("getOutMsgQueueSize", true, true, s.handleOutMsgQueueSize)
	add("getBlockTransactions", true, true, s.handleBlockTransactions, "workchain", "shard", "seqno", "root_hash", "file_hash", "after_lt", "after_hash", "count")
	add("getBlockTransactionsExt", true, true, s.handleBlockTransactionsExt, "workchain", "shard", "seqno", "root_hash", "file_hash", "after_lt", "after_hash", "count")
	add("getTransactions", true, true, s.handleTransactions, "address", "lt", "hash", "to_lt", "archival", "limit")
	add("getTransactionsStd", true, true, s.handleTransactionsStd, "address", "lt", "hash", "to_lt", "archival", "limit")
	add("tryLocateTx", true, true, s.handleTryLocateTx, "source", "destination", "created_lt")
	add("tryLocateResultTx", true, true, s.handleTryLocateResultTx, "source", "destination", "created_lt")
	add("tryLocateSourceTx", true, true, s.handleTryLocateSourceTx, "source", "destination", "created_lt")
	add("getConfigParam", true, true, s.handleConfigParam, "config_id", "param", "seqno")
	add("getConfigAll", true, true, s.handleConfigAll, "seqno")
	add("getLibraries", true, true, s.handleLibraries, "libraries")
	add("runGetMethod", false, true, s.handleRunGetMethod, "address", "method", "stack", "seqno")
	add("runGetMethodStd", false, true, s.handleRunGetMethodStd, "address", "method", "stack", "seqno")
	add("sendBoc", false, true, s.handleSendBoc, "boc")
	add("sendBocReturnHash", false, true, s.handleSendBocReturnHash, "boc")
	add("estimateFee", false, true, s.handleEstimateFee, "address", "body", "init_code", "init_data", "ignore_chksig")
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

	data, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	if len(strings.TrimSpace(string(data))) == 0 {
		data = []byte("{}")
	}

	if err = json.Unmarshal(data, dst); err != nil {
		return err
	}
	return nil
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

func (p requestParams) stringValue(name string) (string, bool, error) {
	if p.query != nil {
		value, ok := p.query[name]
		if !ok || len(value) == 0 {
			return "", false, nil
		}
		return value[0], true, nil
	}

	raw, ok := p.body[name]
	if !ok {
		return "", false, nil
	}

	var value any
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return "", false, err
	}

	switch v := value.(type) {
	case string:
		return v, true, nil
	case json.Number:
		return v.String(), true, nil
	case bool:
		if v {
			return "true", true, nil
		}
		return "false", true, nil
	default:
		return "", false, fmt.Errorf("field %s must be a string", name)
	}
}

func (p requestParams) requiredString(name string) (string, *apiError) {
	value, ok, err := p.stringValue(name)
	if err != nil {
		if p.isPost() {
			return "", p.postFieldError(name, err.Error())
		}
		return "", validationError("failed to parse request: " + err.Error())
	}
	if !ok || strings.TrimSpace(value) == "" {
		return "", p.requiredFieldError(name)
	}
	return value, nil
}

func (p requestParams) optionalInt32(name string) (int32, bool, *apiError) {
	raw, ok, apiErr := p.optionalNonEmptyString(name)
	if apiErr != nil || !ok {
		return 0, ok, apiErr
	}

	value, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		return 0, false, validationError(fmt.Sprintf("failed to parse request: field %q must be int32", name))
	}
	return int32(value), true, nil
}

func (p requestParams) requiredInt32(name string) (int32, *apiError) {
	value, ok, apiErr := p.optionalInt32(name)
	if apiErr != nil {
		return 0, apiErr
	}
	if !ok {
		return 0, p.requiredFieldError(name)
	}
	return value, nil
}

func (p requestParams) optionalInt64(name string) (int64, bool, *apiError) {
	raw, ok, apiErr := p.optionalNonEmptyString(name)
	if apiErr != nil || !ok {
		return 0, ok, apiErr
	}

	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, false, validationError(fmt.Sprintf("failed to parse request: field %q must be int64", name))
	}
	return value, true, nil
}

func (p requestParams) requiredInt64(name string) (int64, *apiError) {
	value, ok, apiErr := p.optionalInt64(name)
	if apiErr != nil {
		return 0, apiErr
	}
	if !ok {
		return 0, p.requiredFieldError(name)
	}
	return value, nil
}

func (p requestParams) optionalUint32(name string) (uint32, bool, *apiError) {
	raw, ok, apiErr := p.optionalNonEmptyString(name)
	if apiErr != nil || !ok {
		return 0, ok, apiErr
	}

	value, err := strconv.ParseUint(raw, 10, 32)
	if err != nil {
		return 0, false, validationError(fmt.Sprintf("failed to parse request: field %q must be uint32", name))
	}
	return uint32(value), true, nil
}

func (p requestParams) requiredUint32(name string) (uint32, *apiError) {
	value, ok, apiErr := p.optionalUint32(name)
	if apiErr != nil {
		return 0, apiErr
	}
	if !ok {
		return 0, p.requiredFieldError(name)
	}
	return value, nil
}

func (p requestParams) optionalUint64(name string) (uint64, bool, *apiError) {
	raw, ok, apiErr := p.optionalNonEmptyString(name)
	if apiErr != nil || !ok {
		return 0, ok, apiErr
	}

	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, false, validationError(fmt.Sprintf("failed to parse request: field %q must be uint64", name))
	}
	return value, true, nil
}

func (p requestParams) optionalBool(name string) (bool, bool, *apiError) {
	raw, ok, apiErr := p.optionalNonEmptyString(name)
	if apiErr != nil || !ok {
		return false, ok, apiErr
	}

	value, err := strconv.ParseBool(raw)
	if err != nil {
		return false, false, validationError(fmt.Sprintf("failed to parse request: field %q must be bool", name))
	}
	return value, true, nil
}

func (p requestParams) optionalNonEmptyString(name string) (string, bool, *apiError) {
	raw, ok, err := p.stringValue(name)
	if err != nil {
		if p.isPost() {
			return "", false, p.postFieldError(name, err.Error())
		}
		return "", false, validationError("failed to parse request: " + err.Error())
	}
	if !ok || strings.TrimSpace(raw) == "" {
		return "", false, nil
	}
	return raw, true, nil
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

	s.writeFailure(w, status, failureEnvelope{
		OK:     false,
		Error:  err.message,
		Result: err.result,
		Code:   code,
		Extra:  s.extra(started),
	})
}

func (s *Server) writeFailure(w http.ResponseWriter, status int, envelope failureEnvelope) {
	s.writeJSON(w, status, envelope)
}

func (s *Server) writeJSON(w http.ResponseWriter, status int, value any) {
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
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
