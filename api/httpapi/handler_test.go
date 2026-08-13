package httpapi

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/xssnick/gton/service/externalmsg"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

var testHTTPAPINow = time.Unix(1_783_269_917, 0)

func TestUtilityEndpoints(t *testing.T) {
	server := newTestServer()

	tests := []struct {
		name         string
		path         string
		expectedCode int
		check        func(t *testing.T, body responseBody)
	}{
		{
			name:         "detect address",
			path:         "/api/v2/detectAddress?address=EQCxE6mUtQJKFnGfaROTKOt1lZbDiiX1kCixRv7Nw2Id_sDs",
			expectedCode: http.StatusOK,
			check: func(t *testing.T, body responseBody) {
				result := resultMap(t, body)
				if result["@type"] != detectedAddressType {
					t.Fatalf("unexpected @type %v", result["@type"])
				}
				if result["raw_form"] != "0:b113a994b5024a16719f69139328eb759596c38a25f59028b146fecdc3621dfe" {
					t.Fatalf("unexpected raw_form %v", result["raw_form"])
				}
				if result["given_type"] != "friendly_bounceable" {
					t.Fatalf("unexpected given_type %v", result["given_type"])
				}

				bounceable := nestedMap(t, result, "bounceable")
				if bounceable["b64"] != "EQCxE6mUtQJKFnGfaROTKOt1lZbDiiX1kCixRv7Nw2Id/sDs" {
					t.Fatalf("unexpected bounceable b64 %v", bounceable["b64"])
				}
				if bounceable["b64url"] != "EQCxE6mUtQJKFnGfaROTKOt1lZbDiiX1kCixRv7Nw2Id_sDs" {
					t.Fatalf("unexpected bounceable b64url %v", bounceable["b64url"])
				}

				nonBounceable := nestedMap(t, result, "non_bounceable")
				if nonBounceable["b64url"] != "UQCxE6mUtQJKFnGfaROTKOt1lZbDiiX1kCixRv7Nw2Id_p0p" {
					t.Fatalf("unexpected non-bounceable b64url %v", nonBounceable["b64url"])
				}
			},
		},
		{
			name:         "pack address",
			path:         "/api/v2/packAddress?address=0:b113a994b5024a16719f69139328eb759596c38a25f59028b146fecdc3621dfe",
			expectedCode: http.StatusOK,
			check: func(t *testing.T, body responseBody) {
				if stringResult(t, body) != "EQCxE6mUtQJKFnGfaROTKOt1lZbDiiX1kCixRv7Nw2Id_sDs" {
					t.Fatalf("unexpected result %s", stringResult(t, body))
				}
			},
		},
		{
			name:         "unpack address",
			path:         "/api/v2/unpackAddress?address=EQCxE6mUtQJKFnGfaROTKOt1lZbDiiX1kCixRv7Nw2Id_sDs",
			expectedCode: http.StatusOK,
			check: func(t *testing.T, body responseBody) {
				if stringResult(t, body) != "0:b113a994b5024a16719f69139328eb759596c38a25f59028b146fecdc3621dfe" {
					t.Fatalf("unexpected result %s", stringResult(t, body))
				}
			},
		},
		{
			name:         "detect hash",
			path:         "/api/v2/detectHash?hash=b113a994b5024a16719f69139328eb759596c38a25f59028b146fecdc3621dfe",
			expectedCode: http.StatusOK,
			check: func(t *testing.T, body responseBody) {
				result := resultMap(t, body)
				if result["@type"] != detectedHashType {
					t.Fatalf("unexpected @type %v", result["@type"])
				}
				if result["b64"] != "sROplLUCShZxn2kTkyjrdZWWw4ol9ZAosUb+zcNiHf4=" {
					t.Fatalf("unexpected b64 %v", result["b64"])
				}
				if result["b64url"] != "sROplLUCShZxn2kTkyjrdZWWw4ol9ZAosUb-zcNiHf4" {
					t.Fatalf("unexpected b64url %v", result["b64url"])
				}
				if result["hex"] != "b113a994b5024a16719f69139328eb759596c38a25f59028b146fecdc3621dfe" {
					t.Fatalf("unexpected hex %v", result["hex"])
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)

			server.ServeHTTP(rec, req)

			if rec.Code != tt.expectedCode {
				t.Fatalf("status = %d, want %d, body=%s", rec.Code, tt.expectedCode, rec.Body.String())
			}
			if got := rec.Header().Get("X-API-Version"); got != DefaultAPIVersion {
				t.Fatalf("X-API-Version = %q, want %q", got, DefaultAPIVersion)
			}

			body := decodeResponse(t, rec.Body.Bytes())
			if !body.OK {
				t.Fatalf("ok = false, body=%s", rec.Body.String())
			}
			if body.Extra != "1783269917:_:0.000" {
				t.Fatalf("unexpected @extra %q", body.Extra)
			}
			tt.check(t, body)
		})
	}
}

func TestUtilityEndpointValidationError(t *testing.T) {
	server := newTestServer()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v2/detectAddress?address=bad", nil)

	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnprocessableEntity)
	}

	body := decodeResponse(t, rec.Body.Bytes())
	if body.OK {
		t.Fatal("ok = true, want false")
	}
	if body.Code != http.StatusUnprocessableEntity {
		t.Fatalf("code = %d, want %d", body.Code, http.StatusUnprocessableEntity)
	}
	if body.Error != "failed to parse get request: Failed to parse ton_addr: 'bad'" {
		t.Fatalf("unexpected error %q", body.Error)
	}
}

func TestUtilityEndpointValidationErrorMatchesTonHTTPAPI(t *testing.T) {
	server := newTestServer()

	tests := []struct {
		name   string
		method string
		path   string
		body   string
		error  string
	}{
		{
			name:   "detect hash get missing",
			method: http.MethodGet,
			path:   "/api/v2/detectHash",
			error:  "empty hash",
		},
		{
			name:   "detect hash get invalid",
			method: http.MethodGet,
			path:   "/api/v2/detectHash?hash=bad",
			error:  "failed to parse get request: invalid hash: 'bad'",
		},
		{
			name:   "detect hash post missing",
			method: http.MethodPost,
			path:   "/api/v2/detectHash",
			body:   `{}`,
			error:  "failed to parse post request: Error at path 'hash': Field is missing",
		},
		{
			name:   "detect hash post invalid",
			method: http.MethodPost,
			path:   "/api/v2/detectHash",
			body:   `{"hash":"bad"}`,
			error:  "failed to parse post request: Error at path 'hash': invalid hash: 'bad'",
		},
		{
			name:   "detect address post invalid",
			method: http.MethodPost,
			path:   "/api/v2/detectAddress",
			body:   `{"address":"bad"}`,
			error:  "failed to parse post request: Error at path 'address': Failed to parse ton_addr: 'bad'",
		},
		{
			name:   "get libraries get invalid",
			method: http.MethodGet,
			path:   "/api/v2/getLibraries?libraries=bad",
			error:  "failed to parse get request: failed to parse libraries",
		},
		{
			name:   "get libraries post invalid item",
			method: http.MethodPost,
			path:   "/api/v2/getLibraries",
			body:   `{"libraries":["bad"]}`,
			error:  "failed to parse post request: Error at path 'libraries[0]': invalid hash: 'bad'",
		},
		{
			name:   "get libraries post string",
			method: http.MethodPost,
			path:   "/api/v2/getLibraries",
			body:   `{"libraries":"bad"}`,
			error:  "failed to parse post request: Error at path 'libraries': Wrong type. Expected: kArrayValue, actual: kStringValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(tt.method, tt.path, bytes.NewBufferString(tt.body))

			server.ServeHTTP(rec, req)

			if rec.Code != http.StatusUnprocessableEntity {
				t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusUnprocessableEntity, rec.Body.String())
			}
			resp := decodeResponse(t, rec.Body.Bytes())
			if resp.Error != tt.error {
				t.Fatalf("unexpected error %q", resp.Error)
			}
		})
	}
}

func TestPostRejectsUnknownProperty(t *testing.T) {
	server := newTestServer()
	rec := httptest.NewRecorder()
	body := bytes.NewBufferString(`{"hash":"sROplLUCShZxn2kTkyjrdZWWw4ol9ZAosUb-zcNiHf4","extra_field":1}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v2/detectHash", body)

	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnprocessableEntity)
	}
	resp := decodeResponse(t, rec.Body.Bytes())
	if resp.Error != "failed to parse post request: Unknown property 'extra_field'" {
		t.Fatalf("unexpected error %q", resp.Error)
	}
}

func TestJSONRPCDispatchesToEndpoint(t *testing.T) {
	server := newTestServer()
	rec := httptest.NewRecorder()
	body := bytes.NewBufferString(`{"jsonrpc":"2.0","id":"abc","method":"detectHash","params":{"hash":"sROplLUCShZxn2kTkyjrdZWWw4ol9ZAosUb-zcNiHf4"}}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v2/jsonRPC", body)

	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	result := resultMap(t, decodeResponse(t, rec.Body.Bytes()))
	if result["hex"] != "b113a994b5024a16719f69139328eb759596c38a25f59028b146fecdc3621dfe" {
		t.Fatalf("unexpected hex %v", result["hex"])
	}
}

func TestJSONRPCRejectsUnknownParamProperty(t *testing.T) {
	server := newTestServer()
	rec := httptest.NewRecorder()
	body := bytes.NewBufferString(`{"jsonrpc":"2.0","id":"abc","method":"detectHash","params":{"hash":"sROplLUCShZxn2kTkyjrdZWWw4ol9ZAosUb-zcNiHf4","extra_field":1}}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v2/jsonRPC", body)

	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnprocessableEntity)
	}
	resp := decodeResponse(t, rec.Body.Bytes())
	if resp.Error != "failed to parse post request: Unknown property 'extra_field'" {
		t.Fatalf("unexpected error %q", resp.Error)
	}
}

func TestConfigParamAcceptsParamAlias(t *testing.T) {
	got, apiErr := configParamID(requestParams{query: mapValues(map[string]string{"param": "34"})})
	if apiErr != nil {
		t.Fatalf("config param error: %v", apiErr.message)
	}
	if got != 34 {
		t.Fatalf("config param id = %d, want 34", got)
	}
}

func TestMasterchainInfoUsesStoreAndZeroState(t *testing.T) {
	server := newTestServer()
	server.store = testStore{
		block: ton.BlockIDExt{
			Workchain: -1,
			Shard:     -1 << 63,
			SeqNo:     77734654,
			RootHash:  bytes.Repeat([]byte{0x21}, 32),
			FileHash:  bytes.Repeat([]byte{0x42}, 32),
		},
		stateRootHash: bytes.Repeat([]byte{0x77}, 32),
	}
	server.zeroState = ton.ZeroStateIDExt{
		Workchain: -1,
		RootHash:  bytes.Repeat([]byte{0x11}, 32),
		FileHash:  bytes.Repeat([]byte{0x22}, 32),
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v2/getMasterchainInfo", nil)

	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	result := resultMap(t, decodeResponse(t, rec.Body.Bytes()))
	if result["@type"] != masterchainInfoType {
		t.Fatalf("unexpected @type %v", result["@type"])
	}

	last := nestedMap(t, result, "last")
	if last["shard"] != "-9223372036854775808" {
		t.Fatalf("unexpected last shard %v", last["shard"])
	}
	if last["root_hash"] != base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x21}, 32)) {
		t.Fatalf("unexpected last root hash %v", last["root_hash"])
	}

	init := nestedMap(t, result, "init")
	if init["shard"] != "0" {
		t.Fatalf("unexpected init shard %v", init["shard"])
	}
	if init["root_hash"] != base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x11}, 32)) {
		t.Fatalf("unexpected init root hash %v", init["root_hash"])
	}
}

func TestSendBocReturnHashChecksAndSendsExternalMessage(t *testing.T) {
	root := cell.BeginCell().MustStoreUInt(7, 8).EndCell()
	addr := address.MustParseRawAddr("0:b113a994b5024a16719f69139328eb759596c38a25f59028b146fecdc3621dfe")
	network := &testNetwork{
		root: root,
		msg:  &tlb.ExternalMessage{DstAddr: addr},
	}
	server := newTestServer()
	var err error
	server.messageSender, err = externalmsg.NewSender(externalmsg.SenderOptions{
		Check:     network.CheckExternalMessage,
		Broadcast: network.SendCheckedExternalMessage,
		Now:       server.now,
	})
	if err != nil {
		t.Fatalf("new sender: %v", err)
	}

	msgRoot, err := tlb.ToCell(&tlb.ExternalMessage{
		DstAddr:   addr,
		ImportFee: tlb.ZeroCoins,
		Body:      cell.BeginCell().EndCell(),
	})
	if err != nil {
		t.Fatalf("build external message: %v", err)
	}
	msgBOC := msgRoot.ToBOC()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(
		http.MethodPost,
		"/api/v2/sendBocReturnHash",
		bytes.NewBufferString(`{"boc":"`+base64.StdEncoding.EncodeToString(msgBOC)+`"}`),
	)

	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if !bytes.Equal(network.checkedBody, msgBOC) {
		t.Fatalf("checked body = %x, want %x", network.checkedBody, msgBOC)
	}
	if !bytes.Equal(network.sentBody, msgBOC) {
		t.Fatalf("sent body = %x, want %x", network.sentBody, msgBOC)
	}

	result := resultMap(t, decodeResponse(t, rec.Body.Bytes()))
	hash := root.Hash()
	if result["@type"] != extMessageInfoType {
		t.Fatalf("unexpected @type %v", result["@type"])
	}
	if result["hash"] != tonHash(hash[:]) {
		t.Fatalf("unexpected hash %v", result["hash"])
	}
	if result["hash_norm"] != tonHash(hash[:]) {
		t.Fatalf("unexpected hash_norm %v", result["hash_norm"])
	}
}

type testStore struct {
	Store
	block         ton.BlockIDExt
	stateRootHash []byte
}

func (s testStore) CurrentMasterchainInfo(context.Context) (ton.BlockIDExt, []byte, uint32, error) {
	return s.block, s.stateRootHash, 0, nil
}

type testNetwork struct {
	root        *cell.Cell
	msg         *tlb.ExternalMessage
	checkedBody []byte
	sentBody    []byte
}

func (n *testNetwork) SendExternalMessage(_ context.Context, body []byte, _ *address.Address) error {
	n.sentBody = append([]byte(nil), body...)
	return nil
}

func (n *testNetwork) SendCheckedExternalMessage(_ context.Context, body []byte, _ *address.Address, _ *cell.Cell, _ *tlb.ExternalMessage) error {
	n.sentBody = append([]byte(nil), body...)
	return nil
}

func (n *testNetwork) CheckExternalMessage(_ context.Context, body []byte, _ *cell.Cell, _ *tlb.ExternalMessage) (externalmsg.CheckResult, error) {
	n.checkedBody = append([]byte(nil), body...)
	return externalmsg.CheckResult{
		Root:    n.root,
		Message: n.msg,
	}, nil
}

type responseBody struct {
	OK     bool            `json:"ok"`
	Result json.RawMessage `json:"result"`
	Error  string          `json:"error"`
	Code   int             `json:"code"`
	Extra  string          `json:"@extra"`
}

func newTestServer() *Server {
	server := &Server{
		requestTimeout: defaultRequestTimeout,
		now: func() time.Time {
			return testHTTPAPINow
		},
	}
	server.routes = server.buildRoutes()
	return server
}

func decodeResponse(t *testing.T, data []byte) responseBody {
	t.Helper()

	var body responseBody
	if err := json.Unmarshal(data, &body); err != nil {
		t.Fatalf("decode response: %v\n%s", err, data)
	}
	return body
}

func resultMap(t *testing.T, body responseBody) map[string]any {
	t.Helper()

	var result map[string]any
	if err := json.Unmarshal(body.Result, &result); err != nil {
		t.Fatalf("decode result: %v\n%s", err, body.Result)
	}
	return result
}

func nestedMap(t *testing.T, values map[string]any, key string) map[string]any {
	t.Helper()

	value, ok := values[key].(map[string]any)
	if !ok {
		t.Fatalf("%s is %T, want object", key, values[key])
	}
	return value
}

func stringResult(t *testing.T, body responseBody) string {
	t.Helper()

	var result string
	if err := json.Unmarshal(body.Result, &result); err != nil {
		t.Fatalf("decode string result: %v\n%s", err, body.Result)
	}
	return result
}

type benchmarkResponseWriter struct {
	header http.Header
	bytes.Buffer
}

func (w *benchmarkResponseWriter) Header() http.Header {
	return w.header
}

func (w *benchmarkResponseWriter) WriteHeader(int) {}

func benchmarkJSONResponse() successEnvelope {
	transactions := make([]shortTxID, 40)
	for i := range transactions {
		transactions[i] = shortTxID{
			Type:    shortTxIDType,
			Mode:    shortTxIDMode,
			Account: "0:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			LT:      "1234567890123456",
			Hash:    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		}
	}
	return successEnvelope{
		OK: true,
		Result: blockTransactions{
			Type:         blockTransactionsType,
			ReqCount:     uint32(len(transactions)),
			Transactions: transactions,
		},
		Extra: "1783269917:_:0.001",
	}
}

func BenchmarkJSONEncoders(b *testing.B) {
	value := benchmarkJSONResponse()

	b.Run("encoding_json", func(b *testing.B) {
		w := &benchmarkResponseWriter{header: http.Header{}}
		b.ReportAllocs()
		for b.Loop() {
			w.Reset()
			if err := json.NewEncoder(w).Encode(value); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("go_json", func(b *testing.B) {
		server := &Server{}
		w := &benchmarkResponseWriter{header: http.Header{}}
		b.ReportAllocs()
		for b.Loop() {
			w.Reset()
			server.writeJSON(w, http.StatusOK, value)
		}
	})
}

func TestWriteJSONMatchesStandardLibrary(t *testing.T) {
	value := benchmarkJSONResponse()
	value.Extra = "<tag>&\u2028\u2029"

	var want bytes.Buffer
	if err := json.NewEncoder(&want).Encode(value); err != nil {
		t.Fatalf("encode with standard library: %v", err)
	}

	recorder := httptest.NewRecorder()
	new(Server).writeJSON(recorder, http.StatusAccepted, value)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusAccepted)
	}
	if !bytes.Equal(recorder.Body.Bytes(), want.Bytes()) {
		t.Fatalf("response differs from encoding/json:\n got %s\nwant %s", recorder.Body.Bytes(), want.Bytes())
	}

	var decoded successEnvelope
	if err := json.Unmarshal(recorder.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode response with standard library: %v", err)
	}
}

func TestDecodeJSONBody(t *testing.T) {
	if maxBodyBytes != 8<<20 {
		t.Fatalf("max body bytes = %d, want %d", maxBodyBytes, 8<<20)
	}

	t.Run("whitespace is an empty object", func(t *testing.T) {
		request := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(" \n\t "))
		var body map[string]json.RawMessage
		if err := decodeJSONBody(request, maxBodyBytes, &body); err != nil {
			t.Fatal(err)
		}
		if body == nil || len(body) != 0 {
			t.Fatalf("decoded body = %#v, want empty object", body)
		}
	})

	t.Run("body limit", func(t *testing.T) {
		request := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(`{"value":1}`))
		var body map[string]json.RawMessage
		err := decodeJSONBody(request, 4, &body)
		var tooLarge *http.MaxBytesError
		if !errors.As(err, &tooLarge) {
			t.Fatalf("error = %v, want MaxBytesError", err)
		}
	})
}

func BenchmarkDecodeJSONBody(b *testing.B) {
	small := []byte(`{"address":"0:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}`)
	large := make([]byte, 0, 64<<10)
	large = append(large, `{"boc":"`...)
	large = append(large, bytes.Repeat([]byte{'a'}, (64<<10)-len(large)-2)...)
	large = append(large, '"', '}')

	benchmarks := []struct {
		name          string
		payload       []byte
		contentLength int64
	}{
		{name: "small", payload: small, contentLength: int64(len(small))},
		{name: "64KiB", payload: large, contentLength: int64(len(large))},
		{name: "oversized_content_length", payload: small, contentLength: maxBodyBytes},
	}
	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				request := &http.Request{
					Body:          io.NopCloser(bytes.NewReader(benchmark.payload)),
					ContentLength: benchmark.contentLength,
				}
				var body map[string]json.RawMessage
				if err := decodeJSONBody(request, maxBodyBytes, &body); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
