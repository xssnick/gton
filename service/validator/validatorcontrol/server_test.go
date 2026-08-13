package validatorcontrol

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/validator/keyring"
	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
)

func TestTLControlQueryRoundTrip(t *testing.T) {
	t.Parallel()

	want := AddValidatorPermanentKey{
		KeyHash:      repeatedBytes(0x11, 32),
		ElectionDate: 1_700_000_000,
		TTL:          1_700_065_500,
	}
	inner, err := tl.Serialize(want, true)
	if err != nil {
		t.Fatalf("serialize inner query: %v", err)
	}

	wire, err := tl.Serialize(ControlQuery{Data: inner}, true)
	if err != nil {
		t.Fatalf("serialize control envelope: %v", err)
	}

	var parsed any
	if _, err = tl.Parse(&parsed, wire, true); err != nil {
		t.Fatalf("parse control envelope: %v", err)
	}
	envelope, ok := parsed.(ControlQuery)
	if !ok {
		t.Fatalf("parsed envelope type = %T", parsed)
	}

	query, err := parseControlQuery(envelope.Data)
	if err != nil {
		t.Fatalf("parse inner query: %v", err)
	}
	got, ok := query.(AddValidatorPermanentKey)
	if !ok {
		t.Fatalf("parsed inner type = %T", query)
	}
	if string(got.KeyHash) != string(want.KeyHash) ||
		got.ElectionDate != want.ElectionDate || got.TTL != want.TTL {
		t.Fatalf("parsed inner query = %#v, want %#v", got, want)
	}
}

func TestTLConstructorIDsMatchTONSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value tl.Serializable
		id    uint32
	}{
		{name: "control query", value: ControlQuery{}, id: 0xa476bdc0},
		{name: "key hash", value: KeyHash{}, id: 0xc2c6a54e},
		{name: "signature", value: Signature{}, id: 0xfb6c4328},
		{name: "one stat", value: OneStat{}, id: 0xa4983aed},
		{name: "stats", value: Stats{}, id: 0x5d49d36f},
		{name: "control error", value: ControlQueryError{}, id: 0x77269a1f},
		{name: "success", value: Success{}, id: 0xb3e4a68b},
		{name: "json config", value: JSONConfig{}, id: 0x132d920b},
		{name: "export public key", value: ExportPublicKey{}, id: 0x6234a8b9},
		{name: "generate key", value: GenerateKeyPair{}, id: 0xeb25607b},
		{name: "add permanent", value: AddValidatorPermanentKey{}, id: 0x92150578},
		{name: "add temp", value: AddValidatorTempKey{}, id: 0x8d336f32},
		{name: "add ADNL", value: AddValidatorADNLAddress{}, id: 0xdacba682},
		{name: "sign", value: Sign{}, id: 0x1aea1a28},
		{name: "get stats", value: GetStats{}, id: 0x52d5c311},
		{name: "get config", value: GetConfig{}, id: 0x59ad2225},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			wire, err := tl.Serialize(test.value, true)
			if err != nil {
				t.Fatalf("serialize: %v", err)
			}
			if got, want := binary.LittleEndian.Uint32(wire), test.id; got != want {
				t.Fatalf("constructor ID = %08x, want %08x", got, want)
			}
		})
	}
}

func TestServerMTCKeyFlow(t *testing.T) {
	_, keyStore, client, closeServer := startTestServer(t, PermissionDefault|PermissionModify|PermissionUnsafe)
	defer closeServer()

	var stats Stats
	queryControl(t, client, GetStats{}, &stats)
	values := make(map[string]string, len(stats.Stats))
	for _, stat := range stats.Stats {
		values[stat.Key] = stat.Value
	}
	if values["masterchainblock"] != fmt.Sprintf(
		"(-1,8000000000000000,123):%s:%s",
		string(repeatedBytes('a', 64)),
		string(repeatedBytes('b', 64)),
	) {
		t.Fatalf("masterchainblock = %q", values["masterchainblock"])
	}
	if values["masterchainblocktime"] != "1699999999" || values["shardclientmasterchainseqno"] != "123" {
		t.Fatalf("unexpected sync stats: %#v", values)
	}

	var initialConfig JSONConfig
	queryControl(t, client, GetConfig{}, &initialConfig)
	var initialParsed jsonValidatorConfig
	if err := json.Unmarshal([]byte(initialConfig.Data), &initialParsed); err != nil {
		t.Fatalf("parse initial getConfig JSON: %v", err)
	}
	if len(initialParsed.ADNL) != 1 || len(initialParsed.Validators) != 0 {
		t.Fatalf("initial getConfig = %#v", initialParsed)
	}

	var generated KeyHash
	queryControl(t, client, GenerateKeyPair{}, &generated)
	keyID := keyIDFromBytes(generated.KeyHash)
	electionDate := uint32(1_700_000_000)
	expireAt := electionDate + 65_500

	var success Success
	queryControl(t, client, AddValidatorPermanentKey{
		KeyHash:      generated.KeyHash,
		ElectionDate: electionDate,
		TTL:          expireAt,
	}, &success)
	queryControl(t, client, AddValidatorTempKey{
		PermanentKeyHash: generated.KeyHash,
		KeyHash:          generated.KeyHash,
		TTL:              expireAt,
	}, &success)

	var publicKey keys.PublicKeyED25519
	queryControl(t, client, ExportPublicKey{KeyHash: generated.KeyHash}, &publicKey)

	// MTC intentionally sends addValidatorTempKey again after exportPublicKey.
	queryControl(t, client, AddValidatorTempKey{
		PermanentKeyHash: generated.KeyHash,
		KeyHash:          generated.KeyHash,
		TTL:              expireAt,
	}, &success)
	queryControl(t, client, AddValidatorADNLAddress{
		PermanentKeyHash: generated.KeyHash,
		KeyHash:          keyStore.localADNLID[:],
		TTL:              expireAt,
	}, &success)

	wantPublicKey, err := keyStore.PublicKeyFor(keyID)
	if err != nil {
		t.Fatalf("read generated public key: %v", err)
	}
	if string(publicKey.Key) != string(wantPublicKey) {
		t.Fatalf("exported public key = %x, want %x", publicKey.Key, wantPublicKey)
	}

	payload := []byte("election payload")
	var signature Signature
	queryControl(t, client, Sign{KeyHash: generated.KeyHash, Data: payload}, &signature)
	if !ed25519.Verify(publicKey.Key, payload, signature.Signature) {
		t.Fatal("validator signature does not verify")
	}

	var config JSONConfig
	queryControl(t, client, GetConfig{}, &config)
	var parsed jsonValidatorConfig
	if err = json.Unmarshal([]byte(config.Data), &parsed); err != nil {
		t.Fatalf("parse getConfig JSON: %v", err)
	}
	if len(parsed.ADNL) != 1 || parsed.ADNL[0].ID != base64.StdEncoding.EncodeToString(keyStore.localADNLID[:]) {
		t.Fatalf("getConfig ADNL = %#v", parsed.ADNL)
	}
	if len(parsed.Validators) != 1 {
		t.Fatalf("getConfig validators = %#v", parsed.Validators)
	}
	validator := parsed.Validators[0]
	if validator.ID != base64.StdEncoding.EncodeToString(keyID[:]) || validator.ElectionDate != electionDate {
		t.Fatalf("getConfig validator = %#v", validator)
	}
	if len(validator.TempKeys) != 1 || validator.TempKeys[0].Key != validator.ID {
		t.Fatalf("getConfig temp keys = %#v", validator.TempKeys)
	}
	if len(validator.ADNLAddresses) != 1 || validator.ADNLAddresses[0].ID != parsed.ADNL[0].ID {
		t.Fatalf("getConfig validator ADNL addresses = %#v", validator.ADNLAddresses)
	}

}

func TestExecuteChecksPermissionsAndADNL(t *testing.T) {
	t.Parallel()

	localADNLID := repeatedID(0x42)
	keys := &fakeKeys{localADNLID: localADNLID}
	server := &Server{
		keys:        keys,
		localADNLID: localADNLID,
		logger:      zerolog.Nop(),
	}
	clientID := repeatedID(0x77)

	result := server.execute(context.Background(), clientID, PermissionDefault, AddValidatorPermanentKey{
		KeyHash: repeatedBytes(0x11, 32),
	})
	assertControlError(t, result, controlErrorCode)

	result = server.execute(context.Background(), clientID, PermissionModify, AddValidatorADNLAddress{
		PermanentKeyHash: repeatedBytes(0x11, 32),
		KeyHash:          repeatedBytes(0x43, 32),
	})
	assertControlError(t, result, controlErrorCode)
	if keys.addADNLCalls != 0 {
		t.Fatalf("AddADNL calls = %d, want 0", keys.addADNLCalls)
	}
}

func TestServerCanRestart(t *testing.T) {
	server, _, client, closeServer := startTestServer(t, PermissionDefault)
	client.Stop()
	if err := server.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}

	if err := server.Start(); err != nil {
		t.Fatalf("restart: %v", err)
	}
	if err := server.Start(); err != nil {
		t.Fatalf("idempotent start: %v", err)
	}
	closeErrors := make(chan error, 4)
	var closes sync.WaitGroup
	for range 4 {
		closes.Go(func() {
			closeErrors <- server.Close()
		})
	}
	closes.Wait()
	close(closeErrors)
	for err := range closeErrors {
		if err != nil {
			t.Fatalf("concurrent close: %v", err)
		}
	}
	if err := server.Close(); err != nil {
		t.Fatalf("idempotent close: %v", err)
	}

	closeServer()
}

func TestGetStatsReturnsNotReadyWithoutState(t *testing.T) {
	t.Parallel()

	server := &Server{
		keys:   &fakeKeys{},
		state:  fakeStateReader{err: errors.New("state not found")},
		logger: zerolog.Nop(),
	}

	result := server.execute(context.Background(), repeatedID(1), PermissionDefault, GetStats{})
	assertControlError(t, result, controlNotReadyCode)
}

func startTestServer(
	t *testing.T,
	permissions uint32,
) (*Server, *fakeKeys, *liteclient.ConnectionPool, func()) {
	t.Helper()

	serverPublicKey, serverPrivateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate server key: %v", err)
	}
	clientPublicKey, clientPrivateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate client key: %v", err)
	}
	clientID, err := tl.Hash(keys.PublicKeyED25519{Key: clientPublicKey})
	if err != nil {
		t.Fatalf("hash client public key: %v", err)
	}

	localADNLID := repeatedID(0x42)
	keyStore := &fakeKeys{
		keys:        make(map[[32]byte]ed25519.PrivateKey),
		localADNLID: localADNLID,
	}
	state := fakeStateReader{current: &storage.CurrentState{
		ShardClientSeqno: 123,
		Masterchain: storage.BlockState{
			Block: ton.BlockIDExt{
				Workchain: -1,
				Shard:     int64(-1 << 63),
				SeqNo:     123,
				RootHash:  repeatedBytes(0xaa, 32),
				FileHash:  repeatedBytes(0xbb, 32),
			},
			Parsed: &tlb.ShardStateUnsplit{GenUTime: 1_699_999_999},
		},
	}}

	var trustedID [32]byte
	copy(trustedID[:], clientID)
	server, err := New(Options{
		ListenAddress: "127.0.0.1:0",
		ServerKey:     serverPrivateKey,
		TrustedClients: []TrustedClient{{
			ID:          trustedID,
			Permissions: permissions,
		}},
		Keys:        keyStore,
		LocalADNLID: localADNLID,
		State:       state,
		Logger:      zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("construct validator control server: %v", err)
	}
	if err = server.Start(); err != nil {
		t.Fatalf("start validator control server: %v", err)
	}

	server.mu.Lock()
	address := server.address
	server.mu.Unlock()
	client := liteclient.NewConnectionPoolWithAuth(clientPrivateKey)
	connectCtx, cancelConnect := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelConnect()
	if err = client.AddConnection(connectCtx, address, base64.StdEncoding.EncodeToString(serverPublicKey)); err != nil {
		_ = server.Close()
		client.Stop()
		t.Fatalf("connect authenticated validator control client: %v", err)
	}

	return server, keyStore, client, func() {
		client.Stop()
		if err := server.Close(); err != nil {
			t.Errorf("close validator control server: %v", err)
		}
	}
}

func queryControl(t *testing.T, client *liteclient.ConnectionPool, query tl.Serializable, result tl.Serializable) {
	t.Helper()

	inner, err := tl.Serialize(query, true)
	if err != nil {
		t.Fatalf("serialize %T: %v", query, err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err = client.QueryADNL(ctx, ControlQuery{Data: inner}, result); err != nil {
		t.Fatalf("query %T: %v", query, err)
	}
}

func assertControlError(t *testing.T, value tl.Serializable, code int32) {
	t.Helper()

	controlErr, ok := value.(ControlQueryError)
	if !ok {
		t.Fatalf("result type = %T, want ControlQueryError", value)
	}
	if controlErr.Code != code {
		t.Fatalf("control error code = %d, want %d", controlErr.Code, code)
	}
}

type fakeStateReader struct {
	current *storage.CurrentState
	err     error
}

func (r fakeStateReader) CurrentState(context.Context) (*storage.CurrentState, error) {
	return r.current, r.err
}

type fakeKeys struct {
	mu           sync.Mutex
	keys         map[[32]byte]ed25519.PrivateKey
	entries      []keyring.KeyInfo
	localADNLID  [32]byte
	addADNLCalls int
}

func (k *fakeKeys) Generate(context.Context) ([32]byte, error) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return [32]byte{}, err
	}
	hash, err := tl.Hash(keys.PublicKeyED25519{Key: publicKey})
	if err != nil {
		return [32]byte{}, err
	}
	id := keyIDFromBytes(hash)
	k.mu.Lock()
	k.keys[id] = privateKey
	k.entries = append(k.entries, keyring.KeyInfo{ID: id})
	k.mu.Unlock()

	return id, nil
}

func (k *fakeKeys) AddPermanent(_ context.Context, keyID [32]byte, electionDate, expireAt uint32) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	for i := range k.entries {
		if k.entries[i].ID == keyID {
			k.entries[i].Permanent = true
			k.entries[i].ElectionDate = electionDate
			k.entries[i].PermanentExpireAt = expireAt

			return nil
		}
	}

	return errors.New("key not found")
}

func (k *fakeKeys) AddTemp(_ context.Context, permanentKeyID, keyID [32]byte, expireAt uint32) error {
	if permanentKeyID != keyID {
		return errors.New("temp key must equal permanent key")
	}
	k.mu.Lock()
	defer k.mu.Unlock()

	for i := range k.entries {
		if k.entries[i].ID == keyID && k.entries[i].Permanent {
			k.entries[i].TempExpireAt = expireAt

			return nil
		}
	}

	return errors.New("permanent key not found")
}

func (k *fakeKeys) AddADNL(_ context.Context, permanentKeyID, adnlID [32]byte, expireAt uint32) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	k.addADNLCalls++
	if adnlID != k.localADNLID {
		return errors.New("ADNL ID is not local")
	}
	for i := range k.entries {
		if k.entries[i].ID == permanentKeyID && k.entries[i].Permanent {
			k.entries[i].HasADNL = true
			k.entries[i].ADNLID = adnlID
			k.entries[i].ADNLExpireAt = expireAt

			return nil
		}
	}

	return errors.New("permanent key not found")
}

func (k *fakeKeys) PublicKeyFor(keyID [32]byte) (ed25519.PublicKey, error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	privateKey, exists := k.keys[keyID]
	if !exists {
		return nil, errors.New("key not found")
	}

	return privateKey.Public().(ed25519.PublicKey), nil
}

func (k *fakeKeys) Sign(keyID [32]byte, payload []byte) ([]byte, error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	privateKey, exists := k.keys[keyID]
	if !exists {
		return nil, errors.New("key not found")
	}

	return ed25519.Sign(privateKey, payload), nil
}

func (k *fakeKeys) Entries() []keyring.KeyInfo {
	k.mu.Lock()
	defer k.mu.Unlock()

	return append([]keyring.KeyInfo(nil), k.entries...)
}

func repeatedID(value byte) [32]byte {
	var id [32]byte
	for i := range id {
		id[i] = value
	}

	return id
}

func repeatedBytes(value byte, count int) []byte {
	data := make([]byte, count)
	for i := range data {
		data[i] = value
	}

	return data
}
