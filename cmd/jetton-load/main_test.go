package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"math/big"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestLoadWalletTopUpIncludesDeliveryReserve(t *testing.T) {
	minimum := big.NewInt(1_000_000_000_000)
	balance := big.NewInt(24_436_810_678)

	got := loadWalletTopUp(minimum, balance)
	want := big.NewInt(975_573_189_322)
	if got.Cmp(want) != 0 {
		t.Fatalf("top up = %s, want %s", got, want)
	}
	if minimum.Cmp(big.NewInt(1_000_000_000_000)) != 0 {
		t.Fatalf("minimum mutated: %s", minimum)
	}
	if balance.Cmp(big.NewInt(24_436_810_678)) != 0 {
		t.Fatalf("balance mutated: %s", balance)
	}
}

func TestSubmitExternalWithRetryReusesExactMessage(t *testing.T) {
	body := cell.BeginCell().MustStoreUInt(0x5a, 8).EndCell()
	external := &tlb.ExternalMessage{
		DstAddr: address.NewAddress(0, 0, make([]byte, 32)),
		Body:    body,
	}
	temporary := errors.New("temporary admission error")
	var sent []*tlb.ExternalMessage
	var waits []time.Duration

	attempts, err := submitExternalWithRetry(
		t.Context(),
		external,
		func(_ context.Context, message *tlb.ExternalMessage) error {
			sent = append(sent, message)
			if len(sent) == 1 {
				return temporary
			}
			return nil
		},
		func(_ context.Context, delay time.Duration) error {
			waits = append(waits, delay)
			return nil
		},
		submissionRetryPeriod,
		submissionRetryWindow,
	)
	if err != nil {
		t.Fatal(err)
	}
	if attempts != 2 || len(sent) != 2 {
		t.Fatalf("attempts = %d, sends = %d, want 2", attempts, len(sent))
	}
	for i, message := range sent {
		if message != external || message.Body != body || message.Body.HashKey() != body.HashKey() {
			t.Fatalf("attempt %d did not reuse the exact external message", i+1)
		}
	}
	if !slices.Equal(waits, []time.Duration{submissionRetryPeriod}) {
		t.Fatalf("waits = %v, want [%s]", waits, submissionRetryPeriod)
	}
}

func TestSubmitExternalWithRetryStopsAtWindow(t *testing.T) {
	external := &tlb.ExternalMessage{}
	permanent := errors.New("permanent admission error")
	sends := 0
	var waited time.Duration

	attempts, err := submitExternalWithRetry(
		t.Context(),
		external,
		func(_ context.Context, message *tlb.ExternalMessage) error {
			if message != external {
				t.Fatal("submission replaced the external message")
			}
			sends++
			return permanent
		},
		func(_ context.Context, delay time.Duration) error {
			waited += delay
			return nil
		},
		submissionRetryPeriod,
		submissionRetryWindow,
	)
	if !errors.Is(err, permanent) {
		t.Fatalf("error = %v, want %v", err, permanent)
	}
	wantAttempts := int(submissionRetryWindow/submissionRetryPeriod) + 1
	if attempts != wantAttempts || sends != wantAttempts {
		t.Fatalf("attempts = %d, sends = %d, want %d", attempts, sends, wantAttempts)
	}
	if waited != submissionRetryWindow {
		t.Fatalf("waited = %s, want %s", waited, submissionRetryWindow)
	}
}

func TestSubmitExternalWithRetryHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	temporary := errors.New("temporary admission error")
	sends := 0
	waits := 0

	attempts, err := submitExternalWithRetry(
		ctx,
		&tlb.ExternalMessage{},
		func(context.Context, *tlb.ExternalMessage) error {
			sends++
			return temporary
		},
		func(ctx context.Context, _ time.Duration) error {
			waits++
			cancel()
			return ctx.Err()
		},
		submissionRetryPeriod,
		submissionRetryWindow,
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want %v", err, context.Canceled)
	}
	if attempts != 1 || sends != 1 || waits != 1 {
		t.Fatalf("attempts = %d, sends = %d, waits = %d, want 1, 1, 1", attempts, sends, waits)
	}
}

func TestSubmitExternalWithRetryReturnsImmediatelyOnSuccess(t *testing.T) {
	sends := 0
	waits := 0

	attempts, err := submitExternalWithRetry(
		t.Context(),
		&tlb.ExternalMessage{},
		func(context.Context, *tlb.ExternalMessage) error {
			sends++
			return nil
		},
		func(context.Context, time.Duration) error {
			waits++
			return nil
		},
		submissionRetryPeriod,
		submissionRetryWindow,
	)
	if err != nil {
		t.Fatal(err)
	}
	if attempts != 1 || sends != 1 || waits != 0 {
		t.Fatalf("attempts = %d, sends = %d, waits = %d, want 1, 1, 0", attempts, sends, waits)
	}
}

func TestInitializeHighloadWalletDelayedActivation(t *testing.T) {
	clock := newHighloadInitTestClock()
	sends := 0
	checks := 0
	err := initializeHighloadWalletWithWait(
		context.Background(),
		time.Minute,
		clock.wait,
		func(context.Context) error {
			sends++
			return nil
		},
		func(context.Context) (bool, error) {
			checks++
			return checks == 5, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if sends != 1 {
		t.Fatalf("send attempts = %d, want 1", sends)
	}
	if checks != 5 {
		t.Fatalf("activation checks = %d, want 5", checks)
	}
	if clock.elapsed != 4*highloadInitPoll {
		t.Fatalf("elapsed = %s, want %s", clock.elapsed, 4*highloadInitPoll)
	}
}

func TestInitializeHighloadWalletErrorButActive(t *testing.T) {
	sendErr := errors.New("import result unavailable")
	clock := newHighloadInitTestClock()
	sends := 0
	err := initializeHighloadWalletWithWait(
		context.Background(),
		time.Minute,
		clock.wait,
		func(context.Context) error {
			sends++
			return sendErr
		},
		func(context.Context) (bool, error) {
			return true, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if sends != 1 {
		t.Fatalf("send attempts = %d, want 1", sends)
	}
	if clock.elapsed != 0 {
		t.Fatalf("elapsed = %s, want 0", clock.elapsed)
	}
}

func TestInitializeHighloadWalletResubmitCadence(t *testing.T) {
	clock := newHighloadInitTestClock()
	var sendTimes []time.Duration
	err := initializeHighloadWalletWithWait(
		context.Background(),
		time.Minute,
		clock.wait,
		func(context.Context) error {
			sendTimes = append(sendTimes, clock.elapsed)
			return nil
		},
		func(context.Context) (bool, error) {
			return clock.elapsed >= 25*time.Second, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	want := []time.Duration{0, 10 * time.Second, 20 * time.Second}
	if !slices.Equal(sendTimes, want) {
		t.Fatalf("send times = %v, want %v", sendTimes, want)
	}
}

func TestInitializeHighloadWalletPermanentTimeoutReturnsLastError(t *testing.T) {
	permanentErr := errors.New("import failed")
	clock := newHighloadInitTestClock()
	var sendTimes []time.Duration
	err := initializeHighloadWalletWithWait(
		context.Background(),
		time.Minute,
		clock.wait,
		func(context.Context) error {
			sendTimes = append(sendTimes, clock.elapsed)
			return permanentErr
		},
		func(context.Context) (bool, error) {
			return false, nil
		},
	)
	if !errors.Is(err, permanentErr) {
		t.Fatalf("initialize error = %v, want wrapping %v", err, permanentErr)
	}
	if !strings.Contains(err.Error(), "remained inactive after 1m0s") {
		t.Fatalf("initialize error = %q, want timeout detail", err)
	}
	want := []time.Duration{
		0,
		10 * time.Second,
		20 * time.Second,
		30 * time.Second,
		40 * time.Second,
		50 * time.Second,
	}
	if !slices.Equal(sendTimes, want) {
		t.Fatalf("send times = %v, want %v", sendTimes, want)
	}
	if clock.elapsed != time.Minute {
		t.Fatalf("elapsed = %s, want 1m", clock.elapsed)
	}
}

func TestInitializeHighloadWalletHonorsConfiguredTimeout(t *testing.T) {
	clock := newHighloadInitTestClock()
	err := initializeHighloadWalletWithWait(
		context.Background(),
		2*time.Minute,
		clock.wait,
		func(context.Context) error { return nil },
		func(context.Context) (bool, error) { return false, nil },
	)
	if err == nil || !strings.Contains(err.Error(), "remained inactive after 2m0s") {
		t.Fatalf("initialize error = %v", err)
	}
	if clock.elapsed != 2*time.Minute {
		t.Fatalf("elapsed = %s, want 2m", clock.elapsed)
	}
}

type highloadInitTestClock struct {
	elapsed time.Duration
}

func newHighloadInitTestClock() *highloadInitTestClock {
	return &highloadInitTestClock{}
}

func (c *highloadInitTestClock) wait(ctx context.Context, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	c.elapsed += delay
	return nil
}

func TestSetupOptions(t *testing.T) {
	setup, err := parseSetupOptions([]string{"--sender-index", "17", "--timeout", "10m"})
	if err != nil {
		t.Fatal(err)
	}
	if setup.senderIndex != 17 {
		t.Fatalf("setup sender index = %d, want 17", setup.senderIndex)
	}
	if setup.timeout != 10*time.Minute {
		t.Fatalf("setup timeout = %s, want 10m", setup.timeout)
	}
	fundingSeed := bytes.Repeat([]byte{0x44}, ed25519.SeedSize)
	setup, err = parseSetupOptions([]string{
		"--funding-key", base64.StdEncoding.EncodeToString(fundingSeed),
	})
	if err != nil {
		t.Fatal(err)
	}
	if setup.fundingKey != base64.StdEncoding.EncodeToString(fundingSeed) {
		t.Fatalf("setup funding key = %q", setup.fundingKey)
	}
	defaults, err := parseSetupOptions(nil)
	if err != nil {
		t.Fatal(err)
	}
	if defaults.timeout != defaultSetupTimeout {
		t.Fatalf("default setup timeout = %s, want %s", defaults.timeout, defaultSetupTimeout)
	}
	if _, err = parseSetupOptions([]string{"--timeout", "0s"}); err == nil || !strings.Contains(err.Error(), "timeout must be positive") {
		t.Fatalf("zero timeout error = %v", err)
	}

	setup, err = parseSetupOptions([]string{"--sender-index", "17"})
	if err != nil {
		t.Fatal(err)
	}

	run, err := parseRunOptions([]string{"--sender-index", "29"})
	if err != nil {
		t.Fatal(err)
	}
	if run.senderIndex != 29 {
		t.Fatalf("run sender index = %d, want 29", run.senderIndex)
	}

	runDefaults, err := parseRunOptions(nil)
	if err != nil {
		t.Fatal(err)
	}
	if runDefaults.senderIndex != 0 {
		t.Fatalf("default sender index = %d, want 0", runDefaults.senderIndex)
	}

	maximum, err := parseSetupOptions([]string{"--sender-index", "18446744073709551615"})
	if err != nil {
		t.Fatal(err)
	}
	if maximum.senderIndex != ^uint64(0) {
		t.Fatalf("maximum sender index = %d, want %d", maximum.senderIndex, ^uint64(0))
	}
}

func TestValidateStateSender(t *testing.T) {
	tests := []struct {
		name        string
		state       loadState
		senderIndex uint64
		wantError   string
	}{
		{
			name:        "matching sender",
			state:       loadState{FormatVersion: stateFormatVersion, SenderIndex: 7},
			senderIndex: 7,
		},
		{
			name:        "old format",
			state:       loadState{FormatVersion: stateFormatVersion - 1, SenderIndex: 7},
			senderIndex: 7,
			wantError:   "unsupported state format",
		},
		{
			name:        "different sender",
			state:       loadState{FormatVersion: stateFormatVersion, SenderIndex: 8},
			senderIndex: 7,
			wantError:   "state sender index 8 differs from requested sender index 7",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateStateSender(test.state, test.senderIndex)
			if test.wantError == "" {
				if err != nil {
					t.Fatalf("validate state: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("validate state error = %v, want containing %q", err, test.wantError)
			}
		})
	}
}

func TestStatePersistsSenderIndex(t *testing.T) {
	path := t.TempDir() + "/sender.state.json"
	want := loadState{
		FormatVersion:       stateFormatVersion,
		SenderIndex:         ^uint64(0),
		Minter:              "minter",
		HighloadWallet:      "highload",
		SourceJettonWallet:  "source",
		NextHighloadQueryID: 42,
	}
	if err := writeState(path, want); err != nil {
		t.Fatal(err)
	}
	got, err := readState(path)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("read state = %+v, want %+v", got, want)
	}
}

func TestDeriveHighloadKeyIsolatesSenders(t *testing.T) {
	seed := make([]byte, ed25519.SeedSize)
	for i := range seed {
		seed[i] = byte(i)
	}

	first := deriveHighloadKey(seed, 41)
	repeat := deriveHighloadKey(seed, 41)
	second := deriveHighloadKey(seed, 42)
	if !bytes.Equal(first, repeat) {
		t.Fatal("same sender index produced different keys")
	}
	if bytes.Equal(first, second) {
		t.Fatal("different sender indexes produced the same key")
	}
	const wantSeed = "2d641c994a9186fcf3c3b256b316280df155bbd4a01a604604c561eb8e9deddf"
	if got := hex.EncodeToString(first.Seed()); got != wantSeed {
		t.Fatalf("sender key seed = %s, want %s", got, wantSeed)
	}
}

func TestHighloadQueryIDSkipsReservedDictionaryIndex(t *testing.T) {
	tests := []struct {
		name string
		id   uint32
		want uint32
	}{
		{name: "ordinary", id: 41, want: 42},
		{name: "reserved index", id: 1022, want: 1024},
		{name: "last valid", id: highloadQueryLimit - 2, want: 0},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := advanceHighloadQueryID(test.id); got != test.want {
				t.Fatalf("advanceHighloadQueryID(%d) = %d, want %d", test.id, got, test.want)
			}
		})
	}
}

func TestNormalizeHighloadQueryID(t *testing.T) {
	if got := normalizeHighloadQueryID(1023); got != 1024 {
		t.Fatalf("normalize reserved ID = %d, want 1024", got)
	}
	if got := normalizeHighloadQueryID(2048); got != 2048 {
		t.Fatalf("normalize valid ID = %d, want 2048", got)
	}
}

func TestNextHighloadQueryIDRebasesExpiredReservations(t *testing.T) {
	now := time.Unix(1_786_342_339, 0)
	current := currentHighloadQueryID(now)
	persisted := advanceHighloadQueryID(current)

	tests := []struct {
		name    string
		state   loadState
		want    uint32
		rebased bool
	}{
		{
			name: "fresh reservation continues",
			state: loadState{
				NextHighloadQueryID:       persisted,
				HighloadQueryIDReservedAt: now.Add(-highloadTTL * time.Second).Unix(),
			},
			want: persisted,
		},
		{
			name: "expired reservation rebases",
			state: loadState{
				NextHighloadQueryID:       persisted,
				HighloadQueryIDReservedAt: now.Add(-(highloadTTL*time.Second + time.Second)).Unix(),
			},
			want:    current,
			rebased: true,
		},
		{
			name:    "legacy reservation rebases",
			state:   loadState{NextHighloadQueryID: persisted},
			want:    current,
			rebased: true,
		},
		{
			name: "future reservation rebases",
			state: loadState{
				NextHighloadQueryID:       persisted,
				HighloadQueryIDReservedAt: now.Add(time.Second).Unix(),
			},
			want:    current,
			rebased: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, rebased := nextHighloadQueryID(test.state, now)
			if got != test.want || rebased != test.rebased {
				t.Fatalf("nextHighloadQueryID() = (%d, %t), want (%d, %t)", got, rebased, test.want, test.rebased)
			}
		})
	}
}

func TestHighloadCreatedAtUsesWalletShardBlockTime(t *testing.T) {
	master := ton.BlockIDExt{Workchain: -1, Shard: -1 << 63, SeqNo: 100}
	left := ton.BlockIDExt{Workchain: 0, Shard: 1 << 62, SeqNo: 200}
	right := ton.BlockIDExt{Workchain: 0, Shard: -1 << 62, SeqNo: 300}
	api := &highloadTimeTestAPI{
		master: &master,
		shards: []*ton.BlockIDExt{&left, &right},
		headers: map[uint32]uint32{
			left.SeqNo:  1_900_000_100,
			right.SeqNo: 1_900_000_200,
		},
	}
	data := make([]byte, 32)
	data[0] = 0xd0
	addr := address.NewAddress(0, 0, data)

	got, err := highloadCreatedAt(context.Background(), api, addr)
	if err != nil {
		t.Fatal(err)
	}
	if want := int64(1_900_000_199); got != want {
		t.Fatalf("created_at = %d, want %d", got, want)
	}
	if api.headerRequests != 1 || api.lastHeader != right.SeqNo {
		t.Fatalf("header requests = %d for seqno %d, want one request for %d",
			api.headerRequests, api.lastHeader, right.SeqNo)
	}
}

func TestHighloadCreatedAtRejectsMissingWalletShard(t *testing.T) {
	master := ton.BlockIDExt{Workchain: -1, Shard: -1 << 63, SeqNo: 100}
	foreign := ton.BlockIDExt{Workchain: 1, Shard: -1 << 63, SeqNo: 200}
	api := &highloadTimeTestAPI{master: &master, shards: []*ton.BlockIDExt{&foreign}}
	addr := address.NewAddress(0, 0, make([]byte, 32))

	_, err := highloadCreatedAt(context.Background(), api, addr)
	if err == nil || !strings.Contains(err.Error(), "wallet shard is absent") {
		t.Fatalf("error = %v, want missing wallet shard", err)
	}
}

type highloadTimeTestAPI struct {
	master         *ton.BlockIDExt
	shards         []*ton.BlockIDExt
	headers        map[uint32]uint32
	headerRequests int
	lastHeader     uint32
}

func (a *highloadTimeTestAPI) CurrentMasterchainInfo(context.Context) (*ton.BlockIDExt, error) {
	return a.master, nil
}

func (a *highloadTimeTestAPI) GetBlockShardsInfo(context.Context, *ton.BlockIDExt) ([]*ton.BlockIDExt, error) {
	return a.shards, nil
}

func (a *highloadTimeTestAPI) GetBlockHeader(_ context.Context, id *ton.BlockIDExt) (*tlb.BlockHeader, error) {
	a.headerRequests++
	a.lastHeader = id.SeqNo
	header := &tlb.BlockHeader{}
	header.GenUtime = a.headers[id.SeqNo]
	return header, nil
}

func TestReadStateMigratesV2QueryReservation(t *testing.T) {
	path := t.TempDir() + "/sender.state.json"
	legacy := loadState{
		FormatVersion:             legacyStateFormat,
		SenderIndex:               7,
		Minter:                    "minter",
		HighloadWallet:            "highload",
		SourceJettonWallet:        "source",
		NextHighloadQueryID:       42,
		HighloadQueryIDReservedAt: 123,
	}
	if err := writeState(path, legacy); err != nil {
		t.Fatal(err)
	}

	state, err := readState(path)
	if err != nil {
		t.Fatal(err)
	}
	if state.FormatVersion != stateFormatVersion {
		t.Fatalf("migrated format = %d, want %d", state.FormatVersion, stateFormatVersion)
	}
	if state.HighloadQueryIDReservedAt != 0 {
		t.Fatalf("migrated reservation timestamp = %d, want 0", state.HighloadQueryIDReservedAt)
	}
	if state.HighloadMessageTTL != previousHighloadTTL {
		t.Fatalf("migrated message TTL = %d, want %d", state.HighloadMessageTTL, previousHighloadTTL)
	}
	if state.NextHighloadQueryID != legacy.NextHighloadQueryID {
		t.Fatalf("migrated query ID = %d, want %d", state.NextHighloadQueryID, legacy.NextHighloadQueryID)
	}
}

func TestReadStateMigratesV3MessageTTL(t *testing.T) {
	path := t.TempDir() + "/sender.state.json"
	previous := loadState{
		FormatVersion:             previousStateFormat,
		SenderIndex:               7,
		Minter:                    "minter",
		HighloadWallet:            "highload",
		SourceJettonWallet:        "source",
		NextHighloadQueryID:       42,
		HighloadQueryIDReservedAt: 123,
	}
	if err := writeState(path, previous); err != nil {
		t.Fatal(err)
	}

	state, err := readState(path)
	if err != nil {
		t.Fatal(err)
	}
	if state.FormatVersion != stateFormatVersion {
		t.Fatalf("migrated format = %d, want %d", state.FormatVersion, stateFormatVersion)
	}
	if state.HighloadQueryIDReservedAt != previous.HighloadQueryIDReservedAt {
		t.Fatalf("migrated reservation timestamp = %d, want %d", state.HighloadQueryIDReservedAt, previous.HighloadQueryIDReservedAt)
	}
	if state.HighloadMessageTTL != previousHighloadTTL {
		t.Fatalf("migrated message TTL = %d, want %d", state.HighloadMessageTTL, previousHighloadTTL)
	}
}

func TestHighloadQueryIDCycleContainsOnlyValidIDs(t *testing.T) {
	id := uint32(0)
	for i := uint32(0); i < highloadValidQueries; i++ {
		if id >= highloadQueryLimit {
			t.Fatalf("ID %d is outside the 23-bit range", id)
		}
		if id&highloadQueryLowMask == highloadQueryLowMask {
			t.Fatalf("ID %d uses reserved dictionary index", id)
		}
		id = advanceHighloadQueryID(id)
	}
	if id != 0 {
		t.Fatalf("full valid cycle ended at %d, want 0", id)
	}
}
