package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math/big"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestResolveContractProfileUsesPinnedArtifacts(t *testing.T) {
	profile, err := resolveContractProfile(defaultContractProfile, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if profile.name != defaultContractProfile {
		t.Fatalf("profile name = %q, want %q", profile.name, defaultContractProfile)
	}
	if got := cellHashHex(profile.minterCode); got != legacyMinterCodeHash {
		t.Fatalf("minter hash = %s, want %s", got, legacyMinterCodeHash)
	}
	if got := cellHashHex(profile.walletCode); got != legacyWalletCodeHash {
		t.Fatalf("wallet hash = %s, want %s", got, legacyWalletCodeHash)
	}
}

func TestResolveContractProfileRejectsUnknownProfile(t *testing.T) {
	_, err := resolveContractProfile("prepaid-from-mutable-cppnode", "", "")
	if err == nil || !strings.Contains(err.Error(), "unknown contract profile") {
		t.Fatalf("resolve profile error = %v, want unknown profile", err)
	}
}

func TestResolveContractProfileRejectsUnknownArtifactHash(t *testing.T) {
	path := t.TempDir() + "/unknown-wallet.boc"
	unknown := cell.BeginCell().MustStoreUInt(7, 3).EndCell().ToBOC()
	if err := os.WriteFile(path, unknown, 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := resolveContractProfile(defaultContractProfile, "", path)
	if err == nil || !strings.Contains(err.Error(), "root cell hash") {
		t.Fatalf("resolve profile error = %v, want root hash mismatch", err)
	}
}

func TestParseJettonUnitsUsesRawUnits(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  string
	}{
		{name: "one raw unit", value: "1", want: "1"},
		{name: "benchmark amount", value: "1000000", want: "1000000"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			amount, err := parseJettonUnits(test.value)
			if err != nil {
				t.Fatal(err)
			}
			if got := amount.Nano().String(); got != test.want {
				t.Fatalf("raw units = %s, want %s", got, test.want)
			}
		})
	}
}

func TestParseJettonUnitsRejectsInvalidValues(t *testing.T) {
	for _, value := range []string{"", "0", "-1", "1.5"} {
		t.Run(value, func(t *testing.T) {
			if _, err := parseJettonUnits(value); err == nil {
				t.Fatalf("parseJettonUnits(%q) succeeded", value)
			}
		})
	}
}

func TestRecipientAddressesAreIsolatedBySender(t *testing.T) {
	var firstEpoch loadRunEpoch
	firstEpoch[0] = 1
	var secondEpoch loadRunEpoch
	secondEpoch[0] = 2

	first := recipientAddress(7, firstEpoch, 11)
	repeat := recipientAddress(7, firstEpoch, 11)
	otherSender := recipientAddress(8, firstEpoch, 11)
	otherRun := recipientAddress(7, secondEpoch, 11)
	canary := canaryRecipientAddress(7, firstEpoch)

	if !first.Equals(repeat) {
		t.Fatal("same sender, run epoch, and recipient index produced different addresses")
	}
	if first.Equals(otherSender) {
		t.Fatal("different senders share a recipient address")
	}
	if first.Equals(otherRun) {
		t.Fatal("different run epochs share a recipient address")
	}
	if first.Equals(canary) {
		t.Fatal("canary address overlaps the load recipient hotset")
	}
	if canary.Equals(canaryRecipientAddress(7, secondEpoch)) {
		t.Fatal("different run epochs share a canary address")
	}
}

func TestDeliveredTransfersUsesDestinationIncrease(t *testing.T) {
	tests := []struct {
		name      string
		before    int64
		after     int64
		amount    int64
		submitted int
		want      int
		wantError string
	}{
		{name: "bounce restores source but destination stays unchanged", before: 50, after: 50, amount: 10, submitted: 3, want: 0},
		{name: "partial destination delivery", before: 50, after: 70, amount: 10, submitted: 3, want: 2},
		{name: "all delivered", before: 50, after: 80, amount: 10, submitted: 3, want: 3},
		{name: "ambiguous units", before: 50, after: 71, amount: 10, submitted: 3, wantError: "not divisible"},
		{name: "more than submitted", before: 50, after: 90, amount: 10, submitted: 3, wantError: "only 3 were submitted"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := deliveredTransfers(
				big.NewInt(test.before),
				big.NewInt(test.after),
				big.NewInt(test.amount),
				test.submitted,
			)
			if test.wantError == "" {
				if err != nil {
					t.Fatal(err)
				}
				if got != test.want {
					t.Fatalf("delivered = %d, want %d", got, test.want)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("delivered error = %v, want containing %q", err, test.wantError)
			}
		})
	}
}

func TestWriteCommandResultEmitsOneVersionedJSONObject(t *testing.T) {
	var output bytes.Buffer
	want := newCommandResult("run", 9)
	want.ContractProfile = defaultContractProfile
	want.Submitted = 10
	want.Accepted = 10

	if err := writeCommandResult(&output, want); err != nil {
		t.Fatal(err)
	}
	if strings.Count(output.String(), "\n") != 1 {
		t.Fatalf("result output = %q, want one JSON line", output.String())
	}
	var got commandResult
	if err := json.Unmarshal(output.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("decoded result = %+v, want %+v", got, want)
	}
}

func TestInspectCommandResultUsesCurrentBalanceFields(t *testing.T) {
	var output bytes.Buffer
	want := newCommandResult("inspect", 9)
	want.SourceBalanceCurrent = "123456"
	want.RecipientBalanceCurrent = "654321"
	want.RunEpoch = "00112233445566778899aabbccddeeff"

	if err := writeCommandResult(&output, want); err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(output.Bytes(), &fields); err != nil {
		t.Fatal(err)
	}
	if got := string(fields["source_balance_current"]); got != `"123456"` {
		t.Fatalf("source_balance_current = %s", got)
	}
	if got := string(fields["recipient_balance_current"]); got != `"654321"` {
		t.Fatalf("recipient_balance_current = %s", got)
	}
	if got := string(fields["run_epoch"]); got != `"00112233445566778899aabbccddeeff"` {
		t.Fatalf("run_epoch = %s", got)
	}
	for _, field := range []string{
		"source_balance_before",
		"source_balance_after",
		"recipient_balance_before",
		"recipient_balance_after",
	} {
		if _, exists := fields[field]; exists {
			t.Fatalf("inspect result unexpectedly contains %q", field)
		}
	}
	if want.SchemaVersion != resultSchemaVersion || resultSchemaVersion != 1 {
		t.Fatalf("result schema version = %d, want backwards-compatible v1", want.SchemaVersion)
	}
}

func TestInspectCommandResultOmitsUnrequestedRecipientFields(t *testing.T) {
	var output bytes.Buffer
	result := newCommandResult("inspect", 9)
	result.SourceBalanceCurrent = "123456"

	if err := writeCommandResult(&output, result); err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(output.Bytes(), &fields); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"recipient_balance_current", "run_epoch"} {
		if _, exists := fields[field]; exists {
			t.Fatalf("source-only inspect result unexpectedly contains %q", field)
		}
	}
}

func TestNormalizeRunCountersCompletesPartialResult(t *testing.T) {
	want := newCommandResult("run", 9)
	want.Submitted = 10
	want.Accepted = 3

	if err := normalizeRunCounters(&want); err != nil {
		t.Fatal(err)
	}

	var output bytes.Buffer
	if err := writeCommandResult(&output, want); err != nil {
		t.Fatal(err)
	}
	var got commandResult
	if err := json.Unmarshal(output.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Undelivered != 7 || got.Submitted != got.Accepted+got.Undelivered {
		t.Fatalf(
			"JSON counters = submitted %d, accepted %d, undelivered %d",
			got.Submitted,
			got.Accepted,
			got.Undelivered,
		)
	}
}

func TestNormalizeRunCountersRejectsAcceptedAboveSubmitted(t *testing.T) {
	result := newCommandResult("run", 9)
	result.Submitted = 10
	result.Accepted = 11
	result.Undelivered = 4

	err := normalizeRunCounters(&result)
	if err == nil || !strings.Contains(err.Error(), "exceeds submitted") {
		t.Fatalf("normalize error = %v, want accepted-above-submitted error", err)
	}
	if result.Accepted != 11 || result.Submitted != 10 || result.Undelivered != 4 {
		t.Fatalf("invalid counters were masked: %+v", result)
	}
}

func TestRunLoadTimeoutIncludesBothDrains(t *testing.T) {
	opts := runOptions{duration: 30 * time.Second, drain: 40 * time.Second}
	want := defaultSetupTimeout + opts.duration + 2*opts.drain

	if got := runLoadTimeout(opts); got != want {
		t.Fatalf("run timeout = %s, want %s", got, want)
	}
}

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
	if defaults.contractProfile != defaultContractProfile {
		t.Fatalf("default contract profile = %q, want %q", defaults.contractProfile, defaultContractProfile)
	}
	if defaults.minterCode != "" || defaults.walletCode != "" {
		t.Fatalf("default setup uses mutable artifacts: minter=%q wallet=%q", defaults.minterCode, defaults.walletCode)
	}
	if defaults.mintJetton != "1000000000000000000" {
		t.Fatalf("default mint units = %q", defaults.mintJetton)
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
	if runDefaults.jettons != "1000000" {
		t.Fatalf("default transfer units = %q, want 1000000", runDefaults.jettons)
	}
	if _, err = parseRunOptions([]string{"--drain", "0s"}); err == nil || !strings.Contains(err.Error(), "destination confirmation") {
		t.Fatalf("zero drain error = %v", err)
	}

	maximum, err := parseSetupOptions([]string{"--sender-index", "18446744073709551615"})
	if err != nil {
		t.Fatal(err)
	}
	if maximum.senderIndex != ^uint64(0) {
		t.Fatalf("maximum sender index = %d, want %d", maximum.senderIndex, ^uint64(0))
	}
}

func TestInspectOptions(t *testing.T) {
	defaults, err := parseInspectOptions(nil)
	if err != nil {
		t.Fatal(err)
	}
	if defaults.timeout != defaultInspectTimeout {
		t.Fatalf("default inspect timeout = %s, want %s", defaults.timeout, defaultInspectTimeout)
	}
	if defaults.contractProfile != defaultContractProfile {
		t.Fatalf("default inspect profile = %q, want %q", defaults.contractProfile, defaultContractProfile)
	}
	if defaults.fundingKey != "" {
		t.Fatalf("source-only inspect unexpectedly requires a funding key")
	}
	if defaults.runEpoch != nil || defaults.recipients != 0 {
		t.Fatalf("default inspect unexpectedly requests recipients: %+v", defaults)
	}

	wantEpoch := "00112233445566778899AABBCCDDEEFF"
	withRecipients, err := parseInspectOptions([]string{
		"--sender-index", "29",
		"--timeout", "45s",
		"--run-epoch", wantEpoch,
		"--recipients", "256",
	})
	if err != nil {
		t.Fatal(err)
	}
	if withRecipients.senderIndex != 29 || withRecipients.timeout != 45*time.Second {
		t.Fatalf("inspect common options = %+v", withRecipients)
	}
	if withRecipients.runEpoch == nil || formatLoadRunEpoch(*withRecipients.runEpoch) != strings.ToLower(wantEpoch) {
		t.Fatalf("inspect run epoch = %v, want %s", withRecipients.runEpoch, strings.ToLower(wantEpoch))
	}
	if withRecipients.recipients != 256 {
		t.Fatalf("inspect recipients = %d, want 256", withRecipients.recipients)
	}

	tests := []struct {
		name      string
		args      []string
		wantError string
	}{
		{name: "zero timeout", args: []string{"--timeout", "0s"}, wantError: "inspect timeout"},
		{name: "timeout above bound", args: []string{"--timeout", (maximumInspectTimeout + time.Nanosecond).String()}, wantError: "inspect timeout"},
		{name: "recipients without epoch", args: []string{"--recipients", "1"}, wantError: "requires run-epoch"},
		{name: "epoch without recipients", args: []string{"--run-epoch", wantEpoch}, wantError: "recipients must be positive"},
		{name: "malformed epoch", args: []string{"--run-epoch", "not-hex", "--recipients", "1"}, wantError: "parse run-epoch"},
		{name: "short epoch", args: []string{"--run-epoch", "0011", "--recipients", "1"}, wantError: "expected 16"},
		{name: "unexpected argument", args: []string{"extra"}, wantError: "unexpected inspect arguments"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, parseErr := parseInspectOptions(test.args)
			if parseErr == nil || !strings.Contains(parseErr.Error(), test.wantError) {
				t.Fatalf("parse inspect error = %v, want containing %q", parseErr, test.wantError)
			}
		})
	}
}

func TestConnectReadOnlyDoesNotValidateFundingKey(t *testing.T) {
	configPath := t.TempDir() + "/config.json"
	if err := os.WriteFile(configPath, []byte(`{"liteserver":{"key":"AQ=="}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	opts := commonOptions{
		nodeConfig: configPath,
		fundingKey: "not-a-base64-funding-key",
	}

	_, err := connectReadOnly(t.Context(), opts)
	if err == nil || !strings.Contains(err.Error(), "liteserver key has 1 bytes") {
		t.Fatalf("read-only connect error = %v, want liteserver key validation", err)
	}
	if strings.Contains(err.Error(), "funding") {
		t.Fatalf("read-only connect inspected funding key: %v", err)
	}
}

func TestInspectLoadRejectsInvalidStateBeforeNetwork(t *testing.T) {
	validState := loadState{
		FormatVersion:      stateFormatVersion,
		SenderIndex:        7,
		ContractProfile:    defaultContractProfile,
		MinterCodeHash:     legacyMinterCodeHash,
		WalletCodeHash:     legacyWalletCodeHash,
		Minter:             "minter",
		HighloadWallet:     "highload",
		SourceJettonWallet: "source",
	}
	tests := []struct {
		name             string
		state            loadState
		requestedSender  uint64
		requestedProfile string
		wantError        string
		wantOutcome      string
		wantStage        string
	}{
		{
			name:             "different sender",
			state:            validState,
			requestedSender:  8,
			requestedProfile: defaultContractProfile,
			wantError:        "state sender index 7 differs",
			wantOutcome:      resultOutcomeComplete,
		},
		{
			name: "different stored profile",
			state: func() loadState {
				state := validState
				state.ContractProfile = "different-profile"
				return state
			}(),
			requestedSender:  7,
			requestedProfile: defaultContractProfile,
			wantError:        "state contract profile",
			wantOutcome:      resultOutcomeWorkloadInvalid,
			wantStage:        "contract_profile",
		},
		{
			name:             "unknown requested profile",
			state:            validState,
			requestedSender:  7,
			requestedProfile: "unknown-profile",
			wantError:        "unknown contract profile",
			wantOutcome:      resultOutcomeComplete,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			statePath := t.TempDir() + "/sender.state.json"
			if err := writeState(statePath, test.state); err != nil {
				t.Fatal(err)
			}
			opts := inspectOptions{
				commonOptions: commonOptions{
					nodeConfig:      t.TempDir() + "/must-not-be-read.json",
					statePath:       statePath,
					senderIndex:     test.requestedSender,
					contractProfile: test.requestedProfile,
				},
				timeout: defaultInspectTimeout,
			}
			result := newCommandResult("inspect", opts.senderIndex)

			err := inspectLoad(opts, &result)
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("inspect error = %v, want containing %q", err, test.wantError)
			}
			if result.Outcome != test.wantOutcome || result.FailureStage != test.wantStage {
				t.Fatalf("inspect result outcome/stage = %q/%q, want %q/%q", result.Outcome, result.FailureStage, test.wantOutcome, test.wantStage)
			}
		})
	}
}

func TestInspectLoadDoesNotRewriteStateWhenConnectFails(t *testing.T) {
	statePath := t.TempDir() + "/sender.state.json"
	state := loadState{
		FormatVersion:             stateFormatVersion,
		SenderIndex:               7,
		ContractProfile:           defaultContractProfile,
		MinterCodeHash:            legacyMinterCodeHash,
		WalletCodeHash:            legacyWalletCodeHash,
		Minter:                    "minter",
		HighloadWallet:            "highload",
		SourceJettonWallet:        "source",
		NextHighloadQueryID:       12345,
		HighloadQueryIDReservedAt: 1_900_000_000,
		HighloadMessageTTL:        highloadTTL,
	}
	if err := writeState(statePath, state); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	opts := inspectOptions{
		commonOptions: commonOptions{
			nodeConfig:      t.TempDir() + "/missing-config.json",
			statePath:       statePath,
			senderIndex:     state.SenderIndex,
			contractProfile: defaultContractProfile,
		},
		timeout: defaultInspectTimeout,
	}
	result := newCommandResult("inspect", opts.senderIndex)

	err = inspectLoad(opts, &result)
	if err == nil || !strings.Contains(err.Error(), "load node config") {
		t.Fatalf("inspect error = %v, want node config error", err)
	}
	after, readErr := os.ReadFile(statePath)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if !bytes.Equal(after, before) {
		t.Fatalf("inspect rewrote state:\nbefore: %s\nafter: %s", before, after)
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

func TestValidateStateContractRejectsProfileAndHashMismatch(t *testing.T) {
	profile, err := resolveContractProfile(defaultContractProfile, "", "")
	if err != nil {
		t.Fatal(err)
	}
	valid := loadState{
		ContractProfile: defaultContractProfile,
		MinterCodeHash:  legacyMinterCodeHash,
		WalletCodeHash:  legacyWalletCodeHash,
	}
	if err = validateStateContract(valid, profile); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name      string
		state     loadState
		wantError string
	}{
		{
			name:      "missing profile from v4 state",
			state:     loadState{MinterCodeHash: legacyMinterCodeHash, WalletCodeHash: legacyWalletCodeHash},
			wantError: "state contract profile",
		},
		{
			name: "unknown minter hash",
			state: loadState{
				ContractProfile: defaultContractProfile,
				MinterCodeHash:  "unknown",
				WalletCodeHash:  legacyWalletCodeHash,
			},
			wantError: "state minter code hash",
		},
		{
			name: "unknown wallet hash",
			state: loadState{
				ContractProfile: defaultContractProfile,
				MinterCodeHash:  legacyMinterCodeHash,
				WalletCodeHash:  "unknown",
			},
			wantError: "state wallet code hash",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateStateContract(test.state, profile)
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
		ContractProfile:     defaultContractProfile,
		MinterCodeHash:      legacyMinterCodeHash,
		WalletCodeHash:      legacyWalletCodeHash,
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

func TestHighloadCreatedAtProviderRefreshesPeriodically(t *testing.T) {
	if highloadTimeRefresh >= highloadTTL*time.Second/3 {
		t.Fatalf("refresh period %s is not substantially below TTL %s", highloadTimeRefresh, highloadTTL*time.Second)
	}

	master := ton.BlockIDExt{Workchain: -1, Shard: -1 << 63, SeqNo: 100}
	shard := ton.BlockIDExt{Workchain: 0, Shard: -1 << 62, SeqNo: 300}
	api := &highloadTimeTestAPI{
		master:  &master,
		shards:  []*ton.BlockIDExt{&shard},
		headers: map[uint32]uint32{shard.SeqNo: 1_900_000_200},
	}
	data := make([]byte, 32)
	data[0] = 0xd0
	provider := highloadCreatedAtProvider{
		api:          api,
		addr:         address.NewAddress(0, 0, data),
		refreshEvery: highloadTimeRefresh,
	}
	now := time.Unix(1_900_000_000, 0)

	first, err := provider.current(t.Context(), now)
	if err != nil {
		t.Fatal(err)
	}
	api.headers[shard.SeqNo] = 1_900_000_300
	cached, err := provider.current(t.Context(), now.Add(highloadTimeRefresh-time.Nanosecond))
	if err != nil {
		t.Fatal(err)
	}
	refreshed, err := provider.current(t.Context(), now.Add(highloadTimeRefresh))
	if err != nil {
		t.Fatal(err)
	}

	if first != 1_900_000_199 || cached != first || refreshed != 1_900_000_299 {
		t.Fatalf("created_at values = %d, %d, %d", first, cached, refreshed)
	}
	if api.headerRequests != 2 {
		t.Fatalf("header requests = %d, want one initial request and one periodic refresh", api.headerRequests)
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

func TestReadStateMigratesV4ButRequiresProfileSetup(t *testing.T) {
	path := t.TempDir() + "/sender.state.json"
	unprofiled := loadState{
		FormatVersion:       unprofiledStateFormat,
		SenderIndex:         7,
		Minter:              "minter",
		HighloadWallet:      "highload",
		SourceJettonWallet:  "source",
		NextHighloadQueryID: 42,
		HighloadMessageTTL:  highloadTTL,
	}
	if err := writeState(path, unprofiled); err != nil {
		t.Fatal(err)
	}

	state, err := readState(path)
	if err != nil {
		t.Fatal(err)
	}
	if state.FormatVersion != stateFormatVersion {
		t.Fatalf("migrated format = %d, want %d", state.FormatVersion, stateFormatVersion)
	}
	profile, err := resolveContractProfile(defaultContractProfile, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if err = validateStateContract(state, profile); err == nil || !strings.Contains(err.Error(), "state contract profile") {
		t.Fatalf("validate migrated v4 state error = %v", err)
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

// TestWaitRecipientsSettledExitsOnDelivery pins the drain as an upper bound
// rather than a fixed cost. The lab sizes it for the worst case — ten minutes
// against a ninety-second load — and paying it in full leaves the node under
// test idle for that whole span and pushes every later phase of the scenario
// back by it.
func TestWaitRecipientsSettledExitsOnDelivery(t *testing.T) {
	amount := big.NewInt(100)
	before := big.NewInt(0)

	polls := 0
	started := time.Now()
	after, err := waitRecipientsSettled(
		context.Background(),
		func(context.Context) (*big.Int, error) {
			polls++
			if polls < 3 {
				return big.NewInt(int64(polls) * 100), nil
			}

			return big.NewInt(500), nil
		},
		before,
		amount,
		5,
		time.Minute,
		time.Millisecond,
	)
	if err != nil {
		t.Fatal(err)
	}
	if after.Cmp(big.NewInt(500)) != 0 {
		t.Fatalf("settled aggregate = %s, want 500", after)
	}
	if polls != 3 {
		t.Fatalf("snapshots taken = %d, want the wait to end on the third", polls)
	}
	if elapsed := time.Since(started); elapsed > 10*time.Second {
		t.Fatalf("settled wait took %s; the drain is an upper bound, not a fixed delay", elapsed)
	}
}

// A run that does not settle reports the balances it did observe. The caller
// turns them into "so many of so many transfers arrived", which says more than
// "the wait expired" — and a failed reading must not become that verdict either.
func TestWaitRecipientsSettledReportsTheLastSnapshotOnTimeout(t *testing.T) {
	polls := 0
	after, err := waitRecipientsSettled(
		context.Background(),
		func(context.Context) (*big.Int, error) {
			polls++
			if polls == 1 {
				return nil, errors.New("liteserver read failed")
			}

			return big.NewInt(200), nil
		},
		big.NewInt(0),
		big.NewInt(100),
		5,
		20*time.Millisecond,
		time.Millisecond,
	)
	if err != nil {
		t.Fatalf("an unsettled drain is not an error: %v", err)
	}
	if after.Cmp(big.NewInt(200)) != 0 {
		t.Fatalf("reported aggregate = %s, want the last successful snapshot 200", after)
	}
	if delivered, countErr := deliveredTransfers(big.NewInt(0), after, big.NewInt(100), 5); countErr != nil ||
		delivered != 2 {
		t.Fatalf("caller verdict = %d, %v; want 2 of 5 delivered", delivered, countErr)
	}
}
