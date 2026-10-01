package gton

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/xssnick/gton/console"
	service2 "github.com/xssnick/gton/service"
	"github.com/xssnick/gton/service/storage/pebblestore"
)

const (
	consoleCommandCancelSerialization = "cancel serialization"
	consoleCommandStartSerialization  = "start serialization"
	consoleCommandStopMigration       = "stop migration"
	consoleCommandStartMigration      = "start migration"
)

type consoleCommandCall struct {
	name  string
	ctx   context.Context
	seqno uint32
	scope service2.PersistentStateSerializationScope
}

type fakeConsoleStateLifecycleCommands struct {
	calls []consoleCommandCall

	cancelSerializationErr error
	startSerializationErr  error
	stopMigrationErr       error
	startMigrationErr      error
}

type seqnoParseTest struct {
	name     string
	value    string
	expected uint32
	wantErr  bool
}

var _ consoleStateLifecycleCommands = (*fakeConsoleStateLifecycleCommands)(nil)

func (f *fakeConsoleStateLifecycleCommands) CancelPersistentStateSerialization(ctx context.Context) error {
	f.calls = append(f.calls, consoleCommandCall{name: consoleCommandCancelSerialization, ctx: ctx})

	return f.cancelSerializationErr
}

func (f *fakeConsoleStateLifecycleCommands) StartPersistentStateSerialization(
	ctx context.Context,
	seqno uint32,
	scope service2.PersistentStateSerializationScope,
) error {
	f.calls = append(f.calls, consoleCommandCall{
		name:  consoleCommandStartSerialization,
		ctx:   ctx,
		seqno: seqno,
		scope: scope,
	})

	return f.startSerializationErr
}

func (f *fakeConsoleStateLifecycleCommands) StopCellGenerationMigration(ctx context.Context) error {
	f.calls = append(f.calls, consoleCommandCall{name: consoleCommandStopMigration, ctx: ctx})

	return f.stopMigrationErr
}

func (f *fakeConsoleStateLifecycleCommands) StartCellGenerationMigration(
	ctx context.Context,
	seqno uint32,
) error {
	f.calls = append(f.calls, consoleCommandCall{
		name:  consoleCommandStartMigration,
		ctx:   ctx,
		seqno: seqno,
	})

	return f.startMigrationErr
}

func TestParseMasterchainSeqno(t *testing.T) {
	tests := []seqnoParseTest{
		{name: "zero", value: "0", expected: 0},
		{name: "positive", value: "123456", expected: 123456},
		{name: "maximum", value: "4294967295", expected: ^uint32(0)},
		{name: "overflow", value: "4294967296", wantErr: true},
		{name: "negative", value: "-1", wantErr: true},
		{name: "not a number", value: "latest", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual, err := parseMasterchainSeqno(test.value)
			if test.wantErr {
				if err == nil {
					t.Fatalf("parseMasterchainSeqno(%q) error = nil, want error", test.value)
				}

				return
			}
			if err != nil {
				t.Fatalf("parseMasterchainSeqno(%q): %v", test.value, err)
			}
			if actual != test.expected {
				t.Fatalf("parseMasterchainSeqno(%q) = %d, want %d", test.value, actual, test.expected)
			}
		})
	}
}

func TestRegisterConsoleCommandsDispatchesStatus(t *testing.T) {
	ctx := context.Background()
	var registry console.Registry
	commands := &fakeConsoleStateLifecycleCommands{}
	statusReads := 0
	dbStatusReads := 0

	err := registerConsoleCommands(
		&registry,
		func(received context.Context) service2.StatusSnapshot {
			if received != ctx {
				t.Fatalf("status context = %v, want command context", received)
			}
			statusReads++

			return service2.StatusSnapshot{}
		},
		commands,
		func(received context.Context) (pebblestore.DBStatus, error) {
			if received != ctx {
				t.Fatalf("db status context = %v, want command context", received)
			}
			dbStatusReads++

			return pebblestore.DBStatus{}, nil
		},
	)
	if err != nil {
		t.Fatalf("register console commands: %v", err)
	}

	status, err := registry.Execute(ctx, "status")
	if err != nil {
		t.Fatalf("execute status: %v", err)
	}
	fullStatus, err := registry.Execute(ctx, "STATUS FULL")
	if err != nil {
		t.Fatalf("execute status full: %v", err)
	}
	dbStatus, err := registry.Execute(ctx, "status DB")
	if err != nil {
		t.Fatalf("execute status db: %v", err)
	}

	if statusReads != 2 {
		t.Fatalf("status reads = %d, want 2", statusReads)
	}
	if dbStatusReads != 1 {
		t.Fatalf("db status reads = %d, want 1", dbStatusReads)
	}
	if len(commands.calls) != 0 {
		t.Fatalf("state command calls = %v, want none", commands.calls)
	}
	if !strings.Contains(status, "Status\n\nNode\n") {
		t.Fatalf("status output is missing header:\n%s", status)
	}
	if !strings.Contains(fullStatus, "Overlays\n") {
		t.Fatalf("full status output is missing overlays:\n%s", fullStatus)
	}
	if !strings.Contains(dbStatus, "DB Status\n") {
		t.Fatalf("db status output is missing header:\n%s", dbStatus)
	}
}

func TestRegisterConsoleCommandsDispatchesStateLifecycle(t *testing.T) {
	ctx := context.Background()
	var registry console.Registry
	commands := &fakeConsoleStateLifecycleCommands{}

	if err := registerConsoleCommands(
		&registry,
		func(context.Context) service2.StatusSnapshot {
			t.Fatal("unexpected status read")

			return service2.StatusSnapshot{}
		},
		commands,
		func(context.Context) (pebblestore.DBStatus, error) {
			t.Fatal("unexpected db status read")

			return pebblestore.DBStatus{}, nil
		},
	); err != nil {
		t.Fatalf("register console commands: %v", err)
	}

	lines := []string{
		" serialize 41 ",
		"SERIALIZE 42 BASECHAIN",
		"serialize cancel",
		"migrate 43",
		"MIGRATE STOP",
	}
	expectedOutputs := []string{
		"persistent state serialization started for masterchain seqno 41",
		"persistent basechain state serialization started for masterchain seqno 42",
		"persistent state serialization canceled",
		"cell generation migration started for masterchain seqno 43",
		"cell generation migration stopped",
	}
	for i, line := range lines {
		output, err := registry.Execute(ctx, line)
		if err != nil {
			t.Fatalf("execute %q: %v", line, err)
		}
		if output != expectedOutputs[i] {
			t.Fatalf("execute %q output = %q, want %q", line, output, expectedOutputs[i])
		}
	}

	expectedCalls := []consoleCommandCall{
		{name: consoleCommandStartSerialization, ctx: ctx, seqno: 41, scope: service2.PersistentStateSerializationAll},
		{name: consoleCommandStartSerialization, ctx: ctx, seqno: 42, scope: service2.PersistentStateSerializationBasechain},
		{name: consoleCommandCancelSerialization, ctx: ctx},
		{name: consoleCommandStartMigration, ctx: ctx, seqno: 43},
		{name: consoleCommandStopMigration, ctx: ctx},
	}
	if len(commands.calls) != len(expectedCalls) {
		t.Fatalf("state command calls = %v, want %v", commands.calls, expectedCalls)
	}
	for i, expected := range expectedCalls {
		actual := commands.calls[i]
		if actual.name != expected.name || actual.ctx != expected.ctx ||
			actual.seqno != expected.seqno || actual.scope != expected.scope {
			t.Fatalf("state command call %d = %+v, want %+v", i, actual, expected)
		}
	}
}

func TestRegisterConsoleCommandsRejectsInvalidCommands(t *testing.T) {
	var registry console.Registry
	commands := &fakeConsoleStateLifecycleCommands{}

	if err := registerConsoleCommands(
		&registry,
		func(context.Context) service2.StatusSnapshot {
			t.Fatal("unexpected status read")

			return service2.StatusSnapshot{}
		},
		commands,
		func(context.Context) (pebblestore.DBStatus, error) {
			t.Fatal("unexpected db status read")

			return pebblestore.DBStatus{}, nil
		},
	); err != nil {
		t.Fatalf("register console commands: %v", err)
	}

	invalid := []string{
		"status unknown",
		"status full extra",
		"status db extra",
		"serialize",
		"serialize 1 unknown",
		"serialize cancel extra",
		"migrate",
		"migrate 1 extra",
		"unknown",
	}
	for _, line := range invalid {
		if _, err := registry.Execute(context.Background(), line); !errors.Is(err, console.ErrNotFound) {
			t.Fatalf("execute %q error = %v, want ErrNotFound", line, err)
		}
	}
	for _, line := range []string{"serialize latest", "migrate latest"} {
		if _, err := registry.Execute(context.Background(), line); err == nil {
			t.Fatalf("execute %q error = nil, want error", line)
		}
	}

	if len(commands.calls) != 0 {
		t.Fatalf("invalid commands dispatched state calls: %v", commands.calls)
	}
}

func TestRegisterConsoleCommandsReturnsCommandErrors(t *testing.T) {
	commandErr := errors.New("command failed")
	commands := &fakeConsoleStateLifecycleCommands{
		cancelSerializationErr: commandErr,
		startSerializationErr:  commandErr,
		stopMigrationErr:       commandErr,
		startMigrationErr:      commandErr,
	}
	var registry console.Registry

	if err := registerConsoleCommands(
		&registry,
		func(context.Context) service2.StatusSnapshot {
			t.Fatal("unexpected status read")

			return service2.StatusSnapshot{}
		},
		commands,
		func(context.Context) (pebblestore.DBStatus, error) {
			return pebblestore.DBStatus{}, commandErr
		},
	); err != nil {
		t.Fatalf("register console commands: %v", err)
	}

	for _, line := range []string{
		"status db",
		"serialize 1",
		"serialize cancel",
		"migrate 2",
		"migrate stop",
	} {
		if _, err := registry.Execute(context.Background(), line); !errors.Is(err, commandErr) {
			t.Fatalf("execute %q error = %v, want %v", line, err, commandErr)
		}
	}
}

func TestRegisterConsoleCommandsRejectsDuplicateBuiltins(t *testing.T) {
	var registry console.Registry
	commands := &fakeConsoleStateLifecycleCommands{}
	readStatus := func(context.Context) service2.StatusSnapshot {
		return service2.StatusSnapshot{}
	}
	dbStatus := func(context.Context) (pebblestore.DBStatus, error) {
		return pebblestore.DBStatus{}, nil
	}

	if err := registerConsoleCommands(&registry, readStatus, commands, dbStatus); err != nil {
		t.Fatalf("register console commands: %v", err)
	}
	if err := registerConsoleCommands(&registry, readStatus, commands, dbStatus); err == nil {
		t.Fatal("duplicate built-in registration error = nil, want error")
	}
}

func TestRegisterConsoleCommandsRejectsEveryClaimedBuiltinPath(t *testing.T) {
	paths := []string{
		"status",
		"status full",
		"status db",
		"serialize",
		"serialize cancel",
		"migrate",
		"migrate stop",
	}

	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			var registry console.Registry
			if err := registry.Register(path, func(context.Context, []string) (string, error) {
				return "extension", nil
			}); err != nil {
				t.Fatal(err)
			}

			err := registerConsoleCommands(
				&registry,
				func(context.Context) service2.StatusSnapshot { return service2.StatusSnapshot{} },
				&fakeConsoleStateLifecycleCommands{},
				func(context.Context) (pebblestore.DBStatus, error) { return pebblestore.DBStatus{}, nil },
			)
			if err == nil {
				t.Fatalf("built-in path %q was shadowed without an error", path)
			}
		})
	}
}

func TestRunConsoleLogsCommandErrorsAndKeepsOutputScriptable(t *testing.T) {
	commandErr := errors.New("command failed")
	var registry console.Registry
	if err := registry.Register("ok", func(context.Context, []string) (string, error) {
		return "success", nil
	}); err != nil {
		t.Fatalf("register ok: %v", err)
	}
	if err := registry.Register("fail", func(context.Context, []string) (string, error) {
		return "", commandErr
	}); err != nil {
		t.Fatalf("register fail: %v", err)
	}

	var logs bytes.Buffer
	var output bytes.Buffer
	runConsole(
		context.Background(),
		zerolog.New(&logs),
		strings.NewReader("ok\nmissing ARG\nfail\nok\n"),
		&output,
		&registry,
	)

	if output.String() != "success\nsuccess\n" {
		t.Fatalf("console output = %q, want success output only", output.String())
	}
	if warnings := strings.Count(logs.String(), `"level":"warn"`); warnings != 2 {
		t.Fatalf("warning count = %d, want 2; logs: %s", warnings, logs.String())
	}
	if !strings.Contains(logs.String(), "unknown console command") {
		t.Fatalf("console logs missing unknown-command message: %s", logs.String())
	}
	if !strings.Contains(logs.String(), "console command failed") {
		t.Fatalf("console logs missing failed-command message: %s", logs.String())
	}
}
