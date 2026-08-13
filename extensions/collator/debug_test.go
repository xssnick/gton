package collator

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/xssnick/gton/console"
	"github.com/xssnick/gton/service/hooks"
	core "github.com/xssnick/gton/service/validator/collator"
)

func TestCollatorDebugStatusJSON(t *testing.T) {
	completed := time.Date(2026, time.August, 11, 4, 5, 6, 7, time.UTC)
	controller := &testController{status: core.ControllerStatus{
		Started:          true,
		ActiveSessions:   2,
		FutureSessions:   3,
		BackendSessions:  4,
		ObserverSessions: 5,
		Backend: core.Status{
			Started:          true,
			ActiveWindows:    6,
			RetryingWindows:  7,
			CompletedWindows: 8,
			FailedWindows:    9,
			LastError:        "candidate failed",
			LastCompleted:    completed,
			Storage: core.StorageStatus{
				Sessions:      10,
				Candidates:    13,
				PendingWrites: 14,
				DB: core.DBMetrics{
					DiskSize:              15,
					LiveSize:              16,
					ReadAmp:               17,
					L0Files:               18,
					L0Sublevels:           19,
					CompactionDebt:        20,
					CompactionsInProgress: 21,
					MemTableSize:          22,
					WALSize:               23,
				},
			},
		},
	}}
	var commands console.Registry
	_, err := New(testExtensionOptions(t, Options{
		Controller: controller,
		ShardTops:  &testShardTopSink{},
	}))(hooks.Node{Store: &testNodeStore{}, Commands: &commands})
	if err != nil {
		t.Fatal(err)
	}

	output, err := commands.Execute(context.Background(), "DeBuG CoLlAtOr StAtUs --json")
	if err != nil {
		t.Fatal(err)
	}
	var status collatorDebugStatus
	if err = json.Unmarshal([]byte(output), &status); err != nil {
		t.Fatalf("decode collator debug status: %v\n%s", err, output)
	}
	if status.Schema != collatorDebugStatusSchema || status.Mode != "standalone" ||
		status.Controller.ActiveSessions != 2 ||
		status.Production.CompletedWindows != 8 ||
		status.Production.LastCompleted != "2026-08-11T04:05:06.000000007Z" ||
		status.Storage.Candidates != 13 || status.Storage.DB.WALSize != 23 {
		t.Fatalf("collator debug status = %+v", status)
	}

	if _, err = commands.Execute(context.Background(), "debug collator status"); !errors.Is(err, console.ErrNotFound) {
		t.Fatalf("status without JSON flag error = %v, want ErrNotFound", err)
	}
}

func TestCollatorDebugStatusDegradesOnControllerStatusError(t *testing.T) {
	statusErr := errors.New("invalid collator candidate record version: " + strings.Repeat("x", 700))
	controller := &testController{statusErr: statusErr}
	var commands console.Registry
	_, err := New(testExtensionOptions(t, Options{
		Controller: controller,
		ShardTops:  &testShardTopSink{},
	}))(hooks.Node{Store: &testNodeStore{}, Commands: &commands})
	if err != nil {
		t.Fatal(err)
	}

	output, err := commands.Execute(context.Background(), "debug collator status --json")
	if err != nil {
		t.Fatal(err)
	}
	var status collatorDebugStatus
	if err = json.Unmarshal([]byte(output), &status); err != nil {
		t.Fatalf("decode degraded collator status: %v\n%s", err, output)
	}
	if !strings.Contains(status.StatusError, "invalid collator candidate") ||
		len([]rune(status.StatusError)) != maxDebugStatusErrorRunes+1 {
		t.Fatalf("bounded collator status error = %q", status.StatusError)
	}

	if _, err = commands.Execute(context.Background(), "status collator"); !errors.Is(err, statusErr) {
		t.Fatalf("strict collator status error = %v, want %v", err, statusErr)
	}
}

func TestFactoryFailsOnCollatorDebugCommandConflict(t *testing.T) {
	var commands console.Registry
	if err := commands.Register("debug collator", func(context.Context, []string) (string, error) {
		return "existing", nil
	}); err != nil {
		t.Fatal(err)
	}

	_, err := New(testExtensionOptions(t, Options{
		Controller: &testController{},
		ShardTops:  &testShardTopSink{},
	}))(hooks.Node{Store: &testNodeStore{}, Commands: &commands})
	if err == nil || !strings.Contains(err.Error(), "register collator debug command") {
		t.Fatalf("factory command conflict error = %v", err)
	}
}
