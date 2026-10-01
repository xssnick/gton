package collator

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/xssnick/gton/console"
	core "github.com/xssnick/gton/service/validator/collator"
)

const (
	collatorDebugStatusSchema = "gton.collator.status.v1"
	maxDebugStatusErrorRunes  = 512
)

type collatorDebugStatus struct {
	Schema      string                  `json:"schema"`
	Mode        string                  `json:"mode"`
	StatusError string                  `json:"status_error,omitempty"`
	Controller  collatorDebugController `json:"controller"`
	Production  collatorDebugProduction `json:"production"`
	Storage     collatorDebugStorage    `json:"storage"`
}

type collatorDebugController struct {
	Started          bool `json:"started"`
	Closing          bool `json:"closing"`
	Closed           bool `json:"closed"`
	ActiveSessions   int  `json:"active_sessions"`
	FutureSessions   int  `json:"future_sessions"`
	BackendSessions  int  `json:"backend_sessions"`
	ObserverSessions int  `json:"observer_sessions"`
}

type collatorDebugProduction struct {
	Started          bool   `json:"started"`
	Closing          bool   `json:"closing"`
	Closed           bool   `json:"closed"`
	ActiveWindows    int    `json:"active_windows"`
	RetryingWindows  int    `json:"retrying_windows"`
	CompletedWindows uint64 `json:"completed_windows"`
	FailedWindows    uint64 `json:"failed_windows"`
	LastError        string `json:"last_error,omitempty"`
	LastCompleted    string `json:"last_completed,omitempty"`
}

type collatorDebugStorage struct {
	Sessions      uint64                 `json:"sessions"`
	Candidates    uint64                 `json:"candidates"`
	PendingWrites int                    `json:"pending_writes"`
	DB            collatorDebugDBMetrics `json:"db"`
}

type collatorDebugDBMetrics struct {
	DiskSize              uint64 `json:"disk_size"`
	LiveSize              uint64 `json:"live_size"`
	ReadAmp               int64  `json:"read_amp"`
	L0Files               int64  `json:"l0_files"`
	L0Sublevels           int64  `json:"l0_sublevels"`
	CompactionDebt        uint64 `json:"compaction_debt"`
	CompactionsInProgress int64  `json:"compactions_in_progress"`
	MemTableSize          uint64 `json:"memtable_size"`
	WALSize               uint64 `json:"wal_size"`
}

func (e *Extension) handleDebug(ctx context.Context, args []string) (string, error) {
	if len(args) != 2 || !strings.EqualFold(args[0], "status") || args[1] != "--json" {
		return "", console.ErrNotFound
	}

	status, statusErr := e.controller.Status(ctx)
	debug := makeCollatorDebugStatus(status)
	if statusErr != nil {
		debug.StatusError = boundedCollatorDebugError(statusErr)
	}
	encoded, err := json.Marshal(debug)
	if err != nil {
		return "", fmt.Errorf("marshal collator debug status: %w", err)
	}

	return string(encoded), nil
}

func makeCollatorDebugStatus(status core.ControllerStatus) collatorDebugStatus {
	backend := status.Backend
	storage := backend.Storage

	return collatorDebugStatus{
		Schema: collatorDebugStatusSchema,
		Mode:   "standalone",
		Controller: collatorDebugController{
			Started:          status.Started,
			Closing:          status.Closing,
			Closed:           status.Closed,
			ActiveSessions:   status.ActiveSessions,
			FutureSessions:   status.FutureSessions,
			BackendSessions:  status.BackendSessions,
			ObserverSessions: status.ObserverSessions,
		},
		Production: collatorDebugProduction{
			Started:          backend.Started,
			Closing:          backend.Closing,
			Closed:           backend.Closed,
			ActiveWindows:    backend.ActiveWindows,
			RetryingWindows:  backend.RetryingWindows,
			CompletedWindows: backend.CompletedWindows,
			FailedWindows:    backend.FailedWindows,
			LastError:        backend.LastError,
			LastCompleted:    debugStatusTime(backend.LastCompleted),
		},
		Storage: collatorDebugStorage{
			Sessions:      storage.Sessions,
			Candidates:    storage.Candidates,
			PendingWrites: storage.PendingWrites,
			DB: collatorDebugDBMetrics{
				DiskSize:              storage.DB.DiskSize,
				LiveSize:              storage.DB.LiveSize,
				ReadAmp:               storage.DB.ReadAmp,
				L0Files:               storage.DB.L0Files,
				L0Sublevels:           storage.DB.L0Sublevels,
				CompactionDebt:        storage.DB.CompactionDebt,
				CompactionsInProgress: storage.DB.CompactionsInProgress,
				MemTableSize:          storage.DB.MemTableSize,
				WALSize:               storage.DB.WALSize,
			},
		},
	}
}

func boundedCollatorDebugError(err error) string {
	message := []rune(err.Error())
	if len(message) <= maxDebugStatusErrorRunes {
		return string(message)
	}

	return string(message[:maxDebugStatusErrorRunes]) + "…"
}

func debugStatusTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}

	return value.UTC().Format(time.RFC3339Nano)
}
