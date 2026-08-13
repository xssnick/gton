package collator

import (
	"fmt"
	"strings"
	"time"

	core "github.com/xssnick/gton/service/validator/collator"
)

func formatStatus(status core.ControllerStatus) string {
	var b strings.Builder

	fmt.Fprintln(&b, "Collator Status")
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "Controller")
	fmt.Fprintf(
		&b,
		"  started=%t closing=%t closed=%t active_sessions=%d future_sessions=%d backend_sessions=%d observer_sessions=%d\n",
		status.Started,
		status.Closing,
		status.Closed,
		status.ActiveSessions,
		status.FutureSessions,
		status.BackendSessions,
		status.ObserverSessions,
	)

	backend := status.Backend
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "Production")
	fmt.Fprintf(
		&b,
		"  started=%t closing=%t closed=%t active_windows=%d retrying_windows=%d\n",
		backend.Started,
		backend.Closing,
		backend.Closed,
		backend.ActiveWindows,
		backend.RetryingWindows,
	)
	fmt.Fprintln(&b)
	fmt.Fprintf(
		&b,
		"  completed_windows=%d failed_windows=%d last_completed=%s last_error=%q\n",
		backend.CompletedWindows,
		backend.FailedWindows,
		formatTime(backend.LastCompleted),
		backend.LastError,
	)

	storage := backend.Storage
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "Collator Storage")
	fmt.Fprintf(
		&b,
		"  sessions=%d candidates=%d pending_writes=%d\n",
		storage.Sessions,
		storage.Candidates,
		storage.PendingWrites,
	)

	db := storage.DB
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "Pebble")
	fmt.Fprintf(
		&b,
		"  disk=%s live=%s wal=%s memtable=%s read_amp=%d l0_files=%d l0_sublevels=%d\n",
		formatBytes(db.DiskSize),
		formatBytes(db.LiveSize),
		formatBytes(db.WALSize),
		formatBytes(db.MemTableSize),
		db.ReadAmp,
		db.L0Files,
		db.L0Sublevels,
	)
	fmt.Fprintf(
		&b,
		"  compaction_debt=%s compactions_running=%d\n",
		formatBytes(db.CompactionDebt),
		db.CompactionsInProgress,
	)

	return b.String()
}

func formatTime(value time.Time) string {
	if value.IsZero() {
		return "-"
	}

	return value.UTC().Format(time.RFC3339Nano)
}

func formatBytes(value uint64) string {
	const unit = uint64(1024)
	if value < unit {
		return fmt.Sprintf("%dB", value)
	}

	divisor := unit
	exponent := 0
	for quotient := value / unit; quotient >= unit && exponent < 5; quotient /= unit {
		divisor *= unit
		exponent++
	}

	return fmt.Sprintf("%.1f%ciB", float64(value)/float64(divisor), "KMGTPE"[exponent])
}
