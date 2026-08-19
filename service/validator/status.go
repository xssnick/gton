package validator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/xssnick/gton/console"
	"github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/msgpool"
	"github.com/xssnick/tonutils-go/ton"
)

// InternalsStatus reports whether internal-message tracking is enabled and,
// when enabled, its current immutable counter snapshot.
type InternalsStatus struct {
	Enabled bool
	Stats   msgpool.InternalsStats
}

// GroupTrackerStatus is the display projection of the latest validator-group
// snapshot. Not initialized is a normal state before a masterchain view has
// been loaded.
type GroupTrackerStatus struct {
	Enabled                  bool
	Initialized              bool
	MasterchainBlock         ton.BlockIDExt
	Ready                    bool
	LifecycleEnabled         bool
	Active                   int
	Future                   int
	PersistentOverlayMembers int
	Local                    []LocalGroupStatus
}

// LocalGroupStatus is one active or tentative group in which a configured
// local signing key appears in the validator roster. It is available even
// before a concrete consensus runtime is wired.
type LocalGroupStatus struct {
	SessionID     [32]byte
	Shard         groups.ShardID
	CatchainSeqno uint32
	LocalIndex    int
	Active        bool
}

// StatusSnapshot is one self-contained validator runtime and storage view.
type StatusSnapshot struct {
	Started    bool
	Closed     bool
	External   msgpool.Stats
	Internals  InternalsStatus
	Groups     GroupTrackerStatus
	Supervisor *SessionSupervisorStatus
	Collator   *collator.Status
	Storage    StorageStatus
}

// Status takes a consistent snapshot from each validator-owned component.
// Storage telemetry is merged into runtime sessions only after the supervisor
// has released its status lock.
func (s *Service) Status(ctx context.Context) (StatusSnapshot, error) {
	status, err := s.statusWithoutCollator(ctx)
	if err != nil {
		return StatusSnapshot{}, err
	}
	if s.localCollator != nil {
		collatorStatus, collatorErr := s.localCollator.Status(ctx)
		if collatorErr != nil {
			return StatusSnapshot{}, fmt.Errorf("read local collator status: %w", collatorErr)
		}
		status.Collator = &collatorStatus
	}

	return status, nil
}

// statusWithoutCollator reads components whose telemetry remains useful when
// a corrupt or legacy collator record prevents the strict collator Status
// method from completing. Public Status adds that component fail-fast; debug
// status reports its error separately without weakening storage validation.
func (s *Service) statusWithoutCollator(ctx context.Context) (StatusSnapshot, error) {
	s.lifecycleMu.Lock()
	started := s.started
	closed := s.closed
	s.lifecycleMu.Unlock()

	status := StatusSnapshot{
		Started:  started,
		Closed:   closed,
		External: s.pool.Stats(),
		Internals: InternalsStatus{
			Enabled: !s.opts.DisableInternals,
		},
		Groups: GroupTrackerStatus{
			Enabled: s.opts.EnableGroups,
		},
	}
	if status.Internals.Enabled {
		status.Internals.Stats = s.pool.Internals().Stats()
	}

	groupSnapshot, err := s.tracker.Snapshot()
	if err == nil {
		status.Groups.Initialized = true
		status.Groups.MasterchainBlock = *groupSnapshot.MasterchainBlock.Copy()
		status.Groups.Ready = groupSnapshot.Ready
		status.Groups.LifecycleEnabled = groupSnapshot.LifecycleEnabled
		status.Groups.Active = len(groupSnapshot.Active)
		status.Groups.Future = len(groupSnapshot.Future)
		status.Groups.PersistentOverlayMembers = len(groupSnapshot.PersistentOverlay)
		status.Groups.Local, err = localGroupStatuses(groupSnapshot, s.opts.Keys.KeyIDs())
		if err != nil {
			return StatusSnapshot{}, err
		}
	} else if !errors.Is(err, groups.ErrNoSnapshot) {
		return StatusSnapshot{}, fmt.Errorf("read validator group status: %w", err)
	}

	if s.sessions != nil {
		supervisor := s.sessions.Status()
		status.Supervisor = &supervisor
	}
	storageStatus, err := s.validatorStore.Status(ctx)
	if err != nil {
		return StatusSnapshot{}, fmt.Errorf("read validator storage status: %w", err)
	}
	status.Storage = cloneStorageStatus(storageStatus)

	return status, nil
}

func cloneStorageStatus(status StorageStatus) StorageStatus {
	cloned := status
	cloned.Sessions = append([]StoredSession(nil), status.Sessions...)
	for i := range cloned.Sessions {
		session := &cloned.Sessions[i]
		if session.LastFinalized != nil {
			last := *session.LastFinalized
			session.LastFinalized = &last
		}
		if session.LastLeaderWindow != nil {
			last := *session.LastLeaderWindow
			session.LastLeaderWindow = &last
		}
	}

	return cloned
}

func (s *Service) handleStatus(ctx context.Context, args []string) (string, error) {
	if len(args) != 0 {
		return "", console.ErrNotFound
	}

	status, err := s.Status(ctx)
	if err != nil {
		return "", err
	}

	return formatStatus(status), nil
}

func formatStatus(status StatusSnapshot) string {
	var b strings.Builder

	fmt.Fprintln(&b, "Validator Status")
	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "Lifecycle")
	fmt.Fprintf(&b, "  started=%t closed=%t\n", status.Started, status.Closed)

	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "External Message Pool")
	fmt.Fprintf(
		&b,
		"  pooled=%d bytes=%s added=%d dedup=%d priority_bumps=%d expired=%d applied=%d\n",
		status.External.Pooled,
		formatStatusBytes(uint64(max(status.External.PooledBytes, 0))),
		status.External.Added,
		status.External.DedupSkipped,
		status.External.PriorityBumps,
		status.External.Expired,
		status.External.AppliedDeleted,
	)
	fmt.Fprintf(
		&b,
		"  overflow_pool=%d overflow_bytes=%d overflow_address=%d invalid=%d retry_delayed=%d retry_retried=%d retry_exhausted=%d retry_pressure=%d stale_feedback=%d applied_requested=%d\n",
		status.External.OverflowMempool,
		status.External.OverflowBytes,
		status.External.OverflowAddress,
		status.External.InvalidDeleted,
		status.External.RejectedDelayed,
		status.External.RejectedRetried,
		status.External.RejectedExhausted,
		status.External.RejectedPressure,
		status.External.StaleFeedback,
		status.External.AppliedRequested,
	)

	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "Internal Message Pool")
	if !status.Internals.Enabled {
		fmt.Fprintln(&b, "  disabled")
	} else {
		internals := status.Internals.Stats
		fmt.Fprintf(
			&b,
			"  destinations=%d sources=%d entries=%d removed=%d candidates=%d seeds=%d applied_blocks=%d applied_added=%d applied_removed=%d candidates_added=%d candidates_dropped=%d cuts=%d cut_messages=%d cut_failures=%d compactions=%d cut_stale_history=%d\n",
			internals.Destinations,
			internals.Sources,
			internals.Entries,
			internals.Removed,
			internals.Candidates,
			internals.Seeds,
			internals.AppliedBlocks,
			internals.AppliedAdded,
			internals.AppliedRemoved,
			internals.CandidatesAdded,
			internals.CandidatesDropped,
			internals.Cuts,
			internals.CutMessages,
			internals.CutFailures,
			internals.Compactions,
			internals.CutStaleHistory,
		)
	}

	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "Validator Groups")
	formatGroupStatus(&b, status.Groups)

	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "Consensus Sessions")
	formatSupervisorStatus(&b, status.Supervisor, status.Storage.Sessions)

	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "Local Collator")
	formatCollatorStatus(&b, status.Collator)

	fmt.Fprintln(&b)
	fmt.Fprintln(&b, "Validator Storage")
	formatStorageStatus(&b, status.Storage)

	return b.String()
}

func formatCollatorStatus(b *strings.Builder, status *collator.Status) {
	if status == nil {
		fmt.Fprintln(b, "  disabled")
		return
	}

	fmt.Fprintf(
		b,
		"  started=%t closing=%t closed=%t active_windows=%d retrying_windows=%d completed_windows=%d failed_windows=%d\n",
		status.Started,
		status.Closing,
		status.Closed,
		status.ActiveWindows,
		status.RetryingWindows,
		status.CompletedWindows,
		status.FailedWindows,
	)
	fmt.Fprintf(
		b,
		"  storage_sessions=%d candidates=%d pending_writes=%d\n",
		status.Storage.Sessions,
		status.Storage.Candidates,
		status.Storage.PendingWrites,
	)
	if status.LastError != "" {
		fmt.Fprintf(b, "  last_error=%s\n", status.LastError)
	}
	if !status.LastCompleted.IsZero() {
		fmt.Fprintf(b, "  last_completed=%s\n", formatStatusTime(status.LastCompleted))
	}
}

func formatGroupStatus(b *strings.Builder, status GroupTrackerStatus) {
	switch {
	case !status.Enabled:
		fmt.Fprintln(b, "  disabled")
	case !status.Initialized:
		fmt.Fprintln(b, "  not initialized")
	default:
		fmt.Fprintf(
			b,
			"  masterchain_seqno=%d ready=%t lifecycle_enabled=%t active=%d future=%d overlay_members=%d\n",
			status.MasterchainBlock.SeqNo,
			status.Ready,
			status.LifecycleEnabled,
			status.Active,
			status.Future,
			status.PersistentOverlayMembers,
		)
		for i := range status.Local {
			membership := &status.Local[i]
			fmt.Fprintf(
				b,
				"  local_session=%x local_index=%d shard=%d:%016x catchain=%d active=%t\n",
				membership.SessionID,
				membership.LocalIndex,
				membership.Shard.Workchain,
				uint64(membership.Shard.Shard),
				membership.CatchainSeqno,
				membership.Active,
			)
		}
	}
}

func localGroupStatuses(snapshot *groups.Snapshot, keyIDs [][32]byte) ([]LocalGroupStatus, error) {
	localKeys := make(map[[32]byte]struct{}, len(keyIDs))
	for _, keyID := range keyIDs {
		localKeys[keyID] = struct{}{}
	}

	result := make([]LocalGroupStatus, 0, len(snapshot.Active)+len(snapshot.Future))
	appendGroups := func(sessions []groups.Session, active bool) error {
		for i := range sessions {
			session := &sessions[i]
			localIndex, _, err := matchLocalValidator(*session, localKeys)
			if err != nil {
				return fmt.Errorf("read local validator group status: %w", err)
			}
			if localIndex < 0 {
				continue
			}

			result = append(result, LocalGroupStatus{
				SessionID:     session.ID,
				Shard:         session.Shard,
				CatchainSeqno: session.CatchainSeqno,
				LocalIndex:    localIndex,
				Active:        active,
			})
		}

		return nil
	}
	if err := appendGroups(snapshot.Active, true); err != nil {
		return nil, err
	}
	if err := appendGroups(snapshot.Future, false); err != nil {
		return nil, err
	}

	return result, nil
}

func formatSupervisorStatus(b *strings.Builder, status *SessionSupervisorStatus, stored []StoredSession) {
	if status == nil {
		fmt.Fprintln(b, "  disabled")
		return
	}

	fmt.Fprintf(
		b,
		"  started=%t closed=%t masterchain_seqno=%d desired=%d preparing=%d prepared=%d running=%d\n",
		status.Started,
		status.Closed,
		status.LatestMasterchainSeqno,
		status.Desired,
		status.Preparing,
		status.Prepared,
		status.Running,
	)
	if len(status.Sessions) == 0 {
		fmt.Fprintln(b, "  no runtime sessions")
		return
	}

	// Keyed by the namespace: a running session and its own stored rows differ
	// in the descriptor fields no namespace derivation reads whenever the
	// consensus config has moved since the session opened, and matching on the
	// whole descriptor would then report no durable telemetry at all.
	storageByNamespace := make(map[sessionNamespaceKey]StoredSession, len(stored))
	for i := range stored {
		if key, err := sessionNamespaceKeyOf(stored[i].ID); err == nil {
			storageByNamespace[key] = stored[i]
		}
	}
	for i := range status.Sessions {
		session := &status.Sessions[i]
		role := "observer"
		if session.StorageID.IsValidator {
			role = "validator"
		}
		fmt.Fprintf(
			b,
			"  session=%x adnl=%x local_index=%d role=%s shard=%d:%016x catchain=%d active=%t phase=%s",
			session.SessionID,
			session.ADNLID,
			session.LocalIndex,
			role,
			session.Shard.Workchain,
			uint64(session.Shard.Shard),
			session.CatchainSeqno,
			session.Active,
			session.Phase,
		)
		if !session.RetryAt.IsZero() {
			fmt.Fprintf(b, " retry_at=%s", formatStatusTime(session.RetryAt))
		}
		key, keyErr := sessionNamespaceKeyOf(session.StorageID)
		durable, exists := storageByNamespace[key]
		if keyErr == nil && exists {
			formatStoredSessionTelemetry(b, &durable)
		}
		fmt.Fprintln(b)
	}
}

func formatStorageStatus(b *strings.Builder, status StorageStatus) {
	db := status.DB
	fmt.Fprintf(
		b,
		"  pending_writes=%d sessions=%d disk=%s live=%s read_amp=%d l0=%d/%d debt=%s compactions=%d memtable=%s wal=%s\n",
		status.PendingWrites,
		len(status.Sessions),
		formatStatusBytes(db.DiskSize),
		formatStatusBytes(db.LiveSize),
		db.ReadAmp,
		db.L0Files,
		db.L0Sublevels,
		formatStatusBytes(db.CompactionDebt),
		db.CompactionsInProgress,
		formatStatusBytes(db.MemTableSize),
		formatStatusBytes(db.WALSize),
	)
	if len(status.Sessions) == 0 {
		fmt.Fprintln(b, "  no durable sessions")
		return
	}

	sessions := append([]StoredSession(nil), status.Sessions...)
	sort.Slice(sessions, func(i, j int) bool {
		return sessionStorageIDLess(sessions[i].ID, sessions[j].ID)
	})
	for i := range sessions {
		session := &sessions[i]
		role := "observer"
		if session.ID.IsValidator {
			role = "validator"
		}
		fmt.Fprintf(
			b,
			"  session=%x adnl=%x local_index=%d role=%s shard=%d:%016x catchain=%d",
			session.ID.SessionID,
			session.ID.LocalADNLID,
			session.ID.ValidatorIndex,
			role,
			session.ID.Shard.Workchain,
			uint64(session.ID.Shard.Shard),
			session.ID.CatchainSeqno,
		)
		formatStoredSessionTelemetry(b, session)
		fmt.Fprintln(b)
	}
}

func sessionStorageIDLess(left, right SessionStorageID) bool {
	if cmp := bytes.Compare(left.SessionID[:], right.SessionID[:]); cmp != 0 {
		return cmp < 0
	}
	if left.ValidatorIndex != right.ValidatorIndex {
		return left.ValidatorIndex < right.ValidatorIndex
	}
	if cmp := bytes.Compare(left.LocalADNLID[:], right.LocalADNLID[:]); cmp != 0 {
		return cmp < 0
	}
	if left.Shard.Workchain != right.Shard.Workchain {
		return left.Shard.Workchain < right.Shard.Workchain
	}
	if left.Shard.Shard != right.Shard.Shard {
		return left.Shard.Shard < right.Shard.Shard
	}
	if left.CatchainSeqno != right.CatchainSeqno {
		return left.CatchainSeqno < right.CatchainSeqno
	}
	if left.IsValidator != right.IsValidator {
		return !left.IsValidator
	}

	return bytes.Compare(left.ValidatorKeyID[:], right.ValidatorKeyID[:]) < 0
}

func formatStatusTime(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}

func formatStatusBytes(value uint64) string {
	units := []string{"B", "KiB", "MiB", "GiB", "TiB", "PiB"}
	size := float64(value)
	unit := 0
	for size >= 1024 && unit+1 < len(units) {
		size /= 1024
		unit++
	}
	if unit == 0 {
		return fmt.Sprintf("%dB", value)
	}
	if size >= 100 {
		return fmt.Sprintf("%.0f%s", size, units[unit])
	}

	return fmt.Sprintf("%.1f%s", size, units[unit])
}

// formatStoredSessionTelemetry writes the durable counters both session
// listings report. They read the same record, so the format lives in one place.
func formatStoredSessionTelemetry(b *strings.Builder, session *StoredSession) {
	fmt.Fprintf(
		b,
		" candidates=%d finalized=%d votes=%d certificates=%d leader_windows=%d first_non_announced_window=%d",
		session.Candidates,
		session.Finalized,
		session.Votes,
		session.Certificates,
		session.LeaderWindows,
		session.FirstNonAnnouncedWindow,
	)
	if session.LastFinalized != nil {
		fmt.Fprintf(b, " last_finalized_slot=%d", session.LastFinalized.Slot)
	}
	if session.LastLeaderWindow != nil {
		fmt.Fprintf(
			b,
			" last_leader_slots=[%d,%d) last_leader_at=%s",
			session.LastLeaderWindow.StartSlot,
			session.LastLeaderWindow.EndSlot,
			formatStatusTime(session.LastLeaderWindow.ObservedAt),
		)
	}
}
