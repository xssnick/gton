package validator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/xssnick/gton/console"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/msgpool"
	"github.com/xssnick/gton/service/validator/simplex"
	"github.com/xssnick/tonutils-go/ton"
)

const (
	validatorDebugStatusSchema = "gton.validator.status.v1"
	collatorDebugStatusSchema  = "gton.collator.status.v1"
	candidateExportSchema      = "gton.validator.candidate-export.v1"
	maxCandidateExportBytes    = 64 << 20
	candidateExportFileMode    = 0o600
	maxDebugStatusErrorRunes   = 512
	// Validator storage can retain every historical consensus namespace. Debug
	// status is consumed over the node console, so retain a small, stable sample
	// instead of serializing that unbounded history.
	maxValidatorDebugStoredSessions = 32
	maxValidatorDebugStatusBytes    = 128 << 10
)

type validatorDebugStatus struct {
	Schema    string                   `json:"schema"`
	Lifecycle validatorDebugLifecycle  `json:"lifecycle"`
	Messages  validatorDebugMessages   `json:"messages"`
	Groups    validatorDebugGroups     `json:"groups"`
	Consensus *validatorDebugConsensus `json:"consensus"`
	Collator  validatorDebugCollator   `json:"collator"`
	Storage   validatorDebugStorage    `json:"storage"`
}

type validatorDebugLifecycle struct {
	Started bool `json:"started"`
	Closed  bool `json:"closed"`
}

type validatorDebugMessages struct {
	External  validatorDebugExternalMessages `json:"external"`
	Internals validatorDebugInternalMessages `json:"internals"`
}

type validatorDebugExternalMessages struct {
	Pooled            int    `json:"pooled"`
	PooledBytes       int64  `json:"pooled_bytes"`
	Added             uint64 `json:"added"`
	DedupSkipped      uint64 `json:"dedup_skipped"`
	PriorityBumps     uint64 `json:"priority_bumps"`
	OverflowPool      uint64 `json:"overflow_pool"`
	OverflowBytes     uint64 `json:"overflow_bytes"`
	OverflowAddress   uint64 `json:"overflow_address"`
	Expired           uint64 `json:"expired"`
	InvalidDeleted    uint64 `json:"invalid_deleted"`
	RejectedDelayed   uint64 `json:"rejected_delayed"`
	RejectedRetried   uint64 `json:"rejected_retried"`
	RejectedExhausted uint64 `json:"rejected_exhausted"`
	RejectedPressure  uint64 `json:"rejected_pressure"`
	StaleFeedback     uint64 `json:"stale_feedback"`
	AppliedRequested  uint64 `json:"applied_requested"`
	AppliedDeleted    uint64 `json:"applied_deleted"`
}

type validatorDebugInternalMessages struct {
	Enabled           bool   `json:"enabled"`
	Destinations      int    `json:"destinations"`
	Sources           int    `json:"sources"`
	Entries           int    `json:"entries"`
	Removed           int    `json:"removed"`
	Candidates        int    `json:"candidates"`
	Seeds             uint64 `json:"seeds"`
	AppliedBlocks     uint64 `json:"applied_blocks"`
	AppliedAdded      uint64 `json:"applied_added"`
	AppliedRemoved    uint64 `json:"applied_removed"`
	CandidatesAdded   uint64 `json:"candidates_added"`
	CandidatesDropped uint64 `json:"candidates_dropped"`
	Cuts              uint64 `json:"cuts"`
	CutMessages       uint64 `json:"cut_messages"`
	CutFailures       uint64 `json:"cut_failures"`
	Compactions       uint64 `json:"compactions"`
	CutStaleHistory   uint64 `json:"cut_stale_history"`
}

type validatorDebugGroups struct {
	Enabled                  bool                       `json:"enabled"`
	Initialized              bool                       `json:"initialized"`
	MasterchainBlock         *validatorDebugBlockID     `json:"masterchain_block"`
	Ready                    bool                       `json:"ready"`
	LifecycleEnabled         bool                       `json:"lifecycle_enabled"`
	Active                   int                        `json:"active"`
	Future                   int                        `json:"future"`
	PersistentOverlayMembers int                        `json:"persistent_overlay_members"`
	Local                    []validatorDebugLocalGroup `json:"local"`
}

type validatorDebugLocalGroup struct {
	SessionID     string              `json:"session_id"`
	Shard         validatorDebugShard `json:"shard"`
	CatchainSeqno uint32              `json:"catchain_seqno"`
	LocalIndex    int                 `json:"local_index"`
	Active        bool                `json:"active"`
}

type validatorDebugConsensus struct {
	Started                bool                           `json:"started"`
	Closed                 bool                           `json:"closed"`
	LatestMasterchainSeqno uint32                         `json:"latest_masterchain_seqno"`
	Desired                int                            `json:"desired"`
	Preparing              int                            `json:"preparing"`
	Prepared               int                            `json:"prepared"`
	Running                int                            `json:"running"`
	Sessions               []validatorDebugRuntimeSession `json:"sessions"`
}

type validatorDebugRuntimeSession struct {
	Namespace      string              `json:"namespace"`
	SessionID      string              `json:"session_id"`
	ADNLID         string              `json:"adnl_id"`
	ValidatorKeyID string              `json:"validator_key_id,omitempty"`
	LocalIndex     int                 `json:"local_index"`
	Role           string              `json:"role"`
	Shard          validatorDebugShard `json:"shard"`
	CatchainSeqno  uint32              `json:"catchain_seqno"`
	Active         bool                `json:"active"`
	Phase          SessionPhase        `json:"phase"`
	RetryAt        string              `json:"retry_at,omitempty"`
}

type validatorDebugCollator struct {
	Enabled     bool                             `json:"enabled"`
	StatusError string                           `json:"status_error,omitempty"`
	Production  validatorDebugCollatorProduction `json:"production"`
	Storage     validatorDebugStorageSummary     `json:"storage"`
}

type validatorDebugCollatorProduction struct {
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

type integratedCollatorDebugStatus struct {
	Schema      string                           `json:"schema"`
	Mode        string                           `json:"mode"`
	StatusError string                           `json:"status_error,omitempty"`
	Production  validatorDebugCollatorProduction `json:"production"`
	Storage     validatorDebugStorageSummary     `json:"storage"`
}

type validatorDebugStorage struct {
	PendingWrites     int                           `json:"pending_writes"`
	DB                validatorDebugDBMetrics       `json:"db"`
	SessionsTotal     uint64                        `json:"sessions_total"`
	SessionsTruncated bool                          `json:"sessions_truncated"`
	Sessions          []validatorDebugStoredSession `json:"sessions"`
}

type validatorDebugStorageSummary struct {
	Sessions      uint64                  `json:"sessions"`
	Candidates    uint64                  `json:"candidates"`
	PendingWrites int                     `json:"pending_writes"`
	DB            validatorDebugDBMetrics `json:"db"`
}

type validatorDebugStoredSession struct {
	Namespace               string                      `json:"namespace"`
	SessionID               string                      `json:"session_id"`
	ADNLID                  string                      `json:"adnl_id"`
	ValidatorKeyID          string                      `json:"validator_key_id,omitempty"`
	LocalIndex              int                         `json:"local_index"`
	Role                    string                      `json:"role"`
	Shard                   validatorDebugShard         `json:"shard"`
	CatchainSeqno           uint32                      `json:"catchain_seqno"`
	Protocol                validatorDebugProtocol      `json:"protocol"`
	Candidates              uint64                      `json:"candidates"`
	Finalized               uint64                      `json:"finalized"`
	Votes                   uint64                      `json:"votes"`
	Certificates            uint64                      `json:"certificates"`
	LeaderWindows           uint64                      `json:"leader_windows"`
	FirstNonAnnouncedWindow uint32                      `json:"first_non_announced_window"`
	LastFinalized           *validatorDebugCandidateID  `json:"last_finalized"`
	LastLeaderWindow        *validatorDebugLeaderWindow `json:"last_leader_window"`
}

type validatorDebugProtocol struct {
	Version              uint8  `json:"version"`
	Flags                uint8  `json:"flags"`
	ProtocolVersion      uint8  `json:"protocol_version"`
	UseQUIC              bool   `json:"use_quic"`
	SlotsPerLeaderWindow uint32 `json:"slots_per_leader_window"`
}

type validatorDebugLeaderWindow struct {
	StartSlot  uint32 `json:"start_slot"`
	EndSlot    uint32 `json:"end_slot"`
	ObservedAt string `json:"observed_at"`
}

type validatorDebugCandidateID struct {
	Slot uint32 `json:"slot"`
	Hash string `json:"hash"`
}

type validatorDebugShard struct {
	Workchain int32  `json:"workchain"`
	Shard     string `json:"shard"`
}

type validatorDebugBlockID struct {
	Workchain int32  `json:"workchain"`
	Shard     string `json:"shard"`
	Seqno     uint32 `json:"seqno"`
	RootHash  string `json:"root_hash"`
	FileHash  string `json:"file_hash"`
}

type validatorDebugDBMetrics struct {
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

type candidateExportResult struct {
	Schema        string `json:"schema"`
	Namespace     string `json:"namespace"`
	Slot          uint32 `json:"slot"`
	CandidateHash string `json:"candidate_hash"`
	WireSHA256    string `json:"wire_sha256"`
	Bytes         int    `json:"bytes"`
	Path          string `json:"path"`
}

func (s *Service) handleDebug(ctx context.Context, args []string) (string, error) {
	if len(args) == 2 && strings.EqualFold(args[0], "status") && args[1] == "--json" {
		return s.debugStatusJSON(ctx)
	}
	if len(args) == 5 && strings.EqualFold(args[0], "export-candidate") {
		return s.debugExportCandidate(ctx, args[1:])
	}

	return "", console.ErrNotFound
}

func (s *Service) debugStatusJSON(ctx context.Context) (string, error) {
	status, err := s.statusWithoutCollator(ctx)
	if err != nil {
		return "", err
	}
	var collatorStatusError string
	if s.localCollator != nil {
		localStatus, statusErr := s.localCollator.Status(ctx)
		if statusErr != nil {
			collatorStatusError = boundedDebugStatusError(fmt.Errorf(
				"read local collator status: %w",
				statusErr,
			))
		} else {
			status.Collator = &localStatus
		}
	}
	debug, err := makeValidatorDebugStatus(status)
	if err != nil {
		return "", err
	}
	if collatorStatusError != "" {
		debug.Collator = validatorDebugCollator{
			Enabled:     true,
			StatusError: collatorStatusError,
		}
	}

	encoded, err := json.Marshal(debug)
	if err != nil {
		return "", fmt.Errorf("marshal validator debug status: %w", err)
	}
	if len(encoded) > maxValidatorDebugStatusBytes {
		return "", fmt.Errorf(
			"validator debug status is %d bytes, limit is %d",
			len(encoded),
			maxValidatorDebugStatusBytes,
		)
	}

	return string(encoded), nil
}

func makeValidatorDebugStatus(status StatusSnapshot) (validatorDebugStatus, error) {
	debug := validatorDebugStatus{
		Schema: validatorDebugStatusSchema,
		Lifecycle: validatorDebugLifecycle{
			Started: status.Started,
			Closed:  status.Closed,
		},
		Messages: validatorDebugMessages{
			External:  makeValidatorDebugExternalMessages(status.External),
			Internals: makeValidatorDebugInternalMessages(status.Internals),
		},
		Groups: makeValidatorDebugGroups(status.Groups),
	}
	storageStatus, err := makeValidatorDebugStorage(status.Storage, status.Supervisor)
	if err != nil {
		return validatorDebugStatus{}, err
	}
	debug.Storage = storageStatus
	if status.Supervisor != nil {
		consensus, err := makeValidatorDebugConsensus(status.Supervisor)
		if err != nil {
			return validatorDebugStatus{}, err
		}
		debug.Consensus = &consensus
	}
	debug.Collator = makeValidatorDebugCollator(status.Collator)

	return debug, nil
}

func makeValidatorDebugExternalMessages(status msgpool.Stats) validatorDebugExternalMessages {
	return validatorDebugExternalMessages{
		Pooled:            status.Pooled,
		PooledBytes:       status.PooledBytes,
		Added:             status.Added,
		DedupSkipped:      status.DedupSkipped,
		PriorityBumps:     status.PriorityBumps,
		OverflowPool:      status.OverflowMempool,
		OverflowBytes:     status.OverflowBytes,
		OverflowAddress:   status.OverflowAddress,
		Expired:           status.Expired,
		InvalidDeleted:    status.InvalidDeleted,
		RejectedDelayed:   status.RejectedDelayed,
		RejectedRetried:   status.RejectedRetried,
		RejectedExhausted: status.RejectedExhausted,
		RejectedPressure:  status.RejectedPressure,
		StaleFeedback:     status.StaleFeedback,
		AppliedRequested:  status.AppliedRequested,
		AppliedDeleted:    status.AppliedDeleted,
	}
}

func makeValidatorDebugInternalMessages(status InternalsStatus) validatorDebugInternalMessages {
	stats := status.Stats

	return validatorDebugInternalMessages{
		Enabled:           status.Enabled,
		Destinations:      stats.Destinations,
		Sources:           stats.Sources,
		Entries:           stats.Entries,
		Removed:           stats.Removed,
		Candidates:        stats.Candidates,
		Seeds:             stats.Seeds,
		AppliedBlocks:     stats.AppliedBlocks,
		AppliedAdded:      stats.AppliedAdded,
		AppliedRemoved:    stats.AppliedRemoved,
		CandidatesAdded:   stats.CandidatesAdded,
		CandidatesDropped: stats.CandidatesDropped,
		Cuts:              stats.Cuts,
		CutMessages:       stats.CutMessages,
		CutFailures:       stats.CutFailures,
		Compactions:       stats.Compactions,
		CutStaleHistory:   stats.CutStaleHistory,
	}
}

func makeValidatorDebugGroups(status GroupTrackerStatus) validatorDebugGroups {
	local := make([]validatorDebugLocalGroup, len(status.Local))
	for i := range status.Local {
		group := &status.Local[i]
		local[i] = validatorDebugLocalGroup{
			SessionID:     hex.EncodeToString(group.SessionID[:]),
			Shard:         makeValidatorDebugShard(group.Shard),
			CatchainSeqno: group.CatchainSeqno,
			LocalIndex:    group.LocalIndex,
			Active:        group.Active,
		}
	}

	debug := validatorDebugGroups{
		Enabled:                  status.Enabled,
		Initialized:              status.Initialized,
		Ready:                    status.Ready,
		LifecycleEnabled:         status.LifecycleEnabled,
		Active:                   status.Active,
		Future:                   status.Future,
		PersistentOverlayMembers: status.PersistentOverlayMembers,
		Local:                    local,
	}
	if status.Initialized {
		block := makeValidatorDebugBlockID(status.MasterchainBlock)
		debug.MasterchainBlock = &block
	}

	return debug
}

func makeValidatorDebugConsensus(status *SessionSupervisorStatus) (validatorDebugConsensus, error) {
	sessions := make([]validatorDebugRuntimeSession, len(status.Sessions))
	for i := range status.Sessions {
		session := &status.Sessions[i]
		identity, err := makeValidatorDebugSessionIdentity(session.StorageID)
		if err != nil {
			return validatorDebugConsensus{}, fmt.Errorf("project validator runtime session: %w", err)
		}
		sessions[i] = validatorDebugRuntimeSession{
			Namespace:      identity.Namespace,
			SessionID:      hex.EncodeToString(session.SessionID[:]),
			ADNLID:         hex.EncodeToString(session.ADNLID[:]),
			ValidatorKeyID: identity.ValidatorKeyID,
			LocalIndex:     session.LocalIndex,
			Role:           identity.Role,
			Shard:          makeValidatorDebugShard(session.Shard),
			CatchainSeqno:  session.CatchainSeqno,
			Active:         session.Active,
			Phase:          session.Phase,
			RetryAt:        debugTime(session.RetryAt),
		}
	}

	return validatorDebugConsensus{
		Started:                status.Started,
		Closed:                 status.Closed,
		LatestMasterchainSeqno: status.LatestMasterchainSeqno,
		Desired:                status.Desired,
		Preparing:              status.Preparing,
		Prepared:               status.Prepared,
		Running:                status.Running,
		Sessions:               sessions,
	}, nil
}

func makeValidatorDebugCollator(status *collator.Status) validatorDebugCollator {
	if status == nil {
		return validatorDebugCollator{}
	}

	return validatorDebugCollator{
		Enabled: true,
		Production: validatorDebugCollatorProduction{
			Started:          status.Started,
			Closing:          status.Closing,
			Closed:           status.Closed,
			ActiveWindows:    status.ActiveWindows,
			RetryingWindows:  status.RetryingWindows,
			CompletedWindows: status.CompletedWindows,
			FailedWindows:    status.FailedWindows,
			LastError:        boundedDebugStatusText(status.LastError),
			LastCompleted:    debugTime(status.LastCompleted),
		},
		Storage: validatorDebugStorageSummary{
			Sessions:      status.Storage.Sessions,
			Candidates:    status.Storage.Candidates,
			PendingWrites: status.Storage.PendingWrites,
			DB:            makeValidatorDebugDBMetrics(status.Storage.DB),
		},
	}
}

func (s *Service) handleCollatorDebug(ctx context.Context, args []string) (string, error) {
	if len(args) != 2 || !strings.EqualFold(args[0], "status") || args[1] != "--json" {
		return "", console.ErrNotFound
	}

	debug := integratedCollatorDebugStatus{
		Schema: collatorDebugStatusSchema,
		Mode:   "integrated",
	}
	status, err := s.localCollator.Status(ctx)
	if err != nil {
		debug.StatusError = boundedDebugStatusError(err)
	} else {
		projected := makeValidatorDebugCollator(&status)
		debug.Production = projected.Production
		debug.Storage = projected.Storage
	}
	encoded, err := json.Marshal(debug)
	if err != nil {
		return "", fmt.Errorf("marshal integrated collator debug status: %w", err)
	}

	return string(encoded), nil
}

func boundedDebugStatusError(err error) string {
	return boundedDebugStatusText(err.Error())
}

func boundedDebugStatusText(message string) string {
	messageRunes := []rune(message)
	if len(messageRunes) <= maxDebugStatusErrorRunes {
		return message
	}

	return string(messageRunes[:maxDebugStatusErrorRunes]) + "…"
}

func makeValidatorDebugStorage(
	status StorageStatus,
	supervisor *SessionSupervisorStatus,
) (validatorDebugStorage, error) {
	selected := selectValidatorDebugStoredSessions(status.Sessions, supervisor)
	sessions := make([]validatorDebugStoredSession, len(selected))
	for i := range selected {
		session := &selected[i]
		identity, err := makeValidatorDebugSessionIdentity(session.ID)
		if err != nil {
			return validatorDebugStorage{}, fmt.Errorf("project validator stored session: %w", err)
		}
		debug := validatorDebugStoredSession{
			Namespace:               identity.Namespace,
			SessionID:               identity.SessionID,
			ADNLID:                  identity.ADNLID,
			ValidatorKeyID:          identity.ValidatorKeyID,
			LocalIndex:              session.ID.ValidatorIndex,
			Role:                    identity.Role,
			Shard:                   makeValidatorDebugShard(session.ID.Shard),
			CatchainSeqno:           session.ID.CatchainSeqno,
			Protocol:                makeValidatorDebugProtocol(session.ID.Protocol),
			Candidates:              session.Candidates,
			Finalized:               session.Finalized,
			Votes:                   session.Votes,
			Certificates:            session.Certificates,
			LeaderWindows:           session.LeaderWindows,
			FirstNonAnnouncedWindow: session.FirstNonAnnouncedWindow,
		}
		if session.LastFinalized != nil {
			candidate := makeValidatorDebugCandidateID(*session.LastFinalized)
			debug.LastFinalized = &candidate
		}
		if session.LastLeaderWindow != nil {
			debug.LastLeaderWindow = &validatorDebugLeaderWindow{
				StartSlot:  session.LastLeaderWindow.StartSlot,
				EndSlot:    session.LastLeaderWindow.EndSlot,
				ObservedAt: debugTime(session.LastLeaderWindow.ObservedAt),
			}
		}
		sessions[i] = debug
	}

	return validatorDebugStorage{
		PendingWrites:     status.PendingWrites,
		DB:                makeValidatorDebugDBMetrics(status.DB),
		SessionsTotal:     uint64(len(status.Sessions)),
		SessionsTruncated: len(sessions) < len(status.Sessions),
		Sessions:          sessions,
	}, nil
}

// selectValidatorDebugStoredSessions returns a canonical bounded storage
// sample. Runtime session IDs are selected before historical IDs so an
// operator can always correlate the active consensus view with durable
// telemetry. The final order is canonical rather than runtime order, which
// keeps repeated debug output stable for scripts and diffs.
func selectValidatorDebugStoredSessions(
	stored []StoredSession,
	supervisor *SessionSupervisorStatus,
) []StoredSession {
	ordered := append([]StoredSession(nil), stored...)
	sort.Slice(ordered, func(i, j int) bool {
		return sessionStorageIDLess(ordered[i].ID, ordered[j].ID)
	})

	var runtime map[SessionStorageID]struct{}
	if supervisor != nil && len(supervisor.Sessions) > 0 {
		runtime = make(map[SessionStorageID]struct{}, len(supervisor.Sessions))
		for i := range supervisor.Sessions {
			runtime[supervisor.Sessions[i].StorageID] = struct{}{}
		}
	}

	limit := min(len(ordered), maxValidatorDebugStoredSessions)
	selected := make([]StoredSession, 0, limit)
	for i := range ordered {
		if _, exists := runtime[ordered[i].ID]; exists {
			selected = append(selected, ordered[i])
			if len(selected) == limit {
				break
			}
		}
	}
	if len(selected) < limit {
		for i := range ordered {
			if _, exists := runtime[ordered[i].ID]; exists {
				continue
			}
			selected = append(selected, ordered[i])
			if len(selected) == limit {
				break
			}
		}
	}
	sort.Slice(selected, func(i, j int) bool {
		return sessionStorageIDLess(selected[i].ID, selected[j].ID)
	})

	return selected
}

type validatorDebugSessionIdentity struct {
	Namespace      string
	SessionID      string
	ADNLID         string
	ValidatorKeyID string
	Role           string
}

func makeValidatorDebugSessionIdentity(id SessionStorageID) (validatorDebugSessionIdentity, error) {
	namespace, err := id.Namespace()
	if err != nil {
		return validatorDebugSessionIdentity{}, err
	}
	identity := validatorDebugSessionIdentity{
		Namespace: hex.EncodeToString(namespace[:]),
		SessionID: hex.EncodeToString(id.SessionID[:]),
		ADNLID:    hex.EncodeToString(id.LocalADNLID[:]),
		Role:      validatorDebugRole(id.IsValidator),
	}
	if id.IsValidator {
		identity.ValidatorKeyID = hex.EncodeToString(id.ValidatorKeyID[:])
	}

	return identity, nil
}

func makeValidatorDebugProtocol(protocol SessionProtocol) validatorDebugProtocol {
	return validatorDebugProtocol{
		Version:              protocol.Version,
		Flags:                protocol.Flags,
		ProtocolVersion:      protocol.ProtocolVersion,
		UseQUIC:              protocol.UseQUIC,
		SlotsPerLeaderWindow: protocol.SlotsPerLeaderWindow,
	}
}

func makeValidatorDebugCandidateID(id simplex.CandidateID) validatorDebugCandidateID {
	return validatorDebugCandidateID{Slot: id.Slot, Hash: hex.EncodeToString(id.Hash[:])}
}

func makeValidatorDebugShard(shard groups.ShardID) validatorDebugShard {
	return validatorDebugShard{
		Workchain: shard.Workchain,
		Shard:     fmt.Sprintf("%016x", uint64(shard.Shard)),
	}
}

func makeValidatorDebugBlockID(block ton.BlockIDExt) validatorDebugBlockID {
	return validatorDebugBlockID{
		Workchain: block.Workchain,
		Shard:     fmt.Sprintf("%016x", uint64(block.Shard)),
		Seqno:     block.SeqNo,
		RootHash:  hex.EncodeToString(block.RootHash),
		FileHash:  hex.EncodeToString(block.FileHash),
	}
}

func makeValidatorDebugDBMetrics(metrics collator.DBMetrics) validatorDebugDBMetrics {
	return validatorDebugDBMetrics{
		DiskSize:              metrics.DiskSize,
		LiveSize:              metrics.LiveSize,
		ReadAmp:               metrics.ReadAmp,
		L0Files:               metrics.L0Files,
		L0Sublevels:           metrics.L0Sublevels,
		CompactionDebt:        metrics.CompactionDebt,
		CompactionsInProgress: metrics.CompactionsInProgress,
		MemTableSize:          metrics.MemTableSize,
		WALSize:               metrics.WALSize,
	}
}

func validatorDebugRole(isValidator bool) string {
	if isValidator {
		return "validator"
	}

	return "observer"
}

func debugTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}

	return value.UTC().Format(time.RFC3339Nano)
}

func (s *Service) debugExportCandidate(ctx context.Context, args []string) (string, error) {
	namespace, err := parseDebugHash("session namespace", args[0])
	if err != nil {
		return "", err
	}
	slot, err := strconv.ParseUint(args[1], 10, 32)
	if err != nil {
		return "", fmt.Errorf("parse candidate slot %q: %w", args[1], err)
	}
	candidateHash, err := parseDebugHash("candidate hash", args[2])
	if err != nil {
		return "", err
	}

	session, err := s.debugSessionByNamespace(ctx, namespace)
	if err != nil {
		return "", err
	}
	id := simplex.CandidateID{Slot: uint32(slot), Hash: candidateHash}
	record, err := s.validatorStore.Candidate(ctx, session, id)
	if err != nil {
		return "", fmt.Errorf("load debug candidate: %w", err)
	}
	if len(record.Wire) > maxCandidateExportBytes {
		return "", fmt.Errorf(
			"candidate wire is %d bytes, export limit is %d",
			len(record.Wire),
			maxCandidateExportBytes,
		)
	}

	path, err := writeCandidateExport(args[3], record.Wire)
	if err != nil {
		return "", err
	}
	wireHash := sha256.Sum256(record.Wire)
	result := candidateExportResult{
		Schema:        candidateExportSchema,
		Namespace:     hex.EncodeToString(namespace[:]),
		Slot:          record.ID.Slot,
		CandidateHash: hex.EncodeToString(record.ID.Hash[:]),
		WireSHA256:    hex.EncodeToString(wireHash[:]),
		Bytes:         len(record.Wire),
		Path:          path,
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return "", fmt.Errorf("marshal candidate export result: %w", err)
	}

	return string(encoded), nil
}

func (s *Service) debugSessionByNamespace(
	ctx context.Context,
	namespace [32]byte,
) (SessionStorageID, error) {
	sessions, err := s.validatorStore.Sessions(ctx)
	if err != nil {
		return SessionStorageID{}, fmt.Errorf("list debug sessions: %w", err)
	}
	for i := range sessions {
		candidate := sessions[i]
		candidateNamespace, namespaceErr := candidate.Namespace()
		if namespaceErr != nil {
			return SessionStorageID{}, fmt.Errorf("read debug session namespace: %w", namespaceErr)
		}
		if candidateNamespace == namespace {
			return candidate, nil
		}
	}

	return SessionStorageID{}, fmt.Errorf("session namespace %x: %w", namespace, storage.ErrNotFound)
}

func parseDebugHash(label, value string) ([32]byte, error) {
	if len(value) != 64 {
		return [32]byte{}, fmt.Errorf("parse %s: expected 64 hexadecimal characters", label)
	}
	decoded, err := hex.DecodeString(value)
	if err != nil {
		return [32]byte{}, fmt.Errorf("parse %s: %w", label, err)
	}

	var hash [32]byte
	copy(hash[:], decoded)

	return hash, nil
}

func writeCandidateExport(path string, wire []byte) (string, error) {
	absolute, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return "", fmt.Errorf("resolve candidate export path: %w", err)
	}
	directory := filepath.Dir(absolute)
	temporary, err := os.CreateTemp(directory, ".gton-candidate-*.tmp")
	if err != nil {
		return "", fmt.Errorf("create candidate export temporary file: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)

	if err = temporary.Chmod(candidateExportFileMode); err != nil {
		_ = temporary.Close()

		return "", fmt.Errorf("set candidate export permissions: %w", err)
	}
	written, err := io.Copy(temporary, bytes.NewReader(wire))
	if err == nil && written != int64(len(wire)) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = temporary.Sync()
	}
	closeErr := temporary.Close()
	if err != nil {
		return "", fmt.Errorf("write candidate export: %w", err)
	}
	if closeErr != nil {
		return "", fmt.Errorf("close candidate export: %w", closeErr)
	}

	// Link publishes the completed inode only if the destination does not
	// already exist. That gives debug export atomic no-overwrite semantics
	// without touching the validator database or its writer queue.
	if err = os.Link(temporaryPath, absolute); err != nil {
		if errors.Is(err, os.ErrExist) {
			return "", fmt.Errorf("candidate export path %q already exists", absolute)
		}

		return "", fmt.Errorf("publish candidate export: %w", err)
	}

	return absolute, nil
}
