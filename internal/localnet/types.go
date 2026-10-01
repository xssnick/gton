package localnet

import "time"

type NodeStatus struct {
	Name              string            `json:"name"`
	Kind              string            `json:"kind"`
	Roles             []string          `json:"roles"`
	Optional          bool              `json:"optional,omitempty"`
	Running           bool              `json:"running"`
	PIDs              []int             `json:"pids"`
	LogPath           string            `json:"log_path"`
	LogSize           int64             `json:"log_size"`
	LogModified       time.Time         `json:"log_modified,omitempty"`
	MasterchainSeqno  uint32            `json:"masterchain_seqno,omitempty"`
	FinalizedBlocks   uint64            `json:"finalized_blocks"`
	CollatedBlocks    uint64            `json:"collated_blocks"`
	EmittedCandidates uint64            `json:"emitted_candidates"`
	ValidatedBlocks   uint64            `json:"validated_blocks"`
	HardErrors        uint64            `json:"hard_errors"`
	AdvisoryWarnings  uint64            `json:"advisory_warnings"`
	ErrorCategories   map[string]uint64 `json:"error_categories,omitempty"`
	WarningCategories map[string]uint64 `json:"warning_categories,omitempty"`
}

type Status struct {
	CapturedAt time.Time    `json:"captured_at"`
	Nodes      []NodeStatus `json:"nodes"`
	Healthy    bool         `json:"healthy"`
}

type Baseline struct {
	CapturedAt time.Time      `json:"captured_at"`
	Nodes      []NodeBaseline `json:"nodes"`
}

type NodeBaseline struct {
	Name             string      `json:"name"`
	Log              LogPosition `json:"log"`
	MasterchainSeqno uint32      `json:"masterchain_seqno,omitempty"`
}

type LogPosition struct {
	CapturedAt     time.Time `json:"captured_at"`
	FileID         string    `json:"file_id"`
	PreviousBackup string    `json:"previous_backup,omitempty"`
	Offset         int64     `json:"offset"`
	Anchor         string    `json:"anchor,omitempty"`
}

type Event struct {
	Time          time.Time `json:"time,omitempty"`
	Node          string    `json:"node"`
	Kind          string    `json:"kind"`
	Category      string    `json:"category,omitempty"`
	Message       string    `json:"message,omitempty"`
	Workchain     int32     `json:"workchain,omitempty"`
	Shard         int64     `json:"shard,omitempty"`
	Seqno         uint32    `json:"seqno,omitempty"`
	Slot          *uint32   `json:"slot,omitempty"`
	WindowStart   *uint32   `json:"window_start,omitempty"`
	WindowEnd     *uint32   `json:"window_end,omitempty"`
	Transition    string    `json:"transition,omitempty"`
	SessionID     string    `json:"session_id,omitempty"`
	Leader        string    `json:"leader,omitempty"`
	CandidateHash string    `json:"candidate_hash,omitempty"`
	BlockRootHash string    `json:"block_root_hash,omitempty"`
	BlockFileHash string    `json:"block_file_hash,omitempty"`
	Replayed      bool      `json:"replayed,omitempty"`
	Error         string    `json:"error,omitempty"`
}

type LoadResult struct {
	StartedAt         time.Time      `json:"started_at"`
	FinishedAt        time.Time      `json:"finished_at"`
	Duration          time.Duration  `json:"duration_ns"`
	ExitCode          int            `json:"exit_code"`
	Outcome           string         `json:"outcome"`
	HardFailure       bool           `json:"hard_failure,omitempty"`
	Submitted         int            `json:"submitted"`
	Accepted          int            `json:"accepted"`
	FailedBatches     int            `json:"failed_batches"`
	IncompleteSenders []uint64       `json:"incomplete_senders"`
	OutcomeCounts     map[string]int `json:"outcome_counts,omitempty"`
	FailureStages     map[string]int `json:"failure_stages,omitempty"`
	Error             string         `json:"error,omitempty"`
	Senders           []SenderResult `json:"senders"`
}

type SenderResult struct {
	SenderIndex uint64        `json:"sender_index"`
	Setup       ProcessResult `json:"setup"`
	Load        ProcessResult `json:"load"`
}

type ProcessResult struct {
	LoadEvidence
	EvidenceValid bool          `json:"evidence_valid"`
	StartedAt     time.Time     `json:"started_at,omitempty"`
	FinishedAt    time.Time     `json:"finished_at,omitempty"`
	Duration      time.Duration `json:"duration_ns,omitempty"`
	ExitCode      int           `json:"exit_code"`
	ProcessError  string        `json:"process_error,omitempty"`
	ProtocolError string        `json:"protocol_error,omitempty"`
}

type LoadEvidence struct {
	SchemaVersion           int     `json:"schema_version"`
	Command                 string  `json:"command"`
	Outcome                 string  `json:"outcome"`
	Error                   string  `json:"error,omitempty"`
	FailureStage            string  `json:"failure_stage,omitempty"`
	ContractProfile         string  `json:"contract_profile,omitempty"`
	MinterCodeHash          string  `json:"minter_code_hash,omitempty"`
	WalletCodeHash          string  `json:"wallet_code_hash,omitempty"`
	SenderIndex             uint64  `json:"sender_index"`
	Submitted               int     `json:"submitted"`
	Accepted                int     `json:"accepted"`
	FailedBatches           int     `json:"failed_batches"`
	ExternalBatches         int     `json:"external_batches"`
	RPCAcceptedBatches      int     `json:"rpc_accepted_batches"`
	CanarySubmitted         int     `json:"canary_submitted"`
	CanaryAccepted          int     `json:"canary_accepted"`
	Undelivered             int     `json:"undelivered"`
	SubmittedTPS            float64 `json:"submitted_tps"`
	SourceBalanceBefore     string  `json:"source_balance_before,omitempty"`
	SourceBalanceAfter      string  `json:"source_balance_after,omitempty"`
	SourceBalanceCurrent    string  `json:"source_balance_current,omitempty"`
	RecipientBalanceBefore  string  `json:"recipient_balance_before,omitempty"`
	RecipientBalanceAfter   string  `json:"recipient_balance_after,omitempty"`
	RecipientBalanceCurrent string  `json:"recipient_balance_current,omitempty"`
	RunEpoch                string  `json:"run_epoch,omitempty"`
	Minter                  string  `json:"minter,omitempty"`
	HighloadWallet          string  `json:"highload_wallet,omitempty"`
	SourceJettonWallet      string  `json:"source_jetton_wallet,omitempty"`
}

type Check struct {
	Name   string `json:"name"`
	Passed bool   `json:"passed"`
	Actual string `json:"actual"`
	Wanted string `json:"wanted"`
}

type TopologyCoverage struct {
	LinearProof          bool                    `json:"linear_proof"`
	Split                bool                    `json:"split"`
	ChildrenProduced     bool                    `json:"children_produced"`
	Rotation             bool                    `json:"rotation"`
	Merge                bool                    `json:"merge"`
	AfterMergeProduced   bool                    `json:"after_merge_produced"`
	ReturnedToLinear     bool                    `json:"returned_to_linear"`
	ProducerNodes        []string                `json:"producer_nodes"`
	ValidationNodes      []string                `json:"validation_nodes"`
	FinalizationNodes    []string                `json:"finalization_nodes"`
	RequiredRoleCoverage bool                    `json:"required_role_coverage"`
	CandidateFlows       []CandidateFlowCoverage `json:"candidate_flows"`
	Complete             bool                    `json:"complete"`
}

type CandidateFlowCoverage struct {
	Producer        string   `json:"producer"`
	Validators      []string `json:"validators"`
	Finalizers      []string `json:"finalizers"`
	ValidatedBy     []string `json:"validated_by"`
	FinalizedBy     []string `json:"finalized_by"`
	MissingEvidence []string `json:"missing_evidence,omitempty"`
	Complete        bool     `json:"complete"`
}

type NodeDelta struct {
	Name              string            `json:"name"`
	StartOffset       int64             `json:"start_offset"`
	EndOffset         int64             `json:"end_offset"`
	MasterchainStart  uint32            `json:"masterchain_start,omitempty"`
	MasterchainEnd    uint32            `json:"masterchain_end,omitempty"`
	FinalizedBlocks   uint64            `json:"finalized_blocks"`
	CollatedBlocks    uint64            `json:"collated_blocks"`
	EmittedCandidates uint64            `json:"emitted_candidates"`
	ValidatedBlocks   uint64            `json:"validated_blocks"`
	HardErrors        uint64            `json:"hard_errors"`
	AdvisoryWarnings  uint64            `json:"advisory_warnings"`
	ErrorCategories   map[string]uint64 `json:"error_categories,omitempty"`
	WarningCategories map[string]uint64 `json:"warning_categories,omitempty"`
}

type RunPhase struct {
	StartedAt      time.Time              `json:"started_at"`
	FinishedAt     time.Time              `json:"finished_at"`
	StartPositions map[string]LogPosition `json:"start_positions"`
	EndPositions   map[string]LogPosition `json:"end_positions"`
}

type RunPhases struct {
	Setup    *RunPhase `json:"setup,omitempty"`
	Load     *RunPhase `json:"load,omitempty"`
	Recovery *RunPhase `json:"recovery,omitempty"`
	Topology *RunPhase `json:"topology,omitempty"`
}

type Summary struct {
	RunDirectory       string           `json:"run_directory"`
	Scenario           string           `json:"scenario"`
	StartedAt          time.Time        `json:"started_at"`
	FinishedAt         time.Time        `json:"finished_at"`
	Verdict            string           `json:"verdict"`
	ConsensusVerdict   string           `json:"consensus_topology_verdict"`
	LoadDeliveryStatus string           `json:"load_delivery_outcome"`
	Load               LoadResult       `json:"load"`
	Nodes              []NodeDelta      `json:"nodes"`
	Checks             []Check          `json:"checks"`
	Topology           TopologyCoverage `json:"topology"`
	Phases             RunPhases        `json:"phases"`
	Events             int              `json:"events"`
}

type RunManifest struct {
	Version      int                    `json:"version"`
	Scenario     string                 `json:"scenario"`
	StartedAt    time.Time              `json:"started_at"`
	FinishedAt   time.Time              `json:"finished_at,omitempty"`
	Config       Config                 `json:"config"`
	Baseline     Baseline               `json:"baseline"`
	Phases       RunPhases              `json:"phases"`
	EndPositions map[string]LogPosition `json:"end_positions,omitempty"`
	Load         LoadResult             `json:"load"`
}
