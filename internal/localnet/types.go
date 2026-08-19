package localnet

import "time"

type NodeStatus struct {
	Name             string    `json:"name"`
	Kind             string    `json:"kind"`
	Optional         bool      `json:"optional,omitempty"`
	Running          bool      `json:"running"`
	PIDs             []int     `json:"pids"`
	LogPath          string    `json:"log_path"`
	LogSize          int64     `json:"log_size"`
	LogModified      time.Time `json:"log_modified,omitempty"`
	MasterchainSeqno uint32    `json:"masterchain_seqno,omitempty"`
	FinalizedBlocks  uint64    `json:"finalized_blocks"`
	CollatedBlocks   uint64    `json:"collated_blocks"`
	ValidatedBlocks  uint64    `json:"validated_blocks"`
	HardErrors       uint64    `json:"hard_errors"`
	AdvisoryWarnings uint64    `json:"advisory_warnings"`
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
	Message       string    `json:"message,omitempty"`
	Workchain     int32     `json:"workchain,omitempty"`
	Shard         int64     `json:"shard,omitempty"`
	Seqno         uint32    `json:"seqno,omitempty"`
	Transition    string    `json:"transition,omitempty"`
	SessionID     string    `json:"session_id,omitempty"`
	Leader        string    `json:"leader,omitempty"`
	CandidateHash string    `json:"candidate_hash,omitempty"`
	BlockRootHash string    `json:"block_root_hash,omitempty"`
	BlockFileHash string    `json:"block_file_hash,omitempty"`
	Error         string    `json:"error,omitempty"`
}

type LoadResult struct {
	StartedAt         time.Time      `json:"started_at"`
	FinishedAt        time.Time      `json:"finished_at"`
	Duration          time.Duration  `json:"duration_ns"`
	ExitCode          int            `json:"exit_code"`
	Outcome           string         `json:"outcome"`
	Advisory          bool           `json:"advisory,omitempty"`
	HardFailure       bool           `json:"hard_failure,omitempty"`
	Submitted         int            `json:"submitted"`
	Accepted          int            `json:"accepted"`
	FailedBatches     int            `json:"failed_batches"`
	IncompleteSenders []uint64       `json:"incomplete_senders"`
	Error             string         `json:"error,omitempty"`
	Senders           []SenderResult `json:"senders"`
}

type SenderResult struct {
	SenderIndex uint64        `json:"sender_index"`
	Setup       ProcessResult `json:"setup"`
	Load        ProcessResult `json:"load"`
}

type ProcessResult struct {
	StartedAt     time.Time     `json:"started_at,omitempty"`
	FinishedAt    time.Time     `json:"finished_at,omitempty"`
	Duration      time.Duration `json:"duration_ns,omitempty"`
	ExitCode      int           `json:"exit_code"`
	Outcome       string        `json:"outcome"`
	Submitted     int           `json:"submitted,omitempty"`
	Accepted      int           `json:"accepted,omitempty"`
	FailedBatches int           `json:"failed_batches,omitempty"`
	Error         string        `json:"error,omitempty"`
}

type Check struct {
	Name   string `json:"name"`
	Passed bool   `json:"passed"`
	Actual string `json:"actual"`
	Wanted string `json:"wanted"`
}

type TopologyCoverage struct {
	LinearProof          bool     `json:"linear_proof"`
	Split                bool     `json:"split"`
	ChildrenProduced     bool     `json:"children_produced"`
	Rotation             bool     `json:"rotation"`
	Merge                bool     `json:"merge"`
	AfterMergeProduced   bool     `json:"after_merge_produced"`
	ReturnedToLinear     bool     `json:"returned_to_linear"`
	ProducerNodes        []string `json:"producer_nodes"`
	ValidationNodes      []string `json:"validation_nodes"`
	RequiredNodeCoverage bool     `json:"required_node_coverage"`
	CppToGoValidated     bool     `json:"cpp_to_go_validated"`
	Complete             bool     `json:"complete"`
}

type NodeDelta struct {
	Name             string `json:"name"`
	StartOffset      int64  `json:"start_offset"`
	EndOffset        int64  `json:"end_offset"`
	MasterchainStart uint32 `json:"masterchain_start,omitempty"`
	MasterchainEnd   uint32 `json:"masterchain_end,omitempty"`
	FinalizedBlocks  uint64 `json:"finalized_blocks"`
	CollatedBlocks   uint64 `json:"collated_blocks"`
	ValidatedBlocks  uint64 `json:"validated_blocks"`
	HardErrors       uint64 `json:"hard_errors"`
	AdvisoryWarnings uint64 `json:"advisory_warnings"`
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
	Events             int              `json:"events"`
}

type RunManifest struct {
	Version      int                    `json:"version"`
	Scenario     string                 `json:"scenario"`
	StartedAt    time.Time              `json:"started_at"`
	FinishedAt   time.Time              `json:"finished_at,omitempty"`
	Config       Config                 `json:"config"`
	Baseline     Baseline               `json:"baseline"`
	EndPositions map[string]LogPosition `json:"end_positions,omitempty"`
	Load         LoadResult             `json:"load"`
}
