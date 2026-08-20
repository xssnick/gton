package validator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/simplex"
	"github.com/xssnick/tonutils-go/ton"
)

// isCapturableValidationFailure reports whether a validation error is a
// producer/validator SEMANTIC disagreement worth dumping for offline replay,
// as opposed to a benign abstain or a local plumbing fault. The incident this
// capture exists for is a replay failure ("TVM execution failed", carried by
// collator.ErrSemanticExecution); a semantic invalid verdict
// (ErrCandidateRejected) is the same disagreement surfaced as a rejection and is
// captured too. Not-ready retries, context cancellation and deadline are benign
// and never captured; anything else (a store read fault, a missing parsed
// predecessor) is local plumbing and is left uncaptured.
func isCapturableValidationFailure(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrBlockNotReady) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	return errors.Is(err, collator.ErrSemanticExecution) || errors.Is(err, ErrCandidateRejected)
}

const (
	// failedCandidateSchema versions the dumped metadata so an offline replay
	// tool can refuse a shape it does not understand.
	failedCandidateSchema = "gton.validator.failed-candidate.v1"
	// maxKeptFailedCandidateDumps bounds how many dumps survive on disk. The
	// oldest is evicted once the cap is exceeded; a validator that keeps refusing
	// candidates must never fill the data disk with them.
	maxKeptFailedCandidateDumps = 8
	// maxFailedCandidateDumpsPerHour rate-limits new dumps within a rolling hour
	// so a persistently disagreeing lane produces evidence without a write storm.
	maxFailedCandidateDumpsPerHour = 8
	// failedCandidateFileMode and failedCandidateDirMode keep the dump readable
	// only by the node's own user, like the debug candidate export.
	failedCandidateFileMode   = 0o600
	failedCandidateDirMode    = 0o700
	failedCandidateDumpPrefix = "failed-"
)

// failedCandidateCapturer writes a bounded, self-describing dump of a candidate
// this validator refused with a TVM/semantic replay error, so the same refusal
// can be reproduced deterministically offline. It is safe for concurrent use and
// costs nothing on the happy path: capture is only ever called after a
// validation has already failed with a capture-worthy error.
type failedCandidateCapturer struct {
	dir       string
	sessionID string
	namespace string
	metrics   ValidationObserver
	log       *zerolog.Logger
	now       func() time.Time

	mu         sync.Mutex
	recentHour []time.Time
}

func newFailedCandidateCapturer(dir string, metrics ValidationObserver, log *zerolog.Logger) *failedCandidateCapturer {
	if strings.TrimSpace(dir) == "" {
		return nil
	}

	return &failedCandidateCapturer{
		dir:     dir,
		metrics: metrics,
		log:     log,
		now:     time.Now,
	}
}

// withIdentity records the consensus identity so the dump metadata can name its
// session and namespace without threading them through every capture call.
func (c *failedCandidateCapturer) withIdentity(sessionID, namespace string) *failedCandidateCapturer {
	if c == nil {
		return nil
	}
	c.sessionID = sessionID
	c.namespace = namespace

	return c
}

// failedCandidateMeta is the cheaply-serializable context around one refused
// candidate. The large state inputs a replay also needs (predecessor STATE
// cells, the masterchain view state, neighbour out-message queues) are the
// node's live database and are deliberately NOT copied here; the captured block
// ids below let an operator reconstruct them from a node holding the same chain.
type failedCandidateMeta struct {
	Schema     string `json:"schema"`
	CapturedAt string `json:"captured_at"`
	Chain      string `json:"chain"`
	Reason     string `json:"reason"`

	SessionID string `json:"session_id,omitempty"`
	Namespace string `json:"namespace,omitempty"`
	Slot      uint32 `json:"slot"`
	Leader    uint32 `json:"leader"`
	IsEmpty   bool   `json:"is_empty"`

	CandidateHash  string                   `json:"candidate_hash"`
	Candidate      failedCandidateBlockID   `json:"candidate"`
	Predecessors   []failedCandidateBlockID `json:"predecessors"`
	MinMasterchain *failedCandidateBlockID  `json:"min_masterchain,omitempty"`

	BlockBOCBytes      int    `json:"block_boc_bytes"`
	BlockBOCSHA256     string `json:"block_boc_sha256"`
	CollatedDataBytes  int    `json:"collated_data_bytes"`
	CollatedDataSHA256 string `json:"collated_data_sha256"`

	Captured    []string `json:"captured"`
	NotCaptured []string `json:"not_captured"`
}

type failedCandidateBlockID struct {
	Workchain int32  `json:"workchain"`
	Shard     string `json:"shard"`
	Seqno     uint32 `json:"seqno"`
	RootHash  string `json:"root_hash"`
	FileHash  string `json:"file_hash"`
}

func failedCandidateBlockIDOf(id ton.BlockIDExt) failedCandidateBlockID {
	return failedCandidateBlockID{
		Workchain: id.Workchain,
		Shard:     fmt.Sprintf("%016x", uint64(id.Shard)),
		Seqno:     id.SeqNo,
		RootHash:  hex.EncodeToString(id.RootHash),
		FileHash:  hex.EncodeToString(id.FileHash),
	}
}

// capture writes one dump for a refused candidate. It never returns an error:
// capturing evidence must not itself fail a validation path or a session. A
// failure to write is logged and dropped.
func (c *failedCandidateCapturer) capture(
	candidate *simplex.Candidate,
	artifact *CandidateArtifact,
	parent *ChainState,
	chain collator.MetricChain,
	reason error,
) {
	if c == nil || candidate == nil || artifact == nil || reason == nil {
		return
	}

	c.mu.Lock()
	admitted := c.admitLocked()
	c.mu.Unlock()
	if !admitted {
		if c.log != nil {
			if event := c.log.Debug(); event != nil {
				event.Uint32("slot", candidate.ID.Slot).
					Msg("skipping failed-candidate capture: hourly cap reached")
			}
		}

		return
	}

	path, err := c.write(candidate, artifact, parent, chain, reason)
	if err != nil {
		if c.log != nil {
			if event := c.log.Error(); event != nil {
				event.Err(err).Uint32("slot", candidate.ID.Slot).
					Msg("failed to capture refused candidate for offline replay")
			}
		}

		return
	}

	c.mu.Lock()
	c.recentHour = append(c.recentHour, c.now())
	c.enforceKeptBoundLocked()
	c.mu.Unlock()

	if c.metrics != nil {
		c.metrics.AddCapturedFailedCandidate(chain)
	}
	if c.log != nil {
		if event := c.log.Error(); event != nil {
			event.
				Str("dump_path", path).
				Uint32("slot", candidate.ID.Slot).
				Uint32("leader", candidate.Leader).
				Hex("candidate_hash", candidate.ID.Hash[:]).
				Int32("workchain", candidate.Block.Workchain).
				Int64("shard", candidate.Block.Shard).
				Uint32("block_seqno", candidate.Block.SeqNo).
				Err(reason).
				Msg("captured refused candidate for offline replay")
		}
	}
}

// admitLocked drops rolling-hour entries older than an hour and reports whether
// another capture is within the hourly cap. The caller holds c.mu.
func (c *failedCandidateCapturer) admitLocked() bool {
	cutoff := c.now().Add(-time.Hour)
	kept := c.recentHour[:0]
	for _, at := range c.recentHour {
		if at.After(cutoff) {
			kept = append(kept, at)
		}
	}
	c.recentHour = kept

	return len(c.recentHour) < maxFailedCandidateDumpsPerHour
}

// enforceKeptBoundLocked removes the oldest dumps until at most
// maxKeptFailedCandidateDumps remain. The caller holds c.mu. It reconciles with
// the directory on disk so a restart that inherited old dumps still converges.
func (c *failedCandidateCapturer) enforceKeptBoundLocked() {
	names := c.listDumpDirs()
	if len(names) <= maxKeptFailedCandidateDumps {
		return
	}
	for _, name := range names[:len(names)-maxKeptFailedCandidateDumps] {
		_ = os.RemoveAll(filepath.Join(c.dir, name))
	}
}

// listDumpDirs returns the dump subdirectory names in chronological order. The
// names carry a lexically sortable timestamp prefix, so a name sort is a time
// sort, which keeps eviction correct across process restarts.
func (c *failedCandidateCapturer) listDumpDirs() []string {
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		return nil
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() && strings.HasPrefix(entry.Name(), failedCandidateDumpPrefix) {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)

	return names
}

func (c *failedCandidateCapturer) write(
	candidate *simplex.Candidate,
	artifact *CandidateArtifact,
	parent *ChainState,
	chain collator.MetricChain,
	reason error,
) (string, error) {
	if err := os.MkdirAll(c.dir, failedCandidateDirMode); err != nil {
		return "", fmt.Errorf("create failed-candidate directory: %w", err)
	}

	now := c.now().UTC()
	name := fmt.Sprintf(
		"%s%s-slot%010d-%s",
		failedCandidateDumpPrefix,
		now.Format("20060102T150405.000000000Z"),
		candidate.ID.Slot,
		hex.EncodeToString(candidate.ID.Hash[:8]),
	)
	final := filepath.Join(c.dir, name)

	// Assemble the whole dump in a sibling temp directory and publish it with one
	// atomic rename, so a reader never sees a half-written dump and the bound
	// only ever counts complete ones.
	temp, err := os.MkdirTemp(c.dir, ".gton-failed-candidate-*")
	if err != nil {
		return "", fmt.Errorf("create failed-candidate temp directory: %w", err)
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.RemoveAll(temp)
		}
	}()

	blockHash := sha256.Sum256(artifact.BlockBOC)
	collatedHash := sha256.Sum256(artifact.CollatedData)

	if err = writeFailedCandidateFile(filepath.Join(temp, "block.boc"), artifact.BlockBOC); err != nil {
		return "", err
	}
	if err = writeFailedCandidateFile(filepath.Join(temp, "collated.boc"), artifact.CollatedData); err != nil {
		return "", err
	}

	meta := failedCandidateMeta{
		Schema:             failedCandidateSchema,
		CapturedAt:         now.Format(time.RFC3339Nano),
		Chain:              failedCandidateChainLabel(chain),
		Reason:             reason.Error(),
		SessionID:          c.sessionID,
		Namespace:          c.namespace,
		Slot:               candidate.ID.Slot,
		Leader:             candidate.Leader,
		IsEmpty:            candidate.Empty,
		CandidateHash:      hex.EncodeToString(candidate.ID.Hash[:]),
		Candidate:          failedCandidateBlockIDOf(candidate.Block),
		BlockBOCBytes:      len(artifact.BlockBOC),
		BlockBOCSHA256:     hex.EncodeToString(blockHash[:]),
		CollatedDataBytes:  len(artifact.CollatedData),
		CollatedDataSHA256: hex.EncodeToString(collatedHash[:]),
		Captured: []string{
			"block.boc (candidate block wire)",
			"collated.boc (candidate collated data)",
			"predecessor block ids",
			"min masterchain block id",
		},
		NotCaptured: []string{
			"predecessor state cells",
			"masterchain view (block/state) cells",
			"neighbour out-message queues and NeighborShardEndLT",
		},
	}
	if parent != nil {
		meta.Predecessors = make([]failedCandidateBlockID, 0, len(parent.tips))
		for i := range parent.tips {
			meta.Predecessors = append(meta.Predecessors, failedCandidateBlockIDOf(parent.tips[i].ID))
		}
		if parent.minMasterchain.SeqNo != 0 || len(parent.minMasterchain.RootHash) != 0 {
			min := failedCandidateBlockIDOf(parent.minMasterchain)
			meta.MinMasterchain = &min
		}
	}

	encoded, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshal failed-candidate metadata: %w", err)
	}
	if err = writeFailedCandidateFile(filepath.Join(temp, "meta.json"), encoded); err != nil {
		return "", err
	}
	if err = writeFailedCandidateFile(filepath.Join(temp, "README.txt"), []byte(failedCandidateReadme)); err != nil {
		return "", err
	}

	if err = os.Rename(temp, final); err != nil {
		return "", fmt.Errorf("publish failed-candidate dump: %w", err)
	}
	cleanup = false

	return final, nil
}

func writeFailedCandidateFile(path string, data []byte) error {
	if err := os.WriteFile(path, data, failedCandidateFileMode); err != nil {
		return fmt.Errorf("write %s: %w", filepath.Base(path), err)
	}

	return nil
}

func failedCandidateChainLabel(chain collator.MetricChain) string {
	if chain == collator.MetricChainMasterchain {
		return "masterchain"
	}

	return "shardchain"
}

const failedCandidateReadme = `This directory is one refused-candidate capture written by the validator when
its own semantic replay of a candidate failed with a TVM/semantic error (for
example "TVM execution failed"). It exists to make that refusal reproducible
offline.

Files:
  block.boc     - the candidate block, as the BlockBOC wire bytes.
  collated.boc  - the candidate collated data (proofs), as wire bytes.
  meta.json     - session/slot/leader, the candidate and predecessor block ids,
                  the min masterchain block id, sizes and sha256 of the two BOCs,
                  and the full error string.

NOT captured (reconstruct from a node holding the same chain, using the ids in
meta.json): predecessor STATE cells, the masterchain view block/state, and the
neighbour out-message queues (including NeighborShardEndLT). These are the live
node database and are far too large to copy per refusal.

To reproduce: load block.boc and collated.boc, resolve the predecessor states
and masterchain view for the ids in meta.json, and drive VerifyShardCandidate /
the semantic verifier over them.
`
