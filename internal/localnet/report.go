package localnet

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"time"

	"github.com/xssnick/gton/service/shard"
)

const (
	loadOutcomeComplete           = "complete"
	loadOutcomeDeliveryIncomplete = "load_delivery_incomplete"
	loadOutcomeFailed             = "failed"
)

func BuildReport(runDirectory string) (Summary, error) {
	manifest, err := readManifest(runDirectory)
	if err != nil {
		return Summary{}, err
	}

	events := make([]Event, 0, 1024)
	deltas := make([]NodeDelta, 0, len(manifest.Config.Nodes))
	for _, node := range manifest.Config.Nodes {
		baseline, err := findBaseline(manifest.Baseline, node.Name)
		if err != nil {
			return Summary{}, err
		}
		end, err := findEndPosition(manifest, node.Name)
		if err != nil {
			return Summary{}, err
		}
		nodeEvents, stats, err := readLogRange(node, baseline.Log, end)
		if err != nil {
			return Summary{}, err
		}
		events = append(events, nodeEvents...)
		deltas = append(deltas, NodeDelta{
			Name: node.Name, StartOffset: baseline.Log.Offset, EndOffset: end.Offset,
			MasterchainStart: baseline.MasterchainSeqno, MasterchainEnd: stats.MasterchainSeqno,
			FinalizedBlocks: stats.Finalized, CollatedBlocks: stats.Collated,
			ValidatedBlocks: stats.Validated, HardErrors: allowedHardErrors(nodeEvents, manifest.Config.Conditions.AllowErrorPatterns),
			AdvisoryWarnings: stats.AdvisoryWarnings,
		})
	}

	coverage := evaluateTopology(events, manifest.Config.Nodes)
	checks := evaluateChecks(manifest, deltas, coverage)
	consensusVerdict := "passed"
	for _, check := range checks {
		if !check.Passed {
			consensusVerdict = "failed"
			break
		}
	}
	if observesTopology(manifest.Scenario) && consensusVerdict == "passed" && !coverage.Complete {
		consensusVerdict = "inconclusive"
	}
	verdict := scenarioVerdict(manifest.Scenario, consensusVerdict, manifest.Load)

	finished := manifest.FinishedAt
	if finished.IsZero() {
		finished = time.Now().UTC()
	}
	summary := Summary{
		RunDirectory:       runDirectory,
		Scenario:           manifest.Scenario,
		StartedAt:          manifest.StartedAt,
		FinishedAt:         finished,
		Verdict:            verdict,
		ConsensusVerdict:   consensusVerdict,
		LoadDeliveryStatus: manifest.Load.Outcome,
		Load:               manifest.Load,
		Nodes:              deltas,
		Checks:             checks,
		Topology:           coverage,
		Events:             len(events),
	}
	if err = writeEvents(filepath.Join(runDirectory, "events.ndjson"), events); err != nil {
		return Summary{}, err
	}
	if err = writeJSON(filepath.Join(runDirectory, "summary.json"), summary); err != nil {
		return Summary{}, err
	}
	return summary, nil
}

func scenarioVerdict(scenario, consensus string, load LoadResult) string {
	if load.HardFailure || load.Outcome == loadOutcomeFailed {
		return "failed"
	}
	if load.Outcome == "" && (load.ExitCode != 0 || load.Error != "") {
		return "failed"
	}
	if load.Outcome == loadOutcomeDeliveryIncomplete && scenario != "topology-cycle" {
		return "failed"
	}
	return consensus
}

func evaluateChecks(manifest RunManifest, deltas []NodeDelta, coverage TopologyCoverage) []Check {
	conditions := manifest.Config.Conditions
	checks := make([]Check, 0, 6)
	if conditions.MinMasterchainAdvance > 0 {
		minimum := ^uint32(0)
		found := false
		for i, delta := range deltas {
			if manifest.Config.Nodes[i].Optional || delta.MasterchainStart == 0 || delta.MasterchainEnd < delta.MasterchainStart {
				continue
			}
			minimum = min(minimum, delta.MasterchainEnd-delta.MasterchainStart)
			found = true
		}
		if !found {
			minimum = 0
		}
		checks = append(checks, Check{Name: "masterchain_advance", Passed: minimum >= conditions.MinMasterchainAdvance, Actual: strconv.FormatUint(uint64(minimum), 10), Wanted: ">=" + strconv.FormatUint(uint64(conditions.MinMasterchainAdvance), 10)})
	}
	if conditions.MaxMasterchainLag > 0 {
		var low, high uint32
		for i, delta := range deltas {
			if manifest.Config.Nodes[i].Optional || delta.MasterchainEnd == 0 {
				continue
			}
			if low == 0 || delta.MasterchainEnd < low {
				low = delta.MasterchainEnd
			}
			high = max(high, delta.MasterchainEnd)
		}
		lag := high - low
		checks = append(checks, Check{Name: "masterchain_lag", Passed: lag <= conditions.MaxMasterchainLag, Actual: strconv.FormatUint(uint64(lag), 10), Wanted: "<=" + strconv.FormatUint(uint64(conditions.MaxMasterchainLag), 10)})
	}
	var finalized, hardErrors uint64
	for _, delta := range deltas {
		finalized = max(finalized, delta.FinalizedBlocks)
		hardErrors += delta.HardErrors
	}
	if conditions.MinFinalizedBlocks > 0 {
		checks = append(checks, Check{Name: "finalized_blocks", Passed: finalized >= conditions.MinFinalizedBlocks, Actual: strconv.FormatUint(finalized, 10), Wanted: ">=" + strconv.FormatUint(conditions.MinFinalizedBlocks, 10)})
	}
	checks = append(checks, Check{Name: "hard_errors", Passed: hardErrors <= uint64(conditions.MaxHardErrors), Actual: strconv.FormatUint(hardErrors, 10), Wanted: "<=" + strconv.Itoa(conditions.MaxHardErrors)})
	if conditions.RequireSplit {
		checks = append(checks, Check{Name: "split", Passed: coverage.Split, Actual: strconv.FormatBool(coverage.Split), Wanted: "true"})
	}
	if conditions.RequireMerge {
		checks = append(checks, Check{Name: "merge", Passed: coverage.Merge, Actual: strconv.FormatBool(coverage.Merge), Wanted: "true"})
	}
	return checks
}

type topologyState struct {
	stopped         map[int64]struct{}
	split           *splitGeneration
	splitParent     int64
	mergedParent    int64
	postMergeBlocks uint32
	coverage        TopologyCoverage
}

type splitGeneration struct {
	parent   int64
	sessions map[int64]string
	produced map[int64]struct{}
	rotated  bool
}

func evaluateTopology(events []Event, nodes []NodeConfig) TopologyCoverage {
	states := make(map[string]*topologyState)
	producers := make(map[string]struct{})
	validators := make(map[string]struct{})
	cppCandidates := make(map[string]struct{})
	goValidated := make(map[string]struct{})
	cppBlocks := make(map[string]struct{})
	goValidatedBlocks := make(map[string]struct{})
	var combined TopologyCoverage
	for _, event := range events {
		state := states[event.Node]
		if state == nil {
			state = &topologyState{
				stopped: map[int64]struct{}{},
			}
			states[event.Node] = state
		}
		state.observe(event)
		if event.Kind == "block_collated" {
			producers[event.Node] = struct{}{}
		}
		if event.Kind == "block_validated" {
			validators[event.Node] = struct{}{}
		}
		if event.CandidateHash != "" {
			kind := nodeKind(nodes, event.Node)
			if kind == "cpp" && event.Kind == "block_collated" {
				cppCandidates[event.CandidateHash] = struct{}{}
			}
			if kind == "go" && event.Kind == "block_validated" {
				goValidated[event.CandidateHash] = struct{}{}
			}
		}
		blockID := eventBlockID(event)
		if blockID != "" {
			kind := nodeKind(nodes, event.Node)
			if kind == "cpp" && event.Kind == "block_collated" {
				cppBlocks[blockID] = struct{}{}
			}
			if kind == "go" && event.Kind == "block_validated" {
				goValidatedBlocks[blockID] = struct{}{}
			}
		}
	}
	for _, state := range states {
		combined.LinearProof = combined.LinearProof || state.coverage.LinearProof
		combined.Split = combined.Split || state.coverage.Split
		combined.ChildrenProduced = combined.ChildrenProduced || state.coverage.ChildrenProduced
		combined.Rotation = combined.Rotation || state.coverage.Rotation
		combined.Merge = combined.Merge || state.coverage.Merge
		combined.AfterMergeProduced = combined.AfterMergeProduced || state.coverage.AfterMergeProduced
		combined.ReturnedToLinear = combined.ReturnedToLinear || state.coverage.ReturnedToLinear
	}
	combined.ProducerNodes = make([]string, 0, len(producers))
	for node := range producers {
		combined.ProducerNodes = append(combined.ProducerNodes, node)
	}
	slices.Sort(combined.ProducerNodes)
	combined.ValidationNodes = make([]string, 0, len(validators))
	for node := range validators {
		combined.ValidationNodes = append(combined.ValidationNodes, node)
	}
	slices.Sort(combined.ValidationNodes)
	combined.RequiredNodeCoverage = true
	for _, node := range nodes {
		if node.Optional {
			continue
		}
		_, collated := producers[node.Name]
		_, validated := validators[node.Name]
		if !collated || node.Kind == "go" && !validated {
			combined.RequiredNodeCoverage = false
		}
	}
	for candidate := range cppCandidates {
		if _, exists := goValidated[candidate]; exists {
			combined.CppToGoValidated = true
			break
		}
	}
	if !combined.CppToGoValidated {
		for blockID := range cppBlocks {
			if _, exists := goValidatedBlocks[blockID]; exists {
				combined.CppToGoValidated = true
				break
			}
		}
	}
	topologyComplete := combined.LinearProof && combined.Split && combined.ChildrenProduced &&
		combined.Rotation && combined.Merge && combined.AfterMergeProduced && combined.ReturnedToLinear
	parityComplete := combined.RequiredNodeCoverage && combined.CppToGoValidated
	combined.Complete = topologyComplete && parityComplete
	return combined
}

func eventBlockID(event Event) string {
	if event.BlockRootHash == "" || event.BlockFileHash == "" {
		return ""
	}
	return event.BlockRootHash + ":" + event.BlockFileHash
}

func nodeKind(nodes []NodeConfig, name string) string {
	for _, node := range nodes {
		if node.Name == name {
			return node.Kind
		}
	}
	return ""
}

func (s *topologyState) observe(event Event) {
	if event.Workchain != 0 || event.Shard == 0 {
		return
	}
	switch event.Kind {
	case "group_stopped":
		s.stopped[event.Shard] = struct{}{}
	case "group_started", "group_promoted":
		topologyChange := false
		parent, parentErr := shard.Parent(event.Shard)
		if parentErr == nil {
			if _, stopped := s.stopped[parent]; stopped {
				topologyChange = true
				if s.coverage.LinearProof && (s.splitParent == 0 || s.splitParent == parent) {
					s.splitParent = parent
					s.coverage.Split = true
					if s.split == nil {
						s.split = &splitGeneration{
							parent:   parent,
							sessions: map[int64]string{},
							produced: map[int64]struct{}{},
						}
					}
					if s.split.parent == parent {
						s.split.sessions[event.Shard] = event.SessionID
						if len(s.split.sessions) >= 2 {
							delete(s.stopped, parent)
						}
					}
				}
			}
		}
		left, leftErr := shard.Child(event.Shard, true)
		right, rightErr := shard.Child(event.Shard, false)
		_, leftStopped := s.stopped[left]
		_, rightStopped := s.stopped[right]
		if leftErr == nil && rightErr == nil && leftStopped && rightStopped {
			topologyChange = true
			if s.split != nil && s.split.parent == event.Shard {
				if s.split.rotated {
					s.coverage.Merge = true
					s.mergedParent = event.Shard
				}
				s.split = nil
			}
			delete(s.stopped, left)
			delete(s.stopped, right)
		}
		if !topologyChange && s.split != nil && s.coverage.ChildrenProduced {
			previous, splitChild := s.split.sessions[event.Shard]
			if splitChild && previous != "" && event.SessionID != "" && previous != event.SessionID {
				s.split.rotated = true
				s.coverage.Rotation = true
			}
		}
		if s.split != nil {
			if _, splitChild := s.split.sessions[event.Shard]; splitChild {
				s.split.sessions[event.Shard] = event.SessionID
			}
		}
	case "block_collated", "block_validated", "block_finalized":
		if !s.coverage.Split && event.Shard == shard.Root {
			s.coverage.LinearProof = true
		}
		if s.split != nil {
			if _, child := s.split.sessions[event.Shard]; child {
				s.split.produced[event.Shard] = struct{}{}
			}
			if len(s.split.produced) >= 2 {
				s.coverage.ChildrenProduced = true
			}
		}
		if s.coverage.Merge && event.Shard == s.mergedParent {
			s.postMergeBlocks++
			s.coverage.AfterMergeProduced = true
			if s.postMergeBlocks >= 2 {
				s.coverage.ReturnedToLinear = true
			}
		}
	}
}

func allowedHardErrors(events []Event, allowed []string) uint64 {
	patterns := make([]*regexp.Regexp, len(allowed))
	for i := range allowed {
		patterns[i] = regexp.MustCompile(allowed[i])
	}
	var count uint64
	for _, event := range events {
		if event.Kind != "hard_error" {
			continue
		}
		ignored := false
		for _, pattern := range patterns {
			if pattern.MatchString(event.Message + " " + event.Error) {
				ignored = true
				break
			}
		}
		if !ignored {
			count++
		}
	}
	return count
}

func isRunScenario(scenario string) bool {
	return scenario == "load" || scenario == "all" || scenario == "full-cycle" ||
		scenario == "topology-cycle"
}

func observesTopology(scenario string) bool {
	return scenario == "all" || scenario == "full-cycle" || scenario == "topology-cycle"
}

func loadStopsScenario(scenario string, load LoadResult) bool {
	if load.HardFailure || load.Outcome == loadOutcomeFailed {
		return true
	}
	if load.Outcome == loadOutcomeDeliveryIncomplete {
		return scenario != "topology-cycle"
	}
	return load.ExitCode != 0
}

func findBaseline(baseline Baseline, name string) (NodeBaseline, error) {
	for _, node := range baseline.Nodes {
		if node.Name == name {
			return node, nil
		}
	}
	return NodeBaseline{}, fmt.Errorf("run manifest has no baseline for node %q", name)
}

func findEndPosition(manifest RunManifest, name string) (LogPosition, error) {
	position, exists := manifest.EndPositions[name]
	if !exists {
		return LogPosition{}, fmt.Errorf("run manifest has no end log position for node %q", name)
	}
	return position, nil
}

func readManifest(runDirectory string) (RunManifest, error) {
	data, err := os.ReadFile(filepath.Join(runDirectory, "manifest.json"))
	if err != nil {
		return RunManifest{}, fmt.Errorf("read run manifest: %w", err)
	}
	var manifest RunManifest
	if err = json.Unmarshal(data, &manifest); err != nil {
		return RunManifest{}, fmt.Errorf("decode run manifest: %w", err)
	}
	if manifest.Version != runManifestVersion {
		return RunManifest{}, fmt.Errorf("unsupported run manifest version %d", manifest.Version)
	}
	return manifest, nil
}

func writeEvents(path string, events []Event) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create events: %w", err)
	}
	writer := bufio.NewWriterSize(file, 128<<10)
	encoder := json.NewEncoder(writer)
	for _, event := range events {
		if err = encoder.Encode(event); err != nil {
			_ = file.Close()
			return fmt.Errorf("encode events: %w", err)
		}
	}
	if err = writer.Flush(); err != nil {
		_ = file.Close()
		return fmt.Errorf("flush events: %w", err)
	}
	if err = file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync events: %w", err)
	}
	if err = file.Close(); err != nil {
		return fmt.Errorf("close events: %w", err)
	}
	return nil
}

func writeJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encode %s: %w", filepath.Base(path), err)
	}
	data = append(data, '\n')
	temporary := path + ".tmp"
	if err = os.WriteFile(temporary, data, 0o600); err != nil {
		return fmt.Errorf("write %s: %w", filepath.Base(path), err)
	}
	if err = os.Rename(temporary, path); err != nil {
		return fmt.Errorf("commit %s: %w", filepath.Base(path), err)
	}
	return nil
}

var errRunLocked = errors.New("another gton-lab run is active")
