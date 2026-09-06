package localnet

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	statusTailBytes = 16 << 20
	maxLogLineBytes = 4 << 20
)

type messageCategory struct {
	Pattern  string
	Category string
}

var (
	ansiPattern       = regexp.MustCompile("\\x1b\\[[0-9;?]*[ -/]*[@-~]")
	goBlockPattern    = regexp.MustCompile(`wc=(-?\d+)\s+shard=([0-9a-fA-F]{16}|-?\d+)\s+seqno=(\d+)`)
	cppBlockPattern   = regexp.MustCompile(`\((-?\d+),([0-9a-fA-F]{16}),(\d+)\)`)
	cppBlockIDPattern = regexp.MustCompile(`\((-?\d+),([0-9a-fA-F]{16}),(\d+)\):([0-9a-fA-F]{64}):([0-9a-fA-F]{64})`)
	hardErrorMessages = []messageCategory{
		{Pattern: "validator session preparation failed", Category: "session_preparation_failed"},
		{Pattern: "validator session state update failed", Category: "session_update_failed"},
		{Pattern: "finalized block acceptance failed", Category: "finalized_block_acceptance_failed"},
		{Pattern: "candidate rejected", Category: "candidate_rejected"},
		{Pattern: " is rejected:", Category: "candidate_rejected"},
		{Pattern: "block collation failed", Category: "block_collation_failed"},
		{Pattern: "local producer rejected consensus progress; skipping window", Category: "producer_window_skipped"},
	}
)

type logStats struct {
	MasterchainSeqno  uint32
	Finalized         uint64
	Collated          uint64
	Emitted           uint64
	Validated         uint64
	HardErrors        uint64
	AdvisoryWarnings  uint64
	ErrorCategories   map[string]uint64
	WarningCategories map[string]uint64
	seenEmissions     map[candidateEmissionKey]struct{}
	seenFinalized     uniqueBlockSignals
	seenValidated     uniqueBlockSignals
}

type candidateEmissionKey struct {
	Node          string
	SessionID     string
	Slot          uint32
	CandidateHash string
}

type blockSignalKey struct {
	Node          string
	BlockRootHash string
	BlockFileHash string
}

type uniqueBlockSignals struct {
	blocks     map[blockSignalKey]struct{}
	candidates map[candidateEmissionKey]struct{}
}

func readActiveLogRange(node NodeConfig, start, end int64) ([]Event, logStats, error) {
	file, err := os.Open(node.LogPath)
	if err != nil {
		return nil, logStats{}, fmt.Errorf("open log %q: %w", node.Name, err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return nil, logStats{}, fmt.Errorf("stat log %q: %w", node.Name, err)
	}
	if end < 0 || end > info.Size() {
		end = info.Size()
	}
	if start < 0 || start > end {
		return nil, logStats{}, fmt.Errorf("log %q was truncated or rotated: range %d..%d, current size %d", node.Name, start, end, info.Size())
	}
	if _, err = file.Seek(start, io.SeekStart); err != nil {
		return nil, logStats{}, fmt.Errorf("seek log %q: %w", node.Name, err)
	}

	reader := bufio.NewReaderSize(io.LimitReader(file, end-start), 128<<10)
	events := make([]Event, 0, 256)
	var stats logStats
	for {
		line, readErr := reader.ReadBytes('\n')
		if len(line) > 0 {
			if len(line) <= maxLogLineBytes {
				event, parsed := parseLogLine(node, line)
				if parsed {
					events = append(events, event)
					stats.add(event)
				}
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return nil, logStats{}, fmt.Errorf("read log %q: %w", node.Name, readErr)
		}
	}
	return events, stats, nil
}

func readLogTail(node NodeConfig) ([]Event, logStats, int64, time.Time, error) {
	info, err := os.Stat(node.LogPath)
	if err != nil {
		return nil, logStats{}, 0, time.Time{}, fmt.Errorf("stat log %q: %w", node.Name, err)
	}
	start := max(int64(0), info.Size()-statusTailBytes)
	events, stats, err := readActiveLogRange(node, start, info.Size())
	return events, stats, info.Size(), info.ModTime(), err
}

func (s *logStats) add(event Event) {
	if event.Workchain == -1 && event.Seqno > s.MasterchainSeqno {
		s.MasterchainSeqno = event.Seqno
	}
	switch event.Kind {
	case "block_finalized":
		if !s.seenFinalized.record(event) {
			return
		}
		s.Finalized++
	case "block_collated":
		// This is completed work, not a unique consensus identity. Rebuilding
		// identical candidate bytes after a retry is real wasted work and remains
		// visible here; candidate_emitted is the unique production counter.
		s.Collated++
	case "candidate_emitted":
		if key, ok := candidateEmissionIdentity(event); ok {
			if s.seenEmissions == nil {
				s.seenEmissions = make(map[candidateEmissionKey]struct{})
			}
			if _, exists := s.seenEmissions[key]; exists {
				return
			}
			s.seenEmissions[key] = struct{}{}
		}
		s.Emitted++
	case "block_validated":
		if !s.seenValidated.record(event) {
			return
		}
		s.Validated++
	case "hard_error":
		s.HardErrors++
		addCategory(&s.ErrorCategories, event.Category)
	case "advisory_warning":
		s.AdvisoryWarnings++
		addCategory(&s.WarningCategories, event.Category)
	}
}

func (s *uniqueBlockSignals) record(event Event) bool {
	block, hasBlock := blockSignalIdentity(event)
	candidate, hasCandidate := candidateSignalIdentity(event)
	duplicate := false
	if hasBlock && s.blocks != nil {
		if _, exists := s.blocks[block]; exists {
			duplicate = true
		}
	}
	if hasCandidate && s.candidates != nil {
		if _, exists := s.candidates[candidate]; exists {
			duplicate = true
		}
	}
	if hasBlock {
		if s.blocks == nil {
			s.blocks = make(map[blockSignalKey]struct{})
		}
		s.blocks[block] = struct{}{}
	}
	if hasCandidate {
		if s.candidates == nil {
			s.candidates = make(map[candidateEmissionKey]struct{})
		}
		s.candidates[candidate] = struct{}{}
	}

	return !duplicate
}

func blockSignalIdentity(event Event) (blockSignalKey, bool) {
	if event.BlockRootHash == "" || event.BlockFileHash == "" {
		return blockSignalKey{}, false
	}

	return blockSignalKey{
		Node:          event.Node,
		BlockRootHash: strings.ToLower(event.BlockRootHash),
		BlockFileHash: strings.ToLower(event.BlockFileHash),
	}, true
}

func candidateSignalIdentity(event Event) (candidateEmissionKey, bool) {
	if event.Slot == nil || event.CandidateHash == "" {
		return candidateEmissionKey{}, false
	}

	return candidateEmissionKey{
		Node:          event.Node,
		SessionID:     event.SessionID,
		Slot:          *event.Slot,
		CandidateHash: strings.ToLower(event.CandidateHash),
	}, true
}

func candidateEmissionIdentity(event Event) (candidateEmissionKey, bool) {
	if event.Kind != "candidate_emitted" {
		return candidateEmissionKey{}, false
	}

	return candidateSignalIdentity(event)
}

func addCategory(categories *map[string]uint64, category string) {
	if category == "" {
		category = "uncategorized"
	}
	if *categories == nil {
		*categories = make(map[string]uint64)
	}
	(*categories)[category]++
}

func parseLogLine(node NodeConfig, raw []byte) (Event, bool) {
	line := strings.TrimSpace(ansiPattern.ReplaceAllString(string(bytes.TrimSpace(raw)), ""))
	if line == "" {
		return Event{}, false
	}
	if line[0] == '{' {
		if event, ok := parseJSONLog(node, []byte(line)); ok {
			return event, true
		}
	}
	return parseTextLog(node, line)
}

func parseJSONLog(node NodeConfig, line []byte) (Event, bool) {
	var record map[string]json.RawMessage
	if json.Unmarshal(line, &record) != nil {
		return Event{}, false
	}
	message := jsonString(record, "message")
	if message == "" {
		message = jsonString(record, "msg")
	}

	event := Event{Node: node.Name, Message: message}
	event.Time, _ = time.Parse(time.RFC3339Nano, jsonString(record, "time"))
	event.Workchain = int32(jsonInt64(record, "workchain"))
	event.Shard = jsonInt64(record, "shard")
	event.Seqno = uint32(jsonUint64(record, "block_seqno"))
	if event.Seqno == 0 {
		event.Seqno = uint32(jsonUint64(record, "seqno"))
	}
	event.SessionID = jsonString(record, "session_id")
	event.Slot = jsonOptionalUint32(record, "slot")
	event.WindowStart = jsonOptionalUint32(record, "window_start")
	event.WindowEnd = jsonOptionalUint32(record, "window_end")
	event.Transition = jsonString(record, "transition")
	event.Leader = jsonScalar(record, "leader")
	event.CandidateHash = strings.ToLower(jsonString(record, "candidate_hash"))
	event.BlockRootHash = strings.ToLower(jsonString(record, "block_root_hash"))
	event.BlockFileHash = strings.ToLower(jsonString(record, "block_file_hash"))
	event.Replayed = jsonBool(record, "replayed")
	event.Error = jsonScalar(record, "error")

	switch message {
	case "block collated":
		event.Kind = "block_collated"
		return event, true
	case "candidate emitted":
		event.Kind = "candidate_emitted"
		return event, true
	case "block validated":
		event.Kind = "block_validated"
		return event, true
	case "validator group lifecycle changed":
		event.Kind = "group_" + event.Transition
		return event, true
	case "next-block masterchain head applied":
		event.Kind = "masterchain_applied"
		if event.Seqno == 0 {
			fillBlockRef(&event, jsonString(record, "block"), goBlockPattern)
		}
		return event, true
	case "next-block catch-up progress":
		event.Kind = "masterchain_applied"
		fillBlockRef(&event, jsonString(record, "masterchain_head"), goBlockPattern)
		return event, true
	}
	if category := advisoryWarningCategory(message, event.Error); category != "" {
		event.Kind = "advisory_warning"
		event.Category = category
		return event, true
	}
	if category := hardErrorCategory(message); category != "" {
		event.Kind = "hard_error"
		event.Category = category
		return event, true
	}
	if message == "validator session stopped" && event.Error != "" {
		event.Kind = "hard_error"
		event.Category = "session_stopped"
		return event, true
	}
	return Event{}, false
}

func parseTextLog(node NodeConfig, line string) (Event, bool) {
	event := Event{Node: node.Name, Message: boundedMessage(line), Error: textField(line, "error")}
	if strings.Contains(line, "validator group lifecycle changed") {
		event.Transition = textField(line, "transition")
		event.SessionID = textField(line, "session_id")
		event.Workchain = int32(parseInt(textField(line, "workchain")))
		event.Shard = parseShard(textField(line, "shard"))
		event.Kind = "group_" + event.Transition
		return event, true
	}
	if strings.Contains(line, "block collated") {
		fillTextBlockEvent(&event, line)
		event.Kind = "block_collated"
		return event, true
	}
	if strings.Contains(line, "candidate emitted") {
		fillTextBlockEvent(&event, line)
		event.Replayed = textField(line, "replayed") == "true"
		event.Kind = "candidate_emitted"
		return event, true
	}
	if strings.Contains(line, "block validated") {
		fillTextBlockEvent(&event, line)
		event.Kind = "block_validated"
		return event, true
	}
	if strings.Contains(line, "next-block masterchain head applied") {
		event.Kind = "masterchain_applied"
		fillBlockRef(&event, line, goBlockPattern)
		return event, true
	}
	if strings.Contains(line, "next-block catch-up progress") {
		event.Kind = "masterchain_applied"
		fillBlockRef(&event, textField(line, "masterchain_head"), goBlockPattern)
		return event, true
	}
	if strings.Contains(line, "Published event") && strings.Contains(line, "FinalizeBlock") {
		event.Kind = "block_finalized"
		fillCPPBlockID(&event, line)
		return event, true
	}
	if strings.Contains(line, "Published event") && strings.Contains(line, "CandidateGenerated") {
		event.Kind = "candidate_emitted"
		fillCPPBlockID(&event, line)
		fillCPPCandidateIdentity(&event, line)
		return event, true
	}
	if strings.Contains(line, "CandidateReceived") {
		event.Kind = "block_validated"
		fillCPPBlockID(&event, line)
		fillCPPCandidateIdentity(&event, line)
		return event, true
	}
	if category := advisoryWarningCategory(line, event.Error); category != "" {
		event.Kind = "advisory_warning"
		event.Category = category
		return event, true
	}
	if category := hardErrorCategory(line); category != "" {
		event.Kind = "hard_error"
		event.Category = category
		return event, true
	}
	if strings.Contains(line, "validator session stopped") && textField(line, "error") != "" {
		event.Kind = "hard_error"
		event.Category = "session_stopped"
		return event, true
	}
	if strings.Contains(line, " FATAL ") {
		event.Kind = "hard_error"
		event.Category = "fatal"
		return event, true
	}
	if strings.Contains(line, "REJECT:") || strings.Contains(line, "CandidateReject") {
		event.Kind = "hard_error"
		event.Category = "candidate_rejected"
		return event, true
	}
	if strings.Contains(line, "prunned branch") || strings.Contains(line, "pruned branch") {
		event.Kind = "hard_error"
		event.Category = "branch_pruned"
		return event, true
	}
	return Event{}, false
}

func fillTextBlockEvent(event *Event, line string) {
	event.SessionID = textField(line, "session_id")
	event.Workchain = int32(parseInt(textField(line, "workchain")))
	event.Shard = parseShard(textField(line, "shard"))
	event.Seqno = uint32(parseUint(textField(line, "block_seqno")))
	if event.Seqno == 0 {
		event.Seqno = uint32(parseUint(textField(line, "seqno")))
	}
	event.Leader = textField(line, "leader")
	event.Slot = textOptionalUint32(line, "slot")
	event.WindowStart = textOptionalUint32(line, "window_start")
	event.WindowEnd = textOptionalUint32(line, "window_end")
	event.CandidateHash = strings.ToLower(textField(line, "candidate_hash"))
	event.BlockRootHash = strings.ToLower(textField(line, "block_root_hash"))
	event.BlockFileHash = strings.ToLower(textField(line, "block_file_hash"))
}

func fillCPPCandidateIdentity(event *Event, line string) {
	marker := "Candidate{id={"
	start := strings.Index(line, marker)
	if start < 0 {
		marker = "CandidateReceived{id={"
		start = strings.Index(line, marker)
	}
	if start < 0 {
		return
	}
	value := line[start+len(marker):]
	firstComma := strings.IndexByte(value, ',')
	if firstComma < 0 {
		return
	}
	slot := uint32(parseUint(strings.TrimSpace(value[:firstComma])))
	event.Slot = &slot
	value = strings.TrimSpace(value[firstComma+1:])
	secondComma := strings.IndexByte(value, ',')
	if secondComma < 0 {
		return
	}
	hash := strings.TrimSpace(value[:secondComma])
	decoded, err := base64.StdEncoding.DecodeString(hash)
	if err == nil && len(decoded) == 32 {
		event.CandidateHash = hex.EncodeToString(decoded)

		return
	}
	event.CandidateHash = strings.ToLower(hash)
}

func textOptionalUint32(line, key string) *uint32 {
	value := textField(line, key)
	if value == "" {
		return nil
	}
	number := uint32(parseUint(value))

	return &number
}

func fillBlockRef(event *Event, line string, pattern *regexp.Regexp) {
	match := pattern.FindStringSubmatch(line)
	if len(match) != 4 {
		return
	}
	event.Workchain = int32(parseInt(match[1]))
	event.Shard = parseShard(match[2])
	event.Seqno = uint32(parseUint(match[3]))
}

func fillCPPBlockID(event *Event, line string) {
	match := cppBlockIDPattern.FindStringSubmatch(line)
	if len(match) != 6 {
		fillBlockRef(event, line, cppBlockPattern)
		return
	}
	event.Workchain = int32(parseInt(match[1]))
	event.Shard = parseShard(match[2])
	event.Seqno = uint32(parseUint(match[3]))
	event.BlockRootHash = strings.ToLower(match[4])
	event.BlockFileHash = strings.ToLower(match[5])
}

func textField(line, key string) string {
	marker := key + "="
	position := strings.Index(line, marker)
	if position < 0 {
		return ""
	}
	value := line[position+len(marker):]
	if strings.HasPrefix(value, `"`) {
		value = value[1:]
		if end := strings.IndexByte(value, '"'); end >= 0 {
			return value[:end]
		}
	}
	if end := strings.IndexAny(value, " \t"); end >= 0 {
		value = value[:end]
	}
	return strings.Trim(value, `"`)
}

func jsonString(record map[string]json.RawMessage, key string) string {
	var value string
	_ = json.Unmarshal(record[key], &value)
	return value
}

func jsonScalar(record map[string]json.RawMessage, key string) string {
	value := record[key]
	if len(value) == 0 || bytes.Equal(value, []byte("null")) {
		return ""
	}
	var text string
	if json.Unmarshal(value, &text) == nil {
		return text
	}
	return string(value)
}

func jsonInt64(record map[string]json.RawMessage, key string) int64 {
	value := record[key]
	var number int64
	if json.Unmarshal(value, &number) == nil {
		return number
	}
	var text string
	if json.Unmarshal(value, &text) == nil {
		return parseShard(text)
	}
	return 0
}

func jsonUint64(record map[string]json.RawMessage, key string) uint64 {
	value := record[key]
	var number uint64
	if json.Unmarshal(value, &number) == nil {
		return number
	}
	var text string
	if json.Unmarshal(value, &text) == nil {
		return parseUint(text)
	}
	return 0
}

func jsonOptionalUint32(record map[string]json.RawMessage, key string) *uint32 {
	if _, exists := record[key]; !exists {
		return nil
	}
	number := uint32(jsonUint64(record, key))

	return &number
}

func jsonBool(record map[string]json.RawMessage, key string) bool {
	var value bool
	_ = json.Unmarshal(record[key], &value)

	return value
}

func parseInt(value string) int64 {
	number, _ := strconv.ParseInt(value, 10, 64)
	return number
}

func parseUint(value string) uint64 {
	number, _ := strconv.ParseUint(value, 10, 64)
	return number
}

func parseShard(value string) int64 {
	if value == "" {
		return 0
	}
	if strings.HasPrefix(value, "-") {
		return parseInt(value)
	}
	base := 10
	if len(value) == 16 && strings.IndexFunc(value, func(r rune) bool {
		return (r < '0' || r > '9') && (r < 'a' || r > 'f') && (r < 'A' || r > 'F')
	}) < 0 {
		base = 16
	}
	number, _ := strconv.ParseUint(value, base, 64)
	return int64(number)
}

func hardErrorCategory(message string) string {
	for _, entry := range hardErrorMessages {
		if strings.Contains(message, entry.Pattern) {
			return entry.Category
		}
	}
	return ""
}

func advisoryWarningCategory(message, detail string) string {
	switch {
	case message == "validator session preparation failed" &&
		(strings.Contains(detail, "validator runtime: block is not ready for acceptance") ||
			strings.Contains(detail, "collator: local acquisition not ready")):
		// Recovery can observe a durable session before the exact node state its
		// genesis references has been published. The supervisor deliberately
		// keeps the session tentative and retries it as catch-up advances, so this
		// condition is a readiness signal rather than a failed validator session.
		return "session_preparation_deferred"
	case message == "block collation failed" && strings.Contains(detail, context.Canceled.Error()):
		// Lifecycle supersession cancels an in-flight collation after a newer
		// consensus view has already made its result unusable. The runtime excludes
		// the same path from failed-production accounting.
		//
		// Matched as a substring, not compared: the collator wraps this error on
		// every path it can be raised on — it tests for it with errors.Is, never
		// by value — so an equality check here recognised the cancellation only
		// when it happened to surface unwrapped, and counted the rest as hard
		// errors against a scenario's max_hard_errors budget.
		return "block_collation_canceled"
	case strings.Contains(message, "finalized block acceptance failed; consensus continues"):
		return "finalized_block_acceptance_deferred"
	case strings.Contains(message, "block applied processor failed, retrying"):
		return "block_apply_retry"
	case strings.Contains(message, "standstill"):
		return "consensus_standstill"
	case strings.Contains(message, "dropping block publication without a signed validator contribution"):
		return "unsigned_publication_dropped"
	default:
		return ""
	}
}

func boundedMessage(message string) string {
	const limit = 4096
	if len(message) <= limit {
		return message
	}
	return message[:limit]
}
