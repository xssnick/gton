package localnet

import (
	"bufio"
	"bytes"
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

var (
	ansiPattern       = regexp.MustCompile("\\x1b\\[[0-9;?]*[ -/]*[@-~]")
	goBlockPattern    = regexp.MustCompile(`wc=(-?\d+)\s+shard=([0-9a-fA-F]{16}|-?\d+)\s+seqno=(\d+)`)
	cppBlockPattern   = regexp.MustCompile(`\((-?\d+),([0-9a-fA-F]{16}),(\d+)\)`)
	hardErrorMessages = []string{
		"validator session preparation failed",
		"validator session state update failed",
		"finalized block acceptance failed",
		"candidate rejected",
		" is rejected:",
	}
)

type logStats struct {
	MasterchainSeqno uint32
	Finalized        uint64
	Collated         uint64
	Validated        uint64
	HardErrors       uint64
	AdvisoryWarnings uint64
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
		s.Finalized++
	case "block_collated":
		s.Collated++
	case "block_validated":
		s.Validated++
	case "hard_error":
		s.HardErrors++
	case "advisory_warning":
		s.AdvisoryWarnings++
	}
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
	event.Transition = jsonString(record, "transition")
	event.Leader = jsonScalar(record, "leader")
	event.CandidateHash = strings.ToLower(jsonString(record, "candidate_hash"))
	event.Error = jsonScalar(record, "error")

	switch message {
	case "block collated":
		event.Kind = "block_collated"
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
	if isAdvisoryWarning(message) {
		event.Kind = "advisory_warning"
		return event, true
	}
	if isHardError(message) || message == "validator session stopped" && event.Error != "" {
		event.Kind = "hard_error"
		return event, true
	}
	return Event{}, false
}

func parseTextLog(node NodeConfig, line string) (Event, bool) {
	event := Event{Node: node.Name, Message: boundedMessage(line)}
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
	if strings.Contains(line, "FinalizeBlock") {
		event.Kind = "block_finalized"
		fillBlockRef(&event, line, cppBlockPattern)
		return event, true
	}
	if strings.Contains(line, "Published event") && strings.Contains(line, "CandidateGenerated") {
		event.Kind = "block_collated"
		fillBlockRef(&event, line, cppBlockPattern)
		event.CandidateHash = cppCandidateHash(line)
		return event, true
	}
	if strings.Contains(line, "CandidateReceived") {
		event.Kind = "block_validated"
		fillBlockRef(&event, line, cppBlockPattern)
		event.CandidateHash = cppCandidateHash(line)
		return event, true
	}
	if isAdvisoryWarning(line) {
		event.Kind = "advisory_warning"
		return event, true
	}
	if isHardError(line) || strings.Contains(line, "validator session stopped") && textField(line, "error") != "" || strings.Contains(line, " FATAL ") || strings.Contains(line, "REJECT:") || strings.Contains(line, "CandidateReject") || strings.Contains(line, "prunned branch") || strings.Contains(line, "pruned branch") {
		event.Kind = "hard_error"
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
	event.CandidateHash = strings.ToLower(textField(line, "candidate_hash"))
}

func cppCandidateHash(line string) string {
	const marker = "Candidate{id={"
	start := strings.Index(line, marker)
	if start < 0 {
		return ""
	}
	value := line[start+len(marker):]
	firstComma := strings.IndexByte(value, ',')
	if firstComma < 0 {
		return ""
	}
	value = strings.TrimSpace(value[firstComma+1:])
	secondComma := strings.IndexByte(value, ',')
	if secondComma < 0 {
		return ""
	}
	hash := strings.TrimSpace(value[:secondComma])
	decoded, err := base64.StdEncoding.DecodeString(hash)
	if err == nil && len(decoded) == 32 {
		return hex.EncodeToString(decoded)
	}
	return strings.ToLower(hash)
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

func isHardError(message string) bool {
	for _, pattern := range hardErrorMessages {
		if strings.Contains(message, pattern) {
			return true
		}
	}
	return false
}

func isAdvisoryWarning(message string) bool {
	return strings.Contains(message, "finalized block acceptance failed; consensus continues")
}

func boundedMessage(message string) string {
	const limit = 4096
	if len(message) <= limit {
		return message
	}
	return message[:limit]
}
