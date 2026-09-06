package localnet

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadLogRangeWithoutRotation(t *testing.T) {
	node, start := newLogRangeTest(t)
	appendLogRangeEvent(t, node.LogPath, 1)
	appendLogRangeEvent(t, node.LogPath, 2)
	end, err := captureLogPosition(node)
	if err != nil {
		t.Fatal(err)
	}
	assertLogRangeSeqnos(t, node, start, end, 1, 2)
}

func TestReadLogRangeAcrossPlainAndCompressedRotation(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		t.Run(fmt.Sprintf("compressed=%t", compressed), func(t *testing.T) {
			node, start := newLogRangeTest(t)
			appendLogRangeEvent(t, node.LogPath, 1)
			rotateLogRangeTest(t, node.LogPath, "2026-08-13T01-02-03.004", compressed)
			appendLogRangeEvent(t, node.LogPath, 2)
			end, err := captureLogPosition(node)
			if err != nil {
				t.Fatal(err)
			}
			assertLogRangeSeqnos(t, node, start, end, 1, 2)
		})
	}
}

func TestReadLogRangeAcrossTwoRotationsExactlyOnce(t *testing.T) {
	node, start := newLogRangeTest(t)
	appendLogRangeEvent(t, node.LogPath, 1)
	rotateLogRangeTest(t, node.LogPath, "2026-08-13T01-02-03.004", true)
	appendLogRangeEvent(t, node.LogPath, 2)
	rotateLogRangeTest(t, node.LogPath, "2026-08-13T01-02-04.005", false)
	appendLogRangeEvent(t, node.LogPath, 3)
	end, err := captureLogPosition(node)
	if err != nil {
		t.Fatal(err)
	}
	assertLogRangeSeqnos(t, node, start, end, 1, 2, 3)
}

func TestReadLogRangeCursorSurvivesCompressionAfterPolling(t *testing.T) {
	node, start := newLogRangeTest(t)
	appendLogRangeEvent(t, node.LogPath, 1)
	rotateLogRangeTest(t, node.LogPath, "2026-08-13T01-02-03.004", false)
	appendLogRangeEvent(t, node.LogPath, 2)
	middle, err := captureLogPosition(node)
	if err != nil {
		t.Fatal(err)
	}
	assertLogRangeSeqnos(t, node, start, middle, 1, 2)

	appendLogRangeEvent(t, node.LogPath, 3)
	rotateLogRangeTest(t, node.LogPath, "2026-08-13T01-02-04.005", true)
	appendLogRangeEvent(t, node.LogPath, 4)
	end, err := captureLogPosition(node)
	if err != nil {
		t.Fatal(err)
	}
	assertLogRangeSeqnos(t, node, middle, end, 3, 4)
}

func TestLogPositionStopsBeforePartialLine(t *testing.T) {
	node, start := newLogRangeTest(t)
	partial := `{"message":"block collated","workchain":0,`
	file, err := os.OpenFile(node.LogPath, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = file.WriteString(partial); err != nil {
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}

	middle, err := captureLogPosition(node)
	if err != nil {
		t.Fatal(err)
	}
	if middle.Offset != start.Offset {
		t.Fatalf("partial-line checkpoint = %d, want %d", middle.Offset, start.Offset)
	}
	assertLogRangeSeqnos(t, node, start, middle)

	file, err = os.OpenFile(node.LogPath, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = file.WriteString(`"block_seqno":7}` + "\n"); err != nil {
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
	end, err := captureLogPosition(node)
	if err != nil {
		t.Fatal(err)
	}
	assertLogRangeSeqnos(t, node, middle, end, 7)
}

func TestCompleteLogOffsetScansAcrossChunks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "go.jsonl")
	prefix := []byte("complete\n")
	partial := make([]byte, logOffsetScanBytes*2+1)
	for index := range partial {
		partial[index] = 'x'
	}
	if err := os.WriteFile(path, append(prefix, partial...), 0o600); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	offset, err := completeLogOffset(file, int64(len(prefix)+len(partial)))
	closeErr := file.Close()
	if err != nil {
		t.Fatal(err)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	if offset != int64(len(prefix)) {
		t.Fatalf("complete offset = %d, want %d", offset, len(prefix))
	}
}

func TestPlainLogGenerationSkipUsesSeek(t *testing.T) {
	path := filepath.Join(t.TempDir(), "large.jsonl")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	reader, err := openLogGeneration(logGeneration{path: path})
	if err != nil {
		t.Fatal(err)
	}
	const offset = int64(68) << 30
	if err = reader.skip(offset); err != nil {
		t.Fatal(err)
	}
	position, err := reader.seeker.Seek(0, io.SeekCurrent)
	closeErr := reader.Close()
	if err != nil {
		t.Fatal(err)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	if position != offset {
		t.Fatalf("plain generation position = %d, want %d", position, offset)
	}
}

func newLogRangeTest(t *testing.T) (NodeConfig, LogPosition) {
	t.Helper()
	node := NodeConfig{Name: "go-0", Kind: "go", LogPath: filepath.Join(t.TempDir(), "go.jsonl")}
	appendLogRangeEvent(t, node.LogPath, 100)
	position, err := captureLogPosition(node)
	if err != nil {
		t.Fatal(err)
	}
	return node, position
}

func appendLogRangeEvent(t *testing.T, path string, seqno uint32) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := fmt.Fprintf(file, `{"message":"block collated","workchain":0,"block_seqno":%d}`+"\n", seqno)
	closeErr := file.Close()
	if writeErr != nil {
		t.Fatal(writeErr)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
}

func rotateLogRangeTest(t *testing.T, activePath, timestamp string, compressed bool) {
	t.Helper()
	extension := filepath.Ext(activePath)
	backup := activePath[:len(activePath)-len(extension)] + "-" + timestamp + extension
	if err := os.Rename(activePath, backup); err != nil {
		t.Fatal(err)
	}
	if compressed {
		data, err := os.ReadFile(backup)
		if err != nil {
			t.Fatal(err)
		}
		file, err := os.Create(backup + ".gz")
		if err != nil {
			t.Fatal(err)
		}
		writer := gzip.NewWriter(file)
		if _, err = writer.Write(data); err != nil {
			t.Fatal(err)
		}
		if err = writer.Close(); err != nil {
			t.Fatal(err)
		}
		if err = file.Close(); err != nil {
			t.Fatal(err)
		}
		if err = os.Remove(backup); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(activePath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
}

func assertLogRangeSeqnos(t *testing.T, node NodeConfig, start, end LogPosition, want ...uint32) {
	t.Helper()
	events, _, err := readLogRange(node, start, end)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != len(want) {
		t.Fatalf("events = %+v, want seqnos %v", events, want)
	}
	for index, seqno := range want {
		if events[index].Seqno != seqno {
			t.Fatalf("event %d seqno = %d, want %d; all events = %+v", index, events[index].Seqno, seqno, events)
		}
	}
}

func TestParseJSONBlockAndSessionStop(t *testing.T) {
	node := NodeConfig{Name: "go-0", Kind: "go"}
	event, ok := parseLogLine(node, []byte(`{"level":"info","time":"2026-08-11T10:00:00Z","message":"block collated","workchain":0,"shard":-9223372036854775808,"block_seqno":42,"leader":3,"candidate_hash":"AABB"}`))
	if !ok || event.Kind != "block_collated" || event.Seqno != 42 || event.CandidateHash != "aabb" || event.Leader != "3" {
		t.Fatalf("event = %+v, parsed=%t", event, ok)
	}
	event, ok = parseLogLine(node, []byte(`{"level":"info","message":"candidate emitted","session_id":"ABCD","workchain":0,"shard":-9223372036854775808,"block_seqno":42,"slot":0,"window_start":0,"window_end":4,"leader":3,"candidate_hash":"AABB","block_root_hash":"CCDD","block_file_hash":"EEFF","replayed":true}`))
	if !ok || event.Kind != "candidate_emitted" || event.Slot == nil || *event.Slot != 0 ||
		event.WindowStart == nil || *event.WindowStart != 0 || event.WindowEnd == nil || *event.WindowEnd != 4 ||
		event.SessionID != "ABCD" || event.CandidateHash != "aabb" || event.BlockRootHash != "ccdd" ||
		event.BlockFileHash != "eeff" || !event.Replayed {
		t.Fatalf("emission = %+v, parsed=%t", event, ok)
	}

	if _, ok = parseLogLine(node, []byte(`{"message":"validator session stopped"}`)); ok {
		t.Fatal("benign session stop classified as an event")
	}
	event, ok = parseLogLine(node, []byte(`{"message":"validator session stopped","error":"backend failed"}`))
	if !ok || event.Kind != "hard_error" || event.Error != "backend failed" {
		t.Fatalf("stop event = %+v, parsed=%t", event, ok)
	}
}

func TestParseColoredLifecycleAndCPPEvents(t *testing.T) {
	line := "\x1b[90m12:00\x1b[0m INF validator group lifecycle changed transition=started workchain=0 shard=-9223372036854775808 session_id=abc"
	event, ok := parseLogLine(NodeConfig{Name: "go-0"}, []byte(line))
	if !ok || event.Kind != "group_started" || event.SessionID != "abc" || event.Shard != -9223372036854775808 {
		t.Fatalf("lifecycle = %+v, parsed=%t", event, ok)
	}

	cpp := "Published event CandidateGenerated {candidate=Candidate{id={17, AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=, ?}, parent=null, block=BlockCandidate{id=(0,4000000000000000,9):AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA:BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB}}}"
	event, ok = parseLogLine(NodeConfig{Name: "cpp", Kind: "cpp"}, []byte(cpp))
	if !ok || event.Kind != "candidate_emitted" || event.Slot == nil || *event.Slot != 17 || event.CandidateHash != "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f" || event.Seqno != 9 || event.BlockRootHash != strings.Repeat("a", 64) || event.BlockFileHash != strings.Repeat("b", 64) {
		t.Fatalf("C++ candidate = %+v, parsed=%t", event, ok)
	}
	trace := "Published event TraceEvent {event=CandidateReceived{id={17, AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=, ?}, parent=consensus genesis, block_id=(0,4000000000000000,9):AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA:BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB}}"
	event, ok = parseLogLine(NodeConfig{Name: "cpp", Kind: "cpp"}, []byte(trace))
	if !ok || event.Kind != "block_validated" || event.Slot == nil || *event.Slot != 17 || event.CandidateHash != "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f" || event.BlockRootHash != strings.Repeat("a", 64) || event.BlockFileHash != strings.Repeat("b", 64) {
		t.Fatalf("C++ trace candidate = %+v, parsed=%t", event, ok)
	}

	event, ok = parseLogLine(NodeConfig{Name: "cpp", Kind: "cpp"}, []byte("REJECT: invalid candidate"))
	if !ok || event.Kind != "hard_error" {
		t.Fatalf("C++ reject = %+v, parsed=%t", event, ok)
	}
}

func TestCandidateEmissionStatsDeduplicateByNodeSessionSlotAndHash(t *testing.T) {
	slot := uint32(17)
	events := []Event{
		{Node: "go", Kind: "block_collated", SessionID: "session-a", Slot: &slot, CandidateHash: "candidate"},
		{Node: "go", Kind: "candidate_emitted", SessionID: "session-a", Slot: &slot, CandidateHash: "candidate"},
		{Node: "go", Kind: "candidate_emitted", SessionID: "session-a", Slot: &slot, CandidateHash: "candidate", Replayed: true},
	}
	otherSlot := slot + 1
	events = append(events,
		Event{Node: "go", Kind: "candidate_emitted", SessionID: "session-a", Slot: &otherSlot, CandidateHash: "candidate"},
		Event{Node: "go", Kind: "candidate_emitted", SessionID: "session-b", Slot: &slot, CandidateHash: "candidate"},
		Event{Node: "cpp", Kind: "candidate_emitted", Slot: &slot, CandidateHash: "candidate"},
		Event{Node: "cpp", Kind: "candidate_emitted", Slot: &slot, CandidateHash: "candidate", Replayed: true},
	)

	var stats logStats
	for _, event := range events {
		stats.add(event)
	}
	if stats.Collated != 1 || stats.Emitted != 4 {
		t.Fatalf("collated=%d emitted=%d, want built=1 unique emissions=4", stats.Collated, stats.Emitted)
	}
}

func TestBlockStatsDeduplicateValidationAndFinalizationEvidence(t *testing.T) {
	slot := uint32(17)
	base := Event{
		Node: "cpp", Slot: &slot, CandidateHash: "candidate",
		BlockRootHash: "ROOT", BlockFileHash: "FILE",
	}
	otherFile := base
	otherFile.BlockFileHash = "other-file"
	otherFile.CandidateHash = "other-candidate"
	otherNode := base
	otherNode.Node = "cpp-2"
	fallback := Event{Node: "cpp", Slot: &slot, CandidateHash: "fallback"}

	var stats logStats
	for _, event := range []Event{
		withEventKind(base, "block_validated"),
		withEventKind(base, "block_validated"),
		withEventKind(otherFile, "block_validated"),
		withEventKind(otherNode, "block_validated"),
		withEventKind(fallback, "block_validated"),
		withEventKind(fallback, "block_validated"),
		withEventKind(base, "block_finalized"),
		withEventKind(base, "block_finalized"),
		withEventKind(otherFile, "block_finalized"),
	} {
		stats.add(event)
	}
	if stats.Validated != 4 || stats.Finalized != 2 {
		t.Fatalf("validated=%d finalized=%d, want unique per-node block/fallback identities 4/2", stats.Validated, stats.Finalized)
	}
}

func TestBlockStatsBridgeBlockAndFallbackIdentitiesInEitherOrder(t *testing.T) {
	slot := uint32(23)
	candidateOnly := Event{Node: "cpp", Kind: "block_validated", Slot: &slot, CandidateHash: "candidate"}
	blockAndCandidate := candidateOnly
	blockAndCandidate.BlockRootHash = "root"
	blockAndCandidate.BlockFileHash = "file"
	blockOnly := Event{Node: "cpp", Kind: "block_validated", BlockRootHash: "root", BlockFileHash: "file"}

	for _, events := range [][]Event{
		{candidateOnly, blockAndCandidate, blockOnly},
		{blockOnly, blockAndCandidate, candidateOnly},
	} {
		var stats logStats
		for _, event := range events {
			stats.add(event)
		}
		if stats.Validated != 1 {
			t.Fatalf("validated=%d for events %+v, want one identity bridged by the complete signal", stats.Validated, events)
		}
	}
}

func TestCollatedBlocksCountCompletedRetryBuilds(t *testing.T) {
	slot := uint32(9)
	build := Event{
		Node: "go", Kind: "block_collated", SessionID: "session", Slot: &slot,
		CandidateHash: "candidate", BlockRootHash: "root", BlockFileHash: "file",
	}

	var stats logStats
	stats.add(build)
	stats.add(build)
	if stats.Collated != 2 {
		t.Fatalf("collated=%d, want both completed build attempts", stats.Collated)
	}
}

func withEventKind(event Event, kind string) Event {
	event.Kind = kind

	return event
}

func TestCPPFinalizeBlockCountsOnlyPublishedEvent(t *testing.T) {
	node := NodeConfig{Name: "cpp", Kind: "cpp"}
	block := "(0,4000000000000000,9):" + strings.Repeat("A", 64) + ":" + strings.Repeat("B", 64)
	lines := []string{
		"Published event FinalizeBlock {block=" + block + "}",
		"Response for validator::consensus::FinalizeBlock@0xdeadbeef is ready",
	}

	var stats logStats
	parsed := 0
	for _, line := range lines {
		event, ok := parseLogLine(node, []byte(line))
		if !ok {
			continue
		}
		parsed++
		stats.add(event)
	}
	if parsed != 1 || stats.Finalized != 1 {
		t.Fatalf("parsed=%d finalized=%d, want one published finalization", parsed, stats.Finalized)
	}
}

func TestAllowedHardErrorsMatchesErrorDetail(t *testing.T) {
	events := []Event{{Kind: "hard_error", Message: "validator session stopped", Error: "expected test failure"}}
	if got := allowedHardErrors(events, []string{`expected test`}); got != 0 {
		t.Fatalf("hard errors = %d", got)
	}
	if got := allowedHardErrors(events, []string{`different`}); got != 1 {
		t.Fatalf("hard errors = %d", got)
	}
}

func TestAcceptanceConsensusContinuesIsAdvisory(t *testing.T) {
	node := NodeConfig{Name: "go", Kind: "go"}
	event, ok := parseLogLine(node, []byte(`{"message":"finalized block acceptance failed; consensus continues","error":"needs more than eight shard proof links"}`))
	if !ok || event.Kind != "advisory_warning" {
		t.Fatalf("advisory event = %+v, parsed=%t", event, ok)
	}
	event, ok = parseLogLine(node, []byte(`{"message":"finalized block acceptance failed","error":"write failed"}`))
	if !ok || event.Kind != "hard_error" {
		t.Fatalf("hard event = %+v, parsed=%t", event, ok)
	}

	var stats logStats
	stats.add(Event{Kind: "advisory_warning"})
	if stats.AdvisoryWarnings != 1 || stats.HardErrors != 0 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestOperationalFailuresKeepCategories(t *testing.T) {
	tests := []struct {
		name     string
		line     string
		kind     string
		category string
	}{
		{
			name:     "deferred session preparation",
			line:     `{"message":"validator session preparation failed","error":"validator runtime: load genesis chain state: validator runtime: block is not ready for acceptance: exact state metadata for block 8124 is unavailable"}`,
			kind:     "advisory_warning",
			category: "session_preparation_deferred",
		},
		{
			name:     "terminal session preparation",
			line:     `{"message":"validator session preparation failed","error":"validator runtime: corrupt consensus journal"}`,
			kind:     "hard_error",
			category: "session_preparation_failed",
		},
		{
			name:     "apply retry",
			line:     `{"message":"block applied processor failed, retrying","error":"local production is still running"}`,
			kind:     "advisory_warning",
			category: "block_apply_retry",
		},
		{
			name:     "skipped producer window",
			line:     `{"message":"local producer rejected consensus progress; skipping window","error":"session not found"}`,
			kind:     "hard_error",
			category: "producer_window_skipped",
		},
		{
			name:     "superseded collation",
			line:     `{"message":"block collation failed","error":"context canceled"}`,
			kind:     "advisory_warning",
			category: "block_collation_canceled",
		},
		{
			// The collator wraps this error on every path that raises it and tests
			// for it with errors.Is, so the classifier has to recognise it wrapped
			// or it counts an ordinary supersession as a hard error.
			name:     "wrapped superseded collation",
			line:     `{"message":"block collation failed","error":"open ready external stream: context canceled"}`,
			kind:     "advisory_warning",
			category: "block_collation_canceled",
		},
		{
			name:     "collation failed",
			line:     `{"message":"block collation failed","error":"commit candidate state failed"}`,
			kind:     "hard_error",
			category: "block_collation_failed",
		},
		{
			name:     "session cancellation remains terminal",
			line:     `{"message":"validator session preparation failed","error":"context canceled"}`,
			kind:     "hard_error",
			category: "session_preparation_failed",
		},
		{
			name:     "near miss readiness text remains terminal",
			line:     `{"message":"validator session preparation failed","error":"storage block is not ready for acceptance"}`,
			kind:     "hard_error",
			category: "session_preparation_failed",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			event, ok := parseLogLine(NodeConfig{Name: "go", Kind: "go"}, []byte(test.line))
			if !ok || event.Kind != test.kind || event.Category != test.category {
				t.Fatalf("event=%+v parsed=%t", event, ok)
			}
		})
	}
}
