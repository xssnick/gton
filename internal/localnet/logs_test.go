package localnet

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
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

	cpp := "Published event CandidateGenerated {candidate=Candidate{id={17, AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=, ?}, parent=null, block=BlockCandidate{id=(0,4000000000000000,9):AAAA:BBBB}}}"
	event, ok = parseLogLine(NodeConfig{Name: "cpp", Kind: "cpp"}, []byte(cpp))
	if !ok || event.Kind != "block_collated" || event.CandidateHash != "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f" || event.Seqno != 9 {
		t.Fatalf("C++ candidate = %+v, parsed=%t", event, ok)
	}

	event, ok = parseLogLine(NodeConfig{Name: "cpp", Kind: "cpp"}, []byte("REJECT: invalid candidate"))
	if !ok || event.Kind != "hard_error" {
		t.Fatalf("C++ reject = %+v, parsed=%t", event, ok)
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
