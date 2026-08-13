package localnet

import (
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestTrimLogsDryRunAndApply(t *testing.T) {
	directory := t.TempDir()
	logPath := filepath.Join(directory, "node.log")
	if err := os.WriteFile(logPath, []byte("0123456789"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := Config{RunRoot: filepath.Join(directory, "runs"), Nodes: []NodeConfig{{Name: "go-0", LogPath: logPath}}}

	dryRun, err := TrimLogs(cfg, 4, false)
	if err != nil {
		t.Fatal(err)
	}
	if dryRun[0].Applied || dryRun[0].ArchivedBytes != 4 {
		t.Fatalf("dry run = %+v", dryRun)
	}
	if data, readErr := os.ReadFile(logPath); readErr != nil || string(data) != "0123456789" {
		t.Fatalf("log changed during dry run: %q, err=%v", data, readErr)
	}

	applied, err := TrimLogs(cfg, 4, true)
	if err != nil {
		t.Fatal(err)
	}
	if !applied[0].Applied {
		t.Fatalf("apply = %+v", applied)
	}
	if info, statErr := os.Stat(logPath); statErr != nil || info.Size() != 0 {
		t.Fatalf("trimmed log info=%v err=%v", info, statErr)
	}
	archive, err := os.Open(applied[0].ArchivePath)
	if err != nil {
		t.Fatal(err)
	}
	defer archive.Close()
	compressed, err := gzip.NewReader(archive)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(compressed)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "6789" {
		t.Fatalf("archive tail = %q", data)
	}
}
