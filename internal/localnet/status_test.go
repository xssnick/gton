package localnet

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCaptureStatusUsesUniqueConsensusCountersAndRawBuildAttempts(t *testing.T) {
	directory := t.TempDir()
	tmux := filepath.Join(directory, "tmux")
	if err := os.WriteFile(tmux, []byte("#!/bin/sh\nprintf '0 123 gton-node\\n'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))

	blockRoot := strings.Repeat("a", 64)
	blockFile := strings.Repeat("b", 64)
	block := "(0,4000000000000000,9):" + blockRoot + ":" + blockFile
	log := strings.Join([]string{
		`{"message":"block collated","session_id":"session","slot":4,"candidate_hash":"candidate","block_root_hash":"` + blockRoot + `","block_file_hash":"` + blockFile + `"}`,
		`{"message":"block collated","session_id":"session","slot":4,"candidate_hash":"candidate","block_root_hash":"` + blockRoot + `","block_file_hash":"` + blockFile + `"}`,
		`{"message":"candidate emitted","session_id":"session","slot":4,"candidate_hash":"candidate","block_root_hash":"` + blockRoot + `","block_file_hash":"` + blockFile + `"}`,
		`{"message":"candidate emitted","session_id":"session","slot":4,"candidate_hash":"candidate","block_root_hash":"` + blockRoot + `","block_file_hash":"` + blockFile + `","replayed":true}`,
		`{"message":"block validated","session_id":"session","slot":4,"candidate_hash":"candidate","block_root_hash":"` + blockRoot + `","block_file_hash":"` + blockFile + `"}`,
		`{"message":"block validated","session_id":"session","slot":4,"candidate_hash":"candidate","block_root_hash":"` + blockRoot + `","block_file_hash":"` + blockFile + `"}`,
		"Published event FinalizeBlock {block=" + block + "}",
		"Published event FinalizeBlock {block=" + block + "}",
	}, "\n") + "\n"
	node := NodeConfig{
		Name: "go", Kind: "go", Roles: []string{nodeRoleProducer, nodeRoleValidator, nodeRoleFinalizer},
		TMUXSession: "test", LogPath: filepath.Join(directory, "node.log"),
	}
	if err := os.WriteFile(node.LogPath, []byte(log), 0o600); err != nil {
		t.Fatal(err)
	}

	status, err := CaptureStatus(context.Background(), Config{Nodes: []NodeConfig{node}})
	if err != nil {
		t.Fatal(err)
	}
	if !status.Healthy || len(status.Nodes) != 1 {
		t.Fatalf("status = %+v", status)
	}
	got := status.Nodes[0]
	if got.CollatedBlocks != 2 || got.EmittedCandidates != 1 || got.ValidatedBlocks != 1 || got.FinalizedBlocks != 1 {
		t.Fatalf("node status counters = %+v", got)
	}
}
