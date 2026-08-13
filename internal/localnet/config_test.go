package localnet

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadConfigFileResolvesPaths(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "lab.json")
	data := `{
  "run_root":"runs",
  "nodes":[{"name":"go-0","kind":"go","log_path":"logs/go.jsonl","tmux_session":"go-0","start_command":["{binary}"]}],
  "load":{"binary":"bin/load","node_config":"node.json","lite_address":"127.0.0.1:7445","state_path":"states/{sender}.json","sender_count":10,"rate":30,"duration":"1m","drain":"10s","settle":"2s","topology_timeout":"10m"},
  "deploy":{"target_binary":"bin/node","health_timeout":"1m"},
  "conditions":{}
}`
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadConfigFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RunRoot != filepath.Join(directory, "runs") {
		t.Fatalf("run root = %q", cfg.RunRoot)
	}
	if cfg.Nodes[0].LogPath != filepath.Join(directory, "logs/go.jsonl") {
		t.Fatalf("node log = %q", cfg.Nodes[0].LogPath)
	}
	if cfg.Load.SenderCount != 10 || cfg.Load.TopologyTimeout.String() != "10m0s" {
		t.Fatalf("load config = %+v", cfg.Load)
	}
}

func TestValidateRunRequiresSenderPlaceholder(t *testing.T) {
	cfg := Config{Load: LoadConfig{
		Binary: "load", NodeConfig: "node.json", LiteAddress: "127.0.0.1:1",
		StatePath: "shared.json", SenderCount: 2, Rate: 1,
		Duration: Duration{Duration: 1},
	}}
	if err := cfg.ValidateRun(); err == nil || !strings.Contains(err.Error(), "{sender}") {
		t.Fatalf("ValidateRun error = %v", err)
	}
}
