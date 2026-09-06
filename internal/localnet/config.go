package localnet

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"
)

type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalJSON(data []byte) error {
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return errors.New("duration must be a string")
	}

	duration, err := time.ParseDuration(value)
	if err != nil {
		return fmt.Errorf("parse duration %q: %w", value, err)
	}
	d.Duration = duration
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}

type Config struct {
	RunRoot    string           `json:"run_root"`
	Nodes      []NodeConfig     `json:"nodes"`
	Load       LoadConfig       `json:"load"`
	Deploy     DeployConfig     `json:"deploy"`
	Conditions ConditionsConfig `json:"conditions"`
}

type NodeConfig struct {
	Name         string   `json:"name"`
	Kind         string   `json:"kind"`
	Roles        []string `json:"roles"`
	LogPath      string   `json:"log_path"`
	ProcessMatch string   `json:"process_match"`
	Optional     bool     `json:"optional,omitempty"`
	Args         []string `json:"args,omitempty"`
	TMUXSession  string   `json:"tmux_session,omitempty"`
	StartCommand []string `json:"start_command,omitempty"`
}

type LoadConfig struct {
	Binary          string   `json:"binary"`
	WorkDir         string   `json:"work_dir,omitempty"`
	NodeConfig      string   `json:"node_config"`
	LiteAddress     string   `json:"lite_address"`
	StatePath       string   `json:"state_path"`
	FirstSender     uint64   `json:"first_sender,omitempty"`
	SenderCount     int      `json:"sender_count,omitempty"`
	SetupBeforeRun  bool     `json:"setup_before_run,omitempty"`
	SetupExtraArgs  []string `json:"setup_extra_args,omitempty"`
	Rate            int      `json:"rate"`
	Duration        Duration `json:"duration"`
	Drain           Duration `json:"drain"`
	Settle          Duration `json:"settle"`
	TopologyTimeout Duration `json:"topology_timeout,omitempty"`
	Recipients      int      `json:"recipients,omitempty"`
	MessageTON      string   `json:"message_ton,omitempty"`
	Jettons         string   `json:"jettons,omitempty"`
	ExtraArgs       []string `json:"extra_args,omitempty"`
}

type DeployConfig struct {
	TargetBinary  string   `json:"target_binary,omitempty"`
	HealthTimeout Duration `json:"health_timeout,omitempty"`
}

type ConditionsConfig struct {
	MinMasterchainAdvance uint32                `json:"min_masterchain_advance,omitempty"`
	MaxMasterchainLag     uint32                `json:"max_masterchain_lag,omitempty"`
	MinFinalizedBlocks    uint64                `json:"min_finalized_blocks,omitempty"`
	MaxHardErrors         int                   `json:"max_hard_errors,omitempty"`
	RequireSplit          bool                  `json:"require_split,omitempty"`
	RequireMerge          bool                  `json:"require_merge,omitempty"`
	AllowErrorPatterns    []string              `json:"allow_error_patterns,omitempty"`
	CandidateFlows        []CandidateFlowConfig `json:"candidate_flows,omitempty"`
}

type CandidateFlowConfig struct {
	Producer   string   `json:"producer"`
	Validators []string `json:"validators"`
	Finalizers []string `json:"finalizers"`
}

const (
	nodeRoleProducer  = "producer"
	nodeRoleValidator = "validator"
	nodeRoleFinalizer = "finalizer"
)

func LoadConfigFile(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read lab config: %w", err)
	}

	var cfg Config
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("decode lab config: %w", err)
	}

	base, err := filepath.Abs(filepath.Dir(path))
	if err != nil {
		return Config{}, fmt.Errorf("resolve lab config directory: %w", err)
	}
	cfg.resolvePaths(base)
	if err = cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c *Config) resolvePaths(base string) {
	c.RunRoot = resolvePath(base, c.RunRoot)
	c.Load.Binary = resolvePath(base, c.Load.Binary)
	c.Load.WorkDir = resolvePath(base, c.Load.WorkDir)
	c.Load.NodeConfig = resolvePath(base, c.Load.NodeConfig)
	c.Load.StatePath = resolvePath(base, c.Load.StatePath)
	c.Deploy.TargetBinary = resolvePath(base, c.Deploy.TargetBinary)
	for i := range c.Nodes {
		c.Nodes[i].LogPath = resolvePath(base, c.Nodes[i].LogPath)
	}
}

func resolvePath(base, value string) string {
	if value == "" || filepath.IsAbs(value) {
		return value
	}
	return filepath.Join(base, value)
}

func (c Config) Validate() error {
	if c.RunRoot == "" {
		return errors.New("lab config: run_root is required")
	}
	if len(c.Nodes) == 0 {
		return errors.New("lab config: at least one node is required")
	}

	names := make(map[string]struct{}, len(c.Nodes))
	for i, node := range c.Nodes {
		if node.Name == "" || node.LogPath == "" || node.ProcessMatch == "" && node.TMUXSession == "" {
			return fmt.Errorf("lab config: node %d requires name, log_path, and process_match or tmux_session", i)
		}
		if node.Kind != "go" && node.Kind != "cpp" {
			return fmt.Errorf("lab config: node %q kind must be go or cpp", node.Name)
		}
		if len(node.Roles) == 0 {
			return fmt.Errorf("lab config: node %q requires at least one role", node.Name)
		}
		roles := make(map[string]struct{}, len(node.Roles))
		for _, role := range node.Roles {
			if role != nodeRoleProducer && role != nodeRoleValidator && role != nodeRoleFinalizer {
				return fmt.Errorf("lab config: node %q has unknown role %q", node.Name, role)
			}
			if _, exists := roles[role]; exists {
				return fmt.Errorf("lab config: node %q repeats role %q", node.Name, role)
			}
			roles[role] = struct{}{}
		}
		if _, exists := names[node.Name]; exists {
			return fmt.Errorf("lab config: duplicate node name %q", node.Name)
		}
		names[node.Name] = struct{}{}
		if len(node.StartCommand) > 0 && node.TMUXSession == "" {
			return fmt.Errorf("lab config: node %q needs tmux_session for its start_command", node.Name)
		}
	}

	for _, pattern := range c.Conditions.AllowErrorPatterns {
		if strings.TrimSpace(pattern) == "" {
			return errors.New("lab config: empty allow_error_patterns entry")
		}
		if _, err := regexp.Compile(pattern); err != nil {
			return fmt.Errorf("lab config: compile allowed error pattern %q: %w", pattern, err)
		}
	}
	if c.Conditions.MaxHardErrors < 0 {
		return errors.New("lab config: max_hard_errors cannot be negative")
	}
	for index, flow := range c.Conditions.CandidateFlows {
		producer, exists := findNodeConfig(c.Nodes, flow.Producer)
		if !exists || !nodeHasRole(producer, nodeRoleProducer) {
			return fmt.Errorf("lab config: candidate flow %d producer %q is not a producer node", index, flow.Producer)
		}
		if len(flow.Validators) == 0 || len(flow.Finalizers) == 0 {
			return fmt.Errorf("lab config: candidate flow %d requires validators and finalizers", index)
		}
		if err := validateFlowTargets(c.Nodes, index, flow.Validators, nodeRoleValidator); err != nil {
			return err
		}
		if err := validateFlowTargets(c.Nodes, index, flow.Finalizers, nodeRoleFinalizer); err != nil {
			return err
		}
	}
	return nil
}

func validateFlowTargets(nodes []NodeConfig, flowIndex int, targets []string, role string) error {
	seen := make(map[string]struct{}, len(targets))
	for _, name := range targets {
		node, exists := findNodeConfig(nodes, name)
		if !exists || !nodeHasRole(node, role) {
			return fmt.Errorf("lab config: candidate flow %d target %q is not a %s node", flowIndex, name, role)
		}
		if _, exists := seen[name]; exists {
			return fmt.Errorf("lab config: candidate flow %d repeats %s target %q", flowIndex, role, name)
		}
		seen[name] = struct{}{}
	}
	return nil
}

func findNodeConfig(nodes []NodeConfig, name string) (NodeConfig, bool) {
	for _, node := range nodes {
		if node.Name == name {
			return node, true
		}
	}
	return NodeConfig{}, false
}

func nodeHasRole(node NodeConfig, role string) bool {
	return slices.Contains(node.Roles, role)
}

func (c Config) ValidateRun() error {
	if c.Load.Binary == "" || c.Load.NodeConfig == "" || c.Load.LiteAddress == "" || c.Load.StatePath == "" {
		return errors.New("lab config: load requires binary, node_config, lite_address, and state_path")
	}
	if c.Load.Rate <= 0 || c.Load.Duration.Duration <= 0 {
		return errors.New("lab config: load rate and duration must be positive")
	}
	// The drain is the loader's own upper bound on confirming that submitted
	// transfers reached their destinations, and it refuses a zero one: there is
	// no budget in which to observe a delivery. Rejected here, by name, rather
	// than as an opaque per-sender failure once the load is already running.
	if c.Load.Drain.Duration <= 0 {
		return errors.New("lab config: load drain must be positive")
	}
	if c.Load.Settle.Duration < 0 {
		return errors.New("lab config: load settle cannot be negative")
	}
	if c.Load.SenderCount < 0 || c.Load.SenderCount > 1024 {
		return errors.New("lab config: load sender_count must be between 0 and 1024")
	}
	if c.Load.SenderCount > 1 && !strings.Contains(c.Load.StatePath, "{sender}") {
		return errors.New("lab config: multi-sender state_path must contain {sender}")
	}
	return nil
}

func (c Config) ValidateDeploy() error {
	if c.Deploy.TargetBinary == "" {
		return errors.New("lab config: deploy.target_binary is required")
	}
	for _, node := range c.Nodes {
		if node.Kind == "go" && !node.Optional && len(node.StartCommand) == 0 {
			return fmt.Errorf("lab config: Go node %q has no deployment command", node.Name)
		}
		if node.Kind == "go" && len(node.StartCommand) > 0 {
			hasBinary := false
			for _, argument := range node.StartCommand {
				hasBinary = hasBinary || strings.Contains(argument, "{binary}")
			}
			if !hasBinary {
				return fmt.Errorf("lab config: Go node %q start_command has no {binary} placeholder", node.Name)
			}
		}
	}
	return nil
}
