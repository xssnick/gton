// Package genesis builds a complete, portable gton database from a declarative
// network genesis file. It owns genesis policy and never reads or writes a node
// configuration.
package genesis

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/bits"
	"os"
	"strings"

	"github.com/xssnick/tonutils-go/adnl/dht"
	"github.com/xssnick/tonutils-go/liteclient"
)

const (
	FormatVersion = 1
	GlobalID      = int32(-777)

	DefaultGenesisPath      = "genesis.json"
	DefaultDataPath         = "data"
	DefaultGlobalConfigPath = "global.config.json"
	DefaultLockPath         = "genesis.lock.json"

	defaultValidatorWeight = uint64(17)
)

var ErrTemplateIncomplete = errors.New("genesis template contains placeholders")

type Spec struct {
	FormatVersion     int                           `json:"format_version"`
	GlobalID          int32                         `json:"global_id"`
	GenesisTime       uint32                        `json:"genesis_time"`
	Validators        []Validator                   `json:"validators"`
	Consensus         Consensus                     `json:"consensus"`
	ValidatorRegistry *ValidatorRegistry            `json:"validator_registry,omitempty"`
	DHTNodes          []liteclient.DHTNode          `json:"dht_static_nodes"`
	Liteservers       []liteclient.LiteserverConfig `json:"liteservers"`
}

type Validator struct {
	PublicKey string `json:"public_key"`
	ADNLID    string `json:"adnl_id"`
	Weight    uint64 `json:"weight"`
}

type Consensus struct {
	TargetBlockRateMS     uint32 `json:"target_block_rate_ms"`
	SlotsPerLeaderWindow  uint32 `json:"slots_per_leader_window"`
	FirstBlockTimeoutMS   uint32 `json:"first_block_timeout_ms"`
	MasterGroupLifetime   uint32 `json:"master_group_lifetime_seconds"`
	ShardGroupLifetime    uint32 `json:"shard_group_lifetime_seconds"`
	MasterchainValidators uint32 `json:"masterchain_validators,omitempty"`
	ShardValidators       uint32 `json:"shard_validators"`
	ProtocolVersion       uint8  `json:"protocol_version"`
}

// ValidatorRegistry enables the on-chain mapping used by validators to
// advertise dedicated collator ADNL identities. Omit it on networks where
// validators collate their own blocks.
type ValidatorRegistry struct {
	MaxCollatorsPerValidator uint32 `json:"max_collators_per_validator"`
}

type validatedSpec struct {
	spec       Spec
	validators []validatorIdentity
}

type validatorIdentity struct {
	publicKey [ed25519.PublicKeySize]byte
	adnlID    [32]byte
	weight    uint64
}

func DefaultSpec() Spec {
	return Spec{
		FormatVersion: FormatVersion,
		GlobalID:      GlobalID,
		Validators: []Validator{
			{PublicKey: "<validator-1-ed25519-public-key-base64>", ADNLID: "<validator-1-adnl-id-base64>", Weight: defaultValidatorWeight},
			{PublicKey: "<validator-2-ed25519-public-key-base64>", ADNLID: "<validator-2-adnl-id-base64>", Weight: defaultValidatorWeight},
			{PublicKey: "<validator-3-ed25519-public-key-base64>", ADNLID: "<validator-3-adnl-id-base64>", Weight: defaultValidatorWeight},
		},
		Consensus: Consensus{
			TargetBlockRateMS:     200,
			SlotsPerLeaderWindow:  4,
			FirstBlockTimeoutMS:   700,
			MasterGroupLifetime:   250,
			ShardGroupLifetime:    250,
			MasterchainValidators: 3,
			ShardValidators:       3,
			ProtocolVersion:       3,
		},
		DHTNodes: []liteclient.DHTNode{
			{
				Type: "dht.node",
				ID:   liteclient.ServerID{Type: "pub.ed25519", Key: "<bootstrap-dht-ed25519-public-key-base64>"},
				AddrList: liteclient.DHTAddressList{
					Type:    "adnl.addressList",
					Addrs:   []liteclient.DHTAddress{{Type: "adnl.address.udp", IP: 2130706433, Port: 30304}},
					Version: 1,
				},
				Version:   1,
				Signature: "<bootstrap-dht-descriptor-signature-base64>",
			},
		},
		Liteservers: []liteclient.LiteserverConfig{},
	}
}

func LoadSpec(path string) (Spec, []byte, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Spec{}, nil, err
	}

	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var spec Spec
	if err = decoder.Decode(&spec); err != nil {
		return Spec{}, nil, fmt.Errorf("decode genesis: %w", err)
	}
	var trailing any
	if err = decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return Spec{}, nil, errors.New("decode genesis: trailing JSON value")
		}
		return Spec{}, nil, fmt.Errorf("decode genesis trailing data: %w", err)
	}
	return spec, raw, nil
}

func WriteTemplate(path string) error {
	raw, err := json.MarshalIndent(DefaultSpec(), "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	return writeNewFile(path, raw, 0o644)
}

func validateSpec(spec Spec) (validatedSpec, error) {
	if spec.FormatVersion != FormatVersion {
		return validatedSpec{}, fmt.Errorf("unsupported genesis format_version %d", spec.FormatVersion)
	}
	if spec.GlobalID == 0 {
		return validatedSpec{}, errors.New("global_id must not be zero")
	}
	if len(spec.Validators) == 0 || len(spec.Validators) > math.MaxUint16 {
		return validatedSpec{}, fmt.Errorf("validators count must be between 1 and %d", math.MaxUint16)
	}

	validators := make([]validatorIdentity, len(spec.Validators))
	publicKeys := make(map[[32]byte]struct{}, len(spec.Validators))
	adnlIDs := make(map[[32]byte]struct{}, len(spec.Validators))
	var totalWeight uint64
	for i, validator := range spec.Validators {
		publicKey, err := decodePublicValue(fmt.Sprintf("validators[%d].public_key", i), validator.PublicKey)
		if err != nil {
			return validatedSpec{}, err
		}
		adnlID, err := decodePublicValue(fmt.Sprintf("validators[%d].adnl_id", i), validator.ADNLID)
		if err != nil {
			return validatedSpec{}, err
		}
		if validator.Weight == 0 {
			return validatedSpec{}, fmt.Errorf("validators[%d].weight must be positive", i)
		}
		if _, exists := publicKeys[publicKey]; exists {
			return validatedSpec{}, fmt.Errorf("validators[%d].public_key is duplicated", i)
		}
		if _, exists := adnlIDs[adnlID]; exists {
			return validatedSpec{}, fmt.Errorf("validators[%d].adnl_id is duplicated", i)
		}
		var carry uint64
		totalWeight, carry = bits.Add64(totalWeight, validator.Weight, 0)
		if carry != 0 || totalWeight >= 1<<61 {
			return validatedSpec{}, errors.New("validator total weight is too large")
		}
		publicKeys[publicKey] = struct{}{}
		adnlIDs[adnlID] = struct{}{}
		validators[i] = validatorIdentity{publicKey: publicKey, adnlID: adnlID, weight: validator.Weight}
	}

	consensus := spec.Consensus
	if consensus.TargetBlockRateMS == 0 || consensus.SlotsPerLeaderWindow == 0 || consensus.FirstBlockTimeoutMS == 0 {
		return validatedSpec{}, errors.New("consensus block timings must be positive")
	}
	if consensus.MasterGroupLifetime == 0 || consensus.ShardGroupLifetime == 0 {
		return validatedSpec{}, errors.New("consensus group lifetimes must be positive")
	}
	if consensus.MasterchainValidators > uint32(len(validators)) {
		return validatedSpec{}, fmt.Errorf("consensus.masterchain_validators must not exceed %d", len(validators))
	}
	if consensus.ShardValidators == 0 || consensus.ShardValidators > uint32(len(validators)) {
		return validatedSpec{}, fmt.Errorf("consensus.shard_validators must be between 1 and %d", len(validators))
	}
	if consensus.ProtocolVersion != 3 {
		return validatedSpec{}, fmt.Errorf("consensus.protocol_version must be 3, got %d", consensus.ProtocolVersion)
	}
	if spec.ValidatorRegistry != nil && spec.ValidatorRegistry.MaxCollatorsPerValidator == 0 {
		return validatedSpec{}, errors.New("validator_registry.max_collators_per_validator must be positive")
	}
	if len(spec.DHTNodes) == 0 {
		return validatedSpec{}, errors.New("dht_static_nodes must contain at least one signed bootstrap descriptor")
	}
	if err := validateDHTNodes(spec.DHTNodes); err != nil {
		return validatedSpec{}, err
	}

	return validatedSpec{spec: spec, validators: validators}, nil
}

func decodePublicValue(field, value string) ([32]byte, error) {
	var result [32]byte
	value = strings.TrimSpace(value)
	if strings.HasPrefix(value, "<") && strings.HasSuffix(value, ">") {
		return result, fmt.Errorf("%w: %s", ErrTemplateIncomplete, field)
	}
	raw, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		return result, fmt.Errorf("%s is not base64: %w", field, err)
	}
	if len(raw) != len(result) {
		return result, fmt.Errorf("%s must decode to 32 bytes, got %d", field, len(raw))
	}
	copy(result[:], raw)
	return result, nil
}

func validateDHTNodes(nodes []liteclient.DHTNode) error {
	config := &liteclient.GlobalConfig{DHT: liteclient.DHTConfig{StaticNodes: liteclient.DHTNodes{Nodes: nodes}}}
	parsed, err := dht.BootstrapNodesFromConfig(config)
	if err != nil {
		return fmt.Errorf("parse dht_static_nodes: %w", err)
	}
	if len(parsed) != len(nodes) {
		return errors.New("dht_static_nodes contains a malformed public key, address, or signature")
	}
	for i, node := range parsed {
		if err = node.CheckSignature(); err != nil {
			return fmt.Errorf("dht_static_nodes[%d] has invalid signature: %w", i, err)
		}
	}
	return nil
}
