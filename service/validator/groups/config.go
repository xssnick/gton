package groups

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/xssnick/gton/service/validator/simplex"
	"math"
	"math/bits"
	"time"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	configParamCatchain             = uint32(28)
	configParamConsensus            = uint32(29)
	configParamNewConsensus         = uint32(30)
	configParamFundamentalContracts = uint32(31)
	configParamPreviousValidators   = uint32(32)
	// configParamPreviousTemporary is named for completeness of the 32..37
	// block. It is never parsed: see ParseConfig.
	configParamPreviousTemporary    = uint32(33)
	configParamCurrentValidators    = uint32(34)
	configParamCurrentTemporary     = uint32(35)
	configParamNextValidators       = uint32(36)
	configParamNextTemporary        = uint32(37)
	configParamValidatorRegistry    = uint32(46)
	maxTotalValidatorWeight         = uint64(1) << 61
	ed25519PublicKeyConstructor     = uint64(0x8e81278a)
	validatorConstructor            = uint64(0x53)
	validatorAddressConstructor     = uint64(0x73)
	validatorSetConstructor         = uint64(0x11)
	validatorSetExtendedConstructor = uint64(0x12)
	defaultCandidateSizeLimit       = uint32(4 << 20)
)

// ErrNotFound reports that an optional configuration value is absent.
var ErrNotFound = errors.New("not found")

// Validator is a strictly decoded Ed25519 validator descriptor.
type Validator struct {
	PublicKey     [32]byte
	PublicKeyHash [32]byte
	ADNL          [32]byte
	Weight        uint64
}

// PersistentOverlayMember is one resolved ADNL identity from persistent
// validator sets 32/34/36. ValidatorKeyIDs retains every signing key mapped to
// the identity across rotations so local observers can be selected exactly.
type PersistentOverlayMember struct {
	ADNL            [32]byte
	ValidatorKeyIDs [][32]byte
}

// ValidatorSet is a complete, ordered validator set. Parsed sets and their
// validator slices are immutable and may be shared between config views.
type ValidatorSet struct {
	Since       uint32
	Until       uint32
	Main        uint16
	TotalWeight uint64
	Validators  []Validator

	cumulativeWeights []uint64
}

// CatchainConfig controls validator roster rotation.
type CatchainConfig struct {
	MasterchainLifetime     uint32
	ShardLifetime           uint32
	ShardValidatorsLifetime uint32
	ShardValidators         uint32
	ShuffleMasterchain      bool
}

// NoncriticalParam preserves a simplex v2 noncritical parameter, including
// parameters unknown to this implementation.
type NoncriticalParam struct {
	ID    uint8
	Value uint32
}

// SimplexConfig contains the #22 simplex configuration. The #21 constructor is
// a different schema and is deliberately not interpreted as #22. Version is
// kept because it is carried in durable session records and on the wire, where
// it still has to be checked against externally supplied values.
type SimplexConfig struct {
	Version               uint8
	Flags                 uint8
	ProtocolVersion       uint8
	UseQUIC               bool
	SlotsPerLeaderWindow  uint32
	MaxLeaderWindowDesync uint32
	NoncriticalParams     []NoncriticalParam
}

// NewConsensusConfig contains independently optional masterchain and shard
// simplex configurations.
type NewConsensusConfig struct {
	Masterchain *SimplexConfig
	Shard       *SimplexConfig
}

// Config contains the validator-group configuration needed by the tracker.
// ActiveValidators selects parameter 35 over 34. NextValidators selects 37
// over 36 and is nil when neither next set is present. AllCurrentValidators is
// the ADNL roster of persistent parameter 34, matching the reference private
// overlay's all_current_validators set. Parsed values are immutable and may
// share their backing storage.
type Config struct {
	Catchain             CatchainConfig
	NewConsensus         NewConsensusConfig
	MaxBlockSize         uint32
	MaxCollatedDataSize  uint32
	ActiveValidators     ValidatorSet
	NextValidators       *ValidatorSet
	AllCurrentValidators [][32]byte

	persistentOverlayMembers []PersistentOverlayMember
	persistentValidatorSets  []ValidatorSet
	fundamentalContracts     *cell.Cell
	validatorRegistry        *cell.Cell
}

type dictionaryEntry struct {
	Key   [32]byte
	Value *cell.Slice
}

// ParseConfig strictly decodes the configuration parameter dictionary carried
// by an applied masterchain state.
func ParseConfig(root *cell.Cell) (*Config, error) {
	entries, err := parseHashmap(root, 32)
	if err != nil {
		return nil, fmt.Errorf("parse config parameter dictionary: %w", err)
	}

	params := make(map[uint32]*cell.Cell, len(entries))
	for i := range entries {
		parameter := binary.BigEndian.Uint32(entries[i].Key[:4])
		parameterCell, err := entries[i].Value.LoadRefCell()
		if err != nil {
			return nil, fmt.Errorf("config parameter %d wrapper: %w", parameter, err)
		}
		if err = requireEmpty(entries[i].Value); err != nil {
			return nil, fmt.Errorf("config parameter %d wrapper: %w", parameter, err)
		}

		params[parameter] = parameterCell
	}

	catchain, err := parseCatchainConfig(params[configParamCatchain])
	if err != nil {
		return nil, fmt.Errorf("config parameter %d: %w", configParamCatchain, err)
	}

	newConsensus, err := parseNewConsensusConfig(params[configParamNewConsensus])
	if err != nil {
		return nil, fmt.Errorf("config parameter %d: %w", configParamNewConsensus, err)
	}
	maxBlockSize, maxCollatedDataSize, err := parseConsensusLimits(params[configParamConsensus])
	if err != nil {
		return nil, fmt.Errorf("config parameter %d: %w", configParamConsensus, err)
	}

	// Only the sets something downstream reads are parsed. Parameter 33 is
	// deliberately absent: no consumer here selects it and the C++ node never
	// looks it up either (get_config_param(35, 34), (37, 36), 34 and 36 are its
	// only validator-set lookups). Parsing it strictly would let an odd value
	// nobody uses fail ParseConfig, which fails masterchain apply and — worse —
	// refuses collation and validation through master_state.go. Parameter 32
	// keeps its strict parse because persistentOverlayMembers is built from it.
	validatorSets := make(map[uint32]ValidatorSet, 5)
	for _, parameter := range [...]uint32{
		configParamPreviousValidators,
		configParamCurrentValidators,
		configParamCurrentTemporary,
		configParamNextValidators,
		configParamNextTemporary,
	} {
		parameterCell, exists := params[parameter]
		if !exists {
			continue
		}

		validatorSet, err := parseValidatorSet(parameterCell)
		if err != nil {
			return nil, fmt.Errorf("config parameter %d: %w", parameter, err)
		}
		validatorSets[parameter] = validatorSet
	}

	current, exists := validatorSets[configParamCurrentValidators]
	if !exists {
		return nil, fmt.Errorf("config parameter %d: %w", configParamCurrentValidators, ErrNotFound)
	}

	active := current
	if temporary, exists := validatorSets[configParamCurrentTemporary]; exists {
		active = temporary
	}

	var next *ValidatorSet
	if temporary, exists := validatorSets[configParamNextTemporary]; exists {
		next = &temporary
	} else if persistent, exists := validatorSets[configParamNextValidators]; exists {
		next = &persistent
	}

	persistentValidatorSets := make([]ValidatorSet, 0, 3)
	for _, parameter := range [...]uint32{
		configParamPreviousValidators,
		configParamCurrentValidators,
		configParamNextValidators,
	} {
		if validatorSet, exists := validatorSets[parameter]; exists {
			persistentValidatorSets = append(persistentValidatorSets, validatorSet)
		}
	}

	return &Config{
		Catchain:                 catchain,
		NewConsensus:             newConsensus,
		MaxBlockSize:             maxBlockSize,
		MaxCollatedDataSize:      maxCollatedDataSize,
		ActiveValidators:         active,
		NextValidators:           next,
		AllCurrentValidators:     validatorADNLIDs(current.Validators),
		persistentOverlayMembers: buildPersistentOverlayMembers(persistentValidatorSets),
		persistentValidatorSets:  persistentValidatorSets,
		fundamentalContracts:     params[configParamFundamentalContracts],
		validatorRegistry:        params[configParamValidatorRegistry],
	}, nil
}

func parseCatchainConfig(configCell *cell.Cell) (CatchainConfig, error) {
	if configCell == nil {
		// These are the protocol defaults for an absent parameter 28.
		// A present malformed parameter is rejected below and never falls back.
		return CatchainConfig{
			MasterchainLifetime:     200,
			ShardLifetime:           200,
			ShardValidatorsLifetime: 3000,
			ShardValidators:         7,
		}, nil
	}

	s, err := configCell.BeginParse()
	if err != nil {
		return CatchainConfig{}, err
	}

	constructor, err := s.LoadUInt(8)
	if err != nil {
		return CatchainConfig{}, fmt.Errorf("load constructor: %w", err)
	}

	var result CatchainConfig
	switch constructor {
	case 0xc1:
	case 0xc2:
		flags, err := s.LoadUInt(7)
		if err != nil {
			return CatchainConfig{}, fmt.Errorf("load flags: %w", err)
		}
		if flags != 0 {
			return CatchainConfig{}, fmt.Errorf("flags must be zero, got %d", flags)
		}

		result.ShuffleMasterchain, err = s.LoadBoolBit()
		if err != nil {
			return CatchainConfig{}, fmt.Errorf("load shuffle_mc_validators: %w", err)
		}
	default:
		return CatchainConfig{}, fmt.Errorf("unsupported constructor #%02x", constructor)
	}

	result.MasterchainLifetime, err = loadUint32(s, "mc_catchain_lifetime")
	if err != nil {
		return CatchainConfig{}, err
	}
	result.ShardLifetime, err = loadUint32(s, "shard_catchain_lifetime")
	if err != nil {
		return CatchainConfig{}, err
	}
	result.ShardValidatorsLifetime, err = loadUint32(s, "shard_validators_lifetime")
	if err != nil {
		return CatchainConfig{}, err
	}
	result.ShardValidators, err = loadUint32(s, "shard_validators_num")
	if err != nil {
		return CatchainConfig{}, err
	}

	if err = requireEmpty(s); err != nil {
		return CatchainConfig{}, err
	}
	if result.MasterchainLifetime == 0 || result.ShardLifetime == 0 ||
		result.ShardValidatorsLifetime == 0 || result.ShardValidators == 0 {
		return CatchainConfig{}, errors.New("all lifetime and validator-count fields must be positive")
	}

	return result, nil
}

func parseConsensusLimits(configCell *cell.Cell) (uint32, uint32, error) {
	if configCell == nil {
		return defaultCandidateSizeLimit, defaultCandidateSizeLimit, nil
	}

	s, err := configCell.BeginParse()
	if err != nil {
		return 0, 0, err
	}

	constructor, err := s.LoadUInt(8)
	if err != nil {
		return 0, 0, fmt.Errorf("load constructor: %w", err)
	}

	// Only consensus_config_v4#d9 is accepted. This node does not run the
	// catchain-era consensus, and parameter 29 survives here solely as the
	// carrier of the two size limits below; the pre-v4 constructors
	// (#d6/#d7/#d8) belong to chains this node cannot validate anyway, so
	// accepting them would only defer the failure to a less obvious place.
	var roundCandidates uint64
	var flagBits, boolBits uint
	switch constructor {
	case 0xd9:
		flagBits, boolBits = 6, 2
	default:
		return 0, 0, fmt.Errorf("unsupported constructor #%02x", constructor)
	}
	if flagBits != 0 {
		// `if err == nil` rather than `:=`, so the post-switch check below sees
		// this error instead of a shadowed nil.
		var flags uint64
		flags, err = s.LoadUInt(flagBits)
		if err == nil && flags != 0 {
			return 0, 0, fmt.Errorf("flags must be zero, got %d", flags)
		}
		if err == nil {
			err = s.SkipBits(boolBits)
		}
		if err == nil {
			roundCandidates, err = s.LoadUInt(8)
		}
	}
	if err != nil {
		return 0, 0, fmt.Errorf("load consensus header: %w", err)
	}
	if roundCandidates == 0 {
		return 0, 0, errors.New("round_candidates must be positive")
	}

	for _, field := range []string{
		"next_candidate_delay_ms",
		"consensus_timeout_ms",
		"fast_attempts",
		"attempt_duration",
		"catchain_max_deps",
	} {
		if _, err = loadUint32(s, field); err != nil {
			return 0, 0, err
		}
	}
	maxBlockSize, err := loadUint32(s, "max_block_bytes")
	if err != nil {
		return 0, 0, err
	}
	maxCollatedDataSize, err := loadUint32(s, "max_collated_bytes")
	if err != nil {
		return 0, 0, err
	}

	// consensus_config_v4#d9 tail; the constructor switch above admits nothing
	// else, so there is no other shape to branch on.
	if _, err = s.LoadUInt(16); err != nil {
		return 0, 0, fmt.Errorf("load proto_version: %w", err)
	}
	if _, err = loadUint32(s, "catchain_max_blocks_coeff"); err != nil {
		return 0, 0, err
	}
	if err = requireEmpty(s); err != nil {
		return 0, 0, err
	}

	return maxBlockSize, maxCollatedDataSize, nil
}

func parseNewConsensusConfig(configCell *cell.Cell) (NewConsensusConfig, error) {
	if configCell == nil {
		// Parameter 30 is optional; the zero value selects legacy consensus.
		return NewConsensusConfig{}, nil
	}

	s, err := configCell.BeginParse()
	if err != nil {
		return NewConsensusConfig{}, err
	}

	constructor, err := s.LoadUInt(8)
	if err != nil {
		return NewConsensusConfig{}, fmt.Errorf("load constructor: %w", err)
	}
	if constructor != 0x10 {
		return NewConsensusConfig{}, fmt.Errorf("unsupported constructor #%02x", constructor)
	}

	masterchain, err := parseMaybeSimplexConfig(s)
	if err != nil {
		return NewConsensusConfig{}, fmt.Errorf("masterchain config: %w", err)
	}

	shard, err := parseMaybeSimplexConfig(s)
	if err != nil {
		return NewConsensusConfig{}, fmt.Errorf("shard config: %w", err)
	}
	if err = requireEmpty(s); err != nil {
		return NewConsensusConfig{}, err
	}

	return NewConsensusConfig{Masterchain: masterchain, Shard: shard}, nil
}

func parseMaybeSimplexConfig(s *cell.Slice) (*SimplexConfig, error) {
	present, err := s.LoadBoolBit()
	if err != nil {
		return nil, err
	}
	if !present {
		return nil, nil
	}

	configCell, err := s.LoadRefCell()
	if err != nil {
		return nil, err
	}
	config, err := parseSimplexConfig(configCell)
	if err != nil {
		return nil, err
	}

	return &config, nil
}

func parseSimplexConfig(configCell *cell.Cell) (SimplexConfig, error) {
	s, err := configCell.BeginParse()
	if err != nil {
		return SimplexConfig{}, err
	}

	constructor, err := s.LoadUInt(8)
	if err != nil {
		return SimplexConfig{}, fmt.Errorf("load constructor: %w", err)
	}

	var result SimplexConfig
	switch constructor {
	case 0x22:
		result.Version = 2
		flags, err := s.LoadUInt(5)
		if err != nil {
			return SimplexConfig{}, fmt.Errorf("load flags: %w", err)
		}
		result.Flags = uint8(flags)
		protocolVersion, err := s.LoadUInt(2)
		if err != nil {
			return SimplexConfig{}, fmt.Errorf("load protocol_version: %w", err)
		}
		result.ProtocolVersion = uint8(protocolVersion)
		result.UseQUIC, err = s.LoadBoolBit()
		if err != nil {
			return SimplexConfig{}, fmt.Errorf("load use_quic: %w", err)
		}
		result.SlotsPerLeaderWindow, err = loadUint32(s, "slots_per_leader_window")
		if err != nil {
			return SimplexConfig{}, err
		}

		entries, err := parseHashmapE(s, 8)
		if err != nil {
			return SimplexConfig{}, fmt.Errorf("noncritical_params: %w", err)
		}
		result.NoncriticalParams = make([]NoncriticalParam, len(entries))
		for i := range entries {
			value, err := entries[i].Value.LoadUInt(32)
			if err != nil {
				return SimplexConfig{}, fmt.Errorf("noncritical parameter %d: %w", entries[i].Key[0], err)
			}
			if err = requireEmpty(entries[i].Value); err != nil {
				return SimplexConfig{}, fmt.Errorf("noncritical parameter %d: %w", entries[i].Key[0], err)
			}

			result.NoncriticalParams[i] = NoncriticalParam{ID: entries[i].Key[0], Value: uint32(value)}
		}
	default:
		return SimplexConfig{}, fmt.Errorf("unsupported constructor #%02x", constructor)
	}

	if err = requireEmpty(s); err != nil {
		return SimplexConfig{}, err
	}
	if result.SlotsPerLeaderWindow == 0 {
		return SimplexConfig{}, errors.New("slots_per_leader_window must be positive")
	}

	return result, nil
}

func parseValidatorSet(setCell *cell.Cell) (ValidatorSet, error) {
	s, err := setCell.BeginParse()
	if err != nil {
		return ValidatorSet{}, err
	}

	constructor, err := s.LoadUInt(8)
	if err != nil {
		return ValidatorSet{}, fmt.Errorf("load constructor: %w", err)
	}
	if constructor != validatorSetConstructor && constructor != validatorSetExtendedConstructor {
		return ValidatorSet{}, fmt.Errorf("unsupported constructor #%02x", constructor)
	}

	var result ValidatorSet
	result.Since, err = loadUint32(s, "utime_since")
	if err != nil {
		return ValidatorSet{}, err
	}
	result.Until, err = loadUint32(s, "utime_until")
	if err != nil {
		return ValidatorSet{}, err
	}
	total, err := s.LoadUInt(16)
	if err != nil {
		return ValidatorSet{}, fmt.Errorf("load total: %w", err)
	}
	main, err := s.LoadUInt(16)
	if err != nil {
		return ValidatorSet{}, fmt.Errorf("load main: %w", err)
	}
	if main == 0 || main > total {
		return ValidatorSet{}, fmt.Errorf("main validator count %d is outside 1..%d", main, total)
	}
	result.Main = uint16(main)

	var entries []dictionaryEntry
	var declaredTotalWeight uint64
	if constructor == validatorSetConstructor {
		root, err := s.ToCell()
		if err != nil {
			return ValidatorSet{}, fmt.Errorf("validator dictionary: %w", err)
		}
		entries, err = parseHashmap(root, 16)
		if err != nil {
			return ValidatorSet{}, fmt.Errorf("validator dictionary: %w", err)
		}
	} else {
		declaredTotalWeight, err = s.LoadUInt(64)
		if err != nil {
			return ValidatorSet{}, fmt.Errorf("load total_weight: %w", err)
		}
		if declaredTotalWeight == 0 {
			return ValidatorSet{}, errors.New("declared total weight must be positive")
		}

		entries, err = parseHashmapE(s, 16)
		if err != nil {
			return ValidatorSet{}, fmt.Errorf("validator dictionary: %w", err)
		}
		if err = requireEmpty(s); err != nil {
			return ValidatorSet{}, err
		}
	}

	if len(entries) != int(total) {
		return ValidatorSet{}, fmt.Errorf("validator dictionary has %d entries, declared total is %d", len(entries), total)
	}

	result.Validators = make([]Validator, len(entries))
	result.cumulativeWeights = make([]uint64, len(entries))
	publicKeys := make(map[[32]byte]struct{}, len(entries))
	publicKeyHashes := make(map[[32]byte]struct{}, len(entries))
	for i := range entries {
		index := binary.BigEndian.Uint16(entries[i].Key[:2])
		if index != uint16(i) {
			return ValidatorSet{}, fmt.Errorf("validator dictionary index %d, want %d", index, i)
		}

		validator, err := parseValidator(entries[i].Value)
		if err != nil {
			return ValidatorSet{}, fmt.Errorf("validator %d: %w", i, err)
		}
		if _, duplicate := publicKeys[validator.PublicKey]; duplicate {
			return ValidatorSet{}, fmt.Errorf("validator %d duplicates public key", i)
		}
		if _, duplicate := publicKeyHashes[validator.PublicKeyHash]; duplicate {
			return ValidatorSet{}, fmt.Errorf("validator %d duplicates public key hash", i)
		}
		if math.MaxUint64-result.TotalWeight < validator.Weight {
			return ValidatorSet{}, errors.New("total validator weight overflows uint64")
		}

		publicKeys[validator.PublicKey] = struct{}{}
		publicKeyHashes[validator.PublicKeyHash] = struct{}{}
		result.cumulativeWeights[i] = result.TotalWeight
		result.TotalWeight += validator.Weight
		result.Validators[i] = validator
	}

	if result.TotalWeight > maxTotalValidatorWeight {
		return ValidatorSet{}, fmt.Errorf("total validator weight %d exceeds 2^61", result.TotalWeight)
	}
	if constructor == validatorSetExtendedConstructor && declaredTotalWeight != result.TotalWeight {
		return ValidatorSet{}, fmt.Errorf("declared total weight %d, computed %d", declaredTotalWeight, result.TotalWeight)
	}

	return result, nil
}

func parseValidator(s *cell.Slice) (Validator, error) {
	constructor, err := s.LoadUInt(8)
	if err != nil {
		return Validator{}, fmt.Errorf("load constructor: %w", err)
	}
	if constructor != validatorConstructor && constructor != validatorAddressConstructor {
		return Validator{}, fmt.Errorf("unsupported descriptor constructor #%02x", constructor)
	}

	publicKey, err := parseEd25519PublicKey(s)
	if err != nil {
		return Validator{}, err
	}
	weight, err := s.LoadUInt(64)
	if err != nil {
		return Validator{}, fmt.Errorf("load weight: %w", err)
	}
	if weight == 0 {
		return Validator{}, errors.New("weight must be positive")
	}

	validator := Validator{PublicKey: publicKey, Weight: weight}
	if constructor == validatorAddressConstructor {
		validator.ADNL, err = loadBytes32(s, "adnl_addr")
		if err != nil {
			return Validator{}, err
		}
	}
	if err = requireEmpty(s); err != nil {
		return Validator{}, err
	}

	validator.PublicKeyHash, err = PublicKeyHash(validator.PublicKey)
	if err != nil {
		return Validator{}, err
	}

	return validator, nil
}

func parseEd25519PublicKey(s *cell.Slice) ([32]byte, error) {
	constructor, err := s.LoadUInt(32)
	if err != nil {
		return [32]byte{}, fmt.Errorf("load public-key constructor: %w", err)
	}
	if constructor != ed25519PublicKeyConstructor {
		return [32]byte{}, fmt.Errorf("unsupported public-key constructor #%08x", constructor)
	}

	return loadBytes32(s, "public key")
}

func parseHashmapE(s *cell.Slice, keyBits uint) ([]dictionaryEntry, error) {
	present, err := s.LoadBoolBit()
	if err != nil {
		return nil, err
	}
	if !present {
		return nil, nil
	}

	root, err := s.LoadRefCell()
	if err != nil {
		return nil, err
	}

	return parseHashmap(root, keyBits)
}

func parseHashmap(root *cell.Cell, keyBits uint) ([]dictionaryEntry, error) {
	entries := make([]dictionaryEntry, 0)
	if err := parseHashmapNode(root, keyBits, 0, [32]byte{}, &entries); err != nil {
		return nil, err
	}

	return entries, nil
}

func parseHashmapNode(root *cell.Cell, keyBits, offset uint, prefix [32]byte, entries *[]dictionaryEntry) error {
	root, err := root.Prewarm()
	if err != nil {
		return fmt.Errorf("load dictionary cell: %w", err)
	}
	if root.IsSpecial() {
		return errors.New("dictionary contains a special cell")
	}

	s, err := root.BeginParse()
	if err != nil {
		return err
	}
	offset, err = parseHashmapLabel(s, keyBits, offset, &prefix)
	if err != nil {
		return err
	}
	if offset == keyBits {
		*entries = append(*entries, dictionaryEntry{Key: prefix, Value: s.Copy()})
		return nil
	}

	if s.BitsLeft() != 0 || s.RefsNum() != 2 {
		return fmt.Errorf("dictionary fork has %d trailing bits and %d references, want 0 bits and 2 references", s.BitsLeft(), s.RefsNum())
	}
	left, err := s.LoadRefCell()
	if err != nil {
		return err
	}
	right, err := s.LoadRefCell()
	if err != nil {
		return err
	}

	leftPrefix := prefix
	setDictionaryKeyBit(&leftPrefix, offset, false)
	if err = parseHashmapNode(left, keyBits, offset+1, leftPrefix, entries); err != nil {
		return err
	}
	rightPrefix := prefix
	setDictionaryKeyBit(&rightPrefix, offset, true)

	return parseHashmapNode(right, keyBits, offset+1, rightPrefix, entries)
}

func parseHashmapLabel(s *cell.Slice, keyBits, offset uint, prefix *[32]byte) (uint, error) {
	remaining := keyBits - offset
	long, err := s.LoadBoolBit()
	if err != nil {
		return 0, fmt.Errorf("load dictionary label: %w", err)
	}
	if !long {
		length := uint(0)
		for {
			one, err := s.LoadBoolBit()
			if err != nil {
				return 0, fmt.Errorf("load short dictionary label length: %w", err)
			}
			if !one {
				break
			}
			length++
			if length > remaining {
				return 0, errors.New("short dictionary label exceeds remaining key length")
			}
		}

		return loadDictionaryLabelBits(s, offset, length, prefix)
	}

	same, err := s.LoadBoolBit()
	if err != nil {
		return 0, fmt.Errorf("load dictionary label constructor: %w", err)
	}
	lengthBits := uint(bits.Len(remaining))
	if !same {
		length, err := loadDictionaryLabelLength(s, lengthBits, remaining)
		if err != nil {
			return 0, err
		}

		return loadDictionaryLabelBits(s, offset, length, prefix)
	}

	value, err := s.LoadBoolBit()
	if err != nil {
		return 0, fmt.Errorf("load same dictionary label bit: %w", err)
	}
	length, err := loadDictionaryLabelLength(s, lengthBits, remaining)
	if err != nil {
		return 0, err
	}
	for i := uint(0); i < length; i++ {
		setDictionaryKeyBit(prefix, offset+i, value)
	}

	return offset + length, nil
}

func loadDictionaryLabelLength(s *cell.Slice, size, maximum uint) (uint, error) {
	if size == 0 {
		return 0, nil
	}

	value, err := s.LoadUInt(size)
	if err != nil {
		return 0, fmt.Errorf("load dictionary label length: %w", err)
	}
	if value > uint64(maximum) {
		return 0, fmt.Errorf("dictionary label length %d exceeds remaining key length %d", value, maximum)
	}

	return uint(value), nil
}

func loadDictionaryLabelBits(s *cell.Slice, offset, length uint, prefix *[32]byte) (uint, error) {
	for i := uint(0); i < length; i++ {
		value, err := s.LoadBoolBit()
		if err != nil {
			return 0, fmt.Errorf("load dictionary label bits: %w", err)
		}
		setDictionaryKeyBit(prefix, offset+i, value)
	}

	return offset + length, nil
}

func setDictionaryKeyBit(key *[32]byte, offset uint, value bool) {
	mask := byte(1 << (7 - offset%8))
	if value {
		key[offset/8] |= mask
	} else {
		key[offset/8] &^= mask
	}
}

func loadUint32(s *cell.Slice, name string) (uint32, error) {
	value, err := s.LoadUInt(32)
	if err != nil {
		return 0, fmt.Errorf("load %s: %w", name, err)
	}

	return uint32(value), nil
}

func loadBytes32(s *cell.Slice, name string) ([32]byte, error) {
	var result [32]byte
	if err := s.LoadSliceInto(result[:], 256); err != nil {
		return [32]byte{}, fmt.Errorf("load %s: %w", name, err)
	}

	return result, nil
}

func requireEmpty(s *cell.Slice) error {
	if s.BitsLeft() != 0 || s.RefsNum() != 0 {
		return fmt.Errorf("has %d trailing bits and %d trailing references", s.BitsLeft(), s.RefsNum())
	}

	return nil
}

// SimplexProtocol is the critical half of a simplex configuration: the fields
// which identify the consensus rules a session runs under, as opposed to the
// noncritical parameters SimplexParams tunes. The validator supervisor and the
// collator's session projection each re-project these five fields into their
// own package's session descriptor, and a field added here but missed by one of
// them is a silent zero value rather than a compile error, so both read them
// through Protocol instead of reaching into SimplexConfig field by field.
type SimplexProtocol struct {
	Version              uint8
	Flags                uint8
	ProtocolVersion      uint8
	UseQUIC              bool
	SlotsPerLeaderWindow uint32
}

// Protocol returns the critical protocol fields of c.
func (c SimplexConfig) Protocol() SimplexProtocol {
	return SimplexProtocol{
		Version:              c.Version,
		Flags:                c.Flags,
		ProtocolVersion:      c.ProtocolVersion,
		UseQUIC:              c.UseQUIC,
		SlotsPerLeaderWindow: c.SlotsPerLeaderWindow,
	}
}

// SupportedProtocol reports whether c describes a consensus this node can join.
// The #22 protocol_version field is two bits wide, so 0 through 3 are the
// complete supported range.
func (c SimplexConfig) SupportedProtocol() bool {
	return c.Version == 2 && c.ProtocolVersion <= simplex.MaxProtocolVersion
}

// EnableBlockSync reports whether candidate transfer uses the dedicated
// block-sync overlay. The reference enables it only for protocol version 1.
func (c SimplexConfig) EnableBlockSync() bool {
	return c.SupportedProtocol() && c.ProtocolVersion == 1
}

// ObserversInPrivateOverlay reports whether non-validator observer identities
// participate in the private consensus overlay.
func (c SimplexConfig) ObserversInPrivateOverlay() bool {
	return c.SupportedProtocol() && c.ProtocolVersion >= 2
}

// EnablePlumtree reports whether public candidate and finality publication may
// use Plumtree.
func (c SimplexConfig) EnablePlumtree() bool {
	return c.SupportedProtocol() && c.ProtocolVersion >= 2
}

// SimplexParams projects the noncritical consensus parameters onto the engine's
// parameter set. Both the validator supervisor and the collator's session
// projection feed the same engine, so the mapping lives here rather than being
// maintained twice.
func (c SimplexConfig) SimplexParams() simplex.Params {
	params := simplex.DefaultParams()
	for _, item := range c.NoncriticalParams {
		switch item.ID {
		case 0:
			params.TargetRate = consensusMilliseconds(item.Value)
		case 1:
			params.FirstBlockTimeout = consensusMilliseconds(item.Value)
		case 2:
			params.FirstBlockTimeoutMultiplier = float64(math.Float32frombits(item.Value))
		case 3:
			params.FirstBlockTimeoutCap = consensusMilliseconds(item.Value)
		case 4:
			params.CandidateResolveTimeout = consensusMilliseconds(item.Value)
		case 5:
			params.CandidateResolveTimeoutMultiplier = float64(math.Float32frombits(item.Value))
		case 6:
			params.CandidateResolveTimeoutCap = consensusMilliseconds(item.Value)
		case 7:
			params.CandidateResolveCooldown = consensusMilliseconds(item.Value)
		case 8:
			params.StandstillTimeout = consensusMilliseconds(item.Value)
		case 9:
			params.StandstillMaxEgressBytesPerS = item.Value
		case 10:
			params.MaxLeaderWindowDesync = item.Value
		case 11:
			params.BadSignatureBanDuration = consensusMilliseconds(item.Value)
		case 12:
			params.CandidateResolveRateLimit = item.Value
		case 13:
			params.MinBlockInterval = consensusMilliseconds(item.Value)
		case 14:
			params.NoEmptyBlocksOnErrTimeout = consensusMilliseconds(item.Value)
		case 15:
			params.CertificateGossipNeighbors = item.Value
		case 16:
			params.StandstillMinEgressBytesPerS = item.Value
		}
	}

	return params
}

func consensusMilliseconds(value uint32) time.Duration {
	return time.Duration(value) * time.Millisecond
}
