package groups

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	validatorRegistryConfigConstructor    = uint64(0x3601163e)
	validatorRegistryStorageConstructor   = uint64(0x9bc722a8)
	validatorRegistryValidatorConstructor = uint64(0x57766eec)
	validatorRegistryEntryConstructor     = uint64(0xeae0b274)
	validatorRegistryMaxShardSetDepth     = uint64(8)
)

// CollatorRegistryEntry associates a validator signing-key short ID with the
// ADNL identities delegated to collate on its behalf. Snapshot projections are
// sorted by ValidatorKeyID; CollatorADNLIDs are sorted and immutable.
type CollatorRegistryEntry struct {
	ValidatorKeyID  [32]byte
	CollatorADNLIDs [][32]byte
}

type validatorRegistryConfig struct {
	contractAddress          [32]byte
	maxCollatorsPerValidator uint32
}

// ResolveCollatorsByValidator derives the best-effort delegated-collator
// registry from one masterchain state. Invalid state or validator-group config
// is returned as an error; unavailable or malformed optional registry data
// produces an empty valid projection.
func ResolveCollatorsByValidator(input StateInput) ([]CollatorRegistryEntry, error) {
	state, err := ParseState(input)
	if err != nil {
		return nil, fmt.Errorf("parse masterchain state: %w", err)
	}
	config, err := ParseConfig(state.ConfigRoot)
	if err != nil {
		return nil, fmt.Errorf("parse validator config: %w", err)
	}

	// The bootstrap path drops the advisory reason: it derives the registry
	// pinned at a past rotation, and the live rotation below reports the same
	// condition against the current state within one catchain epoch.
	entries, _ := resolveCollatorsByValidator(state, config)

	return entries, nil
}

func normalizeCollatorRegistry(entries []CollatorRegistryEntry) ([]CollatorRegistryEntry, error) {
	if len(entries) == 0 {
		return nil, nil
	}

	result := make([]CollatorRegistryEntry, len(entries))
	for i := range entries {
		if len(entries[i].CollatorADNLIDs) == 0 {
			return nil, fmt.Errorf("entry %d has no collator ADNL IDs", i)
		}

		result[i].ValidatorKeyID = entries[i].ValidatorKeyID
		result[i].CollatorADNLIDs = append([][32]byte(nil), entries[i].CollatorADNLIDs...)
		sort.Slice(result[i].CollatorADNLIDs, func(left, right int) bool {
			return bytes.Compare(result[i].CollatorADNLIDs[left][:], result[i].CollatorADNLIDs[right][:]) < 0
		})
		for index := 1; index < len(result[i].CollatorADNLIDs); index++ {
			if result[i].CollatorADNLIDs[index] == result[i].CollatorADNLIDs[index-1] {
				return nil, fmt.Errorf("entry %d has duplicate collator ADNL ID %x", i, result[i].CollatorADNLIDs[index])
			}
		}
	}

	sort.Slice(result, func(left, right int) bool {
		return bytes.Compare(result[left].ValidatorKeyID[:], result[right].ValidatorKeyID[:]) < 0
	})
	for i := 1; i < len(result); i++ {
		if result[i].ValidatorKeyID == result[i-1].ValidatorKeyID {
			return nil, fmt.Errorf("duplicate validator key ID %x", result[i].ValidatorKeyID)
		}
	}

	return result, nil
}

func (t *Tracker) projectCollatorRegistry(
	previous *Snapshot,
	state *State,
	config *Config,
) ([]CollatorRegistryEntry, []CollatorRegistryEntry, error) {
	current := t.initialCollators
	if previous != nil {
		current = previous.pinnedCollators
	}
	if !state.RotatedAllShards {
		return current, current, nil
	}

	next, issue := resolveCollatorsByValidator(state, config)
	if state.IsKeyState {
		return next, next, issue
	}

	return current, next, issue
}

// resolveCollatorsByValidator is intentionally best-effort. Registry data is
// not consensus input: an unavailable or
// malformed contract disables delegation for this rotation, while one broken
// validator entry only disables delegation for that validator.
//
// The second result is advisory, never fatal: it names why an empty projection
// is empty. Without it a broken registry contract is indistinguishable from no
// registry at all, and delegation switches itself off with nothing to see. This
// package stays free of logging so that a replay is a pure function of state,
// so the reason travels out as data and is logged by the caller.
func resolveCollatorsByValidator(state *State, config *Config) ([]CollatorRegistryEntry, error) {
	registryConfig, err := parseValidatorRegistryConfig(config.validatorRegistry)
	if err != nil {
		return nil, fmt.Errorf("parse validator registry config: %w", err)
	}
	if !isSpecialContract(state, config, registryConfig.contractAddress) {
		return nil, fmt.Errorf(
			"validator registry contract %s is not a special contract",
			registryConfig.contractAddress,
		)
	}

	data, err := loadValidatorRegistryData(state, registryConfig.contractAddress)
	if err != nil {
		return nil, fmt.Errorf("load validator registry data: %w", err)
	}
	registry, err := parseValidatorRegistryStorage(data)
	if err != nil {
		return nil, fmt.Errorf("parse validator registry storage: %w", err)
	}

	validatorCount := 0
	for i := range config.persistentValidatorSets {
		validatorCount += len(config.persistentValidatorSets[i].Validators)
	}
	validatorIdentities := make(map[[32]byte]struct{}, validatorCount*2)
	for i := range config.persistentValidatorSets {
		for j := range config.persistentValidatorSets[i].Validators {
			validator := config.persistentValidatorSets[i].Validators[j]
			validatorIdentities[validator.PublicKeyHash] = struct{}{}
			validatorIdentities[ValidatorADNL(validator)] = struct{}{}
		}
	}

	visited := make(map[[32]byte]struct{}, validatorCount)
	result := make([]CollatorRegistryEntry, 0, validatorCount)
	for i := range config.persistentValidatorSets {
		validatorSet := &config.persistentValidatorSets[i]
		for j := range validatorSet.Validators {
			validator := &validatorSet.Validators[j]
			if _, exists := visited[validator.PublicKeyHash]; exists {
				continue
			}
			visited[validator.PublicKeyHash] = struct{}{}

			value, err := registry.LoadValueByBytesKey(validator.PublicKey[:])
			if err != nil {
				continue
			}
			entryCell, err := parseValidatorRegistryValidator(value)
			if err != nil {
				continue
			}
			collators, err := parseValidatorRegistryEntry(entryCell, registryConfig.maxCollatorsPerValidator)
			if err != nil {
				continue
			}

			filtered := collators[:0]
			for _, id := range collators {
				if _, occupied := validatorIdentities[id]; !occupied {
					filtered = append(filtered, id)
				}
			}
			if len(filtered) == 0 {
				continue
			}

			result = append(result, CollatorRegistryEntry{
				ValidatorKeyID:  validator.PublicKeyHash,
				CollatorADNLIDs: filtered,
			})
		}
	}

	sort.Slice(result, func(left, right int) bool {
		return bytes.Compare(result[left].ValidatorKeyID[:], result[right].ValidatorKeyID[:]) < 0
	})

	return result, nil
}

func parseValidatorRegistryConfig(configCell *cell.Cell) (validatorRegistryConfig, error) {
	if configCell == nil {
		return validatorRegistryConfig{}, ErrNotFound
	}

	var loader cell.Slice
	if err := configCell.BeginParseInto(&loader); err != nil {
		return validatorRegistryConfig{}, err
	}
	constructor, err := loader.LoadUInt(32)
	if err != nil {
		return validatorRegistryConfig{}, err
	}
	if constructor != validatorRegistryConfigConstructor {
		return validatorRegistryConfig{}, fmt.Errorf("unsupported constructor #%08x", constructor)
	}

	contractAddress, err := loadBytes32(&loader, "contract_address")
	if err != nil {
		return validatorRegistryConfig{}, err
	}
	maxCollators, err := loadUint32(&loader, "max_collators_per_validator")
	if err != nil {
		return validatorRegistryConfig{}, err
	}
	hasNewCodeHash, err := loader.LoadBoolBit()
	if err != nil {
		return validatorRegistryConfig{}, fmt.Errorf("load new_code_hash presence: %w", err)
	}
	if hasNewCodeHash {
		if _, err = loadBytes32(&loader, "new_code_hash"); err != nil {
			return validatorRegistryConfig{}, err
		}
	}
	if err = requireEmpty(&loader); err != nil {
		return validatorRegistryConfig{}, err
	}

	return validatorRegistryConfig{
		contractAddress:          contractAddress,
		maxCollatorsPerValidator: maxCollators,
	}, nil
}

func isSpecialContract(state *State, config *Config, contractAddress [32]byte) bool {
	if contractAddress == state.configAddress {
		return true
	}
	if config.fundamentalContracts == nil {
		return false
	}

	var loader cell.Slice
	if err := config.fundamentalContracts.BeginParseInto(&loader); err != nil {
		return false
	}
	contracts, err := loader.LoadDict(256)
	if err != nil || requireEmpty(&loader) != nil {
		return false
	}
	_, err = contracts.LoadValueByBytesKey(contractAddress[:])

	return err == nil
}

func loadValidatorRegistryData(state *State, contractAddress [32]byte) (*cell.Cell, error) {
	if state.accounts == nil || state.accounts.IsEmpty() {
		return nil, ErrNotFound
	}

	key := cell.BeginCell().MustStoreSlice(contractAddress[:], 256).EndCell()
	value, err := state.accounts.LoadValue(key)
	if err != nil {
		return nil, err
	}

	var shardAccount tlb.ShardAccount
	if err = tlb.LoadFromCell(&shardAccount, value); err != nil {
		return nil, err
	}
	if err = requireEmpty(value); err != nil {
		return nil, err
	}

	var accountLoader cell.Slice
	if err = shardAccount.Account.BeginParseInto(&accountLoader); err != nil {
		return nil, err
	}
	var account tlb.AccountState
	if err = tlb.LoadFromCell(&account, &accountLoader); err != nil {
		return nil, err
	}
	if err = requireEmpty(&accountLoader); err != nil {
		return nil, err
	}
	if !account.IsValid || account.Status != tlb.AccountStatusActive ||
		account.StateInit == nil || account.StateInit.Data == nil {
		return nil, ErrNotFound
	}

	return account.StateInit.Data, nil
}

func parseValidatorRegistryStorage(data *cell.Cell) (*cell.Dictionary, error) {
	var loader cell.Slice
	if err := data.BeginParseInto(&loader); err != nil {
		return nil, err
	}
	constructor, err := loader.LoadUInt(32)
	if err != nil {
		return nil, err
	}
	if constructor != validatorRegistryStorageConstructor {
		return nil, fmt.Errorf("unsupported storage constructor #%08x", constructor)
	}

	registry, err := loader.LoadDict(256)
	if err != nil {
		return nil, err
	}
	if _, err = loadUint32(&loader, "last_cleanup_key_block_seqno"); err != nil {
		return nil, err
	}
	if err = requireEmpty(&loader); err != nil {
		return nil, err
	}

	return registry, nil
}

func parseValidatorRegistryValidator(value *cell.Slice) (*cell.Cell, error) {
	constructor, err := value.LoadUInt(32)
	if err != nil {
		return nil, err
	}
	if constructor != validatorRegistryValidatorConstructor {
		return nil, fmt.Errorf("unsupported validator constructor #%08x", constructor)
	}
	if _, err = loadUint32(value, "allow_update_at"); err != nil {
		return nil, err
	}

	hasEntry, err := value.LoadBoolBit()
	if err != nil {
		return nil, err
	}
	if !hasEntry {
		if err = requireEmpty(value); err != nil {
			return nil, err
		}

		return nil, ErrNotFound
	}
	entry, err := value.LoadRefCell()
	if err != nil {
		return nil, err
	}
	if err = requireEmpty(value); err != nil {
		return nil, err
	}

	return entry, nil
}

func parseValidatorRegistryEntry(entry *cell.Cell, limit uint32) ([][32]byte, error) {
	var loader cell.Slice
	if err := entry.BeginParseInto(&loader); err != nil {
		return nil, err
	}
	constructor, err := loader.LoadUInt(32)
	if err != nil {
		return nil, err
	}
	if constructor != validatorRegistryEntryConstructor {
		return nil, fmt.Errorf("unsupported entry constructor #%08x", constructor)
	}
	collators, err := loader.LoadDict(256)
	if err != nil {
		return nil, err
	}
	if err = parseValidatorRegistryShardSet(&loader); err != nil {
		return nil, err
	}
	if err = requireEmpty(&loader); err != nil {
		return nil, err
	}

	// max_collators_per_validator is an on-chain uint32 and must not be used as
	// an allocation size before the dictionary proves that many entries exist.
	result := make([][32]byte, 0)
	_, err = collators.CheckForEach(func(_ *cell.Slice, key *cell.Cell) (bool, error) {
		if uint32(len(result)) >= limit {
			return false, nil
		}

		var keyLoader cell.Slice
		if err := key.BeginParseInto(&keyLoader); err != nil {
			return false, err
		}
		id, err := loadBytes32(&keyLoader, "collator ADNL ID")
		if err != nil {
			return false, err
		}
		if err = requireEmpty(&keyLoader); err != nil {
			return false, err
		}
		result = append(result, id)

		return true, nil
	}, false, false)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func parseValidatorRegistryShardSet(loader *cell.Slice) error {
	depth, err := loader.LoadUInt(4)
	if err != nil {
		return fmt.Errorf("load monitoring shard depth: %w", err)
	}
	if depth > validatorRegistryMaxShardSetDepth {
		return fmt.Errorf("monitoring shard depth %d exceeds %d", depth, validatorRegistryMaxShardSetDepth)
	}

	err = loader.SkipBits(uint(1) << depth)
	if err != nil {
		return fmt.Errorf("load monitoring shard mask: %w", err)
	}

	return nil
}

// ValidatorADNL is the ADNL id a validator is reached at: its declared address
// when it has one, and otherwise its public key hash.
func ValidatorADNL(validator Validator) [32]byte {
	if validator.ADNL != ([32]byte{}) {
		return validator.ADNL
	}

	return validator.PublicKeyHash
}

// AllCollatorADNLIDs flattens the current validator registry into the complete
// sorted set of collator identities. Session-specific delegation authorization
// remains in CollatorRegistryEntry; this wider set is transport membership.
func AllCollatorADNLIDs(registry []CollatorRegistryEntry) [][32]byte {
	count := 0
	for i := range registry {
		count += len(registry[i].CollatorADNLIDs)
	}
	collators := make([][32]byte, 0, count)
	for i := range registry {
		collators = append(collators, registry[i].CollatorADNLIDs...)
	}
	sort.Slice(collators, func(left, right int) bool {
		return bytes.Compare(collators[left][:], collators[right][:]) < 0
	})
	unique := collators[:0]
	for _, id := range collators {
		if len(unique) == 0 || id != unique[len(unique)-1] {
			unique = append(unique, id)
		}
	}

	return unique
}

func persistentOverlayWithCollators(
	validators []PersistentOverlayMember,
	registry []CollatorRegistryEntry,
) []PersistentOverlayMember {
	if len(registry) == 0 {
		return validators
	}

	unique := AllCollatorADNLIDs(registry)

	result := make([]PersistentOverlayMember, 0, len(validators)+len(unique))
	validatorIndex := 0
	collatorIndex := 0
	for validatorIndex < len(validators) && collatorIndex < len(unique) {
		switch bytes.Compare(validators[validatorIndex].ADNL[:], unique[collatorIndex][:]) {
		case -1:
			result = append(result, validators[validatorIndex])
			validatorIndex++
		case 0:
			result = append(result, validators[validatorIndex])
			validatorIndex++
			collatorIndex++
		case 1:
			result = append(result, PersistentOverlayMember{ADNL: unique[collatorIndex]})
			collatorIndex++
		}
	}
	result = append(result, validators[validatorIndex:]...)
	for ; collatorIndex < len(unique); collatorIndex++ {
		result = append(result, PersistentOverlayMember{ADNL: unique[collatorIndex]})
	}

	return result
}
