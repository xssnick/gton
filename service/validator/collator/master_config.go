package collator

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

var errConfigAddressAbsent = errors.New("config address is absent")

const validatorRegistryConfigConstructor = uint64(0x3601163e)

var hardMandatoryConfigParameters = [...]int32{
	18, 20, 21, 22, 23, 24, 25, 28, 34,
}

// validateMasterConfigData requires exact references and validates every known
// positive parameter against its TL-B schema, then enforces both the old and
// resulting mandatory parameter sets.
func validateMasterConfigData(
	root *cell.Cell,
	accountID []byte,
	oldRoot *cell.Cell,
	relaxAddress bool,
) error {
	if root == nil || len(accountID) != 32 {
		return fmt.Errorf("%w: config root or account id is malformed", ErrInvalidInput)
	}
	dict := root.AsDict(32)
	if dict.GetKeySize() != 32 || dict.IsEmpty() {
		return fmt.Errorf("%w: config parameter dictionary is empty or has a wrong key size", ErrInvalidInput)
	}
	present := make(map[int32]struct{})
	// The plain dictionary iterator validates fork shape as well as labels.
	// LoadAll also handles augmented trees and permits fork payloads here.
	err := dict.ForEachBorrowed(false, false, func(item cell.DictItemView) error {
		key, loadErr := item.Key.LoadInt(32)
		if loadErr != nil {
			return fmt.Errorf("%w: config parameter key is malformed", ErrInvalidInput)
		}
		parameter, loadErr := item.Value.LoadRefCell()
		if loadErr != nil || item.Value.BitsLeft() != 0 || item.Value.RefsNum() != 0 {
			return fmt.Errorf("%w: config parameter %d is not an exact reference", ErrInvalidInput, key)
		}
		id := int32(key)
		present[id] = struct{}{}

		// Negative parameters have arbitrary values, but may still be named
		// by either mandatory set (Config::unpack_param_dict uses int32 IDs).
		if id < 0 {
			return nil
		}
		if id == 0 {
			address, addressErr := exactBits256(parameter)
			if addressErr != nil || !relaxAddress && !bytes.Equal(address, accountID) {
				return fmt.Errorf("%w: config parameter 0 does not name its contract", ErrInvalidInput)
			}
			return nil
		}
		if err := validateKnownConfigParameter(parameter, uint32(id)); err != nil {
			return fmt.Errorf("%w: config parameter %d: %v", ErrInvalidInput, id, err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("%w: config dictionary: %v", ErrInvalidInput, err)
	}

	for _, id := range hardMandatoryConfigParameters {
		if _, exists := present[id]; !exists {
			return fmt.Errorf("%w: mandatory config parameter %d is absent", ErrInvalidInput, id)
		}
	}
	if err = requireConfigMandatorySet(tlb.BlockchainConfig{Root: root}, present); err != nil {
		return err
	}
	if oldRoot != nil {
		if err = requireConfigMandatorySet(tlb.BlockchainConfig{Root: oldRoot}, present); err != nil {
			return fmt.Errorf("old mandatory set: %w", err)
		}
	}
	return nil
}

func exactConfigAddress(root *cell.Cell) ([]byte, error) {
	parameter, err := (tlb.BlockchainConfig{Root: root}).GetParam(tlb.ConfigParamConfigAddress)
	if errors.Is(err, tlb.ErrBlockchainConfigParamAbsent) {
		return nil, errConfigAddressAbsent
	}
	if err != nil {
		return nil, err
	}
	return exactBits256(parameter)
}

func exactBits256(root *cell.Cell) ([]byte, error) {
	var loader cell.Slice
	err := root.BeginParseInto(&loader)
	if err != nil {
		return nil, err
	}
	value, err := loader.LoadSlice(256)
	if err != nil || loader.BitsLeft() != 0 || loader.RefsNum() != 0 {
		return nil, fmt.Errorf("value is not exactly 256 bits")
	}
	return value, nil
}

func requireConfigMandatorySet(raw tlb.BlockchainConfig, present map[int32]struct{}) error {
	parameter, err := raw.GetParam(tlb.ConfigParamMandatoryParams)
	if errors.Is(err, tlb.ErrBlockchainConfigParamAbsent) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("%w: load mandatory config set: %v", ErrInvalidInput, err)
	}
	dict := parameter.AsDict(32)
	if dict.GetKeySize() != 32 {
		return fmt.Errorf("%w: mandatory config set has a wrong key size", ErrInvalidInput)
	}
	err = dict.ForEachBorrowed(false, false, func(item cell.DictItemView) error {
		id, loadErr := item.Key.LoadInt(32)
		if loadErr != nil || item.Value.BitsLeft() != 0 || item.Value.RefsNum() != 0 {
			return fmt.Errorf("mandatory config entry is malformed")
		}
		if _, exists := present[int32(id)]; !exists {
			return fmt.Errorf("declared mandatory config parameter %d is absent", id)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("%w: mandatory config set: %v", ErrInvalidInput, err)
	}
	return nil
}
