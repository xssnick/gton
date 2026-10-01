package collator

import (
	"bytes"
	"fmt"
	"slices"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type publicLibrary struct {
	root *cell.Cell
}

type globalLibrary struct {
	root       *cell.Cell
	publishers *cell.Dictionary
}

// masterExecutionLibraries exposes the masterchain LibDescr index directly to
// TVM. The reference VM accepts this dictionary as a library collection: a
// lookup reads the descriptor's first reference and verifies its hash. Keeping
// the original root also keeps the predecessor trace, so only a library path
// actually used by a transaction enters the block read set.
func masterExecutionLibraries(libraries *cell.Dictionary) ([]*cell.Cell, error) {
	if libraries == nil || libraries.IsEmpty() {
		return nil, nil
	}
	if libraries.GetKeySize() != 256 {
		return nil, fmt.Errorf("%w: public library dictionary has key size %d", ErrInvalidInput, libraries.GetKeySize())
	}

	return []*cell.Cell{libraries.AsCell()}, nil
}

// updateMasterPublicLibraries applies account SimpleLib visibility changes to
// the masterchain-wide LibDescr publisher index. Only accounts transacted in
// this block can change their StateInit libraries.
func (c *collation) updateMasterPublicLibraries() error {
	libraries := c.oldStats.Libraries
	if libraries == nil {
		libraries = cell.NewDict(256)
	} else {
		if libraries.GetKeySize() != 256 {
			return fmt.Errorf("%w: public library dictionary has key size %d", ErrInvalidInput, libraries.GetKeySize())
		}
		libraries = libraries.Copy()
	}

	lanes := make([]*accountLane, 0, len(c.lanes))
	for _, lane := range c.lanes {
		if lane.touched {
			lanes = append(lanes, lane)
		}
	}
	slices.SortFunc(lanes, func(left, right *accountLane) int {
		return bytes.Compare(left.key[:], right.key[:])
	})

	for _, lane := range lanes {
		var original tlb.AccountState
		if err := parseExact(&original, lane.original.Account); err != nil {
			return fmt.Errorf("decode original account %x for public libraries: %w", lane.key, err)
		}
		before, err := accountPublicLibraries(&original)
		if err != nil {
			return fmt.Errorf("decode original public libraries of %x: %w", lane.key, err)
		}
		after, err := accountPublicLibraries(lane.current.State())
		if err != nil {
			return fmt.Errorf("decode resulting public libraries of %x: %w", lane.key, err)
		}

		keys := publicLibraryDiffKeys(before, after)
		for _, key := range keys {
			oldLibrary, wasPublic := before[key]
			newLibrary, isPublic := after[key]
			switch {
			case wasPublic && !isPublic:
				if err = removeLibraryPublisher(libraries, key, lane.key, oldLibrary.root); err != nil {
					return err
				}
			case !wasPublic && isPublic:
				if err = addLibraryPublisher(libraries, key, lane.key, newLibrary.root); err != nil {
					return err
				}
			}
		}
	}

	c.master.libraries = libraries
	return nil
}

func accountPublicLibraries(state *tlb.AccountState) (map[[32]byte]publicLibrary, error) {
	result := make(map[[32]byte]publicLibrary)
	if state.Status != tlb.AccountStatusActive || state.StateInit == nil ||
		state.StateInit.Lib == nil || state.StateInit.Lib.IsEmpty() {
		return result, nil
	}
	libraries := state.StateInit.Lib
	if libraries.GetKeySize() != 256 {
		return nil, fmt.Errorf("account library dictionary has key size %d", libraries.GetKeySize())
	}
	items, err := libraries.LoadAll()
	if err != nil {
		return nil, err
	}
	for i := range items {
		var key [32]byte
		loadErr := items[i].Key.LoadSliceInto(key[:], 256)
		if loadErr != nil || items[i].Key.BitsLeft() != 0 {
			return nil, fmt.Errorf("library %d has a malformed key", i)
		}
		public, loadErr := items[i].Value.LoadBoolBit()
		if loadErr != nil {
			return nil, fmt.Errorf("library %d public flag: %w", i, loadErr)
		}
		root, loadErr := items[i].Value.LoadRefCell()
		if loadErr != nil || items[i].Value.BitsLeft() != 0 || items[i].Value.RefsNum() != 0 {
			return nil, fmt.Errorf("library %d value is malformed", i)
		}
		// A public entry whose key is not the referenced cell hash does not count
		// as public; the account dictionary itself is still structurally valid.
		if public && root.HashKey() == key {
			result[key] = publicLibrary{root: root}
		}
	}
	return result, nil
}

// publicLibraryDiffCount counts the public library keys whose visibility flips
// in one masterchain transaction. Counting every library of a frozen or deleted
// account yields the same number: such an account exposes no libraries
// afterwards.
func publicLibraryDiffCount(before, after *tlb.AccountState) (uint64, error) {
	original, err := accountPublicLibraries(before)
	if err != nil {
		return 0, fmt.Errorf("original libraries: %w", err)
	}
	resulting, err := accountPublicLibraries(after)
	if err != nil {
		return 0, fmt.Errorf("resulting libraries: %w", err)
	}
	if len(original) == 0 && len(resulting) == 0 {
		return 0, nil
	}

	return uint64(len(publicLibraryDiffKeys(original, resulting))), nil
}

func publicLibraryDiffKeys(before, after map[[32]byte]publicLibrary) [][32]byte {
	keys := make([][32]byte, 0, len(before)+len(after))
	for key := range before {
		if _, unchanged := after[key]; !unchanged {
			keys = append(keys, key)
		}
	}
	for key := range after {
		if _, unchanged := before[key]; !unchanged {
			keys = append(keys, key)
		}
	}
	slices.SortFunc(keys, func(left, right [32]byte) int {
		return bytes.Compare(left[:], right[:])
	})
	return keys
}

func addLibraryPublisher(
	libraries *cell.Dictionary,
	key, publisher [32]byte,
	root *cell.Cell,
) error {
	var value cell.Slice
	err := libraries.LoadValueByBytesKeyInto(key[:], &value)
	var descriptor globalLibrary
	mode := cell.DictSetModeReplace
	if isMissingKey(err) {
		descriptor = globalLibrary{root: root, publishers: cell.NewDict(256)}
		mode = cell.DictSetModeAdd
	} else if err != nil {
		return fmt.Errorf("load public library %x: %w", key, err)
	} else {
		descriptor, err = parseGlobalLibrary(&value, key)
		if err != nil {
			return fmt.Errorf("decode public library %x: %w", key, err)
		}
	}
	inserted, err := descriptor.publishers.SetBuilderByBytesKeyWithMode(
		publisher[:],
		cell.BeginCell(),
		cell.DictSetModeAdd,
	)
	if err != nil {
		return fmt.Errorf("add publisher %x to public library %x: %w", publisher, key, err)
	}
	if !inserted {
		return fmt.Errorf("%w: public library %x already lists publisher %x", ErrInvalidInput, key, publisher)
	}
	serialized, err := serializeGlobalLibrary(descriptor)
	if err != nil {
		return err
	}
	var valueBuilder cell.Builder
	serialized.ToBuilderInto(&valueBuilder)
	changed, err := libraries.SetBuilderByBytesKeyWithMode(key[:], &valueBuilder, mode)
	if err != nil {
		return fmt.Errorf("store public library %x: %w", key, err)
	}
	if !changed {
		return fmt.Errorf("%w: public library %x update did not apply", ErrInvalidInput, key)
	}
	return nil
}

func removeLibraryPublisher(
	libraries *cell.Dictionary,
	key, publisher [32]byte,
	root *cell.Cell,
) error {
	var value cell.Slice
	if err := libraries.LoadValueByBytesKeyInto(key[:], &value); err != nil {
		return fmt.Errorf("%w: public library %x being removed is absent", ErrInvalidInput, key)
	}
	descriptor, err := parseGlobalLibrary(&value, key)
	if err != nil {
		return fmt.Errorf("decode public library %x: %w", key, err)
	}
	if descriptor.root.HashKey() != root.HashKey() {
		return fmt.Errorf("%w: account and global roots of public library %x disagree", ErrInvalidInput, key)
	}
	if err = descriptor.publishers.DeleteByBytesKey(publisher[:]); err != nil {
		return fmt.Errorf("%w: public library %x does not list publisher %x", ErrInvalidInput, key, publisher)
	}
	if descriptor.publishers.IsEmpty() {
		if err = libraries.DeleteByBytesKey(key[:]); err != nil {
			return fmt.Errorf("remove public library %x: %w", key, err)
		}
		return nil
	}

	serialized, err := serializeGlobalLibrary(descriptor)
	if err != nil {
		return err
	}
	var valueBuilder cell.Builder
	serialized.ToBuilderInto(&valueBuilder)
	changed, err := libraries.SetBuilderByBytesKeyWithMode(key[:], &valueBuilder, cell.DictSetModeReplace)
	if err != nil {
		return fmt.Errorf("store public library %x: %w", key, err)
	}
	if !changed {
		return fmt.Errorf("%w: public library %x update did not apply", ErrInvalidInput, key)
	}
	return nil
}

func parseGlobalLibrary(value *cell.Slice, key [32]byte) (globalLibrary, error) {
	tag, err := value.LoadUInt(2)
	if err != nil || tag != 0 {
		return globalLibrary{}, fmt.Errorf("invalid LibDescr tag")
	}
	root, err := value.LoadRefCell()
	if err != nil {
		return globalLibrary{}, fmt.Errorf("load library root: %w", err)
	}
	if root.HashKey() != key {
		return globalLibrary{}, fmt.Errorf("library root hash does not match its key")
	}
	publishers, err := value.ToDict(256)
	if err != nil {
		return globalLibrary{}, fmt.Errorf("load non-empty publisher dictionary: %w", err)
	}
	items, err := publishers.LoadAll()
	if err != nil {
		return globalLibrary{}, fmt.Errorf("load publishers: %w", err)
	}
	for i := range items {
		if loadErr := items[i].Key.SkipBits(256); loadErr != nil || items[i].Key.BitsLeft() != 0 ||
			items[i].Value.BitsLeft() != 0 || items[i].Value.RefsNum() != 0 {
			return globalLibrary{}, fmt.Errorf("publisher %d is malformed", i)
		}
	}
	return globalLibrary{root: root, publishers: publishers}, nil
}

func serializeGlobalLibrary(descriptor globalLibrary) (*cell.Cell, error) {
	if descriptor.publishers.IsEmpty() {
		return nil, fmt.Errorf("%w: public library has no publishers", ErrInvalidInput)
	}
	b := cell.BeginCell().MustStoreUInt(0, 2)
	if err := b.StoreRef(descriptor.root); err != nil {
		return nil, err
	}
	var publishers cell.Builder
	descriptor.publishers.AsCell().ToBuilderInto(&publishers)
	if err := b.StoreBuilder(&publishers); err != nil {
		return nil, fmt.Errorf("serialize public library publishers: %w", err)
	}
	return b.EndCell(), nil
}
