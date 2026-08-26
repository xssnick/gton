package msgpool

import (
	"errors"
	"fmt"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// AppliedNormHashesFromInMsgDescr extracts the normalized hashes of every
// external message imported by a block (msg_import_ext entries of
// InMsgDescr). Feeding the result into EraseApplied is the finalized-block
// mempool cleanup.
func AppliedNormHashesFromInMsgDescr(inMsgDescr *cell.Cell) ([][32]byte, error) {
	var loader cell.Slice
	err := inMsgDescr.BeginParseInto(&loader)
	if err != nil {
		return nil, fmt.Errorf("msgpool: cannot parse InMsgDescr: %w", err)
	}
	// The cell contains a HashmapAugE 256 InMsg ImportFees.
	dict, err := loader.LoadAugDict(256, tlb.AugInMsgDescr{}, false)
	if err != nil {
		return nil, fmt.Errorf("msgpool: cannot load InMsgDescr dictionary: %w", err)
	}

	var out [][32]byte
	seen := map[[32]byte]struct{}{}
	ok, err := dict.CheckForEachExtra(func(value, extra *cell.Slice, key *cell.Cell) (bool, error) {
		v := *value
		tag, err := v.LoadUInt(3)
		if err != nil {
			return false, fmt.Errorf("cannot load InMsg tag: %w", err)
		}
		if tag != 0b000 { // msg_import_ext$000 msg:^(ExternalMessage Any) transaction:^Transaction
			return true, nil
		}
		msgCell, err := v.LoadRefCell()
		if err != nil {
			return false, fmt.Errorf("cannot unpack msg_import_ext: %w", err)
		}
		hash, err := normHashOfMessageCell(msgCell)
		if err != nil {
			return false, fmt.Errorf("cannot normalize applied external message: %w", err)
		}
		if _, dup := seen[hash]; !dup {
			seen[hash] = struct{}{}
			out = append(out, hash)
		}
		return true, nil
	}, false)
	if err != nil {
		return nil, fmt.Errorf("msgpool: failed to iterate applied block InMsgDescr: %w", err)
	}
	if !ok {
		return nil, errors.New("msgpool: failed to iterate applied block InMsgDescr")
	}
	return out, nil
}

const (
	blockMagic      = 0x11ef55aa
	blockExtraMagic = 0x4a33f6fd
)

// AppliedNormHashesFromBlockRoot extracts the applied external hashes
// straight from a block root cell, touching only the two header magics and
// the InMsgDescr reference — no full block deserialization. This is the
// fast path for applied-block hooks that already hold the root.
func AppliedNormHashesFromBlockRoot(root *cell.Cell) ([][32]byte, error) {
	extra, err := blockExtra(root)
	if err != nil {
		return nil, err
	}
	// block_extra in_msg_descr:^InMsgDescr ...
	inMsg, err := extra.PeekRef(0)
	if err != nil {
		return nil, fmt.Errorf("msgpool: block extra has no InMsgDescr: %w", err)
	}
	return AppliedNormHashesFromInMsgDescr(inMsg)
}

// blockExtra walks a block root to its BlockExtra cell, checking both header
// magics on the way. The two references it steps over are fixed by the layout
// block#11ef55aa info:^BlockInfo value_flow:^ValueFlow
// state_update:^(MERKLE_UPDATE ShardState) extra:^BlockExtra, which is why the
// extra is reference 3.
func blockExtra(root *cell.Cell) (*cell.Cell, error) {
	var s cell.Slice
	err := root.BeginParseInto(&s)
	if err != nil {
		return nil, fmt.Errorf("msgpool: cannot parse block root: %w", err)
	}
	magic, err := s.LoadUInt(32)
	if err != nil || magic != blockMagic {
		return nil, errors.New("msgpool: cell is not a block root")
	}
	extra, err := root.PeekRef(3)
	if err != nil {
		return nil, fmt.Errorf("msgpool: block root has no extra: %w", err)
	}
	var es cell.Slice
	err = extra.BeginParseInto(&es)
	if err != nil {
		return nil, fmt.Errorf("msgpool: cannot parse block extra: %w", err)
	}
	magic, err = es.LoadUInt(32)
	if err != nil || magic != blockExtraMagic {
		return nil, errors.New("msgpool: block extra magic mismatch")
	}

	return extra, nil
}
