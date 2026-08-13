package collator

import (
	"errors"
	"fmt"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// parseSemanticTransaction is parseExact for the replay's transaction view,
// called directly so the destination does not travel through an interface.
// The replay compares its own transaction against the recorded cell
// bit-for-bit, which authenticates everything the lean parse skips.
func parseSemanticTransaction(dst *tlb.TransactionLean, root *cell.Cell) error {
	if root == nil {
		return errors.New("cell is absent")
	}
	var s cell.Slice
	if err := root.BeginParseInto(&s); err != nil {
		return err
	}
	if err := dst.LoadFromCell(&s); err != nil {
		return err
	}
	if s.BitsLeft() != 0 || s.RefsNum() != 0 {
		return fmt.Errorf("trailing data: %d bits, %d refs", s.BitsLeft(), s.RefsNum())
	}
	return nil
}

// LoadFromCell parses the CommonMsgInfo prefix by hand; the reflection walk
// this replaces was one of the queue verification's biggest allocation
// sources. Field order and strictness mirror the struct's tlb tags exactly.
func (m *semanticInternalMessageInfo) LoadFromCell(loader *cell.Slice) error {
	magic, err := loader.LoadUInt(1)
	if err != nil {
		return fmt.Errorf("failed to load internal message magic: %w", err)
	}
	if magic != 0 {
		return fmt.Errorf("invalid internal message magic")
	}
	if m.IHRDisabled, err = loader.LoadBoolBit(); err != nil {
		return fmt.Errorf("failed to load ihr disabled flag: %w", err)
	}
	if m.Bounce, err = loader.LoadBoolBit(); err != nil {
		return fmt.Errorf("failed to load bounce flag: %w", err)
	}
	if m.Bounced, err = loader.LoadBoolBit(); err != nil {
		return fmt.Errorf("failed to load bounced flag: %w", err)
	}
	if m.SrcAddr, err = loader.LoadAddr(); err != nil {
		return fmt.Errorf("failed to load source address: %w", err)
	}
	if m.DstAddr, err = loader.LoadAddr(); err != nil {
		return fmt.Errorf("failed to load destination address: %w", err)
	}
	if err = m.Amount.LoadFromCell(loader); err != nil {
		return fmt.Errorf("failed to load amount: %w", err)
	}
	if m.ExtraCurrencies, err = loader.LoadDict(32); err != nil {
		return fmt.Errorf("failed to load extra currencies: %w", err)
	}
	if err = m.IHRFee.LoadFromCell(loader); err != nil {
		return fmt.Errorf("failed to load ihr fee: %w", err)
	}
	if err = m.FwdFee.LoadFromCell(loader); err != nil {
		return fmt.Errorf("failed to load forward fee: %w", err)
	}
	if m.CreatedLT, err = loader.LoadUInt(64); err != nil {
		return fmt.Errorf("failed to load created lt: %w", err)
	}
	createdAt, err := loader.LoadUInt(32)
	if err != nil {
		return fmt.Errorf("failed to load created at: %w", err)
	}
	m.CreatedAt = uint32(createdAt)
	return nil
}
