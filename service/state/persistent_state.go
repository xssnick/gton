package state

import (
	"errors"
	"github.com/xssnick/gton/service/storage"
	"fmt"
	"math/big"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func PersistentStateSplitDepth(master *storage.BlockState, workchain int32) (uint32, error) {
	if workchain == -1 || master == nil || master.Parsed == nil || master.Parsed.McStateExtra == nil {
		return 0, nil
	}

	loader, err := master.Parsed.McStateExtra.BeginParse()
	if err != nil {
		return 0, fmt.Errorf("begin parse masterchain extra: %w", err)
	}

	var extra tlb.McStateExtra
	if err := tlb.LoadFromCell(&extra, loader); err != nil {
		return 0, fmt.Errorf("parse masterchain extra: %w", err)
	}
	if extra.ConfigParams.Config.Params == nil {
		return 0, nil
	}

	config := tlb.BlockchainConfig{Root: extra.ConfigParams.Config.Params.AsCell()}
	workchains, err := config.GetWorkchains()
	if err != nil {
		if errors.Is(err, tlb.ErrBlockchainConfigParamAbsent) {
			return 0, nil
		}
		return 0, err
	}
	if workchains.Workchains == nil {
		return 0, nil
	}

	value, err := workchains.Workchains.LoadValueByIntKey(big.NewInt(int64(workchain)))
	if err != nil {
		if errors.Is(err, cell.ErrNoSuchKeyInDict) {
			return 0, nil
		}
		return 0, err
	}

	return loadWorkchainPersistentStateSplitDepth(value)
}

func loadWorkchainPersistentStateSplitDepth(loader *cell.Slice) (uint32, error) {
	tag, err := loader.Copy().LoadUInt(8)
	if err != nil {
		return 0, err
	}

	switch tag {
	case 0xa6:
		var descr workchainDescriptorV1
		if err = tlb.LoadFromCell(&descr, loader); err != nil {
			return 0, err
		}
		if err = descr.Common.Validate(); err != nil {
			return 0, err
		}
		return 0, nil
	case 0xa7:
		var descr workchainDescriptorV2
		if err = tlb.LoadFromCell(&descr, loader); err != nil {
			return 0, err
		}
		if err = descr.Common.Validate(); err != nil {
			return 0, err
		}
		if descr.PersistentStateSplitDepth > 63 {
			return 0, fmt.Errorf("invalid persistent state split depth %d", descr.PersistentStateSplitDepth)
		}
		return uint32(descr.PersistentStateSplitDepth), nil
	default:
		return 0, fmt.Errorf("unsupported workchain descriptor tag %#x", tag)
	}
}

type workchainDescriptorV1 struct {
	_      tlb.Magic                 `tlb:"#a6"`
	Common workchainDescriptorCommon `tlb:"."`
}

type workchainDescriptorV2 struct {
	_                         tlb.Magic                  `tlb:"#a7"`
	Common                    workchainDescriptorCommon  `tlb:"."`
	SplitMergeTimings         workchainSplitMergeTimings `tlb:"."`
	PersistentStateSplitDepth uint8                      `tlb:"## 8"`
}

type workchainDescriptorCommon struct {
	EnabledSince      uint32          `tlb:"## 32"`
	MonitorMinSplit   uint8           `tlb:"## 8"`
	MinSplit          uint8           `tlb:"## 8"`
	MaxSplit          uint8           `tlb:"## 8"`
	Basic             bool            `tlb:"bool"`
	Active            bool            `tlb:"bool"`
	AcceptMsgs        bool            `tlb:"bool"`
	Flags             uint16          `tlb:"## 13"`
	ZerostateRootHash []byte          `tlb:"bits 256"`
	ZerostateFileHash []byte          `tlb:"bits 256"`
	Version           uint32          `tlb:"## 32"`
	Format            workchainFormat `tlb:"."`
}

func (d workchainDescriptorCommon) Validate() error {
	if d.MonitorMinSplit > d.MinSplit {
		return fmt.Errorf("invalid workchain descriptor split range: monitor_min_split=%d min_split=%d", d.MonitorMinSplit, d.MinSplit)
	}
	if d.MinSplit > d.MaxSplit || d.MaxSplit > 60 {
		return fmt.Errorf("invalid workchain descriptor split range: min_split=%d max_split=%d", d.MinSplit, d.MaxSplit)
	}
	if d.Flags != 0 {
		return fmt.Errorf("invalid workchain descriptor flags %d", d.Flags)
	}

	if d.Basic != d.Format.Basic {
		return fmt.Errorf("workchain format does not match basic flag")
	}
	return nil
}

type workchainFormat struct {
	Basic bool
}

func (f *workchainFormat) LoadFromCell(loader *cell.Slice) error {
	tag, err := loader.Copy().LoadUInt(4)
	if err != nil {
		return err
	}

	switch tag {
	case 0:
		var format workchainFormatExtended
		if err = tlb.LoadFromCell(&format, loader); err != nil {
			return err
		}
		if err = format.Validate(); err != nil {
			return err
		}
		f.Basic = false
		return nil
	case 1:
		var format workchainFormatBasic
		if err = tlb.LoadFromCell(&format, loader); err != nil {
			return err
		}
		f.Basic = true
		return nil
	default:
		return fmt.Errorf("unsupported workchain format tag %#x", tag)
	}
}

type workchainFormatBasic struct {
	_         tlb.Magic `tlb:"#1"`
	VMVersion int32     `tlb:"## 32"`
	VMMode    uint64    `tlb:"## 64"`
}

type workchainFormatExtended struct {
	_               tlb.Magic `tlb:"#0"`
	MinAddrLen      uint16    `tlb:"## 12"`
	MaxAddrLen      uint16    `tlb:"## 12"`
	AddrLenStep     uint16    `tlb:"## 12"`
	WorkchainTypeID uint32    `tlb:"## 32"`
}

func (f workchainFormatExtended) Validate() error {
	if f.MinAddrLen < 64 || f.MinAddrLen > f.MaxAddrLen || f.MaxAddrLen > 1023 || f.AddrLenStep > 1023 {
		return fmt.Errorf("invalid extended workchain format address bounds")
	}
	if f.WorkchainTypeID == 0 {
		return fmt.Errorf("invalid extended workchain type id 0")
	}
	return nil
}

type workchainSplitMergeTimings struct {
	_                     tlb.Magic `tlb:"#0"`
	SplitMergeDelay       uint32    `tlb:"## 32"`
	SplitMergeInterval    uint32    `tlb:"## 32"`
	MinSplitMergeInterval uint32    `tlb:"## 32"`
	MaxSplitMergeDelay    uint32    `tlb:"## 32"`
}
