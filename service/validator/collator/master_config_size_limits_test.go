package collator

import (
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestGovernanceSizeLimitsV3RequiresQueueLimits(t *testing.T) {
	for _, libraryLimit := range []*uint32{nil, new(uint32(123))} {
		current, err := tlb.ToCell(tlb.SizeLimitsConfigV3{
			MaxTransactionLibraryLoads: libraryLimit,
			OutMsgQueueSizeHardLimit:   18000, OutMsgQueueSizeSoftLimit: 12000,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err = validateKnownConfigParameter(current, tlb.ConfigParamSizeLimits); err != nil {
			t.Fatalf("current v3 rejected: %v", err)
		}
		s := current.MustBeginParse()
		prefix := s.MustLoadSlice(s.BitsLeft() - 64)
		legacy := cell.BeginCell().MustStoreSlice(prefix, current.BitsSize()-64).EndCell()
		if err = validateKnownConfigParameter(legacy, tlb.ConfigParamSizeLimits); err == nil {
			t.Fatal("governance accepted v3 without mandatory queue limits")
		}
		var read tlb.SizeLimitsConfig
		if err = tlb.LoadFromCell(&read, legacy.MustBeginParse()); err != nil {
			t.Fatalf("historical read compatibility changed: %v", err)
		}
	}
	v2, err := tlb.ToCell(tlb.SizeLimitsConfigV2{})
	if err != nil {
		t.Fatal(err)
	}
	if err = validateKnownConfigParameter(v2, tlb.ConfigParamSizeLimits); err != nil {
		t.Fatalf("v2 rejected: %v", err)
	}
}
