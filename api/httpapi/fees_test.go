package httpapi

import (
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
)

func TestEstimateFeesFromTransactionUsesOrdinaryPhases(t *testing.T) {
	totalFwdFees := tlb.FromNanoTONU(333)
	tx := &tlb.Transaction{
		Description: &tlb.TransactionDescriptionOrdinary{
			StoragePhase: &tlb.StoragePhase{
				StorageFeesCollected: tlb.FromNanoTONU(111),
			},
			ComputePhase: tlb.ComputePhase{
				Phase: &tlb.ComputePhaseVM{
					GasFees: tlb.FromNanoTONU(222),
				},
			},
			ActionPhase: &tlb.ActionPhase{
				TotalFwdFees: &totalFwdFees,
			},
		},
	}

	got, apiErr := estimateFeesFromTransaction(fees{
		Type:       feesType,
		InFwdFee:   10,
		StorageFee: 20,
	}, tx)
	if apiErr != nil {
		t.Fatalf("estimate fees failed: %v", apiErr.message)
	}
	if got.Type != feesType {
		t.Fatalf("type = %q, want %q", got.Type, feesType)
	}
	if got.InFwdFee != 10 {
		t.Fatalf("in_fwd_fee = %d, want 10", got.InFwdFee)
	}
	if got.StorageFee != 111 {
		t.Fatalf("storage_fee = %d, want 111", got.StorageFee)
	}
	if got.GasFee != 222 {
		t.Fatalf("gas_fee = %d, want 222", got.GasFee)
	}
	if got.FwdFee != 333 {
		t.Fatalf("fwd_fee = %d, want 333", got.FwdFee)
	}
}
