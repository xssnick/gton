package httpapi

import (
	"encoding/json"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestEstimateFeeResultRejectedExternalMatchesTonHTTPAPI(t *testing.T) {
	base := fees{
		Type:       feesType,
		InFwdFee:   10,
		StorageFee: 20,
	}

	got, apiErr := estimateFeeResult(base, &tvm.TransactionExecutionResult{
		Accepted: false,
	})
	if apiErr != nil {
		t.Fatalf("estimate rejected external fee: %v", apiErr.message)
	}

	data, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("marshal fee result: %v", err)
	}
	const want = `{"@type":"query.fees","source_fees":{"@type":"fees","in_fwd_fee":10,"storage_fee":20,"gas_fee":0,"fwd_fee":0},"destination_fees":[]}`
	if string(data) != want {
		t.Fatalf("result = %s, want %s", data, want)
	}
}

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

func TestEstimateFeeMessageUsageStopsAtSeenCell(t *testing.T) {
	const depth = 64

	root := cell.BeginCell().EndCell()
	for range depth {
		root = cell.BeginCell().MustStoreRef(root).MustStoreRef(root).EndCell()
	}

	usage, rootBits, err := estimateFeeMessageUsage(root)
	if err != nil {
		t.Fatalf("estimate message usage: %v", err)
	}
	if rootBits != 0 {
		t.Fatalf("root bits = %d, want 0", rootBits)
	}
	if usage.cells != depth+1 {
		t.Fatalf("cells = %d, want %d", usage.cells, depth+1)
	}
	if usage.bits != 0 {
		t.Fatalf("bits = %d, want 0", usage.bits)
	}
}
