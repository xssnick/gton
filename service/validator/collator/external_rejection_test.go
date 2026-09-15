package collator

import (
	"bytes"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

func TestExternalValidationRejectionDoesNotAbortCandidate(t *testing.T) {
	type rejectionCase struct {
		name string
		err  string
	}
	type executionMode struct {
		name    string
		workers int
	}
	for _, test := range []rejectionCase{
		{name: "bits", err: "inbound external message size exceeds limit"},
		{name: "cells", err: "inbound external message size exceeds limit"},
		{name: "depth", err: "inbound external message depth exceeds limit"},
		{name: "anycast", err: "invalid inbound external message destination"},
		{name: "merkle_depth", err: "inbound external message merkle depth exceeds limit"},
	} {
		for _, mode := range []executionMode{
			{name: "sequential", workers: -1},
			{name: "waves_inline", workers: 1},
			{name: "waves_parallel", workers: 4},
		} {
			t.Run(test.name+"/"+mode.name, func(t *testing.T) {
				req := emptyCandidateRequest(t)
				req.internalWaveWorkers = mode.workers
				dst := address.NewAddress(0, 0, bytes.Repeat([]byte{0x75}, 32))
				other := address.NewAddress(0, 0, bytes.Repeat([]byte{0x76}, 32))
				req.Previous.State = stateWithAccounts(t, req.Previous.State, activeContracts(t, req.Header.GenUtime,
					activeContract{address: dst, code: externalAcceptCode(t), balance: 100_000_000_000},
					activeContract{address: other, code: externalAcceptCode(t), balance: 100_000_000_000},
				))
				loose := req.Masterchain.Config
				configRoot := loose.execution.Root()
				parsed, err := (tlb.BlockchainConfig{Root: configRoot}).GetSizeLimitsConfig()
				if err != nil {
					t.Fatal(err)
				}
				limits, ok := parsed.Config.(tlb.SizeLimitsConfigV2)
				if !ok {
					t.Fatalf("fixture size limits = %T, want V2", parsed.Config)
				}

				tail := cell.BeginCell().MustStoreUInt(0xaa, 8).EndCell()
				for range 3 {
					tail = cell.BeginCell().MustStoreRef(tail).EndCell()
				}
				switch test.name {
				case "bits":
					limits.MaxMsgBits = 4
				case "cells":
					limits.MaxMsgCells = 1
				case "depth":
					limits.MaxExtMsgDepth = 1
				case "anycast":
					// The rewrite preserves the account, while version 10 disables
					// the address form that version 9 admitted.
					dst = dst.WithAnycast(address.NewAnycast(1, []byte{0}))
					version, err := tlb.ToCell(&tlb.GlobalVersion{Version: 9, Capabilities: loose.capabilities})
					if err != nil {
						t.Fatal(err)
					}
					loose = epochConfigOf(t, masterBuildConfigWithParam(t, configRoot, int64(tlb.ConfigParamGlobalVersion), version))
				case "merkle_depth":
					for range 3 {
						tail, err = cell.CreateMerkleProof(tail)
						if err != nil {
							t.Fatal(err)
						}
					}
				}
				limitCell, err := tlb.ToCell(&limits)
				if err != nil {
					t.Fatal(err)
				}
				req.Masterchain.Config = epochConfigOf(t,
					masterBuildConfigWithParam(t, configRoot, int64(tlb.ConfigParamSizeLimits), limitCell))
				root, err := tlb.ToCell(&tlb.ExternalMessage{
					DstAddr: dst,
					Body:    cell.BeginCell().MustStoreRef(tail).EndCell(),
				})
				if err != nil {
					t.Fatal(err)
				}
				bad := externalInput(t, root)

				// Size/depth limits and global version can change while a valid
				// external waits in the pool. Merkle depth is always capped at two.
				if test.name != "merkle_depth" {
					before := req
					before.Masterchain.Config = loose
					before.Externals = []ExternalInput{bad}
					admitted, err := testBuilder().BuildShard(t.Context(), before)
					if err != nil || admitted.Stats.ExternalIncluded != 1 {
						t.Fatalf("message was not executable before config change: %v", err)
					}
				}

				// Pin the actual TVM rejection, not a synthetic error string.
				c, err := testBuilder().prepare(t.Context(), req)
				if err != nil {
					t.Fatal(err)
				}
				lane, err := c.account(dst)
				if err != nil {
					t.Fatal(err)
				}
				_, err = c.builder.machine.EmulateTransaction(c.blockCtx, lane.current, bad.message,
					tvm.TransactionOptions{LogicalTime: int64(c.header.StartLt + 1)})
				if err == nil || err.Error() != test.err {
					t.Fatalf("TVM rejection = %v, want %q", err, test.err)
				}
				goodRoot, err := tlb.ToCell(&tlb.ExternalMessage{DstAddr: other, Body: cell.BeginCell().EndCell()})
				if err != nil {
					t.Fatal(err)
				}
				good := externalInput(t, goodRoot)
				req.Externals = []ExternalInput{bad, good}
				candidate, err := testBuilder().BuildShard(t.Context(), req)
				if err != nil {
					t.Fatalf("external rejection aborted collation: %v", err)
				}
				if candidate.Stats.ExternalAttempts != 2 || candidate.Stats.ExternalNotAccepted != 1 ||
					candidate.Stats.ExternalIncluded != 1 || candidate.Stats.Transactions != 1 {
					t.Fatalf("unexpected stats: %+v", candidate.Stats)
				}
				if len(candidate.Externals) != 2 || candidate.Externals[0].Ref != bad.Ref ||
					candidate.Externals[0].Outcome != msgpool.ExternalNotAccepted ||
					candidate.Externals[1].Ref != good.Ref || candidate.Externals[1].Outcome != msgpool.ExternalIncluded {
					t.Fatalf("unexpected feedback: %+v", candidate.Externals)
				}

				req.Externals = []ExternalInput{good}
				acceptedOnly, err := testBuilder().BuildShard(t.Context(), req)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(candidate.BlockBOC, acceptedOnly.BlockBOC) ||
					candidate.State.HashKey() != acceptedOnly.State.HashKey() {
					t.Fatal("rejected external changed the block or account state")
				}
			})
		}
	}
}
