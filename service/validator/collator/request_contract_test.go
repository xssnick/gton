package collator

import (
	"bytes"
	"context"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/groups"
)

type requestBoundaryTestCase struct {
	name    string
	wantErr string
	mutate  func(testing.TB, *ShardRequest)
}

func TestNextBlockStartLT(t *testing.T) {
	tests := []struct {
		name       string
		previousLT uint64
		masterLT   uint64
		want       uint64
		wantErr    bool
	}{
		{name: "genesis", want: logicalTimeAlignment},
		{name: "previous state bound", previousLT: 7_002_000, masterLT: 7_001_000, want: 8_000_000},
		{name: "masterchain bound", previousLT: 7_001_000, masterLT: 7_002_000, want: 8_000_000},
		{name: "aligned bound", previousLT: 8_000_000, want: 9_000_000},
		{name: "signed range", previousLT: math.MaxInt64, wantErr: true},
		{name: "overflow", previousLT: math.MaxUint64, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := nextBlockStartLT(test.previousLT, test.masterLT)
			if (err != nil) != test.wantErr {
				t.Fatalf("error = %v, wantErr %t", err, test.wantErr)
			}
			if got != test.want {
				t.Fatalf("start lt = %d, want %d", got, test.want)
			}
		})
	}
}

func TestValidatePredecessorMasterchainReferenceAllowsShardZerostate(t *testing.T) {
	next := &tlb.ExtBlkRef{SeqNo: 1, EndLt: 1}
	zerostate := ton.BlockIDExt{Workchain: 0, Shard: math.MinInt64, SeqNo: 0}
	if err := validatePredecessorMasterchainReference(zerostate, nil, next); err != nil {
		t.Fatalf("allow initial basechain master reference: %v", err)
	}

	block := zerostate
	block.SeqNo = 1
	if err := validatePredecessorMasterchainReference(block, nil, next); err == nil ||
		!strings.Contains(err.Error(), "previous basechain state has no masterchain reference") {
		t.Fatalf("non-zero predecessor error = %v", err)
	}
}

func TestBuildCandidateSerializesRequestHeaderContext(t *testing.T) {
	req := emptyCandidateRequest(t)
	previousState := loadPreviousShardState(t, req)

	req.Masterchain.EndLT = previousState.GenLT + 700
	req.Masterchain.GenUtime = previousState.GenUTime + 1
	req.Masterchain.VertSeqno = previousState.VertSeqno + 1
	req.Header.GenUtime = previousState.GenUTime + 2
	req.Header.GenUtimeMS = uint64(req.Header.GenUtime)*1000 + 321
	req.Masterchain.Groups.Active[0].ValidatorSetHash = 0x10203040
	req.Masterchain.Groups.Active[0].CatchainSeqno = 0x50607080
	req.Masterchain.Groups.LastKeyBlockSeqno = 6_900_001

	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}

	var block tlb.Block
	if err = parseExact(&block, candidateBlock(t, candidate)); err != nil {
		t.Fatalf("decode candidate block: %v", err)
	}
	if block.GlobalID != previousState.GlobalID {
		t.Fatalf("block global ID = %d, want %d", block.GlobalID, previousState.GlobalID)
	}
	wantStartLT := requestStartLT(t, req)
	if block.BlockInfo.GenUtime != req.Header.GenUtime || block.BlockInfo.StartLt != wantStartLT {
		t.Fatalf("block epoch = (%d, %d), want (%d, %d)",
			block.BlockInfo.GenUtime, block.BlockInfo.StartLt, req.Header.GenUtime, wantStartLT)
	}
	if block.BlockInfo.VertSeqNo != req.Masterchain.VertSeqno {
		t.Fatalf("block vertical seqno = %d, want %d", block.BlockInfo.VertSeqNo, req.Masterchain.VertSeqno)
	}
	if block.BlockInfo.GenValidatorListHashShort != req.Masterchain.Groups.Active[0].ValidatorSetHash ||
		block.BlockInfo.GenCatchainSeqno != req.Masterchain.Groups.Active[0].CatchainSeqno ||
		block.BlockInfo.PrevKeyBlockSeqno != req.Masterchain.Groups.LastKeyBlockSeqno {
		t.Fatalf("unexpected session header: %+v", block.BlockInfo)
	}
	if block.BlockInfo.PrevRef.Pruned || block.BlockInfo.PrevRef.Prev2 != nil {
		t.Fatalf("unexpected previous block reference shape: %+v", block.BlockInfo.PrevRef)
	}
	assertRequestBlockReference(t, block.BlockInfo.PrevRef.Prev1, req.Previous.ID, previousState.GenLT)
	if block.BlockInfo.MasterRef == nil {
		t.Fatal("masterchain reference is absent")
	}
	assertRequestBlockReference(t, *block.BlockInfo.MasterRef, req.Masterchain.ID, req.Masterchain.EndLT)

	if candidate.ID.Workchain != req.Previous.ID.Workchain || candidate.ID.Shard != req.Previous.ID.Shard ||
		candidate.ID.SeqNo != req.Previous.ID.SeqNo+1 {
		t.Fatalf("candidate ID = %+v, want shard (%d, %d) seqno %d",
			candidate.ID, req.Previous.ID.Workchain, req.Previous.ID.Shard, req.Previous.ID.SeqNo+1)
	}
	if block.BlockInfo.EndLt != candidate.Stats.EndLT {
		t.Fatalf("block end lt = %d, want candidate end lt %d", block.BlockInfo.EndLt, candidate.Stats.EndLT)
	}
	if block.Extra == nil || !bytes.Equal(block.Extra.RandSeed, req.RandSeed[:]) ||
		!bytes.Equal(block.Extra.CreatedBy, req.CreatedBy[:]) {
		t.Fatal("block extra does not preserve request seed and creator")
	}

	var nextState tlb.ShardStateUnsplit
	if err = parseExact(&nextState, candidate.State); err != nil {
		t.Fatalf("decode candidate state: %v", err)
	}
	if nextState.GlobalID != previousState.GlobalID || nextState.GenUTime != req.Header.GenUtime ||
		nextState.GenLT != candidate.Stats.EndLT {
		t.Fatalf("unexpected candidate state boundary: %+v", nextState)
	}
	var nextStats tlb.ShardStateStats
	if err = parseExact(&nextStats, nextState.Stats); err != nil {
		t.Fatalf("decode candidate state statistics: %v", err)
	}
	if nextStats.MasterRef == nil {
		t.Fatal("candidate state masterchain reference is absent")
	}
	assertRequestBlockReference(t, *nextStats.MasterRef, req.Masterchain.ID, req.Masterchain.EndLT)
}

func TestBuildCandidateRejectsInconsistentRequestBoundaries(t *testing.T) {
	base := emptyCandidateRequest(t)
	previousState := loadPreviousShardState(t, base)
	var previousStats tlb.ShardStateStats
	if err := parseExact(&previousStats, previousState.Stats); err != nil {
		t.Fatal(err)
	}

	tests := []requestBoundaryTestCase{
		{
			name:    "zero random seed",
			wantErr: "random seed is zero",
			mutate: func(_ testing.TB, req *ShardRequest) {
				req.RandSeed = [32]byte{}
			},
		},
		{
			name:    "previous root hash width",
			wantErr: "previous block id: root and file hashes must be 256 bits",
			mutate: func(_ testing.TB, req *ShardRequest) {
				req.Previous.ID.RootHash = append([]byte(nil), req.Previous.ID.RootHash[:31]...)
			},
		},
		{
			name:    "previous file hash width",
			wantErr: "previous block id: root and file hashes must be 256 bits",
			mutate: func(_ testing.TB, req *ShardRequest) {
				req.Previous.ID.FileHash = append([]byte(nil), req.Previous.ID.FileHash[:31]...)
			},
		},
		{
			name:    "previous workchain",
			wantErr: "previous block id differs from the previous state",
			mutate: func(_ testing.TB, req *ShardRequest) {
				req.Previous.ID.Workchain++
			},
		},
		{
			name:    "previous shard",
			wantErr: "previous block id differs from the previous state",
			mutate: func(_ testing.TB, req *ShardRequest) {
				req.Previous.ID.Shard++
			},
		},
		{
			name:    "previous sequence number",
			wantErr: "previous block id differs from the previous state",
			mutate: func(_ testing.TB, req *ShardRequest) {
				req.Previous.ID.SeqNo++
			},
		},
		{
			name:    "masterchain root hash width",
			wantErr: "masterchain block id: root and file hashes must be 256 bits",
			mutate: func(_ testing.TB, req *ShardRequest) {
				req.Masterchain.ID.RootHash = append([]byte(nil), req.Masterchain.ID.RootHash[:31]...)
			},
		},
		{
			name:    "masterchain file hash width",
			wantErr: "masterchain block id: root and file hashes must be 256 bits",
			mutate: func(_ testing.TB, req *ShardRequest) {
				req.Masterchain.ID.FileHash = append([]byte(nil), req.Masterchain.ID.FileHash[:31]...)
			},
		},
		{
			name:    "masterchain workchain",
			wantErr: "reference block is not a masterchain block",
			mutate: func(_ testing.TB, req *ShardRequest) {
				req.Masterchain.ID.Workchain = 0
			},
		},
		{
			name:    "masterchain shard",
			wantErr: "reference block is not a masterchain block",
			mutate: func(_ testing.TB, req *ShardRequest) {
				req.Masterchain.ID.Shard = 0
			},
		},
		{
			name:    "masterchain reference regression",
			wantErr: "masterchain reference precedes",
			mutate: func(_ testing.TB, req *ShardRequest) {
				req.Masterchain.ID.SeqNo = previousStats.MasterRef.SeqNo - 1
				req.Masterchain.EndLT = previousStats.MasterRef.EndLt - 1
			},
		},
		{
			name:    "masterchain end lt regression",
			wantErr: "masterchain reference precedes",
			mutate: func(_ testing.TB, req *ShardRequest) {
				req.Masterchain.ID.SeqNo = previousStats.MasterRef.SeqNo + 1
				req.Masterchain.EndLT = previousStats.MasterRef.EndLt
			},
		},
		{
			name:    "masterchain reference conflict",
			wantErr: "masterchain reference conflicts",
			mutate: func(_ testing.TB, req *ShardRequest) {
				req.Masterchain.ID.SeqNo = previousStats.MasterRef.SeqNo
				req.Masterchain.EndLT = previousStats.MasterRef.EndLt
			},
		},
		{
			name:    "previous masterchain reference absent",
			wantErr: "previous basechain state has no masterchain reference",
			mutate: func(tb testing.TB, req *ShardRequest) {
				rewritePreviousShardState(tb, req, func(state *tlb.ShardStateUnsplit) {
					var stats tlb.ShardStateStats
					if err := parseExact(&stats, state.Stats); err != nil {
						tb.Fatal(err)
					}
					stats.MasterRef = nil
					root, err := stats.ToCell()
					if err != nil {
						tb.Fatal(err)
					}
					state.Stats = root
				})
			},
		},
		{
			name:    "millisecond generation time",
			wantErr: "millisecond generation time",
			mutate: func(_ testing.TB, req *ShardRequest) {
				req.Header.GenUtimeMS += 1000
			},
		},
		{
			name:    "masterchain vertical seqno regression",
			wantErr: "vertical seqno",
			mutate: func(_ testing.TB, req *ShardRequest) {
				req.Masterchain.VertSeqno = previousState.VertSeqno - 1
			},
		},
		{
			name:    "generation time precedes previous state",
			wantErr: "generation time",
			mutate: func(_ testing.TB, req *ShardRequest) {
				req.Header.GenUtime = previousState.GenUTime - 1
				req.Header.GenUtimeMS = uint64(req.Header.GenUtime) * 1000
			},
		},
		{
			name:    "generation time precedes masterchain context",
			wantErr: "generation time",
			mutate: func(_ testing.TB, req *ShardRequest) {
				req.Masterchain.GenUtime = req.Header.GenUtime + 1
			},
		},
		{
			name:    "start lt signed range",
			wantErr: "start lt exceeds signed TVM range",
			mutate: func(_ testing.TB, req *ShardRequest) {
				req.Masterchain.EndLT = uint64(math.MaxInt64)
			},
		},
		{
			name:    "validator groups absent",
			wantErr: "masterchain config or group snapshot is absent",
			mutate: func(_ testing.TB, req *ShardRequest) {
				req.Masterchain.Groups = nil
			},
		},
		{
			name:    "validator groups not ready",
			wantErr: "validator group snapshot is not ready",
			mutate: func(_ testing.TB, req *ShardRequest) {
				req.Masterchain.Groups.Ready = false
			},
		},
		{
			name:    "validator groups masterchain mismatch",
			wantErr: "validator group snapshot belongs to another masterchain block",
			mutate: func(_ testing.TB, req *ShardRequest) {
				req.Masterchain.Groups.MasterchainBlock.SeqNo--
			},
		},
		{
			name:    "validator groups shard absent",
			wantErr: "validator group snapshot has no active shard",
			mutate: func(_ testing.TB, req *ShardRequest) {
				req.Masterchain.Groups.Active = nil
			},
		},
		{
			name:    "validator groups shard duplicate",
			wantErr: "validator group snapshot has duplicate active shard",
			mutate: func(_ testing.TB, req *ShardRequest) {
				req.Masterchain.Groups.Active = append(req.Masterchain.Groups.Active, req.Masterchain.Groups.Active[0])
			},
		},
		{
			name:    "validator groups key block ahead",
			wantErr: "previous key block is ahead of the masterchain context",
			mutate: func(_ testing.TB, req *ShardRequest) {
				req.Masterchain.Groups.LastKeyBlockSeqno = req.Masterchain.ID.SeqNo + 1
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			req := base
			if base.Masterchain.Groups != nil {
				groupsCopy := *base.Masterchain.Groups
				groupsCopy.Active = append([]groups.Session(nil), base.Masterchain.Groups.Active...)
				req.Masterchain.Groups = &groupsCopy
			}
			test.mutate(t, &req)

			_, err := testBuilder().BuildShard(context.Background(), req)
			if !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("error = %v, want ErrInvalidInput", err)
			}
			if !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("error = %q, want it to contain %q", err, test.wantErr)
			}
		})
	}
}

func TestBuildShardUsesPredecessorGlobalID(t *testing.T) {
	req := emptyCandidateRequest(t)
	rewritePreviousShardState(t, &req, func(state *tlb.ShardStateUnsplit) {
		state.GlobalID++
	})

	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	root, err := cell.FromBOC(candidate.BlockBOC)
	if err != nil {
		t.Fatal(err)
	}
	var block tlb.Block
	if err = parseExact(&block, root); err != nil {
		t.Fatal(err)
	}
	var previous tlb.ShardStateUnsplit
	if err = parseExact(&previous, req.Previous.State); err != nil {
		t.Fatal(err)
	}
	if block.GlobalID != previous.GlobalID {
		t.Fatalf("block global id = %d, want predecessor %d", block.GlobalID, previous.GlobalID)
	}
}

func TestBuildCandidateMarksBeforeSplitState(t *testing.T) {
	req := emptyCandidateRequest(t)
	req.BeforeSplit = true
	req.Masterchain.Groups.Active[0].Registered[0].FSM = groups.ShardFSM{
		Kind:     groups.ShardFSMSplit,
		UTime:    req.Header.GenUtime,
		Interval: 10,
	}

	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	var block tlb.Block
	if err = parseExact(&block, candidateBlock(t, candidate)); err != nil {
		t.Fatal(err)
	}
	if !block.BlockInfo.BeforeSplit {
		t.Fatal("block header is not marked before split")
	}
	var state tlb.ShardStateUnsplit
	if err = parseExact(&state, candidate.State); err != nil {
		t.Fatal(err)
	}
	if !state.BeforeSplit {
		t.Fatal("next state is not marked before split")
	}
}

func TestBuildCandidateRejectsBeforeSplitOutsideRegisteredWindow(t *testing.T) {
	tests := []struct {
		name string
		fsm  func(uint32) groups.ShardFSM
	}{
		{
			name: "announcement absent",
			fsm:  func(uint32) groups.ShardFSM { return groups.ShardFSM{} },
		},
		{
			name: "before interval",
			fsm: func(genUtime uint32) groups.ShardFSM {
				return groups.ShardFSM{Kind: groups.ShardFSMSplit, UTime: genUtime + 1, Interval: 10}
			},
		},
		{
			name: "at interval end",
			fsm: func(genUtime uint32) groups.ShardFSM {
				return groups.ShardFSM{Kind: groups.ShardFSMSplit, UTime: genUtime - 10, Interval: 10}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			req := emptyCandidateRequest(t)
			req.BeforeSplit = true
			req.Masterchain.Groups.Active[0].Registered[0].FSM = test.fsm(req.Header.GenUtime)

			_, err := testBuilder().BuildShard(context.Background(), req)
			if !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("error = %v, want ErrInvalidInput", err)
			}
		})
	}
}

func TestBuildCandidateRejectsMismatchedStorageStatHash(t *testing.T) {
	req := emptyCandidateRequest(t)
	req.StorageStats = AccountStorageStats{
		cell.Hash{1}: cell.BeginCell().MustStoreUInt(1, 1).EndCell(),
	}

	if _, err := testBuilder().BuildShard(context.Background(), req); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("error = %v, want ErrInvalidInput", err)
	}
}

func TestFullCollatedCandidateRequiresPreviousBlockBinding(t *testing.T) {
	req := emptyCandidateRequest(t)
	req.Masterchain.Config.capabilities |= capFullCollatedData
	attachFullCollatedTestNeighbors(t, &req)

	_, err := testBuilder().BuildShard(context.Background(), req)
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "previous block root") {
		t.Fatalf("error = %v, want missing previous-block binding", err)
	}
}

func loadPreviousShardState(tb testing.TB, req ShardRequest) tlb.ShardStateUnsplit {
	tb.Helper()

	var state tlb.ShardStateUnsplit
	if err := parseExact(&state, req.Previous.State); err != nil {
		tb.Fatalf("decode previous state: %v", err)
	}
	return state
}

func rewritePreviousShardState(tb testing.TB, req *ShardRequest, mutate func(*tlb.ShardStateUnsplit)) {
	tb.Helper()

	state := loadPreviousShardState(tb, *req)
	mutate(&state)
	root, err := tlb.ToCell(&state)
	if err != nil {
		tb.Fatalf("serialize previous state: %v", err)
	}
	req.Previous.State = root
}

func assertRequestBlockReference(tb testing.TB, got tlb.ExtBlkRef, id ton.BlockIDExt, endLT uint64) {
	tb.Helper()

	if got.SeqNo != id.SeqNo || got.EndLt != endLT || !bytes.Equal(got.RootHash, id.RootHash) ||
		!bytes.Equal(got.FileHash, id.FileHash) {
		tb.Fatalf("block reference = %+v, want id %+v with end lt %d", got, id, endLT)
	}
}
