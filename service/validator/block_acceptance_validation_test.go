package validator

import (
	"bytes"
	"crypto/sha256"
	"math/big"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
)

type acceptedTrailingDataTestCase struct {
	name  string
	index int
	want  string
}

type acceptedKeyConfigTestCase struct {
	name   string
	params map[uint32]*cell.Cell
	want   string
}

func TestParseAcceptedBlockRejectsTrailingReferencedData(t *testing.T) {
	fixture := newAcceptanceTestFixture(t, groups.ShardID{Workchain: 0, Shard: -1 << 63})
	original, err := cell.FromBOC(fixture.candidate.BlockBOC)
	if err != nil {
		t.Fatal(err)
	}

	tests := []acceptedTrailingDataTestCase{
		{name: "block info", index: 0, want: "block info has"},
		{name: "value flow", index: 1, want: "value flow"},
		{name: "block extra", index: 3, want: "block extra has"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			refs := make([]*cell.Cell, original.RefsNum())
			for i := range refs {
				refs[i], err = original.PeekRef(i)
				if err != nil {
					t.Fatal(err)
				}
			}
			refs[test.index] = refs[test.index].ToBuilder().MustStoreBoolBit(true).EndCell()
			poisoned, err := original.RebuildWithRefs(refs)
			if err != nil {
				t.Fatal(err)
			}
			boc := poisoned.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: true})
			rootHash := poisoned.HashKey()
			fileHash := sha256.Sum256(boc)
			block := fixture.candidate.Candidate.Block
			block.RootHash = rootHash[:]
			block.FileHash = fileHash[:]

			for _, resident := range []bool{false, true} {
				name := "decode wire"
				var readyRoot *cell.Cell
				if resident {
					name = "resident root"
					readyRoot = poisoned
				}
				t.Run(name, func(t *testing.T) {
					_, _, err := parseAcceptedBlock(block, boc, readyRoot)
					if err == nil || !strings.Contains(err.Error(), test.want) {
						t.Fatalf("parse block with trailing %s error = %v", test.name, err)
					}
				})
			}
		})
	}
}

func TestBlockAccepterRejectsMalformedCandidateBeforeSerialization(t *testing.T) {
	fixture := newAcceptanceTestFixture(t, groups.ShardID{Workchain: 0, Shard: -1 << 63})
	acceptance := fixture.acceptance(simplex.VoteNotarize, false)
	acceptance.Candidate.Candidate.Block.RootHash = []byte{1}
	node := &acceptanceTestNode{}
	accepter, err := newAcceptanceTestAccepter(fixture, node)
	if err != nil {
		t.Fatal(err)
	}

	err = accepter.acceptForTest(t.Context(), acceptance, acceptanceTestViewResolver(BlockAcceptanceView{}))
	if err == nil || !strings.Contains(err.Error(), "candidate block hashes must be 32 bytes") {
		t.Fatalf("malformed candidate error = %v", err)
	}
	if len(node.blocks) != 0 {
		t.Fatalf("submitted %d malformed candidates", len(node.blocks))
	}
}

func TestValidateAcceptedKeyBlockConfigMatchesValidatorParamPolicy(t *testing.T) {
	validSet := acceptanceTestValidatorSetCell(t)
	tests := []acceptedKeyConfigTestCase{
		{
			name: "current set required",
			want: "current validator set",
		},
		{
			name: "valid current set",
			params: map[uint32]*cell.Cell{
				tlb.ConfigParamCurrentValidators: validSet,
			},
		},
		{
			name: "malformed optional set rejected",
			params: map[uint32]*cell.Cell{
				tlb.ConfigParamCurrentValidators: validSet,
				tlb.ConfigParamNextValidators:    cell.BeginCell().MustStoreUInt(0xff, 8).EndCell(),
			},
			want: "next validator set",
		},
		{
			name: "malformed catchain config defaults",
			params: map[uint32]*cell.Cell{
				tlb.ConfigParamCurrentValidators: validSet,
				tlb.ConfigParamCatchainConfig:    cell.BeginCell().MustStoreUInt(0xff, 8).EndCell(),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			block := acceptanceTestKeyBlock(t, test.params)
			err := validateAcceptedKeyBlockConfig(block)
			if test.want == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("key block config error = %v, want %q", err, test.want)
			}
		})
	}
}

func acceptanceTestKeyBlock(t *testing.T, params map[uint32]*cell.Cell) *tlb.Block {
	t.Helper()

	dict := cell.NewDict(32)
	for id, value := range params {
		wrapped := cell.BeginCell().MustStoreRef(value).EndCell()
		if err := dict.SetIntKey(new(big.Int).SetUint64(uint64(id)), wrapped); err != nil {
			t.Fatal(err)
		}
	}
	config := &tlb.ConfigParams{ConfigAddr: make([]byte, 32)}
	config.Config.Params = dict

	return &tlb.Block{Extra: &tlb.BlockExtra{Custom: &tlb.McBlockExtra{
		KeyBlock:     true,
		ConfigParams: config,
	}}}
}

func acceptanceTestValidatorSetCell(t *testing.T) *cell.Cell {
	t.Helper()

	validators := cell.NewDict(16)
	validator := cell.BeginCell().
		MustStoreUInt(0x73, 8).
		MustStoreUInt(0x8e81278a, 32).
		MustStoreSlice(bytes.Repeat([]byte{0x31}, 32), 256).
		MustStoreUInt(1, 64).
		MustStoreSlice(bytes.Repeat([]byte{0x41}, 32), 256).
		EndCell()
	if err := validators.SetIntKey(big.NewInt(0), validator); err != nil {
		t.Fatal(err)
	}
	root, err := tlb.ToCell(&tlb.ValidatorSetExt{
		UTimeSince:  1,
		UTimeUntil:  2,
		Total:       1,
		Main:        1,
		TotalWeight: 1,
		List:        validators,
	})
	if err != nil {
		t.Fatal(err)
	}

	return root
}
