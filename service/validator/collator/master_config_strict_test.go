package collator

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestMasterConfigRejectsMalformedGovernanceParameters(t *testing.T) {
	base := loadMainnetConfig(t).execution.Root()
	prices := cell.BeginCell().MustStoreCoins(1).MustStoreCoins(2).EndCell()
	critical := cell.NewDict(32)
	if err := critical.SetIntKey(big.NewInt(20), cell.BeginCell().EndCell()); err != nil {
		t.Fatal(err)
	}
	malformedCritical := cell.NewDict(32)
	if err := malformedCritical.SetIntKey(big.NewInt(20), cell.BeginCell().MustStoreBoolBit(true).EndCell()); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name             string
		id               int64
		valid, malformed *cell.Cell
	}{
		{"mint prices trailing bit", 6, prices, prices.ToBuilder().MustStoreBoolBit(true).EndCell()},
		{"mint prices trailing reference", 6, prices, prices.ToBuilder().MustStoreRef(cell.BeginCell().EndCell()).EndCell()},
		{"critical set nonempty True", 10, critical.AsCell(), malformedCritical.AsCell()},
		{"mint price nonminimal zero", 6, prices, cell.BeginCell().MustStoreUInt(1, 4).MustStoreUInt(0, 8).MustStoreCoins(2).EndCell()},
		{"mint price nonminimal positive", 6, prices, cell.BeginCell().MustStoreUInt(2, 4).MustStoreUInt(1, 16).MustStoreCoins(2).EndCell()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			validRoot := masterBuildConfigWithParam(t, base, tc.id, tc.valid)
			malformedRoot := masterBuildConfigWithParam(t, base, tc.id, tc.malformed)
			validFixture := newMasterBuildFixtureWith(t, masterBuildFixtureOptions{accountConfigRoot: validRoot})
			candidate, err := testBuilder().BuildMaster(context.Background(), validFixture.request)
			if err != nil {
				t.Fatal(err)
			}
			verification := MasterVerificationRequest{
				Previous: validFixture.request.Previous, Config: validFixture.request.Config,
				Groups: validFixture.request.Groups, ShardTops: validFixture.request.ShardTops,
				Neighbors: validFixture.request.Neighbors, NeighborShardEndLT: validFixture.request.NeighborShardEndLT,
				Semantics: NewSemanticVerifier(tvm.NewTVM()), Candidate: candidate,
			}
			if err = verifyMasterCandidateForTest(context.Background(), verification); err != nil {
				t.Fatalf("valid governance change: %v", err)
			}

			malformedFixture := newMasterBuildFixtureWith(t, masterBuildFixtureOptions{accountConfigRoot: malformedRoot})
			_, err = testBuilder().BuildMaster(context.Background(), malformedFixture.request)
			assertInvalidConfigParameter(t, err, tc.id)

			// The config account has no transactions in this fixture. Replace its
			// carried config in both endpoints and the adopted config in McStateExtra,
			// then rebuild the Merkle update: all balances and transaction results stay
			// valid, and validation must reject the TL-B itself.
			forged := cloneVerificationCandidate(candidate)
			forged.State = replaceMasterConfigTestCell(t, candidate.State, validRoot.HashKey(), malformedRoot)
			previous := malformedFixture.request.Previous
			forged.State = masterConfigTestRewriteHistory(t, forged.State, previous)
			update, err := cell.CreateMerkleUpdate(previous.State, forged.State)
			if err != nil {
				t.Fatal(err)
			}
			forged.StateUpdate = update
			custom := replaceMasterConfigTestCell(t, verificationMasterCustomCell(t, candidate), validRoot.HashKey(), malformedRoot)
			rewriteVerificationMasterBlock(t, forged, custom, func(block *tlb.Block) {
				block.StateUpdate = update
				block.BlockInfo.PrevRef.Prev1.RootHash = previous.ID.RootHash
				block.BlockInfo.PrevRef.Prev1.FileHash = previous.ID.FileHash
			})
			verification.Previous = previous
			verification.Groups = malformedFixture.request.Groups
			verification.Candidate = forged
			err = verifyMasterCandidateForTest(context.Background(), verification)
			assertInvalidConfigParameter(t, err, tc.id)
		})
	}
}

func assertInvalidConfigParameter(t *testing.T, err error, id int64) {
	t.Helper()
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), fmt.Sprintf("config parameter %d:", id)) {
		t.Fatalf("malformed parameter %d error = %v", id, err)
	}
}

// Ordinary state cells only; Merkle updates are rebuilt from the rewritten
// endpoints instead of editing their cached hashes.
func replaceMasterConfigTestCell(t *testing.T, root *cell.Cell, from cell.Hash, to *cell.Cell) *cell.Cell {
	t.Helper()
	if root.HashKey() == from {
		return to
	}
	if root.RefsNum() == 0 {
		return root
	}
	s := root.MustBeginParse()
	data := s.MustLoadSlice(s.BitsLeft())
	result := cell.BeginCell().MustStoreSlice(data, root.BitsSize())
	changed := false
	for s.RefsNum() != 0 {
		child := s.MustLoadRef()
		childRoot := child.MustToCell()
		next := replaceMasterConfigTestCell(t, childRoot, from, to)
		changed = changed || next.HashKey() != childRoot.HashKey()
		result.MustStoreRef(next)
	}
	if !changed {
		return root
	}
	return result.EndCell()
}

func TestConfigParameterNestedValidation(t *testing.T) {
	proposal := tlb.ConfigProposalSetup{MinTotRounds: 1, MaxTotRounds: 2, MinStoreSec: 3, MaxStoreSec: 4}
	proposalRoot, err := tlb.ToCell(proposal)
	if err != nil {
		t.Fatal(err)
	}
	voting := func(child *cell.Cell) *cell.Cell {
		return cell.BeginCell().MustStoreUInt(0x91, 8).MustStoreRef(child).MustStoreRef(proposalRoot).EndCell()
	}
	proposal.MinTotRounds = 3
	badProposal, err := tlb.ToCell(proposal)
	if err != nil {
		t.Fatal(err)
	}
	dict := func(keyBits uint, value *cell.Cell) *cell.Cell {
		d := cell.NewDict(keyBits)
		if err := d.SetIntKey(big.NewInt(1), value); err != nil {
			t.Fatal(err)
		}
		return cell.BeginCell().MustStoreDict(d).EndCell()
	}
	simplex := cell.BeginCell().MustStoreUInt(0x22, 8).MustStoreUInt(0, 8).MustStoreUInt(1, 32).
		MustStoreBuilder(dict(8, cell.BeginCell().MustStoreUInt(1, 33).EndCell()).ToBuilder()).EndCell()
	workchain := tlb.WorkchainDescrV1{WorkchainDescrFields: tlb.WorkchainDescrFields{
		Basic: true, Active: true, AcceptMsgs: true, ZeroStateRootHash: make([]byte, 32), ZeroStateFileHash: make([]byte, 32),
		Format: tlb.WorkchainFormatBasic{},
	}}
	goodWorkchain, err := tlb.ToCell(workchain)
	if err != nil {
		t.Fatal(err)
	}
	workchain.MinSplit = 2
	workchain.MaxSplit = 1
	badWorkchain, err := tlb.ToCell(workchain)
	if err != nil {
		t.Fatal(err)
	}
	validator := cell.BeginCell().MustStoreUInt(0x53, 8).MustStoreUInt(0x8e81278a, 32).MustStoreUInt(1, 256).MustStoreUInt(1, 64).EndCell()
	validatorSet := func(value *cell.Cell) *cell.Cell {
		return cell.BeginCell().MustStoreUInt(0x12, 8).MustStoreUInt(0, 64).MustStoreUInt(1, 16).MustStoreUInt(1, 16).
			MustStoreUInt(1, 64).MustStoreBuilder(dict(16, value).ToBuilder()).EndCell()
	}
	set := cell.NewDict(32)
	if err := set.SetIntKey(big.NewInt(0), cell.BeginCell().EndCell()); err != nil {
		t.Fatal(err)
	}
	if err := set.SetIntKey(big.NewInt(1), cell.BeginCell().EndCell()); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name  string
		id    uint32
		root  *cell.Cell
		valid bool
	}{
		{"proposal", 11, voting(proposalRoot), true},
		{"proposal trailing bit", 11, voting(proposalRoot.ToBuilder().MustStoreBoolBit(true).EndCell()), false},
		{"proposal bounds", 11, voting(badProposal), false},
		{"workchain", 12, dict(32, goodWorkchain), true},
		{"workchain bounds", 12, dict(32, badWorkchain), false},
		{"workchain trailing bit", 12, dict(32, goodWorkchain.ToBuilder().MustStoreBoolBit(true).EndCell()), false},
		{"simplex uint32 dictionary value", 30, cell.BeginCell().MustStoreUInt(0x10, 8).MustStoreBoolBit(true).MustStoreBoolBit(false).MustStoreRef(simplex).EndCell(), false},
		{"fundamental True reference", 31, dict(256, cell.BeginCell().MustStoreRef(cell.BeginCell().EndCell()).EndCell()), false},
		{"validator", 34, validatorSet(validator), true},
		{"validator trailing bit", 34, validatorSet(validator.ToBuilder().MustStoreBoolBit(true).EndCell()), false},
		{"critical fork", 10, set.AsCell(), true},
		{"critical fork trailing bit", 10, set.AsCell().ToBuilder().MustStoreBoolBit(true).EndCell(), false},
		{"burn fraction", 5, cell.BeginCell().MustStoreUInt(1, 8).MustStoreBoolBit(false).MustStoreUInt(2, 32).MustStoreUInt(1, 32).EndCell(), false},
		{"catchain zero lifetime", 28, cell.BeginCell().MustStoreUInt(0xc1, 8).MustStoreBigUInt(big.NewInt(0), 128).EndCell(), false},
		{"consensus v4 QUIC allowed", 29, cell.BeginCell().MustStoreUInt(0xd9, 8).MustStoreUInt(2, 8).MustStoreUInt(1, 8).MustStoreSlice(make([]byte, 34), 7*32+16+32).EndCell(), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateKnownConfigParameter(tc.root, tc.id)
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%t, err=%v", tc.valid, err)
			}
		})
	}
}

func masterConfigTestRewriteHistory(t *testing.T, root *cell.Cell, previous PreviousBlock) *cell.Cell {
	t.Helper()
	var state tlb.ShardStateUnsplit
	if err := parseExact(&state, root); err != nil {
		t.Fatal(err)
	}
	var extra tlb.McStateExtra
	if err := parseExact(&extra, state.McStateExtra); err != nil {
		t.Fatal(err)
	}
	info, err := parseMasterStateInfo(extra.Info)
	if err != nil {
		t.Fatal(err)
	}
	var value cell.Slice
	if err := info.PrevBlocks.LoadValueByUintKeyInto(uint64(previous.ID.SeqNo), &value); err != nil {
		t.Fatal(err)
	}
	var ref tlb.KeyExtBlkRef
	if err := tlb.LoadFromCell(&ref, &value); err != nil {
		t.Fatal(err)
	}
	ref.BlkRef.RootHash = previous.ID.RootHash
	ref.BlkRef.FileHash = previous.ID.FileHash
	valueRoot, err := tlb.ToCell(ref)
	if err != nil {
		t.Fatal(err)
	}
	dict, err := writableOldMCBlocks(info.PrevBlocks)
	if err != nil {
		t.Fatal(err)
	}
	if err = dict.Set(cell.BeginCell().MustStoreUInt(uint64(previous.ID.SeqNo), 32).EndCell(), valueRoot); err != nil {
		t.Fatal(err)
	}
	info.PrevBlocks = &tlb.OldMcBlocksInfoAugDict{AugmentedDictionary: dict}
	if info.LastKeyBlock != nil && info.LastKeyBlock.SeqNo == previous.ID.SeqNo {
		info.LastKeyBlock.RootHash = previous.ID.RootHash
		info.LastKeyBlock.FileHash = previous.ID.FileHash
	}
	extra.Info, err = info.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	state.McStateExtra, err = tlb.ToCell(extra)
	if err != nil {
		t.Fatal(err)
	}
	root, err = tlb.ToCell(state)
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func TestConfigParameterAlternativeConstructors(t *testing.T) {
	pubkey := cell.BeginCell().MustStoreUInt(0x8e81278a, 32).MustStoreUInt(1, 256).EndCell()
	signature := cell.BeginCell().MustStoreUInt(5, 4).MustStoreSlice(make([]byte, 64), 512).EndCell()
	certificate := cell.BeginCell().MustStoreUInt(4, 4).MustStoreBuilder(pubkey.ToBuilder()).MustStoreUInt(0, 64).
		MustStoreBuilder(signature.ToBuilder()).EndCell()
	chained := cell.BeginCell().MustStoreUInt(15, 4).MustStoreRef(certificate).MustStoreBuilder(signature.ToBuilder()).EndCell()
	tempKey := cell.BeginCell().MustStoreUInt(3, 4).MustStoreUInt(1, 256).MustStoreBuilder(pubkey.ToBuilder()).MustStoreUInt(0, 64).EndCell()
	keyDict := cell.NewDict(256)
	if err := keyDict.SetIntKey(big.NewInt(1), cell.BeginCell().MustStoreUInt(4, 4).MustStoreRef(tempKey).MustStoreBuilder(chained.ToBuilder()).EndCell()); err != nil {
		t.Fatal(err)
	}
	list := cell.NewDict(16)
	if err := list.SetIntKey(big.NewInt(0), cell.BeginCell().MustStoreUInt(0x53, 8).MustStoreBuilder(pubkey.ToBuilder()).MustStoreUInt(1, 64).EndCell()); err != nil {
		t.Fatal(err)
	}
	validators := cell.BeginCell().MustStoreUInt(0x11, 8).MustStoreUInt(0, 64).MustStoreUInt(1, 16).MustStoreUInt(1, 16).MustStoreBuilder(list.AsCell().ToBuilder()).EndCell()
	gas := cell.BeginCell().MustStoreUInt(0xdd, 8).MustStoreSlice(make([]byte, 48), 6*64).EndCell()
	gasPrefix := cell.BeginCell().MustStoreUInt(0xd1, 8).MustStoreUInt(0, 64).MustStoreUInt(0, 64).EndCell()
	simplex1 := cell.BeginCell().MustStoreUInt(0x21, 8).MustStoreUInt(0xff, 8).MustStoreUInt(0, 32).MustStoreUInt(1, 32).MustStoreUInt(0, 64).EndCell()
	simplex2 := cell.BeginCell().MustStoreUInt(0x22, 8).MustStoreUInt(0xff, 8).MustStoreUInt(1, 32).MustStoreBoolBit(false).EndCell()
	cases := []struct {
		name string
		id   uint32
		root *cell.Cell
	}{
		{"plain gas", 20, gas},
		{"flat prefix around plain gas", 20, gasPrefix.ToBuilder().MustStoreBuilder(gas.ToBuilder()).EndCell()},
		{"nested flat gas prefixes", 20, gasPrefix.ToBuilder().MustStoreBuilder(gasPrefix.ToBuilder()).MustStoreBuilder(gas.ToBuilder()).EndCell()},
		{"inline validator list", 34, validators},
		{"chained signed temporary key", 39, cell.BeginCell().MustStoreDict(keyDict).EndCell()},
		{"both simplex constructors", 30, cell.BeginCell().MustStoreUInt(0x10, 8).MustStoreBoolBit(true).MustStoreBoolBit(true).MustStoreRef(simplex1).MustStoreRef(simplex2).EndCell()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateKnownConfigParameter(tc.root, tc.id); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestConfigParameterValidationCellBudget(t *testing.T) {
	for _, count := range []int{512, 513} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			dict := cell.NewDict(256)
			for i := 0; i < count; i++ {
				if err := dict.SetIntKey(big.NewInt(int64(i)), cell.BeginCell().EndCell()); err != nil {
					t.Fatal(err)
				}
			}
			root := cell.BeginCell().MustStoreDict(dict).EndCell()
			err := validateKnownConfigParameter(root, tlb.ConfigParamFundamentalSMCAddresses)
			// N leaves need 2N-1 dictionary nodes and one HashmapE parameter cell.
			if count == 512 && err != nil {
				t.Fatalf("exactly 1024 cells: %v", err)
			}
			if count == 513 && (err == nil || !strings.Contains(err.Error(), "cell budget")) {
				t.Fatalf("1026 cells: %v", err)
			}
		})
	}
}

func TestMasterConfigRejectsOuterForkPayloads(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)
	// The negative subtree is outside every known-positive getter path,
	// including the mandatory-set lookup. Governance permits arbitrary negative
	// values, but the dictionary containing them must still be well formed.
	root := masterBuildConfigWithParam(t, fixture.configRoot, -1, cell.BeginCell().EndCell())
	root = masterBuildConfigWithParam(t, root, -2, cell.BeginCell().EndCell())
	s := root.MustBeginParse()
	label := s.MustLoadSlice(s.BitsLeft())
	left, right := s.MustLoadRef().MustToCell(), s.MustLoadRef().MustToCell()
	if right.RefsNum() != 2 {
		t.Fatal("negative config branch must be a fork")
	}
	if err := validateMasterConfigData(root, fixture.configAddress, fixture.configRoot, false); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		branch *cell.Cell
	}{
		{"trailing bit", right.ToBuilder().MustStoreBoolBit(true).EndCell()},
		{"third reference", right.ToBuilder().MustStoreRef(cell.BeginCell().EndCell()).EndCell()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			malformed := cell.BeginCell().MustStoreSlice(label, root.BitsSize()).MustStoreRef(left).MustStoreRef(tc.branch).EndCell()
			err := validateMasterConfigData(malformed, fixture.configAddress, fixture.configRoot, false)
			if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "fork") {
				t.Fatalf("malformed config fork: %v", err)
			}
		})
	}
}
