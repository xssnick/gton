package validator

import (
	"crypto/sha256"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
)

type selectedBaseProgressFixture struct {
	candidate simplex.CandidateID
	chain     *ChainState
}

// newSelectedBaseProgressFixture builds a real ordinary block and its exact
// ShardStateUnsplit successor. NewSelectedBaseState intentionally validates
// both TL-B roots, so marker cells would weaken the progress-boundary test.
func newSelectedBaseProgressFixture(t *testing.T) selectedBaseProgressFixture {
	t.Helper()

	accounts, err := tlb.NewShardAccountsAugDict()
	if err != nil {
		t.Fatal(err)
	}
	outQueue, err := tlb.NewOutMsgQueueAugDict()
	if err != nil {
		t.Fatal(err)
	}
	queueInfo, err := (tlb.OutMsgQueueInfo{
		OutQueue: outQueue,
		ProcInfo: cell.NewDict(96),
	}).ToCell()
	if err != nil {
		t.Fatal(err)
	}
	stats, err := (tlb.ShardStateStats{}).ToCell()
	if err != nil {
		t.Fatal(err)
	}

	shard := groups.ShardID{Workchain: 0, Shard: -1 << 63}
	state := tlb.ShardStateUnsplit{
		GlobalID:        -239,
		ShardIdent:      tlb.ShardIdent{WorkchainID: shard.Workchain},
		Seqno:           2,
		GenUTime:        1_900_000_000,
		GenLT:           2_000,
		OutMsgQueueInfo: queueInfo,
		Stats:           stats,
	}
	state.Accounts.ShardAccounts = accounts
	stateRoot, err := tlb.ToCell(&state)
	if err != nil {
		t.Fatal(err)
	}

	blockRoot := acceptanceTestBlockRoot(t, shard, 7, 0x10203040)
	refs := make([]*cell.Cell, blockRoot.RefsNum())
	for i := range refs {
		refs[i], err = blockRoot.PeekRef(i)
		if err != nil {
			t.Fatal(err)
		}
	}
	oldState := cell.BeginCell().MustStoreUInt(1, 32).EndCell()
	refs[2] = acceptanceTestMerkleUpdate(t, oldState, stateRoot)
	blockRoot, err = blockRoot.RebuildWithRefs(refs)
	if err != nil {
		t.Fatal(err)
	}
	blockBOC := blockRoot.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: true})
	fileHash := sha256.Sum256(blockBOC)
	block := ton.BlockIDExt{
		Workchain: shard.Workchain,
		Shard:     shard.Shard,
		SeqNo:     state.Seqno,
		RootHash:  blockRoot.Hash(),
		FileHash:  fileHash[:],
	}
	candidate := simplex.Candidate{Parent: simplex.Genesis(), Leader: 0, Block: block}
	candidate.ID = candidate.ComputeID(3)

	return selectedBaseProgressFixture{
		candidate: candidate.ID,
		chain: &ChainState{
			shard: shard,
			tips: []ChainTip{{
				ID:       block,
				BlockBOC: blockBOC,
				Block:    blockRoot,
				State:    stateRoot,
			}},
			root: stateRoot,
		},
	}
}

func TestCollatorConsensusProgressCarriesSelectedBaseState(t *testing.T) {
	fixture := newSelectedBaseProgressFixture(t)
	progress := sessionConsensusProgress{
		Window: simplex.Window{
			Base:      simplex.Parent(fixture.candidate),
			StartSlot: 4,
			EndSlot:   8,
		},
		BaseState: fixture.chain,
	}
	converted, err := collatorConsensusProgress([32]byte{0x75}, progress)
	if err != nil {
		t.Fatal(err)
	}
	if converted.SessionID != ([32]byte{0x75}) || converted.Window != progress.Window || converted.Base == nil {
		t.Fatalf("converted selected-base progress = %+v", converted)
	}
}

func TestCollatorConsensusProgressLeavesGenesisBaseEmpty(t *testing.T) {
	progress := sessionConsensusProgress{
		Window: simplex.Window{
			Base:      simplex.Genesis(),
			StartSlot: 4,
			EndSlot:   8,
		},
	}
	converted, err := collatorConsensusProgress([32]byte{0x76}, progress)
	if err != nil {
		t.Fatal(err)
	}
	if converted.Base != nil {
		t.Fatal("consensus genesis carried a selected base")
	}

	progress.BaseState = newSelectedBaseProgressFixture(t).chain
	if _, err = collatorConsensusProgress([32]byte{0x76}, progress); err == nil {
		t.Fatal("consensus genesis accepted an ordinary selected base state")
	}
}
