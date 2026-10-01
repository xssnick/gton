package service

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"strings"
	"testing"

	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestHardforkProofRawEmptySignaturesAppliesStateUpdate(t *testing.T) {
	downloaded, current := newHardforkProofFixture(t)
	svc := testServiceWithHardfork(t, downloaded.ID)
	prepared, err := svc.prepareDownloadedBlockForApply(*downloaded)
	if err != nil {
		t.Fatalf("prepare synthesized hardfork BOCs: %v", err)
	}
	if prepared.consensus == nil || prepared.consensus.signaturePrepareErr == nil {
		t.Fatal("expected the real empty signature cell to fail ordinary signature preparation")
	}
	if prepared.consensus.signaturesChecked || prepared.consensus.hardforkChecked {
		t.Fatal("unverified proof was marked checked during preparation")
	}

	checked, err := svc.checkedConsensusForPreparedBlock(current, prepared)
	if err != nil {
		t.Fatalf("check configured hardfork proof: %v", err)
	}
	if !prepared.consensus.hardforkChecked || prepared.consensus.signaturesChecked {
		t.Fatal("expected only the exact configured hardfork exception")
	}
	if err = checked.validateFor(current, downloaded.ID); err != nil {
		t.Fatalf("bind checked proof to predecessor: %v", err)
	}

	applied, err := applyBlockWithPreviousStates([]*storage.BlockState{current}, prepared, nil)
	if err != nil {
		t.Fatalf("apply synthesized hardfork Merkle update: %v", err)
	}
	if !applied.Next.Block.Equals(&downloaded.ID) {
		t.Fatal("applied state belongs to another block")
	}
	wantRoot := downloaded.StateUpdate.MustPeekRef(1).HashKeyAt(0)
	if !bytes.Equal(applied.Next.StateRootHash, wantRoot[:]) {
		t.Fatalf("applied state root = %x, want %x", applied.Next.StateRootHash, wantRoot)
	}
}

type hardforkProofRejectionCase struct {
	name       string
	mutate     func(*testing.T, *p2p.DownloadedBlock, *storage.BlockState) *p2p.DownloadedBlock
	configured func(ton.BlockIDExt) ton.BlockIDExt
	want       string
}

func TestHardforkProofRawRejectsInvalidTrustBoundary(t *testing.T) {
	cases := []hardforkProofRejectionCase{
		{
			name: "wrong predecessor",
			mutate: func(_ *testing.T, block *p2p.DownloadedBlock, current *storage.BlockState) *p2p.DownloadedBlock {
				current.Block.RootHash = bytes32(0x71)
				return block
			},
			want: errMasterchainPrevMismatch.Error(),
		},
		{
			name: "wrong previous state",
			mutate: func(_ *testing.T, block *p2p.DownloadedBlock, current *storage.BlockState) *p2p.DownloadedBlock {
				current.Cell = cell.BeginCell().MustStoreUInt(0x72, 8).EndCell()
				return block
			},
			want: "invalid previous state hash",
		},
		{
			name: "missing key block flag",
			mutate: func(t *testing.T, block *p2p.DownloadedBlock, _ *storage.BlockState) *p2p.DownloadedBlock {
				header := hardforkProofHeader(t, block.Block)
				header.KeyBlock = false
				return synthesizeHardforkProofBlock(t, block.Block, header, block.StateUpdate)
			},
			want: "not a key block",
		},
		{
			name: "missing vertical increment",
			mutate: func(t *testing.T, block *p2p.DownloadedBlock, _ *storage.BlockState) *p2p.DownloadedBlock {
				header := hardforkProofHeader(t, block.Block)
				header.VertSeqnoIncr = false
				header.PrevVertRef = nil
				return synthesizeHardforkProofBlock(t, block.Block, header, block.StateUpdate)
			},
			want: "no vert_seqno_incr",
		},
		{
			name: "unconfigured root hash",
			configured: func(id ton.BlockIDExt) ton.BlockIDExt {
				id.RootHash = bytes32(0x73)
				return id
			},
			want: "validator set hash mismatch",
		},
		{
			name: "unconfigured file hash",
			configured: func(id ton.BlockIDExt) ton.BlockIDExt {
				id.FileHash = bytes32(0x74)
				return id
			},
			want: "validator set hash mismatch",
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			downloaded, current := newHardforkProofFixture(t)
			if test.mutate != nil {
				downloaded = test.mutate(t, downloaded, current)
			}
			configured := downloaded.ID
			if test.configured != nil {
				configured = test.configured(configured)
			}
			svc := testServiceWithHardfork(t, configured)
			prepared, err := svc.prepareDownloadedBlockForApply(*downloaded)
			if err != nil {
				t.Fatalf("prepare raw proof: %v", err)
			}
			if _, err = svc.checkedConsensusForPreparedBlock(current, prepared); err == nil ||
				!strings.Contains(err.Error(), test.want) {
				t.Fatalf("consensus error = %v, want %q", err, test.want)
			}
			if prepared.consensus.hardforkChecked || prepared.consensus.signaturesChecked {
				t.Fatal("rejected proof was marked checked")
			}
		})
	}
}

func TestHardforkProofRawExceptionDoesNotAuthorizeOrdinarySuccessor(t *testing.T) {
	hardfork, current := newHardforkProofFixture(t)
	svc := testServiceWithHardfork(t, hardfork.ID)
	prepared, err := svc.prepareDownloadedBlockForApply(*hardfork)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = svc.checkedConsensusForPreparedBlock(current, prepared); err != nil {
		t.Fatal(err)
	}
	applied, err := applyBlockWithPreviousStates([]*storage.BlockState{current}, prepared, nil)
	if err != nil {
		t.Fatal(err)
	}

	header := hardforkProofHeader(t, hardfork.Block)
	header.SeqNo++
	header.KeyBlock = false
	header.VertSeqnoIncr = false
	header.PrevVertRef = nil
	header.PrevKeyBlockSeqno = hardfork.ID.SeqNo
	header.PrevRef = tlb.BlkPrevInfo{Prev1: tlb.ExtBlkRef{
		EndLt:    header.EndLt,
		SeqNo:    hardfork.ID.SeqNo,
		RootHash: hardfork.ID.RootHash,
		FileHash: hardfork.ID.FileHash,
	}}
	// This successor only exercises consensus admission, so its no-op update
	// keeps the state binding without serializing the fixture's virtual tree.
	stateRoot := mustPrunedBranch(t, hardfork.StateUpdate.MustPeekRef(1))
	update := mustMerkleUpdateCell(t, stateRoot, stateRoot)
	successor := synthesizeHardforkProofBlock(t, hardfork.Block, header, update)
	prepared, err = svc.prepareDownloadedBlockForApply(*successor)
	if err != nil {
		t.Fatalf("prepare ordinary successor: %v", err)
	}
	_, err = svc.checkedConsensusForPreparedBlock(applied.Next, prepared)
	if err == nil || !errors.Is(err, prepared.consensus.signaturePrepareErr) {
		t.Fatalf("ordinary successor error = %v, want original signature error", err)
	}
	if prepared.consensus.hardforkChecked {
		t.Fatal("hardfork exception leaked to the next block")
	}
}

// newHardforkProofFixture synthesizes a synchronization fixture from an ordinary
// mainnet block. It is not a C++-generated recovery block or a collation fixture:
// only the header is changed, preserving the original Merkle state transition.
// The proof uses C++'s empty ordinary #11 signature cell with zero context.
func newHardforkProofFixture(t testing.TB) (*p2p.DownloadedBlock, *storage.BlockState) {
	t.Helper()

	original := mustLoadFixtureDownloadedBlock(t)
	header := hardforkProofHeader(t, original.Block)
	header.KeyBlock = true
	header.VertSeqnoIncr = true
	header.PrevVertRef = &tlb.BlkPrevInfo{Prev1: header.PrevRef.Prev1}
	downloaded := synthesizeHardforkProofBlock(t, original.Block, header, original.StateUpdate)
	previous := downloaded.Meta.PrevRefs[0]
	oldState := downloaded.StateUpdate.MustPeekRef(0)
	current, err := storage.ParseStateProof(&previous, oldState, nil, nil, nil)
	if err != nil {
		t.Fatalf("parse fixture previous state: %v", err)
	}
	return downloaded, current
}

func hardforkProofHeader(t testing.TB, root *cell.Cell) tlb.BlockHeader {
	t.Helper()

	var header tlb.BlockHeader
	if err := tlb.LoadFromCell(&header, root.MustPeekRef(0).MustBeginParse()); err != nil {
		t.Fatalf("parse fixture block header: %v", err)
	}
	return header
}

func synthesizeHardforkProofBlock(
	t testing.TB,
	source *cell.Cell,
	header tlb.BlockHeader,
	update *cell.Cell,
) *p2p.DownloadedBlock {
	t.Helper()

	info, err := tlb.ToCell(header)
	if err != nil {
		t.Fatalf("serialize synthesized header: %v", err)
	}
	root, err := source.RebuildWithRefs([]*cell.Cell{info, source.MustPeekRef(1), update, source.MustPeekRef(3)})
	if err != nil {
		t.Fatalf("rebuild synthesized block: %v", err)
	}
	blockBOC, err := root.ToBOCWithOptionsErr(cell.BOCSerializeOptions{WithCRC32C: false})
	if err != nil {
		t.Fatalf("serialize synthesized block: %v", err)
	}
	rootHash := root.HashKey()
	fileHash := sha256.Sum256(blockBOC)
	id := ton.BlockIDExt{
		Workchain: -1,
		Shard:     topShard,
		SeqNo:     header.SeqNo,
		RootHash:  rootHash[:],
		FileHash:  fileHash[:],
	}
	proofBody, err := cell.CreateMerkleProof(root)
	if err != nil {
		t.Fatalf("build full synthesized proof body: %v", err)
	}
	signatures := cell.BeginCell().MustStoreUInt(0x11, 8).
		MustStoreUInt(0, 32).MustStoreUInt(0, 32).
		MustStoreUInt(0, 32).MustStoreUInt(0, 64).MustStoreDict(nil).EndCell()
	_, proofBOC, err := blockproof.ProofFromRoot(id, proofBody, signatures)
	if err != nil {
		t.Fatalf("serialize synthesized proof: %v", err)
	}
	root, err = cell.FromBOC(blockBOC)
	if err != nil {
		t.Fatalf("decode synthesized block BOC: %v", err)
	}
	proofRoot, err := cell.FromBOC(proofBOC)
	if err != nil {
		t.Fatalf("decode synthesized proof BOC: %v", err)
	}
	if err = blockproof.CheckProofShape(id, proofRoot, false); err != nil {
		t.Fatalf("check C++-shaped full proof: %v", err)
	}
	parsed, err := storage.ParseVerifiedBlockCell(id, root)
	if err != nil {
		t.Fatalf("parse synthesized block: %v", err)
	}
	meta, err := storage.BuildBlockMetaFromParsedBlock(id, parsed)
	if err != nil {
		t.Fatalf("parse synthesized metadata: %v", err)
	}
	return &p2p.DownloadedBlock{
		ID:               id,
		Kind:             "synthesized hardfork proof fixture",
		Block:            root,
		BlockBOC:         blockBOC,
		Proof:            proofRoot,
		ProofBOC:         proofBOC,
		Meta:             meta,
		StateUpdate:      parsed.StateUpdate,
		VerifiedRootHash: true,
	}
}
