package validator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
)

type acceptanceBenchmarkScenario struct {
	name         string
	submissions  int
	descriptions int
}

type acceptanceBenchmarkNode struct {
	acceptanceTestNode
}

func (*acceptanceBenchmarkNode) SubmitBlockLocally(p2p.DownloadedBlock) {}

type acceptanceBenchmarkFixture struct {
	accepter   *BlockAccepter
	acceptance BlockAcceptance
	blockRoot  *cell.Cell
}

func newAcceptanceBenchmarkFixture(b *testing.B) acceptanceBenchmarkFixture {
	b.Helper()
	fixture := loadReceiveFixture(b, fixtureFullCollated)
	root := fixture.roots[0]
	loader, err := root.BeginParse()
	if err != nil {
		b.Fatal(err)
	}
	var block tlb.Block
	if err = tlb.LoadFromCell(&block, loader); err != nil {
		b.Fatal(err)
	}
	config, key := runtimeTestConfig(0x71, &runtimeTestJournal{})
	workchain, shard := tlb.ConvertShardIdentToShard(block.BlockInfo.Shard)
	config.Shard = groups.ShardID{Workchain: workchain, Shard: int64(shard)}
	config.ValidatorSetHash, err = groups.ValidatorSetHash(groups.ValidatorSetHashInput{
		CatchainSeqno: config.CatchainSeqno,
		Validators:    config.Validators,
	})
	if err != nil {
		b.Fatal(err)
	}
	block.BlockInfo.GenCatchainSeqno = config.CatchainSeqno
	block.BlockInfo.GenValidatorListHashShort = config.ValidatorSetHash
	header, err := block.BlockInfo.ToCell()
	if err != nil {
		b.Fatal(err)
	}
	loader, err = root.BeginParse()
	if err != nil {
		b.Fatal(err)
	}
	refs := []*cell.Cell{header, nil, nil, nil}
	for i := 1; i < len(refs); i++ {
		refs[i], err = loader.PeekRefCellAt(i)
		if err != nil {
			b.Fatal(err)
		}
	}
	root, err = root.RebuildWithRefs(refs)
	if err != nil {
		b.Fatal(err)
	}
	boc, err := receiveBlockBOC(root, 0)
	if err != nil {
		b.Fatal(err)
	}
	fileHash := sha256.Sum256(boc)
	artifact := &CandidateArtifact{
		Candidate: simplex.Candidate{
			Parent: simplex.Genesis(),
			Block: ton.BlockIDExt{
				Workchain: workchain,
				Shard:     int64(shard),
				SeqNo:     block.BlockInfo.SeqNo,
				RootHash:  root.Hash(),
				FileHash:  fileHash[:],
			},
			CollatedFileHash: sha256.Sum256([]byte("collated")),
		},
		BlockBOC: boc,
	}
	artifact.Candidate.ID = artifact.Candidate.ComputeID(0)
	acceptance := BlockAcceptance{
		Candidate:          artifact,
		CertifiedCandidate: artifact,
		Certificate:        runtimeTestSeal(b, config, key, simplex.FinalizeVote(artifact.Candidate.ID)),
	}
	accepter, err := NewBlockAccepter(BlockAccepterOptions{Config: config, Node: &acceptanceBenchmarkNode{}})
	if err != nil {
		b.Fatal(err)
	}
	return acceptanceBenchmarkFixture{accepter: accepter, acceptance: acceptance, blockRoot: root}
}

// BenchmarkBlockAcceptancePreparation uses the existing full-collated mainnet
// block body, changing only its header's committee identity to the test signer.
// It compares the former per-phase preparation with one prepared workflow. The
// registry stays unavailable, so description attempts exercise precisely the
// preparation overhead paid while waiting; no sleeps or disk writes are timed.
func BenchmarkBlockAcceptancePreparation(b *testing.B) {
	fixture := newAcceptanceBenchmarkFixture(b)
	accepter, acceptance := fixture.accepter, fixture.acceptance
	resolveView := func() (BlockAcceptanceView, error) {
		return BlockAcceptanceView{}, ErrBlockNotReady
	}
	prepare := func(b *testing.B, ctx context.Context) *preparedBlockAcceptance {
		prepared, err := accepter.Prepare(ctx, acceptance, resolveView)
		if err != nil {
			b.Fatal(err)
		}
		return prepared
	}

	for _, scenario := range []acceptanceBenchmarkScenario{
		{name: "handoff_and_description", submissions: 1, descriptions: 1},
		{name: "submission_and_description_retries", submissions: 2, descriptions: 3},
	} {
		for _, reuse := range []bool{false, true} {
			mode := "rebuild_each_phase"
			if reuse {
				mode = "reuse_prepared"
			}
			b.Run(scenario.name+"/"+mode, func(b *testing.B) {
				b.ReportAllocs()
				ctx := b.Context()
				for b.Loop() {
					prepared := prepare(b, ctx)
					for attempt := range scenario.submissions {
						if !reuse && attempt > 0 {
							prepared = prepare(b, ctx)
						}
						if err := prepared.Submit(ctx); err != nil {
							b.Fatal(err)
						}
					}
					for range scenario.descriptions {
						if !reuse {
							prepared = prepare(b, ctx)
						}
						if err := prepared.Describe(ctx); !errors.Is(err, ErrBlockNotReady) {
							b.Fatalf("describe error = %v, want ErrBlockNotReady", err)
						}
					}
				}
			})
		}
	}
}

// BenchmarkBlockAcceptanceRoot compares the complete Prepare path with and
// without the exact parsed tip the state resolver already retains. It preserves
// certificate, file/root hash and strict TL-B checks in every arm. The optional
// full state is irrelevant until Submit, which is outside this measurement.
func BenchmarkBlockAcceptanceRoot(b *testing.B) {
	fixture := newAcceptanceBenchmarkFixture(b)
	for _, mode := range []string{"decode_wire", "resident_root", "resident_root_copied_wire"} {
		b.Run(mode, func(b *testing.B) {
			acceptance := fixture.acceptance
			if mode != "decode_wire" {
				acceptance.state = &ChainState{tips: []ChainTip{{
					ID:       acceptance.Candidate.Candidate.Block,
					BlockBOC: acceptance.Candidate.BlockBOC,
					Block:    fixture.blockRoot,
				}}}
			}
			if mode == "resident_root_copied_wire" {
				artifact := *acceptance.Candidate
				artifact.BlockBOC = bytes.Clone(artifact.BlockBOC)
				acceptance.Candidate = &artifact
				acceptance.CertifiedCandidate = &artifact
			}
			b.ReportAllocs()
			for b.Loop() {
				if _, err := fixture.accepter.Prepare(b.Context(), acceptance, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
