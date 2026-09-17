package collator

import (
	"bytes"
	"errors"
	"testing"

	"github.com/xssnick/tonutils-go/address"

	sharddomain "github.com/xssnick/gton/service/shard"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/msgpool"
)

func TestMasterShardTopPreparedAndFallbackCutsMatch(t *testing.T) {
	previous := emptyCandidateRequest(t).Previous
	previous.State = stateWithMasterchainBoundEntries(t, previous.State, 7)
	source := blockShardIdent(previous.ID)
	destination := targetShardIdent(groups.ShardID{
		Workchain: address.MasterchainID,
		Shard:     sharddomain.Root,
	})
	ref, err := localSourceRef(previous.ID)
	if err != nil {
		t.Fatal(err)
	}

	type result struct {
		cut      *msgpool.Cut
		proofBOC []byte
	}
	run := func(t *testing.T, prepared bool) result {
		t.Helper()

		pool := msgpool.New(msgpool.Config{})
		defer pool.Close()
		if err := pool.Internals().ReconcileDestinations([]msgpool.ShardIdent{destination}); err != nil {
			t.Fatal(err)
		}
		if prepared {
			routed, total, err := pool.Internals().SeedsFromStateRoot(source, ref, previous.State)
			if err != nil {
				t.Fatal(err)
			}
			seeded := false
			for index := range routed {
				if routed[index].Destination != destination {
					continue
				}
				if err = pool.Internals().Seed(destination, source, ref, routed[index].Messages, total); err != nil {
					t.Fatal(err)
				}
				seeded = true
				break
			}
			if !seeded {
				t.Fatal("prepared seed omitted the masterchain destination")
			}
		}

		view, err := localViewFromPrevious(previous, true, true)
		if err != nil {
			t.Fatal(err)
		}
		branch, err := pool.Internals().OpenBranch(destination)
		if err != nil {
			t.Fatal(err)
		}
		defer branch.Close()
		acquisition := &LocalAcquisition{messages: pool}
		cut, err := acquisition.cutCommittedViews(
			branch,
			destination,
			map[msgpool.ShardIdent]*localNeighborView{source: view},
			nil,
			nil,
			true,
			&prewarmHints{},
		)
		if err != nil {
			t.Fatal(err)
		}
		if len(cut.Messages) == 0 {
			t.Fatal("masterchain cut is empty")
		}
		if !branch.SourcePinnable(source, ref) {
			t.Fatal("branch did not retain the exact source ref")
		}
		_, globalErr := pool.Internals().SourceTop(destination, source)
		if prepared && globalErr != nil {
			t.Fatalf("prepared source disappeared: %v", globalErr)
		}
		if !prepared && !errors.Is(globalErr, msgpool.ErrNotFound) {
			t.Fatalf("fallback mutated the finalized pool: %v", globalErr)
		}

		last := cut.Messages[len(cut.Messages)-1]
		if _, err = traceInternalCut(FullCollatedQueueScan{
			Target: destination,
			LT:     last.EnqueuedLT,
			Hash:   processedInfinityHash,
		}, cut.Messages, map[msgpool.ShardIdent]*localNeighborView{source: view}); err != nil {
			t.Fatal(err)
		}
		proof, err := view.proof.CreateProof()
		if err != nil {
			t.Fatal(err)
		}

		return result{cut: cut, proofBOC: proof.ToBOC()}
	}

	prepared := run(t, true)
	fallback := run(t, false)
	if prepared.cut.More != fallback.cut.More || len(prepared.cut.Messages) != len(fallback.cut.Messages) {
		t.Fatalf("cut shape differs: prepared=%d/%v fallback=%d/%v",
			len(prepared.cut.Messages), prepared.cut.More,
			len(fallback.cut.Messages), fallback.cut.More,
		)
	}
	for index := range prepared.cut.Messages {
		left := prepared.cut.Messages[index]
		right := fallback.cut.Messages[index]
		if left.Key != right.Key || left.EnqueuedLT != right.EnqueuedLT || left.QueueLT != right.QueueLT ||
			left.EnvHash != right.EnvHash || left.Source != right.Source || left.SourceSeqno != right.SourceSeqno ||
			left.EnvelopeCell.HashKey() != right.EnvelopeCell.HashKey() || left.Root.HashKey() != right.Root.HashKey() {
			t.Fatalf("message %d differs between prepared and fallback cuts", index)
		}
		if index > 0 && msgpool.CompareLtHash(prepared.cut.Messages[index-1], left) >= 0 {
			t.Fatalf("prepared cut is not canonical at message %d", index)
		}
	}
	if !bytes.Equal(prepared.proofBOC, fallback.proofBOC) {
		t.Fatal("prepared and fallback cuts produced different queue proofs")
	}
}
