package validator

import (
	"context"
	"errors"
	"testing"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
)

func TestLocalBlockAcceptanceRetriesThePreparedSubmission(t *testing.T) {
	fixture := newAcceptanceTestFixture(t, groups.ShardID{Workchain: 0, Shard: -1 << 63})
	node := &acceptanceTestNode{}
	publisher := &acceptanceTestPublisher{}
	accepter, err := NewBlockAccepter(BlockAccepterOptions{
		Config: fixture.config, Node: node, Publisher: publisher,
	})
	if err != nil {
		t.Fatal(err)
	}

	observations := 0
	backend := &LocalSessionBackend{
		config:   fixture.config,
		accepter: accepter,
		finalized: func(context.Context, ton.BlockIDExt) error {
			observations++
			if len(node.blocks) != observations || len(publisher.blocks) != 1 {
				t.Fatal("finalization was observed before local submission and the single publication")
			}
			if observations == 1 {
				return ErrBlockNotReady
			}
			return nil
		},
	}
	prepared, err := backend.PrepareBlockAcceptance(t.Context(), fixture.acceptance(simplex.VoteFinalize, false))
	if err != nil {
		t.Fatal(err)
	}
	if len(node.blocks) != 0 || len(publisher.blocks) != 0 {
		t.Fatal("preparation submitted or published the block")
	}
	if err = prepared.Submit(t.Context()); !errors.Is(err, ErrBlockNotReady) {
		t.Fatalf("first submit error = %v, want ErrBlockNotReady", err)
	}
	if err = prepared.Submit(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(node.blocks) != 2 || len(publisher.blocks) != 1 || observations != 2 {
		t.Fatalf("submissions/publications/observations = %d/%d/%d, want 2/1/2",
			len(node.blocks), len(publisher.blocks), observations)
	}
	if node.blocks[0].Block != node.blocks[1].Block || node.blocks[0].Proof != node.blocks[1].Proof {
		t.Fatal("the retry decoded the block or rebuilt its proof")
	}
	if &node.blocks[0].ProofBOC[0] != &node.blocks[1].ProofBOC[0] {
		t.Fatal("the retry reserialized its prepared proof")
	}

	// There is no group snapshot yet. That can stall only the description,
	// without resubmitting or republishing the accepted block on each retry.
	for range 2 {
		if err = prepared.Describe(t.Context()); !errors.Is(err, ErrBlockNotReady) {
			t.Fatalf("description without a registry error = %v, want ErrBlockNotReady", err)
		}
	}
	if len(node.blocks) != 2 || len(publisher.blocks) != 1 || observations != 2 {
		t.Fatal("describing the shard top repeated a successful submission step")
	}
}

func TestPreparedDescriptionReleasesSubmissionPayloadAndState(t *testing.T) {
	fixture := newAcceptanceTestFixture(t, groups.ShardID{Workchain: 0, Shard: -1 << 63})
	node := &acceptanceTestNode{}
	accepter, err := newAcceptanceTestAccepter(fixture, node)
	if err != nil {
		t.Fatal(err)
	}
	acceptance := fixture.acceptance(simplex.VoteFinalize, false)
	root := cell.BeginCell().MustStoreUInt(0x1234, 16).EndCell()
	acceptance.state = &ChainState{
		shard: fixture.config.Shard,
		tips:  []ChainTip{{ID: acceptance.Candidate.Candidate.Block, State: root}},
		root:  root,
	}
	prepared, err := accepter.Prepare(t.Context(), acceptance, func() (BlockAcceptanceView, error) {
		return BlockAcceptanceView{}, ErrBlockNotReady
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = prepared.Submit(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(node.published) != 1 || node.published[0].State.Cell != root {
		t.Fatal("submission did not publish the original computed state")
	}
	proof := prepared.link.proofRoot
	if err = prepared.Describe(t.Context()); !errors.Is(err, ErrBlockNotReady) {
		t.Fatalf("description error = %v, want ErrBlockNotReady", err)
	}
	if prepared.state != nil || prepared.block.Block != nil || prepared.block.BlockBOC != nil ||
		prepared.block.StateUpdate != nil || prepared.signatures != nil {
		t.Fatal("the pending description retains submission payloads or full state")
	}
	if prepared.link.proofRoot != proof || prepared.signaturesCell == nil {
		t.Fatal("the pending description discarded its reusable proof or final signatures")
	}
}

func TestPreparedLocalBlockAcceptanceStopsAfterBackendClose(t *testing.T) {
	fixture := newAcceptanceTestFixture(t, groups.ShardID{Workchain: 0, Shard: -1 << 63})
	node := &acceptanceTestNode{}
	accepter, err := newAcceptanceTestAccepter(fixture, node)
	if err != nil {
		t.Fatal(err)
	}
	backend := &LocalSessionBackend{config: fixture.config, accepter: accepter}
	prepared, err := backend.PrepareBlockAcceptance(t.Context(), fixture.acceptance(simplex.VoteFinalize, false))
	if err != nil {
		t.Fatal(err)
	}
	backend.controlMu.Lock()
	backend.closed = true
	backend.controlMu.Unlock()
	if err = prepared.Submit(t.Context()); !errors.Is(err, ErrLocalSessionBackendClosed) {
		t.Fatalf("submit after close error = %v, want ErrLocalSessionBackendClosed", err)
	}
	if err = prepared.Describe(t.Context()); !errors.Is(err, ErrLocalSessionBackendClosed) {
		t.Fatalf("describe after close error = %v, want ErrLocalSessionBackendClosed", err)
	}
	if len(node.blocks) != 0 {
		t.Fatal("closed backend submitted a prepared block")
	}
}

func TestPreparedSubmissionPublishesAfterCanceledAttempt(t *testing.T) {
	fixture := newAcceptanceTestFixture(t, groups.ShardID{Workchain: 0, Shard: -1 << 63})
	node := &acceptanceTestNode{}
	publisher := &acceptanceTestPublisher{}
	accepter, err := NewBlockAccepter(BlockAccepterOptions{
		Config: fixture.config, Node: node, Publisher: publisher,
	})
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := accepter.Prepare(t.Context(), fixture.acceptance(simplex.VoteFinalize, false), nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err = prepared.Submit(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled submit error = %v, want context.Canceled", err)
	}
	if len(node.blocks) != 0 || len(publisher.blocks) != 0 {
		t.Fatal("the canceled attempt submitted or published the block")
	}
	if err = prepared.Submit(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(node.blocks) != 1 || len(publisher.blocks) != 1 {
		t.Fatal("the canceled attempt suppressed the first submission or publication")
	}
}
