package p2p

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/xssnick/gton/service/archive"
	"github.com/xssnick/tonutils-go/tl"
)

type scriptedPeerQueryTransport struct {
	query func(context.Context, tl.Serializable) (tl.Serializable, error)
	raw   func(context.Context, tl.Serializable) ([]byte, error)
}

func (t scriptedPeerQueryTransport) Query(
	ctx context.Context,
	_ uint64,
	req tl.Serializable,
	result tl.Serializable,
) error {
	if t.query == nil {
		return errors.New("typed query is not configured")
	}

	resp, err := t.query(ctx, req)
	if err != nil {
		return err
	}
	out, ok := result.(*tl.Serializable)
	if !ok {
		return fmt.Errorf("unexpected typed query destination %T", result)
	}
	*out = resp
	return nil
}

func (t scriptedPeerQueryTransport) QueryRaw(
	ctx context.Context,
	_ uint64,
	req tl.Serializable,
) ([]byte, error) {
	if t.raw == nil {
		return nil, errors.New("raw query is not configured")
	}
	return t.raw(ctx, req)
}

func TestCustomHedgedQueryCandidatesStayReadyAcceptorsAndRandomize(t *testing.T) {
	node := &Node{localID: testPeerID("local-custom-query")}
	acceptors := []PeerID{node.localID}
	peers := make(map[PeerID]*overlayPeer)
	eligible := make(map[PeerID]struct{})

	for i := 0; i < 30; i++ {
		peer := testReadyQueryPeer(fmt.Sprintf("custom-query-%02d", i))
		acceptors = append(acceptors, peer.id)
		peers[peer.id] = peer
		eligible[peer.id] = struct{}{}
	}

	cooled := peers[acceptors[1]]
	cooled.ignoreApplicationQueries(time.Now().Add(time.Hour))
	delete(eligible, cooled.id)

	unready := peers[acceptors[2]]
	unready.queryProbeFailed()
	delete(eligible, unready.id)

	unlisted := testReadyQueryPeer("unlisted-custom-query")
	peers[unlisted.id] = unlisted

	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Kind:           overlayKindCustomFixed,
			SendQueries:    true,
			QueryAcceptors: acceptors,
		},
		log:   discardLogger(),
		peers: peers,
	})

	firstPeers := make(map[PeerID]struct{})
	for range 64 {
		candidates := sub.hedgedQueryCandidates(0, 0, 12)
		if len(candidates) != 12 {
			t.Fatalf("candidate count = %d, want 12", len(candidates))
		}

		seen := make(map[PeerID]struct{}, len(candidates))
		for _, candidate := range candidates {
			if _, ok := eligible[candidate.id]; !ok {
				t.Fatalf("ineligible custom candidate selected: %q", candidate.addr)
			}
			if _, ok := seen[candidate.id]; ok {
				t.Fatalf("custom candidate selected twice: %q", candidate.addr)
			}
			seen[candidate.id] = struct{}{}
		}
		firstPeers[candidates[0].id] = struct{}{}
	}

	if len(firstPeers) < 2 {
		t.Fatalf("custom candidate order did not vary across 64 selections: %v", firstPeers)
	}
}

func TestKeyBlockQueryOutcomeAccounting(t *testing.T) {
	t.Run("custom error cools down once", func(t *testing.T) {
		sub, peer := newQueryOutcomeSubscription(overlayKindCustomFixed, "custom-key-error")
		peer.queryTransport = fixedTypedQueryTransport(KeyBlocks{Error: true})

		startedAt := time.Now()
		_, err := sub.queryKeyBlocks(context.Background(), GetNextKeyBlockIDs{})
		if err == nil {
			t.Fatal("key block error response succeeded")
		}
		requireCustomQueryCooldown(t, peer, startedAt)

		stats := peer.statsSnapshot()
		if stats.failedQueries != 0 || stats.unreliability != 2 {
			t.Fatalf("custom error changed public reputation: failed=%d unreliability=%v", stats.failedQueries, stats.unreliability)
		}
	})

	t.Run("custom empty batch is success", func(t *testing.T) {
		sub, peer := newQueryOutcomeSubscription(overlayKindCustomFixed, "custom-key-empty")
		peer.queryTransport = fixedTypedQueryTransport(KeyBlocks{})

		resp, err := sub.queryKeyBlocks(context.Background(), GetNextKeyBlockIDs{})
		if err != nil {
			t.Fatalf("empty key block response: %v", err)
		}
		keyBlocks, ok := resp.(KeyBlocks)
		if !ok || len(keyBlocks.Blocks) != 0 {
			t.Fatalf("empty key block response = %#v", resp)
		}

		stats := peer.statsSnapshot()
		if !stats.queryIgnoreUntil.IsZero() {
			t.Fatalf("successful empty response cooled custom peer until %s", stats.queryIgnoreUntil)
		}
		if stats.failedQueries != 0 || stats.unreliability != 2 {
			t.Fatalf("custom success changed public reputation: failed=%d unreliability=%v", stats.failedQueries, stats.unreliability)
		}
	})

	t.Run("public error is notready", func(t *testing.T) {
		sub, peer := newQueryOutcomeSubscription(overlayKindPublicShard, "public-key-error")
		peer.queryTransport = fixedTypedQueryTransport(KeyBlocks{Error: true})

		_, err := sub.queryKeyBlocks(context.Background(), GetNextKeyBlockIDs{})
		if err == nil {
			t.Fatal("key block error response succeeded")
		}

		stats := peer.statsSnapshot()
		if stats.failedQueries != 0 {
			t.Fatalf("notready response recorded %d query failures", stats.failedQueries)
		}
		if stats.unreliability != 1 {
			t.Fatalf("notready response unreliability = %v, want 1", stats.unreliability)
		}
	})
}

func TestPeerQueryOperationFinishesOnlyOnce(t *testing.T) {
	t.Run("public reputation", func(t *testing.T) {
		sub, peer := newQueryOutcomeSubscription(overlayKindPublicShard, "public-once")
		query := sub.beginPeerQueryOperation(peer)

		query.finish(errors.New("first failure"))
		query.finish(errors.New("second failure"))

		stats := peer.statsSnapshot()
		if stats.failedQueries != 1 {
			t.Fatalf("operation recorded %d failures, want 1", stats.failedQueries)
		}
		if stats.unreliability != 3 {
			t.Fatalf("operation unreliability = %v, want 3", stats.unreliability)
		}
	})

	t.Run("custom cooldown", func(t *testing.T) {
		sub, peer := newQueryOutcomeSubscription(overlayKindCustomFixed, "custom-once")
		query := sub.beginPeerQueryOperation(peer)

		query.finish(errors.New("first failure"))
		firstUntil := peer.statsSnapshot().queryIgnoreUntil
		time.Sleep(time.Millisecond)
		query.finish(errors.New("second failure"))

		if secondUntil := peer.statsSnapshot().queryIgnoreUntil; secondUntil != firstUntil {
			t.Fatalf("operation moved cooldown from %s to %s", firstUntil, secondUntil)
		}
	})
}

func TestCustomProofEmptyAndMalformedOutcomesCoolDown(t *testing.T) {
	tests := []struct {
		name      string
		prepare   tl.Serializable
		proofData []byte
	}{
		{
			name:    "empty",
			prepare: PreparedProofEmpty{},
		},
		{
			name:      "malformed",
			prepare:   PreparedProof{},
			proofData: []byte{1, 2, 3},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sub, peer := newQueryOutcomeSubscription(overlayKindCustomFixed, "custom-proof-"+test.name)
			peer.queryTransport = scriptedPeerQueryTransport{
				query: func(context.Context, tl.Serializable) (tl.Serializable, error) {
					return test.prepare, nil
				},
				raw: func(context.Context, tl.Serializable) ([]byte, error) {
					return test.proofData, nil
				},
			}

			startedAt := time.Now()
			_, err := sub.downloadProofFromPeer(
				context.Background(),
				peer,
				testBlockID(-1, topShard, 10),
				false,
				false,
			)
			if err == nil {
				t.Fatal("invalid proof outcome succeeded")
			}
			requireCustomQueryCooldown(t, peer, startedAt)
		})
	}
}

func TestPublicMalformedProofRecordsOneFailure(t *testing.T) {
	sub, peer := newQueryOutcomeSubscription(overlayKindPublicShard, "public-proof-malformed")
	peer.queryTransport = scriptedPeerQueryTransport{
		query: func(context.Context, tl.Serializable) (tl.Serializable, error) {
			return PreparedProof{}, nil
		},
		raw: func(context.Context, tl.Serializable) ([]byte, error) {
			return []byte{1, 2, 3}, nil
		},
	}

	_, err := sub.downloadProofFromPeer(
		context.Background(),
		peer,
		testBlockID(-1, topShard, 10),
		false,
		false,
	)
	if err == nil {
		t.Fatal("malformed proof succeeded")
	}

	stats := peer.statsSnapshot()
	if stats.failedQueries != 1 {
		t.Fatalf("malformed proof recorded %d failures, want 1", stats.failedQueries)
	}
	if stats.unreliability != 3 {
		t.Fatalf("malformed proof unreliability = %v, want 3", stats.unreliability)
	}
}

func TestCustomArchiveNotFoundOutcomeCoolsDown(t *testing.T) {
	sub, peer := newQueryOutcomeSubscription(overlayKindCustomFixed, "custom-archive-not-found")
	peer.queryTransport = fixedTypedQueryTransport(ArchiveNotFound{})

	pool := testArchivePool(t, sub)
	if !addTestArchiveOnlyPeer(pool, peer) {
		t.Fatal("add archive peer")
	}
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	beginTestArchiveRequest(t, pool, shard, 10)

	session := sub.node.BeginArchiveSession()
	defer session.Close()

	startedAt := time.Now()
	_, err := sub.fetchArchiveCandidate(
		context.Background(),
		session,
		pool,
		peer,
		10,
		shard,
		false,
	)
	if !errors.Is(err, archive.ErrNotAvailable) {
		t.Fatalf("archive query error = %v, want not available", err)
	}
	requireCustomQueryCooldown(t, peer, startedAt)
}

func TestCustomPersistentStateNotFoundOutcomeCoolsDown(t *testing.T) {
	sub, peer := newQueryOutcomeSubscription(overlayKindCustomFixed, "custom-state-not-found")
	peer.queryTransport = fixedTypedQueryTransport(NotFoundState{})

	downloader := persistentStateSnapshotDownloader{
		node:   sub.node,
		sub:    sub,
		block:  testBlockID(0, topShard, 10),
		master: testBlockID(-1, topShard, 20),
	}

	startedAt := time.Now()
	_, err := downloader.preparePersistentStateCandidate(context.Background(), peer, 0)
	if !errors.Is(err, ErrStateNotAvailable) {
		t.Fatalf("persistent state prepare error = %v, want not available", err)
	}
	requireCustomQueryCooldown(t, peer, startedAt)
}

func TestPublicPersistentStateMalformedSizeRecordsOneFailure(t *testing.T) {
	sub, peer := newQueryOutcomeSubscription(overlayKindPublicShard, "public-state-malformed")
	peer.queryTransport = scriptedPeerQueryTransport{
		query: func(_ context.Context, req tl.Serializable) (tl.Serializable, error) {
			switch req.(type) {
			case PreparePersistentState:
				return PreparedState{}, nil
			case GetPersistentStateSizeV2:
				return PersistentStateSize{Size: -1}, nil
			default:
				return nil, fmt.Errorf("unexpected persistent state query %T", req)
			}
		},
	}

	downloader := persistentStateSnapshotDownloader{
		node:   sub.node,
		sub:    sub,
		block:  testBlockID(0, topShard, 10),
		master: testBlockID(-1, topShard, 20),
	}

	if _, err := downloader.preparePersistentStateCandidate(context.Background(), peer, 0); err == nil {
		t.Fatal("malformed persistent state size succeeded")
	}

	stats := peer.statsSnapshot()
	if stats.failedQueries != 1 {
		t.Fatalf("malformed state size recorded %d failures, want 1", stats.failedQueries)
	}
	if stats.unreliability != 3 {
		t.Fatalf("malformed state size unreliability = %v, want 3", stats.unreliability)
	}
}

func fixedTypedQueryTransport(resp tl.Serializable) peerQueryTransport {
	return scriptedPeerQueryTransport{
		query: func(context.Context, tl.Serializable) (tl.Serializable, error) {
			return resp, nil
		},
	}
}

func newQueryOutcomeSubscription(kind overlayKind, label string) (*overlaySubscription, *overlayPeer) {
	node := &Node{
		localID: testPeerID(label + "-local"),
		log:     discardLogger(),
		peerUse: map[PeerID]peerUse{},
	}
	peer := testReadyQueryPeer(label)
	peer.fixedMember = true
	peer.lastReceiveAt = time.Now()
	peer.unreliability = 2

	spec := overlaySpec{Kind: kind}
	if kind == overlayKindCustomFixed {
		spec.SendQueries = true
		spec.QueryAcceptors = []PeerID{peer.id}
	}
	sub := testOverlaySubscription(&overlaySubscription{
		node:       node,
		spec:       spec,
		log:        discardLogger(),
		peers:      map[PeerID]*overlayPeer{peer.id: peer},
		neighbours: []PeerID{peer.id},
	})
	return sub, peer
}

func requireCustomQueryCooldown(t *testing.T, peer *overlayPeer, startedAt time.Time) {
	t.Helper()

	stats := peer.statsSnapshot()
	if !stats.queryResponds {
		t.Fatal("application outcome cleared capabilities readiness")
	}
	minUntil := startedAt.Add(customQueryFailureCooldown - 100*time.Millisecond)
	if stats.queryIgnoreUntil.Before(minUntil) {
		t.Fatalf("custom query cooldown ends at %s, want at least %s", stats.queryIgnoreUntil, minUntil)
	}
}
