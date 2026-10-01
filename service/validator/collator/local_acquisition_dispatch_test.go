package collator

import (
	"context"
	"strings"
	"testing"

	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/msgpool"
)

// registeredDispatchSession installs a session the pipeline lock will accept,
// so BuildCandidate reaches the chain guards inside AcquireShard/AcquireMaster.
func registeredDispatchSession(t *testing.T, shard groups.ShardID) (*LocalAcquisition, BuildRequest) {
	t.Helper()

	id := [32]byte{0x5a}
	// One roster entry so the leader-index guard passes and the chain guards
	// inside the two acquisitions are actually reached.
	session := Session{ID: id, Shard: shard, Validators: []SessionValidator{{Weight: 1}}}
	activation := SessionActivation{SessionID: id}
	update := SessionUpdate{SessionID: id}
	pool := msgpool.New(msgpool.Config{})
	t.Cleanup(pool.Close)

	acquisition := &LocalAcquisition{
		builder:       testBuilder(),
		messages:      pool,
		externalLimit: 1,
		sessions: map[[32]byte]*localAcquisitionSession{
			id: {session: session, activation: &activation, update: update},
		},
	}

	return acquisition, BuildRequest{
		Session: activatedSession(session, activation),
		Update:  update,
	}
}

// BuildCandidate picks the acquisition by chain. Both acquisitions reject a
// session from the other chain, so routing the wrong way is observable: the
// guard fires before anything else can. The dispatch cannot be pinned on a stub
// acquisition — the session lock runs ahead of the guards on the real object.
func TestBuildCandidateDispatchesByChain(t *testing.T) {
	tests := []struct {
		name     string
		shard    groups.ShardID
		rejected string
	}{
		{
			name:     "shard",
			shard:    groups.ShardID{Workchain: 0, Shard: -1 << 63},
			rejected: "master acquisition received a shardchain session",
		},
		{
			name:     "masterchain",
			shard:    groups.ShardID{Workchain: -1, Shard: -1 << 63},
			rejected: "shard acquisition received a masterchain session",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			acquisition, request := registeredDispatchSession(t, test.shard)

			_, err := acquisition.BuildCandidate(context.Background(), request)
			if err == nil {
				t.Fatal("a session without state produced a candidate")
			}
			if strings.Contains(err.Error(), test.rejected) {
				t.Fatalf("%s session was routed to the other acquisition: %v", test.name, err)
			}
		})
	}
}
