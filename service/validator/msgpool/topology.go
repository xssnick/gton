package msgpool

import (
	"fmt"
	"maps"
	"slices"

	sharddomain "github.com/xssnick/gton/service/shard"
	"github.com/xssnick/gton/service/validator/groups"
)

// Topology is the immutable internal-message projection of one validator
// group snapshot. Future groups are destinations because they can be prepared
// before activation, but only active registered shards are queue sources.
type Topology struct {
	destinations []ShardIdent
	sources      map[ShardIdent]struct{}
}

// NewTopology derives the exact destination and source sets used by both the
// validator feed and a standalone collator.
func NewTopology(snapshot *groups.Snapshot) Topology {
	destinationSet := make(map[ShardIdent]struct{}, len(snapshot.Active)+len(snapshot.Future))
	for i := range snapshot.Active {
		destinationSet[topologyShard(snapshot.Active[i].Shard)] = struct{}{}
	}
	for i := range snapshot.Future {
		destinationSet[topologyShard(snapshot.Future[i].Shard)] = struct{}{}
	}
	destinations := make([]ShardIdent, 0, len(destinationSet))
	for destination := range destinationSet {
		destinations = append(destinations, destination)
	}
	slices.SortFunc(destinations, CompareShardIdent)

	sources := make(map[ShardIdent]struct{}, len(snapshot.Active)+1)
	sources[ShardIdent{Workchain: -1, Shard: ShardAll}] = struct{}{}
	for i := range snapshot.Active {
		for j := range snapshot.Active[i].Registered {
			sources[topologyShard(snapshot.Active[i].Registered[j].Shard)] = struct{}{}
		}
	}

	return Topology{destinations: destinations, sources: sources}
}

// Equal reports whether two snapshots publish the same message topology.
func (t Topology) Equal(other Topology) bool {
	return slices.Equal(t.destinations, other.destinations) && maps.Equal(t.sources, other.sources)
}

// ContainsSource reports whether an applied shard owns a live outbound queue.
func (t Topology) ContainsSource(source ShardIdent) bool {
	_, exists := t.sources[source]

	return exists
}

// Reconcile atomically publishes destinations, then prunes sources which are
// no longer live or no longer neighbors of their destination.
func (t Topology) Reconcile(internals *Internals) error {
	if err := internals.ReconcileDestinations(t.destinations); err != nil {
		return err
	}

	var pruneErr error
	internals.VisitDestinations(func(destination ShardIdent) bool {
		pruneErr = internals.PruneSources(destination, func(source ShardIdent) bool {
			return t.ContainsSource(source) && sharddomain.IsNeighbor(
				destination.Workchain,
				int64(destination.Shard),
				source.Workchain,
				int64(source.Shard),
			)
		})
		if pruneErr != nil {
			pruneErr = fmt.Errorf(
				"prune destination %d:%016x sources: %w",
				destination.Workchain,
				destination.Shard,
				pruneErr,
			)
		}

		return pruneErr == nil
	})

	return pruneErr
}

// ShardIdentFrom converts the signed shard prefix carried by TL block ids and
// validator-group descriptions into the unsigned form this package uses for
// prefix arithmetic. The conversion is bit-preserving; it exists once so that
// every caller spells it the same way.
func ShardIdentFrom(workchain int32, shard int64) ShardIdent {
	return ShardIdent{Workchain: workchain, Shard: uint64(shard)}
}

func topologyShard(shard groups.ShardID) ShardIdent {
	return ShardIdentFrom(shard.Workchain, shard.Shard)
}
