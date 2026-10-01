package network

import (
	"crypto/ed25519"
	"reflect"
	"testing"

	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/validator/collator"
)

// specFieldMutation names one sessionSpec field together with a mutation of it.
// physical records whether the field affects an open overlay. Every field
// except the derived peers slice must be observed by equal.
type specFieldMutation struct {
	field    string
	mutate   func(*sessionSpec)
	physical bool
	observed bool
}

func sessionSpecFieldMutations() []specFieldMutation {
	other := p2p.PeerID{0x50}

	return []specFieldMutation{
		{field: "id", physical: true, observed: true, mutate: func(s *sessionSpec) { s.id = [32]byte{9} }},
		{field: "kind", observed: true, mutate: func(s *sessionSpec) { s.kind = sessionKindObserver }},
		{field: "role", observed: true, mutate: func(s *sessionSpec) { s.role = collator.OverlayRoleObserver }},
		{field: "protocolVersion", physical: true, observed: true, mutate: func(s *sessionSpec) { s.protocolVersion = 1 }},
		{field: "useQUIC", physical: true, observed: true, mutate: func(s *sessionSpec) { s.useQUIC = false }},
		{field: "slotsPerLeaderWindow", physical: true, observed: true, mutate: func(s *sessionSpec) { s.slotsPerLeaderWindow = 2 }},
		{field: "openConsensus", physical: true, observed: true, mutate: func(s *sessionSpec) { s.openConsensus = false }},
		{field: "workchain", physical: true, observed: true, mutate: func(s *sessionSpec) { s.workchain = -1 }},
		{field: "shard", physical: true, observed: true, mutate: func(s *sessionSpec) { s.shard = 1 }},
		{
			field: "fullOverlayID", physical: true, observed: true,
			mutate: func(s *sessionSpec) { s.fullOverlayID = []byte{9, 9, 9} },
		},
		{
			field: "members", physical: true, observed: true,
			mutate: func(s *sessionSpec) { s.members = append(append([]p2p.PeerID(nil), s.members...), other) },
		},
		// peers is derived: remoteMembers(members, localADNLID) at every
		// construction site, so equal observes members instead.
		{field: "peers", mutate: func(s *sessionSpec) { s.peers = append(append([]p2p.PeerID(nil), s.peers...), other) }},
		{
			field: "blockSyncFullID", physical: true, observed: true,
			mutate: func(s *sessionSpec) { s.blockSyncFullID = []byte{8, 8, 8} },
		},
		{
			field: "blockSyncMembers", physical: true, observed: true,
			mutate: func(s *sessionSpec) { s.blockSyncMembers = []p2p.PeerID{other} },
		},
		{
			field: "twoStepMembers", physical: true, observed: true,
			mutate: func(s *sessionSpec) { s.twoStepMembers = []p2p.PeerID{other} },
		},
		{
			field: "validatorByADNL", physical: true, observed: true,
			mutate: func(s *sessionSpec) { s.validatorByADNL = map[p2p.PeerID]int{other: 0} },
		},
		{
			field: "validatorKeys", physical: true, observed: true,
			mutate: func(s *sessionSpec) { s.validatorKeys = [][32]byte{{0x51}} },
		},
		{field: "validatorCount", physical: true, observed: true, mutate: func(s *sessionSpec) { s.validatorCount = 2 }},
		{field: "catchainSeqno", physical: true, observed: true, mutate: func(s *sessionSpec) { s.catchainSeqno = 77 }},
		{
			field: "validatorSetHash", physical: true, observed: true,
			mutate: func(s *sessionSpec) { s.validatorSetHash = 78 },
		},
		{field: "maxReplyBytes", physical: true, observed: true, mutate: func(s *sessionSpec) { s.maxReplyBytes = 1 << 21 }},
		{
			field: "consensusAuthorized", physical: true, observed: true,
			mutate: func(s *sessionSpec) { s.consensusAuthorized = map[p2p.PeerID]uint32{other: 1} },
		},
		{
			field: "authorized", physical: true, observed: true,
			mutate: func(s *sessionSpec) { s.authorized = map[p2p.PeerID]uint32{other: 1} },
		},
		{
			field: "candidateADNL", physical: true, observed: true,
			mutate: func(s *sessionSpec) { s.candidateADNL = map[p2p.PeerID]p2p.PeerID{other: other} },
		},
		{
			field: "validatorSource", physical: true, observed: true,
			mutate: func(s *sessionSpec) { s.validatorSource = map[p2p.PeerID]int{other: 0} },
		},
		{
			field: "signer", observed: true,
			mutate: func(s *sessionSpec) {
				s.signer = testOverlaySigner{key: ed25519.NewKeyFromSeed(bytesOf(2, ed25519.SeedSize))}
			},
		},
	}
}

func fullSessionSpec() sessionSpec {
	local := p2p.PeerID{0x10}
	member := p2p.PeerID{0x20}
	source := p2p.PeerID{0x30}

	authorized := map[p2p.PeerID]uint32{source: 1 << 20}
	return sessionSpec{
		id:                   [32]byte{1},
		kind:                 sessionKindValidator,
		role:                 0,
		protocolVersion:      3,
		useQUIC:              true,
		slotsPerLeaderWindow: 1,
		openConsensus:        true,
		workchain:            0,
		shard:                -1 << 63,
		fullOverlayID:        []byte{1, 2, 3},
		members:              []p2p.PeerID{local, member},
		peers:                []p2p.PeerID{member},
		twoStepMembers:       []p2p.PeerID{local, member},
		validatorByADNL:      map[p2p.PeerID]int{member: 0},
		validatorKeys:        [][32]byte{{1}},
		validatorCount:       1,
		catchainSeqno:        9,
		validatorSetHash:     10,
		maxReplyBytes:        1 << 20,
		consensusAuthorized:  authorized,
		authorized:           authorized,
		candidateADNL:        map[p2p.PeerID]p2p.PeerID{source: member},
		validatorSource:      map[p2p.PeerID]int{source: 0},
		signer:               testOverlaySigner{key: ed25519.NewKeyFromSeed(bytesOf(1, ed25519.SeedSize))},
	}
}

// TestSessionSpecMutationTableCoversEveryField guards the two equality
// predicates: adding a field cannot silently omit it from freshness checks.
func TestSessionSpecMutationTableCoversEveryField(t *testing.T) {
	covered := make(map[string]struct{}, len(sessionSpecFieldMutations()))
	for _, mutation := range sessionSpecFieldMutations() {
		if _, duplicate := covered[mutation.field]; duplicate {
			t.Fatalf("field %s is listed twice in the mutation table", mutation.field)
		}
		covered[mutation.field] = struct{}{}
	}

	specType := reflect.TypeFor[sessionSpec]()
	for i := range specType.NumField() {
		name := specType.Field(i).Name
		if _, ok := covered[name]; !ok {
			t.Fatalf("sessionSpec field %s has no mutation: add it here and to the equality predicates", name)
		}
	}
	if len(covered) != specType.NumField() {
		t.Fatalf("mutation table has %d fields, sessionSpec has %d", len(covered), specType.NumField())
	}
}

func TestSessionSpecEqualObservesEveryFieldButDerivedPeers(t *testing.T) {
	base := fullSessionSpec()
	if !base.equal(fullSessionSpec()) {
		t.Fatal("equal rejected two identical specs")
	}

	for _, mutation := range sessionSpecFieldMutations() {
		t.Run(mutation.field, func(t *testing.T) {
			mutated := base
			mutation.mutate(&mutated)
			if base.equal(mutated) == mutation.observed {
				t.Fatalf("equal on mutated %s = %v, want %v", mutation.field, base.equal(mutated), !mutation.observed)
			}
			if mutated.equal(base) == mutation.observed {
				t.Fatalf("equal is not symmetric on %s", mutation.field)
			}
		})
	}
}

func TestSessionSpecOverlayEqualityObservesPhysicalFields(t *testing.T) {
	base := fullSessionSpec()
	if !base.overlayFieldsEqual(fullSessionSpec()) {
		t.Fatal("overlayFieldsEqual rejected two identical specs")
	}

	for _, mutation := range sessionSpecFieldMutations() {
		t.Run(mutation.field, func(t *testing.T) {
			mutated := base
			mutation.mutate(&mutated)
			if base.overlayFieldsEqual(mutated) == mutation.physical {
				t.Fatalf(
					"overlayFieldsEqual on mutated %s = %v, want %v",
					mutation.field, base.overlayFieldsEqual(mutated), !mutation.physical,
				)
			}
		})
	}
}
