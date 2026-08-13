package network

import (
	"crypto/ed25519"
	"errors"
	"reflect"
	"testing"

	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/validator/collator"
)

// specFieldMutation names one sessionSpec field together with a mutation of it.
// mergeable records whether the mergeSessionSpecs precondition observes the
// field; every field except the derived peers slice must be observed by equal,
// which doubles as the handle-freshness check in session.handleMatches.
type specFieldMutation struct {
	field     string
	mutate    func(*sessionSpec)
	mergeable bool
	observed  bool
}

func sessionSpecFieldMutations() []specFieldMutation {
	other := p2p.PeerID{0x50}

	return []specFieldMutation{
		{field: "id", mergeable: true, observed: true, mutate: func(s *sessionSpec) { s.id = [32]byte{9} }},
		{field: "kind", observed: true, mutate: func(s *sessionSpec) { s.kind = sessionKindObserver }},
		{field: "role", observed: true, mutate: func(s *sessionSpec) { s.role = collator.OverlayRoleObserver }},
		{field: "workchain", mergeable: true, observed: true, mutate: func(s *sessionSpec) { s.workchain = -1 }},
		{field: "shard", mergeable: true, observed: true, mutate: func(s *sessionSpec) { s.shard = 1 }},
		{
			field: "fullOverlayID", mergeable: true, observed: true,
			mutate: func(s *sessionSpec) { s.fullOverlayID = []byte{9, 9, 9} },
		},
		{
			field: "members", observed: true,
			mutate: func(s *sessionSpec) { s.members = append(append([]p2p.PeerID(nil), s.members...), other) },
		},
		// peers is derived: remoteMembers(members, localADNLID) at every
		// construction site, so equal observes members instead.
		{field: "peers", mutate: func(s *sessionSpec) { s.peers = append(append([]p2p.PeerID(nil), s.peers...), other) }},
		{
			field: "validatorByADNL", mergeable: true, observed: true,
			mutate: func(s *sessionSpec) { s.validatorByADNL = map[p2p.PeerID]int{other: 0} },
		},
		{field: "validatorCount", mergeable: true, observed: true, mutate: func(s *sessionSpec) { s.validatorCount = 2 }},
		{field: "catchainSeqno", mergeable: true, observed: true, mutate: func(s *sessionSpec) { s.catchainSeqno = 77 }},
		{
			field: "validatorSetHash", mergeable: true, observed: true,
			mutate: func(s *sessionSpec) { s.validatorSetHash = 78 },
		},
		{field: "maxReplyBytes", mergeable: true, observed: true, mutate: func(s *sessionSpec) { s.maxReplyBytes = 1 << 21 }},
		{
			field: "authorized", mergeable: true, observed: true,
			mutate: func(s *sessionSpec) { s.authorized = map[p2p.PeerID]uint32{other: 1} },
		},
		{
			field: "candidateADNL", mergeable: true, observed: true,
			mutate: func(s *sessionSpec) { s.candidateADNL = map[p2p.PeerID]p2p.PeerID{other: other} },
		},
		{
			field: "validatorSource", mergeable: true, observed: true,
			mutate: func(s *sessionSpec) { s.validatorSource = map[p2p.PeerID]struct{}{other: {}} },
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

	return sessionSpec{
		id:               [32]byte{1},
		kind:             sessionKindValidator,
		role:             collator.OverlayRoleCollator,
		workchain:        0,
		shard:            -1 << 63,
		fullOverlayID:    []byte{1, 2, 3},
		members:          []p2p.PeerID{local, member},
		peers:            []p2p.PeerID{member},
		validatorByADNL:  map[p2p.PeerID]int{member: 0},
		validatorCount:   1,
		catchainSeqno:    9,
		validatorSetHash: 10,
		maxReplyBytes:    1 << 20,
		authorized:       map[p2p.PeerID]uint32{source: 1 << 20},
		candidateADNL:    map[p2p.PeerID]p2p.PeerID{source: member},
		validatorSource:  map[p2p.PeerID]struct{}{source: {}},
		signer:           testOverlaySigner{key: ed25519.NewKeyFromSeed(bytesOf(1, ed25519.SeedSize))},
	}
}

// TestSessionSpecMutationTableCoversEveryField is the guard the two predicates
// need: a newly added sessionSpec field is silently absent from equal and from
// the merge precondition by default, and neither omission is a compile error.
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
			t.Fatalf("sessionSpec field %s has no mutation: add it here and to equal/mergeableFieldsEqual", name)
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

func TestSessionSpecMergePreconditionObservesSharedFields(t *testing.T) {
	base := fullSessionSpec()
	if !base.mergeableFieldsEqual(fullSessionSpec()) {
		t.Fatal("mergeableFieldsEqual rejected two identical specs")
	}

	local := [32]byte{0x10}
	for _, mutation := range sessionSpecFieldMutations() {
		t.Run(mutation.field, func(t *testing.T) {
			mutated := base
			mutation.mutate(&mutated)
			if base.mergeableFieldsEqual(mutated) == mutation.mergeable {
				t.Fatalf(
					"mergeableFieldsEqual on mutated %s = %v, want %v",
					mutation.field, base.mergeableFieldsEqual(mutated), !mutation.mergeable,
				)
			}

			_, err := mergeSessionSpecs(base, mutated, local)
			if mutation.mergeable != errors.Is(err, ErrSessionConflict) {
				t.Fatalf("mergeSessionSpecs on mutated %s = %v, conflict expected: %v", mutation.field, err, mutation.mergeable)
			}
		})
	}
}

// The merged spec is the input to overlayConfig and to handleMatches, so its
// union order and its deliberately cleared role fields are load-bearing.
func TestMergeSessionSpecsUnionsMembersAndClearsRoleFields(t *testing.T) {
	local := [32]byte{0x10}
	extra := p2p.PeerID{0x50}
	left := fullSessionSpec()
	right := fullSessionSpec()
	right.kind = sessionKindObserver
	right.role = collator.OverlayRoleObserver
	right.signer = nil
	right.members = []p2p.PeerID{p2p.PeerID(local), extra}

	merged, err := mergeSessionSpecs(left, right, local)
	if err != nil {
		t.Fatal(err)
	}
	if merged.kind != 0 || merged.role != 0 || merged.signer != nil {
		t.Fatal("merge kept a role-scoped field that receiveCandidate resolves from the contribution instead")
	}
	wantMembers := []p2p.PeerID{p2p.PeerID(local), p2p.PeerID{0x20}, extra}
	if !reflect.DeepEqual(merged.members, wantMembers) {
		t.Fatalf("merged members = %v, want left-then-right union %v", merged.members, wantMembers)
	}
	if !reflect.DeepEqual(merged.peers, []p2p.PeerID{{0x20}, extra}) {
		t.Fatalf("merged peers = %v, want the union without the local ID", merged.peers)
	}
}
