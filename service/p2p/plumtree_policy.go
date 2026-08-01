package p2p

// PlumtreePolicy is an immutable snapshot of the validator keys authorized to
// originate public overlay Plumtree broadcasts.
type PlumtreePolicy struct {
	authorizedKeys map[PeerID]struct{}
}

type plumtreePolicySource interface {
	current() *PlumtreePolicy
}

type nodePlumtreePolicySource struct {
	node *Node
}

type fixedPlumtreePolicySource struct {
	policy PlumtreePolicy
}

var _ plumtreePolicySource = nodePlumtreePolicySource{}
var _ plumtreePolicySource = (*fixedPlumtreePolicySource)(nil)

func (s nodePlumtreePolicySource) current() *PlumtreePolicy {
	return s.node.plumtreePolicy.Load()
}

func (s *fixedPlumtreePolicySource) current() *PlumtreePolicy {
	return &s.policy
}

func NewPlumtreePolicy(authorizedKeys []PeerID) PlumtreePolicy {
	keys := make(map[PeerID]struct{}, len(authorizedKeys))
	for _, id := range authorizedKeys {
		keys[id] = struct{}{}
	}
	return PlumtreePolicy{
		authorizedKeys: keys,
	}
}

func (p PlumtreePolicy) authorizes(id PeerID, size uint32) bool {
	_, ok := p.authorizedKeys[id]
	return ok && size <= uint32(plumtreeMaxPayloadSize)
}
