package p2p

const plumtreeProtocolVersion = 2

// PlumtreePolicy is an immutable snapshot of the config that controls public
// overlay Plumtree broadcasts.
type PlumtreePolicy struct {
	masterchainProtocolVersion uint8
	shardProtocolVersion       uint8
	authorizedKeys             map[PeerID]struct{}
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

func NewPlumtreePolicy(
	masterchainProtocolVersion,
	shardProtocolVersion uint8,
	authorizedKeys []PeerID,
) PlumtreePolicy {
	keys := make(map[PeerID]struct{}, len(authorizedKeys))
	for _, id := range authorizedKeys {
		keys[id] = struct{}{}
	}
	return PlumtreePolicy{
		masterchainProtocolVersion: masterchainProtocolVersion,
		shardProtocolVersion:       shardProtocolVersion,
		authorizedKeys:             keys,
	}
}

func (p PlumtreePolicy) enabled(workchain int32) bool {
	version := p.shardProtocolVersion
	if workchain == -1 {
		version = p.masterchainProtocolVersion
	}
	return version >= plumtreeProtocolVersion
}

func (p PlumtreePolicy) authorizes(id PeerID, size uint32) bool {
	_, ok := p.authorizedKeys[id]
	return ok && size <= uint32(plumtreeMaxPayloadSize)
}
