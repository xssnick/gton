package p2p

import (
	"time"

	"github.com/xssnick/gton/service/p2p/internal/peerroute"
)

var peerRouteRetryPolicy = peerroute.RetryPolicy{
	// Route retry is transport liveness, not Plumtree eager-peer retention.
	// Keep its established ten-second backoff independent of the protocol's
	// exact thirty-second eager inactivity window.
	MinDelay: 10 * time.Second,
	MaxDelay: 30 * time.Second,
}

// PlumtreePolicy is an immutable snapshot of the validator keys authorized to
// originate public overlay Plumtree broadcasts.
type PlumtreePolicy struct {
	authorizedKeys map[PeerID]uint32
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
	keys := make(map[PeerID]uint32, len(authorizedKeys))
	for _, id := range authorizedKeys {
		keys[id] = uint32(plumtreeMaxPayloadSize)
	}
	return PlumtreePolicy{
		authorizedKeys: keys,
	}
}

func (n *Node) SetPlumtreePolicy(policy PlumtreePolicy) {
	n.plumtreePolicy.Store(&policy)

	if len(policy.authorizedKeys) > 0 {
		n.plumtreePolicyLogOnce.Do(func() {
			n.log.Info().
				Int("authorized_keys", len(policy.authorizedKeys)).
				Msg("loaded Plumtree broadcast policy")
		})
	}
}

func (p PlumtreePolicy) authorizes(id PeerID, size uint32) bool {
	maxSize, ok := p.authorizedKeys[id]
	return ok && size <= maxSize
}
