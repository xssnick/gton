package p2p

import "testing"

func TestPlumtreePolicyUsesWorkchainProtocolVersion(t *testing.T) {
	policy := NewPlumtreePolicy(2, 1, nil)

	if !policy.enabled(-1) {
		t.Fatal("masterchain Plumtree is disabled at protocol version 2")
	}
	if policy.enabled(0) {
		t.Fatal("shard Plumtree is enabled below protocol version 2")
	}
}

func TestPlumtreePolicyCopiesAuthorizedKeys(t *testing.T) {
	authorized := []PeerID{testPeerID("plumtree-authorized")}
	policy := NewPlumtreePolicy(2, 2, authorized)
	id := authorized[0]
	authorized[0] = testPeerID("plumtree-mutated")

	if !policy.authorizes(id, uint32(plumtreeMaxPayloadSize)) {
		t.Fatal("authorized validator was rejected")
	}
	if policy.authorizes(authorized[0], 1) {
		t.Fatal("caller mutation changed the policy")
	}
	if policy.authorizes(id, uint32(plumtreeMaxPayloadSize)+1) {
		t.Fatal("authorized validator exceeded the C++ payload limit")
	}
}
