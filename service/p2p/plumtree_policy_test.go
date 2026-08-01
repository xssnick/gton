package p2p

import "testing"

func TestPlumtreePolicyCopiesAuthorizedKeys(t *testing.T) {
	authorized := []PeerID{testPeerID("plumtree-authorized")}
	policy := NewPlumtreePolicy(authorized)
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
