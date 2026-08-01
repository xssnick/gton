package p2p

import (
	"strings"
	"testing"
)

func TestBuildCustomOverlaySpecsSkipsNonMember(t *testing.T) {
	zeroHash := make([]byte, 32)
	localID, err := NewPeerID([]byte(strings.Repeat("\x22", 32)))
	if err != nil {
		t.Fatal(err)
	}
	otherID, err := NewPeerID([]byte(strings.Repeat("\x33", 32)))
	if err != nil {
		t.Fatal(err)
	}

	specs, err := buildCustomOverlaySpecs(zeroHash, []CustomOverlayConfig{{
		Name: "private-a",
		Nodes: []CustomOverlayNodeConfig{{
			ADNLID:      otherID,
			BlockSender: true,
		}},
	}}, localID)
	if err != nil {
		t.Fatal(err)
	}
	if len(specs) != 0 {
		t.Fatalf("expected no local custom overlays, got %d", len(specs))
	}
}

func TestBuildCustomOverlaySpecUsesOverlayTransportAndLocalQueryPolicy(t *testing.T) {
	zeroHash := make([]byte, 32)
	localID := testPeerID("custom-local-query-policy")
	otherID := testPeerID("custom-other-query-policy")

	cfg := CustomOverlayConfig{
		Name:        "private-transport",
		UseQUIC:     true,
		SendQueries: true,
		Nodes: []CustomOverlayNodeConfig{
			{
				ADNLID:        localID,
				AcceptQueries: false,
			},
			{
				ADNLID:        otherID,
				AcceptQueries: true,
			},
		},
	}

	spec, localMember, err := buildCustomOverlaySpec(zeroHash, cfg, localID)
	if err != nil {
		t.Fatalf("build custom overlay: %v", err)
	}
	if !localMember {
		t.Fatal("local roster member was not recognized")
	}
	if !spec.UseQUIC || !spec.SendQueries {
		t.Fatalf("overlay-wide transport/query policy was not preserved: %+v", spec)
	}
	if spec.AcceptQueries {
		t.Fatal("another roster member's accept_queries enabled the local node")
	}
	if len(spec.QueryAcceptors) != 1 || spec.QueryAcceptors[0] != otherID {
		t.Fatalf("query acceptors = %v, want remote acceptor", spec.QueryAcceptors)
	}

	cfg.Nodes[0].AcceptQueries = true
	spec, _, err = buildCustomOverlaySpec(zeroHash, cfg, localID)
	if err != nil {
		t.Fatalf("rebuild custom overlay: %v", err)
	}
	if !spec.AcceptQueries {
		t.Fatal("local roster member's accept_queries was not preserved")
	}
	if len(spec.QueryAcceptors) != 2 {
		t.Fatalf("query acceptors = %d, want local and remote", len(spec.QueryAcceptors))
	}
}
