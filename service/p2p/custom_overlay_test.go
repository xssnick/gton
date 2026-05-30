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
