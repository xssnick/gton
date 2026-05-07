package main

import (
	"strings"
	"testing"
	"time"

	service2 "flexserver/service"
	"flexserver/service/p2p"

	"github.com/xssnick/tonutils-go/ton"
)

func TestFormatStatusReadableSections(t *testing.T) {
	latestMaster := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     42,
	}
	localMaster := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     40,
	}
	latestBase := ton.BlockIDExt{
		Workchain: 0,
		Shard:     int64(-1 << 63),
		SeqNo:     77,
	}
	localBase := ton.BlockIDExt{
		Workchain: 0,
		Shard:     int64(-1 << 63),
		SeqNo:     77,
	}

	out := formatStatusWithNow(service2.StatusSnapshot{
		StatusSnapshot: p2p.StatusSnapshot{
			ListenAddr:        "0.0.0.0:30303",
			LatestMasterchain: &latestMaster,
			LatestBasechain:   &latestBase,
			Overlays: []p2p.OverlayStatusSnapshot{
				{
					Name:             "masterchain",
					KnownPeers:       3,
					AliveKnownPeers:  2,
					ActiveNeighbours: 1,
					AliveNeighbours:  1,
					Neighbours: []p2p.NeighbourStatusSnapshot{
						{
							Addr:          "127.0.0.1:30303",
							Alive:         true,
							LastSuccessAt: time.Now().Add(-time.Minute),
							FailedQueries: 2,
							Unreliability: 1.5,
						},
					},
				},
			},
		},
		LocalMasterchain: &localMaster,
		LocalBasechain:   &localBase,
		LocalStateLoaded: true,
	}, false, time.Unix(1000, 0))

	for _, want := range []string{
		"Status\n\nNode\n",
		"Chain Lag\n",
		"masterchain  local=40 latest=42 lag_seconds=unknown",
		"basechain    local=77 latest=77 lag_seconds=unknown",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("formatted status missing %q:\n%s", want, out)
		}
	}

	if strings.Contains(out, "Overlays\n") {
		t.Fatalf("plain status should not include overlay blocks by default: \n%s", out)
	}

	out = formatStatusWithNow(service2.StatusSnapshot{
		StatusSnapshot: p2p.StatusSnapshot{
			ListenAddr:        "0.0.0.0:30303",
			LatestMasterchain: &latestMaster,
			LatestBasechain:   &latestBase,
			Overlays: []p2p.OverlayStatusSnapshot{
				{
					Name:             "masterchain",
					KnownPeers:       3,
					AliveKnownPeers:  2,
					ActiveNeighbours: 1,
					AliveNeighbours:  1,
					Neighbours: []p2p.NeighbourStatusSnapshot{
						{
							Addr:          "127.0.0.1:30303",
							Alive:         true,
							LastSuccessAt: time.Now().Add(-time.Minute),
							FailedQueries: 2,
							Unreliability: 1.5,
						},
					},
				},
			},
		},
		LocalMasterchain:      &localMaster,
		LocalBasechain:        &localBase,
		LocalStateLoaded:      true,
		LocalMasterchainUtime: 990,
		LocalBasechainUtime:   980,
	}, true, time.Unix(1000, 0))

	for _, want := range []string{
		"Overlays\n",
		"  masterchain",
		"    alive last ok        fail    score  addr",
		"lag_seconds=10s",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("formatted status missing %q:\n%s", want, out)
		}
	}
}

func TestFormatStatusReportsMissingLagData(t *testing.T) {
	out := formatStatus(service2.StatusSnapshot{
		StatusSnapshot: p2p.StatusSnapshot{},
	}, false)

	for _, want := range []string{
		"masterchain  no local current state latest=<none>",
		"basechain    no local current state latest=<none>",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("formatted status missing %q:\n%s", want, out)
		}
	}

	out = formatStatus(service2.StatusSnapshot{
		StatusSnapshot: p2p.StatusSnapshot{},
	}, true)

	if !strings.Contains(out, "Overlays\n") {
		t.Fatalf("full status should include peers blocks: \n%s", out)
	}
}
