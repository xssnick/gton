package main

import (
	"strings"
	"testing"
	"time"

	service2 "github.com/xssnick/gton/service"
	"github.com/xssnick/gton/service/p2p"

	"github.com/xssnick/tonutils-go/ton"
)

func TestFormatStatusUsesLocalBlockTimeForLagSeconds(t *testing.T) {
	local := ton.BlockIDExt{Workchain: -1, Shard: int64(-1 << 63), SeqNo: 10}
	latest := ton.BlockIDExt{Workchain: -1, Shard: int64(-1 << 63), SeqNo: 20}
	snapshot := service2.StatusSnapshot{
		StatusSnapshot: p2p.StatusSnapshot{
			LatestMasterchain: &latest,
		},
		LocalMasterchain:      &local,
		LocalStateLoaded:      true,
		LocalMasterchainUtime: 990,
	}

	out := formatStatusWithNow(snapshot, false, time.Unix(1000, 0))
	if !strings.Contains(out, "masterchain  local=10 latest=20 lag_seconds=10s") {
		t.Fatalf("status does not use local block time for lag seconds:\n%s", out)
	}
	if strings.Contains(out, "blocks") {
		t.Fatalf("status should not include block lag:\n%s", out)
	}
}
