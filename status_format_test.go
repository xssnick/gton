package gton

import (
	"strings"
	"testing"
	"time"

	service2 "github.com/xssnick/gton/service"
	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage/pebblestore"

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
		LocalMasterchain:      &localMaster,
		LocalBasechain:        &localBase,
		LocalStateLoaded:      true,
		LocalMasterchainTx:    7,
		LocalBasechainTx:      11,
		LocalMasterchainHasTx: true,
		LocalBasechainHasTx:   true,
		RecentTPS: service2.StatusTPSSnapshot{
			WindowMasters:   10,
			Transactions:    1234,
			DurationSeconds: 45,
			TPS:             27.422,
			Complete:        true,
		},
	}, false, time.Unix(1000, 0))

	for _, want := range []string{
		"Status\n\nNode\n",
		"Chain Lag\n",
		"masterchain  local=40 latest=42 lag_seconds=unknown block_time=unknown tx=7",
		"basechain    local=77 latest=77 lag_seconds=unknown block_time=unknown tx=11",
		"tps          window_masters=10 tx=1234 duration=45s tps=27.42",
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

func TestFormatStatusListsSplitBasechainShards(t *testing.T) {
	localMaster := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     100,
	}
	localLeft := ton.BlockIDExt{
		Workchain: 0,
		Shard:     int64(0x4000000000000000),
		SeqNo:     200,
	}
	localRight := ton.BlockIDExt{
		Workchain: 0,
		Shard:     int64(-0x4000000000000000),
		SeqNo:     201,
	}
	latestLeft := localLeft
	latestLeft.SeqNo = 202

	out := formatStatusWithNow(service2.StatusSnapshot{
		StatusSnapshot: p2p.StatusSnapshot{
			LatestBasechain:       &latestLeft,
			LatestBasechainShards: []ton.BlockIDExt{latestLeft},
		},
		LocalMasterchain: &localMaster,
		LocalStateLoaded: true,
		LocalBasechainShards: []service2.ShardStatusSnapshot{
			{Block: localLeft, Utime: 990, Transactions: 21, HasTransactions: true},
			{Block: localRight, Utime: 980, Transactions: 34, HasTransactions: true},
		},
	}, false, time.Unix(1000, 0))

	for _, want := range []string{
		"basechain/4000000000000000 local=200 latest=202 lag_seconds=10s block_time=1970-01-01T00:16:30Z tx=21",
		"basechain/c000000000000000 local=201 latest=<none> lag_seconds=20s block_time=1970-01-01T00:16:20Z tx=34",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("formatted status missing %q:\n%s", want, out)
		}
	}
}

func TestFormatDBStatusIncludesCellDBShardMetrics(t *testing.T) {
	out := formatDBStatus(pebblestore.DBStatus{
		Meta: &pebblestore.MetaDBStatus{
			Cache: pebblestore.CellDBCacheStatus{
				BlockCacheSize:   16 << 20,
				BlockCacheHits:   7,
				BlockCacheMisses: 3,
				FileCacheSize:    0,
			},
			DB: pebblestore.CellDBShardStatus{
				Shard:                    -1,
				DiskSize:                 128 << 20,
				LiveSize:                 96 << 20,
				ReadAmp:                  3,
				L0Files:                  2,
				L0Sublevels:              1,
				CompactionDebt:           32 << 20,
				CompactionsInProgress:    1,
				CompactionInProgressSize: 4 << 20,
				MemTableSize:             2 << 20,
				TableIters:               2,
				Flushes:                  3,
				Ingests:                  0,
			},
		},
		CellGenerations: []pebblestore.CellDBGenerationStatus{
			{
				ID:   1,
				Role: "active",
				Cache: pebblestore.CellDBCacheStatus{
					BlockCacheSize:   64 << 20,
					BlockCacheHits:   90,
					BlockCacheMisses: 10,
					FileCacheSize:    32 << 20,
				},
				Shards: []pebblestore.CellDBShardStatus{
					{
						Shard:                    0,
						DiskSize:                 10 << 30,
						LiveSize:                 9 << 30,
						ReadAmp:                  7,
						L0Files:                  4,
						L0Sublevels:              3,
						CompactionDebt:           2 << 30,
						CompactionsInProgress:    1,
						CompactionInProgressSize: 256 << 20,
						MemTableSize:             16 << 20,
						TableIters:               3,
						Flushes:                  4,
						Ingests:                  5,
						ReadCellsPerSecond:       120,
						WrittenCellsPerSecond:    45,
					},
				},
				Total: pebblestore.CellDBShardStatus{
					Shard:                 -1,
					DiskSize:              10 << 30,
					LiveSize:              9 << 30,
					ReadAmp:               7,
					L0Files:               4,
					L0Sublevels:           3,
					ReadCellsPerSecond:    120,
					WrittenCellsPerSecond: 45,
				},
			},
		},
	})

	for _, want := range []string{
		"DB Status\n\nMeta DB\n",
		"cache block=16.0MiB file=0B block_hit=70.0%",
		"meta",
		"1/4.0MiB",
		"Cell DB\n",
		"generation 1 role=active",
		"cache block=64.0MiB file=32.0MiB block_hit=90.0%",
		"shard",
		"rd/wr/s",
		"0",
		"120/45",
		"4/3",
		"1/256MiB",
		"total",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("formatted db status missing %q:\n%s", want, out)
		}
	}
}

func TestFormatDBStatusShowsPendingCellGenerationSwitchWait(t *testing.T) {
	out := formatDBStatus(pebblestore.DBStatus{
		CellGenerations: []pebblestore.CellDBGenerationStatus{
			{
				ID:   2,
				Role: "pending",
				Total: pebblestore.CellDBShardStatus{
					Shard:                    -1,
					ReadAmp:                  service2.CellGenerationSwitchMaxReadAmp + 1,
					L0Files:                  12,
					L0Sublevels:              8,
					CompactionDebt:           2 << 30,
					CompactionsInProgress:    1,
					CompactionInProgressSize: 256 << 20,
				},
			},
		},
	})

	for _, want := range []string{
		"generation 2 role=pending",
		"switch_wait pending read_amp=13 > 12, waiting for compaction",
		"debt=2.0GiB",
		"comp=1/256MiB",
		"l0=12/8",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("formatted db status missing %q:\n%s", want, out)
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
