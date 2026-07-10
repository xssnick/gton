package metrics

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xssnick/gton/service"
	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/storage/pebblestore"

	"github.com/xssnick/tonutils-go/ton"
)

func TestMetricsHandlerExposesSyncAndStatusMetrics(t *testing.T) {
	namespace := "testgton"
	m := New(namespace)
	m.ObserveSyncBlock(service.SyncBlockObservation{
		Pipeline:         "next_block",
		Chain:            ChainMasterchain,
		Shard:            "masterchain",
		Source:           service.SyncBlockSourceQueue,
		Origin:           service.SyncBlockOriginBroadcast,
		Result:           "success",
		DownloadDuration: time.Second,
		PrepareDuration:  750 * time.Millisecond,
		ApplyDuration:    500 * time.Millisecond,
	})
	m.ObserveSyncObtain(service.SyncObtainObservation{
		Pipeline: "next_block",
		Stage:    "master",
		Result:   "success",
		Duration: 3500 * time.Millisecond,
	})
	m.ObserveSyncPersist(service.SyncPersistObservation{
		Mode:          "next_block_async",
		Result:        "success",
		QueueDuration: 10 * time.Millisecond,
		Duration:      20 * time.Millisecond,
		Stages: []service.SyncPersistStageObservation{
			{Stage: "metadata_sync", Duration: 15 * time.Millisecond},
		},
	})
	m.ObserveBroadcastPipelineStage(p2p.BroadcastPipelineStageObservation{
		Stage:    "candidate_decode",
		Kind:     "tonNode.newBlockCandidateBroadcast",
		Delivery: p2p.DeliveryFEC,
		Result:   "success",
		Duration: 3 * time.Millisecond,
	})
	archivePackagesDir := filepath.Join(t.TempDir(), "archive", "packages")
	stateFilesDir := filepath.Join(t.TempDir(), "archive", "states")
	if err := os.MkdirAll(filepath.Join(archivePackagesDir, "arch0000"), 0o755); err != nil {
		t.Fatalf("create archive packages dir: %v", err)
	}
	if err := os.MkdirAll(stateFilesDir, 0o755); err != nil {
		t.Fatalf("create state files dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(archivePackagesDir, "arch0000", "archive.00000.pack"), []byte("pack"), 0o644); err != nil {
		t.Fatalf("write archive package: %v", err)
	}
	if err := os.WriteFile(filepath.Join(archivePackagesDir, "arch0000", "archive.tmp"), []byte("ignored"), 0o644); err != nil {
		t.Fatalf("write ignored archive file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(stateFilesDir, "state_42_-1_8000000000000000_hash"), []byte("state-a"), 0o644); err != nil {
		t.Fatalf("write state file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(stateFilesDir, "statesplit_42_0_8000000000000000_hash"), []byte("state-b"), 0o644); err != nil {
		t.Fatalf("write split state file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(stateFilesDir, "stateaccount_43_0_8000000000000000_4000000000000000_hash"), []byte("state-c"), 0o644); err != nil {
		t.Fatalf("write account state file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(stateFilesDir, "partial.tmp"), []byte("ignored"), 0o644); err != nil {
		t.Fatalf("write ignored state file: %v", err)
	}
	localMaster := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     42,
	}
	networkMaster := localMaster
	networkMaster.SeqNo = 41
	serviceStatusReader := func() service.StatusSnapshot {
		return service.StatusSnapshot{
			StatusSnapshot: p2p.StatusSnapshot{
				LatestMasterchain: &networkMaster,
				Broadcasts: []p2p.BroadcastStatusSnapshot{
					{
						Direction: "accepted",
						Overlay:   "masterchain",
						Kind:      "tonNode.blockBroadcastCompressedV2",
						Count:     3,
					},
					{
						Direction: "queue_rebroadcasted",
						Overlay:   "masterchain",
						Kind:      "tonNode.externalMessageBroadcast",
						Count:     4,
					},
				},
				BroadcastDrops: []p2p.BroadcastDropStatusSnapshot{
					{
						Overlay: "masterchain",
						Kind:    "tonNode.blockBroadcastCompressedV2",
						Reason:  "signature_check_failed",
						Count:   2,
					},
				},
				FECReceivers: []p2p.FECReceiverStatusSnapshot{
					{
						Overlay:                 "masterchain",
						ActiveStreams:           2,
						ActiveBytes:             4096,
						DeliveredBroadcasts:     7,
						DroppedTotal:            5,
						EvictedTotal:            1,
						CompletedTotal:          11,
						DeliveredCacheHitsTotal: 13,
						SimpleRelaySentTotal:    17,
						SimpleRelayFailedTotal:  19,
						FECRelaySentTotal:       23,
						FECRelayFailedTotal:     29,
					},
				},
			},
			LocalMasterchain:      &localMaster,
			LocalMasterchainUtime: time.Now().Unix() - 12,
			LocalStateLoaded:      true,
			RecentTPS: service.StatusTPSSnapshot{
				WindowMasters:   1,
				Transactions:    42,
				DurationSeconds: 1,
				TPS:             42,
				Complete:        true,
			},
		}
	}
	dbStatusReader := func(context.Context) (pebblestore.DBStatus, error) {
		return pebblestore.DBStatus{
			LazyCellLoads: []storage.LazyCellLoadMetric{
				{Layer: storage.LazyCellLoadLayerDecodedCache, Count: 3},
				{Layer: storage.LazyCellLoadLayerPebble, Count: 5},
			},
			CellGenerations: []pebblestore.CellDBGenerationStatus{
				{
					ID:   1,
					Role: "active",
					Shards: []pebblestore.CellDBShardStatus{
						{
							Shard:        0,
							ReadCells:    7,
							WrittenCells: 11,
						},
					},
					Total: pebblestore.CellDBShardStatus{
						Shard:        -1,
						ReadCells:    7,
						WrittenCells: 11,
					},
				},
			},
		}, nil
	}
	if err := m.RegisterRuntimeCollectors(RuntimeReaders{
		ServiceStatusReader: serviceStatusReader,
		DBStatusReader:      dbStatusReader,
		LazyCellLoadReader: func() []storage.LazyCellLoadMetric {
			return []storage.LazyCellLoadMetric{
				{Layer: storage.LazyCellLoadLayerStateWindow, Count: 2},
			}
		},
		ArchivePackagesDir: archivePackagesDir,
		StateFilesDir:      stateFilesDir,
	}); err != nil {
		t.Fatalf("register runtime collectors: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("metrics status = %d, want %d", rec.Code, http.StatusOK)
	}

	body := rec.Body.String()
	for _, want := range []string{
		namespace + `_sync_blocks_total{catch_up="false",chain="masterchain",pipeline="next_block",result="success",source="queue"} 1`,
		namespace + `_sync_block_origins_total{catch_up="false",chain="masterchain",origin="broadcast",pipeline="next_block",result="success"} 1`,
		namespace + `_sync_block_prepare_duration_seconds_bucket{catch_up="false",chain="masterchain",pipeline="next_block",result="success",shard="masterchain",source="queue",le="1"} 1`,
		namespace + `_sync_master_shards_obtain_duration_seconds_bucket{catch_up="false",pipeline="next_block",result="success",stage="master",le="5"} 1`,
		namespace + `_sync_checkpoints_total{mode="next_block_async",result="success"} 1`,
		namespace + `_sync_checkpoint_stage_duration_seconds_bucket{mode="next_block_async",result="success",stage="metadata_sync",le="0.025"} 1`,
		namespace + `_p2p_broadcast_pipeline_stage_duration_seconds_count{delivery="fec",kind="tonNode.newBlockCandidateBroadcast",result="success",stage="candidate_decode"} 1`,
		namespace + `_sync_gap_blocks{chain="masterchain",shard="masterchain"} 0`,
		namespace + `_sync_lag_seconds{chain="masterchain",shard="masterchain"}`,
		namespace + `_sync_recent_tps 42`,
		namespace + `_sync_recent_transactions 42`,
		namespace + `_sync_recent_tps_complete 1`,
		namespace + `_service_background_task{task="idle"} 1`,
		namespace + `_p2p_broadcasts_total{direction="accepted",kind="tonNode.blockBroadcastCompressedV2",overlay="masterchain"} 3`,
		namespace + `_p2p_broadcasts_total{direction="queue_rebroadcasted",kind="tonNode.externalMessageBroadcast",overlay="masterchain"} 4`,
		namespace + `_p2p_broadcast_dropped_total{kind="tonNode.blockBroadcastCompressedV2",overlay="masterchain",reason="signature_check_failed"} 2`,
		namespace + `_p2p_fec_receiver_active_streams{overlay="masterchain"} 2`,
		namespace + `_p2p_fec_receiver_active_bytes{overlay="masterchain"} 4096`,
		namespace + `_p2p_fec_receiver_delivered_cache_items{overlay="masterchain"} 7`,
		namespace + `_p2p_fec_receiver_dropped_total{overlay="masterchain"} 5`,
		namespace + `_p2p_fec_receiver_evicted_total{overlay="masterchain"} 1`,
		namespace + `_p2p_fec_receiver_completed_total{overlay="masterchain"} 11`,
		namespace + `_p2p_fec_receiver_delivered_cache_hits_total{overlay="masterchain"} 13`,
		namespace + `_p2p_broadcast_relay_sent_total{delivery="simple",overlay="masterchain"} 17`,
		namespace + `_p2p_broadcast_relay_failed_total{delivery="simple",overlay="masterchain"} 19`,
		namespace + `_p2p_broadcast_relay_sent_total{delivery="fec",overlay="masterchain"} 23`,
		namespace + `_p2p_broadcast_relay_failed_total{delivery="fec",overlay="masterchain"} 29`,
		namespace + `_storage_archive_package_bytes 4`,
		namespace + `_storage_persistent_state_bytes 21`,
		namespace + `_storage_cell_db_generation{generation="active"} 1`,
		namespace + `_storage_lazy_cell_loads_total{layer="decoded_cache"} 3`,
		namespace + `_storage_lazy_cell_loads_total{layer="pebble"} 5`,
		namespace + `_storage_lazy_cell_loads_total{layer="state_window"} 2`,
		namespace + `_storage_cell_db_read_cells_total{generation="active",shard="0"} 7`,
		namespace + `_storage_cell_db_written_cells_total{generation="active",shard="0"} 11`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics output does not contain %q\n%s", want, body)
		}
	}

	for _, removed := range []string{
		namespace + `_p2p_queue_pushed_total`,
		namespace + `_storage_archive_packages`,
		namespace + `_storage_persistent_state_masters`,
		namespace + `_storage_cell_db_file_cache_tables`,
		namespace + `_storage_cell_db_live_tables`,
		namespace + `_storage_cell_db_l0_bytes`,
		namespace + `_storage_cell_db_memtable_count`,
		namespace + `_storage_lazy_cell_loads_total{layer="state_window",result=`,
	} {
		if strings.Contains(body, removed) {
			t.Fatalf("metrics output contains removed metric %q\n%s", removed, body)
		}
	}
}

func TestSyncMetricsUseAppliedMasterchainForMasterGap(t *testing.T) {
	namespace := "testgton"
	m := New(namespace)

	currentMaster := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     100,
	}
	appliedMaster := currentMaster
	appliedMaster.SeqNo = 105
	networkMaster := currentMaster
	networkMaster.SeqNo = 106

	err := m.RegisterRuntimeCollectors(RuntimeReaders{
		ServiceStatusReader: func() service.StatusSnapshot {
			return service.StatusSnapshot{
				StatusSnapshot: p2p.StatusSnapshot{
					LatestMasterchain: &networkMaster,
				},
				LocalMasterchain:        &currentMaster,
				LocalMasterchainUtime:   time.Now().Unix() - 60,
				AppliedMasterchain:      &appliedMaster,
				AppliedMasterchainUtime: time.Now().Unix() - 5,
			}
		},
		DBStatusReader: func(context.Context) (pebblestore.DBStatus, error) {
			return pebblestore.DBStatus{}, nil
		},
	})
	if err != nil {
		t.Fatalf("register runtime collectors: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("metrics status = %d, want %d", rec.Code, http.StatusOK)
	}

	body := rec.Body.String()
	for _, want := range []string{
		namespace + `_sync_local_seqno{chain="masterchain",shard="masterchain"} 105`,
		namespace + `_sync_gap_blocks{chain="masterchain",shard="masterchain"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics output does not contain %q\n%s", want, body)
		}
	}
	removed := namespace + `_sync_gap_blocks{chain="masterchain",shard="masterchain"} 6`
	if strings.Contains(body, removed) {
		t.Fatalf("metrics output contains coupled master gap %q\n%s", removed, body)
	}
}

func TestStorageArtifactMetricsUseStartupSnapshotAndDeltas(t *testing.T) {
	namespace := "testgton_artifacts"
	m := New(namespace)
	archivePackagesDir := filepath.Join(t.TempDir(), "archive", "packages")
	stateFilesDir := filepath.Join(t.TempDir(), "archive", "states")
	if err := os.MkdirAll(filepath.Join(archivePackagesDir, "arch0000"), 0o755); err != nil {
		t.Fatalf("create archive packages dir: %v", err)
	}
	if err := os.MkdirAll(stateFilesDir, 0o755); err != nil {
		t.Fatalf("create state files dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(archivePackagesDir, "arch0000", "archive.00000.pack"), []byte("pack-a"), 0o644); err != nil {
		t.Fatalf("write archive package: %v", err)
	}
	if err := os.WriteFile(filepath.Join(stateFilesDir, "state_42_-1_8000000000000000_hash"), []byte("state-a"), 0o644); err != nil {
		t.Fatalf("write state file: %v", err)
	}

	err := m.RegisterRuntimeCollectors(RuntimeReaders{
		ServiceStatusReader: func() service.StatusSnapshot {
			return service.StatusSnapshot{}
		},
		DBStatusReader: func(context.Context) (pebblestore.DBStatus, error) {
			return pebblestore.DBStatus{}, nil
		},
		ArchivePackagesDir: archivePackagesDir,
		StateFilesDir:      stateFilesDir,
	})
	if err != nil {
		t.Fatalf("register runtime collectors: %v", err)
	}

	if err := os.WriteFile(filepath.Join(archivePackagesDir, "arch0000", "archive.00001.pack"), []byte("pack-b-is-not-scanned"), 0o644); err != nil {
		t.Fatalf("write archive package after register: %v", err)
	}
	if err := os.WriteFile(filepath.Join(stateFilesDir, "state_43_-1_8000000000000000_hash"), []byte("state-b-is-not-scanned"), 0o644); err != nil {
		t.Fatalf("write state file after register: %v", err)
	}

	body := metricsBody(t, m)
	for _, want := range []string{
		namespace + `_storage_archive_package_bytes 6`,
		namespace + `_storage_persistent_state_bytes 7`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics output does not contain %q\n%s", want, body)
		}
	}

	m.AddArchivePackageBytes(11)
	m.AddPersistentStateBytes(-2)

	body = metricsBody(t, m)
	for _, want := range []string{
		namespace + `_storage_archive_package_bytes 17`,
		namespace + `_storage_persistent_state_bytes 5`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics output does not contain %q\n%s", want, body)
		}
	}
}

func TestMetricsSyncBlockExplicitOriginOverridesDownloadSourceOrigin(t *testing.T) {
	namespace := "testgton_origin"
	m := New(namespace)
	m.ObserveSyncBlock(service.SyncBlockObservation{
		Pipeline: "next_block_bootstrap",
		Chain:    "shardchain",
		Source:   service.SyncBlockSourceNextBlock,
		Origin:   service.SyncBlockOriginBroadcast,
		Result:   "success",
	})

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, req)

	body := rec.Body.String()
	source := namespace + `_sync_blocks_total{catch_up="false",chain="shardchain",pipeline="next_block_bootstrap",result="success",source="next_block"} 1`
	if !strings.Contains(body, source) {
		t.Fatalf("metrics output does not contain %q\n%s", source, body)
	}
	origin := namespace + `_sync_block_origins_total{catch_up="false",chain="shardchain",origin="broadcast",pipeline="next_block_bootstrap",result="success"} 1`
	if !strings.Contains(body, origin) {
		t.Fatalf("metrics output does not contain %q\n%s", origin, body)
	}
	download := namespace + `_sync_block_origins_total{catch_up="false",chain="shardchain",origin="download",pipeline="next_block_bootstrap",result="success"}`
	if strings.Contains(body, download) {
		t.Fatalf("metrics output contains download origin %q\n%s", download, body)
	}
}

func metricsBody(t *testing.T, m *Metrics) string {
	t.Helper()

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("metrics status = %d, want %d", rec.Code, http.StatusOK)
	}
	return rec.Body.String()
}
