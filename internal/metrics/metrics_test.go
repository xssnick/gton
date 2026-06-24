package metrics

import (
	"context"
	"github.com/xssnick/gton/api/liteserver"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xssnick/gton/service"
	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage/pebblestore"

	"github.com/xssnick/tonutils-go/ton"
)

func TestMetricsHandlerExposesLiteserverSyncAndStatusMetrics(t *testing.T) {
	namespace := "testgton"
	m := New(namespace)
	m.AddLiteserverInflight(1)
	m.AddLiteserverInflight(-1)
	m.ObserveLiteserverQuery(liteserver.QueryObservation{
		Method:       "GetTime",
		Response:     "CurrentTime",
		Duration:     1500 * time.Millisecond,
		WaitDuration: 200 * time.Millisecond,
	})
	m.ObserveSyncBlock(service.SyncBlockObservation{
		Pipeline:         "next_block",
		Chain:            ChainMasterchain,
		Shard:            "masterchain",
		Source:           "queue",
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
		ArchivePackagesDir:  archivePackagesDir,
		StateFilesDir:       stateFilesDir,
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
		namespace + `_liteserver_query_duration_seconds_bucket{error_code="0",method="GetTime",reason="none",response="CurrentTime",le="2.5"} 1`,
		namespace + `_liteserver_query_wait_seconds_bucket{error_code="0",method="GetTime",reason="none",response="CurrentTime",le="0.25"} 1`,
		namespace + `_liteserver_queries_total{error_code="0",method="GetTime",reason="none",response="CurrentTime"} 1`,
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
	} {
		if strings.Contains(body, removed) {
			t.Fatalf("metrics output contains removed metric %q\n%s", removed, body)
		}
	}
}

func TestLiteserverMetricsTreatUnspecifiedLSErrorAsError(t *testing.T) {
	namespace := "testgton"
	m := New(namespace)
	m.ObserveLiteserverQuery(liteserver.QueryObservation{
		Method:      "SendMessage",
		Response:    "LSError",
		Error:       true,
		ErrorCode:   0,
		ErrorReason: "tvm_rejected",
		Duration:    time.Millisecond,
	})

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("metrics status = %d, want %d", rec.Code, http.StatusOK)
	}

	body := rec.Body.String()
	want := namespace + `_liteserver_queries_total{error_code="unspecified",method="SendMessage",reason="tvm_rejected",response="LSError"} 1`
	if !strings.Contains(body, want) {
		t.Fatalf("metrics output does not contain %q\n%s", want, body)
	}

	removed := namespace + `_liteserver_queries_total{error_code="0",method="SendMessage",reason="tvm_rejected",response="LSError"}`
	if strings.Contains(body, removed) {
		t.Fatalf("metrics output contains successful error label %q\n%s", removed, body)
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

func TestLiteserverMetricsUseUnspecifiedReasonForUnclassifiedErrors(t *testing.T) {
	namespace := "testgton"
	m := New(namespace)
	m.ObserveLiteserverQuery(liteserver.QueryObservation{
		Method:   "SendMessage",
		Response: "LSError",
		Error:    true,
		Duration: time.Millisecond,
	})

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("metrics status = %d, want %d", rec.Code, http.StatusOK)
	}

	body := rec.Body.String()
	want := namespace + `_liteserver_queries_total{error_code="unspecified",method="SendMessage",reason="unspecified",response="LSError"} 1`
	if !strings.Contains(body, want) {
		t.Fatalf("metrics output does not contain %q\n%s", want, body)
	}
}

func TestNilMetricsObserverMethodsAreNoop(t *testing.T) {
	var m *Metrics

	m.AddLiteserverInflight(1)
	m.ObserveLiteserverQuery(liteserver.QueryObservation{Method: "GetTime", Duration: time.Second})
	m.ObserveSyncBlock(service.SyncBlockObservation{})
	m.ObserveSyncObtain(service.SyncObtainObservation{})
	m.ObserveSyncPersist(service.SyncPersistObservation{})
	m.ObserveBroadcastPipelineStage(p2p.BroadcastPipelineStageObservation{})
}
