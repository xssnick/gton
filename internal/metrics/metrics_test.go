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

	"github.com/xssnick/gton/liteserver"
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
		Source:           "queue",
		Result:           "success",
		DownloadDuration: time.Second,
		ApplyDuration:    500 * time.Millisecond,
	})
	m.ObserveSyncPersist(service.SyncPersistObservation{
		Mode:          "next_block_async",
		Result:        "success",
		QueueDuration: 10 * time.Millisecond,
		Duration:      20 * time.Millisecond,
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
	if err := os.WriteFile(filepath.Join(stateFilesDir, "state_42_-1_8000000000000000_hash"), []byte("state-a"), 0o644); err != nil {
		t.Fatalf("write state file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(stateFilesDir, "statesplit_42_0_8000000000000000_hash"), []byte("state-b"), 0o644); err != nil {
		t.Fatalf("write split state file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(stateFilesDir, "stateaccount_43_0_8000000000000000_4000000000000000_hash"), []byte("state-c"), 0o644); err != nil {
		t.Fatalf("write account state file: %v", err)
	}
	m.SetStorageArtifactDirs(archivePackagesDir, stateFilesDir)

	localMaster := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     42,
	}
	m.SetServiceStatusReader(func() service.StatusSnapshot {
		return service.StatusSnapshot{
			StatusSnapshot: p2p.StatusSnapshot{
				LatestMasterchain: &localMaster,
				Broadcasts: []p2p.BroadcastStatusSnapshot{
					{
						Direction: "accepted",
						Overlay:   "masterchain",
						Kind:      "tonNode.blockBroadcastCompressedV2",
						Count:     3,
					},
				},
			},
			LocalMasterchain:      &localMaster,
			LocalMasterchainUtime: time.Now().Unix() - 12,
			LocalStateLoaded:      true,
		}
	})
	m.SetDBStatusReader(func(context.Context) (pebblestore.DBStatus, error) {
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
	})

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("metrics status = %d, want %d", rec.Code, http.StatusOK)
	}

	body := rec.Body.String()
	for _, want := range []string{
		namespace + `_liteserver_query_duration_seconds_bucket{error_code="0",method="GetTime",response="CurrentTime",le="2.5"} 1`,
		namespace + `_liteserver_query_wait_seconds_bucket{error_code="0",method="GetTime",response="CurrentTime",le="0.25"} 1`,
		namespace + `_liteserver_queries_total{error_code="0",method="GetTime",response="CurrentTime"} 1`,
		namespace + `_sync_blocks_total{catch_up="false",chain="masterchain",pipeline="next_block",result="success",source="queue"} 1`,
		namespace + `_sync_block_origins_total{catch_up="false",chain="masterchain",origin="broadcast",pipeline="next_block",result="success"} 1`,
		namespace + `_sync_checkpoints_total{mode="next_block_async",result="success"} 1`,
		namespace + `_sync_lag_seconds{chain="masterchain",shard="masterchain"}`,
		namespace + `_service_background_task{task="idle"} 1`,
		namespace + `_p2p_broadcasts_total{direction="accepted",kind="tonNode.blockBroadcastCompressedV2",overlay="masterchain"} 3`,
		namespace + `_storage_archive_packages 1`,
		namespace + `_storage_archive_package_bytes 4`,
		namespace + `_storage_persistent_state_masters 2`,
		namespace + `_storage_persistent_state_bytes 21`,
		namespace + `_storage_cell_db_generation{generation="active"} 1`,
		namespace + `_storage_cell_db_read_cells_total{generation="active",shard="0"} 7`,
		namespace + `_storage_cell_db_written_cells_total{generation="active",shard="0"} 11`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics output does not contain %q\n%s", want, body)
		}
	}
}

func TestNilMetricsObserverMethodsAreNoop(t *testing.T) {
	var m *Metrics

	m.AddLiteserverInflight(1)
	m.ObserveLiteserverQuery(liteserver.QueryObservation{Method: "GetTime", Duration: time.Second})
	m.ObserveSyncBlock(service.SyncBlockObservation{})
	m.ObserveSyncPersist(service.SyncPersistObservation{})
}
