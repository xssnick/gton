package metrics

import (
	"context"
	"net/http"
	"net/http/httptest"
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

	localMaster := ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     42,
	}
	m.SetServiceStatusReader(func() service.StatusSnapshot {
		return service.StatusSnapshot{
			StatusSnapshot: p2p.StatusSnapshot{
				LatestMasterchain: &localMaster,
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
		namespace + `_sync_checkpoints_total{mode="next_block_async",result="success"} 1`,
		namespace + `_sync_lag_seconds{chain="masterchain",shard="masterchain"}`,
		namespace + `_storage_cell_db_read_cells_total{generation="1",role="active",shard="0"} 7`,
		namespace + `_storage_cell_db_written_cells_total{generation="1",role="active",shard="0"} 11`,
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
	m.ObserveSyncCurrentState(service.SyncCurrentStateObservation{})
	m.ObserveSyncBlock(service.SyncBlockObservation{})
	m.ObserveSyncPersist(service.SyncPersistObservation{})
}
