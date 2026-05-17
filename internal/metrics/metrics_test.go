package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMetricsHandlerExposesLiteserverAndSyncMetrics(t *testing.T) {
	m := New()
	m.ObserveLiteserverQuery("GetTime", 1500*time.Millisecond)
	m.SetSyncLag(ChainMasterchain, 12)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("metrics status = %d, want %d", rec.Code, http.StatusOK)
	}

	body := rec.Body.String()
	for _, want := range []string{
		`flexserver_liteserver_query_duration_seconds_bucket{method="GetTime",le="2.5"} 1`,
		`flexserver_sync_lag_seconds{chain="masterchain"} 12`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics output does not contain %q\n%s", want, body)
		}
	}
}

func TestNilMetricsObserverMethodsAreNoop(t *testing.T) {
	var m *Metrics

	m.ObserveLiteserverQuery("GetTime", time.Second)
	m.SetSyncLag(ChainMasterchain, 12)
}
