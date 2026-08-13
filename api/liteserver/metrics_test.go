package liteserver

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xssnick/gton/internal/metrics"
)

var _ MetricsRegistry = (*metrics.Metrics)(nil)

func TestLiteserverMetricsRegisterAndObserveQueries(t *testing.T) {
	namespace := "testgton"
	m := metrics.New(namespace)
	observer, err := NewQueryObserver(m)
	if err != nil {
		t.Fatalf("register liteserver metrics: %v", err)
	}
	if observer == nil {
		t.Fatal("expected liteserver metrics observer")
	}

	observer.AddLiteserverInflight(1)
	observer.AddLiteserverInflight(-1)
	observer.ObserveLiteserverQuery(QueryObservation{
		Method:       "GetTime",
		Response:     "CurrentTime",
		Duration:     1500 * time.Millisecond,
		WaitDuration: 200 * time.Millisecond,
	})

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
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics output does not contain %q\n%s", want, body)
		}
	}
}

func TestLiteserverMetricsTreatUnspecifiedLSErrorAsError(t *testing.T) {
	namespace := "testgton"
	m := metrics.New(namespace)
	observer, err := NewQueryObserver(m)
	if err != nil {
		t.Fatalf("register liteserver metrics: %v", err)
	}

	observer.ObserveLiteserverQuery(QueryObservation{
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

func TestLiteserverMetricsUseUnspecifiedReasonForUnclassifiedErrors(t *testing.T) {
	namespace := "testgton"
	m := metrics.New(namespace)
	observer, err := NewQueryObserver(m)
	if err != nil {
		t.Fatalf("register liteserver metrics: %v", err)
	}

	observer.ObserveLiteserverQuery(QueryObservation{
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

func TestLiteserverMetricsDisabledWithoutRegistry(t *testing.T) {
	observer, err := NewQueryObserver(nil)
	if err != nil {
		t.Fatalf("disabled liteserver metrics: %v", err)
	}
	if observer != nil {
		t.Fatal("expected nil observer without metrics registry")
	}
}
