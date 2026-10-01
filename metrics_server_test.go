package gton

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/gton/console"
	"github.com/xssnick/gton/service"
	"github.com/xssnick/gton/service/storage/pebblestore"
)

type statusHTTPTest struct {
	name    string
	method  string
	path    string
	command string
	code    int
}

func TestMetricsHTTPStatusMatchesConsole(t *testing.T) {
	var registry console.Registry
	err := registerConsoleCommands(
		&registry,
		func(context.Context) service.StatusSnapshot { return service.StatusSnapshot{} },
		&fakeConsoleStateLifecycleCommands{},
		func(context.Context) (pebblestore.DBStatus, error) { return pebblestore.DBStatus{}, nil },
	)
	if err != nil {
		t.Fatal(err)
	}

	metrics := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "test_metric 1\n")
	})
	handler := metricsHTTPHandler(zerolog.Nop(), metrics, &registry)
	tests := []statusHTTPTest{
		{name: "short", method: "GET", path: "/status", command: "status", code: 200},
		{name: "explicit short", method: "GET", path: "/status?mode=short", command: "status", code: 200},
		{name: "full", method: "GET", path: "/status?mode=full", command: "status full", code: 200},
		{name: "db", method: "GET", path: "/status?mode=db", command: "status db", code: 200},
		{name: "metrics", method: "GET", path: "/metrics", code: 200},
		{name: "unknown mode", method: "GET", path: "/status?mode=invalid", code: 400},
		{name: "maintenance command", method: "GET", path: "/status?mode=serialize", code: 400},
		{name: "multiple modes", method: "GET", path: "/status?mode=full&mode=db", code: 400},
		{name: "malformed query", method: "GET", path: "/status?mode=%zz", code: 400},
		{name: "post", method: "POST", path: "/status", code: 405},
		{name: "head", method: "HEAD", path: "/status", code: 405},
		{name: "unknown path", method: "GET", path: "/status/full", code: 404},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(test.method, test.path, nil))
			if response.Code != test.code {
				t.Fatalf("status = %d, want %d: %s", response.Code, test.code, response.Body.String())
			}
			if test.command != "" {
				expected, err := registry.Execute(t.Context(), test.command)
				if err != nil {
					t.Fatal(err)
				}
				if response.Body.String() != expected {
					t.Fatalf("HTTP output differs from console:\n%s\nwant:\n%s", response.Body.String(), expected)
				}
				if response.Header().Get("Content-Type") != "text/plain; charset=utf-8" ||
					response.Header().Get("Cache-Control") != "no-store" {
					t.Fatalf("status headers = %v", response.Header())
				}
			}
			if test.path == "/metrics" && response.Body.String() != "test_metric 1\n" {
				t.Fatalf("metrics response = %q", response.Body.String())
			}
			if test.code == 405 && response.Header().Get("Allow") != "GET" {
				t.Fatalf("Allow = %q", response.Header().Get("Allow"))
			}
		})
	}
}

func TestMetricsHTTPStatusReportsDBErrors(t *testing.T) {
	var registry console.Registry
	if err := registry.Register("status db", func(context.Context, []string) (string, error) {
		return "", errors.New("db unavailable")
	}); err != nil {
		t.Fatal(err)
	}

	handler := metricsHTTPHandler(zerolog.Nop(), http.NotFoundHandler(), &registry)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest("GET", "/status?mode=db", nil))
	if response.Code != 500 || !strings.Contains(response.Body.String(), "db unavailable") {
		t.Fatalf("response = %d %s", response.Code, response.Body.String())
	}
}

func TestMetricsServerCloseWaitsForRequests(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	entered := make(chan struct{})
	release := make(chan struct{})
	var registry console.Registry
	if err := registry.Register("status", func(ctx context.Context, args []string) (string, error) {
		close(entered)
		select {
		case <-release:
			return "ready\n", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}); err != nil {
		t.Fatal(err)
	}
	server, err := startMetricsServer(ctx, zerolog.Nop(), "127.0.0.1:0",
		metricsHTTPHandler(zerolog.Nop(), http.NotFoundHandler(), &registry))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		closeCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := server.Close(closeCtx); err != nil {
			t.Error(err)
		}
	})

	result := make(chan error, 1)
	go func() {
		client := &http.Client{Timeout: 5 * time.Second}
		response, err := client.Get("http://" + server.server.Addr + "/status")
		if err != nil {
			result <- err
			return
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		if err == nil && string(body) != "ready\n" {
			err = errors.New("unexpected status response: " + string(body))
		}
		result <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("status handler did not start")
	}

	closed := make(chan error, 1)
	go func() {
		closeCtx, stop := context.WithTimeout(t.Context(), 5*time.Second)
		defer stop()
		closed <- server.Close(closeCtx)
	}()
	<-server.done
	select {
	case err := <-closed:
		t.Fatalf("shutdown returned before the active request: %v", err)
	default:
	}

	close(release)
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestMetricsServerCancelsRequestsWithNode(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	entered := make(chan struct{})
	canceled := make(chan struct{})
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-r.Context().Done()
		close(canceled)
	})
	server, err := startMetricsServer(ctx, zerolog.Nop(), "127.0.0.1:0", handler)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		closeCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := server.Close(closeCtx); err != nil {
			t.Error(err)
		}
	})
	result := make(chan error, 1)
	go func() {
		client := &http.Client{Timeout: 5 * time.Second}
		response, err := client.Get("http://" + server.server.Addr + "/status")
		if err == nil {
			response.Body.Close()
		}
		result <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("request did not start")
	}
	cancel()
	closeCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	if err := server.Close(closeCtx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-canceled:
	default:
		t.Fatal("node shutdown did not cancel and join the request")
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestMetricsServerCloseCanRetryAfterDeadline(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	entered := make(chan struct{})
	finished := make(chan struct{})
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-r.Context().Done()
		close(finished)
	})
	server, err := startMetricsServer(ctx, zerolog.Nop(), "127.0.0.1:0", handler)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		closeCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := server.Close(closeCtx); err != nil {
			t.Error(err)
		}
	})
	result := make(chan error, 1)
	go func() {
		client := &http.Client{Timeout: 5 * time.Second}
		response, err := client.Get("http://" + server.server.Addr + "/status")
		if err == nil {
			response.Body.Close()
		}
		result <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("request did not start")
	}
	closeCtx, stop := context.WithCancel(t.Context())
	stop()
	if err := server.Close(closeCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("interrupted shutdown = %v", err)
	}
	select {
	case <-finished:
		t.Fatal("interrupted shutdown closed an active request")
	default:
	}

	cancel()
	closeCtx, stop = context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	if err := server.Close(closeCtx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-finished:
	default:
		t.Fatal("retried shutdown did not join the request")
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}
