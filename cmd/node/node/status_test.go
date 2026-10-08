package node

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	nodeconfig "github.com/xssnick/gton/cmd/node/config"
)

type statusCommandTest struct {
	name string
	args []string
	mode string
}

type statusClientTest struct {
	name string
	mode string
	code int
	body string
}

type statusAddressTest struct {
	name    string
	address string
}

func TestParseNodeFlagsStatus(t *testing.T) {
	tests := []statusCommandTest{
		{name: "short", args: []string{"status"}, mode: "short"},
		{name: "full", args: []string{"status", "full"}, mode: "full"},
		{name: "db", args: []string{"status", "db"}, mode: "db"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			args := append([]string{"--config", "other.json"}, test.args...)
			options, commands, err := parseNodeFlags(args, &bytes.Buffer{})
			if err != nil {
				t.Fatal(err)
			}
			if commands.statusMode != test.mode || options.ConfigFile != "other.json" {
				t.Fatalf("status mode = %q, config = %q", commands.statusMode, options.ConfigFile)
			}
		})
	}
}

func TestParseNodeFlagsRejectsInvalidStatus(t *testing.T) {
	tests := []statusCommandTest{
		{name: "unknown command", args: []string{"statuz"}},
		{name: "unknown mode", args: []string{"status", "validator"}},
		{name: "extra argument", args: []string{"status", "full", "db"}},
		{name: "flags after command", args: []string{"status", "--config", "other.json"}},
		{name: "version conflict", args: []string{"--version", "status"}},
		{name: "public key conflict", args: []string{"--ls-pubkey", "status"}},
		{name: "liteserver config conflict", args: []string{"--print-ls-config", "status"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, _, err := parseNodeFlags(test.args, &bytes.Buffer{}); err == nil {
				t.Fatal("invalid status command was accepted")
			}
		})
	}
}

func TestWriteRemoteStatus(t *testing.T) {
	tests := []statusClientTest{
		{name: "short", mode: "short", code: 200, body: "Status\n\nNode\n  syncing\n"},
		{name: "full", mode: "full", code: 200, body: "Status\n\nOverlays\n"},
		{name: "db", mode: "db", code: 200, body: "DB Status\n\nMeta DB\n"},
		{name: "old server", mode: "short", code: 404, body: "404 page not found\n"},
		{name: "db error", mode: "db", code: 500, body: "load db status: unavailable\n"},
		{name: "redirect", mode: "short", code: 302, body: "redirect disabled\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "GET" || r.URL.Path != "/status" {
					t.Errorf("request = %s %s", r.Method, r.URL.String())
				}
				expectedMode := test.mode
				if expectedMode == "short" {
					expectedMode = ""
				}
				if r.URL.Query().Get("mode") != expectedMode {
					t.Errorf("request mode = %q, want %q", r.URL.Query().Get("mode"), expectedMode)
				}
				w.Header().Set("Location", "/unexpected")
				w.WriteHeader(test.code)
				io.WriteString(w, test.body)
			}))
			defer server.Close()

			// Wildcard listeners must connect through loopback, even with proxy settings.
			_, port, err := net.SplitHostPort(server.Listener.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			t.Setenv("HTTP_PROXY", "http://127.0.0.1:1")
			cfg := nodeconfig.Metrics{Enabled: true, ListenAddr: "0.0.0.0:" + port}
			var output bytes.Buffer
			err = writeRemoteStatus(t.Context(), &output, cfg, test.mode)
			if test.code == 200 {
				if err != nil {
					t.Fatal(err)
				}
				if output.String() != test.body {
					t.Fatalf("status output = %q, want %q", output.String(), test.body)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), strings.TrimSpace(test.body)) {
				t.Fatalf("status error = %v, want HTTP error with body", err)
			}
			if output.Len() != 0 {
				t.Fatalf("failed request wrote to stdout: %q", output.String())
			}
		})
	}
}

func TestWriteRemoteStatusRejectsDisabledAndInvalidAddresses(t *testing.T) {
	var output bytes.Buffer
	if err := writeRemoteStatus(t.Context(), &output, nodeconfig.Metrics{}, "short"); err == nil ||
		!strings.Contains(err.Error(), "metrics.enabled is false") {
		t.Fatalf("disabled server error = %v", err)
	}
	tests := []statusAddressTest{
		{name: "empty", address: ""},
		{name: "missing port", address: "127.0.0.1"},
		{name: "dynamic port", address: "127.0.0.1:0"},
		{name: "invalid port", address: "127.0.0.1:abc"},
		{name: "large port", address: "127.0.0.1:65536"},
		{name: "remote address", address: "203.0.113.10:9090"},
		{name: "remote hostname", address: "example.org:9090"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := nodeconfig.Metrics{Enabled: true, ListenAddr: test.address}
			if err := writeRemoteStatus(t.Context(), &output, cfg, "short"); err == nil {
				t.Fatal("invalid local status address was accepted")
			}
		})
	}
}

func TestWriteRemoteStatusDeadline(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer server.Close()
	cfg := nodeconfig.Metrics{Enabled: true, ListenAddr: server.Listener.Addr().String()}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	var output bytes.Buffer
	if err := writeRemoteStatus(ctx, &output, cfg, "short"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline error = %v", err)
	}
	if output.Len() != 0 {
		t.Fatalf("timed out request wrote to stdout: %q", output.String())
	}
}

func TestWriteRemoteStatusIPv6Wildcard(t *testing.T) {
	listener, err := net.Listen("tcp", "[::1]:0")
	if err != nil {
		t.Skipf("IPv6 loopback is unavailable: %v", err)
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "IPv6 status\n")
	}))
	server.Listener.Close()
	server.Listener = listener
	server.Start()
	defer server.Close()

	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	cfg := nodeconfig.Metrics{Enabled: true, ListenAddr: net.JoinHostPort("::", port)}
	var output bytes.Buffer
	if err := writeRemoteStatus(t.Context(), &output, cfg, "short"); err != nil {
		t.Fatal(err)
	}
	if output.String() != "IPv6 status\n" {
		t.Fatalf("IPv6 output = %q", output.String())
	}
}

func TestWriteRemoteStatusUnavailable(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	var output bytes.Buffer
	err = writeRemoteStatus(t.Context(), &output, nodeconfig.Metrics{Enabled: true, ListenAddr: address}, "short")
	if err == nil || !strings.Contains(err.Error(), "get status from") {
		t.Fatalf("unavailable node error = %v", err)
	}
}

func TestStatusConfigLoadIsReadOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	if _, _, err := loadNodeConfig(t.Context(), path, true); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing config error = %v", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("status created a config: %v", err)
	}

	contents := []byte(`{"metrics":{"enabled":true,"listen_addr":"127.0.0.1:19090"},"storage":{"dir":"missing-data"}}`)
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, created, err := loadNodeConfig(t.Context(), path, true)
	if err != nil || created || cfg.Metrics.ListenAddr != "127.0.0.1:19090" {
		t.Fatalf("config = %+v, created = %t, error = %v", cfg.Metrics, created, err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, contents) {
		t.Fatal("status modified the config")
	}
}
