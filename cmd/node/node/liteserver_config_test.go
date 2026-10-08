package node

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	nodeconfig "github.com/xssnick/gton/cmd/node/config"
)

const liteServerConfigTestSeed = "nWGxne/9WmC6hEr0kuwsxERJxWl7MmkZcDusAxyuf2A="
const liteServerConfigTestPublicKey = "11qYAYKxCrfVS/7TyWQHOg7hcvPapiMlrwIaaPcHURo="

type liteServerConfigOutputTest struct {
	name         string
	externalAddr string
	listenAddr   string
	ip           int64
	port         int
}

type liteServerConfigInvalidTest struct {
	name      string
	change    func(*nodeconfig.Config)
	wantError string
}

func liteServerConfigTestConfig(t *testing.T) nodeconfig.Config {
	t.Helper()
	seed, err := base64.StdEncoding.DecodeString(liteServerConfigTestSeed)
	if err != nil {
		t.Fatal(err)
	}

	return nodeconfig.Config{
		ADNL: nodeconfig.ADNL{ExternalAddr: "203.0.113.10:30303"},
		Lite: nodeconfig.Lite{Key: seed, ListenAddr: "0.0.0.0:7445"},
	}
}

func TestParseNodeFlagsPrintLiteServerConfig(t *testing.T) {
	options, commands, err := parseNodeFlags([]string{
		"--config", "other.json",
		"--print-ls-config",
		"--verbosity", "unused",
	}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if !commands.printLSConfig || options.ConfigFile != "other.json" {
		t.Fatalf("print liteserver config = %t, config = %q", commands.printLSConfig, options.ConfigFile)
	}
}

func TestWriteLiteServerConfig(t *testing.T) {
	tests := []liteServerConfigOutputTest{
		{
			name:         "signed IPv4 and external host",
			externalAddr: "203.0.113.10:30303", listenAddr: "192.0.2.20:7445",
			ip: -889163510, port: 7445,
		},
		{
			name:         "positive IPv4 and maximum port",
			externalAddr: "1.2.3.4:30303", listenAddr: "0.0.0.0:65535",
			ip: 16909060, port: 65535,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := liteServerConfigTestConfig(t)
			cfg.ADNL.ExternalAddr = test.externalAddr
			cfg.Lite.ListenAddr = test.listenAddr

			var out bytes.Buffer
			if err := writeLiteServerConfig(&out, cfg, "other.json"); err != nil {
				t.Fatal(err)
			}
			want := fmt.Sprintf(`{
  "ip": %d,
  "port": %d,
  "id": {
    "@type": "pub.ed25519",
    "key": "%s"
  }
}
`, test.ip, test.port, liteServerConfigTestPublicKey)
			if out.String() != want {
				t.Fatalf("stdout = %q, want %q", out.String(), want)
			}
		})
	}
}

func TestWriteLiteServerConfigRejectsInvalidConfig(t *testing.T) {
	tests := []liteServerConfigInvalidTest{
		{
			name: "missing key", wantError: "liteserver key is missing in other.json",
			change: func(cfg *nodeconfig.Config) { cfg.Lite.Key = nil },
		},
		{
			name: "invalid key", wantError: "invalid liteserver key size",
			change: func(cfg *nodeconfig.Config) { cfg.Lite.Key = []byte{1} },
		},
		{
			name: "missing external address", wantError: "parse adnl.external_addr",
			change: func(cfg *nodeconfig.Config) { cfg.ADNL.ExternalAddr = "" },
		},
		{
			name: "malformed external address", wantError: "parse adnl.external_addr",
			change: func(cfg *nodeconfig.Config) { cfg.ADNL.ExternalAddr = "203.0.113.10" },
		},
		{
			name: "hostname", wantError: "adnl.external_addr must contain a non-zero IPv4 address",
			change: func(cfg *nodeconfig.Config) { cfg.ADNL.ExternalAddr = "example.com:30303" },
		},
		{
			name: "IPv6", wantError: "adnl.external_addr must contain a non-zero IPv4 address",
			change: func(cfg *nodeconfig.Config) { cfg.ADNL.ExternalAddr = "[2001:db8::1]:30303" },
		},
		{
			name: "unspecified IP", wantError: "adnl.external_addr must contain a non-zero IPv4 address",
			change: func(cfg *nodeconfig.Config) { cfg.ADNL.ExternalAddr = "0.0.0.0:30303" },
		},
		{
			name: "missing listen address", wantError: "parse liteserver.listen_addr",
			change: func(cfg *nodeconfig.Config) { cfg.Lite.ListenAddr = "" },
		},
		{
			name: "zero port", wantError: "liteserver.listen_addr must contain a port between 1 and 65535",
			change: func(cfg *nodeconfig.Config) { cfg.Lite.ListenAddr = "0.0.0.0:0" },
		},
		{
			name: "port out of range", wantError: "liteserver.listen_addr must contain a port between 1 and 65535",
			change: func(cfg *nodeconfig.Config) { cfg.Lite.ListenAddr = "0.0.0.0:65536" },
		},
		{
			name: "non-numeric port", wantError: "liteserver.listen_addr must contain a port between 1 and 65535",
			change: func(cfg *nodeconfig.Config) { cfg.Lite.ListenAddr = "0.0.0.0:invalid" },
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := liteServerConfigTestConfig(t)
			test.change(&cfg)

			var out bytes.Buffer
			err := writeLiteServerConfig(&out, cfg, "other.json")
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("error = %v, want %q", err, test.wantError)
			}
			if out.Len() != 0 {
				t.Fatalf("stdout = %q, want empty output", out.String())
			}
		})
	}
}

type liteServerConfigErrorWriter struct{}

func (liteServerConfigErrorWriter) Write([]byte) (int, error) {
	return 0, io.ErrClosedPipe
}

func TestWriteLiteServerConfigWriterError(t *testing.T) {
	err := writeLiteServerConfig(liteServerConfigErrorWriter{}, liteServerConfigTestConfig(t), "other.json")
	if !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("error = %v, want %v", err, io.ErrClosedPipe)
	}
}
