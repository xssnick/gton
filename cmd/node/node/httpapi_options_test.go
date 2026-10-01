package node

import (
	"testing"
	"time"

	"github.com/xssnick/gton"
	nodeconfig "github.com/xssnick/gton/cmd/node/config"
)

func TestHTTPAPIOptionsFromConfig(t *testing.T) {
	cfg := nodeconfig.Config{
		HTTPAPI: nodeconfig.HTTPAPI{
			Enabled:               true,
			ListenAddr:            "127.0.0.1:8081",
			RequestTimeoutSeconds: 7,
		},
	}

	opts := httpapiOptionsFromConfig(cfg, httpRuntimeOptionsForTest(cfg))
	if !opts.Enabled {
		t.Fatal("expected http api to be enabled")
	}
	if opts.ListenAddr != "127.0.0.1:8081" {
		t.Fatalf("unexpected listen addr %q", opts.ListenAddr)
	}
	if opts.RequestTimeout != 7*time.Second {
		t.Fatalf("unexpected request timeout %s", opts.RequestTimeout)
	}
}

func TestConfigureHTTPAPISetsTypedNodeOptions(t *testing.T) {
	cfg := nodeconfig.Config{
		HTTPAPI: nodeconfig.HTTPAPI{
			Enabled:               true,
			ListenAddr:            "127.0.0.1:8081",
			RequestTimeoutSeconds: 7,
		},
	}
	var runOpts gton.NodeOptions

	_, err := configureHTTPAPI(&runOpts, cfg, httpRuntimeOptionsForTest(cfg), liteserverTestGlobalConfig())
	if err != nil {
		t.Fatalf("configure http api: %v", err)
	}
	if runOpts.HTTPAPI == nil {
		t.Fatal("expected typed http api options")
	}
	if runOpts.HTTPAPI.ListenAddr != "127.0.0.1:8081" {
		t.Fatalf("unexpected listen addr %q", runOpts.HTTPAPI.ListenAddr)
	}
	if runOpts.HTTPAPI.RequestTimeout != 7*time.Second {
		t.Fatalf("unexpected request timeout %s", runOpts.HTTPAPI.RequestTimeout)
	}
}

func TestConfigureHTTPAPIDisabledClearsBuiltInOptions(t *testing.T) {
	cfg := nodeconfig.Config{}
	runOpts := gton.NodeOptions{HTTPAPI: &gton.HTTPAPIOptions{ListenAddr: "stale"}}
	_, err := configureHTTPAPI(&runOpts, cfg, httpRuntimeOptionsForTest(cfg), liteserverTestGlobalConfig())
	if err != nil {
		t.Fatalf("configure http api: %v", err)
	}
	if runOpts.HTTPAPI != nil {
		t.Fatal("expected disabled http api to have no built-in options")
	}
}

func TestHTTPAPIOptionsRejectInvalidLimits(t *testing.T) {
	tests := []struct {
		name string
		cfg  nodeconfig.HTTPAPI
	}{
		{
			name: "negative timeout",
			cfg:  nodeconfig.HTTPAPI{RequestTimeoutSeconds: -1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := nodeconfig.Config{
				TON:     nodeconfig.TON{SyncBefore: int64(time.Hour / time.Second)},
				HTTPAPI: tt.cfg,
			}
			_, err := cfg.RuntimeOptions(nodeconfig.RuntimeOptions{}.Node)
			if err == nil {
				t.Fatal("expected invalid http api options to fail")
			}
		})
	}
}

func httpRuntimeOptionsForTest(cfg nodeconfig.Config) nodeconfig.RuntimeOptions {
	timeout := nodeconfig.DefaultHTTPAPIRequestTimeout
	if cfg.HTTPAPI.RequestTimeoutSeconds != 0 {
		timeout = time.Duration(cfg.HTTPAPI.RequestTimeoutSeconds) * time.Second
	}
	return nodeconfig.RuntimeOptions{
		HTTPAPI: nodeconfig.HTTPAPIOptions{
			RequestTimeout: timeout,
		},
	}
}
