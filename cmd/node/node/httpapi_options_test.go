package node

import (
	"testing"
	"time"

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

func TestConfigureHTTPAPIReturnsExtensionFactory(t *testing.T) {
	cfg := nodeconfig.Config{
		HTTPAPI: nodeconfig.HTTPAPI{
			Enabled: true,
		},
	}

	_, factory, err := configureHTTPAPI(cfg, httpRuntimeOptionsForTest(cfg), liteserverTestGlobalConfig())
	if err != nil {
		t.Fatalf("configure http api: %v", err)
	}
	if factory == nil {
		t.Fatal("expected http api extension factory")
	}
}

func TestConfigureHTTPAPIDisabledReturnsNoFactory(t *testing.T) {
	cfg := nodeconfig.Config{}
	_, factory, err := configureHTTPAPI(cfg, httpRuntimeOptionsForTest(cfg), liteserverTestGlobalConfig())
	if err != nil {
		t.Fatalf("configure http api: %v", err)
	}
	if factory != nil {
		t.Fatal("expected no extension factory for disabled http api")
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
