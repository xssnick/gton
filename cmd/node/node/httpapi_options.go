package node

import (
	"strings"
	"time"

	"github.com/xssnick/gton/api/httpapi"
	nodeconfig "github.com/xssnick/gton/cmd/node/config"
	"github.com/xssnick/gton/service/hooks"

	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/ton"
)

type httpapiOptions struct {
	Enabled        bool
	ListenAddr     string
	RequestTimeout time.Duration
	ZeroState      ton.ZeroStateIDExt
}

// configureHTTPAPI resolves the HTTP API options and returns the HTTP API
// extension factory, nil when the API is disabled.
func configureHTTPAPI(cfg nodeconfig.Config, runtimeOpts nodeconfig.RuntimeOptions, globalConfig *liteclient.GlobalConfig) (httpapiOptions, hooks.ExtensionFactory, error) {
	opts := httpapiOptionsFromConfig(cfg, runtimeOpts)
	if !opts.Enabled {
		return opts, nil, nil
	}

	zeroState, err := liteserverZeroStateFromGlobalConfig(globalConfig)
	if err != nil {
		return httpapiOptions{}, nil, err
	}
	opts.ZeroState = zeroState
	return opts, httpapiExtensionFactory(opts), nil
}

func httpapiExtensionFactory(opts httpapiOptions) hooks.ExtensionFactory {
	return func(node hooks.Node) (hooks.Extension, error) {
		return httpapi.NewExtension(node, httpapi.ExtensionConfig{
			ListenAddr:     opts.ListenAddr,
			RequestTimeout: opts.RequestTimeout,
			ZeroState:      opts.ZeroState,
		})
	}
}

func httpapiOptionsFromConfig(cfg nodeconfig.Config, runtimeOpts nodeconfig.RuntimeOptions) httpapiOptions {
	listenAddr := strings.TrimSpace(cfg.HTTPAPI.ListenAddr)
	if listenAddr == "" {
		listenAddr = nodeconfig.DefaultHTTPAPIListen
	}

	return httpapiOptions{
		Enabled:        cfg.HTTPAPI.Enabled,
		ListenAddr:     listenAddr,
		RequestTimeout: runtimeOpts.HTTPAPI.RequestTimeout,
	}
}
