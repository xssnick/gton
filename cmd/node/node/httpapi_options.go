package node

import (
	"strings"
	"time"

	"github.com/xssnick/gton"
	nodeconfig "github.com/xssnick/gton/cmd/node/config"

	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/ton"
)

type httpapiOptions struct {
	Enabled        bool
	ListenAddr     string
	RequestTimeout time.Duration
	ZeroState      ton.ZeroStateIDExt
}

// configureHTTPAPI resolves the built-in HTTP API configuration. RunNode
// supplies its runtime Store and Network dependencies.
func configureHTTPAPI(runOpts *gton.NodeOptions, cfg nodeconfig.Config, runtimeOpts nodeconfig.RuntimeOptions, globalConfig *liteclient.GlobalConfig) (httpapiOptions, error) {
	opts := httpapiOptionsFromConfig(cfg, runtimeOpts)
	runOpts.HTTPAPI = nil
	if !opts.Enabled {
		return opts, nil
	}

	zeroState, err := liteserverZeroStateFromGlobalConfig(globalConfig)
	if err != nil {
		return httpapiOptions{}, err
	}
	opts.ZeroState = zeroState
	runOpts.HTTPAPI = &gton.HTTPAPIOptions{
		ListenAddr:     opts.ListenAddr,
		RequestTimeout: opts.RequestTimeout,
		ZeroState:      opts.ZeroState,
	}
	return opts, nil
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
