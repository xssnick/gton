package liteserver

import (
	"context"
	"crypto/ed25519"
	"reflect"

	core "github.com/xssnick/gton/api/liteserver"
	"github.com/xssnick/gton/service/hooks"

	"github.com/xssnick/tonutils-go/ton"
)

type ExtensionConfig struct {
	PrivateKey              ed25519.PrivateKey
	ListenAddr              string
	NonFinal                bool
	AllowDuplicateExternals bool
	ZeroState               ton.ZeroStateIDExt
	RequestLimits           core.RequestLimitOptions
	QueryConcurrency        int
}

type Extension struct {
	server *core.Server
}

var _ hooks.Extension = (*Extension)(nil)

func NewExtension(node hooks.Node, cfg ExtensionConfig) (*Extension, error) {
	queryObserver, err := extensionQueryObserver(node.Metrics)
	if err != nil {
		return nil, err
	}

	server, err := core.New(core.Options{
		Logger:                  &node.Logger,
		Store:                   node.Store,
		MessageSender:           node.Network,
		TVM:                     node.TVM,
		CheckExternalMessage:    node.Network.CheckExternalMessage,
		QueryObserver:           queryObserver,
		PrivateKey:              cfg.PrivateKey,
		ListenAddr:              cfg.ListenAddr,
		NonFinal:                cfg.NonFinal,
		AllowDuplicateExternals: cfg.AllowDuplicateExternals,
		ZeroState:               cfg.ZeroState,
		RequestLimits:           cfg.RequestLimits,
		QueryConcurrency:        cfg.QueryConcurrency,
	})
	if err != nil {
		return nil, err
	}

	return &Extension{server: server}, nil
}

// extensionQueryObserver keeps the released untyped extension capability
// compatible without leaking reflection into the core liteserver package.
func extensionQueryObserver(metrics any) (core.QueryObserver, error) {
	if metrics == nil {
		return nil, nil
	}

	value := reflect.ValueOf(metrics)
	if value.Kind() == reflect.Pointer && value.IsNil() {
		return nil, nil
	}

	registry, ok := metrics.(core.MetricsRegistry)
	if !ok {
		return nil, nil
	}

	return core.NewQueryObserver(registry)
}

func (e *Extension) Start(ctx context.Context) error {
	return e.server.Start(ctx)
}

func (e *Extension) Close(ctx context.Context) error {
	if err := e.server.Close(); err != nil {
		return err
	}

	done := make(chan struct{})
	go func() {
		e.server.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *Extension) OnBlockApplied(context.Context, hooks.BlockAppliedEvent) error {
	return nil
}

func (e *Extension) OnExternalMessage(context.Context, hooks.ExternalMessageEvent) error {
	return nil
}

func (e *Extension) OnBlockReceived(context.Context, hooks.BlockReceivedEvent) error {
	return nil
}
