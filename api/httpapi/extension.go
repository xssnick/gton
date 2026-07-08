package httpapi

import (
	"context"
	"time"

	"github.com/xssnick/gton/service/hooks"

	"github.com/xssnick/tonutils-go/ton"
)

type ExtensionConfig struct {
	ListenAddr     string
	RequestTimeout time.Duration
	ZeroState      ton.ZeroStateIDExt
}

type Extension struct {
	server *Server
}

var _ hooks.Extension = (*Extension)(nil)

func NewExtension(node hooks.Node, cfg ExtensionConfig) (*Extension, error) {
	server, err := New(Options{
		Logger:         &node.Logger,
		Store:          node.Store,
		Network:        node.Network,
		TVM:            node.TVM,
		ListenAddr:     cfg.ListenAddr,
		RequestTimeout: cfg.RequestTimeout,
		ZeroState:      cfg.ZeroState,
	})
	if err != nil {
		return nil, err
	}
	return &Extension{server: server}, nil
}

func (e *Extension) Start(ctx context.Context) error {
	return e.server.Start(ctx)
}

func (e *Extension) Close(ctx context.Context) error {
	if err := e.server.Close(ctx); err != nil {
		return err
	}
	e.server.Wait()
	return nil
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
