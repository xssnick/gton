package liteserver

import (
	"context"
	"crypto/ed25519"

	"github.com/xssnick/gton/service/hooks"

	"github.com/xssnick/tonutils-go/ton"
)

type ExtensionConfig struct {
	PrivateKey    ed25519.PrivateKey
	ListenAddr    string
	NonFinal      bool
	ZeroState     ton.ZeroStateIDExt
	RequestLimits RequestLimitOptions
}

type Extension struct {
	server *Server
}

var _ hooks.Extension = (*Extension)(nil)

func NewExtension(node hooks.Node, cfg ExtensionConfig) (*Extension, error) {
	queryObserver, err := liteserverQueryObserver(node.Metrics)
	if err != nil {
		return nil, err
	}

	server, err := New(Options{
		Logger:        &node.Logger,
		Store:         node.Store,
		MessageSender: node.Network,
		QueryObserver: queryObserver,
		PrivateKey:    cfg.PrivateKey,
		ListenAddr:    cfg.ListenAddr,
		NonFinal:      cfg.NonFinal,
		ZeroState:     cfg.ZeroState,
		RequestLimits: cfg.RequestLimits,
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
