package node

import (
	"context"
	"errors"
	"fmt"

	"github.com/xssnick/gton/service/hooks"
	"github.com/xssnick/gton/service/validator/keyring"
	"github.com/xssnick/gton/service/validator/validatorcontrol"
)

type validatorControlExtension struct {
	server *validatorcontrol.Server
}

func newValidatorControlFactory(
	options validatorControlOptions,
	keys *keyring.Keyring,
	localADNLID [32]byte,
) hooks.ExtensionFactory {
	return func(node hooks.Node) (hooks.Extension, error) {
		if node.Store == nil {
			return nil, errors.New("validator control composition: node store is required")
		}
		if keys == nil {
			return nil, errors.New("validator control composition: keyring is required")
		}

		clients := make([]validatorcontrol.TrustedClient, len(options.clients))
		for i := range options.clients {
			clients[i] = validatorcontrol.TrustedClient{
				ID:          options.clients[i].id,
				Permissions: options.clients[i].permissions,
			}
		}

		server, err := validatorcontrol.New(validatorcontrol.Options{
			ListenAddress:  options.listenAddr,
			ServerKey:      options.serverKey,
			TrustedClients: clients,
			Keys:           keys,
			LocalADNLID:    localADNLID,
			State:          node.Store,
			Logger:         node.Logger.With().Str("component", "validator_control").Logger(),
		})
		if err != nil {
			return nil, fmt.Errorf("validator control composition: %w", err)
		}

		return &validatorControlExtension{server: server}, nil
	}
}

func (e *validatorControlExtension) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := e.server.Start(); err != nil {
		return err
	}

	go func() {
		<-ctx.Done()
		_ = e.server.Close()
	}()

	return nil
}

func (e *validatorControlExtension) Close(ctx context.Context) error {
	done := make(chan error, 1)
	go func() {
		done <- e.server.Close()
	}()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (*validatorControlExtension) OnBlockApplied(context.Context, hooks.BlockAppliedEvent) error {
	return nil
}

func (*validatorControlExtension) OnExternalMessage(context.Context, hooks.ExternalMessageEvent) error {
	return nil
}

func (*validatorControlExtension) OnBlockReceived(context.Context, hooks.BlockReceivedEvent) error {
	return nil
}
