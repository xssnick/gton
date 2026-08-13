package node

import (
	"crypto/ed25519"
	"errors"
	"fmt"
	"net"
	"strings"

	nodeconfig "github.com/xssnick/gton/cmd/node/config"
	"github.com/xssnick/gton/service/validator"
)

type validatorControlClient struct {
	id          [32]byte
	permissions uint32
}

type validatorControlOptions struct {
	listenAddr string
	serverKey  ed25519.PrivateKey
	clients    []validatorControlClient
}

type validatorOptions struct {
	Enabled   bool
	Control   validatorControlOptions
	Extension validator.Options
	Runtime   validator.SharedRuntimeOptions
}

func configureValidator(cfg nodeconfig.Validator) (validatorOptions, error) {
	opts := validatorOptions{Enabled: cfg.Enabled}
	if !cfg.Enabled {
		return opts, nil
	}

	control, err := configureValidatorControl(cfg.Control)
	if err != nil {
		return validatorOptions{}, err
	}

	opts.Control = control
	opts.Extension.EnableGroups = true

	return opts, nil
}

func configureValidatorControl(cfg nodeconfig.ValidatorControl) (validatorControlOptions, error) {
	listenAddr := strings.TrimSpace(cfg.ListenAddr)
	if listenAddr == "" {
		return validatorControlOptions{}, errors.New("validator.control.listen_addr is required")
	}
	if _, _, err := net.SplitHostPort(listenAddr); err != nil {
		return validatorControlOptions{}, fmt.Errorf("invalid validator.control.listen_addr %q: %w", listenAddr, err)
	}
	if len(cfg.Key) != ed25519.SeedSize {
		return validatorControlOptions{}, fmt.Errorf(
			"validator.control.key must contain a %d-byte Ed25519 seed",
			ed25519.SeedSize,
		)
	}
	if len(cfg.Clients) == 0 {
		return validatorControlOptions{}, errors.New("validator.control.clients must contain at least one trusted client")
	}

	clients := make([]validatorControlClient, len(cfg.Clients))
	seen := make(map[[32]byte]struct{}, len(cfg.Clients))
	for i := range cfg.Clients {
		client := cfg.Clients[i]
		if len(client.ID) != 32 {
			return validatorControlOptions{}, fmt.Errorf(
				"validator.control.clients[%d].id must contain a 32-byte key ID",
				i,
			)
		}
		if client.Permissions == 0 {
			return validatorControlOptions{}, fmt.Errorf(
				"validator.control.clients[%d].permissions must be non-zero",
				i,
			)
		}
		var id [32]byte
		copy(id[:], client.ID)
		if id == ([32]byte{}) {
			return validatorControlOptions{}, fmt.Errorf(
				"validator.control.clients[%d].id must be non-zero",
				i,
			)
		}
		if _, duplicate := seen[id]; duplicate {
			return validatorControlOptions{}, fmt.Errorf(
				"validator.control.clients[%d].id is duplicated",
				i,
			)
		}
		seen[id] = struct{}{}

		clients[i] = validatorControlClient{
			id:          id,
			permissions: client.Permissions,
		}
	}

	return validatorControlOptions{
		listenAddr: listenAddr,
		serverKey:  ed25519.NewKeyFromSeed(cfg.Key),
		clients:    clients,
	}, nil
}
