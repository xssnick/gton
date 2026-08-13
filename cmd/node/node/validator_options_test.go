package node

import (
	"bytes"
	"crypto/ed25519"
	"testing"

	nodeconfig "github.com/xssnick/gton/cmd/node/config"
)

func TestConfigureValidatorDisabled(t *testing.T) {
	t.Parallel()

	opts, err := configureValidator(nodeconfig.Validator{})
	if err != nil {
		t.Fatal(err)
	}
	if opts.Enabled {
		t.Fatal("validator was enabled")
	}
}

func TestConfigureValidatorControl(t *testing.T) {
	t.Parallel()

	serverSeed := bytes.Repeat([]byte{0x31}, ed25519.SeedSize)
	clientID := bytes.Repeat([]byte{0x32}, 32)
	opts, err := configureValidator(nodeconfig.Validator{
		Enabled: true,
		Control: nodeconfig.ValidatorControl{
			ListenAddr: "127.0.0.1:3030",
			Key:        serverSeed,
			Clients: []nodeconfig.ValidatorControlClient{{
				ID:          clientID,
				Permissions: 15,
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !opts.Enabled || !opts.Extension.EnableGroups {
		t.Fatalf("validator startup options = %+v", opts)
	}
	if opts.Extension.Keys != nil {
		t.Fatal("validator signing keys must be loaded from storage after it is opened")
	}
	if opts.Control.listenAddr != "127.0.0.1:3030" || len(opts.Control.clients) != 1 {
		t.Fatalf("validator control options = %+v", opts.Control)
	}
	if opts.Control.clients[0].permissions != 15 ||
		opts.Control.clients[0].id != validatorControlTestID(clientID) {
		t.Fatalf("validator control client = %+v", opts.Control.clients[0])
	}
	if !bytes.Equal(opts.Control.serverKey.Seed(), serverSeed) {
		t.Fatal("validator control server key does not match its configured seed")
	}
}

func TestConfigureValidatorRejectsInvalidControl(t *testing.T) {
	t.Parallel()

	serverSeed := bytes.Repeat([]byte{0x41}, ed25519.SeedSize)
	clientID := bytes.Repeat([]byte{0x42}, 32)
	tests := []nodeconfig.Validator{
		{Enabled: true},
		{Enabled: true, Control: nodeconfig.ValidatorControl{ListenAddr: "bad", Key: serverSeed, Clients: []nodeconfig.ValidatorControlClient{{ID: clientID, Permissions: 15}}}},
		{Enabled: true, Control: nodeconfig.ValidatorControl{ListenAddr: "127.0.0.1:3030", Key: []byte{0x01}, Clients: []nodeconfig.ValidatorControlClient{{ID: clientID, Permissions: 15}}}},
		{Enabled: true, Control: nodeconfig.ValidatorControl{ListenAddr: "127.0.0.1:3030", Key: serverSeed}},
		{Enabled: true, Control: nodeconfig.ValidatorControl{ListenAddr: "127.0.0.1:3030", Key: serverSeed, Clients: []nodeconfig.ValidatorControlClient{{ID: []byte{0x01}, Permissions: 15}}}},
		{Enabled: true, Control: nodeconfig.ValidatorControl{ListenAddr: "127.0.0.1:3030", Key: serverSeed, Clients: []nodeconfig.ValidatorControlClient{{ID: make([]byte, 32), Permissions: 15}}}},
		{Enabled: true, Control: nodeconfig.ValidatorControl{ListenAddr: "127.0.0.1:3030", Key: serverSeed, Clients: []nodeconfig.ValidatorControlClient{{ID: clientID}}}},
		{Enabled: true, Control: nodeconfig.ValidatorControl{ListenAddr: "127.0.0.1:3030", Key: serverSeed, Clients: []nodeconfig.ValidatorControlClient{{ID: clientID, Permissions: 1}, {ID: bytes.Clone(clientID), Permissions: 2}}}},
	}

	for i := range tests {
		if _, err := configureValidator(tests[i]); err == nil {
			t.Fatalf("invalid validator control config %d was accepted", i)
		}
	}
}

func validatorControlTestID(value []byte) [32]byte {
	var id [32]byte
	copy(id[:], value)

	return id
}
