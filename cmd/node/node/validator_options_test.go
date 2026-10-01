package node

import (
	"bytes"
	"crypto/ed25519"
	"testing"
	"time"

	nodeconfig "github.com/xssnick/gton/cmd/node/config"
	"github.com/xssnick/gton/service/validator/keyring"
)

func TestValidateValidatorADNL(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	localID := [32]byte{1}
	active := keyring.KeyInfo{
		ID: [32]byte{2}, Permanent: true, HasADNL: true,
		ADNLID: localID, PermanentExpireAt: uint32(now.Unix() + 100),
		ADNLExpireAt: uint32(now.Unix() + 100),
	}
	type testCase struct {
		name      string
		entry     keyring.KeyInfo
		wantError bool
	}
	other := active
	other.ADNLID = [32]byte{3}
	expiredPermanent := other
	expiredPermanent.PermanentExpireAt = uint32(now.Unix())
	expiredADNL := other
	expiredADNL.ADNLExpireAt = uint32(now.Unix())
	unbound := other
	unbound.HasADNL = false
	for _, test := range []testCase{
		{name: "preserved identity", entry: active},
		{name: "active different identity", entry: other, wantError: true},
		{name: "expired signing key", entry: expiredPermanent},
		{name: "expired ADNL binding", entry: expiredADNL},
		{name: "unbound key", entry: unbound},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateValidatorADNL([]keyring.KeyInfo{test.entry}, localID, now)
			if (err != nil) != test.wantError {
				t.Fatalf("validate ADNL = %v, want error %v", err, test.wantError)
			}
		})
	}
}

func TestConfigureValidatorDisabled(t *testing.T) {
	t.Parallel()

	opts, err := configureValidator(nodeconfig.Validator{}, 3)
	if err != nil {
		t.Fatal(err)
	}
	if opts.Enabled {
		t.Fatal("validator was enabled")
	}
	if opts.Runtime.Groups.MaximalVerticalSeqno != 3 {
		t.Fatalf("maximal vertical seqno = %d, want 3", opts.Runtime.Groups.MaximalVerticalSeqno)
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
	}, 3)
	if err != nil {
		t.Fatal(err)
	}
	if !opts.Enabled || !opts.Extension.EnableGroups {
		t.Fatalf("validator startup options = %+v", opts)
	}
	if opts.Runtime.Groups.MaximalVerticalSeqno != 3 {
		t.Fatalf("maximal vertical seqno = %d, want 3", opts.Runtime.Groups.MaximalVerticalSeqno)
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
		if _, err := configureValidator(tests[i], 3); err == nil {
			t.Fatalf("invalid validator control config %d was accepted", i)
		}
	}
}

func validatorControlTestID(value []byte) [32]byte {
	var id [32]byte
	copy(id[:], value)

	return id
}
