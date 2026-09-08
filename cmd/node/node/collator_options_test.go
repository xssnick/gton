package node

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"testing"

	nodeconfig "github.com/xssnick/gton/cmd/node/config"
	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/tl"
)

func TestConfigureCollatorIdentityUsesNodeADNLSeed(t *testing.T) {
	seed := bytes.Repeat([]byte{0x31}, ed25519.SeedSize)
	identity, err := configureCollatorIdentity(nodeconfig.Config{ADNL: nodeconfig.ADNL{Key: seed}})
	if err != nil {
		t.Fatal(err)
	}
	ids := identity.keys.KeyIDs()
	if len(ids) != 1 || ids[0] != identity.keyID || identity.keyID == ([32]byte{}) {
		t.Fatalf("collator identity = %x keys=%x", identity.keyID, ids)
	}
}

func TestDedicatedConsensusIdentity(t *testing.T) {
	mainSeed := bytes.Repeat([]byte{0x31}, ed25519.SeedSize)
	privateSeed := bytes.Repeat([]byte{0x32}, ed25519.SeedSize)
	cfg := nodeconfig.Config{
		ADNL: nodeconfig.ADNL{Key: mainSeed},
		ConsensusADNL: &nodeconfig.ConsensusADNL{
			Enabled: true,
			ADNL:    nodeconfig.ADNL{Key: privateSeed},
		},
	}
	identity, err := configureCollatorIdentity(cfg)
	if err != nil {
		t.Fatal(err)
	}
	privateKey := ed25519.NewKeyFromSeed(privateSeed)
	wantID, err := tl.Hash(keys.PublicKeyED25519{Key: privateKey.Public().(ed25519.PublicKey)})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(identity.keyID[:], wantID) {
		t.Fatalf("consensus identity = %x, want %x", identity.keyID, wantID)
	}
	payload := []byte("collator candidate")
	signature, err := identity.keys.Sign(identity.keyID, payload)
	if err != nil {
		t.Fatal(err)
	}
	if !ed25519.Verify(privateKey.Public().(ed25519.PublicKey), payload, signature) {
		t.Fatal("collator signed with a key other than the dedicated ADNL key")
	}

	var output bytes.Buffer
	if err = writeADNLID(&output, consensusADNL(cfg), "config.json"); err != nil {
		t.Fatal(err)
	}
	if want := base64.StdEncoding.EncodeToString(wantID) + "\n"; output.String() != want {
		t.Fatalf("printed consensus ID = %q, want %q", output.String(), want)
	}
	output.Reset()
	if err = writeADNLID(&output, cfg.ADNL, "config.json"); err != nil {
		t.Fatal(err)
	}
	if output.String() == base64.StdEncoding.EncodeToString(wantID)+"\n" {
		t.Fatal("ordinary ADNL command printed the consensus identity")
	}
}

func TestEnabledConsensusIdentityRequiresKey(t *testing.T) {
	cfg := nodeconfig.Config{
		ADNL:          nodeconfig.ADNL{Key: bytes.Repeat([]byte{0x31}, ed25519.SeedSize)},
		ConsensusADNL: &nodeconfig.ConsensusADNL{Enabled: true},
	}
	if _, err := configureCollatorIdentity(cfg); err == nil {
		t.Fatal("explicit consensus network silently used the ordinary key")
	}
}

func TestDisabledConsensusIdentityUsesNodeKey(t *testing.T) {
	mainSeed := bytes.Repeat([]byte{0x31}, ed25519.SeedSize)
	mainKey := ed25519.NewKeyFromSeed(mainSeed)
	wantID, err := tl.Hash(keys.PublicKeyED25519{Key: mainKey.Public().(ed25519.PublicKey)})
	if err != nil {
		t.Fatal(err)
	}

	for _, privateSeed := range [][]byte{nil, bytes.Repeat([]byte{0x32}, ed25519.SeedSize)} {
		cfg := nodeconfig.Config{
			ADNL: nodeconfig.ADNL{Key: mainSeed},
			ConsensusADNL: &nodeconfig.ConsensusADNL{
				ADNL: nodeconfig.ADNL{Key: privateSeed},
			},
		}
		identity, err := configureCollatorIdentity(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(identity.keyID[:], wantID) {
			t.Fatalf("disabled consensus identity = %x, want primary %x", identity.keyID, wantID)
		}
		payload := []byte("collator candidate")
		signature, err := identity.keys.Sign(identity.keyID, payload)
		if err != nil {
			t.Fatal(err)
		}
		if !ed25519.Verify(mainKey.Public().(ed25519.PublicKey), payload, signature) {
			t.Fatal("disabled consensus network changed the signing key")
		}

		var output bytes.Buffer
		if err = writeADNLID(&output, consensusADNL(cfg), "config.json"); err != nil {
			t.Fatal(err)
		}
		if want := base64.StdEncoding.EncodeToString(wantID) + "\n"; output.String() != want {
			t.Fatalf("printed disabled consensus ID = %q, want %q", output.String(), want)
		}
	}
}

func TestConfigureStandaloneValidatorPolicy(t *testing.T) {
	open, err := configureStandaloneValidatorPolicy(nodeconfig.CollatorValidatorAllowlist{})
	if err != nil {
		t.Fatal(err)
	}
	if !open.allowAll || len(open.allowed) != 0 {
		t.Fatalf("disabled allowlist policy = %+v", open)
	}

	id := bytes.Repeat([]byte{0x42}, 32)
	restricted, err := configureStandaloneValidatorPolicy(nodeconfig.CollatorValidatorAllowlist{
		Enabled: true,
		ADNLIDs: [][]byte{id},
	})
	if err != nil {
		t.Fatal(err)
	}
	var key [32]byte
	copy(key[:], id)
	if restricted.allowAll || len(restricted.allowed) != 1 {
		t.Fatalf("enabled allowlist policy = %+v", restricted)
	}
	if _, exists := restricted.allowed[key]; !exists {
		t.Fatal("configured validator ADNL id is absent")
	}
}

func TestConfigureStandaloneValidatorPolicyRejectsUnsafeValues(t *testing.T) {
	tests := []nodeconfig.CollatorValidatorAllowlist{
		{Enabled: true},
		{Enabled: true, ADNLIDs: [][]byte{{1}}},
		{Enabled: true, ADNLIDs: [][]byte{make([]byte, 32)}},
		{Enabled: true, ADNLIDs: [][]byte{bytes.Repeat([]byte{1}, 32), bytes.Repeat([]byte{1}, 32)}},
	}
	for i := range tests {
		if _, err := configureStandaloneValidatorPolicy(tests[i]); err == nil {
			t.Fatalf("unsafe policy %d accepted", i)
		}
	}
}
