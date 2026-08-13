package node

import (
	"bytes"
	"crypto/ed25519"
	"testing"

	nodeconfig "github.com/xssnick/gton/cmd/node/config"
)

func TestConfigureCollatorIdentityUsesNodeADNLSeed(t *testing.T) {
	seed := bytes.Repeat([]byte{0x31}, ed25519.SeedSize)
	identity, err := configureCollatorIdentity(seed)
	if err != nil {
		t.Fatal(err)
	}
	ids := identity.keys.KeyIDs()
	if len(ids) != 1 || ids[0] != identity.keyID || identity.keyID == ([32]byte{}) {
		t.Fatalf("collator identity = %x keys=%x", identity.keyID, ids)
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
