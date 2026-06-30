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

func TestWriteLiteServerPublicKey(t *testing.T) {
	seed := bytes.Repeat([]byte{0x42}, ed25519.SeedSize)
	cfg := nodeconfig.Config{Lite: nodeconfig.Lite{Key: seed}}

	var out bytes.Buffer
	if err := writeLiteServerPublicKey(&out, cfg, "config.json"); err != nil {
		t.Fatalf("write liteserver public key: %v", err)
	}

	publicKey := ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey)
	want := base64.StdEncoding.EncodeToString(publicKey) + "\n"
	if out.String() != want {
		t.Fatalf("stdout = %q, want %q", out.String(), want)
	}
}

func TestWriteADNLID(t *testing.T) {
	seed := bytes.Repeat([]byte{0x24}, ed25519.SeedSize)
	cfg := nodeconfig.Config{ADNL: nodeconfig.ADNL{Key: seed}}

	var out bytes.Buffer
	if err := writeADNLID(&out, cfg, "config.json"); err != nil {
		t.Fatalf("write ADNL id: %v", err)
	}

	privateKey := ed25519.NewKeyFromSeed(seed)
	id, err := tl.Hash(keys.PublicKeyED25519{Key: privateKey.Public().(ed25519.PublicKey)})
	if err != nil {
		t.Fatalf("hash ADNL id: %v", err)
	}
	want := base64.StdEncoding.EncodeToString(id) + "\n"
	if out.String() != want {
		t.Fatalf("stdout = %q, want %q", out.String(), want)
	}
}
