package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xssnick/gton/genesis"
	adnladdress "github.com/xssnick/tonutils-go/adnl/address"
	"github.com/xssnick/tonutils-go/adnl/dht"
	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/tl"
)

func TestRunWithoutArgumentsCreatesTemplateThenGenesis(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := run(context.Background(), nil, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), "created genesis template") {
		t.Fatalf("stdout = %q", stdout.String())
	}
	for _, path := range []string{genesis.DefaultDataPath, genesis.DefaultGlobalConfigPath, genesis.DefaultLockPath} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("first run created %s", path)
		}
	}
	if _, err := os.Stat("config.json"); !os.IsNotExist(err) {
		t.Fatalf("genesis command touched node config: %v", err)
	}

	spec, _, err := genesis.LoadSpec(genesis.DefaultGenesisPath)
	if err != nil {
		t.Fatal(err)
	}
	completeTemplate(t, &spec)
	raw, err := json.MarshalIndent(spec, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(genesis.DefaultGenesisPath, append(raw, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}

	stdout.Reset()
	stderr.Reset()
	if err = run(context.Background(), nil, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), "genesis ready") {
		t.Fatalf("stdout = %q", stdout.String())
	}
	for _, path := range []string{genesis.DefaultDataPath, genesis.DefaultGlobalConfigPath, genesis.DefaultLockPath} {
		if _, err = os.Stat(filepath.Clean(path)); err != nil {
			t.Fatalf("stat %s: %v", path, err)
		}
	}
	if _, err = os.Stat("config.json"); !os.IsNotExist(err) {
		t.Fatalf("genesis command touched node config: %v", err)
	}
}

func TestRunRejectsMissingExplicitGenesis(t *testing.T) {
	dir := t.TempDir()
	err := run(
		context.Background(),
		[]string{"--genesis", filepath.Join(dir, "missing.json")},
		&bytes.Buffer{},
		&bytes.Buffer{},
	)
	if err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("error = %v", err)
	}
}

func completeTemplate(t *testing.T, spec *genesis.Spec) {
	t.Helper()

	spec.GenesisTime = 1_700_000_000
	for i := range spec.Validators {
		validatorKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{byte(i + 1)}, ed25519.SeedSize))
		adnlKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{byte(i + 21)}, ed25519.SeedSize))
		adnlID, err := tl.Hash(keys.PublicKeyED25519{Key: adnlKey.Public().(ed25519.PublicKey)})
		if err != nil {
			t.Fatal(err)
		}
		spec.Validators[i].PublicKey = base64.StdEncoding.EncodeToString(validatorKey.Public().(ed25519.PublicKey))
		spec.Validators[i].ADNLID = base64.StdEncoding.EncodeToString(adnlID)
	}

	dhtKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{91}, ed25519.SeedSize))
	dhtPublic := dhtKey.Public().(ed25519.PublicKey)
	ip := net.IPv4(127, 0, 0, 1).To4()
	addresses := &adnladdress.List{
		Addresses: []adnladdress.Address{&adnladdress.UDP{IP: ip, Port: 30304}},
		Version:   1,
	}
	node, err := dht.BuildSignedNode(keys.PublicKeyED25519{Key: dhtPublic}, addresses, 1, -1, dhtKey)
	if err != nil {
		t.Fatal(err)
	}
	spec.DHTNodes = []liteclient.DHTNode{{
		Type: "dht.node",
		ID:   liteclient.ServerID{Type: "pub.ed25519", Key: base64.StdEncoding.EncodeToString(dhtPublic)},
		AddrList: liteclient.DHTAddressList{
			Type:    "adnl.addressList",
			Addrs:   []liteclient.DHTAddress{{Type: "adnl.address.udp", IP: int(int32(binary.BigEndian.Uint32(ip))), Port: 30304}},
			Version: 1,
		},
		Version:   1,
		Signature: base64.StdEncoding.EncodeToString(node.Signature),
	}}
}
