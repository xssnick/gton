package node

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/xssnick/gton"
	nodeconfig "github.com/xssnick/gton/cmd/node/config"
	"github.com/xssnick/gton/service/hooks"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/adnl/dht"
	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/tl"
)

func TestLiteServerSendQueueSize(t *testing.T) {
	if liteserverSendQueueSize != 2048 {
		t.Fatalf("liteserver send queue size = %d, want %d", liteserverSendQueueSize, 2048)
	}
}

func TestComposeExtensionsExcludesBuiltInAPIs(t *testing.T) {
	var initialized []string
	factory := func(name string) hooks.ExtensionFactory {
		return func(hooks.Node) (hooks.Extension, error) {
			initialized = append(initialized, name)
			return startupTestExtension{}, nil
		}
	}

	runOpts := gton.NodeOptions{
		Extension: factory("configured"),
		HTTPAPI:   &gton.HTTPAPIOptions{ListenAddr: "127.0.0.1:8081"},
		Liteserver: &gton.LiteserverOptions{
			ListenAddr: "127.0.0.1:7445",
		},
	}
	composeExtensions(&runOpts, []hooks.ExtensionFactory{factory("argument")})

	if runOpts.Extension == nil {
		t.Fatal("expected composed external extension factory")
	}
	if _, err := runOpts.Extension(hooks.Node{}); err != nil {
		t.Fatalf("initialize composed extensions: %v", err)
	}
	if len(initialized) != 2 || initialized[0] != "argument" || initialized[1] != "configured" {
		t.Fatalf("initialized factories = %v, want [argument configured]", initialized)
	}
}

type startupTestExtension struct{}

func (startupTestExtension) Start(context.Context) error { return nil }
func (startupTestExtension) Close(context.Context) error { return nil }
func (startupTestExtension) OnBlockApplied(context.Context, hooks.BlockAppliedEvent) error {
	return nil
}
func (startupTestExtension) OnExternalMessage(context.Context, hooks.ExternalMessageEvent) error {
	return nil
}
func (startupTestExtension) OnBlockReceived(context.Context, hooks.BlockReceivedEvent) error {
	return nil
}

func TestParseNodeFlagsRejectsNegativeArchivePrefetchWindows(t *testing.T) {
	_, _, err := parseNodeFlags([]string{"--archive-prefetch-windows=-1"}, &bytes.Buffer{})
	if err == nil {
		t.Fatal("expected negative archive prefetch windows error")
	}
	if err.Error() != "archive prefetch windows cannot be negative: -1" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestParseNodeFlagsValidatorControlPubkey(t *testing.T) {
	_, commands, err := parseNodeFlags([]string{"--validator-control-pubkey"}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if !commands.validatorControlPubkey {
		t.Fatal("validator control public key command was not enabled")
	}
}

func TestParseNodeFlagsGenesisOverridesAndDHTDescriptor(t *testing.T) {
	options, commands, err := parseNodeFlags([]string{
		"--data-dir", "seeded-data",
		"--global-config-file", "local-global.json",
		"--dht-descriptor",
	}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if options.DataDir != "seeded-data" || options.GlobalConfigFile != "local-global.json" {
		t.Fatalf("startup overrides = %#v", options)
	}
	if !commands.dhtDescriptor {
		t.Fatal("DHT descriptor command was not enabled")
	}
}

func TestStartPprofRejectsOccupiedAddress(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen on pprof test address: %v", err)
	}
	defer listener.Close()

	addr := listener.Addr().String()
	err = startPprof(t.Context(), zerolog.Nop(), addr)
	if err == nil {
		t.Fatal("expected occupied pprof address error")
	}
	if !strings.Contains(err.Error(), "listen pprof on "+addr) {
		t.Fatalf("unexpected error: %v", err)
	}
}

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

func TestWriteDHTDescriptor(t *testing.T) {
	seed := bytes.Repeat([]byte{0x25}, ed25519.SeedSize)
	cfg := nodeconfig.Config{
		ADNL: nodeconfig.ADNL{ExternalAddr: "127.0.0.1:30303"},
		DHT:  nodeconfig.DHT{Key: seed, ListenAddr: "0.0.0.0:30304"},
	}

	var out bytes.Buffer
	if err := writeDHTDescriptor(&out, cfg, "config.json", time.Unix(1_700_000_000, 0)); err != nil {
		t.Fatal(err)
	}
	var descriptor liteclient.DHTNode
	if err := json.Unmarshal(out.Bytes(), &descriptor); err != nil {
		t.Fatal(err)
	}
	global := &liteclient.GlobalConfig{DHT: liteclient.DHTConfig{
		StaticNodes: liteclient.DHTNodes{Nodes: []liteclient.DHTNode{descriptor}},
	}}
	nodes, err := dht.BootstrapNodesFromConfig(global)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 {
		t.Fatalf("parsed DHT nodes = %d", len(nodes))
	}
	if err = nodes[0].CheckSignature(); err != nil {
		t.Fatalf("verify DHT descriptor: %v", err)
	}
	if descriptor.AddrList.Addrs[0].Port != 30304 {
		t.Fatalf("DHT descriptor port = %d", descriptor.AddrList.Addrs[0].Port)
	}
}

func TestWriteValidatorControlPublicKey(t *testing.T) {
	seed := bytes.Repeat([]byte{0x31}, ed25519.SeedSize)
	cfg := nodeconfig.Config{Validator: nodeconfig.Validator{
		Control: nodeconfig.ValidatorControl{Key: seed},
	}}

	var out bytes.Buffer
	if err := writeValidatorControlPublicKey(&out, cfg, "config.json"); err != nil {
		t.Fatalf("write validator control public key: %v", err)
	}

	serialized, err := base64.StdEncoding.DecodeString(strings.TrimSpace(out.String()))
	if err != nil {
		t.Fatal(err)
	}
	var publicKey any
	if _, err = tl.Parse(&publicKey, serialized, true); err != nil {
		t.Fatalf("parse boxed validator control public key: %v", err)
	}
	got, ok := publicKey.(keys.PublicKeyED25519)
	if !ok {
		t.Fatalf("public key type = %T", publicKey)
	}
	want := ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey)
	if !bytes.Equal(got.Key, want) {
		t.Fatalf("public key = %x, want %x", got.Key, want)
	}
}

func TestWriteValidatorControlPublicKeyRejectsMissingKey(t *testing.T) {
	var out bytes.Buffer
	err := writeValidatorControlPublicKey(&out, nodeconfig.Config{}, "config.json")
	if err == nil {
		t.Fatal("missing validator control key was accepted")
	}
	if err.Error() != "validator control key is missing in config.json" {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Len() != 0 {
		t.Fatalf("stdout = %q, want empty output", out.String())
	}
}
