package p2p

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"time"

	"github.com/xssnick/tonutils-go/adnl"
	adnladdr "github.com/xssnick/tonutils-go/adnl/address"
	"github.com/xssnick/tonutils-go/adnl/dht"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/liteclient"
)

type dhtBackend interface {
	FindOverlayNodes(ctx context.Context, overlayKey []byte, continuation ...*dht.Continuation) (*overlay.NodesList, *dht.Continuation, error)
	FindAddresses(ctx context.Context, key []byte) (*adnladdr.List, ed25519.PublicKey, error)
	FindValue(ctx context.Context, key *dht.Key, continuation ...*dht.Continuation) (*dht.Value, *dht.Continuation, error)
	StoreAddress(ctx context.Context, addresses adnladdr.List, ttl time.Duration, ownerKey ed25519.PrivateKey) (storedCount int, idKey []byte, err error)
	StoreOverlayNodes(ctx context.Context, overlayKey []byte, nodes *overlay.NodesList, ttl time.Duration) (storedCount int, idKey []byte, err error)
}

var _ dhtBackend = (*dht.Client)(nil)
var _ dhtBackend = (*dht.Server)(nil)

func (n *Node) startDHT(ctx context.Context, cfg *liteclient.GlobalConfig) error {
	if n.dhtListenAddr != "" {
		return n.startDHTServer(ctx, cfg)
	}
	return n.startDHTClient(cfg)
}

func (n *Node) startDHTClient(cfg *liteclient.GlobalConfig) error {
	dhtGateway := adnl.NewGateway(n.privKey)

	if err := dhtGateway.StartClient(); err != nil {
		return fmt.Errorf("start DHT gateway: %w", err)
	}

	client, err := dht.NewClientFromConfig(dhtGateway, cfg)
	if err != nil {
		_ = dhtGateway.Close()
		return fmt.Errorf("init DHT client: %w", err)
	}

	n.dhtGateway = dhtGateway
	n.dhtClient = client
	n.dht = client

	return nil
}
