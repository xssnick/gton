package p2p

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/adnl/address"
	"github.com/xssnick/tonutils-go/adnl/dht"
	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/tl"
)

const dhtUnknownNetworkID = int32(-1)

func (n *Node) startDHTServer(ctx context.Context, cfg *liteclient.GlobalConfig) error {
	if len(n.dhtPrivKey) == 0 {
		_, priv, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			return fmt.Errorf("generate DHT ADNL key: %w", err)
		}
		n.dhtPrivKey = priv
	}

	dhtGateway := adnl.NewGateway(n.dhtPrivKey)
	if err := dhtGateway.StartServer(n.dhtListenAddr); err != nil {
		return fmt.Errorf("start DHT gateway: %w", err)
	}

	publishedAddr, err := n.dhtPublishedAddress()
	if err != nil {
		_ = dhtGateway.Close()
		return err
	}
	dhtGateway.SetAddressList([]address.Address{publishedAddr})
	addrList := dhtGateway.GetAddressList()

	store := dht.NewMemoryValueStore(dhtServerStoreMaxKeys)
	cfg = dhtServerGlobalConfig(cfg)
	server, err := dht.NewServerFromConfig(dhtGateway, n.dhtPrivKey, cfg, store)
	if err != nil {
		_ = store.Close()
		_ = dhtGateway.Close()
		return fmt.Errorf("init DHT server: %w", err)
	}

	publishedDial, err := address.DialString(publishedAddr)
	if err != nil {
		publishedDial = "<invalid>"
	}
	n.log.Info().
		Str("listen_addr", n.dhtListenAddr).
		Str("published_addr", publishedDial).
		Str("adnl_id", dhtADNLIDBase64(n.dhtPrivKey)).
		Msg("started DHT server")

	n.dhtGateway = dhtGateway
	n.dhtServer = server
	n.dht = &serverDHTBackend{Server: server}

	n.runAsync(func() {
		n.runDHTServerPublishLoop(ctx, addrList)
	})

	return nil
}

func dhtServerGlobalConfig(cfg *liteclient.GlobalConfig) *liteclient.GlobalConfig {
	if cfg == nil || cfg.DHT.NetworkID != nil {
		return cfg
	}

	// The reference node maps the current dht.config.global format, which has no
	// network_id field, to -1 before creating a DHT server.
	networkID := dhtUnknownNetworkID
	cfgCopy := *cfg
	cfgCopy.DHT.NetworkID = &networkID
	return &cfgCopy
}

func (n *Node) dhtPublishedAddress() (address.Address, error) {
	_, portStr, err := net.SplitHostPort(n.dhtListenAddr)
	if err != nil {
		return nil, fmt.Errorf("parse DHT listen address: %w", err)
	}

	ip := append(net.IP(nil), n.externalIP...)
	if ip == nil || ip.IsUnspecified() {
		return nil, fmt.Errorf("adnl.external_addr is required for DHT server")
	}

	parsed, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		return nil, fmt.Errorf("parse DHT listen port: %w", err)
	}
	port := int(parsed)
	if port == 0 {
		return nil, fmt.Errorf("DHT public port is required when listening on %q", n.dhtListenAddr)
	}

	return address.NewAddress(ip, int32(port))
}

func (n *Node) runDHTServerPublishLoop(ctx context.Context, addrList address.List) {
	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if n.IsOffline() {
				return
			}
			delay := publicAnnounceTTL / 2
			if err := n.publishDHTServerAddress(ctx, addrList); err != nil {
				n.log.Warn().Err(err).Msg("failed to publish DHT server address")
				delay = publicAnnounceRetryDelay
			}
			timer.Reset(delay)
		}
	}
}

func (n *Node) publishDHTServerAddress(ctx context.Context, addrList address.List) error {
	if n.IsOffline() {
		return ErrOffline
	}
	if n.dhtServer == nil || len(n.dhtPrivKey) == 0 {
		return nil
	}

	storeCtx, cancel := context.WithTimeout(ctx, dhtStoreTimeout)
	defer cancel()

	_, _, err := n.dhtServer.StoreAddress(storeCtx, addrList, publicAnnounceTTL, n.dhtPrivKey)
	return err
}

func dhtADNLIDBase64(key ed25519.PrivateKey) string {
	if len(key) == 0 {
		return ""
	}

	id, err := tl.Hash(keys.PublicKeyED25519{Key: key.Public().(ed25519.PublicKey)})
	if err != nil {
		return ""
	}
	return base64.StdEncoding.EncodeToString(id)
}
