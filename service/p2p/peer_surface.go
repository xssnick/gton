package p2p

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"net"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/adnl/address"
	"github.com/xssnick/tonutils-go/adnl/dht"
	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/overlay"
)

func (n *Node) isPublicServer() bool {
	return n.listenAddr != ""
}

func (n *Node) startGateway() error {
	// TODO: optimize specific workers to own worker pools later.
	listenThreads := runtime.GOMAXPROCS(0) * 2

	if n.isPublicServer() {
		if err := n.configurePublicAddress(); err != nil {
			return err
		}
		return n.gateway.StartServer(n.listenAddr, listenThreads)
	}

	return n.gateway.StartClient(listenThreads)
}

func (n *Node) configurePublicAddress() error {
	host, portStr, err := net.SplitHostPort(n.listenAddr)
	if err != nil {
		return fmt.Errorf("parse listen address: %w", err)
	}

	ip := append(net.IP(nil), n.externalIP...)
	if len(ip) == 0 {
		ip = net.ParseIP(host)
	}
	if ip == nil || ip.IsUnspecified() {
		return nil
	}

	port := int(n.externalPort)
	if port == 0 {
		parsed, err := strconv.ParseUint(portStr, 10, 16)
		if err != nil {
			return fmt.Errorf("parse listen port: %w", err)
		}
		port = int(parsed)
	}

	addr, err := address.NewAddress(ip, int32(port))
	if err != nil {
		return err
	}
	n.gateway.SetAddressList([]address.Address{addr})
	return nil
}

func (n *Node) handleInboundPeer(peer adnl.Peer) error {
	if !n.beginInbound() {
		return context.Canceled
	}
	defer n.finishInbound()

	pooled, fresh, err := n.pool.wrap(peer)
	if err != nil {
		return err
	}
	if !fresh {
		return nil
	}

	for _, sub := range n.subscriptionsSnapshot() {
		sub.attachPooledPeer(pooled, nil)
	}
	return nil
}

func (n *Node) attachSubscriptionPeers(sub *overlaySubscription) {
	for _, peer := range n.pool.snapshot() {
		sub.attachPooledPeer(peer, nil)
	}
}

func (n *Node) runEventLoop(ctx context.Context) {
	defer close(n.events)

	for {
		event, ok := n.eventQueue.Pop(ctx)
		if !ok {
			return
		}

		select {
		case <-ctx.Done():
			return
		case n.events <- event:
		}
	}
}

func (n *Node) runAnnounceLoop(ctx context.Context) {
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
			delay := publicAnnounceEvery
			if err := n.announceSelf(ctx); err != nil && ctx.Err() == nil && !errors.Is(err, context.Canceled) {
				n.log.Warn().Err(err).Msg("failed to announce ordinary-node surface")
				delay = publicAnnounceRetryDelay
			}
			timer.Reset(delay)
		}
	}
}

func (n *Node) announceSelf(ctx context.Context) error {
	if n.IsOffline() {
		return ErrOffline
	}
	if !n.isPublicServer() || n.dht == nil {
		return nil
	}

	addrList := n.gateway.GetAddressList()
	if len(addrList.Addresses) == 0 {
		n.log.Warn().
			Str("listen_addr", n.listenAddr).
			Msg("skipping public announce because gateway has no routable address list")
		return nil
	}

	var errs []error

	err := n.storeAddressWithRetry(ctx, addrList)
	if err != nil {
		errs = append(errs, fmt.Errorf("store address: %w", err))
	}

	for _, sub := range n.subscriptionsSnapshot() {
		if !sub.isActive() {
			continue
		}
		if !sub.spec.Announce {
			continue
		}

		self, err := n.selfOverlayNode(sub.spec)
		if err != nil {
			errs = append(errs, fmt.Errorf("build overlay node for %s: %w", sub.spec.Name, err))
			continue
		}

		err = n.storeOverlayNodeWithRetry(ctx, sub.spec.FullID, &overlay.NodesList{
			List: []overlay.Node{*self},
		})
		if err != nil {
			errs = append(errs, fmt.Errorf("store overlay node for %s: %w", sub.spec.Name, err))
		}
	}

	return errors.Join(errs...)
}

func (n *Node) selfOverlayNode(spec overlaySpec) (*overlay.Node, error) {
	return overlay.NewNode(spec.FullID, n.privKey)
}

func (n *Node) storeAddressWithRetry(ctx context.Context, addrList address.List) error {
	if n.IsOffline() {
		return ErrOffline
	}
	store := func(attemptCtx context.Context) error {
		if n.IsOffline() {
			return ErrOffline
		}
		_, _, err := n.dht.StoreAddress(attemptCtx, addrList, publicAnnounceTTL, n.privKey)
		return err
	}
	return n.retryStoreWithWarmup(ctx, store, n.warmupDHTAddress)
}

func (n *Node) storeOverlayNodeWithRetry(ctx context.Context, overlayKey []byte, nodes *overlay.NodesList) error {
	if n.IsOffline() {
		return ErrOffline
	}
	store := func(attemptCtx context.Context) error {
		if n.IsOffline() {
			return ErrOffline
		}
		_, _, err := n.dht.StoreOverlayNodes(attemptCtx, overlayKey, nodes, publicAnnounceTTL)
		return err
	}
	warmup := func(warmCtx context.Context) error {
		return n.warmupDHTOverlay(warmCtx, overlayKey)
	}
	return n.retryStoreWithWarmup(ctx, store, warmup)
}

func (n *Node) retryStoreWithWarmup(ctx context.Context, store, warmup func(context.Context) error) error {
	attempt := func() error {
		if n.IsOffline() {
			return ErrOffline
		}
		storeCtx, cancel := context.WithTimeout(ctx, dhtStoreTimeout)
		defer cancel()
		return store(storeCtx)
	}

	err := attempt()
	if err == nil || !isTransientDHTStoreError(err) {
		return err
	}

	if n.IsOffline() {
		return ErrOffline
	}
	if warmErr := warmup(ctx); warmErr != nil && !errors.Is(warmErr, context.Canceled) {
		n.log.Debug().Err(warmErr).Msg("failed to warm up DHT before retrying store")
	}
	return attempt()
}

func (n *Node) warmupDHTAddress(ctx context.Context) error {
	if n.IsOffline() {
		return ErrOffline
	}
	if n.dht == nil {
		return nil
	}

	warmCtx, cancel := context.WithTimeout(ctx, dhtFindTimeout)
	defer cancel()

	_, _, err := n.dht.FindAddresses(warmCtx, n.gateway.GetID())
	if errors.Is(err, dht.ErrDHTValueIsNotFound) {
		return nil
	}
	return err
}

func (n *Node) warmupDHTOverlay(ctx context.Context, overlayKey []byte) error {
	if n.IsOffline() {
		return ErrOffline
	}
	if n.dht == nil {
		return nil
	}

	warmCtx, cancel := context.WithTimeout(ctx, dhtFindTimeout)
	defer cancel()

	_, _, err := n.dht.FindOverlayNodes(warmCtx, overlayKey)
	if errors.Is(err, dht.ErrDHTValueIsNotFound) {
		return nil
	}
	return err
}

func isTransientDHTStoreError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "no alive nodes found to store this key")
}

func cloneOverlayNode(node *overlay.Node) *overlay.Node {
	if node == nil {
		return nil
	}

	cloned := *node
	cloned.ID = cloneOverlayNodeID(node.ID)
	cloned.Overlay = append([]byte(nil), node.Overlay...)
	cloned.Signature = append([]byte(nil), node.Signature...)
	return &cloned
}

func cloneOverlayNodeID(id any) any {
	switch key := id.(type) {
	case keys.PublicKeyED25519:
		return keys.PublicKeyED25519{Key: append(ed25519.PublicKey(nil), key.Key...)}
	case keys.PublicKeyAES:
		return keys.PublicKeyAES{Key: append([]byte(nil), key.Key...)}
	default:
		return id
	}
}

func overlayNodeHasSerializableID(node *overlay.Node) bool {
	if node == nil {
		return false
	}
	switch key := node.ID.(type) {
	case keys.PublicKeyED25519:
		return len(key.Key) == ed25519.PublicKeySize
	case keys.PublicKeyAES:
		return len(key.Key) == ed25519.PublicKeySize
	default:
		return false
	}
}
