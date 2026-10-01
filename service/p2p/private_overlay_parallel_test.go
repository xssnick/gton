package p2p

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/xssnick/tonutils-go/adnl/overlay"
)

type privateOverlayBlockedRLDP struct {
	*testBroadcastRLDP
	started  chan<- struct{}
	finished chan<- struct{}
	release  <-chan struct{}
}

func (r *privateOverlayBlockedRLDP) SendMessage(ctx context.Context, _ []byte) error {
	r.started <- struct{}{}
	defer func() { r.finished <- struct{}{} }()
	if r.release == nil {
		return nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.release:
		return nil
	}
}

func TestPrivateOverlayTwoStepDispatchesEntireRosterIndependently(t *testing.T) {
	for name, payload := range map[string][]byte{
		"simple": []byte("candidate"),
		"fec":    bytes.Repeat([]byte{0x51}, 2048),
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const peerCount = overlay.DefaultBroadcastTwoStepSendConcurrency + 1
				started := make(chan struct{}, peerCount)
				finished := make(chan struct{}, peerCount)
				release := make(chan struct{})
				overlayID := testPeerID("independent-private-roster")
				members := make(map[PeerID]struct{}, peerCount)
				peers := make([]*overlayPeer, 0, peerCount)
				for i := range peerCount {
					id := testPeerID(fmt.Sprintf("private-roster-peer-%d", i))
					members[id] = struct{}{}
					transport := &privateOverlayBlockedRLDP{
						testBroadcastRLDP: &testBroadcastRLDP{adnl: newTestOverlayADNL()},
						started:           started,
						finished:          finished,
					}
					if i < peerCount-1 {
						transport.release = release
					}
					wrapped := overlay.CreateExtendedRLDP(transport).CreateOverlay(overlayID[:])
					defer wrapped.Close()
					peers = append(peers, &overlayPeer{id: id, rldpOverlay: wrapped})
				}

				sub := &overlaySubscription{
					node: &Node{localID: testPeerID("private-roster-source")},
					spec: overlaySpec{
						Kind:                          overlayKindPrivate,
						PrivateTwoStep:                true,
						PrivateTwoStepIntermediateIDs: members,
					},
				}
				sub.broadcastTargets.Store(&broadcastTargetsSnapshot{builtAt: time.Now(), peers: peers})
				handle := &PrivateOverlay{
					sub: sub,
					signer: privateOverlayTestSigner{
						key: ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x52}, ed25519.SeedSize)),
					},
				}
				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				defer cancel()
				done := make(chan error, 1)
				go func() {
					_, err := handle.BroadcastTwoStep(
						ctx,
						nil,
						payload,
						nil,
						0,
					)
					done <- err
				}()

				synctest.Wait()
				if got := len(started); got != peerCount {
					t.Errorf("started sends = %d, want all %d before blocked peers finish", got, peerCount)
				}
				close(release)
				if err := <-done; err != nil {
					t.Errorf("broadcast: %v", err)
				}
				for range peerCount {
					<-finished
				}
				synctest.Wait()
			})
		})
	}
}
