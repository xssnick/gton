package p2p

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"github.com/xssnick/gton/service/storage"
	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/ton"
)

func TestPrivateNetworkFastSyncUsesConsensusIdentity(t *testing.T) {
	node, err := New(privateNetworkTestOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { node.EnterOffline("test complete"); waitForNodeStop(t, node) })
	node.zeroStateFileHash = make([]byte, PeerIDSize)
	private := node.privateNetwork
	roster := NewFastSyncValidatorRoster(nil, []FastSyncValidator{
		fastSyncOverlayTestValidator(0x11, private.localID),
	}, nil)
	node.SetMonitorMinSplitDepth(0, 1)
	if err = node.SetFastSyncOverlays(FastSyncState{
		Roster:                     roster,
		Shards:                     []FastSyncShard{{Workchain: 0, Shard: 0x4000000000000000}},
		MasterchainPlumtreeEnabled: true, ShardPlumtreeEnabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	block := ton.BlockIDExt{Workchain: -1, Shard: topShard}
	sub, err := node.fastSyncSubscriptionForBlock(block)
	if err != nil {
		t.Fatal(err)
	}
	if sub == nil {
		t.Fatal("validator FastSync disappeared after moving its ADNL key to the dedicated network")
	}
	if sub.node != private || !sub.fastSync.spec.localValidator || sub.plumtree == nil {
		t.Fatal("FastSync must use the dedicated validator identity and transport")
	}
	if len(node.fastSyncSubscriptions) != 0 || len(private.fastSyncSubscriptions) != 2 {
		t.Fatal("FastSync subscriptions belong to the dedicated network")
	}
	if private.fastSyncSubscriptions[FastSyncShard{Workchain: 0, Shard: 0x4000000000000000}] == nil {
		t.Fatal("FastSync did not retain the primary node's monitor depth")
	}
	target, err := node.BlockBroadcasts().fastSyncPublicationTarget(block, true)
	if err != nil || target != sub {
		t.Fatalf("publication target = %p, %v, want %p", target, err, sub)
	}
	// Admission and publication certificates use the chain workflow and the
	// actual sending transport respectively.
	node.broadcastAdmission = testBroadcastAdmission(false)
	disposition := sub.handleOverlayBroadcastPayload(nil, tonnodeapi.BlockBroadcast{}, nil, DeliveryTwoStep, false, testPeerID("remote"))
	if disposition != overlay.BroadcastDispositionRetry {
		t.Fatalf("shared admission = %v", disposition)
	}
	signer := privateOverlayTestSigner{key: ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0xb3}, ed25519.SeedSize))}
	certificate, err := node.BlockBroadcasts().publicationCertificate(sub, signer, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	check, err := certificate.Check(private.localID[:], sub.spec.ShortID, 1, true)
	if err != nil || check != overlay.CertCheckResultTrusted {
		t.Fatalf("dedicated publication certificate: %v, %v", check, err)
	}
}

func TestPrivateNetworkFastSyncCertificatePersistsForDedicatedIdentity(t *testing.T) {
	opts := privateNetworkTestOptions(t)
	privateID := probeTestPeerID(t, opts.PrivateNetwork.PrivateKey)
	issuer := newFastSyncMembershipTestIssuer(t, 0xb1)
	certificate := fastSyncMembershipTestCertificate(t, issuer, privateID, 1, int32(time.Now().Add(time.Hour).Unix()))
	store := &fastSyncCertificateTestStorage{}
	snapshot, err := encodeFastSyncCertificateSnapshot([]overlay.MemberCertificate{certificate})
	if err != nil {
		t.Fatal(err)
	}
	store.snapshot = snapshot
	opts.FastSyncCertificateStorage = store
	node, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { node.EnterOffline("test complete"); waitForNodeStop(t, node) })
	if len(node.privateNetwork.fastSyncCertificateSnapshot()) != 1 || store.deletes != 0 {
		t.Fatal("loading the primary identity discarded the dedicated identity's stored certificate")
	}
	if len(node.fastSyncCertificateSnapshot()) != 0 {
		t.Fatal("primary identity adopted the dedicated certificate")
	}
}

func TestPrivateNetworkFastSyncLoopbackCertificateAndBlockServing(t *testing.T) {
	harness := startPrivateNetworkLoopback(t)
	node, remote := harness.node, harness.remote
	private := node.privateNetwork
	issuer := newFastSyncMembershipTestIssuer(t, 0xb2)
	var publicKey FastSyncValidatorPublicKey
	copy(publicKey[:], issuer.public.Key)
	state := FastSyncState{
		Roster:                     NewFastSyncValidatorRoster(nil, []FastSyncValidator{{PublicKey: publicKey, ADNLID: remote.localID}}, nil),
		MasterchainPlumtreeEnabled: true,
	}
	for _, network := range []*Node{node, remote} {
		if err := network.SetFastSyncOverlays(state); err != nil {
			t.Fatal(err)
		}
	}
	certificate := fastSyncMembershipTestCertificate(t, issuer, private.localID, 1, int32(time.Now().Add(time.Hour).Unix()))
	peer, err := remote.gateway.RegisterClient(private.listenAddr, private.privKey.Public().(ed25519.PublicKey))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err = peer.SendCustomMessage(ctx, NewFastSyncMemberCertificate{ADNLID: private.localID[:], Certificate: certificate}); err != nil {
		t.Fatal(err)
	}
	block := testStoredMasterBlockID(42)
	var localSub *overlaySubscription
	for localSub == nil {
		localSub, err = node.fastSyncSubscriptionForBlock(block)
		if err != nil {
			t.Fatal(err)
		}
		if localSub != nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("certificate push did not create dedicated FastSync subscription")
		case <-time.After(10 * time.Millisecond):
		}
	}
	if localSub.fastSync.spec.localValidator {
		t.Fatal("certified collator was classified as validator")
	}
	if _, err = node.resolvePublicBroadcastReceiver(localSub.spec.ShortID); !errors.Is(err, overlay.ErrBroadcastReceiverNotFound) {
		t.Fatalf("FastSync receiver leaked onto primary identity: %v", err)
	}
	store := node.peerStorage.(*testPeerStore)
	full := &storage.ServedBlockFull{ID: block, Block: []byte{1, 2, 3, 4}, Proof: []byte{5, 6, 7}}
	if err = store.SaveBlockFull(full); err != nil {
		t.Fatal(err)
	}
	remoteSub, err := remote.fastSyncSubscriptionForBlock(block)
	if err != nil || remoteSub == nil {
		t.Fatalf("remote overlay: %v", err)
	}
	connectPrivateNetworkLoopbackPeer(t, &PrivateOverlay{sub: localSub}, remote)
	// Discovery must carry the certificate before the validator knows this
	// collator, and makes the reverse ADNL/RLDP path eligible too.
	localSub.exchangeFastSyncRandomPeers(ctx, localSub.peerByID(remote.localID))
	connectPrivateNetworkLoopbackPeer(t, &PrivateOverlay{sub: remoteSub}, private)
	remotePeer := remoteSub.peerByID(private.localID)
	for _, protocol := range []string{"adnl", "rldp", "quic"} {
		t.Run(protocol, func(t *testing.T) {
			var answer tonnodeapi.DataFull
			query := tonnodeapi.DownloadBlockFull{Block: block}
			var queryErr error
			switch protocol {
			case "adnl":
				queryErr = remotePeer.overlay.Query(ctx, query, &answer)
			case "rldp":
				queryErr = remotePeer.rldpOverlay.DoQuery(ctx, 4096, query, &answer)
			case "quic":
				queryErr = remotePeer.queryTransport.Query(ctx, 4096, query, &answer)
			}
			if queryErr != nil {
				t.Fatal(queryErr)
			}
			if !bytes.Equal(answer.Block, full.Block) || !bytes.Equal(answer.Proof, full.Proof) {
				t.Fatal("dedicated FastSync did not serve the shared chain store")
			}
		})
	}
}
