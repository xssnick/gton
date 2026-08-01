package p2p

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
)

func TestHandleQUICMessageRewrapsCertifiedPlumtreeRelay(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	node := newTestNode(t)
	node.zeroStateFileHash = make([]byte, PeerIDSize)

	issuer := newFastSyncMembershipTestIssuer(t, 0x71)
	var validatorKey FastSyncValidatorPublicKey
	copy(validatorKey[:], issuer.public.Key)
	roster := NewFastSyncValidatorRoster(
		nil,
		[]FastSyncValidator{{PublicKey: validatorKey}},
		nil,
	)

	localCertificate := fastSyncMembershipTestCertificate(
		t,
		issuer,
		node.localID,
		0,
		0,
		int32(now.Add(time.Hour).Unix()),
	)
	relayID := testPeerID("certified-plumtree-relay")
	relayCertificate := fastSyncMembershipTestCertificate(
		t,
		issuer,
		relayID,
		1,
		0,
		int32(now.Add(time.Hour).Unix()),
	)
	node.fastSyncCertificates = []overlay.MemberCertificate{
		localCertificate,
	}
	if err := node.SetFastSyncOverlays(FastSyncState{
		Roster:                     roster,
		MasterchainPlumtreeEnabled: true,
	}); err != nil {
		t.Fatalf("create FastSync overlay: %v", err)
	}

	sub := node.fastSyncSubscriptions[FastSyncShard{
		Workchain: -1,
		Shard:     topShard,
	}]
	if sub == nil {
		t.Fatal("masterchain FastSync subscription is missing")
	}
	sub.plumtree.engine.mu.Lock()
	sub.plumtree.engine.promoteEagerLocked(
		&sub.plumtree.engine.slots[plumtreeSimpleTree],
		relayID,
		true,
		now,
	)
	sub.plumtree.engine.mu.Unlock()

	data, err := tl.Serialize(overlay.Ping{}, true)
	if err != nil {
		t.Fatalf("serialize broadcast data: %v", err)
	}
	broadcastID := sha256.Sum256([]byte("certified-plumtree-relay-data"))
	dataHash := sha256.Sum256(data)
	signedData := serializeValidatedPlumtreeSimpleToSign(
		BroadcastPlumtreeSimpleToSign{
			ID:        broadcastID[:],
			Timestamp: plumtreeUnixSeconds(now),
			TreeIndex: plumtreeSimpleTree,
			DataSize:  int32(len(data)),
			DataHash:  dataHash[:],
		},
	)
	message := BroadcastPlumtreeSimple{
		Timestamp: plumtreeUnixSeconds(now),
		Source: keys.PublicKeyED25519{
			Key: issuer.private.Public().(ed25519.PublicKey),
		},
		Certificate: overlay.CertificateEmpty{},
		BroadcastID: broadcastID[:],
		TreeIndex:   plumtreeSimpleTree,
		Data:        data,
		Signature:   ed25519.Sign(issuer.private, signedData),
	}
	relayEnvelope, err := newQUICOverlayEnvelope(
		sub.spec.ShortID,
		&relayCertificate,
	)
	if err != nil {
		t.Fatalf("create relay envelope: %v", err)
	}
	inboundWire, err := relayEnvelope.Message(message)
	if err != nil {
		t.Fatalf("serialize relayed message: %v", err)
	}

	if err = node.handleQUICMessage(
		context.Background(),
		&authenticatedQUICPeer{id: relayID},
		inboundWire,
	); err != nil {
		t.Fatalf("handle relayed message: %v", err)
	}

	sub.plumtree.engine.mu.Lock()
	state := sub.plumtree.engine.simpleStates[broadcastID]
	if state == nil {
		sub.plumtree.engine.mu.Unlock()
		t.Fatal("relayed message was not retained")
	}
	retainedWire := state.part.wire.Message
	retainedBody := state.part.wire.Body
	sub.plumtree.engine.mu.Unlock()

	if &retainedWire[0] == &inboundWire[0] {
		t.Fatal("remote sender envelope was retained for local relay")
	}
	header, parsedBody, err := parseQUICMessageEnvelope(retainedWire)
	if err != nil {
		t.Fatalf("parse retained local envelope: %v", err)
	}
	if header.certificateKind != quicMembershipCertificateMember ||
		header.certificate.Slot != localCertificate.Slot ||
		!bytes.Equal(
			header.certificate.Signature,
			localCertificate.Signature,
		) {
		t.Fatalf(
			"retained envelope certificate = %+v, want local slot %d",
			header.certificate,
			localCertificate.Slot,
		)
	}
	if len(retainedBody) == 0 ||
		&retainedBody[0] != &parsedBody[0] {
		t.Fatal("retained body is not a view into the rewrapped wire")
	}
}
