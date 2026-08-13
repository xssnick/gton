package p2p

import (
	"bytes"
	"crypto/ed25519"
	"slices"
	"testing"
	"time"

	"github.com/xssnick/gton/service/p2p/internal/fastsync"
	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
)

type fastSyncMembershipTestIssuer struct {
	public  keys.PublicKeyED25519
	private ed25519.PrivateKey
	id      PeerID
}

type fastSyncMembershipTester interface {
	Helper()
	Fatalf(format string, arguments ...any)
}

func newFastSyncMembershipTestIssuer(
	t fastSyncMembershipTester,
	value byte,
) fastSyncMembershipTestIssuer {
	t.Helper()

	private := ed25519.NewKeyFromSeed(
		bytes.Repeat([]byte{value}, ed25519.SeedSize),
	)
	public := keys.PublicKeyED25519{
		Key: ed25519.PublicKey(
			slices.Clone([]byte(private[ed25519.SeedSize:])),
		),
	}
	rawID, err := tl.Hash(public)
	if err != nil {
		t.Fatalf("hash membership test issuer: %v", err)
	}
	id, err := NewPeerID(rawID)
	if err != nil {
		t.Fatalf("parse membership test issuer id: %v", err)
	}

	return fastSyncMembershipTestIssuer{
		public:  public,
		private: private,
		id:      id,
	}
}

func fastSyncMembershipTestRoster(
	roots,
	permanent []PeerID,
) FastSyncValidatorRoster {
	return FastSyncValidatorRoster{
		rootPublicKeyIDs: slices.Clone(roots),
		adnlIDs:          slices.Clone(permanent),
	}
}

func newFastSyncTestMembership(
	roster FastSyncValidatorRoster,
	permanentFlags uint32,
) *fastsync.Membership {
	return fastsync.NewMembership(
		roster.rootPublicKeyIDsRef(),
		roster.adnlIDsRef(),
		permanentFlags,
	)
}

func fastSyncMembershipTestCertificate(
	t fastSyncMembershipTester,
	issuer fastSyncMembershipTestIssuer,
	node PeerID,
	slot int32,
	expireAt int32,
) overlay.MemberCertificate {
	t.Helper()

	certificate := overlay.MemberCertificate{
		IssuedBy: issuer.public,
		Slot:     slot,
		ExpireAt: expireAt,
	}
	toSign, err := tl.Serialize(overlay.MemberCertificateID{
		Node:     node[:],
		Flags:    certificate.Flags,
		Slot:     certificate.Slot,
		ExpireAt: certificate.ExpireAt,
	}, true)
	if err != nil {
		t.Fatalf("serialize membership test certificate: %v", err)
	}
	certificate.Signature = ed25519.Sign(issuer.private, toSign)
	return certificate
}

func peerRuntimeTestRootRuntime(
	t *testing.T,
	overlayID FastSyncOverlayShortID,
	now time.Time,
) (
	*fastsync.Membership,
	*fastsync.PeerRuntime,
	ed25519.PrivateKey,
) {
	t.Helper()

	issuerKey := peerRuntimeTestKey(0xd1)
	issuerPublic := issuerKey.Public().(ed25519.PublicKey)
	issuerID := peerRuntimeTestPeerID(issuerPublic)
	roster := peerRuntimeTestRoster(
		issuerPublic,
		FastSyncValidator{ADNLID: issuerID},
	)
	membership := newFastSyncTestMembership(roster, 1)
	runtime, err := fastsync.NewPeerRuntime(
		issuerKey,
		overlayID,
		0,
		overlay.EmptyMemberCertificate{},
		membership,
		now,
	)
	if err != nil {
		t.Fatalf("create root runtime: %v", err)
	}
	return membership, runtime, issuerKey
}

func peerRuntimeTestRoster(
	rootPublic ed25519.PublicKey,
	validators ...FastSyncValidator,
) FastSyncValidatorRoster {
	var root FastSyncValidatorPublicKey
	copy(root[:], rootPublic)
	for index := range validators {
		validators[index].PublicKey = root
	}
	return NewFastSyncValidatorRoster(nil, validators, nil)
}

func peerRuntimeTestKey(marker byte) ed25519.PrivateKey {
	return ed25519.NewKeyFromSeed(
		bytes.Repeat([]byte{marker}, ed25519.SeedSize),
	)
}

func peerRuntimeTestPeerID(publicKey ed25519.PublicKey) PeerID {
	var value FastSyncValidatorPublicKey
	copy(value[:], publicKey)
	return fastsync.ValidatorID[PeerID](value)
}

func peerRuntimeTestOverlayID(marker byte) FastSyncOverlayShortID {
	var id FastSyncOverlayShortID
	for index := range id {
		id[index] = marker + byte(index)
	}
	return id
}

func peerRuntimeTestCertificate(
	t *testing.T,
	issuerKey ed25519.PrivateKey,
	node PeerID,
	flags uint32,
	slot int32,
	expireAt int32,
) overlay.MemberCertificate {
	t.Helper()

	certificate := overlay.MemberCertificate{
		IssuedBy: keys.PublicKeyED25519{
			Key: issuerKey.Public().(ed25519.PublicKey),
		},
		Flags:    flags,
		Slot:     slot,
		ExpireAt: expireAt,
	}
	toSign, err := tl.Serialize(overlay.MemberCertificateID{
		Node:     node[:],
		Flags:    flags,
		Slot:     slot,
		ExpireAt: expireAt,
	}, true)
	if err != nil {
		t.Fatalf("serialize certificate: %v", err)
	}
	certificate.Signature = ed25519.Sign(issuerKey, toSign)
	return certificate
}

func peerRuntimeTestNode(
	t *testing.T,
	privateKey ed25519.PrivateKey,
	overlayID FastSyncOverlayShortID,
	flags uint32,
	version int32,
	certificate any,
) overlay.NodeV2 {
	t.Helper()

	node := overlay.NodeV2{
		ID: keys.PublicKeyED25519{
			Key: privateKey.Public().(ed25519.PublicKey),
		},
		Overlay:     overlayID[:],
		Flags:       flags,
		Version:     version,
		Certificate: certificate,
	}
	if err := node.Sign(privateKey); err != nil {
		t.Fatalf("sign node: %v", err)
	}
	return node
}

func peerRuntimeTestNodeID(
	t *testing.T,
	node overlay.NodeV2,
) PeerID {
	t.Helper()

	publicKey, ok := node.ID.(keys.PublicKeyED25519)
	if !ok {
		t.Fatalf("node id type = %T, want ED25519", node.ID)
	}
	return peerRuntimeTestPeerID(publicKey.Key)
}
