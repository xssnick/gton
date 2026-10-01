package p2p

import (
	"errors"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
)

// The scheme string is the only thing that fixes the constructor id, and a typo
// in it would make us silently ignore every certificate a validator pushes.
func TestNewFastSyncMemberCertificateWireRoundTrip(t *testing.T) {
	t.Parallel()

	issuer := newFastSyncMembershipTestIssuer(t, 0xc1)
	node := fastSyncTestPeerID(0xc2)
	certificate := fastSyncMembershipTestCertificate(
		t,
		issuer,
		node,
		1,
		int32(time.Now().Add(time.Hour).Unix()),
	)

	encoded, err := tl.Serialize(NewFastSyncMemberCertificate{
		ADNLID:      node[:],
		Certificate: certificate,
	}, true)
	if err != nil {
		t.Fatalf("serialize certificate push: %v", err)
	}

	var parsed NewFastSyncMemberCertificate
	rest, err := tl.Parse(&parsed, encoded, true)
	if err != nil {
		t.Fatalf("parse certificate push: %v", err)
	}
	if len(rest) != 0 {
		t.Fatalf("certificate push left %d trailing bytes", len(rest))
	}
	if got, want := PeerID(parsed.ADNLID), node; got != want {
		t.Fatalf("adnl id = %v, want %v", got, want)
	}
	value, isMember := parsed.Certificate.(overlay.MemberCertificate)
	if !isMember {
		t.Fatalf("certificate type = %T, want member certificate", parsed.Certificate)
	}
	if value.ExpireAt != certificate.ExpireAt || value.Slot != certificate.Slot {
		t.Fatalf("certificate = %+v, want %+v", value, certificate)
	}

	// Validators send the empty variant too; it must decode rather than error.
	encoded, err = tl.Serialize(NewFastSyncMemberCertificate{
		ADNLID:      node[:],
		Certificate: overlay.EmptyMemberCertificate{},
	}, true)
	if err != nil {
		t.Fatalf("serialize empty certificate push: %v", err)
	}
	if _, err = tl.Parse(&parsed, encoded, true); err != nil {
		t.Fatalf("parse empty certificate push: %v", err)
	}
	if _, isEmpty := parsed.Certificate.(overlay.EmptyMemberCertificate); !isEmpty {
		t.Fatalf("empty certificate type = %T", parsed.Certificate)
	}
}

// fastSyncCertificateTestNode builds a node whose roster really contains the
// issuer's key: ContainsRoot matches on the public key, not on the ADNL id, so a
// synthetic key here would reject every certificate.
func fastSyncCertificateTestNode(
	t *testing.T,
	issuer fastSyncMembershipTestIssuer,
) *Node {
	t.Helper()

	var publicKey FastSyncValidatorPublicKey
	copy(publicKey[:], issuer.public.Key)

	node := newTestNode(t)
	node.fastSyncState = &FastSyncState{
		Roster: NewFastSyncValidatorRoster(
			nil,
			[]FastSyncValidator{{PublicKey: publicKey, ADNLID: issuer.id}},
			nil,
		),
	}
	return node
}

// A certificate is adopted only when it is for us, unexpired, slotted, and
// issued by a roster validator. Each of these is the reference node's import
// check; failing any of them must leave the stored set untouched.
func TestImportFastSyncCertificateRejects(t *testing.T) {
	now := time.Unix(1_800_002_000, 0)
	issuer := newFastSyncMembershipTestIssuer(t, 0xd1)
	stranger := newFastSyncMembershipTestIssuer(t, 0xd2)
	valid := int32(now.Add(time.Hour).Unix())

	tests := []struct {
		name     string
		issuer   fastSyncMembershipTestIssuer
		node     func(local PeerID) PeerID
		slot     int32
		expireAt int32
	}{
		{
			name:     "expires too soon",
			issuer:   issuer,
			node:     func(local PeerID) PeerID { return local },
			slot:     1,
			expireAt: int32(now.Add(59 * time.Second).Unix()),
		},
		{
			name:     "slot out of range",
			issuer:   issuer,
			node:     func(local PeerID) PeerID { return local },
			slot:     FastSyncMemberSlotCount,
			expireAt: valid,
		},
		{
			name:     "issuer is not a validator",
			issuer:   stranger,
			node:     func(local PeerID) PeerID { return local },
			slot:     1,
			expireAt: valid,
		},
		{
			name:     "issued for another node",
			issuer:   issuer,
			node:     func(PeerID) PeerID { return fastSyncTestPeerID(0xd9) },
			slot:     1,
			expireAt: valid,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			node := fastSyncCertificateTestNode(t, issuer)
			certificate := fastSyncMembershipTestCertificate(
				t,
				test.issuer,
				test.node(node.localID),
				test.slot,
				test.expireAt,
			)

			if err := node.importFastSyncCertificate(certificate, now); !errors.Is(
				err,
				errFastSyncCertificateNotStored,
			) {
				t.Fatalf("invalid certificate import error = %v", err)
			}
			if got := len(node.fastSyncCertificateSnapshot()); got != 0 {
				t.Fatalf("stored certificates = %d, want 0", got)
			}
		})
	}
}

// Without a masterchain state there is no roster to judge an issuer against, so
// nothing may be adopted.
func TestImportFastSyncCertificateNeedsRoster(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_002_100, 0)
	issuer := newFastSyncMembershipTestIssuer(t, 0xe1)
	node := newTestNode(t)
	certificate := fastSyncMembershipTestCertificate(
		t,
		issuer,
		node.localID,
		1,
		int32(now.Add(time.Hour).Unix()),
	)

	if err := node.importFastSyncCertificate(certificate, now); !errors.Is(
		err,
		errFastSyncCertificateNotStored,
	) {
		t.Fatalf("certificate import error = %v", err)
	}
}

// One certificate per issuer, replaced only by a longer-lived one. Readers use
// the slice without a lock, so a replacement must not mutate it in place.
func TestImportFastSyncCertificateDedupesByIssuer(t *testing.T) {
	now := time.Unix(1_800_002_200, 0)
	issuer := newFastSyncMembershipTestIssuer(t, 0xf1)
	node := fastSyncCertificateTestNode(t, issuer)

	first := fastSyncMembershipTestCertificate(
		t, issuer, node.localID, 1, int32(now.Add(time.Hour).Unix()),
	)
	if err := node.importFastSyncCertificate(first, now); err != nil {
		t.Fatalf("import first certificate: %v", err)
	}
	stored := node.fastSyncCertificateSnapshot()
	if len(stored) != 1 {
		t.Fatalf("stored certificates = %d, want 1", len(stored))
	}

	same := fastSyncMembershipTestCertificate(
		t, issuer, node.localID, 1, first.ExpireAt,
	)
	if err := node.importFastSyncCertificate(same, now); !errors.Is(
		err,
		errFastSyncCertificateNotStored,
	) {
		t.Fatalf("equal-expiry certificate import error = %v", err)
	}

	newer := fastSyncMembershipTestCertificate(
		t, issuer, node.localID, 1, first.ExpireAt+600,
	)
	if err := node.importFastSyncCertificate(newer, now); err != nil {
		t.Fatalf("import longer-lived certificate: %v", err)
	}
	updated := node.fastSyncCertificateSnapshot()
	if len(updated) != 1 {
		t.Fatalf("stored certificates after rotation = %d, want 1", len(updated))
	}
	if updated[0].ExpireAt != newer.ExpireAt {
		t.Fatalf("stored expiry = %d, want %d", updated[0].ExpireAt, newer.ExpireAt)
	}
	if stored[0].ExpireAt != first.ExpireAt {
		t.Fatal("rotation mutated the slice an earlier reader still holds")
	}
}

// The handler is the pool's catch-all for non-overlay ADNL messages, so it must
// ignore everything it does not own.
func TestHandlePeerCustomMessageIgnoresOtherTypes(t *testing.T) {
	t.Parallel()

	issuer := newFastSyncMembershipTestIssuer(t, 0xa1)
	node := fastSyncCertificateTestNode(t, issuer)

	if err := node.handlePeerCustomMessage(&adnl.MessageCustom{
		Data: ForgetPeer{},
	}); err != nil {
		t.Fatalf("unrelated custom message returned an error: %v", err)
	}
	if got := len(node.fastSyncCertificateSnapshot()); got != 0 {
		t.Fatalf("stored certificates = %d, want 0", got)
	}

	// The empty variant carries nothing to adopt.
	if err := node.handlePeerCustomMessage(&adnl.MessageCustom{
		Data: NewFastSyncMemberCertificate{
			ADNLID:      node.localID[:],
			Certificate: overlay.EmptyMemberCertificate{},
		},
	}); err != nil {
		t.Fatalf("empty certificate push returned an error: %v", err)
	}
	if got := len(node.fastSyncCertificateSnapshot()); got != 0 {
		t.Fatalf("stored certificates after empty push = %d, want 0", got)
	}
}

// Only a certificate whose issuer is still in the routing rosters is usable -
// fastSyncLocalCertificate filters on exactly that. Refusing every new one once
// the set is full let a validator-set rotation strand the node: the 32 slots
// held unexpired but unroutable certificates, no replacement could be stored,
// and every fast-sync overlay stayed down until they timed out, up to an hour.
func TestStoreFastSyncCertificateEvictsUnroutedIssuers(t *testing.T) {
	now := time.Unix(1_800_003_000, 0)
	expireAt := int32(now.Add(time.Hour).Unix())

	current := newFastSyncMembershipTestIssuer(t, 0xa1)
	node := fastSyncCertificateTestNode(t, current)
	roster := node.fastSyncState.Roster

	// Fill every slot with certificates from issuers outside the roster.
	for i := 0; i < fastSyncCertificateLimit; i++ {
		stale := newFastSyncMembershipTestIssuer(t, byte(0x40+i))
		certificate := fastSyncMembershipTestCertificate(
			t, stale, node.localID, 1, expireAt,
		)
		if err := node.storeFastSyncCertificate(stale.id, certificate, roster, now); err != nil {
			t.Fatalf("seed certificate %d: %v", i, err)
		}
	}
	if got := len(node.fastSyncCertificateSnapshot()); got != fastSyncCertificateLimit {
		t.Fatalf("seeded certificates = %d, want %d", got, fastSyncCertificateLimit)
	}

	// A certificate from the validator that is actually routing must get in.
	fresh := fastSyncMembershipTestCertificate(
		t, current, node.localID, 1, expireAt,
	)
	if err := node.importFastSyncCertificate(fresh, now); err != nil {
		t.Fatalf("import certificate from a routing validator: %v", err)
	}

	stored := node.fastSyncCertificateSnapshot()
	if len(stored) != fastSyncCertificateLimit {
		t.Fatalf("stored certificates = %d, want %d", len(stored), fastSyncCertificateLimit)
	}
	found := false
	for _, certificate := range stored {
		issuerBytes, err := certificate.IssuerID()
		if err != nil {
			t.Fatalf("stored issuer id: %v", err)
		}
		issuer, err := NewPeerID(issuerBytes)
		if err != nil {
			t.Fatalf("stored issuer: %v", err)
		}
		if issuer == current.id {
			found = true
		}
	}
	if !found {
		t.Fatal("a full set refused the only usable certificate")
	}
}
