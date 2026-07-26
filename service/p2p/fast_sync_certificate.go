package p2p

import (
	"fmt"
	"time"

	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
)

const (
	// fastSyncCertificateMinRemaining mirrors the reference node's import check:
	// a certificate that is about to expire is not worth adopting.
	fastSyncCertificateMinRemaining = time.Minute

	// fastSyncCertificateLimit bounds the stored set. Certificates are keyed by
	// issuer and only roster validators may issue one, so this is a backstop
	// against a roster far larger than any real one.
	fastSyncCertificateLimit = 32
)

// NewFastSyncMemberCertificate is a validator pushing us a fresh FastSync
// membership certificate. Validators reissue every minute with an hour of
// lifetime, so a node that only ever reads its certificate from config loses
// FastSync an hour after start with no way back.
//
// It arrives as a plain ADNL message rather than an overlay one, which is why it
// is handled through the peer pool's custom-message handler instead of any
// overlay dispatch.
type NewFastSyncMemberCertificate struct {
	ADNLID      []byte `tl:"int256"`
	Certificate any    `tl:"struct boxed [overlay.emptyMemberCertificate,overlay.memberCertificate]"`
}

func init() {
	tl.Register(
		NewFastSyncMemberCertificate{},
		"tonNode.newFastSyncMemberCertificate adnl_id:int256 certificate:overlay.MemberCertificate = tonNode.NewFastSyncMemberCertificate",
	)
}

// handlePeerCustomMessage receives ADNL messages that are not overlay traffic.
// Anything unrecognised is ignored: this is the catch-all handler for the peer
// pool, not a protocol endpoint.
func (n *Node) handlePeerCustomMessage(msg *adnl.MessageCustom) error {
	push, isCertificate := msg.Data.(NewFastSyncMemberCertificate)
	if !isCertificate {
		return nil
	}

	certificate, isMember := push.Certificate.(overlay.MemberCertificate)
	if !isMember {
		return nil
	}
	if !n.importFastSyncCertificate(certificate, time.Now()) {
		return nil
	}

	// Overlays are otherwise reconciled only while applying a masterchain block.
	// A node whose certificate already expired has no FastSync overlay to
	// receive blocks on, so waiting for the next apply would never happen.
	n.runAsync(n.refreshFastSyncOverlays)
	return nil
}

// importFastSyncCertificate validates a pushed certificate and stores it when it
// improves on what we hold. It reports whether the stored set changed.
//
// Checks run cheapest-first so that an unauthenticated sender cannot make us
// verify signatures for free; the reference node checks the signature first, and
// it does not have this path open to arbitrary peers.
func (n *Node) importFastSyncCertificate(
	certificate overlay.MemberCertificate,
	now time.Time,
) bool {
	roster, known := n.fastSyncRoster()
	if !known {
		// No masterchain state yet, so no roster to judge the issuer against.
		return false
	}

	if err := certificate.CheckSlot(FastSyncMemberSlotCount); err != nil {
		return false
	}
	if time.Unix(int64(certificate.ExpireAt), 0).Sub(now) < fastSyncCertificateMinRemaining {
		return false
	}

	issuerBytes, err := certificate.IssuerID()
	if err != nil {
		return false
	}
	issuer, err := NewPeerID(issuerBytes)
	if err != nil {
		return false
	}
	if !roster.ContainsRoot(issuer) {
		return false
	}

	// The signature binds the certificate to a node id, so verifying it against
	// our own id is also what proves the certificate was issued for us.
	if err = certificate.CheckSignature(n.localID[:]); err != nil {
		return false
	}
	cloned, err := cloneFastSyncCertificate(certificate)
	if err != nil {
		return false
	}

	return n.storeFastSyncCertificate(issuer, cloned)
}

// storeFastSyncCertificate keeps one certificate per issuer, replacing it only
// when the new one lives longer.
func (n *Node) storeFastSyncCertificate(
	issuer PeerID,
	certificate overlay.MemberCertificate,
) bool {
	n.fastSyncCertificatesMx.Lock()
	defer n.fastSyncCertificatesMx.Unlock()

	current := n.fastSyncCertificates
	for i := range current {
		storedIssuer, err := current[i].IssuerID()
		if err != nil {
			continue
		}
		storedID, err := NewPeerID(storedIssuer)
		if err != nil || storedID != issuer {
			continue
		}
		if certificate.ExpireAt <= current[i].ExpireAt {
			return false
		}
		// Readers take the slice without copying it, so replace by rebuilding
		// rather than assigning into the existing backing array.
		updated := make([]overlay.MemberCertificate, len(current))
		copy(updated, current)
		updated[i] = certificate
		n.fastSyncCertificates = updated
		return true
	}

	if len(current) >= fastSyncCertificateLimit {
		return false
	}
	updated := make([]overlay.MemberCertificate, len(current), len(current)+1)
	copy(updated, current)
	n.fastSyncCertificates = append(updated, certificate)
	return true
}

// fastSyncCertificateSnapshot returns the stored certificates. The slice is
// never mutated in place, so callers may read it without holding the lock.
func (n *Node) fastSyncCertificateSnapshot() []overlay.MemberCertificate {
	n.fastSyncCertificatesMx.Lock()
	defer n.fastSyncCertificatesMx.Unlock()

	return n.fastSyncCertificates
}

func (n *Node) refreshFastSyncOverlays() {
	if err := n.reapplyFastSyncOverlays(); err != nil {
		n.log.Warn().
			Err(err).
			Msg("failed to reapply FastSync overlays after certificate import")
	}
}

func (n *Node) reapplyFastSyncOverlays() error {
	n.fastSyncStateMx.Lock()
	defer n.fastSyncStateMx.Unlock()

	if n.fastSyncState == nil {
		return nil
	}
	if err := n.applyFastSyncOverlaysLocked(*n.fastSyncState); err != nil {
		return fmt.Errorf("reapply FastSync overlays: %w", err)
	}
	return nil
}

// fastSyncRoster returns the roster from the last applied masterchain state.
func (n *Node) fastSyncRoster() (FastSyncValidatorRoster, bool) {
	n.fastSyncStateMx.Lock()
	defer n.fastSyncStateMx.Unlock()

	if n.fastSyncState == nil {
		return FastSyncValidatorRoster{}, false
	}
	return n.fastSyncState.Roster, true
}
