package p2p

import (
	"context"
	"errors"
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

var errFastSyncCertificateNotStored = errors.New("fast sync certificate was not stored")

// NewFastSyncMemberCertificate is a validator pushing us a fresh FastSync
// membership certificate. Validators reissue every minute with an hour of
// lifetime. Accepted pushes are persisted so a restarted node can rejoin
// FastSync before the next refresh arrives.
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
	if err := n.importFastSyncCertificate(certificate, time.Now()); err != nil {
		if !errors.Is(err, errFastSyncCertificateNotStored) {
			return err
		}
		return nil
	}

	// Overlays are otherwise reconciled only while applying a masterchain block.
	// A node whose certificate already expired has no FastSync overlay to
	// receive blocks on, so waiting for the next apply would never happen.
	n.runAsync(n.refreshFastSyncOverlays)
	return nil
}

// importFastSyncCertificate validates a pushed certificate and stores it when it
// improves on what we hold. errFastSyncCertificateNotStored means the incoming
// value was invalid or did not improve the stored set.
//
// Checks run cheapest-first so that an unauthenticated sender cannot make us
// verify signatures for free; the reference node checks the signature first, and
// it does not have this path open to arbitrary peers.
func (n *Node) importFastSyncCertificate(
	certificate overlay.MemberCertificate,
	now time.Time,
) error {
	roster, known := n.fastSyncRoster()
	if !known {
		// No masterchain state yet, so no roster to judge the issuer against.
		return errFastSyncCertificateNotStored
	}

	if err := certificate.CheckSlot(FastSyncMemberSlotCount); err != nil {
		return errFastSyncCertificateNotStored
	}
	if time.Unix(int64(certificate.ExpireAt), 0).Sub(now) < fastSyncCertificateMinRemaining {
		return errFastSyncCertificateNotStored
	}

	issuerBytes, err := certificate.IssuerID()
	if err != nil {
		return errFastSyncCertificateNotStored
	}
	issuer, err := NewPeerID(issuerBytes)
	if err != nil {
		return errFastSyncCertificateNotStored
	}
	if !roster.ContainsRoot(issuer) {
		return errFastSyncCertificateNotStored
	}

	// The signature binds the certificate to a node id, so verifying it against
	// our own id is also what proves the certificate was issued for us.
	if err = certificate.CheckSignature(n.localID[:]); err != nil {
		return errFastSyncCertificateNotStored
	}
	cloned, err := cloneFastSyncCertificate(certificate)
	if err != nil {
		return errFastSyncCertificateNotStored
	}

	return n.storeFastSyncCertificate(issuer, cloned, roster, now)
}

// storeFastSyncCertificate keeps one certificate per issuer, replacing it only
// when the new one lives longer.
func (n *Node) storeFastSyncCertificate(
	issuer PeerID,
	certificate overlay.MemberCertificate,
	roster FastSyncValidatorRoster,
	now time.Time,
) error {
	n.fastSyncCertificatesMx.Lock()
	defer n.fastSyncCertificatesMx.Unlock()

	current := n.fastSyncCertificates
	updated := make([]overlay.MemberCertificate, 0, len(current)+1)
	replaced := false
	for i := range current {
		if current[i].IsExpired(now) {
			continue
		}

		storedIssuer, err := current[i].IssuerID()
		if err != nil {
			updated = append(updated, current[i])
			continue
		}
		storedID, err := NewPeerID(storedIssuer)
		if err != nil || storedID != issuer {
			updated = append(updated, current[i])
			continue
		}
		if certificate.ExpireAt <= current[i].ExpireAt {
			return errFastSyncCertificateNotStored
		}

		updated = append(updated, certificate)
		replaced = true
	}

	if !replaced {
		if len(updated) >= fastSyncCertificateLimit {
			// Only a certificate whose issuer is still in the routing rosters is
			// usable (fastSyncLocalCertificate filters on exactly that), so make
			// room by dropping one that is not. Without this the set can fill
			// with unexpired-but-useless certificates after a validator-set
			// rotation and refuse every replacement until they time out, taking
			// the node's fast-sync overlays down for as long as an hour.
			if !dropUnroutedFastSyncCertificate(&updated, roster) {
				return errFastSyncCertificateNotStored
			}
		}
		updated = append(updated, certificate)
	}

	if err := n.persistFastSyncCertificates(context.Background(), updated); err != nil {
		return fmt.Errorf("persist fast sync certificates: %w", err)
	}

	n.fastSyncCertificates = updated
	return nil
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

// dropUnroutedFastSyncCertificate removes the first certificate whose issuer is
// no longer one of the routing rosters' roots, reporting whether it found one.
func dropUnroutedFastSyncCertificate(
	certificates *[]overlay.MemberCertificate,
	roster FastSyncValidatorRoster,
) bool {
	stored := *certificates
	for i := range stored {
		issuerBytes, err := stored[i].IssuerID()
		if err != nil {
			// Unreadable issuer: it can never match a roster root either.
			*certificates = append(stored[:i:i], stored[i+1:]...)
			return true
		}
		issuer, err := NewPeerID(issuerBytes)
		if err != nil || !roster.ContainsRoot(issuer) {
			*certificates = append(stored[:i:i], stored[i+1:]...)
			return true
		}
	}
	return false
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
