package p2p

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"slices"

	"github.com/xssnick/gton/service/p2p/internal/fastsync"
)

const (
	FastSyncMemberSlotCount = 5

	// tonNode.fastSyncOverlayId zero_state_file_hash:int256
	// shard:tonNode.shardId = tonNode.FastSyncOverlayId
	fastSyncOverlayIDConstructor = uint32(0x1af10554)

	// pub.overlay name:bytes = PublicKey
	fastSyncOverlayPublicKeyConstructor = uint32(0x34ba45cb)
)

var (
	ErrFastSyncNotFound = errors.New("fast sync: not found")
	ErrFastSyncNotReady = errors.New("fast sync: not ready")
)

type FastSyncFileHash [PeerIDSize]byte

type FastSyncOverlayFullID [PeerIDSize]byte

type FastSyncOverlayShortID [PeerIDSize]byte

type FastSyncValidatorPublicKey [ed25519.PublicKeySize]byte

type FastSyncShard struct {
	Workchain int32
	Shard     int64
}

type FastSyncOverlayIdentity struct {
	FullID  FastSyncOverlayFullID
	ShortID FastSyncOverlayShortID
}

func NewFastSyncOverlayIdentity(
	zeroStateFileHash FastSyncFileHash,
	shard FastSyncShard,
) FastSyncOverlayIdentity {
	// tonNode.shardId is a concrete nested TL type and is serialized bare.
	var boxed [4 + PeerIDSize + 4 + 8]byte
	binary.LittleEndian.PutUint32(boxed[0:4], fastSyncOverlayIDConstructor)
	copy(boxed[4:4+PeerIDSize], zeroStateFileHash[:])
	binary.LittleEndian.PutUint32(
		boxed[4+PeerIDSize:4+PeerIDSize+4],
		uint32(shard.Workchain),
	)
	binary.LittleEndian.PutUint64(
		boxed[4+PeerIDSize+4:],
		uint64(shard.Shard),
	)
	fullID := FastSyncOverlayFullID(sha256.Sum256(boxed[:]))

	return FastSyncOverlayIdentity{
		FullID:  fullID,
		ShortID: fastSyncOverlayShortID(fullID),
	}
}

func fastSyncOverlayShortID(
	fullID FastSyncOverlayFullID,
) FastSyncOverlayShortID {
	// A 32-byte TL bytes field has a one-byte length followed by three bytes
	// of zero padding.
	var boxed [4 + 1 + PeerIDSize + 3]byte
	binary.LittleEndian.PutUint32(
		boxed[0:4],
		fastSyncOverlayPublicKeyConstructor,
	)
	boxed[4] = byte(PeerIDSize)
	copy(boxed[5:5+PeerIDSize], fullID[:])
	return FastSyncOverlayShortID(sha256.Sum256(boxed[:]))
}

type FastSyncValidator struct {
	PublicKey FastSyncValidatorPublicKey
	ADNLID    PeerID
}

type FastSyncValidatorRoster struct {
	rootPublicKeyIDs []PeerID
	adnlIDs          []PeerID
	fingerprint      [sha256.Size]byte
}

func NewFastSyncValidatorRoster(
	previous,
	current,
	next []FastSyncValidator,
) FastSyncValidatorRoster {
	total := len(previous) + len(current) + len(next)
	rootPublicKeyIDs := make([]PeerID, 0, total)
	adnlIDs := make([]PeerID, 0, total)

	for _, validators := range [3][]FastSyncValidator{previous, current, next} {
		for _, validator := range validators {
			validatorID := fastsync.ValidatorID[PeerID](
				validator.PublicKey,
			)
			rootPublicKeyIDs = append(rootPublicKeyIDs, validatorID)
			if validator.ADNLID.IsZero() {
				adnlIDs = append(adnlIDs, validatorID)
			} else {
				adnlIDs = append(adnlIDs, validator.ADNLID)
			}
		}
	}

	roster := FastSyncValidatorRoster{
		rootPublicKeyIDs: sortUniqueFastSyncPeerIDs(rootPublicKeyIDs),
		adnlIDs:          sortUniqueFastSyncPeerIDs(adnlIDs),
	}
	roster.fingerprint = fastSyncRosterFingerprint(
		roster.rootPublicKeyIDs,
		roster.adnlIDs,
	)
	return roster
}

func fastSyncRosterFingerprint(
	rootPublicKeyIDs,
	adnlIDs []PeerID,
) [sha256.Size]byte {
	hash := sha256.New()
	var count [8]byte
	binary.LittleEndian.PutUint64(count[:], uint64(len(rootPublicKeyIDs)))
	_, _ = hash.Write(count[:])
	for _, id := range rootPublicKeyIDs {
		_, _ = hash.Write(id[:])
	}
	binary.LittleEndian.PutUint64(count[:], uint64(len(adnlIDs)))
	_, _ = hash.Write(count[:])
	for _, id := range adnlIDs {
		_, _ = hash.Write(id[:])
	}

	var fingerprint [sha256.Size]byte
	copy(fingerprint[:], hash.Sum(nil))
	return fingerprint
}

func (r FastSyncValidatorRoster) Len() int {
	return len(r.adnlIDs)
}

func (r FastSyncValidatorRoster) Fingerprint() [sha256.Size]byte {
	return r.fingerprint
}

func (r FastSyncValidatorRoster) RootPublicKeyIDs() []PeerID {
	return slices.Clone(r.rootPublicKeyIDs)
}

func (r FastSyncValidatorRoster) ADNLIDs() []PeerID {
	return slices.Clone(r.adnlIDs)
}

// rootCount and adnlIDsRef exist because the exported accessors above clone,
// and three callers run where that allocation is not acceptable: peerLimit on
// the peer-attach path, validatorPingTargets on the ping sweep, and the overlay
// spec build. They stay unexported so a roster handed out of this package can
// still only be read through the cloning accessors.
func (r FastSyncValidatorRoster) rootCount() int {
	return len(r.rootPublicKeyIDs)
}

// rootPublicKeyIDsRef returns the immutable backing slice for state owners that
// copy every ID into their own maps during construction.
func (r FastSyncValidatorRoster) rootPublicKeyIDsRef() []PeerID {
	return r.rootPublicKeyIDs
}

// adnlIDsRef returns the backing slice. Callers must not mutate or retain it
// beyond the roster's lifetime.
func (r FastSyncValidatorRoster) adnlIDsRef() []PeerID {
	return r.adnlIDs
}

func (r FastSyncValidatorRoster) ContainsADNL(id PeerID) bool {
	_, found := slices.BinarySearchFunc(r.adnlIDs, id, compareFastSyncPeerID)
	return found
}

func (r FastSyncValidatorRoster) ContainsRoot(id PeerID) bool {
	_, found := slices.BinarySearchFunc(
		r.rootPublicKeyIDs,
		id,
		compareFastSyncPeerID,
	)
	return found
}

func sortUniqueFastSyncPeerIDs(ids []PeerID) []PeerID {
	slices.SortFunc(ids, compareFastSyncPeerID)
	return slices.Compact(ids)
}

func compareFastSyncPeerID(left, right PeerID) int {
	return bytes.Compare(left[:], right[:])
}
