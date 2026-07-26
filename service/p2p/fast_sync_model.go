package p2p

import (
	"bytes"
	"cmp"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math/bits"
	"slices"
)

const (
	FastSyncMemberSlotCount = 5

	// tonNode.fastSyncOverlayId zero_state_file_hash:int256
	// shard:tonNode.shardId = tonNode.FastSyncOverlayId
	fastSyncOverlayIDConstructor = uint32(0x1af10554)

	// pub.overlay name:bytes = PublicKey
	fastSyncOverlayPublicKeyConstructor = uint32(0x34ba45cb)

	// pub.ed25519 key:int256 = PublicKey
	fastSyncValidatorPublicKeyConstructor = uint32(0x4813b4c6)
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
			validatorID := fastSyncValidatorShortID(validator.PublicKey)
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

func fastSyncValidatorShortID(publicKey FastSyncValidatorPublicKey) PeerID {
	var boxed [4 + ed25519.PublicKeySize]byte
	binary.LittleEndian.PutUint32(
		boxed[0:4],
		fastSyncValidatorPublicKeyConstructor,
	)
	copy(boxed[4:], publicKey[:])
	return PeerID(sha256.Sum256(boxed[:]))
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

type FastSyncShardSet struct {
	shards []FastSyncShard
}

func NewFastSyncShardSet(shards []FastSyncShard) FastSyncShardSet {
	shards = slices.Clone(shards)
	slices.SortFunc(shards, compareFastSyncShard)
	shards = slices.Compact(shards)
	return FastSyncShardSet{shards: shards}
}

func (s FastSyncShardSet) Shards() []FastSyncShard {
	return slices.Clone(s.shards)
}

func (s FastSyncShardSet) Select(requested FastSyncShard) (FastSyncShard, error) {
	current := requested
	for {
		if s.contains(current) {
			return current, nil
		}
		if fastSyncShardPrefixLength(current.Shard) == 0 {
			return FastSyncShard{}, ErrFastSyncNotFound
		}

		current.Shard = fastSyncShardParent(current.Shard)
	}
}

func (s FastSyncShardSet) contains(shard FastSyncShard) bool {
	_, found := slices.BinarySearchFunc(s.shards, shard, compareFastSyncShard)
	return found
}

func compareFastSyncShard(left, right FastSyncShard) int {
	if order := cmp.Compare(left.Workchain, right.Workchain); order != 0 {
		return order
	}
	return cmp.Compare(uint64(left.Shard), uint64(right.Shard))
}

func fastSyncShardPrefixLength(shard int64) int {
	if shard == 0 {
		return 0
	}

	lowBit := uint64(shard) & -uint64(shard)
	return 63 - bits.TrailingZeros64(lowBit)
}

func fastSyncShardParent(shard int64) int64 {
	value := uint64(shard)
	lowBit := value & -value
	return int64((value - lowBit) | (lowBit << 1))
}
