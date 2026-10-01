package fastsync

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	IDSize          = 32
	memberSlotCount = 5

	validatorPublicKeyConstructor = uint32(0x4813b4c6)
)

var (
	ErrNotFound    = errors.New("fast sync: not found")
	ErrPeerIsLocal = errors.New("fast sync peer runtime: local node descriptor")
)

// ID32 lets cold construction and hashing boundaries accept a caller's named
// identifier without making this package depend on its parent p2p package.
// Converting these fixed-size wire values never allocates.
type ID32 interface {
	~[IDSize]byte
}

// ID is a FastSync peer or certificate-issuer identifier. Keeping mutable
// hot-path owners concrete lets the compiler inline their methods without a
// generic dictionary lookup.
type ID [IDSize]byte

// OverlayID identifies a FastSync overlay on the wire.
type OverlayID [IDSize]byte

func ValidatorID[Target ID32](publicKey [ed25519.PublicKeySize]byte) Target {
	var boxed [4 + ed25519.PublicKeySize]byte
	binary.LittleEndian.PutUint32(boxed[0:4], validatorPublicKeyConstructor)
	copy(boxed[4:], publicKey[:])
	return Target(sha256.Sum256(boxed[:]))
}

func idFromBytes(raw []byte) (ID, error) {
	var id ID
	if len(raw) != len(id) {
		return id, fmt.Errorf(
			"fast sync identifier must be %d bytes, got %d",
			len(id),
			len(raw),
		)
	}

	copy(id[:], raw)
	return id, nil
}

func idIsZero(id ID) bool {
	return id == ID{}
}
