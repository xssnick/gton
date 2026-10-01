package network

import (
	"errors"
	"fmt"

	"github.com/xssnick/tonutils-go/adnl/keys"
	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/tl"
)

// OverlayIdentity is the full private-overlay name and its TON short ID.
// FullID is the boxed overlay seed itself, not its hash. ShortID is
// sha256(boxed pub.overlay{name: FullID}), matching C++ OverlayIdFull.
type OverlayIdentity struct {
	FullID  []byte
	ShortID [32]byte
}

// BuildConsensusOverlayIdentity derives the private consensus overlay from a
// session and its validator signing-key short IDs. Validator order is part of
// the overlay identity and is preserved exactly.
func BuildConsensusOverlayIdentity(
	sessionID [32]byte,
	validatorKeyIDs [][32]byte,
) (OverlayIdentity, error) {
	if len(validatorKeyIDs) == 0 {
		return OverlayIdentity{}, errors.New("validator network: empty consensus overlay roster")
	}

	nodes := make([][]byte, len(validatorKeyIDs))
	for i := range validatorKeyIDs {
		nodes[i] = validatorKeyIDs[i][:]
	}

	fullID, err := tl.Serialize(tonnodeapi.ConsensusOverlayID{
		SessionID: sessionID[:],
		Nodes:     nodes,
	}, true)
	if err != nil {
		return OverlayIdentity{}, fmt.Errorf("validator network: serialize consensus overlay id: %w", err)
	}

	return overlayIdentity(fullID)
}

// BuildBlockSyncOverlayIdentity derives the protocol-v1 candidate broadcast
// overlay from the session ID alone.
func BuildBlockSyncOverlayIdentity(sessionID [32]byte) (OverlayIdentity, error) {
	fullID, err := tl.Serialize(ConsensusBlockSyncOverlayID{SessionID: sessionID[:]}, true)
	if err != nil {
		return OverlayIdentity{}, fmt.Errorf("validator network: serialize block-sync overlay id: %w", err)
	}

	return overlayIdentity(fullID)
}

// OverlayShortID derives the TON short ID for a serialized full overlay name.
func OverlayShortID(fullID []byte) ([32]byte, error) {
	if len(fullID) == 0 {
		return [32]byte{}, errors.New("validator network: empty full overlay id")
	}

	hash, err := tl.Hash(keys.PublicKeyOverlay{Key: fullID})
	if err != nil {
		return [32]byte{}, fmt.Errorf("validator network: hash overlay id: %w", err)
	}

	var shortID [32]byte
	copy(shortID[:], hash)

	return shortID, nil
}

func overlayIdentity(fullID []byte) (OverlayIdentity, error) {
	shortID, err := OverlayShortID(fullID)
	if err != nil {
		return OverlayIdentity{}, err
	}

	return OverlayIdentity{FullID: fullID, ShortID: shortID}, nil
}
