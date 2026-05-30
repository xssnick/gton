package p2p

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
)

const PeerIDSize = 32

type PeerID [PeerIDSize]byte

func NewPeerID(raw []byte) (PeerID, error) {
	if len(raw) != PeerIDSize {
		return PeerID{}, fmt.Errorf("peer id must be %d bytes, got %d", PeerIDSize, len(raw))
	}

	var id PeerID
	copy(id[:], raw)
	return id, nil
}

func (id PeerID) String() string {
	return hex.EncodeToString(id[:])
}

func (id PeerID) Base64() string {
	return base64.StdEncoding.EncodeToString(id[:])
}

func (id PeerID) IsZero() bool {
	return id == PeerID{}
}

func (id PeerID) Bytes() []byte {
	return append([]byte(nil), id[:]...)
}
