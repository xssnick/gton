package network

import (
	"crypto/ed25519"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/xssnick/tonutils-go/tl"

	"github.com/xssnick/gton/service/validator/simplex"
)

// EncodePleaseCollatePrepare serializes one Delegated-v3 availability probe.
func EncodePleaseCollatePrepare(request simplex.ConsensusPleaseCollatePrepare) ([]byte, error) {
	if request.WindowStartSlot < 0 {
		return nil, errors.New("validator network: negative delegation window")
	}
	wire, err := tl.Serialize(request, true)
	if err != nil {
		return nil, fmt.Errorf("validator network: encode pleaseCollatePrepare: %w", err)
	}
	return wire, nil
}

// DecodePleaseCollatePrepare parses one exact Delegated-v3 availability probe.
func DecodePleaseCollatePrepare(wire []byte) (simplex.ConsensusPleaseCollatePrepare, error) {
	if !IsPleaseCollatePrepare(wire) {
		return simplex.ConsensusPleaseCollatePrepare{}, errors.New("validator network: unexpected pleaseCollatePrepare constructor")
	}
	var request simplex.ConsensusPleaseCollatePrepare
	rest, err := tl.ParseNoCopy(&request, wire, true)
	if err != nil {
		return simplex.ConsensusPleaseCollatePrepare{}, fmt.Errorf("validator network: decode pleaseCollatePrepare: %w", err)
	}
	if len(rest) != 0 {
		return simplex.ConsensusPleaseCollatePrepare{}, errors.New("validator network: trailing pleaseCollatePrepare bytes")
	}
	if request.WindowStartSlot < 0 {
		return simplex.ConsensusPleaseCollatePrepare{}, errors.New("validator network: negative delegation window")
	}
	return request, nil
}

// IsPleaseCollatePrepare reports the Delegated-v3 probe constructor prefix.
func IsPleaseCollatePrepare(wire []byte) bool {
	return len(wire) >= 4 && binary.LittleEndian.Uint32(wire) == idPleaseCollatePrepare
}

// EncodePleaseCollate serializes one final Delegated-v3 window delegation.
func EncodePleaseCollate(request simplex.ConsensusPleaseCollate) ([]byte, error) {
	if request.WindowStartSlot < 0 {
		return nil, errors.New("validator network: negative delegation window")
	}
	if len(request.Signature) != ed25519.SignatureSize {
		return nil, fmt.Errorf("validator network: delegation signature has size %d", len(request.Signature))
	}
	wire, err := tl.Serialize(request, true)
	if err != nil {
		return nil, fmt.Errorf("validator network: encode pleaseCollate: %w", err)
	}
	return wire, nil
}

// DecodePleaseCollate parses one exact final Delegated-v3 window delegation.
func DecodePleaseCollate(wire []byte) (simplex.ConsensusPleaseCollate, error) {
	if !IsPleaseCollate(wire) {
		return simplex.ConsensusPleaseCollate{}, errors.New("validator network: unexpected pleaseCollate constructor")
	}
	var request simplex.ConsensusPleaseCollate
	rest, err := tl.ParseNoCopy(&request, wire, true)
	if err != nil {
		return simplex.ConsensusPleaseCollate{}, fmt.Errorf("validator network: decode pleaseCollate: %w", err)
	}
	if len(rest) != 0 {
		return simplex.ConsensusPleaseCollate{}, errors.New("validator network: trailing pleaseCollate bytes")
	}
	if request.WindowStartSlot < 0 {
		return simplex.ConsensusPleaseCollate{}, errors.New("validator network: negative delegation window")
	}
	if len(request.Signature) != ed25519.SignatureSize {
		return simplex.ConsensusPleaseCollate{}, fmt.Errorf(
			"validator network: delegation signature has size %d",
			len(request.Signature),
		)
	}
	return request, nil
}

// IsPleaseCollate reports the final Delegated-v3 constructor prefix.
func IsPleaseCollate(wire []byte) bool {
	return len(wire) >= 4 && binary.LittleEndian.Uint32(wire) == idPleaseCollate
}
