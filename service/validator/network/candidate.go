package network

import (
	"crypto/ed25519"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/xssnick/gton/service/validator"
	"github.com/xssnick/gton/service/validator/simplex"

	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

const (
	// CandidateRequestWireSize is the fixed boxed requestCandidate size:
	// request ctor, boxed CandidateId, slot, hash, and two boxed Bool values.
	CandidateRequestWireSize = 52

	requestErrorWireSize = 4
)

// ErrRequestRejected reports an exact boxed consensus.requestError response.
var ErrRequestRejected = errors.New("validator network: peer rejected request")

// EncodeCandidateRequest serializes a validator candidate request. SessionID
// and MaximumReplyBytes are transport context and are intentionally absent from
// the requestCandidate wire value.
func EncodeCandidateRequest(request validator.CandidateRequest) ([]byte, error) {
	if request.MaximumReplyBytes == 0 {
		return nil, errors.New("validator network: zero candidate reply limit")
	}

	wire, err := tl.Append(make([]byte, 0, CandidateRequestWireSize), ConsensusSimplexRequestCandidate{
		ID: ton.ConsensusCandidateID{
			Slot: int32(request.ID.Slot),
			Hash: request.ID.Hash[:],
		},
		WantCandidate: request.WantCandidate,
		WantNotar:     request.WantNotarization,
	}, true)
	if err != nil {
		return nil, fmt.Errorf("validator network: encode candidate request: %w", err)
	}
	if len(wire) != CandidateRequestWireSize {
		return nil, fmt.Errorf("validator network: candidate request encoded to %d bytes", len(wire))
	}

	return wire, nil
}

// DecodeCandidateRequest parses an inbound request and binds the transport
// context that is not carried by requestCandidate itself.
func DecodeCandidateRequest(
	wire []byte,
	sessionID [32]byte,
	maximumReplyBytes uint32,
) (validator.CandidateRequest, error) {
	if maximumReplyBytes == 0 {
		return validator.CandidateRequest{}, errors.New("validator network: zero candidate reply limit")
	}
	if len(wire) != CandidateRequestWireSize {
		return validator.CandidateRequest{}, fmt.Errorf(
			"validator network: candidate request size %d, want %d",
			len(wire), CandidateRequestWireSize,
		)
	}
	if binary.LittleEndian.Uint32(wire) != idRequestCandidate {
		return validator.CandidateRequest{}, fmt.Errorf(
			"validator network: unexpected candidate request constructor %08x",
			binary.LittleEndian.Uint32(wire),
		)
	}
	if !isTLBool(wire[44:48]) || !isTLBool(wire[48:52]) {
		return validator.CandidateRequest{}, errors.New("validator network: invalid candidate request Bool")
	}

	var value ConsensusSimplexRequestCandidate
	rest, err := tl.ParseNoCopy(&value, wire, true)
	if err != nil {
		return validator.CandidateRequest{}, fmt.Errorf("validator network: decode candidate request: %w", err)
	}
	if len(rest) != 0 {
		return validator.CandidateRequest{}, fmt.Errorf(
			"validator network: %d trailing candidate request bytes",
			len(rest),
		)
	}
	if len(value.ID.Hash) != len(simplex.CandidateID{}.Hash) {
		return validator.CandidateRequest{}, fmt.Errorf(
			"validator network: candidate hash length %d",
			len(value.ID.Hash),
		)
	}

	id := simplex.CandidateID{Slot: uint32(value.ID.Slot)}
	copy(id.Hash[:], value.ID.Hash)

	return validator.CandidateRequest{
		SessionID:         sessionID,
		ID:                id,
		WantCandidate:     value.WantCandidate,
		WantNotarization:  value.WantNotar,
		MaximumReplyBytes: maximumReplyBytes,
	}, nil
}

// EncodeCandidateResponse serializes candidateAndCert according to the wants
// in request and enforces the complete reply limit. Notarization is encoded as
// its boxed voteSignatureSet, matching the C++ requestCandidate protocol.
func EncodeCandidateResponse(
	request validator.CandidateRequest,
	response validator.CandidateResponse,
) ([]byte, error) {
	if request.MaximumReplyBytes == 0 {
		return nil, errors.New("validator network: zero candidate reply limit")
	}
	if !request.WantCandidate && len(response.CandidateWire) != 0 {
		return nil, errors.New("validator network: response contains unrequested candidate")
	}
	if !request.WantNotarization && response.Notarization != nil {
		return nil, errors.New("validator network: response contains unrequested notarization")
	}

	var notar []byte
	if response.Notarization != nil {
		if response.Notarization.Vote != simplex.NotarizeVote(request.ID) {
			return nil, errors.New("validator network: notarization vote does not match request")
		}
		if err := validateSignatures(response.Notarization.Signatures, -1); err != nil {
			return nil, err
		}

		signatureSetSize, err := signatureSetWireSize(response.Notarization.Signatures)
		if err != nil {
			return nil, err
		}
		estimated, err := candidateResponseWireSize(len(response.CandidateWire), signatureSetSize)
		if err != nil {
			return nil, err
		}
		if estimated > uint64(request.MaximumReplyBytes) {
			return nil, candidateResponseTooLarge(estimated, request.MaximumReplyBytes)
		}

		notar = response.Notarization.SignatureSetBytes()
	}

	estimated, err := candidateResponseWireSize(len(response.CandidateWire), len(notar))
	if err != nil {
		return nil, err
	}
	if estimated > uint64(request.MaximumReplyBytes) {
		return nil, candidateResponseTooLarge(estimated, request.MaximumReplyBytes)
	}

	wire, err := tl.Append(make([]byte, 0, int(estimated)), ConsensusSimplexCandidateAndCert{
		Candidate: response.CandidateWire,
		Notar:     notar,
	}, true)
	if err != nil {
		return nil, fmt.Errorf("validator network: encode candidate response: %w", err)
	}
	if uint64(len(wire)) != estimated {
		return nil, fmt.Errorf(
			"validator network: candidate response encoded to %d bytes, expected %d",
			len(wire), estimated,
		)
	}

	return wire, nil
}

// DecodeCandidateResponse parses an owned response buffer. On success,
// CandidateWire and certificate signature slices refer directly to wire; the
// caller must keep wire alive and must not mutate or reuse it while the result
// is in use. validatorCount bounds and validates the signature set before it is
// handed to validator verification.
func DecodeCandidateResponse(
	request validator.CandidateRequest,
	wire []byte,
	validatorCount int,
) (validator.CandidateResponse, error) {
	if request.MaximumReplyBytes == 0 {
		return validator.CandidateResponse{}, errors.New("validator network: zero candidate reply limit")
	}
	if validatorCount < 0 {
		return validator.CandidateResponse{}, errors.New("validator network: negative validator count")
	}
	if uint64(len(wire)) > uint64(request.MaximumReplyBytes) {
		return validator.CandidateResponse{}, candidateResponseTooLarge(
			uint64(len(wire)),
			request.MaximumReplyBytes,
		)
	}
	if IsRequestError(wire) {
		return validator.CandidateResponse{}, ErrRequestRejected
	}

	var value ConsensusSimplexCandidateAndCert
	rest, err := tl.ParseNoCopy(&value, wire, true)
	if err != nil {
		return validator.CandidateResponse{}, fmt.Errorf("validator network: decode candidate response: %w", err)
	}
	if len(rest) != 0 {
		return validator.CandidateResponse{}, fmt.Errorf(
			"validator network: %d trailing candidate response bytes",
			len(rest),
		)
	}
	if !request.WantCandidate && len(value.Candidate) != 0 {
		return validator.CandidateResponse{}, errors.New("validator network: peer returned unrequested candidate")
	}
	if !request.WantNotarization && len(value.Notar) != 0 {
		return validator.CandidateResponse{}, errors.New("validator network: peer returned unrequested notarization")
	}

	response := validator.CandidateResponse{CandidateWire: value.Candidate}
	if len(value.Notar) == 0 {
		return response, nil
	}

	signatures, err := decodeSignatureSet(value.Notar, validatorCount)
	if err != nil {
		return validator.CandidateResponse{}, err
	}
	response.Notarization = &simplex.Certificate{
		Vote:       simplex.NotarizeVote(request.ID),
		Signatures: signatures,
	}

	return response, nil
}

// EncodeRequestError returns a fresh boxed consensus.requestError value.
func EncodeRequestError() []byte {
	return binary.LittleEndian.AppendUint32(nil, idRequestError)
}

// IsRequestError reports whether wire is exactly one boxed
// consensus.requestError value. A matching prefix with trailing bytes is not a
// valid request error.
func IsRequestError(wire []byte) bool {
	return len(wire) == requestErrorWireSize && binary.LittleEndian.Uint32(wire) == idRequestError
}

// IsCandidateRequest reports the requestCandidate constructor prefix. Shape
// and trailing bytes are validated by DecodeCandidateRequest.
func IsCandidateRequest(wire []byte) bool {
	return len(wire) >= 4 && binary.LittleEndian.Uint32(wire) == idRequestCandidate
}

func decodeSignatureSet(wire []byte, validatorCount int) ([]simplex.VoteSignature, error) {
	if len(wire) < 8 {
		return nil, errors.New("validator network: truncated notarization signature set")
	}

	count := binary.LittleEndian.Uint32(wire[4:8])
	if uint64(count) > uint64(validatorCount) {
		return nil, fmt.Errorf(
			"validator network: notarization has %d signatures for %d validators",
			count, validatorCount,
		)
	}

	var value simplex.ConsensusSimplexVoteSignatureSet
	rest, err := tl.ParseNoCopy(&value, wire, true)
	if err != nil {
		return nil, fmt.Errorf("validator network: decode notarization signature set: %w", err)
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf(
			"validator network: %d trailing notarization bytes",
			len(rest),
		)
	}

	signatures := make([]simplex.VoteSignature, len(value.Votes))
	for i := range value.Votes {
		signatures[i] = simplex.VoteSignature{
			ValidatorIndex: uint32(value.Votes[i].Who),
			Signature:      value.Votes[i].Signature,
		}
	}
	if err = validateSignatures(signatures, validatorCount); err != nil {
		return nil, err
	}

	return signatures, nil
}

func validateSignatures(signatures []simplex.VoteSignature, validatorCount int) error {
	seen := make(map[uint32]struct{}, len(signatures))
	for _, signature := range signatures {
		if validatorCount >= 0 && uint64(signature.ValidatorIndex) >= uint64(validatorCount) {
			return fmt.Errorf(
				"validator network: notarization validator index %d exceeds count %d",
				signature.ValidatorIndex, validatorCount,
			)
		}
		if _, ok := seen[signature.ValidatorIndex]; ok {
			return fmt.Errorf(
				"validator network: duplicate notarization validator index %d",
				signature.ValidatorIndex,
			)
		}
		seen[signature.ValidatorIndex] = struct{}{}

		if len(signature.Signature) != ed25519.SignatureSize {
			return fmt.Errorf(
				"validator network: notarization signature length %d, want %d",
				len(signature.Signature), ed25519.SignatureSize,
			)
		}
	}

	return nil
}

func isTLBool(wire []byte) bool {
	id := binary.LittleEndian.Uint32(wire)

	return id == idBoolTrue || id == idBoolFalse
}

func signatureSetWireSize(signatures []simplex.VoteSignature) (int, error) {
	// voteSignatureSet ctor + vector count. Each entry has a boxed
	// voteSignature ctor, who:int, and one TL bytes field.
	size := uint64(8)
	for _, signature := range signatures {
		fieldSize, err := tlBytesWireSize(len(signature.Signature))
		if err != nil {
			return 0, err
		}
		size += 8 + fieldSize
	}
	if size > uint64(^uint(0)>>1) {
		return 0, errors.New("validator network: notarization size overflows int")
	}

	return int(size), nil
}

func candidateResponseWireSize(candidateBytes, notarBytes int) (uint64, error) {
	if candidateBytes < 0 || notarBytes < 0 {
		return 0, errors.New("validator network: candidate response size overflow")
	}

	candidateSize, err := tlBytesWireSize(candidateBytes)
	if err != nil {
		return 0, err
	}
	notarSize, err := tlBytesWireSize(notarBytes)
	if err != nil {
		return 0, err
	}

	return 4 + candidateSize + notarSize, nil
}

func tlBytesWireSize(size int) (uint64, error) {
	if size < 0 {
		return 0, errors.New("validator network: negative TL bytes size")
	}

	n := uint64(size)
	var prefix uint64
	switch {
	case n < 0xfe:
		prefix = 1
	case n < 1<<24:
		prefix = 4
	case n < 1<<32:
		prefix = 8
	default:
		return 0, fmt.Errorf("validator network: TL bytes size %d exceeds uint32", n)
	}

	encoded := prefix + n
	if remainder := encoded % 4; remainder != 0 {
		encoded += 4 - remainder
	}

	return encoded, nil
}

func candidateResponseTooLarge(size uint64, limit uint32) error {
	return fmt.Errorf(
		"validator network: candidate response size %d exceeds limit %d",
		size, limit,
	)
}
