package network

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/xssnick/gton/service/validator"
	"github.com/xssnick/gton/service/validator/simplex"

	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/tl"
)

func TestConstructorIDsGolden(t *testing.T) {
	golden := map[string]uint32{
		"consensus.overlayId":                     0x21298d9c,
		"consensus.requestError":                  0x832d2574,
		"consensus.simplex.candidateAndCert":      0xd2462c2c,
		"consensus.simplex.requestCandidate":      0x543fba6c,
		"consensus.candidateId dependency":        0xb691cd3f,
		"consensus.simplex.voteSignatureSet dep.": 0x365ee1f8,
	}
	got := map[string]uint32{
		"consensus.overlayId":                     tl.CRC(schemeConsensusOverlayID),
		"consensus.requestError":                  idRequestError,
		"consensus.simplex.candidateAndCert":      tl.CRC(schemeCandidateAndCert),
		"consensus.simplex.requestCandidate":      idRequestCandidate,
		"consensus.candidateId dependency":        tl.CRC("consensus.candidateId slot:int hash:int256 = consensus.CandidateId"),
		"consensus.simplex.voteSignatureSet dep.": tl.CRC("consensus.simplex.voteSignatureSet votes:(vector consensus.simplex.VoteSignature) = consensus.simplex.VoteSignatureSet"),
	}

	for name, want := range golden {
		if got[name] != want {
			t.Errorf("%s constructor = 0x%08x, want 0x%08x", name, got[name], want)
		}
	}
}

func TestWireGolden(t *testing.T) {
	request := candidateRequest(7, 0xab, true, false, 1024)
	requestWire, err := EncodeCandidateRequest(request)
	if err != nil {
		t.Fatal(err)
	}

	candidateAndCertWire := mustSerializeTL(t, ConsensusSimplexCandidateAndCert{
		Candidate: []byte{1, 2, 3},
		Notar:     []byte{4, 5},
	})

	golden := map[string][2]string{
		"requestCandidate": {
			hex.EncodeToString(requestWire),
			"6cba3f543fcd91b607000000ababababababababababababababababababababababababababababababababb5757299379779bc",
		},
		"candidateAndCert bytes": {
			hex.EncodeToString(candidateAndCertWire),
			"2c2c46d20301020302040500",
		},
		"requestError": {
			hex.EncodeToString(EncodeRequestError()),
			"74252d83",
		},
	}
	for name, pair := range golden {
		if pair[0] != pair[1] {
			t.Errorf("%s wire changed:\n got %s\nwant %s", name, pair[0], pair[1])
		}
	}
}

func TestConsensusOverlayIdentityGolden(t *testing.T) {
	sessionID := filledID(0x11)
	nodes := [][32]byte{filledID(0x22), filledID(0x33)}

	identity, err := BuildConsensusOverlayIdentity(sessionID, nodes)
	if err != nil {
		t.Fatal(err)
	}

	wantFull := mustHex(t,
		"9c8d2921"+
			strings.Repeat("11", 32)+
			"02000000"+
			strings.Repeat("22", 32)+
			strings.Repeat("33", 32),
	)
	wantShort := mustHex(t, "6416108dffcf70b58f3f80b57772605e5d4219f38866b13b1c4c234630b3e4c8")
	if !bytes.Equal(identity.FullID, wantFull) {
		t.Fatalf("full consensus overlay id:\n got %x\nwant %x", identity.FullID, wantFull)
	}
	if !bytes.Equal(identity.ShortID[:], wantShort) {
		t.Fatalf("short consensus overlay id:\n got %x\nwant %x", identity.ShortID, wantShort)
	}

	reversed, err := BuildConsensusOverlayIdentity(sessionID, [][32]byte{nodes[1], nodes[0]})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(reversed.FullID, identity.FullID) || reversed.ShortID == identity.ShortID {
		t.Fatal("validator roster order must be part of the overlay identity")
	}

	if _, err = BuildConsensusOverlayIdentity(sessionID, nil); err == nil {
		t.Fatal("expected empty validator roster error")
	}

	shortID, err := OverlayShortID(identity.FullID)
	if err != nil {
		t.Fatal(err)
	}
	if shortID != identity.ShortID {
		t.Fatal("OverlayShortID disagrees with identity builder")
	}
	if _, err = OverlayShortID(nil); err == nil {
		t.Fatal("expected empty full overlay id error")
	}
}

func TestOverlayIDRegistrationRemainsTonutilsType(t *testing.T) {
	identity, err := BuildConsensusOverlayIdentity(filledID(1), [][32]byte{filledID(2)})
	if err != nil {
		t.Fatal(err)
	}

	var parsed any
	rest, err := tl.Parse(&parsed, identity.FullID, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(rest) != 0 {
		t.Fatalf("overlay parse left %d bytes", len(rest))
	}
	if _, ok := parsed.(tonnodeapi.ConsensusOverlayID); !ok {
		t.Fatalf("overlay constructor globally resolves to %T, want node.ConsensusOverlayID", parsed)
	}
}

func TestCandidateRequestRoundTrip(t *testing.T) {
	sessionID := filledID(0x41)
	request := candidateRequest(^uint32(0), 0x52, true, true, 3<<20)
	wire, err := EncodeCandidateRequest(request)
	if err != nil {
		t.Fatal(err)
	}

	decoded, err := DecodeCandidateRequest(wire, sessionID, request.MaximumReplyBytes)
	if err != nil {
		t.Fatal(err)
	}
	request.SessionID = sessionID
	if decoded != request {
		t.Fatalf("decoded request = %#v, want %#v", decoded, request)
	}
}

func TestCandidateRequestRejectsInvalidShape(t *testing.T) {
	request := candidateRequest(1, 2, true, false, 1024)
	wire, err := EncodeCandidateRequest(request)
	if err != nil {
		t.Fatal(err)
	}

	for name, mutate := range map[string]func([]byte) []byte{
		"truncated": func(data []byte) []byte { return data[:len(data)-1] },
		"trailing":  func(data []byte) []byte { return append(data, 0) },
		"constructor": func(data []byte) []byte {
			binary.LittleEndian.PutUint32(data, 0)
			return data
		},
		"want candidate bool": func(data []byte) []byte {
			binary.LittleEndian.PutUint32(data[44:48], 0)
			return data
		},
		"want notar bool": func(data []byte) []byte {
			binary.LittleEndian.PutUint32(data[48:52], 1)
			return data
		},
	} {
		t.Run(name, func(t *testing.T) {
			data := mutate(bytes.Clone(wire))
			if _, err := DecodeCandidateRequest(data, [32]byte{}, 1024); err == nil {
				t.Fatal("expected invalid request error")
			}
		})
	}

	request.MaximumReplyBytes = 0
	if _, err = EncodeCandidateRequest(request); err == nil {
		t.Fatal("expected zero reply limit error")
	}
	if _, err = DecodeCandidateRequest(wire, [32]byte{}, 0); err == nil {
		t.Fatal("expected zero reply limit error")
	}
}

func TestCandidateResponseRoundTripWithoutPayloadCopies(t *testing.T) {
	request := candidateRequest(9, 0x67, true, true, 8192)
	signatureA := bytes.Repeat([]byte{0xa1}, 64)
	signatureB := bytes.Repeat([]byte{0xb2}, 64)
	response := validator.CandidateResponse{
		CandidateWire: []byte{0xde, 0xad, 0xbe, 0xef, 0x42},
		Notarization: &simplex.Certificate{
			Vote: simplex.NotarizeVote(request.ID),
			Signatures: []simplex.VoteSignature{
				{ValidatorIndex: 0, Signature: signatureA},
				{ValidatorIndex: 2, Signature: signatureB},
			},
		},
	}

	wire, err := EncodeCandidateResponse(request, response)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeCandidateResponse(request, wire, 3)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded.CandidateWire, response.CandidateWire) {
		t.Fatal("candidate bytes changed")
	}
	if decoded.Notarization == nil || decoded.Notarization.Vote != response.Notarization.Vote {
		t.Fatal("notarization vote was not reconstructed from the request")
	}
	if len(decoded.Notarization.Signatures) != 2 {
		t.Fatalf("decoded %d signatures, want 2", len(decoded.Notarization.Signatures))
	}

	candidateOffset := bytes.Index(wire, response.CandidateWire)
	if candidateOffset < 0 || &decoded.CandidateWire[0] != &wire[candidateOffset] {
		t.Fatal("candidate payload was copied instead of borrowing response wire")
	}
	signatureOffset := bytes.Index(wire, signatureA)
	if signatureOffset < 0 || &decoded.Notarization.Signatures[0].Signature[0] != &wire[signatureOffset] {
		t.Fatal("signature payload was copied instead of borrowing response wire")
	}

	var envelope ConsensusSimplexCandidateAndCert
	rest, err := tl.ParseNoCopy(&envelope, wire, true)
	if err != nil || len(rest) != 0 {
		t.Fatalf("parse encoded response: rest=%d err=%v", len(rest), err)
	}
	if !bytes.Equal(envelope.Notar, response.Notarization.SignatureSetBytes()) {
		t.Fatal("notar field is not the boxed voteSignatureSet")
	}
}

func TestCandidateResponseEmptyParts(t *testing.T) {
	request := candidateRequest(1, 2, true, true, 12)
	wire, err := EncodeCandidateResponse(request, validator.CandidateResponse{})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := hex.EncodeToString(wire), "2c2c46d20000000000000000"; got != want {
		t.Fatalf("empty response = %s, want %s", got, want)
	}

	response, err := DecodeCandidateResponse(request, wire, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(response.CandidateWire) != 0 || response.Notarization != nil {
		t.Fatalf("empty response decoded as %#v", response)
	}

	request.MaximumReplyBytes = 11
	if _, err = EncodeCandidateResponse(request, validator.CandidateResponse{}); err == nil {
		t.Fatal("expected complete response size limit error")
	}
}

func TestRequestErrorDiscrimination(t *testing.T) {
	first := EncodeRequestError()
	if !IsRequestError(first) {
		t.Fatal("boxed requestError was not recognized")
	}
	first[0] ^= 0xff
	if !IsRequestError(EncodeRequestError()) {
		t.Fatal("EncodeRequestError returned shared mutable storage")
	}

	request := candidateRequest(1, 2, true, true, 1024)
	_, err := DecodeCandidateResponse(request, EncodeRequestError(), 1)
	if !errors.Is(err, ErrRequestRejected) {
		t.Fatalf("request error decoded as %v", err)
	}

	withTrailing := append(EncodeRequestError(), 0)
	if IsRequestError(withTrailing) {
		t.Fatal("requestError with trailing data was accepted")
	}
	_, err = DecodeCandidateResponse(request, withTrailing, 1)
	if err == nil || errors.Is(err, ErrRequestRejected) {
		t.Fatalf("malformed requestError decoded as %v", err)
	}
}

func TestCandidateResponseRejectsRequestMismatchAndLimits(t *testing.T) {
	request := candidateRequest(1, 2, false, false, 1024)
	if _, err := EncodeCandidateResponse(request, validator.CandidateResponse{CandidateWire: []byte{1}}); err == nil {
		t.Fatal("encoded an unrequested candidate")
	}
	if _, err := EncodeCandidateResponse(request, validator.CandidateResponse{
		Notarization: &simplex.Certificate{Vote: simplex.NotarizeVote(request.ID)},
	}); err == nil {
		t.Fatal("encoded an unrequested notarization")
	}

	request.WantCandidate = true
	request.WantNotarization = true
	wrongID := request.ID
	wrongID.Slot++
	if _, err := EncodeCandidateResponse(request, validator.CandidateResponse{
		Notarization: &simplex.Certificate{Vote: simplex.NotarizeVote(wrongID)},
	}); err == nil {
		t.Fatal("encoded a notarization for another candidate")
	}

	unrequestedCandidate := mustSerializeTL(t, ConsensusSimplexCandidateAndCert{Candidate: []byte{1}})
	request.WantCandidate = false
	if _, err := DecodeCandidateResponse(request, unrequestedCandidate, 1); err == nil {
		t.Fatal("decoded an unrequested candidate")
	}

	request.WantCandidate = true
	request.MaximumReplyBytes = uint32(len(unrequestedCandidate) - 1)
	if _, err := DecodeCandidateResponse(request, unrequestedCandidate, 1); err == nil {
		t.Fatal("decoded an oversized candidate response")
	}

	request.MaximumReplyBytes = 1024
	if _, err := DecodeCandidateResponse(request, append(unrequestedCandidate, 0), 1); err == nil {
		t.Fatal("decoded a response with trailing bytes")
	}
	if _, err := DecodeCandidateResponse(request, unrequestedCandidate, -1); err == nil {
		t.Fatal("accepted a negative validator count")
	}
}

func TestCandidateResponseRejectsInvalidSignatureSetShape(t *testing.T) {
	request := candidateRequest(1, 2, true, true, 4096)
	validSignature := bytes.Repeat([]byte{0x55}, 64)

	tests := []struct {
		name  string
		votes []simplex.ConsensusSimplexVoteSignature
		count int
	}{
		{
			name:  "signature length",
			votes: []simplex.ConsensusSimplexVoteSignature{{Who: 0, Signature: validSignature[:63]}},
			count: 1,
		},
		{
			name:  "validator index",
			votes: []simplex.ConsensusSimplexVoteSignature{{Who: 1, Signature: validSignature}},
			count: 1,
		},
		{
			name: "duplicate validator",
			votes: []simplex.ConsensusSimplexVoteSignature{
				{Who: 0, Signature: validSignature},
				{Who: 0, Signature: validSignature},
			},
			count: 2,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			notar := mustSerializeTL(t, simplex.ConsensusSimplexVoteSignatureSet{Votes: test.votes})
			wire := mustSerializeTL(t, ConsensusSimplexCandidateAndCert{Notar: notar})
			if _, err := DecodeCandidateResponse(request, wire, test.count); err == nil {
				t.Fatal("expected invalid signature set error")
			}
		})
	}

	// Count is checked before TL allocates the declared vector.
	declaredTooMany := make([]byte, 8)
	binary.LittleEndian.PutUint32(declaredTooMany, tl.CRC(
		"consensus.simplex.voteSignatureSet votes:(vector consensus.simplex.VoteSignature) = consensus.simplex.VoteSignatureSet",
	))
	binary.LittleEndian.PutUint32(declaredTooMany[4:], 3)
	wire := mustSerializeTL(t, ConsensusSimplexCandidateAndCert{Notar: declaredTooMany})
	if _, err := DecodeCandidateResponse(request, wire, 2); err == nil {
		t.Fatal("accepted signature count above validator count")
	}

	badLocal := validator.CandidateResponse{Notarization: &simplex.Certificate{
		Vote: simplex.NotarizeVote(request.ID),
		Signatures: []simplex.VoteSignature{
			{ValidatorIndex: 0, Signature: validSignature},
			{ValidatorIndex: 0, Signature: validSignature},
		},
	}}
	if _, err := EncodeCandidateResponse(request, badLocal); err == nil {
		t.Fatal("encoded duplicate local signatures")
	}
}

func TestRegisteredConstructorsDecodeByID(t *testing.T) {
	request := candidateRequest(1, 2, true, false, 1024)
	requestWire, err := EncodeCandidateRequest(request)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		wire []byte
		want any
	}{
		{"candidate response", mustSerializeTL(t, ConsensusSimplexCandidateAndCert{}), ConsensusSimplexCandidateAndCert{}},
		{"candidate request", requestWire, ConsensusSimplexRequestCandidate{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var value any
			rest, err := tl.Parse(&value, test.wire, true)
			if err != nil {
				t.Fatal(err)
			}
			if len(rest) != 0 {
				t.Fatalf("constructor parse left %d bytes", len(rest))
			}
			if typeName(value) != typeName(test.want) {
				t.Fatalf("decoded %T, want %T", value, test.want)
			}
		})
	}
}

func candidateRequest(
	slot uint32,
	hashByte byte,
	wantCandidate bool,
	wantNotarization bool,
	maximumReplyBytes uint32,
) validator.CandidateRequest {
	id := simplex.CandidateID{Slot: slot, Hash: filledID(hashByte)}

	return validator.CandidateRequest{
		ID:                id,
		WantCandidate:     wantCandidate,
		WantNotarization:  wantNotarization,
		MaximumReplyBytes: maximumReplyBytes,
	}
}

func filledID(value byte) [32]byte {
	return [32]byte(bytes.Repeat([]byte{value}, 32))
}

func mustSerializeTL(t *testing.T, value any) []byte {
	t.Helper()

	wire, err := tl.Serialize(value, true)
	if err != nil {
		t.Fatal(err)
	}

	return wire
}

func mustHex(t *testing.T, value string) []byte {
	t.Helper()

	decoded, err := hex.DecodeString(value)
	if err != nil {
		t.Fatal(err)
	}

	return decoded
}

func typeName(value any) string {
	return fmt.Sprintf("%T", value)
}
