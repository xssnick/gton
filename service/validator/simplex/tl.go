package simplex

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"github.com/xssnick/gton/service/blockproof"

	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

// The wire layer reuses the shared tonutils-go TL machinery: types that
// already exist there (consensus.candidateId, consensus.dataToSign,
// consensus.simplex.finalizeVote, consensus.candidateHashData*,
// tonNode.blockIdExt) are used directly from the ton package, and the
// missing consensus.simplex.* constructors are registered here. The notarize
// vote keeps using blockproof's released public TL type.
//
// Scheme strings are exact lines of ton_api.tl (trailing ';' stripped); golden constructor ids are pinned in tl_test.go.
const (
	schemeNotarizeVote     = "consensus.simplex.notarizeVote id:consensus.CandidateId = consensus.simplex.UnsignedVote"
	schemeFinalizeVote     = "consensus.simplex.finalizeVote id:consensus.CandidateId = consensus.simplex.UnsignedVote"
	schemeSkipVote         = "consensus.simplex.skipVote slot:int = consensus.simplex.UnsignedVote"
	schemeSignedVote       = "consensus.simplex.vote vote:consensus.simplex.UnsignedVote signature:bytes = consensus.simplex.Vote"
	schemeVoteSignature    = "consensus.simplex.voteSignature who:int signature:bytes = consensus.simplex.VoteSignature"
	schemeVoteSignatureSet = "consensus.simplex.voteSignatureSet votes:(vector consensus.simplex.VoteSignature) = consensus.simplex.VoteSignatureSet"
	schemeCertificate      = "consensus.simplex.certificate vote:consensus.simplex.UnsignedVote signatures:consensus.simplex.VoteSignatureSet = consensus.simplex.Certificate"

	// Registered by tonutils-go, repeated here only to derive the constructor
	// id the hand-written vote codec dispatches on; the id is pinned in
	// tl_test.go and byte compatibility is proven by the *MatchesTonutils tests.
	schemeCandidateID = "consensus.candidateId slot:int hash:int256 = consensus.CandidateId"

	schemeDbKeyVote      = "consensus.simplex.db.key.vote vote_hash:int256 = consensus.simplex.db.key.Vote"
	schemeDbOurVote      = "consensus.simplex.db.ourVote vote:consensus.simplex.UnsignedVote seqno:long = consensus.simplex.db.Vote"
	schemeDbCert         = "consensus.simplex.db.cert cert:consensus.simplex.Certificate = consensus.simplex.db.Vote"
	schemeDbKeyPoolState = "consensus.simplex.db.key.poolState = consensus.simplex.db.key.PoolState"
	schemeDbPoolState    = "consensus.simplex.db.poolState first_nonannounced_window:int = consensus.simplex.db.PoolState"
)

// ConsensusSimplexSkipVote is consensus.simplex.skipVote.
type ConsensusSimplexSkipVote struct {
	Slot int32 `tl:"int"`
}

// ConsensusSimplexVote is consensus.simplex.vote — a signed vote message.
type ConsensusSimplexVote struct {
	Vote      any    `tl:"struct boxed [consensus.simplex.notarizeVote,consensus.simplex.finalizeVote,consensus.simplex.skipVote]"`
	Signature []byte `tl:"bytes"`
}

// ConsensusSimplexVoteSignature is consensus.simplex.voteSignature.
type ConsensusSimplexVoteSignature struct {
	Who       int32  `tl:"int"`
	Signature []byte `tl:"bytes"`
}

// ConsensusSimplexVoteSignatureSet is consensus.simplex.voteSignatureSet.
type ConsensusSimplexVoteSignatureSet struct {
	Votes []ConsensusSimplexVoteSignature `tl:"vector struct boxed [consensus.simplex.voteSignature]"`
}

// ConsensusSimplexCertificate is consensus.simplex.certificate.
type ConsensusSimplexCertificate struct {
	Vote       any                              `tl:"struct boxed [consensus.simplex.notarizeVote,consensus.simplex.finalizeVote,consensus.simplex.skipVote]"`
	Signatures ConsensusSimplexVoteSignatureSet `tl:"struct boxed"`
}

// Consensus DB record types (consensus.simplex.db.*), byte-compatible with the
// canonical consensus database layout.
type ConsensusSimplexDbKeyVote struct {
	VoteHash []byte `tl:"int256"`
}

type ConsensusSimplexDbOurVote struct {
	Vote  any   `tl:"struct boxed [consensus.simplex.notarizeVote,consensus.simplex.finalizeVote,consensus.simplex.skipVote]"`
	Seqno int64 `tl:"long"`
}

type ConsensusSimplexDbCert struct {
	Cert ConsensusSimplexCertificate `tl:"struct boxed"`
}

type ConsensusSimplexDbKeyPoolState struct{}

type ConsensusSimplexDbPoolState struct {
	FirstNonAnnouncedWindow int32 `tl:"int"`
}

func init() {
	tl.Register(ConsensusSimplexSkipVote{}, schemeSkipVote)
	tl.Register(ConsensusSimplexVote{}, schemeSignedVote)
	tl.Register(ConsensusSimplexVoteSignature{}, schemeVoteSignature)
	tl.Register(ConsensusSimplexVoteSignatureSet{}, schemeVoteSignatureSet)
	tl.Register(ConsensusSimplexCertificate{}, schemeCertificate)
	tl.Register(ConsensusSimplexDbKeyVote{}, schemeDbKeyVote)
	tl.Register(ConsensusSimplexDbOurVote{}, schemeDbOurVote)
	tl.Register(ConsensusSimplexDbCert{}, schemeDbCert)
	tl.Register(ConsensusSimplexDbKeyPoolState{}, schemeDbKeyPoolState)
	tl.Register(ConsensusSimplexDbPoolState{}, schemeDbPoolState)
}

// Constructor ids used for message and record dispatch, registered by init
// above. The three UnsignedVote ids and the candidate id are the dispatch
// table of the hand-written vote codec below.
var (
	idSignedVote  = tl.CRC(schemeSignedVote)
	idCertificate = tl.CRC(schemeCertificate)
	idDbOurVote   = tl.CRC(schemeDbOurVote)
	idDbCert      = tl.CRC(schemeDbCert)

	idNotarizeVote = tl.CRC(schemeNotarizeVote)
	idFinalizeVote = tl.CRC(schemeFinalizeVote)
	idSkipVote     = tl.CRC(schemeSkipVote)
	idCandidateID  = tl.CRC(schemeCandidateID)
)

// mustSerialize wraps tl.Serialize for statically well-formed values; an
// error here is a programming bug (broken tag or registration), not a
// runtime condition.
func mustSerialize(v tl.Serializable) []byte {
	b, err := tl.Serialize(v, true)
	if err != nil {
		panic(fmt.Sprintf("simplex: tl serialize: %v", err))
	}
	return b
}

// mustSerializeSized is mustSerialize with a right-sized destination buffer.
// tl.Serialize always allocates DefaultSerializeBufferSize (1 KiB); the vote
// hot path serializes hundreds of <128-byte messages per slot, so exact
// capacities cut most of the package's garbage. A short hint only costs a
// grow, never truncates.
func mustSerializeSized(size int, v tl.Serializable) []byte {
	b, err := tl.Append(make([]byte, 0, size), v, true)
	if err != nil {
		panic(fmt.Sprintf("simplex: tl serialize: %v", err))
	}
	return b
}

// Wire-size upper bounds for the sized hot-path serializations.
const (
	// notarize/finalize: ctor + boxed candidateId (ctor+slot+hash) = 48;
	// skip is smaller.
	voteWireMax = 48
	// dataToSign: ctor + session id + bytes(votePayload<=48) = 4+32+52 = 88.
	dataToSignWireMax = 96
	// vote message: ctor + unsigned vote + bytes(64-sig) = 4+48+68 = 120.
	signedVoteWireMax = 128
)

// ---- domain <-> TL conversions ----

func tlCandidateID(id CandidateID) ton.ConsensusCandidateID {
	return ton.ConsensusCandidateID{Slot: int32(id.Slot), Hash: id.Hash[:]}
}

// parentToTL converts an optional parent to the boxed
// consensus.CandidateParent union.
func parentToTL(p ParentID) any {
	if !p.Exists {
		return ton.ConsensusCandidateWithoutParents{}
	}
	return ton.ConsensusCandidateParent{ID: tlCandidateID(p.ID)}
}

func candidateIDFromTL(v any) (CandidateID, error) {
	var t ton.ConsensusCandidateID
	switch x := v.(type) {
	case ton.ConsensusCandidateID:
		t = x
	case *ton.ConsensusCandidateID:
		t = *x
	default:
		return CandidateID{}, fmt.Errorf("simplex/tl: unexpected candidate id type %T", v)
	}
	if len(t.Hash) != 32 {
		return CandidateID{}, fmt.Errorf("simplex/tl: invalid candidate hash length %d", len(t.Hash))
	}
	id := CandidateID{Slot: uint32(t.Slot)}
	copy(id.Hash[:], t.Hash)
	return id, nil
}

func voteToTL(v Vote) any {
	switch v.Kind {
	case VoteNotarize:
		return blockproof.ConsensusSimplexNotarizeVote{ID: tlCandidateID(v.ID)}
	case VoteFinalize:
		return ton.ConsensusSimplexFinalizeVote{ID: tlCandidateID(v.ID)}
	case VoteSkip:
		return ConsensusSimplexSkipVote{Slot: int32(v.ID.Slot)}
	}
	panic(fmt.Sprintf("simplex: invalid vote kind %d", v.Kind))
}

func voteFromTL(v any) (Vote, error) {
	switch x := v.(type) {
	case blockproof.ConsensusSimplexNotarizeVote:
		id, err := candidateIDFromTL(x.ID)
		if err != nil {
			return Vote{}, err
		}
		return NotarizeVote(id), nil
	case ton.ConsensusSimplexFinalizeVote:
		id, err := candidateIDFromTL(x.ID)
		if err != nil {
			return Vote{}, err
		}
		return FinalizeVote(id), nil
	case ConsensusSimplexSkipVote:
		return SkipVote(uint32(x.Slot)), nil
	}
	return Vote{}, fmt.Errorf("simplex/tl: unexpected vote type %T", v)
}

// VoteBytes returns the boxed UnsignedVote serialization — the exact payload
// wrapped into consensus.dataToSign for signing.
func VoteBytes(v Vote) []byte {
	return mustSerializeSized(voteWireMax, voteToTL(v))
}

// DataToSign wraps a payload into boxed consensus.dataToSign — the byte
// string that is actually signed and verified.
func DataToSign(sessionID [32]byte, payload []byte) []byte {
	size := dataToSignWireMax
	if len(payload) > voteWireMax {
		// Candidate ids and collation windows stay under the vote bound too,
		// but the helper is public — never under-size for a foreign payload.
		size = 44 + len(payload)
	}
	return mustSerializeSized(size, ton.ConsensusDataToSign{SessionID: sessionID[:], Data: payload})
}

// consensus.simplex.vote is the hottest wire type in the package — a session
// of n validators encodes one and decodes n-1 of them per vote round, twice
// per slot — and its layout is fixed and tiny, so the codec below is written
// out by hand instead of going through the reflection loader. Only the two
// hot-path functions are hand-written: ConsensusSimplexVote stays registered
// as the reference the differential tests in tl_vote_codec_test.go compare
// against, and VoteBytes/DataToSign keep using the reflection path.
//
// The TL bytes field goes through tl.AppendBytes/tl.FromBytes rather than a
// local length encoder. That is deliberate on both sides: the length forms and
// the alignment padding stay byte-identical to the reference encoder, and the
// decoder stays exactly as permissive as the reference — TL bytes padding
// content is never checked (see the note on Certificate.Serialize below), and
// a hand-rolled decoder that got stricter would silently drop frames peers
// consider valid. tl.FromBytes also copies, which the signature needs: it
// outlives the transport frame in slot state and in the journal.

// Serialize returns the boxed consensus.simplex.vote wire message.
func (sv *SignedVote) Serialize() []byte {
	dst := make([]byte, 0, signedVoteWireMax)
	dst = binary.LittleEndian.AppendUint32(dst, idSignedVote)
	dst = appendUnsignedVote(dst, sv.Vote)
	dst, err := tl.AppendBytes(dst, sv.Signature)
	if err != nil {
		// Only reachable for a signature longer than 4 GiB.
		panic(fmt.Sprintf("simplex: tl serialize vote signature: %v", err))
	}
	return dst
}

// appendUnsignedVote appends the boxed consensus.simplex.UnsignedVote form of
// v — the same bytes voteToTL produces through the reflection encoder.
func appendUnsignedVote(dst []byte, v Vote) []byte {
	switch v.Kind {
	case VoteNotarize:
		dst = binary.LittleEndian.AppendUint32(dst, idNotarizeVote)
	case VoteFinalize:
		dst = binary.LittleEndian.AppendUint32(dst, idFinalizeVote)
	case VoteSkip:
		dst = binary.LittleEndian.AppendUint32(dst, idSkipVote)
		return binary.LittleEndian.AppendUint32(dst, v.ID.Slot)
	default:
		panic(fmt.Sprintf("simplex: invalid vote kind %d", v.Kind))
	}
	// notarize and finalize carry a boxed consensus.candidateId.
	dst = binary.LittleEndian.AppendUint32(dst, idCandidateID)
	dst = binary.LittleEndian.AppendUint32(dst, v.ID.Slot)
	return append(dst, v.ID.Hash[:]...)
}

// parseSignedVote decodes a boxed consensus.simplex.vote message.
func parseSignedVote(data []byte) (Vote, []byte, error) {
	if len(data) < 4 {
		return Vote{}, nil, fmt.Errorf("simplex/tl: truncated vote")
	}
	if id := binary.LittleEndian.Uint32(data); id != idSignedVote {
		return Vote{}, nil, fmt.Errorf("simplex/tl: unexpected vote ctor %08x", id)
	}
	v, rest, err := parseUnsignedVote(data[4:])
	if err != nil {
		return Vote{}, nil, err
	}
	sig, rest, err := tl.FromBytes(rest)
	if err != nil {
		return Vote{}, nil, fmt.Errorf("simplex/tl: vote signature: %w", err)
	}
	if len(rest) != 0 {
		return Vote{}, nil, fmt.Errorf("simplex/tl: %d trailing bytes in vote", len(rest))
	}
	return v, sig, nil
}

// parseUnsignedVote decodes a boxed consensus.simplex.UnsignedVote and returns
// the remaining buffer.
func parseUnsignedVote(data []byte) (Vote, []byte, error) {
	if len(data) < 4 {
		return Vote{}, nil, fmt.Errorf("simplex/tl: truncated unsigned vote")
	}
	var kind VoteKind
	switch id := binary.LittleEndian.Uint32(data); id {
	case idNotarizeVote:
		kind = VoteNotarize
	case idFinalizeVote:
		kind = VoteFinalize
	case idSkipVote:
		if len(data) < 8 {
			return Vote{}, nil, fmt.Errorf("simplex/tl: truncated skip vote")
		}
		return SkipVote(binary.LittleEndian.Uint32(data[4:])), data[8:], nil
	default:
		return Vote{}, nil, fmt.Errorf("simplex/tl: unexpected unsigned vote ctor %08x", id)
	}
	// ctor + boxed consensus.candidateId (ctor + slot + int256 hash).
	if len(data) < 4+4+4+32 {
		return Vote{}, nil, fmt.Errorf("simplex/tl: truncated candidate id in vote")
	}
	if id := binary.LittleEndian.Uint32(data[4:]); id != idCandidateID {
		return Vote{}, nil, fmt.Errorf("simplex/tl: unexpected candidate id ctor %08x", id)
	}
	v := Vote{Kind: kind, ID: CandidateID{Slot: binary.LittleEndian.Uint32(data[8:])}}
	copy(v.ID.Hash[:], data[12:44])
	return v, data[44:], nil
}

// ---- certificates ----

func toTLSignatureSet(sigs []VoteSignature) ConsensusSimplexVoteSignatureSet {
	set := ConsensusSimplexVoteSignatureSet{Votes: make([]ConsensusSimplexVoteSignature, len(sigs))}
	for i, s := range sigs {
		set.Votes[i] = ConsensusSimplexVoteSignature{Who: int32(s.ValidatorIndex), Signature: s.Signature}
	}
	return set
}

// SignatureSetBytes returns the boxed consensus.simplex.voteSignatureSet of
// the certificate — the representation stored in candidate-resolver records
// and requestCandidate responses.
func (c *Certificate) SignatureSetBytes() []byte {
	return mustSerialize(toTLSignatureSet(c.Signatures))
}

// Serialize returns the boxed consensus.simplex.certificate wire message.
//
// The bytes are always the canonical re-serialization of the parsed
// certificate, never the received frame: re-encoding from the decoded object is
// what every node does, and TL bytes fields carry unchecked alignment padding,
// so echoing an inbound frame would let a peer choose the content hash we
// journal and re-gossip.
//
// The result is cached behind a sync.Once: certificates are immutable once
// constructed, the same bytes are needed for journaling, gossip and
// misbehavior proofs, and event sinks receive the certificate and may serialize it
// from their own goroutine.
func (c *Certificate) Serialize() []byte {
	c.wireOnce.Do(func() {
		// ctor + vote + set ctor + count + per-signature (ctor+who+bytes(64)).
		size := 8 + voteWireMax + len(c.Signatures)*76
		c.wire = mustSerializeSized(size, ConsensusSimplexCertificate{
			Vote:       voteToTL(c.Vote),
			Signatures: toTLSignatureSet(c.Signatures),
		})
	})
	return c.wire
}

// parseCertificate decodes a boxed consensus.simplex.certificate message
// without verifying it.
func parseCertificate(data []byte, maxSigs int) (*Certificate, error) {
	var x ConsensusSimplexCertificate
	rest, err := tl.Parse(&x, data, true)
	if err != nil {
		return nil, err
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("simplex/tl: %d trailing bytes in certificate", len(rest))
	}
	v, err := voteFromTL(x.Vote)
	if err != nil {
		return nil, err
	}
	if len(x.Signatures.Votes) > maxSigs {
		return nil, fmt.Errorf("simplex/tl: %d signatures exceed limit %d", len(x.Signatures.Votes), maxSigs)
	}
	cert := &Certificate{Vote: v, Signatures: make([]VoteSignature, len(x.Signatures.Votes))}
	for i, s := range x.Signatures.Votes {
		cert.Signatures[i] = VoteSignature{ValidatorIndex: uint32(s.Who), Signature: s.Signature}
	}
	return cert, nil
}

// ---- journal records (byte-compatible with the canonical consensus DB) ----

// VoteRecordKey builds the boxed consensus.simplex.db.key.vote for a record
// body: sha256 of the serialized inner object (unsigned vote / certificate).
func VoteRecordKey(hash [32]byte) []byte {
	return mustSerialize(ConsensusSimplexDbKeyVote{VoteHash: hash[:]})
}

// OurVoteRecordHash is the dedup hash of an own-vote journal record.
func OurVoteRecordHash(v Vote) [32]byte {
	return sha256.Sum256(VoteBytes(v))
}

// CertificateRecordHash is the dedup hash of a certificate journal record.
func CertificateRecordHash(c *Certificate) [32]byte {
	return sha256.Sum256(c.Serialize())
}

// EncodeOurVoteRecord builds the boxed consensus.simplex.db.ourVote value.
func EncodeOurVoteRecord(v Vote, seqno int64) []byte {
	return mustSerialize(ConsensusSimplexDbOurVote{Vote: voteToTL(v), Seqno: seqno})
}

// EncodeCertificateRecord builds the boxed consensus.simplex.db.cert value.
// The layout is the record constructor id followed by the boxed certificate,
// so the cached certificate serialization is reused as-is.
func EncodeCertificateRecord(c *Certificate) []byte {
	wire := c.Serialize()
	dst := make([]byte, 0, 4+len(wire))
	dst = binary.LittleEndian.AppendUint32(dst, idDbCert)
	return append(dst, wire...)
}

// VoteJournalRecord is a decoded consensus.simplex.db.Vote value: either an
// own vote with its seqno or a saved certificate.
type VoteJournalRecord struct {
	Vote  *Vote
	Seqno int64
	Cert  *Certificate
}

// DecodeVoteJournalRecord parses a consensus.simplex.db.Vote value.
func DecodeVoteJournalRecord(data []byte, maxSigs int) (VoteJournalRecord, error) {
	if len(data) < 4 {
		return VoteJournalRecord{}, fmt.Errorf("simplex/tl: truncated db.Vote record")
	}
	switch binary.LittleEndian.Uint32(data) {
	case idDbOurVote:
		var x ConsensusSimplexDbOurVote
		rest, err := tl.Parse(&x, data, true)
		if err != nil {
			return VoteJournalRecord{}, err
		}
		if len(rest) != 0 {
			return VoteJournalRecord{}, fmt.Errorf("simplex/tl: %d trailing bytes in db.ourVote", len(rest))
		}
		v, err := voteFromTL(x.Vote)
		if err != nil {
			return VoteJournalRecord{}, err
		}
		return VoteJournalRecord{Vote: &v, Seqno: x.Seqno}, nil
	case idDbCert:
		cert, err := parseCertificate(data[4:], maxSigs)
		if err != nil {
			return VoteJournalRecord{}, err
		}
		return VoteJournalRecord{Cert: cert}, nil
	}
	return VoteJournalRecord{}, fmt.Errorf("simplex/tl: unexpected db.Vote ctor %08x", binary.LittleEndian.Uint32(data))
}

// PoolStateKey returns the boxed consensus.simplex.db.key.poolState key —
// the canonical key form; the local store namespaces keys itself and builds
// this one on its own, see validator/pebblestore/keys.go.
func PoolStateKey() []byte {
	return mustSerialize(ConsensusSimplexDbKeyPoolState{})
}

// EncodePoolStateRecord builds the boxed consensus.simplex.db.poolState value.
func EncodePoolStateRecord(firstNonAnnouncedWindow uint32) []byte {
	return mustSerialize(ConsensusSimplexDbPoolState{FirstNonAnnouncedWindow: int32(firstNonAnnouncedWindow)})
}

// DecodePoolStateRecord parses a consensus.simplex.db.poolState value.
func DecodePoolStateRecord(data []byte) (uint32, error) {
	var x ConsensusSimplexDbPoolState
	rest, err := tl.Parse(&x, data, true)
	if err != nil {
		return 0, err
	}
	if len(rest) != 0 {
		return 0, fmt.Errorf("simplex/tl: %d trailing bytes in db.poolState", len(rest))
	}
	return uint32(x.FirstNonAnnouncedWindow), nil
}
