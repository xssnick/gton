package simplex

import (
	"crypto/ed25519"
	"crypto/sha256"
	"fmt"

	"github.com/xssnick/tonutils-go/ton"
)

// Candidate is the consensus-level view of a leader proposal. The engine only
// needs its identity, chain position and leadership facts; block payloads
// (data / collated data) travel through the candidate transport and stay out
// of the headless engine.
type Candidate struct {
	ID     CandidateID
	Parent ParentID
	// Leader is the validator index of the proposer.
	Leader uint32
	// Empty marks an "empty" candidate that merely references an existing
	// block instead of proposing a new one. Empty candidates always have a
	// defined Parent.
	Empty bool
	// Block is the proposed block id (ordinary) or the referenced block id
	// (empty). RootHash and FileHash must be 32 bytes.
	Block ton.BlockIDExt
	// CollatedFileHash is the hash of collated data; ordinary candidates only.
	CollatedFileHash [32]byte
	// Signature is the signature over
	// dataToSign(sessionID, boxed consensus.candidateId): the leader's key
	// for ordinary candidates, the collator's key when Delegation is set.
	Signature []byte
	// Delegation marks a candidate produced by a delegated collator on the
	// leader's behalf.
	Delegation *Delegation
}

// Delegation authorizes a collator to produce candidates of one leader
// window: the leader signs consensus.delegationToSign(window_start,
// collator_id) and the collator signs the candidates with its own key.
type Delegation struct {
	// CollatorKey is the collator's ed25519 public key (pub.ed25519).
	CollatorKey ed25519.PublicKey
	// Signature is the LEADER's signature over
	// dataToSign(sessionID, boxed consensus.delegationToSign).
	Signature []byte
}

// HashDataBytes serializes the boxed consensus.CandidateHashData of the
// candidate — the preimage of the candidate hash and the exact `candidate`
// payload of liteServer.signatureSet.simplex used in TON block proofs.
func (c *Candidate) HashDataBytes() []byte {
	if c.Empty {
		// candidateHashDataEmpty.parent is a bare consensus.candidateId and
		// is mandatory.
		return mustSerialize(ton.ConsensusCandidateHashDataEmpty{
			Block:  c.Block,
			Parent: tlCandidateID(c.Parent.ID),
		})
	}
	return mustSerialize(ton.ConsensusCandidateHashDataOrdinary{
		Block:            c.Block,
		CollatedFileHash: c.CollatedFileHash[:],
		Parent:           parentToTL(c.Parent),
	})
}

// ComputeID derives the candidate id for a slot: sha256 of the boxed
// CandidateHashData.
func (c *Candidate) ComputeID(slot uint32) CandidateID {
	return CandidateID{Slot: slot, Hash: sha256.Sum256(c.HashDataBytes())}
}

// SignCandidate produces the leader signature for a candidate id.
func SignCandidate(signer Signer, sessionID [32]byte, id CandidateID) ([]byte, error) {
	return signer.Sign(DataToSign(sessionID, mustSerialize(tlCandidateID(id))))
}

// DelegationToSignBytes returns the boxed consensus.delegationToSign payload
// authorized by a validator leader in Delegated v3.
func DelegationToSignBytes(windowStart uint32, collatorID [32]byte) []byte {
	return mustSerialize(ConsensusDelegationToSign{
		WindowStartSlot: int32(windowStart),
		CollatorID:      collatorID[:],
	})
}

// SignDelegation authorizes collatorID to produce candidates for the leader
// window starting at windowStart.
func SignDelegation(
	signer Signer,
	sessionID [32]byte,
	windowStart uint32,
	collatorID [32]byte,
) ([]byte, error) {
	return signer.Sign(DataToSign(sessionID, DelegationToSignBytes(windowStart, collatorID)))
}

// VerifyDelegationSignature verifies one Delegated-v3 leader authorization
// without exposing TL serialization to the collator runtime.
func VerifyDelegationSignature(
	leaderKey ed25519.PublicKey,
	sessionID [32]byte,
	windowStart uint32,
	collatorID [32]byte,
	signature []byte,
) bool {
	return len(signature) == ed25519.SignatureSize && ed25519.Verify(
		leaderKey,
		DataToSign(sessionID, DelegationToSignBytes(windowStart, collatorID)),
		signature,
	)
}

// VerifyCandidateSignature checks a leader or delegated collator signature
// over the boxed candidate ID in the session domain.
func VerifyCandidateSignature(key ed25519.PublicKey, sessionID [32]byte, id CandidateID, sig []byte) bool {
	return len(sig) == ed25519.SignatureSize &&
		ed25519.Verify(key, DataToSign(sessionID, mustSerialize(tlCandidateID(id))), sig)
}

// verifyDelegatedCandidate checks the delegation trio: the leader signed the
// collation window for exactly this collator, and the candidate itself is
// signed by the collator key.
func verifyDelegatedCandidate(
	c *Candidate,
	leaderKey ed25519.PublicKey,
	sessionID [32]byte,
	slotsPerWindow uint32,
	protocolVersion uint8,
) error {
	if protocolVersion < 3 {
		return fmt.Errorf("simplex: delegated candidate requires protocol version 3")
	}
	d := c.Delegation
	if len(d.CollatorKey) != ed25519.PublicKeySize {
		return fmt.Errorf("simplex: delegated candidate collator key is invalid")
	}
	collatorID := KeyNodeIDShort(d.CollatorKey)
	windowStart := c.ID.Slot - c.ID.Slot%slotsPerWindow
	// Config 46 controls who may join the private session overlay. Candidate
	// authority itself is the leader's delegation signature, so the consensus
	// verifier deliberately has no per-leader registry check.
	if !VerifyDelegationSignature(leaderKey, sessionID, windowStart, collatorID, d.Signature) {
		return fmt.Errorf("simplex: delegation signature is not valid")
	}
	if !VerifyCandidateSignature(d.CollatorKey, sessionID, c.ID, c.Signature) {
		return fmt.Errorf("simplex: delegated candidate signature is not valid")
	}
	return nil
}

// ValidateShape performs structural checks before any candidate TL
// serialization. In particular it keeps malformed external data from reaching
// serializers whose fixed-size hash fields assume a valid wire shape.
func (c *Candidate) ValidateShape() error {
	if len(c.Block.RootHash) != 32 || len(c.Block.FileHash) != 32 {
		return fmt.Errorf("simplex: candidate block hashes must be 32 bytes")
	}
	if c.Empty && !c.Parent.Exists {
		return fmt.Errorf("simplex: empty candidate must have a parent")
	}
	if c.Parent.Exists && c.Parent.ID.Slot >= c.ID.Slot {
		return fmt.Errorf("simplex: candidate parent slot %d is not before slot %d", c.Parent.ID.Slot, c.ID.Slot)
	}
	return nil
}
