package simplex

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
)

// parallelVerifyMinSigs is the certificate size from which signature
// verification fans out across cores.
const parallelVerifyMinSigs = 8

// RosterDigest commits to the exact validator slice a certificate was verified
// against: the length, the order (certificate signatures address validators by
// index), every public key (what was verified) and every weight (what the
// 2/3+1 quorum threshold was computed from). The count is length-prefixed and
// every field is fixed-size, so the digest is injective over rosters.
func RosterDigest(validators []Validator) ([32]byte, error) {
	var out [32]byte
	if len(validators) == 0 {
		return out, fmt.Errorf("simplex: empty validator set")
	}

	h := sha256.New()
	var scratch [8]byte
	binary.LittleEndian.PutUint64(scratch[:], uint64(len(validators)))
	h.Write(scratch[:])
	for i := range validators {
		if len(validators[i].PublicKey) != ed25519.PublicKeySize {
			return out, fmt.Errorf("simplex: validator %d has invalid public key", i)
		}
		h.Write(validators[i].PublicKey)
		binary.LittleEndian.PutUint64(scratch[:], validators[i].Weight)
		h.Write(scratch[:])
	}
	copy(out[:], h.Sum(nil))

	return out, nil
}

// CertificateBinding is the (session id, validator roster) pair a certificate
// was verified against. A certificate proves a quorum only relative to this
// pair: the signed payload commits to the session id (see DataToSign), and the
// signature indices, the public keys and the 2/3+1 weight threshold all come
// from the roster slice. Two bindings are equal iff both halves are.
//
// The session id already commits to the roster through groups.SessionID, so the
// roster digest is nominally redundant — but only while that derivation, which
// lives in another package, is correct. A seal must not rest on a property
// proved elsewhere, so both halves are carried and both are checked.
type CertificateBinding struct {
	sessionID [32]byte
	roster    [32]byte
}

// NewCertificateBinding derives the binding of one consensus session.
func NewCertificateBinding(sessionID [32]byte, validators []Validator) (CertificateBinding, error) {
	roster, err := RosterDigest(validators)
	if err != nil {
		return CertificateBinding{}, err
	}

	return CertificateBinding{sessionID: sessionID, roster: roster}, nil
}

// SessionID is the session id half of the binding.
func (b CertificateBinding) SessionID() [32]byte { return b.sessionID }

// IsZero reports the unusable zero binding, which no roster can produce: a
// roster digest is a sha256 over at least one entry.
func (b CertificateBinding) IsZero() bool { return b == CertificateBinding{} }

// Check accepts a verified certificate only if it was verified under exactly
// this binding. It rejects the zero binding, the zero (never verified)
// certificate, and any certificate sealed for another session or another
// validator roster.
//
// This is the whole guard a consumer runs in place of re-verifying the quorum,
// so it is deliberately total: two 32-byte comparisons, no fallthrough.
func (b CertificateBinding) Check(c VerifiedCertificate) error {
	if b.IsZero() {
		return fmt.Errorf("simplex: certificate binding is not initialized")
	}
	if c.IsZero() {
		return fmt.Errorf("simplex: certificate was not verified")
	}
	if c.binding.sessionID != b.sessionID {
		return fmt.Errorf("simplex: certificate was verified for another consensus session")
	}
	if c.binding.roster != b.roster {
		return fmt.Errorf("simplex: certificate was verified against another validator set")
	}

	return nil
}

// VerifiedCertificate is a Certificate whose entire quorum of Ed25519
// signatures has been verified, carried together with the binding it was
// verified under.
//
// Both fields are unexported and the type has no populated literal form outside
// this package: every value in existence came out of sealCertificate, which has
// exactly three callers, each of which has just verified or attested the quorum:
//
//  1. CertificateVerifier.Verify (and the free VerifyCertificate) — ran
//     verifyCertificatePayload over the certificate; ValidateBootstrap uses
//     the same verifier for every journaled certificate;
//  2. Engine.verifyCertificate — ran verifyCertificatePayload with the memoized
//     payload and the precomputed threshold;
//  3. Engine.seal — caller attestation for a certificate the engine assembled
//     itself out of votes it had already verified one by one; see the comment
//     there.
//
// A consumer holding one, after checking its binding, has cryptographic proof
// of the quorum and does not repeat it. That is what lets the Simplex block
// accepter call blockproof.CheckPreparedSignatureWeight instead of
// blockproof.CheckPreparedSignatures.
//
// The certificate is copied into the seal before the proof is exposed. Public
// accessors return another copy, so neither the verification input nor a
// consumer can mutate the statement the seal proves.
type VerifiedCertificate struct {
	cert    *Certificate
	binding CertificateBinding
}

// sealCertificate is the single funnel every VerifiedCertificate comes out of.
// It is unexported and takes no error path on purpose: its callers are the
// verification sites, and the type is meaningless without one of them. The
// deep copy is the ownership boundary: callers may keep and reuse cert after
// this function returns without changing the sealed proof.
func sealCertificate(cert *Certificate, binding CertificateBinding) VerifiedCertificate {
	return VerifiedCertificate{cert: cloneCertificate(cert), binding: binding}
}

// Certificate returns a copy of the verified certificate, or nil for the zero
// value. Mutating the result cannot change this proof.
func (c VerifiedCertificate) Certificate() *Certificate { return cloneCertificate(c.cert) }

// certificate returns the package-owned immutable certificate. It is for
// simplex internals only; a pointer crossing an injected interface or package
// boundary must go through Certificate instead.
func (c VerifiedCertificate) certificate() *Certificate { return c.cert }

// Vote is the certified vote, or the zero Vote (Kind 0, which no VoteKind
// constant uses) for the zero value.
func (c VerifiedCertificate) Vote() Vote {
	if c.cert == nil {
		return Vote{}
	}

	return c.cert.Vote
}

// Binding is the (session id, roster) pair this certificate was verified under.
func (c VerifiedCertificate) Binding() CertificateBinding { return c.binding }

// IsZero reports the never-verified zero value.
func (c VerifiedCertificate) IsZero() bool { return c.cert == nil }

func cloneCertificate(cert *Certificate) *Certificate {
	if cert == nil {
		return nil
	}

	cloned := &Certificate{Vote: cert.Vote}
	if cert.Signatures != nil {
		cloned.Signatures = make([]VoteSignature, len(cert.Signatures))
	}
	for i := range cert.Signatures {
		cloned.Signatures[i] = VoteSignature{
			ValidatorIndex: cert.Signatures[i].ValidatorIndex,
			Signature:      bytes.Clone(cert.Signatures[i].Signature),
		}
	}

	return cloned
}

func certificatesEqual(left, right *Certificate) bool {
	if left == nil || right == nil {
		return left == right
	}
	if left.Vote != right.Vote || len(left.Signatures) != len(right.Signatures) {
		return false
	}
	for i := range left.Signatures {
		if left.Signatures[i].ValidatorIndex != right.Signatures[i].ValidatorIndex ||
			!bytes.Equal(left.Signatures[i].Signature, right.Signatures[i].Signature) {
			return false
		}
	}

	return true
}

// CertificateVerifier verifies certificates of one session without an Engine.
// It exists so that the roster digest and the quorum threshold are derived once
// per session rather than once per certificate; the candidate resolver holds
// one for the whole session.
type CertificateVerifier struct {
	binding    CertificateBinding
	validators []Validator
	threshold  uint64
}

// NewCertificateVerifier prepares the per-session verification state.
func NewCertificateVerifier(sessionID [32]byte, validators []Validator) (*CertificateVerifier, error) {
	ownedValidators := cloneValidators(validators)
	if len(ownedValidators) == 0 {
		return nil, fmt.Errorf("simplex: empty validator set")
	}
	totalWeight, err := totalValidatorWeight(ownedValidators)
	if err != nil {
		return nil, err
	}
	binding, err := NewCertificateBinding(sessionID, ownedValidators)
	if err != nil {
		return nil, err
	}

	return &CertificateVerifier{
		binding:    binding,
		validators: ownedValidators,
		threshold:  totalWeight*2/3 + 1,
	}, nil
}

func cloneValidators(validators []Validator) []Validator {
	cloned := make([]Validator, len(validators))
	for i := range validators {
		cloned[i] = Validator{
			PublicKey: bytes.Clone(validators[i].PublicKey),
			Weight:    validators[i].Weight,
		}
	}

	return cloned
}

// Binding is the binding every certificate this verifier seals carries.
func (v *CertificateVerifier) Binding() CertificateBinding { return v.binding }

// Verify checks the vote shape, index bounds, per-validator uniqueness, the
// 2/3+1 weighted quorum and every signature, and seals the result.
func (v *CertificateVerifier) Verify(cert *Certificate) (VerifiedCertificate, error) {
	if cert == nil {
		return VerifiedCertificate{}, fmt.Errorf("simplex: nil certificate")
	}
	// Reject shapes that cannot possibly verify before copying their payload.
	// The same checks run on the owned snapshot below; this preflight only keeps
	// an oversized in-process input from turning a cheap rejection into a large
	// allocation.
	if err := validateCertificateVote(cert.Vote); err != nil {
		return VerifiedCertificate{}, err
	}
	if len(cert.Signatures) > len(v.validators) {
		return VerifiedCertificate{}, fmt.Errorf("too many signatures in certificate")
	}

	sealed := sealCertificate(cert, v.binding)
	owned := sealed.certificate()
	if err := validateCertificateVote(owned.Vote); err != nil {
		return VerifiedCertificate{}, err
	}

	payload := DataToSign(v.binding.sessionID, VoteBytes(owned.Vote))
	if err := verifyCertificatePayload(v.validators, v.threshold, payload, owned); err != nil {
		return VerifiedCertificate{}, err
	}

	return sealed, nil
}

// VerifyCertificate checks a certificate without requiring an Engine. It is
// used by the candidate resolver before data received through the request
// protocol is allowed into session state.
//
// Callers verifying more than one certificate of the same session should hold a
// CertificateVerifier instead: this rebuilds the roster digest and the quorum
// threshold on every call.
func VerifyCertificate(
	sessionID [32]byte,
	validators []Validator,
	cert *Certificate,
) (VerifiedCertificate, error) {
	verifier, err := NewCertificateVerifier(sessionID, validators)
	if err != nil {
		return VerifiedCertificate{}, err
	}

	return verifier.Verify(cert)
}

func validateCertificateVote(vote Vote) error {
	switch vote.Kind {
	case VoteNotarize, VoteFinalize:
		return nil
	case VoteSkip:
		if vote.ID.Hash != ([32]byte{}) {
			return fmt.Errorf("simplex: skip vote has a candidate hash")
		}

		return nil
	default:
		return fmt.Errorf("simplex: invalid vote kind %d", vote.Kind)
	}
}

func verifyCertificatePayload(
	validators []Validator,
	threshold uint64,
	payload []byte,
	cert *Certificate,
) error {
	// Both entry points (VerifyCertificate and Engine.verifyCertificate)
	// reject a nil certificate before reaching here.
	if len(cert.Signatures) > len(validators) {
		return fmt.Errorf("too many signatures in certificate")
	}

	voted := make([]bool, len(validators))
	var weight uint64
	for _, signature := range cert.Signatures {
		if int(signature.ValidatorIndex) >= len(validators) {
			return fmt.Errorf("invalid validator index %d in certificate", signature.ValidatorIndex)
		}
		if voted[signature.ValidatorIndex] {
			return fmt.Errorf("duplicate validator index %d in certificate", signature.ValidatorIndex)
		}
		voted[signature.ValidatorIndex] = true
		weight += validators[signature.ValidatorIndex].Weight
	}
	if weight < threshold {
		return fmt.Errorf("not enough signatures in certificate")
	}

	verify := func(signature VoteSignature) bool {
		return len(signature.Signature) == ed25519.SignatureSize && ed25519.Verify(
			validators[signature.ValidatorIndex].PublicKey,
			payload,
			signature.Signature,
		)
	}

	if len(cert.Signatures) < parallelVerifyMinSigs || runtime.GOMAXPROCS(0) < 2 {
		for _, signature := range cert.Signatures {
			if !verify(signature) {
				return fmt.Errorf("invalid vote signature for validator %d", signature.ValidatorIndex)
			}
		}

		return nil
	}

	// A quorum is all-or-nothing, so the first failure lowers the bound and the
	// workers abandon the rest of the set.
	bad := verifyFanOut(len(cert.Signatures), func(i int) int {
		if verify(cert.Signatures[i]) {
			return len(cert.Signatures)
		}

		return i
	})
	if bad < len(cert.Signatures) {
		return fmt.Errorf("invalid vote signature for validator %d", cert.Signatures[bad].ValidatorIndex)
	}

	return nil
}

// verifyFanOut spreads the indices [0,n) over up to GOMAXPROCS goroutines and
// calls body for each, returning the final bound.
//
// body must be safe for concurrent use and returns the exclusive upper bound
// of indices still worth taking: a certificate lowers it to the first failing
// signature because one bad signature voids the whole quorum, while a batch of
// independent votes judges every entry on its own and always returns n. The
// bound is only ever lowered, so it converges on the lowest index body
// rejected among those actually dispensed.
func verifyFanOut(n int, body func(i int) int) int {
	var wg sync.WaitGroup
	var next atomic.Int64
	var bound atomic.Int64
	bound.Store(int64(n))

	for range min(runtime.GOMAXPROCS(0), n) {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				i := next.Add(1) - 1
				if i >= int64(n) || i >= bound.Load() {
					return
				}
				b := int64(body(int(i)))
				for {
					current := bound.Load()
					if b >= current || bound.CompareAndSwap(current, b) {
						break
					}
				}
			}
		}()
	}
	wg.Wait()

	return int(bound.Load())
}
