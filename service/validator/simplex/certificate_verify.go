package simplex

import (
	"crypto/ed25519"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
)

// parallelVerifyMinSigs is the certificate size from which signature
// verification fans out across cores.
const parallelVerifyMinSigs = 8

// VerifyCertificate checks a certificate without requiring an Engine. It is
// used by the candidate resolver before data received through the request
// protocol is allowed into session state.
func VerifyCertificate(sessionID [32]byte, validators []Validator, cert *Certificate) error {
	if len(validators) == 0 {
		return fmt.Errorf("simplex: empty validator set")
	}
	if cert == nil {
		return fmt.Errorf("simplex: nil certificate")
	}
	if err := validateCertificateVote(cert.Vote); err != nil {
		return err
	}

	totalWeight, err := totalValidatorWeight(validators)
	if err != nil {
		return err
	}

	threshold := totalWeight*2/3 + 1
	payload := DataToSign(sessionID, VoteBytes(cert.Vote))

	return verifyCertificatePayload(validators, threshold, payload, cert)
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
