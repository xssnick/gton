package blockproof

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"fmt"

	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

// SimplexBroadcastSignatureSet builds the tonNode signature set carried by
// public block and finality broadcasts. The returned value owns all of its
// byte slices and contains a decoded, exact boxed candidate value.
func (s *ValidatorSignatureSet) SimplexBroadcastSignatureSet() (tonnodeapi.SignatureSetSimplex, error) {
	if !s.simplex {
		return tonnodeapi.SignatureSetSimplex{}, fmt.Errorf("validator signature set is not simplex")
	}
	if len(s.sessionID) != sha256.Size {
		return tonnodeapi.SignatureSetSimplex{}, fmt.Errorf("invalid simplex session id len %d", len(s.sessionID))
	}
	if len(s.signatures) > maxBlockSignatures {
		return tonnodeapi.SignatureSetSimplex{}, fmt.Errorf("too many validator signatures: %d", len(s.signatures))
	}

	signatures := make([]tonnodeapi.BlockSignature, len(s.signatures))
	for i, signature := range s.signatures {
		if len(signature.NodeIDShort) != sha256.Size {
			return tonnodeapi.SignatureSetSimplex{}, fmt.Errorf(
				"invalid validator node id len %d at index %d",
				len(signature.NodeIDShort),
				i,
			)
		}
		if len(signature.Signature) != ed25519.SignatureSize {
			return tonnodeapi.SignatureSetSimplex{}, fmt.Errorf(
				"invalid validator signature len %d at index %d",
				len(signature.Signature),
				i,
			)
		}

		signatures[i] = tonnodeapi.BlockSignature{
			Who:       bytes.Clone(signature.NodeIDShort),
			Signature: bytes.Clone(signature.Signature),
		}
	}

	candidate, err := decodeSimplexBroadcastCandidate(s.candidateData)
	if err != nil {
		return tonnodeapi.SignatureSetSimplex{}, fmt.Errorf("decode simplex candidate: %w", err)
	}

	return tonnodeapi.SignatureSetSimplex{
		Final:            s.final,
		CatchainSeqno:    int32(s.catchainSeqno),
		ValidatorSetHash: int32(s.validatorSetHash),
		Signatures:       signatures,
		SessionID:        bytes.Clone(s.sessionID),
		Slot:             s.slot,
		Candidate:        candidate,
	}, nil
}

func decodeSimplexBroadcastCandidate(data []byte) (any, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("candidate data is empty")
	}

	var candidate any
	left, err := tl.Parse(&candidate, data, true)
	if err != nil {
		return nil, err
	}
	if len(left) != 0 {
		return nil, fmt.Errorf("candidate data has %d trailing bytes", len(left))
	}
	if err = validateSimplexBroadcastCandidate(candidate); err != nil {
		return nil, err
	}

	return candidate, nil
}

func validateSimplexBroadcastCandidate(candidate any) error {
	switch candidate := candidate.(type) {
	case ton.ConsensusCandidateHashDataOrdinary:
		if err := validateSimplexCandidateBlock(candidate.Block); err != nil {
			return err
		}
		if len(candidate.CollatedFileHash) != sha256.Size {
			return fmt.Errorf("invalid collated file hash len %d", len(candidate.CollatedFileHash))
		}

		switch parent := candidate.Parent.(type) {
		case ton.ConsensusCandidateWithoutParents:
			return nil
		case ton.ConsensusCandidateParent:
			id, ok := parent.ID.(ton.ConsensusCandidateID)
			if !ok {
				return fmt.Errorf("unexpected simplex candidate parent id type %T", parent.ID)
			}
			return validateSimplexCandidateID(id)
		default:
			return fmt.Errorf("unexpected simplex candidate parent type %T", candidate.Parent)
		}

	case ton.ConsensusCandidateHashDataEmpty:
		if err := validateSimplexCandidateBlock(candidate.Block); err != nil {
			return err
		}
		return validateSimplexCandidateID(candidate.Parent)

	default:
		return fmt.Errorf("unsupported simplex candidate type %T", candidate)
	}
}

func validateSimplexCandidateBlock(block ton.BlockIDExt) error {
	if len(block.RootHash) != sha256.Size {
		return fmt.Errorf("invalid candidate block root hash len %d", len(block.RootHash))
	}
	if len(block.FileHash) != sha256.Size {
		return fmt.Errorf("invalid candidate block file hash len %d", len(block.FileHash))
	}

	return nil
}

func validateSimplexCandidateID(id ton.ConsensusCandidateID) error {
	if len(id.Hash) != sha256.Size {
		return fmt.Errorf("invalid simplex candidate id hash len %d", len(id.Hash))
	}

	return nil
}
