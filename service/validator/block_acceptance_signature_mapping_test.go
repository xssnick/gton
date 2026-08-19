package validator

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"math"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
)

// acceptanceMappingFixture is the accepter fixture with more than one validator.
//
// Every other accepter fixture has exactly one, and with one validator the map
// from a certificate's ValidatorIndex to a ton.Signature.NodeIDShort is the
// identity whatever it does: index 0 to entry 0. Nothing in those fixtures can
// tell a correct mapping from any other.
type acceptanceMappingFixture struct {
	config      SessionConfig
	privateKeys []ed25519.PrivateKey
	candidate   *CandidateArtifact
	validators  *blockproof.PreparedValidatorSet
}

func newAcceptanceMappingFixture(t *testing.T, count int) acceptanceMappingFixture {
	t.Helper()

	shard := groups.ShardID{Workchain: 0, Shard: math.MinInt64}
	const catchainSeqno = uint32(7)

	keys := make([]ed25519.PrivateKey, count)
	validators := make([]groups.Validator, count)
	addrs := make([]*tlb.ValidatorAddr, count)
	for i := range count {
		keys[i] = ed25519.NewKeyFromSeed(bytes.Repeat([]byte{byte(0x21 + i)}, ed25519.SeedSize))
		publicKey := keys[i].Public().(ed25519.PublicKey)
		var publicKeyArray [32]byte
		copy(publicKeyArray[:], publicKey)
		publicKeyHash, err := groups.PublicKeyHash(publicKeyArray)
		if err != nil {
			t.Fatalf("hash validator %d public key: %v", i, err)
		}
		validators[i] = groups.Validator{
			PublicKey:     publicKeyArray,
			PublicKeyHash: publicKeyHash,
			ADNL:          [32]byte{byte(0x44 + i)},
			Weight:        1,
		}
		addrs[i] = &tlb.ValidatorAddr{
			PublicKey: tlb.SigPubKeyED25519{Key: publicKey},
			Weight:    validators[i].Weight,
			ADNLAddr:  validators[i].ADNL[:],
		}
	}

	validatorSetHash, err := groups.ValidatorSetHash(groups.ValidatorSetHashInput{
		CatchainSeqno: catchainSeqno,
		Validators:    validators,
	})
	if err != nil {
		t.Fatalf("calculate validator set hash: %v", err)
	}
	prepared, err := blockproof.PrepareValidatorSet(catchainSeqno, addrs)
	if err != nil {
		t.Fatalf("prepare validators: %v", err)
	}

	root := acceptanceTestBlockRoot(t, shard, catchainSeqno, validatorSetHash, nil)
	blockBOC := root.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: true})
	rootHash := root.HashKey()
	fileHash := sha256.Sum256(blockBOC)
	candidate := simplex.Candidate{
		Parent: simplex.Genesis(),
		Leader: 0,
		Block: ton.BlockIDExt{
			Workchain: shard.Workchain,
			Shard:     shard.Shard,
			SeqNo:     2,
			RootHash:  rootHash[:],
			FileHash:  fileHash[:],
		},
		CollatedFileHash: sha256.Sum256([]byte("collated data")),
	}
	candidate.ID = candidate.ComputeID(3)

	return acceptanceMappingFixture{
		config: SessionConfig{
			SessionID:        [32]byte{0x55},
			Shard:            shard,
			CatchainSeqno:    catchainSeqno,
			ValidatorSetHash: validatorSetHash,
			Validators:       validators,
		},
		privateKeys: keys,
		candidate:   &CandidateArtifact{Candidate: candidate, BlockBOC: blockBOC},
		validators:  prepared,
	}
}

// acceptance seals a notarization signed by every validator, each under its own
// index, through the real certificate verification the accepter's inputs go
// through in production.
func (f acceptanceMappingFixture) acceptance(t *testing.T) BlockAcceptance {
	t.Helper()

	vote := simplex.NotarizeVote(f.candidate.Candidate.ID)
	payload := simplex.DataToSign(f.config.SessionID, simplex.VoteBytes(vote))
	signatures := make([]simplex.VoteSignature, len(f.privateKeys))
	for i, key := range f.privateKeys {
		signatures[i] = simplex.VoteSignature{
			ValidatorIndex: uint32(i),
			Signature:      ed25519.Sign(key, payload),
		}
	}
	verified, err := simplex.VerifyCertificate(
		f.config.SessionID,
		runtimeValidators(f.config.Validators),
		&simplex.Certificate{Vote: vote, Signatures: signatures},
	)
	if err != nil {
		t.Fatalf("verify certificate: %v", err)
	}

	return BlockAcceptance{
		Candidate:          f.candidate,
		Certificate:        verified,
		CertifiedCandidate: f.candidate,
	}
}

// BlockAccepter.prepare deliberately drops the Ed25519 pass and keeps only
// CheckPreparedSignatureWeight. That is sound for the signatures themselves —
// the consensus engine verified this quorum over a byte-identical payload — but
// it is the one thing that stops checking the step in between: signatureSet
// re-attributes each certificate signature to a NodeIDShort taken from
// validatorIDs at the signature's ValidatorIndex, and the set it publishes is
// what other nodes verify.
//
// A wrong mapping survives the weight check completely: the same identities with
// the same weights are present, exactly once each, so the roster, the duplicate
// rule and the threshold all still pass. Only the Ed25519 pass, which this node
// no longer runs, can see it — on a peer, after the block has been published.
//
// The test therefore runs the pass this node dropped, against the set this node
// publishes, and pins that the mapping it produces is the one the certificate
// meant.
func TestBlockAccepterSignatureSetBindsEachSignatureToItsValidator(t *testing.T) {
	fixture := newAcceptanceMappingFixture(t, 4)
	accepter, err := NewBlockAccepter(BlockAccepterOptions{
		Config: fixture.config,
		Node:   &acceptanceTestNode{},
	})
	if err != nil {
		t.Fatalf("build accepter: %v", err)
	}
	acceptance := fixture.acceptance(t)
	blockID := acceptance.Candidate.Candidate.Block

	signatures, err := accepter.signatureSet(acceptance)
	if err != nil {
		t.Fatalf("build published signature set: %v", err)
	}
	if got := signatures.SignatureCount(); got != len(fixture.privateKeys) {
		t.Fatalf("published %d signatures, want %d", got, len(fixture.privateKeys))
	}
	if err = blockproof.CheckPreparedSignatures(blockID, signatures, fixture.validators); err != nil {
		t.Fatalf("the published signature set does not verify: %v", err)
	}

	// The perturbation the single-validator fixtures cannot express: the same
	// four identities and the same four signatures, paired one step off. This is
	// what a validatorIDs list built in a different order than the certificate's
	// index basis produces, and it is exactly what the dropped pass used to
	// catch.
	rotated := make([][32]byte, len(accepter.validatorIDs))
	for i := range accepter.validatorIDs {
		rotated[i] = accepter.validatorIDs[(i+1)%len(accepter.validatorIDs)]
	}
	accepter.validatorIDs = rotated

	misattributed, err := accepter.signatureSet(acceptance)
	if err != nil {
		t.Fatalf("build misattributed signature set: %v", err)
	}
	if _, err = blockproof.CheckPreparedSignatureWeight(blockID, misattributed, fixture.validators); err != nil {
		t.Fatalf("the weight check rejected a rotation it cannot see: %v", err)
	}
	if err = blockproof.CheckPreparedSignatures(blockID, misattributed, fixture.validators); err == nil {
		t.Fatal("a rotated index-to-validator mapping produced a set that verifies, " +
			"so this test cannot hold the mapping")
	}

	// And the whole acceptance carries it: the mapping is not merely built
	// correctly, it reaches the published proof.
	accepter.validatorIDs = nil
	for i := range fixture.config.Validators {
		hash, hashErr := groups.PublicKeyHash(fixture.config.Validators[i].PublicKey)
		if hashErr != nil {
			t.Fatalf("hash validator %d public key: %v", i, hashErr)
		}
		accepter.validatorIDs = append(accepter.validatorIDs, hash)
	}
	prepared, err := accepter.prepare(acceptance)
	if err != nil {
		t.Fatalf("prepare acceptance: %v", err)
	}
	if err = blockproof.CheckPreparedSignatures(blockID, prepared.signatures, fixture.validators); err != nil {
		t.Fatalf("the prepared acceptance signature set does not verify: %v", err)
	}
}
