package blockproof

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"math/bits"
	"slices"
	"sort"

	sharddomain "github.com/xssnick/gton/service/shard"
	tnstore "github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type BlockProof struct {
	_          tlb.Magic     `tlb:"#c3"`
	ProofFor   blockIDExtTLB `tlb:"."`
	Root       *cell.Cell    `tlb:"^"`
	Signatures *cell.Cell    `tlb:"maybe ^"`
}

type Parsed struct {
	Proof *BlockProof
	Block *tlb.Block
	Meta  *tnstore.BlockMeta
}

const (
	masterchainShard   = int64(-1 << 63)
	maxBlockSignatures = 1024
)

func ValidateHardforkBlockHeader(block ton.BlockIDExt, keyBlock bool, vertSeqnoIncr bool) error {
	if block.Workchain != -1 || block.Shard != masterchainShard {
		return fmt.Errorf("hardfork block is not masterchain: %s", tnstore.FormatBlockRef(block))
	}
	if len(block.RootHash) != 32 || len(block.FileHash) != 32 {
		return fmt.Errorf("hardfork block has invalid hashes: %s", tnstore.FormatBlockRef(block))
	}
	if !keyBlock {
		return fmt.Errorf("hardfork block %s is not a key block", tnstore.FormatBlockRef(block))
	}
	if !vertSeqnoIncr {
		return fmt.Errorf("hardfork block %s has no vert_seqno_incr", tnstore.FormatBlockRef(block))
	}
	return nil
}

func ValidateHardforkBlock(block ton.BlockIDExt, parsed *tlb.Block) error {
	return ValidateHardforkBlockHeader(
		block,
		parsed != nil && parsed.BlockInfo.KeyBlock,
		parsed != nil && parsed.BlockInfo.VertSeqnoIncr,
	)
}

type blockIDExtTLB struct {
	ShardID  tlb.ShardIdent `tlb:"."`
	SeqNo    uint32         `tlb:"## 32"`
	RootHash []byte         `tlb:"bits 256"`
	FileHash []byte         `tlb:"bits 256"`
}

type blockSignaturesOrdinary struct {
	_                tlb.Magic        `tlb:"#11"`
	ValidatorSetHash uint32           `tlb:"## 32"`
	CatchainSeqno    uint32           `tlb:"## 32"`
	SigCount         uint32           `tlb:"## 32"`
	SigWeight        uint64           `tlb:"## 64"`
	Signatures       *cell.Dictionary `tlb:"dict 16"`
}

type blockSignaturesSimplex struct {
	_                tlb.Magic        `tlb:"#12"`
	ValidatorSetHash uint32           `tlb:"## 32"`
	CatchainSeqno    uint32           `tlb:"## 32"`
	SigCount         uint32           `tlb:"## 32"`
	SigWeight        uint64           `tlb:"## 64"`
	Signatures       *cell.Dictionary `tlb:"dict 16"`
	SessionID        []byte           `tlb:"bits 256"`
	Slot             uint32           `tlb:"## 32"`
	CandidateData    *cell.Cell       `tlb:"^"`
}

type ConsensusSimplexNotarizeVote struct {
	ID any `tl:"struct boxed [consensus.candidateId]"`
}

func init() {
	tl.Register(ConsensusSimplexNotarizeVote{}, "consensus.simplex.notarizeVote id:consensus.CandidateId = consensus.simplex.UnsignedVote")
}

type ValidatorSignatureSet struct {
	validatorSetHash uint32
	catchainSeqno    uint32
	signatures       []ton.Signature
	sessionID        []byte
	slot             int32
	candidateData    []byte
	final            bool
	simplex          bool
}

// BlockSignatureSet is the serialized BlockSignatures boundary. Unlike the
// logical validator signature set used by TL broadcasts, the cell declares
// the total signer weight and that declaration is part of validation.
type BlockSignatureSet struct {
	ValidatorSignatures *ValidatorSignatureSet
	DeclaredWeight      uint64
}

func ParseBOC(id ton.BlockIDExt, proofBOC []byte) (*Parsed, error) {
	if len(proofBOC) == 0 {
		return nil, fmt.Errorf("block proof %s is empty", tnstore.FormatBlockRef(id))
	}

	root, err := cell.FromBOC(proofBOC)
	if err != nil {
		return nil, fmt.Errorf("parse block proof %s: %w", tnstore.FormatBlockRef(id), err)
	}
	return ParseCell(id, root)
}

func LinkBOC(id ton.BlockIDExt, proofBOC []byte) ([]byte, error) {
	parsed, err := ParseBOC(id, proofBOC)
	if err != nil {
		return nil, err
	}

	_, boc, err := LinkFromRoot(id, parsed.Proof.Root)
	if err != nil {
		return nil, err
	}
	return boc, nil
}

func LinkFromRoot(id ton.BlockIDExt, proofRoot *cell.Cell) (*cell.Cell, []byte, error) {
	if proofRoot == nil {
		return nil, nil, fmt.Errorf("block proof link %s has no root", tnstore.FormatBlockRef(id))
	}
	proofFor, err := blockIDExtTLBFromBlock(id)
	if err != nil {
		return nil, nil, fmt.Errorf("encode block proof link id %s: %w", tnstore.FormatBlockRef(id), err)
	}

	root, err := tlb.ToCell(&BlockProof{
		ProofFor: proofFor,
		Root:     proofRoot,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("serialize block proof link %s: %w", tnstore.FormatBlockRef(id), err)
	}
	return root, root.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false}), nil
}

func ProofFromRoot(id ton.BlockIDExt, proofRoot *cell.Cell, signatures *cell.Cell) (*cell.Cell, []byte, error) {
	proofFor, err := blockIDExtTLBFromBlock(id)
	if err != nil {
		return nil, nil, fmt.Errorf("encode block proof id %s: %w", tnstore.FormatBlockRef(id), err)
	}
	root, err := tlb.ToCell(&BlockProof{
		ProofFor:   proofFor,
		Root:       proofRoot,
		Signatures: signatures,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("serialize block proof %s: %w", tnstore.FormatBlockRef(id), err)
	}
	return root, root.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false}), nil
}

func CheckProofShape(id ton.BlockIDExt, proofRoot *cell.Cell, isLink bool) error {
	return checkProofShape(id, proofRoot, isLink, false)
}

func CheckHardforkProofShape(id ton.BlockIDExt, proofRoot *cell.Cell, isLink bool) error {
	return checkProofShape(id, proofRoot, isLink, true)
}

func checkProofShape(id ton.BlockIDExt, proofRoot *cell.Cell, isLink bool, allowUnsignedMasterchain bool) error {
	loader, err := proofRoot.BeginParse()
	if err != nil {
		return fmt.Errorf("begin parse block proof %s: %w", tnstore.FormatBlockRef(id), err)
	}

	var proof BlockProof
	if err := tlb.LoadFromCell(&proof, loader); err != nil {
		return fmt.Errorf("parse block proof %s: %w", tnstore.FormatBlockRef(id), err)
	}

	proofFor := proof.ProofFor.blockID()
	if !proofFor.Equals(&id) {
		return fmt.Errorf("block proof is for %s, expected %s", tnstore.FormatBlockRef(proofFor), tnstore.FormatBlockRef(id))
	}
	if id.Workchain != -1 {
		if !isLink {
			return fmt.Errorf("non-masterchain block %s must be served with a proof link", tnstore.FormatBlockRef(id))
		}
		if proof.Signatures != nil {
			return fmt.Errorf("invalid ProofLink for non-masterchain block %s with validator signatures present", tnstore.FormatBlockRef(id))
		}
		return nil
	}
	if isLink {
		return nil
	}
	if proof.Signatures == nil && !allowUnsignedMasterchain {
		return fmt.Errorf("masterchain block proof %s has no validator signatures", tnstore.FormatBlockRef(id))
	}
	return nil
}

func ParseCell(id ton.BlockIDExt, proofRoot *cell.Cell) (*Parsed, error) {
	loader, err := proofRoot.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("begin parse block proof %s: %w", tnstore.FormatBlockRef(id), err)
	}

	var proof BlockProof
	if err := tlb.LoadFromCell(&proof, loader); err != nil {
		return nil, fmt.Errorf("parse block proof %s: %w", tnstore.FormatBlockRef(id), err)
	}

	proofFor := proof.ProofFor.blockID()
	if !proofFor.Equals(&id) {
		return nil, fmt.Errorf("block proof is for %s, expected %s", tnstore.FormatBlockRef(proofFor), tnstore.FormatBlockRef(id))
	}
	if proof.Signatures != nil && proofFor.Workchain != -1 {
		return nil, fmt.Errorf("invalid ProofLink for non-masterchain block %s with validator signatures present", tnstore.FormatBlockRef(id))
	}

	block, err := ton.CheckBlockProof(proof.Root, id.RootHash)
	if err != nil {
		return nil, fmt.Errorf("check block proof %s: %w", tnstore.FormatBlockRef(id), err)
	}
	if err = tnstore.VerifyBlockIdentity(id, block); err != nil {
		return nil, err
	}

	meta, err := tnstore.BuildBlockMetaFromParsedBlock(id, block)
	if err != nil {
		return nil, fmt.Errorf("build block proof meta %s: %w", tnstore.FormatBlockRef(id), err)
	}
	return &Parsed{Proof: &proof, Block: block, Meta: meta}, nil
}

func ConfigFromKeyBlock(block *tlb.Block) (*tlb.BlockchainConfig, error) {
	if block == nil || block.Extra == nil || block.Extra.Custom == nil || block.Extra.Custom.ConfigParams == nil {
		return nil, fmt.Errorf("key block proof does not contain config params")
	}
	if block.Extra.Custom.ConfigParams.Config.Params == nil {
		return nil, fmt.Errorf("key block config params are empty")
	}
	return &tlb.BlockchainConfig{Root: block.Extra.Custom.ConfigParams.Config.Params.AsCell()}, nil
}

func ConfigFromMasterchainState(current *tnstore.BlockState) (*tlb.BlockchainConfig, error) {
	if current.Parsed == nil || current.Parsed.McStateExtra == nil {
		return nil, fmt.Errorf("current masterchain state %s is missing parsed config", tnstore.FormatBlockRef(current.Block))
	}

	loader, err := current.Parsed.McStateExtra.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("begin parse mc_state_extra for %s: %w", tnstore.FormatBlockRef(current.Block), err)
	}

	var extra tlb.McStateExtra
	if err := tlb.LoadFromCell(&extra, loader); err != nil {
		return nil, fmt.Errorf("parse mc_state_extra for %s: %w", tnstore.FormatBlockRef(current.Block), err)
	}
	if extra.ConfigParams.Config.Params == nil {
		return nil, fmt.Errorf("masterchain state %s has no config params", tnstore.FormatBlockRef(current.Block))
	}
	return &tlb.BlockchainConfig{Root: extra.ConfigParams.Config.Params.AsCell()}, nil
}

func CheckMasterchainSignatures(blockID ton.BlockIDExt, block *tlb.Block, signatures *cell.Cell, cfg *tlb.BlockchainConfig) error {
	blockSignatures, err := PrepareMasterchainSignatureSet(blockID, block, signatures)
	if err != nil {
		return err
	}
	sigSet := blockSignatures.ValidatorSignatures

	validators, err := MasterchainValidatorsForBlock(cfg, &blockID, sigSet.CatchainSeqno())
	if err != nil {
		return err
	}
	prepared, err := PrepareValidatorSet(sigSet.catchainSeqno, validators)
	if err != nil {
		return fmt.Errorf("prepare validator set for %s: %w", tnstore.FormatBlockRef(blockID), err)
	}
	return CheckPreparedMasterchainSignatures(blockID, blockSignatures, prepared)
}

func LiteSignatureSet(signatures *cell.Cell) (any, error) {
	if signatures == nil {
		return nil, fmt.Errorf("masterchain block proof has no validator signatures")
	}

	blockSignatures, err := parseSignatureSet(signatures)
	if err != nil {
		return nil, err
	}
	sigSet := blockSignatures.ValidatorSignatures
	if sigSet.simplex {
		return ton.SignatureSetSimplex{
			CCSeqno:          int32(sigSet.catchainSeqno),
			ValidatorSetHash: int32(sigSet.validatorSetHash),
			Signatures:       sigSet.signatures,
			SessionID:        bytes.Clone(sigSet.sessionID),
			Slot:             sigSet.slot,
			Candidate:        bytes.Clone(sigSet.candidateData),
		}, nil
	}
	return ton.SignatureSetOrdinary{
		ValidatorSetHash: int32(sigSet.validatorSetHash),
		CatchainSeqno:    int32(sigSet.catchainSeqno),
		Signatures:       sigSet.signatures,
	}, nil
}

func NewOrdinaryValidatorSignatureSet(catchainSeqno uint32, validatorSetHash uint32, signatures []ton.Signature) *ValidatorSignatureSet {
	return &ValidatorSignatureSet{
		validatorSetHash: validatorSetHash,
		catchainSeqno:    catchainSeqno,
		signatures:       cloneSignatures(signatures),
		final:            true,
	}
}

func NewSimplexValidatorSignatureSet(catchainSeqno uint32, validatorSetHash uint32, signatures []ton.Signature, final bool, sessionID []byte, slot int32, candidateData []byte) *ValidatorSignatureSet {
	return &ValidatorSignatureSet{
		validatorSetHash: validatorSetHash,
		catchainSeqno:    catchainSeqno,
		signatures:       cloneSignatures(signatures),
		sessionID:        bytes.Clone(sessionID),
		slot:             slot,
		candidateData:    bytes.Clone(candidateData),
		final:            final,
		simplex:          true,
	}
}

// ParseBlockSignatureSetCell decodes the cell-format set together with its
// declared signer weight.
func ParseBlockSignatureSetCell(signatures *cell.Cell) (*BlockSignatureSet, error) {
	if signatures == nil {
		return nil, fmt.Errorf("validator signatures are empty")
	}

	sigSet, err := parseSignatureSet(signatures)
	if err != nil {
		return nil, err
	}
	return &sigSet, nil
}

func PrepareMasterchainSignatureSet(blockID ton.BlockIDExt, block *tlb.Block, signatures *cell.Cell) (*BlockSignatureSet, error) {
	if blockID.Workchain != -1 {
		return nil, fmt.Errorf("validator signatures are only supported for masterchain blocks, got %s", tnstore.FormatBlockRef(blockID))
	}
	if signatures == nil {
		return nil, fmt.Errorf("masterchain block proof %s has no validator signatures", tnstore.FormatBlockRef(blockID))
	}

	blockSignatures, err := ParseBlockSignatureSetCell(signatures)
	if err != nil {
		return nil, fmt.Errorf("parse validator signatures for %s: %w", tnstore.FormatBlockRef(blockID), err)
	}
	sigSet := blockSignatures.ValidatorSignatures
	if !sigSet.final {
		return nil, fmt.Errorf("masterchain block %s has non-final validator signatures", tnstore.FormatBlockRef(blockID))
	}
	if _, err = PrepareValidatorSignatureSet(blockID, block, sigSet); err != nil {
		return nil, err
	}

	return blockSignatures, nil
}

func PrepareValidatorSignatureSet(blockID ton.BlockIDExt, block *tlb.Block, sigSet *ValidatorSignatureSet) (*ValidatorSignatureSet, error) {
	if sigSet == nil {
		return nil, fmt.Errorf("block %s has no prepared validator signatures", tnstore.FormatBlockRef(blockID))
	}
	if blockID.Workchain == -1 && !sigSet.final {
		return nil, fmt.Errorf("masterchain block %s has non-final validator signatures", tnstore.FormatBlockRef(blockID))
	}
	if block.BlockInfo.GenValidatorListHashShort != sigSet.validatorSetHash {
		return nil, fmt.Errorf("validator set hash mismatch for %s: header=%08x signatures=%08x", tnstore.FormatBlockRef(blockID), block.BlockInfo.GenValidatorListHashShort, sigSet.validatorSetHash)
	}
	if block.BlockInfo.GenCatchainSeqno != sigSet.catchainSeqno {
		return nil, fmt.Errorf("catchain seqno mismatch for %s: header=%d signatures=%d", tnstore.FormatBlockRef(blockID), block.BlockInfo.GenCatchainSeqno, sigSet.catchainSeqno)
	}
	return sigSet, nil
}

func CheckPreparedMasterchainSignatures(blockID ton.BlockIDExt, blockSignatures *BlockSignatureSet, validators *PreparedValidatorSet) error {
	if blockID.Workchain != -1 {
		return fmt.Errorf("validator signatures are only supported for masterchain blocks, got %s", tnstore.FormatBlockRef(blockID))
	}
	if blockSignatures != nil && blockSignatures.ValidatorSignatures != nil && !blockSignatures.ValidatorSignatures.final {
		return fmt.Errorf("masterchain block %s has non-final validator signatures", tnstore.FormatBlockRef(blockID))
	}

	return CheckPreparedBlockSignatures(blockID, blockSignatures, validators)
}

// CheckPreparedSignatures validates a prepared signature set in full: the
// non-cryptographic half (see CheckPreparedSignatureWeight) and then an
// Ed25519 verification of every listed signature.
//
// This is the mandatory entry point for every signature set that arrived from
// the network — SyncCoordinator.CheckBlockBroadcastSignatures and
// CheckBlockFinalitySignatures are the node's trust boundary with its peers and
// nothing verified those upstream. Exactly one caller in the tree may use the
// weaker CheckPreparedSignatureWeight instead, and only because it holds a
// type-level proof that the same quorum was already verified; see the note
// there.
func CheckPreparedSignatures(blockID ton.BlockIDExt, sigSet *ValidatorSignatureSet, validators *PreparedValidatorSet) error {
	if _, err := CheckPreparedSignatureWeight(blockID, sigSet, validators); err != nil {
		return err
	}
	if err := verifySignatures(blockID, sigSet, validators); err != nil {
		return fmt.Errorf("check validator signatures for %s: %w", tnstore.FormatBlockRef(blockID), err)
	}

	return nil
}

// CheckPreparedSignatureWeight runs everything CheckPreparedSignatures does
// except the Ed25519 pass: roster identity (catchain seqno and validator set
// hash), per-signature length, unknown-validator and duplicate rejection, and
// the 2/3+1 weighted threshold. It returns the signed weight.
//
// None of that is cryptography, and none of it is implied by a verified
// quorum: the seal below is expressed in validator *indices* while the wire
// form addresses validators by node id, so this is what catches an
// index-to-node-id mapping bug, and the threshold it proves is the one computed
// in this package's own weight model — the model whose numbers land on the wire
// as SigWeight in FinalitySignaturesCell.
//
// It exists for one caller: the Simplex block accepter
// (service/validator.BlockAccepter), which holds a simplex.VerifiedCertificate
// — a type-level proof that this exact quorum was already verified by the
// consensus engine over a byte-identical signed payload. Every other caller
// must use CheckPreparedSignatures.
func CheckPreparedSignatureWeight(
	blockID ton.BlockIDExt,
	sigSet *ValidatorSignatureSet,
	validators *PreparedValidatorSet,
) (uint64, error) {
	if sigSet == nil {
		return 0, fmt.Errorf("block %s has no prepared validator signatures", tnstore.FormatBlockRef(blockID))
	}

	weight, err := validateSignatureWeight(sigSet, validators)
	if err != nil {
		return 0, fmt.Errorf("check validator signatures for %s: %w", tnstore.FormatBlockRef(blockID), err)
	}

	return weight, nil
}

// CheckPreparedBlockSignatures validates a serialized BlockSignatures value.
// Its declared weight is checked before the Ed25519 batch, then every listed
// signature is verified.
func CheckPreparedBlockSignatures(
	blockID ton.BlockIDExt,
	blockSignatures *BlockSignatureSet,
	validators *PreparedValidatorSet,
) error {
	if blockSignatures == nil || blockSignatures.ValidatorSignatures == nil {
		return fmt.Errorf("block %s has no prepared block signatures", tnstore.FormatBlockRef(blockID))
	}

	signedWeight, err := validateSignatureWeight(blockSignatures.ValidatorSignatures, validators)
	if err != nil {
		return fmt.Errorf("check block signatures for %s: %w", tnstore.FormatBlockRef(blockID), err)
	}
	if signedWeight != blockSignatures.DeclaredWeight {
		return fmt.Errorf(
			"check block signatures for %s: signature weight mismatch: declared=%d actual=%d",
			tnstore.FormatBlockRef(blockID),
			blockSignatures.DeclaredWeight,
			signedWeight,
		)
	}
	if err = verifySignatures(blockID, blockSignatures.ValidatorSignatures, validators); err != nil {
		return fmt.Errorf("check block signatures for %s: %w", tnstore.FormatBlockRef(blockID), err)
	}

	return nil
}

func (s *ValidatorSignatureSet) CatchainSeqno() uint32 {
	return s.catchainSeqno
}

func (s *ValidatorSignatureSet) Final() bool {
	return s.final
}

func (s *ValidatorSignatureSet) ValidatorSetHash() uint32 {
	return s.validatorSetHash
}

func (s *ValidatorSignatureSet) IsSimplex() bool {
	return s.simplex
}

// SignatureCount returns the number of listed validator signatures.
func (s *ValidatorSignatureSet) SignatureCount() int {
	return len(s.signatures)
}

func (s *ValidatorSignatureSet) FinalitySignaturesCell(validators *PreparedValidatorSet) (*cell.Cell, error) {
	if !s.simplex {
		return nil, fmt.Errorf("block finality signatures must use simplex signature set")
	}
	if !s.final {
		return nil, fmt.Errorf("non-final validator signatures cannot be stored in block proof")
	}

	signatures, weight, err := s.signaturesDict(validators)
	if err != nil {
		return nil, err
	}

	candidateData := cell.BeginCell()
	if err := candidateData.StoreBinarySnake(s.candidateData); err != nil {
		return nil, fmt.Errorf("store simplex candidate data: %w", err)
	}
	return tlb.ToCell(&blockSignaturesSimplex{
		ValidatorSetHash: s.validatorSetHash,
		CatchainSeqno:    s.catchainSeqno,
		SigCount:         uint32(len(s.signatures)),
		SigWeight:        weight,
		Signatures:       signatures,
		SessionID:        bytes.Clone(s.sessionID),
		Slot:             uint32(s.slot),
		CandidateData:    candidateData.EndCell(),
	})
}

func (s *ValidatorSignatureSet) signaturesDict(validators *PreparedValidatorSet) (*cell.Dictionary, uint64, error) {
	if len(validators.validators) == 0 {
		return nil, 0, fmt.Errorf("validator set is empty")
	}
	if s.catchainSeqno != validators.catchainSeqno {
		return nil, 0, fmt.Errorf("incorrect validator catchain seqno")
	}
	if s.validatorSetHash != validators.setHash {
		return nil, 0, fmt.Errorf("incorrect validator set hash")
	}

	dict := cell.NewDict(16)
	seen := make(map[[32]byte]struct{}, len(s.signatures))
	var weight uint64
	for i, sig := range s.signatures {
		var nodeID [32]byte
		if len(sig.NodeIDShort) != len(nodeID) {
			return nil, 0, fmt.Errorf("signature of unknown validator %s", hex.EncodeToString(sig.NodeIDShort))
		}
		if len(sig.Signature) != ed25519.SignatureSize {
			return nil, 0, fmt.Errorf("invalid validator signature len %d", len(sig.Signature))
		}
		copy(nodeID[:], sig.NodeIDShort)
		if _, ok := seen[nodeID]; ok {
			return nil, 0, fmt.Errorf("duplicated node signature")
		}
		seen[nodeID] = struct{}{}

		validator, ok := validators.validators[nodeID]
		if !ok {
			return nil, 0, fmt.Errorf("signature of unknown validator %s", hex.EncodeToString(sig.NodeIDShort))
		}
		weight += validator.weight

		value := cell.BeginCell().
			MustStoreSlice(sig.NodeIDShort, 256).
			MustStoreUInt(5, 4).
			MustStoreSlice(sig.Signature[:32], 256).
			MustStoreSlice(sig.Signature[32:], 256).
			EndCell()
		key := cell.BeginCell().MustStoreUInt(uint64(i), 16).EndCell()
		if err := dict.Set(key, value); err != nil {
			return nil, 0, fmt.Errorf("store validator signature %d: %w", i, err)
		}
	}
	return dict, weight, nil
}

// ContentKey digests everything a successful signature check of this set for
// blockID proves: the signed-payload inputs and the canonical (deduplicated,
// order-independent) signature list. Two sets with equal keys are
// interchangeable for "signatures already verified" purposes, so a later
// verification of an equal set against the same block may be skipped.
func (s *ValidatorSignatureSet) ContentKey(blockID ton.BlockIDExt) []byte {
	hasher := sha256.New()
	hasher.Write([]byte("gton:validator-signatures:v2"))
	var scratch [8]byte
	writeUint32 := func(v uint32) {
		binary.BigEndian.PutUint32(scratch[:4], v)
		hasher.Write(scratch[:4])
	}
	writeBytes := func(data []byte) {
		writeUint32(uint32(len(data)))
		hasher.Write(data)
	}

	writeBytes(blockID.RootHash)
	writeBytes(blockID.FileHash)
	writeUint32(s.catchainSeqno)
	writeUint32(s.validatorSetHash)
	flags := uint32(0)
	if s.final {
		flags |= 1
	}
	if s.simplex {
		flags |= 2
	}
	writeUint32(flags)
	if s.simplex {
		writeBytes(s.sessionID)
		binary.BigEndian.PutUint64(scratch[:], uint64(s.slot))
		hasher.Write(scratch[:])
		writeBytes(s.candidateData)
	}

	sigs := slices.Clone(s.signatures)
	sort.Slice(sigs, func(i, j int) bool {
		return bytes.Compare(sigs[i].NodeIDShort, sigs[j].NodeIDShort) < 0
	})
	writeUint32(uint32(len(sigs)))
	for _, sig := range sigs {
		writeBytes(sig.NodeIDShort)
		writeBytes(sig.Signature)
	}
	return hasher.Sum(nil)
}

// ContentKey includes both the logical signature set and the weight declared
// by its BlockSignatures cell. It is safe as a cache key for the complete
// cell-format verification, unlike the TL signature-set key alone.
func (s *BlockSignatureSet) ContentKey(blockID ton.BlockIDExt) []byte {
	hasher := sha256.New()
	hasher.Write([]byte("gton:block-signatures:v1"))
	hasher.Write(s.ValidatorSignatures.ContentKey(blockID))

	var weight [8]byte
	binary.BigEndian.PutUint64(weight[:], s.DeclaredWeight)
	hasher.Write(weight[:])

	return hasher.Sum(nil)
}

func MasterchainValidatorsForBlock(cfg *tlb.BlockchainConfig, block *ton.BlockIDExt, ccSeqno uint32) ([]*tlb.ValidatorAddr, error) {
	if block.Workchain != -1 {
		return nil, fmt.Errorf("only masterchain blocks are supported, got %s", tnstore.FormatBlockRef(*block))
	}
	validators, err := CurrentValidatorsForBlock(cfg, block, ccSeqno)
	if err != nil {
		return nil, fmt.Errorf("compute masterchain validators for %s: %w", tnstore.FormatBlockRef(*block), err)
	}
	return validators, nil
}

func (id blockIDExtTLB) blockID() ton.BlockIDExt {
	workchain, shard := tlb.ConvertShardIdentToShard(id.ShardID)
	return ton.BlockIDExt{
		Workchain: workchain,
		Shard:     int64(shard),
		SeqNo:     id.SeqNo,
		RootHash:  bytes.Clone(id.RootHash),
		FileHash:  bytes.Clone(id.FileHash),
	}
}

func blockIDExtTLBFromBlock(id ton.BlockIDExt) (blockIDExtTLB, error) {
	if id.Workchain == math.MinInt32 {
		return blockIDExtTLB{}, fmt.Errorf("block id workchain is invalid")
	}

	prefixBits, err := sharddomain.PrefixLength(id.Shard)
	if err != nil {
		return blockIDExtTLB{}, err
	}
	if prefixBits > 60 {
		return blockIDExtTLB{}, fmt.Errorf("block id shard prefix length %d exceeds TL-B maximum 60", prefixBits)
	}
	if err = tnstore.ValidateBlockIDHashes(id); err != nil {
		return blockIDExtTLB{}, fmt.Errorf("block id hashes: %w", err)
	}

	shardPrefix := uint64(id.Shard)
	shardPrefix &^= uint64(1) << (63 - prefixBits)
	return blockIDExtTLB{
		ShardID: tlb.ShardIdent{
			PrefixBits:  int8(prefixBits),
			WorkchainID: id.Workchain,
			ShardPrefix: shardPrefix,
		},
		SeqNo:    id.SeqNo,
		RootHash: bytes.Clone(id.RootHash),
		FileHash: bytes.Clone(id.FileHash),
	}, nil
}

func parseSignatureSet(root *cell.Cell) (BlockSignatureSet, error) {
	loader, err := root.BeginParse()
	if err != nil {
		return BlockSignatureSet{}, fmt.Errorf("begin parse block signatures: %w", err)
	}
	magic, err := loader.PreloadUInt(8)
	if err != nil {
		return BlockSignatureSet{}, fmt.Errorf("load block signatures tag: %w", err)
	}

	switch magic {
	case 0x11:
		var ordinary blockSignaturesOrdinary
		if err = loadExactTLB(&ordinary, loader); err != nil {
			return BlockSignatureSet{}, fmt.Errorf("parse ordinary block signatures: %w", err)
		}
		signatures, err := parseSignaturesDict(ordinary.Signatures, ordinary.SigCount)
		if err != nil {
			return BlockSignatureSet{}, err
		}
		return BlockSignatureSet{
			ValidatorSignatures: &ValidatorSignatureSet{
				validatorSetHash: ordinary.ValidatorSetHash,
				catchainSeqno:    ordinary.CatchainSeqno,
				signatures:       signatures,
				final:            true,
			},
			DeclaredWeight: ordinary.SigWeight,
		}, nil

	case 0x12:
		var simplex blockSignaturesSimplex
		if err = loadExactTLB(&simplex, loader); err != nil {
			return BlockSignatureSet{}, fmt.Errorf("parse simplex block signatures: %w", err)
		}
		signatures, err := parseSignaturesDict(simplex.Signatures, simplex.SigCount)
		if err != nil {
			return BlockSignatureSet{}, err
		}
		candidateLoader, err := simplex.CandidateData.BeginParse()
		if err != nil {
			return BlockSignatureSet{}, fmt.Errorf("begin parse simplex candidate data: %w", err)
		}
		candidateData, err := candidateLoader.LoadBinarySnake()
		if err != nil {
			return BlockSignatureSet{}, fmt.Errorf("load simplex candidate data: %w", err)
		}
		return BlockSignatureSet{
			ValidatorSignatures: &ValidatorSignatureSet{
				validatorSetHash: simplex.ValidatorSetHash,
				catchainSeqno:    simplex.CatchainSeqno,
				signatures:       signatures,
				sessionID:        bytes.Clone(simplex.SessionID),
				slot:             int32(simplex.Slot),
				candidateData:    candidateData,
				final:            true,
				simplex:          true,
			},
			DeclaredWeight: simplex.SigWeight,
		}, nil

	default:
		return BlockSignatureSet{}, fmt.Errorf("unsupported block signatures tag %02x", magic)
	}
}

func loadExactTLB[T any](value *T, loader *cell.Slice) error {
	if err := tlb.LoadFromCell(value, loader); err != nil {
		return err
	}
	if loader.BitsLeft() != 0 || loader.RefsNum() != 0 {
		return fmt.Errorf("trailing data: %d bits, %d refs", loader.BitsLeft(), loader.RefsNum())
	}

	return nil
}

func signaturePayload(blockID ton.BlockIDExt, sigSet *ValidatorSignatureSet) ([]byte, error) {
	if !sigSet.simplex {
		if !sigSet.final {
			return nil, fmt.Errorf("ordinary signature set cannot be non-final")
		}
		return tl.Serialize(ton.BlockID{RootHash: blockID.RootHash, FileHash: blockID.FileHash}, true)
	}

	return SimplexSignaturePayload(blockID, sigSet.final, sigSet.sessionID, sigSet.slot, sigSet.candidateData)
}

// SimplexSignaturePayload builds the exact byte string a Simplex validator
// signature covers: the boxed notarize/finalize vote over the candidate id,
// wrapped in consensus.dataToSign with the session id.
//
// It must stay byte-identical to simplex.DataToSign(sessionID,
// simplex.VoteBytes(vote)) for the same vote — the consensus engine signs
// through that constructor and this package verifies through this one. The
// equality used to be enforced implicitly, because block acceptance re-verified
// on this side every quorum the engine had already verified on that side; it is
// now pinned explicitly by TestSimplexSignaturePayloadMatchesConsensusEngine.
func SimplexSignaturePayload(blockID ton.BlockIDExt, final bool, sessionID []byte, slot int32, candidate []byte) ([]byte, error) {
	if len(sessionID) != 32 {
		return nil, fmt.Errorf("invalid simplex session id len %d", len(sessionID))
	}
	if len(candidate) == 0 {
		return nil, fmt.Errorf("empty simplex candidate")
	}

	candidateBlock, err := parseSimplexCandidateBlock(candidate)
	if err != nil {
		return nil, fmt.Errorf("parse simplex candidate: %w", err)
	}
	if !candidateBlock.Equals(&blockID) {
		return nil, fmt.Errorf("simplex candidate block id mismatch")
	}

	candidateHash := sha256.Sum256(candidate)
	candidateID := ton.ConsensusCandidateID{
		Slot: slot,
		Hash: candidateHash[:],
	}

	var vote any
	if final {
		vote = ton.ConsensusSimplexFinalizeVote{ID: candidateID}
	} else {
		vote = ConsensusSimplexNotarizeVote{ID: candidateID}
	}
	voteData, err := tl.Serialize(vote, true)
	if err != nil {
		return nil, fmt.Errorf("serialize simplex vote: %w", err)
	}

	return tl.Serialize(ton.ConsensusDataToSign{
		SessionID: sessionID,
		Data:      voteData,
	}, true)
}

func parseSimplexCandidateBlock(candidate []byte) (*ton.BlockIDExt, error) {
	var ordinary ton.ConsensusCandidateHashDataOrdinary
	left, ordinaryErr := tl.Parse(&ordinary, candidate, true)
	if ordinaryErr == nil {
		if len(left) > 0 {
			return nil, fmt.Errorf("ordinary candidate has %d trailing bytes", len(left))
		}
		return &ordinary.Block, nil
	}

	var empty ton.ConsensusCandidateHashDataEmpty
	left, emptyErr := tl.Parse(&empty, candidate, true)
	if emptyErr == nil {
		if len(left) > 0 {
			return nil, fmt.Errorf("empty candidate has %d trailing bytes", len(left))
		}
		return &empty.Block, nil
	}

	return nil, fmt.Errorf("unsupported candidate type: ordinary parse failed: %v; empty parse failed: %v", ordinaryErr, emptyErr)
}

func validateSignatureWeight(sigSet *ValidatorSignatureSet, validators *PreparedValidatorSet) (uint64, error) {
	if len(sigSet.signatures) == 0 || validators == nil || len(validators.validators) == 0 {
		return 0, fmt.Errorf("zero signatures or validators")
	}
	if sigSet.catchainSeqno != validators.catchainSeqno {
		return 0, fmt.Errorf("incorrect validator catchain seqno")
	}
	if sigSet.validatorSetHash != validators.setHash {
		return 0, fmt.Errorf("incorrect validator set hash")
	}

	var stackSeen [16]uint64
	seen := stackSeen[:]
	if len(validators.validators) > len(stackSeen)*64 {
		seen = make([]uint64, (len(validators.validators)+63)/64)
	}
	var signedWeight uint64
	for _, sig := range sigSet.signatures {
		var nodeID [32]byte
		if len(sig.NodeIDShort) != len(nodeID) {
			return 0, fmt.Errorf("signature of unknown validator %s", hex.EncodeToString(sig.NodeIDShort))
		}
		if len(sig.Signature) != ed25519.SignatureSize {
			return 0, fmt.Errorf("invalid validator signature len %d", len(sig.Signature))
		}
		copy(nodeID[:], sig.NodeIDShort)

		v, ok := validators.validators[nodeID]
		if !ok {
			return 0, fmt.Errorf("signature of unknown validator %s", hex.EncodeToString(sig.NodeIDShort))
		}
		word := v.ordinal / 64
		mask := uint64(1) << (v.ordinal % 64)
		if seen[word]&mask != 0 {
			return 0, fmt.Errorf("duplicated node signature")
		}
		seen[word] |= mask

		var carry uint64
		signedWeight, carry = bits.Add64(signedWeight, v.weight, 0)
		if carry != 0 {
			return 0, fmt.Errorf("signed validator weight overflow")
		}
	}

	if !hasSignatureThreshold(signedWeight, validators.totalWeight) {
		return 0, fmt.Errorf("insufficient signed weight (%d/%d)", signedWeight, validators.totalWeight)
	}

	return signedWeight, nil
}

func verifySignatures(
	blockID ton.BlockIDExt,
	sigSet *ValidatorSignatureSet,
	validators *PreparedValidatorSet,
) error {
	toSign, err := signaturePayload(blockID, sigSet)
	if err != nil {
		return fmt.Errorf("build validator signature payload: %w", err)
	}

	for _, sig := range sigSet.signatures {
		var nodeID [32]byte
		copy(nodeID[:], sig.NodeIDShort)
		validator := validators.validators[nodeID]
		if !ed25519.Verify(validator.publicKey, toSign, sig.Signature) {
			return fmt.Errorf("incorrect signature of validator %s", hex.EncodeToString(nodeID[:]))
		}
	}

	return nil
}

func hasSignatureThreshold(signedWeight, totalWeight uint64) bool {
	signedHigh, signedLow := bits.Mul64(signedWeight, 3)
	totalHigh, totalLow := bits.Mul64(totalWeight, 2)

	return signedHigh > totalHigh || signedHigh == totalHigh && signedLow > totalLow
}

func parseSignaturesDict(dict *cell.Dictionary, expectedCount uint32) ([]ton.Signature, error) {
	if expectedCount > maxBlockSignatures {
		return nil, fmt.Errorf("too many validator signatures: %d", expectedCount)
	}
	if dict == nil {
		if expectedCount == 0 {
			return nil, nil
		}
		return nil, fmt.Errorf("signature dict is empty, expected %d signatures", expectedCount)
	}

	items, err := dict.LoadAll()
	if err != nil {
		return nil, fmt.Errorf("load signatures dict: %w", err)
	}
	if uint32(len(items)) != expectedCount {
		return nil, fmt.Errorf("signature count mismatch: got %d, expected %d", len(items), expectedCount)
	}

	type signatureItem struct {
		index uint64
		value *cell.Slice
	}
	indexed := make([]signatureItem, 0, len(items))
	for _, item := range items {
		key, err := item.Key.LoadUInt(16)
		if err != nil {
			return nil, fmt.Errorf("load signature index: %w", err)
		}
		indexed = append(indexed, signatureItem{index: key, value: item.Value})
	}
	sort.Slice(indexed, func(i, j int) bool {
		return indexed[i].index < indexed[j].index
	})

	signatures := make([]ton.Signature, 0, len(indexed))
	for i, item := range indexed {
		if item.index != uint64(i) {
			return nil, fmt.Errorf("unexpected signature index %d, want %d", item.index, i)
		}

		nodeID, err := item.value.LoadSlice(256)
		if err != nil {
			return nil, fmt.Errorf("load signature node id: %w", err)
		}
		magic, err := item.value.LoadUInt(4)
		if err != nil {
			return nil, fmt.Errorf("load signature magic: %w", err)
		}
		if magic != 5 {
			return nil, fmt.Errorf("unsupported crypto signature magic %x", magic)
		}
		r, err := item.value.LoadSlice(256)
		if err != nil {
			return nil, fmt.Errorf("load signature R: %w", err)
		}
		s, err := item.value.LoadSlice(256)
		if err != nil {
			return nil, fmt.Errorf("load signature s: %w", err)
		}
		if item.value.BitsLeft() != 0 || item.value.RefsNum() != 0 {
			return nil, fmt.Errorf("signature value has trailing data")
		}

		signature := make([]byte, 0, len(r)+len(s))
		signature = append(signature, r...)
		signature = append(signature, s...)
		signatures = append(signatures, ton.Signature{
			NodeIDShort: bytes.Clone(nodeID),
			Signature:   signature,
		})
	}
	return signatures, nil
}
