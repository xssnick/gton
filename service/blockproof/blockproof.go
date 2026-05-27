package blockproof

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"

	tnstore "github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/adnl/keys"
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

	root, err := tlb.ToCell(&BlockProof{
		ProofFor: parsed.Proof.ProofFor,
		Root:     parsed.Proof.Root,
	})
	if err != nil {
		return nil, fmt.Errorf("serialize block proof link %s: %w", tnstore.FormatBlockRef(id), err)
	}
	return root.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false}), nil
}

func CheckProofShape(id ton.BlockIDExt, proofRoot *cell.Cell, isLink bool) error {
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
	if proof.Signatures == nil {
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
	sigSet, err := PrepareMasterchainSignatureSet(blockID, block, signatures)
	if err != nil {
		return err
	}

	validators, err := MasterchainValidatorsForBlock(cfg, &blockID, sigSet.CatchainSeqno())
	if err != nil {
		return err
	}
	return CheckPreparedMasterchainSignaturesWithValidators(blockID, sigSet, validators)
}

func CheckMasterchainSignaturesWithValidators(blockID ton.BlockIDExt, block *tlb.Block, signatures *cell.Cell, validators []*tlb.ValidatorAddr) error {
	sigSet, err := PrepareMasterchainSignatureSet(blockID, block, signatures)
	if err != nil {
		return err
	}
	return CheckPreparedMasterchainSignaturesWithValidators(blockID, sigSet, validators)
}

func LiteSignatureSet(signatures *cell.Cell) (any, error) {
	if signatures == nil {
		return nil, fmt.Errorf("masterchain block proof has no validator signatures")
	}

	sigSet, err := parseSignatureSet(signatures)
	if err != nil {
		return nil, err
	}
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

func ParseValidatorSignatureSetCell(signatures *cell.Cell) (*ValidatorSignatureSet, error) {
	if signatures == nil {
		return nil, fmt.Errorf("validator signatures are empty")
	}

	sigSet, err := parseSignatureSet(signatures)
	if err != nil {
		return nil, err
	}
	return &sigSet, nil
}

func validateMasterchainSignatureInputs(blockID ton.BlockIDExt, block *tlb.Block, signatures *cell.Cell) error {
	if blockID.Workchain != -1 {
		return fmt.Errorf("validator signatures are only supported for masterchain blocks, got %s", tnstore.FormatBlockRef(blockID))
	}
	if signatures == nil {
		return fmt.Errorf("masterchain block proof %s has no validator signatures", tnstore.FormatBlockRef(blockID))
	}
	return nil
}

func PrepareMasterchainSignatureSet(blockID ton.BlockIDExt, block *tlb.Block, signatures *cell.Cell) (*ValidatorSignatureSet, error) {
	if err := validateMasterchainSignatureInputs(blockID, block, signatures); err != nil {
		return nil, err
	}

	sigSet, err := ParseValidatorSignatureSetCell(signatures)
	if err != nil {
		return nil, fmt.Errorf("parse validator signatures for %s: %w", tnstore.FormatBlockRef(blockID), err)
	}
	if !sigSet.final {
		return nil, fmt.Errorf("masterchain block %s has non-final validator signatures", tnstore.FormatBlockRef(blockID))
	}
	return PrepareValidatorSignatureSet(blockID, block, sigSet)
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

func CheckPreparedMasterchainSignaturesWithValidators(blockID ton.BlockIDExt, sigSet *ValidatorSignatureSet, validators []*tlb.ValidatorAddr) error {
	if blockID.Workchain != -1 {
		return fmt.Errorf("validator signatures are only supported for masterchain blocks, got %s", tnstore.FormatBlockRef(blockID))
	}
	if sigSet != nil && !sigSet.final {
		return fmt.Errorf("masterchain block %s has non-final validator signatures", tnstore.FormatBlockRef(blockID))
	}
	return CheckPreparedSignaturesWithValidators(blockID, sigSet, validators)
}

func CheckPreparedSignaturesWithValidators(blockID ton.BlockIDExt, sigSet *ValidatorSignatureSet, validators []*tlb.ValidatorAddr) error {
	if sigSet == nil {
		return fmt.Errorf("block %s has no prepared validator signatures", tnstore.FormatBlockRef(blockID))
	}

	payload, err := signaturePayload(blockID, sigSet)
	if err != nil {
		return fmt.Errorf("build validator signature payload for %s: %w", tnstore.FormatBlockRef(blockID), err)
	}
	if err = checkSignaturesPayload(payload, sigSet.catchainSeqno, sigSet.validatorSetHash, cloneSignatures(sigSet.signatures), validators); err != nil {
		return fmt.Errorf("check validator signatures for %s: %w", tnstore.FormatBlockRef(blockID), err)
	}
	return nil
}

func (s *ValidatorSignatureSet) CatchainSeqno() uint32 {
	if s == nil {
		return 0
	}
	return s.catchainSeqno
}

func (s *ValidatorSignatureSet) ValidatorSetHash() uint32 {
	if s == nil {
		return 0
	}
	return s.validatorSetHash
}

func (s *ValidatorSignatureSet) IsFinal() bool {
	return s != nil && s.final
}

func (s *ValidatorSignatureSet) IsSimplex() bool {
	return s != nil && s.simplex
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

func parseSignatureSet(root *cell.Cell) (ValidatorSignatureSet, error) {
	var ordinary blockSignaturesOrdinary
	if ordinaryLoader, err := root.BeginParse(); err == nil && tlb.LoadFromCell(&ordinary, ordinaryLoader) == nil {
		signatures, err := parseSignaturesDict(ordinary.Signatures, ordinary.SigCount)
		if err != nil {
			return ValidatorSignatureSet{}, err
		}
		return ValidatorSignatureSet{
			validatorSetHash: ordinary.ValidatorSetHash,
			catchainSeqno:    ordinary.CatchainSeqno,
			signatures:       signatures,
			final:            true,
		}, nil
	}

	simplexLoader, err := root.BeginParse()
	if err != nil {
		return ValidatorSignatureSet{}, fmt.Errorf("begin parse simplex signatures: %w", err)
	}

	var simplex blockSignaturesSimplex
	if err := tlb.LoadFromCell(&simplex, simplexLoader); err != nil {
		return ValidatorSignatureSet{}, err
	}
	signatures, err := parseSignaturesDict(simplex.Signatures, simplex.SigCount)
	if err != nil {
		return ValidatorSignatureSet{}, err
	}
	candidateLoader, err := simplex.CandidateData.BeginParse()
	if err != nil {
		return ValidatorSignatureSet{}, fmt.Errorf("begin parse simplex candidate data: %w", err)
	}
	candidateData, err := candidateLoader.LoadBinarySnake()
	if err != nil {
		return ValidatorSignatureSet{}, fmt.Errorf("load simplex candidate data: %w", err)
	}
	return ValidatorSignatureSet{
		validatorSetHash: simplex.ValidatorSetHash,
		catchainSeqno:    simplex.CatchainSeqno,
		signatures:       signatures,
		sessionID:        bytes.Clone(simplex.SessionID),
		slot:             int32(simplex.Slot),
		candidateData:    candidateData,
		final:            true,
		simplex:          true,
	}, nil
}

func signaturePayload(blockID ton.BlockIDExt, sigSet *ValidatorSignatureSet) ([]byte, error) {
	if !sigSet.simplex {
		if !sigSet.final {
			return nil, fmt.Errorf("ordinary signature set cannot be non-final")
		}
		return tl.Serialize(ton.BlockID{RootHash: blockID.RootHash, FileHash: blockID.FileHash}, true)
	}

	return buildSimplexToSignPayload(blockID, sigSet.final, sigSet.sessionID, sigSet.slot, sigSet.candidateData)
}

func buildSimplexToSignPayload(blockID ton.BlockIDExt, final bool, sessionID []byte, slot int32, candidate []byte) ([]byte, error) {
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

func checkSignaturesPayload(toSign []byte, chainSeqno, setHash uint32, sigs []ton.Signature, validators []*tlb.ValidatorAddr) error {
	if len(sigs) == 0 || len(validators) == 0 {
		return fmt.Errorf("zero signatures or validators")
	}

	calcedSetHash, err := ValidatorSetHash(chainSeqno, validators)
	if err != nil {
		return fmt.Errorf("calc validator set hash: %w", err)
	}
	if setHash != calcedSetHash {
		return fmt.Errorf("incorrect validator set hash")
	}

	var totalWeight, signedWeight uint64
	validatorsMap := map[string]*tlb.ValidatorAddr{}
	for _, v := range validators {
		kid, err := tl.Hash(keys.PublicKeyED25519{Key: v.PublicKey.Key})
		if err != nil {
			return fmt.Errorf("calc validator key id: %w", err)
		}

		totalWeight += v.Weight
		validatorsMap[string(kid)] = v
	}

	sort.Slice(sigs, func(i, j int) bool {
		return string(sigs[i].NodeIDShort) < string(sigs[j].NodeIDShort)
	})

	for i, sig := range sigs {
		if i > 0 && string(sigs[i-1].NodeIDShort) == string(sig.NodeIDShort) {
			return fmt.Errorf("duplicated node signature")
		}

		v, ok := validatorsMap[string(sig.NodeIDShort)]
		if !ok {
			return fmt.Errorf("signature of unknown validator %s", hex.EncodeToString(sig.NodeIDShort))
		}
		if !ed25519.Verify(v.PublicKey.Key, toSign, sig.Signature) {
			return fmt.Errorf("incorrect signature of validator %s", hex.EncodeToString(sig.NodeIDShort))
		}

		signedWeight += v.Weight
		if signedWeight > totalWeight {
			break
		}
	}

	if 3*signedWeight <= 2*totalWeight {
		return fmt.Errorf("insufficient signed weight (%d/%d)", 3*signedWeight, 2*totalWeight)
	}
	return nil
}

func parseSignaturesDict(dict *cell.Dictionary, expectedCount uint32) ([]ton.Signature, error) {
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
