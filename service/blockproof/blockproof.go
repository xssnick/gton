package blockproof

import (
	"bytes"
	"fmt"
	"sort"

	tnstore "github.com/xssnick/gton/service/storage"

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

type signatureSet struct {
	validatorSetHash uint32
	catchainSeqno    uint32
	signatures       []ton.Signature
	sessionID        []byte
	slot             int32
	candidateData    []byte
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
	if proofRoot == nil {
		return fmt.Errorf("block proof %s root is nil", tnstore.FormatBlockRef(id))
	}

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
		if proof.Signatures != nil {
			return fmt.Errorf("invalid masterchain proof link %s with validator signatures present", tnstore.FormatBlockRef(id))
		}
		return nil
	}
	if proof.Signatures == nil {
		return fmt.Errorf("masterchain block proof %s has no validator signatures", tnstore.FormatBlockRef(id))
	}
	return nil
}

func ParseCell(id ton.BlockIDExt, proofRoot *cell.Cell) (*Parsed, error) {
	if proofRoot == nil {
		return nil, fmt.Errorf("block proof %s root is nil", tnstore.FormatBlockRef(id))
	}

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

func ValidateStateUpdateStartsFrom(current *tnstore.BlockState, block ton.BlockIDExt, update *cell.Cell) error {
	if update == nil {
		return fmt.Errorf("block proof %s has no state update", tnstore.FormatBlockRef(block))
	}
	if update.GetType() != cell.MerkleUpdateCellType {
		return fmt.Errorf("block proof %s has non-merkle state update", tnstore.FormatBlockRef(block))
	}

	oldState, err := update.PeekRef(0)
	if err != nil {
		return fmt.Errorf("read old state hash from %s: %w", tnstore.FormatBlockRef(block), err)
	}
	oldHash := oldState.HashKey(0)

	currentRoot := current.Cell.Virtualize(0)
	currentHash := currentRoot.HashKey(0)
	if !bytes.Equal(oldHash[:], currentHash[:]) {
		return fmt.Errorf("invalid previous state hash in proof %s: expected %x, got %x", tnstore.FormatBlockRef(block), currentHash[:], oldHash[:])
	}
	return nil
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
	if current == nil {
		return nil, fmt.Errorf("current masterchain state is nil")
	}
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
	if cfg == nil {
		return fmt.Errorf("validator config is nil for %s", tnstore.FormatBlockRef(blockID))
	}
	sigSet, err := prepareMasterchainSignatureCheck(blockID, block, signatures)
	if err != nil {
		return err
	}

	validators, err := MasterchainValidatorsForBlock(cfg, &blockID, sigSet.catchainSeqno)
	if err != nil {
		return err
	}
	return checkMasterchainSignatureSet(blockID, sigSet, validators)
}

func CheckMasterchainSignaturesWithValidators(blockID ton.BlockIDExt, block *tlb.Block, signatures *cell.Cell, validators []*tlb.ValidatorAddr) error {
	sigSet, err := prepareMasterchainSignatureCheck(blockID, block, signatures)
	if err != nil {
		return err
	}
	return checkMasterchainSignatureSet(blockID, sigSet, validators)
}

func LiteSignatureSet(signatures *cell.Cell) (any, error) {
	if signatures == nil {
		return nil, fmt.Errorf("masterchain block proof has no validator signatures")
	}

	sigSet, err := parseSignatureSet(signatures)
	if err != nil {
		return nil, err
	}
	if len(sigSet.candidateData) > 0 {
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

func validateMasterchainSignatureInputs(blockID ton.BlockIDExt, block *tlb.Block, signatures *cell.Cell) error {
	if blockID.Workchain != -1 {
		return fmt.Errorf("validator signatures are only supported for masterchain blocks, got %s", tnstore.FormatBlockRef(blockID))
	}
	if block == nil {
		return fmt.Errorf("block %s is nil", tnstore.FormatBlockRef(blockID))
	}
	if signatures == nil {
		return fmt.Errorf("masterchain block proof %s has no validator signatures", tnstore.FormatBlockRef(blockID))
	}
	return nil
}

func prepareMasterchainSignatureCheck(blockID ton.BlockIDExt, block *tlb.Block, signatures *cell.Cell) (signatureSet, error) {
	if err := validateMasterchainSignatureInputs(blockID, block, signatures); err != nil {
		return signatureSet{}, err
	}

	sigSet, err := parseSignatureSet(signatures)
	if err != nil {
		return signatureSet{}, fmt.Errorf("parse validator signatures for %s: %w", tnstore.FormatBlockRef(blockID), err)
	}
	if block.BlockInfo.GenValidatorListHashShort != sigSet.validatorSetHash {
		return signatureSet{}, fmt.Errorf("validator set hash mismatch for %s: header=%08x signatures=%08x", tnstore.FormatBlockRef(blockID), block.BlockInfo.GenValidatorListHashShort, sigSet.validatorSetHash)
	}
	if block.BlockInfo.GenCatchainSeqno != sigSet.catchainSeqno {
		return signatureSet{}, fmt.Errorf("catchain seqno mismatch for %s: header=%d signatures=%d", tnstore.FormatBlockRef(blockID), block.BlockInfo.GenCatchainSeqno, sigSet.catchainSeqno)
	}
	return sigSet, nil
}

func checkMasterchainSignatureSet(blockID ton.BlockIDExt, sigSet signatureSet, validators []*tlb.ValidatorAddr) error {
	var err error
	if len(sigSet.candidateData) > 0 {
		err = ton.CheckBlockSignaturesSimplex(&blockID, sigSet.catchainSeqno, sigSet.validatorSetHash, sigSet.sessionID, sigSet.slot, sigSet.candidateData, sigSet.signatures, validators)
	} else {
		err = ton.CheckBlockSignatures(&blockID, sigSet.catchainSeqno, sigSet.validatorSetHash, sigSet.signatures, validators)
	}
	if err != nil {
		return fmt.Errorf("check validator signatures for %s: %w", tnstore.FormatBlockRef(blockID), err)
	}
	return nil
}

func MasterchainValidatorsForBlock(cfg *tlb.BlockchainConfig, block *ton.BlockIDExt, ccSeqno uint32) ([]*tlb.ValidatorAddr, error) {
	catchainCfg, err := cfg.GetCatchainConfig()
	if err != nil {
		return nil, fmt.Errorf("load catchain config: %w", err)
	}
	validatorsCfg, err := cfg.GetCurrentValidators()
	if err != nil {
		return nil, fmt.Errorf("load current validators: %w", err)
	}

	validators, err := ton.GetMainValidators(block, catchainCfg, *validatorsCfg, ccSeqno)
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

func parseSignatureSet(root *cell.Cell) (signatureSet, error) {
	var ordinary blockSignaturesOrdinary
	if ordinaryLoader, err := root.BeginParse(); err == nil && tlb.LoadFromCell(&ordinary, ordinaryLoader) == nil {
		signatures, err := parseSignaturesDict(ordinary.Signatures, ordinary.SigCount)
		if err != nil {
			return signatureSet{}, err
		}
		return signatureSet{
			validatorSetHash: ordinary.ValidatorSetHash,
			catchainSeqno:    ordinary.CatchainSeqno,
			signatures:       signatures,
		}, nil
	}

	simplexLoader, err := root.BeginParse()
	if err != nil {
		return signatureSet{}, fmt.Errorf("begin parse simplex signatures: %w", err)
	}

	var simplex blockSignaturesSimplex
	if err := tlb.LoadFromCell(&simplex, simplexLoader); err != nil {
		return signatureSet{}, err
	}
	signatures, err := parseSignaturesDict(simplex.Signatures, simplex.SigCount)
	if err != nil {
		return signatureSet{}, err
	}
	candidateLoader, err := simplex.CandidateData.BeginParse()
	if err != nil {
		return signatureSet{}, fmt.Errorf("begin parse simplex candidate data: %w", err)
	}
	candidateData, err := candidateLoader.LoadBinarySnake()
	if err != nil {
		return signatureSet{}, fmt.Errorf("load simplex candidate data: %w", err)
	}
	return signatureSet{
		validatorSetHash: simplex.ValidatorSetHash,
		catchainSeqno:    simplex.CatchainSeqno,
		signatures:       signatures,
		sessionID:        bytes.Clone(simplex.SessionID),
		slot:             int32(simplex.Slot),
		candidateData:    candidateData,
	}, nil
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
