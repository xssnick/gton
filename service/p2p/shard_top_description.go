package p2p

import (
	"bytes"
	"fmt"
	"math"

	"github.com/xssnick/gton/service/blockproof"
	sharddomain "github.com/xssnick/gton/service/shard"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const topBlockDescriptionMagic = 0xd5

type shardTopBlockDescription struct {
	Block            ton.BlockIDExt
	CatchainSeqno    uint32
	ValidatorSetHash uint32
	Signatures       *blockproof.BlockSignatureSet
	Chain            []shardTopBlockDescriptionLink
}

// ParsedShardTopDescription is the strict protocol projection used both by
// inbound broadcast verification and by the local validator acceptance path.
// Signature authentication remains the caller's policy decision.
type ParsedShardTopDescription struct {
	Description *ShardBlockDescription
	Signatures  *blockproof.BlockSignatureSet
}

type shardTopBlockDescriptionLink struct {
	Block          ton.BlockIDExt
	PrevRefs       []ton.BlockIDExt
	MasterchainRef *ton.BlockIDExt
	ProofRoot      *cell.Cell
	GenUtime       uint32
	VertSeqno      uint32
	StartLT        uint64
	EndLT          uint64
	MinRefMCSeqno  uint32
	BeforeSplit    bool
	AfterSplit     bool
	AfterMerge     bool
	WantSplit      bool
	WantMerge      bool
	CreatedBy      [32]byte
	FeesCollected  tlb.CurrencyCollection
	FundsCreated   tlb.CurrencyCollection
}

type shardDescriptionBlockIDExtTLB struct {
	ShardID  shardDescriptionShardIdentTLB `tlb:"."`
	SeqNo    uint32                        `tlb:"## 32"`
	RootHash []byte                        `tlb:"bits 256"`
	FileHash []byte                        `tlb:"bits 256"`
}

// tlb.ShardIdent currently models shard_pfx_bits as a signed int8. The field
// is an unsigned six-bit natural in block.tlb, so this wire boundary keeps its
// own exact representation and can encode/decode every canonical depth 0..60.
type shardDescriptionShardIdentTLB struct {
	_           tlb.Magic `tlb:"$00"`
	PrefixBits  uint8     `tlb:"## 6"`
	WorkchainID int32     `tlb:"## 32"`
	ShardPrefix uint64    `tlb:"## 64"`
}

type shardDescriptionSignatureMeta struct {
	ValidatorSetHash uint32
	CatchainSeqno    uint32
}

// BuildShardTopBlockDescription builds the exact top_block_descr#d5 envelope.
// Proofs are ordered newest to oldest, as in AcceptBlockQuery.
func BuildShardTopBlockDescription(
	block ton.BlockIDExt,
	signatures *cell.Cell,
	proofs []*cell.Cell,
) (*cell.Cell, error) {
	if err := storage.ValidateBlockIDHashes(block); err != nil {
		return nil, err
	}
	if block.Workchain == -1 {
		return nil, fmt.Errorf("masterchain block cannot be a shard top")
	}
	if signatures == nil {
		return nil, fmt.Errorf("shard top block description has no signatures")
	}
	if len(proofs) == 0 || len(proofs) > 8 {
		return nil, fmt.Errorf("invalid shard top block description proof count %d", len(proofs))
	}
	for index, proof := range proofs {
		if proof == nil || proof.GetType() != cell.MerkleProofCellType {
			return nil, fmt.Errorf("shard top block description proof %d is not a Merkle proof", index)
		}
	}

	target, err := shardDescriptionBlockIDFromBlock(block)
	if err != nil {
		return nil, fmt.Errorf("encode shard top block description target: %w", err)
	}
	targetCell, err := tlb.ToCell(&target)
	if err != nil {
		return nil, fmt.Errorf("encode shard top block description target: %w", err)
	}

	var previous *cell.Cell
	for index := len(proofs) - 1; index > 0; index-- {
		link := cell.BeginCell().MustStoreRef(proofs[index])
		if previous != nil {
			link.MustStoreRef(previous)
		}
		previous = link.EndCell()
	}

	builder := cell.BeginCell().
		MustStoreUInt(topBlockDescriptionMagic, 8).
		MustStoreBuilder(targetCell.ToBuilder()).
		MustStoreBoolBit(true).
		MustStoreRef(signatures).
		MustStoreUInt(uint64(len(proofs)), 8).
		MustStoreRef(proofs[0])
	if previous != nil {
		builder.MustStoreRef(previous)
	}
	return builder.EndCell(), nil
}

func shardDescriptionBlockIDFromBlock(block ton.BlockIDExt) (shardDescriptionBlockIDExtTLB, error) {
	if block.Workchain == math.MinInt32 {
		return shardDescriptionBlockIDExtTLB{}, fmt.Errorf("invalid workchain %d", block.Workchain)
	}
	prefixBits, err := sharddomain.PrefixLength(block.Shard)
	if err != nil {
		return shardDescriptionBlockIDExtTLB{}, err
	}
	if prefixBits > 60 {
		return shardDescriptionBlockIDExtTLB{}, fmt.Errorf("shard prefix length %d exceeds 60", prefixBits)
	}
	marker := uint64(1) << (63 - prefixBits)

	return shardDescriptionBlockIDExtTLB{
		ShardID: shardDescriptionShardIdentTLB{
			PrefixBits:  uint8(prefixBits),
			WorkchainID: block.Workchain,
			ShardPrefix: uint64(block.Shard) &^ marker,
		},
		SeqNo:    block.SeqNo,
		RootHash: bytes.Clone(block.RootHash),
		FileHash: bytes.Clone(block.FileHash),
	}, nil
}

func ParseShardTopBlockDescription(
	block ton.BlockIDExt,
	catchainSeqno int32,
	root *cell.Cell,
) (*ParsedShardTopDescription, error) {
	if root == nil || root.IsSpecial() {
		return nil, fmt.Errorf("shard top block description root is nil or special")
	}

	s, err := root.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("parse shard top block description: %w", err)
	}
	magic, err := s.LoadUInt(8)
	if err != nil {
		return nil, fmt.Errorf("load shard top block description magic: %w", err)
	}
	if magic != topBlockDescriptionMagic {
		return nil, fmt.Errorf("unexpected shard top block description magic %02x", magic)
	}

	proofFor, err := loadShardDescriptionBlockID(s)
	if err != nil {
		return nil, fmt.Errorf("load shard top block description target: %w", err)
	}
	if !proofFor.Equals(&block) {
		return nil, fmt.Errorf("shard top block description is for %s, expected %s", storage.FormatBlockRef(proofFor), storage.FormatBlockRef(block))
	}

	signatures, err := loadMaybeRefCell(s)
	if err != nil {
		return nil, fmt.Errorf("load shard top block description signatures: %w", err)
	}
	if signatures == nil {
		return nil, fmt.Errorf("shard top block description has no validator signatures")
	}

	signatureSet, err := blockproof.ParseBlockSignatureSetCell(signatures)
	if err != nil {
		return nil, fmt.Errorf("parse shard top block description signatures: %w", err)
	}
	validatorSignatures := signatureSet.ValidatorSignatures
	if validatorSignatures.SignatureCount() == 0 {
		return nil, fmt.Errorf("shard top block description has empty validator signatures")
	}
	broadcastCatchainSeqno := uint32(catchainSeqno)
	if validatorSignatures.CatchainSeqno() != broadcastCatchainSeqno {
		return nil, fmt.Errorf(
			"shard top block description catchain seqno mismatch: broadcast=%d signatures=%d",
			broadcastCatchainSeqno,
			validatorSignatures.CatchainSeqno(),
		)
	}
	signatureMeta := shardDescriptionSignatureMeta{
		ValidatorSetHash: validatorSignatures.ValidatorSetHash(),
		CatchainSeqno:    validatorSignatures.CatchainSeqno(),
	}

	length, err := s.LoadUInt(8)
	if err != nil {
		return nil, fmt.Errorf("load shard top block description chain length: %w", err)
	}
	if length == 0 || length > 8 {
		return nil, fmt.Errorf("invalid shard top block description chain length %d", length)
	}

	proofs, err := loadShardDescriptionProofChain(s, int(length))
	if err != nil {
		return nil, err
	}

	links, err := validateShardDescriptionProofChain(proofFor, signatureMeta, proofs)
	if err != nil {
		return nil, err
	}

	parsed := &shardTopBlockDescription{
		Block:            proofFor,
		CatchainSeqno:    signatureMeta.CatchainSeqno,
		ValidatorSetHash: signatureMeta.ValidatorSetHash,
		Signatures:       signatureSet,
		Chain:            links,
	}
	description, err := shardBlockDescriptionFromParsed(parsed)
	if err != nil {
		return nil, err
	}

	return &ParsedShardTopDescription{Description: description, Signatures: signatureSet}, nil
}

func shardBlockDescriptionFromParsed(desc *shardTopBlockDescription) (*ShardBlockDescription, error) {
	out := &ShardBlockDescription{
		Block:            desc.Block,
		CatchainSeqno:    desc.CatchainSeqno,
		ValidatorSetHash: desc.ValidatorSetHash,
		Chain:            make([]ShardDescriptionLink, 0, len(desc.Chain)),
	}
	for _, link := range desc.Chain {
		proofRoot, proofBOC, err := blockproof.LinkFromRoot(link.Block, link.ProofRoot)
		if err != nil {
			return nil, fmt.Errorf(
				"build shard description proof link for %s: %w",
				storage.FormatBlockRef(link.Block),
				err,
			)
		}

		var masterchainRef *ton.BlockIDExt
		if link.MasterchainRef != nil {
			ref := *link.MasterchainRef.Copy()
			masterchainRef = &ref
		}
		out.Chain = append(out.Chain, ShardDescriptionLink{
			Block:          *link.Block.Copy(),
			PrevRefs:       append([]ton.BlockIDExt(nil), link.PrevRefs...),
			MasterchainRef: masterchainRef,
			TopBlockProof:  link.ProofRoot,
			ProofRoot:      proofRoot,
			ProofBOC:       proofBOC,
			GenUtime:       link.GenUtime,
			VertSeqno:      link.VertSeqno,
			StartLT:        link.StartLT,
			EndLT:          link.EndLT,
			MinRefMCSeqno:  link.MinRefMCSeqno,
			BeforeSplit:    link.BeforeSplit,
			AfterSplit:     link.AfterSplit,
			AfterMerge:     link.AfterMerge,
			WantSplit:      link.WantSplit,
			WantMerge:      link.WantMerge,
			CreatedBy:      link.CreatedBy,
			FeesCollected:  link.FeesCollected,
			FundsCreated:   link.FundsCreated,
		})
	}

	return out, nil
}

func loadShardDescriptionBlockID(s *cell.Slice) (ton.BlockIDExt, error) {
	var id shardDescriptionBlockIDExtTLB
	if err := tlb.LoadFromCell(&id, s); err != nil {
		return ton.BlockIDExt{}, err
	}
	prefixBits := int(id.ShardID.PrefixBits)
	if prefixBits > 60 {
		return ton.BlockIDExt{}, fmt.Errorf("shard prefix length %d exceeds 60", prefixBits)
	}
	if id.ShardID.WorkchainID == math.MinInt32 {
		return ton.BlockIDExt{}, fmt.Errorf("invalid workchain %d", id.ShardID.WorkchainID)
	}

	suffixMask := ^uint64(0)
	if prefixBits != 0 {
		suffixMask = uint64(1)<<(64-prefixBits) - 1
	}
	if id.ShardID.ShardPrefix&suffixMask != 0 {
		return ton.BlockIDExt{}, fmt.Errorf("shard prefix has non-zero bits after depth %d", prefixBits)
	}

	shard := id.ShardID.ShardPrefix | uint64(1)<<(63-prefixBits)
	return ton.BlockIDExt{
		Workchain: id.ShardID.WorkchainID,
		Shard:     int64(shard),
		SeqNo:     id.SeqNo,
		RootHash:  bytes.Clone(id.RootHash),
		FileHash:  bytes.Clone(id.FileHash),
	}, nil
}

func loadMaybeRefCell(s *cell.Slice) (*cell.Cell, error) {
	has, err := s.LoadBoolBit()
	if err != nil {
		return nil, err
	}
	if !has {
		return nil, nil
	}
	return s.LoadRefCell()
}

func loadShardDescriptionProofChain(s *cell.Slice, length int) ([]*cell.Cell, error) {
	if length < 1 || length > 8 {
		return nil, fmt.Errorf("invalid shard top block description chain length %d", length)
	}

	root, err := s.LoadRefCell()
	if err != nil {
		return nil, fmt.Errorf("load shard top block description proof link: %w", err)
	}

	proofs := []*cell.Cell{root}
	if length > 1 {
		next, err := s.LoadRef()
		if err != nil {
			return nil, fmt.Errorf("load shard top block description previous proof chain: %w", err)
		}
		rest, err := loadShardDescriptionProofChain(next, length-1)
		if err != nil {
			return nil, err
		}
		proofs = append(proofs, rest...)
	}

	if s.BitsLeft() != 0 || s.RefsNum() != 0 {
		return nil, fmt.Errorf("shard top block description proof chain has trailing data")
	}
	return proofs, nil
}

func validateShardDescriptionProofChain(block ton.BlockIDExt, signatures shardDescriptionSignatureMeta, proofs []*cell.Cell) ([]shardTopBlockDescriptionLink, error) {
	if len(proofs) == 0 || len(proofs) > 8 {
		return nil, fmt.Errorf("invalid shard top block description proof count %d", len(proofs))
	}

	current := block
	links := make([]shardTopBlockDescriptionLink, 0, len(proofs))
	for i, proof := range proofs {
		parsed, err := ton.CheckBlockProof(proof, current.RootHash)
		if err != nil {
			return nil, fmt.Errorf("check shard top block description proof for %s: %w", storage.FormatBlockRef(current), err)
		}
		if err = storage.VerifyBlockIdentity(current, parsed); err != nil {
			return nil, err
		}
		if parsed.BlockInfo.Version != 0 {
			return nil, fmt.Errorf("shard top block description proof %s has unsupported block version %d", storage.FormatBlockRef(current), parsed.BlockInfo.Version)
		}
		if !parsed.BlockInfo.NotMaster || parsed.BlockInfo.MasterRef == nil {
			return nil, fmt.Errorf("shard top block description proof %s is not a shard block", storage.FormatBlockRef(current))
		}
		if parsed.BlockInfo.GenCatchainSeqno != signatures.CatchainSeqno {
			return nil, fmt.Errorf("shard top block description proof %s catchain seqno mismatch: header=%d signatures=%d", storage.FormatBlockRef(current), parsed.BlockInfo.GenCatchainSeqno, signatures.CatchainSeqno)
		}
		if parsed.BlockInfo.GenValidatorListHashShort != signatures.ValidatorSetHash {
			return nil, fmt.Errorf("shard top block description proof %s validator set hash mismatch: header=%08x signatures=%08x", storage.FormatBlockRef(current), parsed.BlockInfo.GenValidatorListHashShort, signatures.ValidatorSetHash)
		}

		meta, err := storage.BuildBlockMetaFromParsedBlock(current, parsed)
		if err != nil {
			return nil, fmt.Errorf("build shard top block description proof metadata for %s: %w", storage.FormatBlockRef(current), err)
		}
		if len(meta.PrevRefs) == 0 || len(meta.PrevRefs) > 2 {
			return nil, fmt.Errorf("shard top block description proof %s has %d previous refs", storage.FormatBlockRef(current), len(meta.PrevRefs))
		}
		if parsed.ValueFlow == nil {
			return nil, fmt.Errorf("shard top block description proof %s has no value flow", storage.FormatBlockRef(current))
		}
		valueFlow, err := parseShardDescriptionValueFlow(parsed.ValueFlow)
		if err != nil {
			return nil, fmt.Errorf("parse shard top block description proof %s value flow: %w", storage.FormatBlockRef(current), err)
		}
		if parsed.Extra == nil || len(parsed.Extra.CreatedBy) != 32 {
			return nil, fmt.Errorf("shard top block description proof %s has no creator id", storage.FormatBlockRef(current))
		}

		masterchainRef := masterchainBlockIDFromExtRef(parsed.BlockInfo.MasterRef)
		if len(links) > 0 {
			newer := &links[len(links)-1]
			if newer.MasterchainRef.SeqNo < masterchainRef.SeqNo {
				return nil, fmt.Errorf(
					"shard top block description proof %s refers to newer masterchain block %s than next link %s",
					storage.FormatBlockRef(current),
					storage.FormatBlockRef(masterchainRef),
					storage.FormatBlockRef(*newer.MasterchainRef),
				)
			}
			if newer.MasterchainRef.SeqNo == masterchainRef.SeqNo && !newer.MasterchainRef.Equals(&masterchainRef) {
				return nil, fmt.Errorf("shard top block description proof %s refers to a different masterchain fork at seqno %d", storage.FormatBlockRef(current), masterchainRef.SeqNo)
			}
			if parsed.BlockInfo.GenUtime > newer.GenUtime {
				return nil, fmt.Errorf("shard top block description proof %s generation time %d exceeds next link time %d", storage.FormatBlockRef(current), parsed.BlockInfo.GenUtime, newer.GenUtime)
			}
			if parsed.BlockInfo.VertSeqNo != links[0].VertSeqno {
				return nil, fmt.Errorf("shard top block description proof %s vertical seqno %d differs from chain value %d", storage.FormatBlockRef(current), parsed.BlockInfo.VertSeqNo, links[0].VertSeqno)
			}
			if parsed.BlockInfo.BeforeSplit {
				return nil, fmt.Errorf("non-tip shard top block description proof %s is before a split", storage.FormatBlockRef(current))
			}
		}

		var createdBy [32]byte
		copy(createdBy[:], parsed.Extra.CreatedBy)

		links = append(links, shardTopBlockDescriptionLink{
			Block:          current,
			PrevRefs:       append([]ton.BlockIDExt(nil), meta.PrevRefs...),
			MasterchainRef: &masterchainRef,
			ProofRoot:      proof,
			GenUtime:       parsed.BlockInfo.GenUtime,
			VertSeqno:      parsed.BlockInfo.VertSeqNo,
			StartLT:        parsed.BlockInfo.StartLt,
			EndLT:          parsed.BlockInfo.EndLt,
			MinRefMCSeqno:  parsed.BlockInfo.MinRefMcSeqno,
			BeforeSplit:    parsed.BlockInfo.BeforeSplit,
			AfterSplit:     parsed.BlockInfo.AfterSplit,
			AfterMerge:     parsed.BlockInfo.AfterMerge,
			WantSplit:      parsed.BlockInfo.WantSplit,
			WantMerge:      parsed.BlockInfo.WantMerge,
			CreatedBy:      createdBy,
			FeesCollected:  valueFlow.FeesCollected,
			FundsCreated:   valueFlow.Created,
		})

		maxPrevSeqno := meta.PrevRefs[0].SeqNo
		for _, prev := range meta.PrevRefs[1:] {
			if prev.SeqNo > maxPrevSeqno {
				maxPrevSeqno = prev.SeqNo
			}
		}
		if maxPrevSeqno+1 != current.SeqNo {
			return nil, fmt.Errorf("shard top block description proof %s is not the next shard block after previous refs", storage.FormatBlockRef(current))
		}

		if i+1 == len(proofs) {
			break
		}

		if parsed.BlockInfo.AfterSplit || parsed.BlockInfo.AfterMerge {
			return nil, fmt.Errorf("intermediate shard top block description proof %s crosses split or merge boundary", storage.FormatBlockRef(current))
		}
		if len(meta.PrevRefs) != 1 {
			return nil, fmt.Errorf("intermediate shard top block description proof %s has %d previous refs", storage.FormatBlockRef(current), len(meta.PrevRefs))
		}
		prev := meta.PrevRefs[0]
		if prev.Workchain != current.Workchain || prev.Shard != current.Shard || prev.SeqNo+1 != current.SeqNo {
			return nil, fmt.Errorf("intermediate shard top block description proof %s does not link to direct predecessor %s", storage.FormatBlockRef(current), storage.FormatBlockRef(prev))
		}
		current = prev
	}
	return links, nil
}

func parseShardDescriptionValueFlow(root *cell.Cell) (tlb.ValueFlow, error) {
	loader, err := root.BeginParse()
	if err != nil {
		return tlb.ValueFlow{}, err
	}

	var flow tlb.ValueFlow
	if err = tlb.LoadFromCell(&flow, loader); err != nil {
		return tlb.ValueFlow{}, err
	}
	if loader.BitsLeft() != 0 || loader.RefsNum() != 0 {
		return tlb.ValueFlow{}, fmt.Errorf("value flow has %d trailing bits and %d refs", loader.BitsLeft(), loader.RefsNum())
	}

	return flow, nil
}

func masterchainBlockIDFromExtRef(ref *tlb.ExtBlkRef) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: -1,
		Shard:     topShard,
		SeqNo:     ref.SeqNo,
		RootHash:  bytes.Clone(ref.RootHash),
		FileHash:  bytes.Clone(ref.FileHash),
	}
}
