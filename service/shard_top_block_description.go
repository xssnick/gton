package service

import (
	"bytes"
	"fmt"

	"github.com/xssnick/gton/service/blockproof"
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
	Signatures       *blockproof.ValidatorSignatureSet
	Chain            []shardTopBlockDescriptionLink
}

type shardTopBlockDescriptionLink struct {
	Block          ton.BlockIDExt
	PrevRefs       []ton.BlockIDExt
	MasterchainRef *ton.BlockIDExt
}

type shardDescriptionBlockIDExtTLB struct {
	ShardID  tlb.ShardIdent `tlb:"."`
	SeqNo    uint32         `tlb:"## 32"`
	RootHash []byte         `tlb:"bits 256"`
	FileHash []byte         `tlb:"bits 256"`
}

type shardDescriptionSignatureMeta struct {
	ValidatorSetHash uint32
	CatchainSeqno    uint32
	Count            uint32
}

func parseShardTopBlockDescription(block ton.BlockIDExt, catchainSeqno int32, data []byte) (*shardTopBlockDescription, error) {
	if catchainSeqno < 0 {
		return nil, fmt.Errorf("negative catchain seqno %d", catchainSeqno)
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("shard top block description is empty")
	}

	root, err := cell.FromBOC(data)
	if err != nil {
		return nil, fmt.Errorf("parse shard top block description boc: %w", err)
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

	signatureMeta, err := parseShardDescriptionSignatureMeta(signatures)
	if err != nil {
		return nil, err
	}
	signatureSet, err := blockproof.ParseValidatorSignatureSetCell(signatures)
	if err != nil {
		return nil, fmt.Errorf("parse shard top block description signatures: %w", err)
	}
	if signatureMeta.Count == 0 {
		return nil, fmt.Errorf("shard top block description has empty validator signatures")
	}
	if signatureMeta.CatchainSeqno != uint32(catchainSeqno) {
		return nil, fmt.Errorf("shard top block description catchain seqno mismatch: broadcast=%d signatures=%d", catchainSeqno, signatureMeta.CatchainSeqno)
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

	return &shardTopBlockDescription{
		Block:            proofFor,
		CatchainSeqno:    signatureMeta.CatchainSeqno,
		ValidatorSetHash: signatureMeta.ValidatorSetHash,
		Signatures:       signatureSet,
		Chain:            links,
	}, nil
}

func loadShardDescriptionBlockID(s *cell.Slice) (ton.BlockIDExt, error) {
	var id shardDescriptionBlockIDExtTLB
	if err := tlb.LoadFromCell(&id, s); err != nil {
		return ton.BlockIDExt{}, err
	}

	workchain, shard := tlb.ConvertShardIdentToShard(id.ShardID)
	return ton.BlockIDExt{
		Workchain: workchain,
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

func parseShardDescriptionSignatureMeta(root *cell.Cell) (shardDescriptionSignatureMeta, error) {
	s, err := root.BeginParse()
	if err != nil {
		return shardDescriptionSignatureMeta{}, fmt.Errorf("parse shard top block description signature: %w", err)
	}
	magic, err := s.LoadUInt(8)
	if err != nil {
		return shardDescriptionSignatureMeta{}, fmt.Errorf("load shard top block description signature magic: %w", err)
	}
	if magic != 0x11 && magic != 0x12 {
		return shardDescriptionSignatureMeta{}, fmt.Errorf("unsupported shard top block description signature magic %02x", magic)
	}

	validatorSetHash, err := s.LoadUInt(32)
	if err != nil {
		return shardDescriptionSignatureMeta{}, fmt.Errorf("load shard validator set hash: %w", err)
	}
	catchainSeqno, err := s.LoadUInt(32)
	if err != nil {
		return shardDescriptionSignatureMeta{}, fmt.Errorf("load shard catchain seqno: %w", err)
	}
	count, err := s.LoadUInt(32)
	if err != nil {
		return shardDescriptionSignatureMeta{}, fmt.Errorf("load shard signature count: %w", err)
	}

	return shardDescriptionSignatureMeta{
		ValidatorSetHash: uint32(validatorSetHash),
		CatchainSeqno:    uint32(catchainSeqno),
		Count:            uint32(count),
	}, nil
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

		links = append(links, shardTopBlockDescriptionLink{
			Block:          current,
			PrevRefs:       append([]ton.BlockIDExt(nil), meta.PrevRefs...),
			MasterchainRef: meta.MasterchainRef,
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

		if parsed.BlockInfo.AfterSplit || parsed.BlockInfo.AfterMerge || parsed.BlockInfo.BeforeSplit {
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
