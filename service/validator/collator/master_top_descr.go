package collator

import (
	"bytes"
	"fmt"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const masterTopBlockDescrTag = uint64(0xd5)
const masterTopBlockDescrSetTag = uint64(0x4ac789f3)

type masterTopBlockDescrID struct {
	Shard    tlb.ShardIdent `tlb:"."`
	SeqNo    uint32         `tlb:"## 32"`
	RootHash []byte         `tlb:"bits 256"`
	FileHash []byte         `tlb:"bits 256"`
}

func buildMasterTopBlockDescrSet(tops []ShardTop) (*cell.Cell, error) {
	dict := cell.NewDict(96)
	for i := range tops {
		block := &tops[i].Block
		key := cell.BeginCell().
			MustStoreInt(int64(block.Workchain), 32).
			MustStoreUInt(uint64(block.Shard), 64).
			EndCell()
		value := cell.BeginCell().MustStoreRef(tops[i].TopBlockDescr).EndCell()
		if err := dict.Set(key, value); err != nil {
			return nil, fmt.Errorf("serialize shard TopBlockDescr %d: %w", i, err)
		}
	}
	set := cell.BeginCell().MustStoreUInt(masterTopBlockDescrSetTag, 32)
	if err := set.StoreDict(dict); err != nil {
		return nil, fmt.Errorf("serialize shard top descriptor set: %w", err)
	}

	return set.EndCell(), nil
}

// masterTopBlockDescrEnvelope is the decoded top_block_descr#d5 header: the tip
// it claims to prove, its optional signature cell, the declared proof-chain
// length and the slice positioned at the first proof link.
type masterTopBlockDescrEnvelope struct {
	target     ton.BlockIDExt
	signatures *cell.Cell
	length     int
	chain      *cell.Slice
}

// parseMasterTopBlockDescrEnvelope decodes only the part of top_block_descr#d5
// that the collation and validation sides read identically. Every policy
// decision stays with the caller: whether the target must match a registry
// entry, whether signatures are required and authenticated, how many links the
// transition consumes, and how the chain itself is walked.
func parseMasterTopBlockDescrEnvelope(root *cell.Cell) (masterTopBlockDescrEnvelope, error) {
	if root == nil || root.IsSpecial() {
		return masterTopBlockDescrEnvelope{}, fmt.Errorf("TopBlockDescr is nil or special")
	}
	loader, err := root.BeginParse()
	if err != nil {
		return masterTopBlockDescrEnvelope{}, fmt.Errorf("parse TopBlockDescr: %w", err)
	}
	tag, err := loader.LoadUInt(8)
	if err != nil || tag != masterTopBlockDescrTag {
		return masterTopBlockDescrEnvelope{}, fmt.Errorf("invalid TopBlockDescr tag")
	}
	var proofFor masterTopBlockDescrID
	if err = tlb.LoadFromCell(&proofFor, loader); err != nil {
		return masterTopBlockDescrEnvelope{}, fmt.Errorf("decode TopBlockDescr target: %w", err)
	}
	// crypto/block/block-parse.cpp:2113-2124 ShardIdent::unpack reads the prefix
	// length with fetch_uint_leq(60), rejects the invalid workchain marker and
	// requires the prefix to carry no bits below its own length. tlb.ShardIdent
	// accepts all six length bits and applies none of that, so a non-canonical
	// encoding of an otherwise correct tip would pass the target comparison
	// below while the reference node discards the whole descriptor.
	if proofFor.Shard.PrefixBits < 0 || proofFor.Shard.PrefixBits > 60 {
		return masterTopBlockDescrEnvelope{}, fmt.Errorf(
			"TopBlockDescr shard prefix length is %d, want at most 60", proofFor.Shard.PrefixBits,
		)
	}
	if proofFor.Shard.WorkchainID == invalidWorkchainID {
		return masterTopBlockDescrEnvelope{}, fmt.Errorf("TopBlockDescr names the invalid workchain")
	}
	// 2*pow2-1 masks the bits below the prefix; at length 0 it wraps to all ones,
	// which is the same value the reference computes.
	pow2 := uint64(1) << (63 - uint(proofFor.Shard.PrefixBits))
	if proofFor.Shard.ShardPrefix&(2*pow2-1) != 0 {
		return masterTopBlockDescrEnvelope{}, fmt.Errorf("TopBlockDescr shard prefix is not canonical")
	}
	workchain, shardID := tlb.ConvertShardIdentToShard(proofFor.Shard)
	envelope := masterTopBlockDescrEnvelope{
		target: ton.BlockIDExt{
			Workchain: workchain,
			Shard:     int64(shardID),
			SeqNo:     proofFor.SeqNo,
			RootHash:  proofFor.RootHash,
			FileHash:  proofFor.FileHash,
		},
	}
	hasSignatures, err := loader.LoadBoolBit()
	if err != nil {
		return masterTopBlockDescrEnvelope{}, fmt.Errorf("load TopBlockDescr signature flag: %w", err)
	}
	if hasSignatures {
		if envelope.signatures, err = loader.LoadRefCell(); err != nil {
			return masterTopBlockDescrEnvelope{}, fmt.Errorf("load TopBlockDescr signatures: %w", err)
		}
	}
	length, err := loader.LoadUInt(8)
	if err != nil {
		return masterTopBlockDescrEnvelope{}, fmt.Errorf("load TopBlockDescr chain length: %w", err)
	}
	if length == 0 || length > uint64(maxShardTopChainLength) {
		return masterTopBlockDescrEnvelope{}, fmt.Errorf(
			"TopBlockDescr chain length %d is outside 1..%d", length, maxShardTopChainLength,
		)
	}
	envelope.length = int(length)
	envelope.chain = loader

	return envelope, nil
}

// masterTopChainLink is one virtualized TopBlockDescr proof link: the proven
// block root together with the header and value flow it exposes.
type masterTopChainLink struct {
	root      *cell.Cell
	header    tlb.BlockHeader
	valueFlow tlb.ValueFlow
}

// validateMasterTopBlockDescrBinding validates the deterministic envelope
// around a TopBlockDescr already signature-checked by the acquisition
// pipeline. In particular, the cell placed in collated data cannot name a
// different tip or carry fewer proof links than the transition consumed by
// ShardRegistry. It returns that transition's newest proof prefix; a valid
// TopBlockDescr may retain older links that the current masterchain state no
// longer needs.
func validateMasterTopBlockDescrBinding(
	root *cell.Cell,
	block ton.BlockIDExt,
	chainLength uint32,
) ([]*cell.Cell, error) {
	envelope, err := parseMasterTopBlockDescrEnvelope(root)
	if err != nil {
		return nil, err
	}
	if !sameShardBlock(envelope.target, block) {
		return nil, fmt.Errorf("TopBlockDescr target differs from the accepted shard tip")
	}
	// Collation does not authenticate the descriptor here: the acquisition
	// pipeline already did, so the signature cell is deliberately discarded.
	if chainLength == 0 || uint64(chainLength) > uint64(envelope.length) {
		return nil, fmt.Errorf("TopBlockDescr chain length is %d, shorter than required %d",
			envelope.length, chainLength)
	}
	proofs, err := validateMasterTopProofChain(envelope.chain, envelope.length)
	if err != nil {
		return nil, err
	}

	// C++ prevalidate returns the number of links newer than the masterchain's
	// registered shard top, which may be smaller than the descriptor's retained
	// chain. get_top_descr(chain_len) and get_creator_list(chain_len) consume
	// only that newest prefix.
	return proofs[:chainLength], nil
}

func validateMasterTopProofChain(chain *cell.Slice, length int) ([]*cell.Cell, error) {
	proofs := make([]*cell.Cell, 0, length)
	for i := 0; i < length; i++ {
		wantRefs := 1
		if i+1 < length {
			wantRefs = 2
		}
		if chain.BitsLeft() != 0 || chain.RefsNum() != wantRefs {
			return nil, fmt.Errorf("TopBlockDescr proof link %d has %d bits and %d refs, want 0 and %d",
				i, chain.BitsLeft(), chain.RefsNum(), wantRefs)
		}
		proof, err := chain.LoadRefCell()
		if err != nil {
			return nil, fmt.Errorf("load TopBlockDescr proof link %d: %w", i, err)
		}
		if proof.GetType() != cell.MerkleProofCellType {
			return nil, fmt.Errorf("TopBlockDescr proof link %d is not a Merkle proof", i)
		}
		proofs = append(proofs, proof)
		if i+1 < length {
			chain, err = chain.LoadRef()
			if err != nil {
				return nil, fmt.Errorf("load TopBlockDescr previous link %d: %w", i, err)
			}
		}
	}
	return proofs, nil
}

// parseMasterTopChainLink virtualizes one proof link and reads the two block
// parts the shard descriptor is projected from. Building the descriptor reads
// the same two records, so a proof that pruned either of them is unusable for
// registration.
func parseMasterTopChainLink(proof *cell.Cell) (masterTopChainLink, error) {
	root, err := unwrapCollatedProof(proof)
	if err != nil {
		return masterTopChainLink{}, fmt.Errorf("virtualize proof: %w", err)
	}
	loader, err := root.BeginParse()
	if err != nil {
		return masterTopChainLink{}, err
	}
	tag, err := loader.LoadUInt(32)
	if err != nil || tag != blockTag {
		return masterTopChainLink{}, fmt.Errorf("proven cell is not a block")
	}
	if _, err = loader.LoadInt(32); err != nil {
		return masterTopChainLink{}, fmt.Errorf("load block global id: %w", err)
	}
	if loader.BitsLeft() != 0 || loader.RefsNum() != 4 {
		return masterTopChainLink{}, fmt.Errorf("proven block has an invalid shape")
	}
	info, err := loader.LoadRefCell()
	if err != nil {
		return masterTopChainLink{}, fmt.Errorf("load block info: %w", err)
	}
	valueFlow, err := loader.LoadRefCell()
	if err != nil {
		return masterTopChainLink{}, fmt.Errorf("load block value flow: %w", err)
	}

	link := masterTopChainLink{root: root}
	if err = parseExact(&link.header, info); err != nil {
		return masterTopChainLink{}, fmt.Errorf("decode proven block info: %w", err)
	}
	if err = parseExact(&link.valueFlow, valueFlow); err != nil {
		return masterTopChainLink{}, fmt.Errorf("decode proven value flow: %w", err)
	}
	return link, nil
}

// verifyMasterTopBlockDescrFields binds the supplied shard descriptor to the
// blocks the TopBlockDescr actually proves. Validation rebuilds the same
// projection from collated data and rejects the masterchain block on any
// difference (the descriptor built from the block plus the chain fee sum done by
// ShardTopBlockDescrQ::get_prev_descr), so every field it compares has to be
// derived here rather than trusted from the acquisition pipeline.
func verifyMasterTopBlockDescrFields(
	proofs []*cell.Cell,
	block ton.BlockIDExt,
	fields *shardDescriptorFields,
) error {
	if len(proofs) == 0 {
		return fmt.Errorf("TopBlockDescr proves no block")
	}
	links := make([]masterTopChainLink, len(proofs))
	for i := range proofs {
		link, err := parseMasterTopChainLink(proofs[i])
		if err != nil {
			return fmt.Errorf("proof link %d: %w", i, err)
		}
		links[i] = link
	}

	top := &links[0]
	header := &top.header
	if !bytes.Equal(top.root.Hash(0), block.RootHash) {
		return fmt.Errorf("proven block root differs from the accepted shard tip")
	}
	if header.Shard.WorkchainID != block.Workchain || int64(header.Shard.GetShardID()) != block.Shard {
		return fmt.Errorf("proven block shard differs from the accepted shard tip")
	}

	switch {
	case fields.seqno != header.SeqNo:
		return fmt.Errorf("descriptor seqno is %d, proven block has %d", fields.seqno, header.SeqNo)
	case fields.startLT != header.StartLt || fields.endLT != header.EndLt:
		return fmt.Errorf("descriptor logical-time interval differs from the proven block")
	case fields.genUtime != header.GenUtime:
		return fmt.Errorf("descriptor generation time is %d, proven block has %d", fields.genUtime, header.GenUtime)
	case fields.minRefMCSeqno != header.MinRefMcSeqno:
		return fmt.Errorf("descriptor min_ref_mc_seqno is %d, proven block has %d",
			fields.minRefMCSeqno, header.MinRefMcSeqno)
	case fields.nextCatchainSeqno != header.GenCatchainSeqno:
		return fmt.Errorf("descriptor next catchain seqno is %d, proven block has %d",
			fields.nextCatchainSeqno, header.GenCatchainSeqno)
	case fields.nextValidatorShard != block.Shard:
		return fmt.Errorf("descriptor next validator shard is %016x, want %016x",
			uint64(fields.nextValidatorShard), uint64(block.Shard))
	case fields.beforeSplit != header.BeforeSplit:
		return fmt.Errorf("descriptor before_split differs from the proven block")
	case fields.wantSplit != header.WantSplit || fields.wantMerge != header.WantMerge:
		return fmt.Errorf("descriptor split/merge intent differs from the proven block")
	}

	fees := tlb.CurrencyCollection{Coins: tlb.FromNanoTONU(0)}
	created := tlb.CurrencyCollection{Coins: tlb.FromNanoTONU(0)}
	var err error
	for i := range links {
		if fees, err = fees.Add(links[i].valueFlow.FeesCollected); err != nil {
			return fmt.Errorf("sum chain fees: %w", err)
		}
		if created, err = created.Add(links[i].valueFlow.Created); err != nil {
			return fmt.Errorf("sum chain created funds: %w", err)
		}
	}
	if !fields.fees.Equals(fees) {
		return fmt.Errorf("descriptor collected fees differ from the proven chain")
	}
	if !fields.created.Equals(created) {
		return fmt.Errorf("descriptor created funds differ from the proven chain")
	}

	return nil
}
