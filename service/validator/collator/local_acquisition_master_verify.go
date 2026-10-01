package collator

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"
)

func (a *LocalAcquisition) masterValidationTops(
	ctx context.Context,
	master *localMasterView,
	candidate *verifiedCandidate,
) (*ShardRegistry, []ShardTop, error) {
	if candidate.block.Extra.Custom == nil {
		return nil, nil, fmt.Errorf("%w: masterchain block extra is absent", ErrInvalidInput)
	}
	registry, err := ParseShardRegistry(candidate.block.Extra.Custom.ShardHashes)
	if err != nil {
		return nil, nil, err
	}
	descriptors, err := loadMasterTopBlockDescrSet(candidate.collated.roots)
	if err != nil {
		return nil, nil, err
	}

	leaves := sortedShardRegistryLeaves(registry.leaves)
	tops := make([]ShardTop, 0, len(leaves))
	for i := range leaves {
		if err = ctx.Err(); err != nil {
			return nil, nil, err
		}
		block := leaves[i].top.Block
		anchors := shardTopIntersectingLeaves(master.registry, block)
		unchanged := false
		for j := range anchors {
			if sameShardBlock(anchors[j].top.Block, block) {
				unchanged = true
				break
			}
		}
		if unchanged {
			continue
		}
		// A workchain zerostate has no preceding shard block and therefore no
		// TopBlockDescr. The deterministic master transition verifies every
		// zerostate field against the workchain configuration later.
		if len(anchors) == 0 && block.SeqNo == 0 {
			continue
		}

		descriptor, loadErr := loadValidationTopBlockDescr(descriptors, block)
		if loadErr != nil {
			return nil, nil, loadErr
		}
		description, parseErr := parseValidationTopBlockDescr(ctx, master, descriptor, block)
		if parseErr != nil {
			return nil, nil, fmt.Errorf(
				"%w: validate TopBlockDescr for %d:%016x:%d: %v",
				ErrInvalidInput,
				block.Workchain,
				uint64(block.Shard),
				block.SeqNo,
				parseErr,
			)
		}
		predecessors, chainLength, disposition := shardTopRegistryAnchor(master.registry, description)
		if disposition != shardTopDispositionCandidate {
			return nil, nil, fmt.Errorf(
				"%w: TopBlockDescr for %d:%016x:%d does not extend the predecessor shard registry",
				ErrInvalidInput,
				block.Workchain,
				uint64(block.Shard),
				block.SeqNo,
			)
		}
		top, buildErr := buildSelectedShardTop(descriptor, description, predecessors, chainLength)
		if buildErr != nil {
			return nil, nil, fmt.Errorf("%w: project TopBlockDescr: %v", ErrInvalidInput, buildErr)
		}
		tops = append(tops, top)
	}

	return registry, tops, nil
}

func loadMasterTopBlockDescrSet(roots []*cell.Cell) (*cell.Dictionary, error) {
	var result *cell.Dictionary
	for _, root := range roots {
		if !isTopBlockDescrSet(root) {
			continue
		}
		if result != nil {
			return nil, fmt.Errorf("%w: candidate has duplicate shard top descriptor sets", ErrInvalidInput)
		}
		var loader cell.Slice
		err := root.BeginParseInto(&loader)
		if err != nil {
			return nil, fmt.Errorf("%w: parse shard top descriptor set: %v", ErrInvalidInput, err)
		}
		if _, err = loader.LoadUInt(32); err != nil {
			return nil, fmt.Errorf("%w: load shard top descriptor set tag: %v", ErrInvalidInput, err)
		}
		result, err = loader.LoadDict(96)
		if err != nil || loader.BitsLeft() != 0 || loader.RefsNum() != 0 || !result.ValidateAll() {
			return nil, fmt.Errorf("%w: malformed shard top descriptor set", ErrInvalidInput)
		}
		if _, err = result.ForEachRefValue(nil); err != nil {
			return nil, fmt.Errorf("%w: malformed shard top descriptor value: %v", ErrInvalidInput, err)
		}
	}

	return result, nil
}

func loadValidationTopBlockDescr(descriptors *cell.Dictionary, block ton.BlockIDExt) (*cell.Cell, error) {
	if descriptors == nil {
		return nil, fmt.Errorf("%w: candidate has no shard top descriptor set", ErrInvalidInput)
	}
	var key [12]byte
	binary.BigEndian.PutUint32(key[:4], uint32(block.Workchain))
	binary.BigEndian.PutUint64(key[4:], uint64(block.Shard))
	var value cell.Slice
	if err := descriptors.LoadValueByBytesKeyInto(key[:], &value); err != nil {
		return nil, fmt.Errorf(
			"%w: collated TopBlockDescr for %d:%016x is absent",
			ErrInvalidInput,
			block.Workchain,
			uint64(block.Shard),
		)
	}
	descriptor, err := value.LoadRefCell()
	if err != nil || value.BitsLeft() != 0 || value.RefsNum() != 0 {
		return nil, fmt.Errorf("%w: collated TopBlockDescr value is malformed", ErrInvalidInput)
	}

	return descriptor, nil
}

func parseValidationTopBlockDescr(
	ctx context.Context,
	master *localMasterView,
	root *cell.Cell,
	block ton.BlockIDExt,
) (*p2p.ShardBlockDescription, error) {
	envelope, err := parseMasterTopBlockDescrEnvelope(root)
	if err != nil {
		return nil, err
	}
	target := envelope.target
	if !sameShardBlock(target, block) {
		return nil, fmt.Errorf("TopBlockDescr target differs from the candidate shard registry")
	}
	// Unlike collation, validation authenticates the descriptor itself, so the
	// signature cell is mandatory here.
	if envelope.signatures == nil {
		return nil, fmt.Errorf("TopBlockDescr has no validator signatures")
	}
	signatures, err := blockproof.ParseBlockSignatureSetCell(envelope.signatures)
	if err != nil {
		return nil, fmt.Errorf("parse TopBlockDescr signatures: %w", err)
	}
	signatureSet := signatures.ValidatorSignatures
	if signatureSet == nil || signatureSet.SignatureCount() == 0 {
		return nil, fmt.Errorf("TopBlockDescr has empty validator signatures")
	}
	proofs, err := validateMasterTopProofChain(&envelope.chain, envelope.length)
	if err != nil {
		return nil, err
	}
	links, err := validateValidationTopProofChain(master, target, signatureSet, proofs)
	if err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if err = checkValidationTopSignatures(master.context.Config, target, signatures); err != nil {
		return nil, err
	}

	return &p2p.ShardBlockDescription{
		Block:            target,
		CatchainSeqno:    signatureSet.CatchainSeqno(),
		ValidatorSetHash: signatureSet.ValidatorSetHash(),
		Chain:            links,
	}, nil
}

func checkValidationTopSignatures(
	config *Config,
	block ton.BlockIDExt,
	signatures *blockproof.BlockSignatureSet,
) error {
	signatureSet := signatures.ValidatorSignatures
	prepared, err := prepareValidationTopValidatorSet(
		config,
		block,
		signatureSet.CatchainSeqno(),
		signatureSet.ValidatorSetHash(),
	)
	if err != nil {
		return err
	}
	if err = blockproof.CheckPreparedBlockSignatures(block, signatures, prepared); err != nil {
		return fmt.Errorf("check TopBlockDescr signatures: %w", err)
	}

	return nil
}

func prepareValidationTopValidatorSet(
	config *Config,
	block ton.BlockIDExt,
	catchainSeqno uint32,
	validatorSetHash uint32,
) (*blockproof.PreparedValidatorSet, error) {
	// Only the shard's current set may sign a descriptor that a masterchain
	// block claims as its new shard configuration: C++ withholds allow_next_vset
	// from the validation-side prevalidate (validate-query.cpp:2044-2045),
	// exactly as its collator does when importing shard tops
	// (collator.cpp:1867). The next set is honoured only while admitting a
	// broadcast descriptor (top-shard-descr.cpp:257, :574). gton keeps the same
	// split: broadcastValidatorSetFromConfig is the lenient admission side, and
	// classifyShardTopValidatorSet already defers next-set descriptors instead
	// of collating them.
	raw := &tlb.BlockchainConfig{Root: config.execution.Root()}
	currentValidators, err := blockproof.CurrentValidatorsForBlock(raw, &block, catchainSeqno)
	if err != nil {
		return nil, fmt.Errorf("load TopBlockDescr current validator set: %w", err)
	}
	current, err := blockproof.PrepareValidatorSet(catchainSeqno, currentValidators)
	if err != nil {
		return nil, fmt.Errorf("prepare TopBlockDescr current validator set: %w", err)
	}
	if current.Hash() != validatorSetHash {
		return nil, fmt.Errorf(
			"TopBlockDescr validator set %08x is not the shard's current set %08x",
			validatorSetHash,
			current.Hash(),
		)
	}

	return current, nil
}

func validateValidationTopProofChain(
	master *localMasterView,
	block ton.BlockIDExt,
	signatures *blockproof.ValidatorSignatureSet,
	proofs []*cell.Cell,
) ([]p2p.ShardDescriptionLink, error) {
	current := block
	links := make([]p2p.ShardDescriptionLink, 0, len(proofs))
	for i, proof := range proofs {
		parsed, err := ton.CheckBlockProof(proof, current.RootHash)
		if err != nil {
			return nil, fmt.Errorf("check TopBlockDescr proof for %s: %w", storage.FormatBlockRef(current), err)
		}
		if err = storage.VerifyBlockIdentity(current, parsed); err != nil {
			return nil, err
		}
		if parsed.BlockInfo.Version != 0 {
			return nil, fmt.Errorf("TopBlockDescr proof %s has unsupported block version %d", storage.FormatBlockRef(current), parsed.BlockInfo.Version)
		}
		if !parsed.BlockInfo.NotMaster || parsed.BlockInfo.MasterRef == nil {
			return nil, fmt.Errorf("TopBlockDescr proof %s is not a shard block", storage.FormatBlockRef(current))
		}
		if parsed.BlockInfo.GenCatchainSeqno != signatures.CatchainSeqno() {
			return nil, fmt.Errorf("TopBlockDescr proof %s catchain seqno differs from signatures", storage.FormatBlockRef(current))
		}
		if parsed.BlockInfo.GenValidatorListHashShort != signatures.ValidatorSetHash() {
			return nil, fmt.Errorf("TopBlockDescr proof %s validator set hash differs from signatures", storage.FormatBlockRef(current))
		}
		meta, err := storage.BuildBlockMetaFromParsedBlock(current, parsed)
		if err != nil {
			return nil, fmt.Errorf("build TopBlockDescr proof metadata for %s: %w", storage.FormatBlockRef(current), err)
		}
		if len(meta.PrevRefs) == 0 || len(meta.PrevRefs) > 2 {
			return nil, fmt.Errorf("TopBlockDescr proof %s has %d previous refs", storage.FormatBlockRef(current), len(meta.PrevRefs))
		}
		if parsed.ValueFlow == nil {
			return nil, fmt.Errorf("TopBlockDescr proof %s has no value flow", storage.FormatBlockRef(current))
		}
		var flow tlb.ValueFlow
		if err = parseExact(&flow, parsed.ValueFlow); err != nil {
			return nil, fmt.Errorf("decode TopBlockDescr proof %s value flow: %w", storage.FormatBlockRef(current), err)
		}
		if parsed.Extra == nil || len(parsed.Extra.CreatedBy) != 32 {
			return nil, fmt.Errorf("TopBlockDescr proof %s has no creator id", storage.FormatBlockRef(current))
		}

		masterchainRef := validationMasterchainBlockID(parsed.BlockInfo.MasterRef)
		resolved, err := master.blockAt(masterchainRef.SeqNo)
		if err != nil || !resolved.Equals(&masterchainRef) {
			return nil, fmt.Errorf("TopBlockDescr proof %s refers to a non-ancestor masterchain block", storage.FormatBlockRef(current))
		}
		if len(links) > 0 {
			newer := &links[len(links)-1]
			if newer.MasterchainRef.SeqNo < masterchainRef.SeqNo ||
				newer.MasterchainRef.SeqNo == masterchainRef.SeqNo && !newer.MasterchainRef.Equals(&masterchainRef) {
				return nil, fmt.Errorf("TopBlockDescr proof %s has non-monotonic masterchain references", storage.FormatBlockRef(current))
			}
			if parsed.BlockInfo.GenUtime > newer.GenUtime {
				return nil, fmt.Errorf("TopBlockDescr proof %s has non-monotonic generation time", storage.FormatBlockRef(current))
			}
			if parsed.BlockInfo.VertSeqNo != links[0].VertSeqno {
				return nil, fmt.Errorf("TopBlockDescr proof %s changes vertical seqno", storage.FormatBlockRef(current))
			}
			if parsed.BlockInfo.BeforeSplit {
				return nil, fmt.Errorf("intermediate TopBlockDescr proof %s is before a split", storage.FormatBlockRef(current))
			}
		}

		var createdBy [32]byte
		copy(createdBy[:], parsed.Extra.CreatedBy)
		links = append(links, p2p.ShardDescriptionLink{
			Block:          current,
			PrevRefs:       meta.PrevRefs,
			MasterchainRef: &masterchainRef,
			TopBlockProof:  proof,
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
			FeesCollected:  flow.FeesCollected,
			FundsCreated:   flow.Created,
		})

		maxPreviousSeqno := meta.PrevRefs[0].SeqNo
		for _, previous := range meta.PrevRefs[1:] {
			maxPreviousSeqno = max(maxPreviousSeqno, previous.SeqNo)
		}
		if maxPreviousSeqno+1 != current.SeqNo {
			return nil, fmt.Errorf("TopBlockDescr proof %s is not the next shard block", storage.FormatBlockRef(current))
		}
		if i+1 == len(proofs) {
			break
		}
		if parsed.BlockInfo.AfterSplit || parsed.BlockInfo.AfterMerge || len(meta.PrevRefs) != 1 {
			return nil, fmt.Errorf("intermediate TopBlockDescr proof %s crosses a topology boundary", storage.FormatBlockRef(current))
		}
		previous := meta.PrevRefs[0]
		if previous.Workchain != current.Workchain || previous.Shard != current.Shard || previous.SeqNo+1 != current.SeqNo {
			return nil, fmt.Errorf("intermediate TopBlockDescr proof %s does not link to its direct predecessor", storage.FormatBlockRef(current))
		}
		current = previous
	}
	if len(links) == 0 || links[0].VertSeqno != master.context.VertSeqno {
		return nil, fmt.Errorf("TopBlockDescr vertical seqno differs from the masterchain context")
	}

	return links, nil
}

func validationMasterchainBlockID(reference *tlb.ExtBlkRef) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: masterchainWorkchainID,
		Shard:     math.MinInt64,
		SeqNo:     reference.SeqNo,
		RootHash:  bytes.Clone(reference.RootHash),
		FileHash:  bytes.Clone(reference.FileHash),
	}
}
