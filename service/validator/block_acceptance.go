package validator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
)

const acceptedBlockKind = "tonNode.blockFinalityBroadcast"

// BlockAccepter turns a Simplex certificate into the proof representation
// consumed by the node's ordinary block pipeline.
type BlockAccepter struct {
	shard            groups.ShardID
	sessionID        [32]byte
	catchainSeqno    uint32
	validatorSetHash uint32
	validatorIDs     [][32]byte
	validators       *blockproof.PreparedValidatorSet
	localValidator   *ValidatorIdentity
	node             LocalSessionNode
	publisher        BlockPublisher
	shardTops        *collator.ShardTopInbox
	log              zerolog.Logger
}

type BlockAccepterOptions struct {
	Config    SessionConfig
	Node      LocalSessionNode
	Publisher BlockPublisher
	ShardTops *collator.ShardTopInbox
	Logger    zerolog.Logger
}

type preparedBlockAcceptance struct {
	block          p2p.DownloadedBlock
	parsed         *tlb.Block
	proofRoot      *cell.Cell
	signatures     *blockproof.ValidatorSignatureSet
	signaturesCell *cell.Cell
	final          bool
}

type acceptedValidatorSetLoader struct {
	name string
	load func() (*tlb.ValidatorSetAny, error)
}

// NewBlockAccepter prepares the fixed validator roster used to authenticate
// every acceptance certificate in one session.
func NewBlockAccepter(options BlockAccepterOptions) (*BlockAccepter, error) {
	if options.Node == nil {
		return nil, errors.New("validator block acceptance: node is required")
	}
	config := options.Config
	// The address mapping is shared with groups.ValidatorSetHash: the hash
	// comparison below is only meaningful while both sides build the roster the
	// same way. The short ids are derived rather than taken from
	// Validator.PublicKeyHash because they become ton.Signature.NodeIDShort, and
	// hand-built rosters may leave that field zero.
	validatorAddrs := groups.ValidatorAddrs(config.Validators)
	validatorIDs := make([][32]byte, len(config.Validators))
	for i := range config.Validators {
		validator := &config.Validators[i]

		validatorID, err := groups.PublicKeyHash(validator.PublicKey)
		if err != nil {
			return nil, fmt.Errorf("validator block acceptance: hash validator %d public key: %w", i, err)
		}
		validatorIDs[i] = validatorID
	}

	validators, err := blockproof.PrepareValidatorSet(config.CatchainSeqno, validatorAddrs)
	if err != nil {
		return nil, fmt.Errorf("validator block acceptance: prepare validator set: %w", err)
	}
	if validators.Hash() != config.ValidatorSetHash {
		return nil, fmt.Errorf(
			"validator block acceptance: validator set hash mismatch: configured=%08x calculated=%08x",
			config.ValidatorSetHash,
			validators.Hash(),
		)
	}

	return &BlockAccepter{
		shard:            config.Shard,
		sessionID:        config.SessionID,
		catchainSeqno:    config.CatchainSeqno,
		validatorSetHash: config.ValidatorSetHash,
		validatorIDs:     validatorIDs,
		validators:       validators,
		localValidator:   config.Identity.Validator,
		node:             options.Node,
		publisher:        options.Publisher,
		shardTops:        options.ShardTops,
		log:              options.Logger,
	}, nil
}

// Accept validates the acceptance evidence, constructs the interoperable
// proof form, durably retains shard proof links, and hands the block to the
// best-effort local ingress. It does not wait for node application or network
// delivery. resolveView is consulted only for a finalized shard block and only
// after local submission and publication, so a chain view that is not ready yet
// can never suppress the single network publication of that block.
func (a *BlockAccepter) Accept(
	ctx context.Context,
	acceptance BlockAcceptance,
	resolveView BlockAcceptanceViewResolver,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	prepared, err := a.prepare(acceptance)
	if err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}

	a.node.SubmitBlockLocally(prepared.block)
	if !acceptance.Retry && !acceptance.Replay && a.publisher != nil {
		a.publisher.PublishAcceptedBlock(AcceptedBlockPublication{
			SessionID:  a.sessionID,
			Block:      prepared.block,
			Signatures: prepared.signatures,
			Public: a.localValidator != nil &&
				acceptance.Candidate.Candidate.Leader == a.localValidator.Index,
		})
	}
	if a.shard.IsMasterchain() || !prepared.final {
		return nil
	}

	return a.buildShardTopDescription(ctx, resolveView, prepared)
}

func (a *BlockAccepter) prepare(acceptance BlockAcceptance) (*preparedBlockAcceptance, error) {
	if err := a.validate(acceptance); err != nil {
		return nil, err
	}

	blockID := acceptance.Candidate.Candidate.Block
	blockRoot, parsed, err := parseAcceptedBlock(blockID, acceptance.Candidate.BlockBOC)
	if err != nil {
		return nil, err
	}
	if err = validateAcceptedBlockHeader(blockID, parsed); err != nil {
		return nil, err
	}

	signatures, err := a.signatureSet(acceptance)
	if err != nil {
		return nil, err
	}
	if _, err = blockproof.PrepareValidatorSignatureSet(blockID, parsed, signatures); err != nil {
		return nil, fmt.Errorf("validator block acceptance: prepare validator signatures: %w", err)
	}

	meta, err := storage.BuildBlockMetaFromParsedBlock(blockID, parsed)
	if err != nil {
		return nil, fmt.Errorf("validator block acceptance: build block metadata: %w", err)
	}
	if err = blockproof.CheckPreparedSignatures(blockID, signatures, a.validators); err != nil {
		return nil, fmt.Errorf("validator block acceptance: %w", err)
	}

	proofRoot, err := blockproof.BroadcastProofRoot(blockID, blockRoot)
	if err != nil {
		return nil, fmt.Errorf("validator block acceptance: build broadcast proof: %w", err)
	}

	proof, proofBOC, isLink, verifiedKey, err := a.buildProof(blockID, proofRoot, signatures)
	if err != nil {
		return nil, err
	}

	prepared := &preparedBlockAcceptance{
		block: p2p.DownloadedBlock{
			ID:                    blockID,
			Kind:                  acceptedBlockKind,
			Block:                 blockRoot,
			Proof:                 proof,
			BlockBOC:              acceptance.Candidate.BlockBOC,
			ProofBOC:              proofBOC,
			Meta:                  meta,
			StateUpdate:           parsed.StateUpdate,
			IsLink:                isLink,
			VerifiedRootHash:      true,
			SignaturesVerifiedKey: verifiedKey,
		},
		parsed:     parsed,
		proofRoot:  proofRoot,
		signatures: signatures,
		final:      acceptance.Certificate.Vote.Kind == simplex.VoteFinalize,
	}
	if !a.shard.IsMasterchain() && prepared.final {
		prepared.signaturesCell, err = signatures.FinalitySignaturesCell(a.validators)
		if err != nil {
			return nil, fmt.Errorf("validator block acceptance: serialize shard final signatures: %w", err)
		}
	}

	return prepared, nil
}

func (a *BlockAccepter) validate(acceptance BlockAcceptance) error {
	if acceptance.Candidate == nil {
		return errors.New("validator block acceptance: candidate is absent")
	}
	if acceptance.Certificate == nil {
		return errors.New("validator block acceptance: certificate is absent")
	}
	if acceptance.CertifiedCandidate == nil {
		return errors.New("validator block acceptance: certified candidate is absent")
	}

	candidate := &acceptance.Candidate.Candidate
	certified := &acceptance.CertifiedCandidate.Candidate
	certificate := acceptance.Certificate
	if candidate.Empty {
		return errors.New("validator block acceptance: accepted candidate is empty")
	}
	if err := candidate.ValidateShape(); err != nil {
		return fmt.Errorf("validator block acceptance: invalid candidate: %w", err)
	}
	if err := certified.ValidateShape(); err != nil {
		return fmt.Errorf("validator block acceptance: invalid certified candidate: %w", err)
	}
	if candidate.Block.Workchain != a.shard.Workchain || candidate.Block.Shard != a.shard.Shard {
		return fmt.Errorf(
			"validator block acceptance: block shard mismatch: block=(%d,%016x) session=(%d,%016x)",
			candidate.Block.Workchain,
			uint64(candidate.Block.Shard),
			a.shard.Workchain,
			uint64(a.shard.Shard),
		)
	}
	if candidate.ID != candidate.ComputeID(candidate.ID.Slot) {
		return errors.New("validator block acceptance: candidate id does not match candidate data")
	}
	if certified.ID != certified.ComputeID(certified.ID.Slot) {
		return errors.New("validator block acceptance: certified candidate id does not match candidate data")
	}
	if certificate.Vote.ID != certified.ID {
		return errors.New("validator block acceptance: certificate vote does not match certified candidate")
	}
	if !candidate.Block.Equals(&certified.Block) {
		return errors.New("validator block acceptance: certified candidate references another block")
	}

	switch certificate.Vote.Kind {
	case simplex.VoteNotarize:
		if a.shard.IsMasterchain() {
			return errors.New("validator block acceptance: masterchain block requires final certificate")
		}
		if certified.Empty || certified.ID != candidate.ID {
			return errors.New("validator block acceptance: notarization does not certify accepted candidate")
		}
	case simplex.VoteFinalize:
	default:
		return fmt.Errorf("validator block acceptance: unsupported certificate vote kind %d", certificate.Vote.Kind)
	}

	return nil
}

func (a *BlockAccepter) signatureSet(acceptance BlockAcceptance) (*blockproof.ValidatorSignatureSet, error) {
	certificate := acceptance.Certificate
	signatures := make([]ton.Signature, len(certificate.Signatures))
	for i, signature := range certificate.Signatures {
		if int(signature.ValidatorIndex) >= len(a.validatorIDs) {
			return nil, fmt.Errorf(
				"validator block acceptance: signature %d has validator index %d outside set of %d",
				i,
				signature.ValidatorIndex,
				len(a.validatorIDs),
			)
		}

		signatures[i] = ton.Signature{
			NodeIDShort: a.validatorIDs[signature.ValidatorIndex][:],
			Signature:   signature.Signature,
		}
	}

	return blockproof.NewSimplexValidatorSignatureSet(
		a.catchainSeqno,
		a.validatorSetHash,
		signatures,
		certificate.Vote.Kind == simplex.VoteFinalize,
		a.sessionID[:],
		int32(certificate.Vote.ID.Slot),
		acceptance.CertifiedCandidate.Candidate.HashDataBytes(),
	), nil
}

func (a *BlockAccepter) buildProof(
	blockID ton.BlockIDExt,
	proofRoot *cell.Cell,
	signatures *blockproof.ValidatorSignatureSet,
) (*cell.Cell, []byte, bool, []byte, error) {
	if !a.shard.IsMasterchain() {
		proof, proofBOC, err := blockproof.LinkFromRoot(blockID, proofRoot)
		if err != nil {
			return nil, nil, false, nil, fmt.Errorf("validator block acceptance: build shard proof link: %w", err)
		}

		return proof, proofBOC, true, signatures.ContentKey(blockID), nil
	}

	signaturesCell, err := signatures.FinalitySignaturesCell(a.validators)
	if err != nil {
		return nil, nil, false, nil, fmt.Errorf("validator block acceptance: serialize final signatures: %w", err)
	}
	proof, proofBOC, err := blockproof.ProofFromRoot(blockID, proofRoot, signaturesCell)
	if err != nil {
		return nil, nil, false, nil, fmt.Errorf("validator block acceptance: build masterchain proof: %w", err)
	}
	blockSignatures, err := blockproof.ParseBlockSignatureSetCell(signaturesCell)
	if err != nil {
		return nil, nil, false, nil, fmt.Errorf("validator block acceptance: parse serialized final signatures: %w", err)
	}

	return proof, proofBOC, false, blockSignatures.ContentKey(blockID), nil
}

func parseAcceptedBlock(
	id ton.BlockIDExt,
	blockBOC []byte,
) (*cell.Cell, *tlb.Block, error) {
	if len(blockBOC) == 0 {
		return nil, nil, errors.New("validator block acceptance: block boc is empty")
	}

	root, err := cell.FromBOC(blockBOC)
	if err != nil {
		return nil, nil, fmt.Errorf("validator block acceptance: parse block boc: %w", err)
	}
	rootHash := root.HashKey()
	if !bytes.Equal(rootHash[:], id.RootHash) {
		return nil, nil, fmt.Errorf("validator block acceptance: root hash mismatch for %s", storage.FormatBlockRef(id))
	}
	fileHash := sha256.Sum256(blockBOC)
	if !bytes.Equal(fileHash[:], id.FileHash) {
		return nil, nil, fmt.Errorf("validator block acceptance: file hash mismatch for %s", storage.FormatBlockRef(id))
	}

	rootView, err := root.BeginParse()
	if err != nil {
		return nil, nil, fmt.Errorf("validator block acceptance: begin block root: %w", err)
	}
	infoCell, err := rootView.PeekRefCellAt(0)
	if err != nil {
		return nil, nil, fmt.Errorf("validator block acceptance: load block info cell: %w", err)
	}
	extraCell, err := rootView.PeekRefCellAt(3)
	if err != nil {
		return nil, nil, fmt.Errorf("validator block acceptance: load block extra cell: %w", err)
	}

	loader, err := root.BeginParse()
	if err != nil {
		return nil, nil, fmt.Errorf("validator block acceptance: begin block: %w", err)
	}
	var parsed tlb.Block
	if err = tlb.LoadFromCell(&parsed, loader); err != nil {
		return nil, nil, fmt.Errorf("validator block acceptance: parse block: %w", err)
	}
	if err = requireAcceptedCellEmpty(loader, "block root"); err != nil {
		return nil, nil, err
	}
	if err = storage.VerifyBlockIdentity(id, &parsed); err != nil {
		return nil, nil, fmt.Errorf("validator block acceptance: verify block identity: %w", err)
	}
	if err = validateAcceptedInfoCell(infoCell, &parsed.BlockInfo); err != nil {
		return nil, nil, err
	}
	if err = validateAcceptedExtraCell(extraCell); err != nil {
		return nil, nil, err
	}
	if parsed.StateUpdate == nil {
		return nil, nil, fmt.Errorf("validator block acceptance: block %s has no state update", storage.FormatBlockRef(id))
	}
	stateUpdate, err := parsed.StateUpdate.BeginParse()
	if err != nil {
		return nil, nil, fmt.Errorf(
			"validator block acceptance: load state update for %s: %w",
			storage.FormatBlockRef(id),
			err,
		)
	}
	const merkleUpdateBits = 8 + 256 + 256 + 16 + 16
	if stateUpdate.BaseCell().GetType() != cell.MerkleUpdateCellType ||
		stateUpdate.BitsLeft() != merkleUpdateBits || stateUpdate.RefsNum() != 2 {
		return nil, nil, fmt.Errorf(
			"validator block acceptance: invalid Merkle update shape for %s",
			storage.FormatBlockRef(id),
		)
	}
	valueFlow, err := parsed.ValueFlow.BeginParse()
	if err != nil {
		return nil, nil, fmt.Errorf(
			"validator block acceptance: load value flow for %s: %w",
			storage.FormatBlockRef(id),
			err,
		)
	}
	var flow tlb.ValueFlow
	// AcceptBlock force-validates the TL-B shape here; the balance equation is
	// part of candidate validation and must not change the acceptance set.
	if err = flow.LoadFromCell(valueFlow); err != nil {
		return nil, nil, fmt.Errorf(
			"validator block acceptance: decode value flow for %s: %w",
			storage.FormatBlockRef(id),
			err,
		)
	}
	if err = requireAcceptedCellEmpty(valueFlow, "value flow"); err != nil {
		return nil, nil, err
	}

	return root, &parsed, nil
}

func validateAcceptedInfoCell(root *cell.Cell, header *tlb.BlockHeader) error {
	if root == nil || root.IsSpecial() {
		return errors.New("validator block acceptance: block info cell is nil or special")
	}
	loader, err := root.BeginParse()
	if err != nil {
		return fmt.Errorf("validator block acceptance: begin block info: %w", err)
	}
	var exact tlb.BlockHeader
	if err = tlb.LoadFromCell(&exact, loader); err != nil {
		return fmt.Errorf("validator block acceptance: parse block info: %w", err)
	}
	if err = requireAcceptedCellEmpty(loader, "block info"); err != nil {
		return err
	}

	refIndex := 0
	if header.NotMaster {
		masterRef, err := root.PeekRef(refIndex)
		if err != nil {
			return fmt.Errorf("validator block acceptance: load masterchain reference cell: %w", err)
		}
		if err = validateAcceptedExtBlockRefCell(masterRef, "masterchain reference"); err != nil {
			return err
		}
		refIndex++
	}
	previous, err := root.PeekRef(refIndex)
	if err != nil {
		return fmt.Errorf("validator block acceptance: load predecessor reference cell: %w", err)
	}
	if err = validateAcceptedPrevRefCell(previous, header.AfterMerge, "predecessor reference"); err != nil {
		return err
	}
	refIndex++
	if header.VertSeqnoIncr {
		previousVertical, err := root.PeekRef(refIndex)
		if err != nil {
			return fmt.Errorf("validator block acceptance: load vertical predecessor cell: %w", err)
		}
		if err = validateAcceptedPrevRefCell(previousVertical, false, "vertical predecessor"); err != nil {
			return err
		}
	}

	return nil
}

func validateAcceptedPrevRefCell(root *cell.Cell, afterMerge bool, label string) error {
	if root == nil || root.IsSpecial() {
		return fmt.Errorf("validator block acceptance: %s cell is nil or special", label)
	}
	if !afterMerge {
		return validateAcceptedExtBlockRefCell(root, label)
	}
	loader, err := root.BeginParse()
	if err != nil {
		return fmt.Errorf("validator block acceptance: begin %s: %w", label, err)
	}
	if loader.BitsLeft() != 0 || loader.RefsNum() != 2 {
		return fmt.Errorf("validator block acceptance: merged %s has trailing data", label)
	}
	for i := 0; i < 2; i++ {
		ref, err := root.PeekRef(i)
		if err != nil {
			return fmt.Errorf("validator block acceptance: load merged %s %d: %w", label, i, err)
		}
		if err = validateAcceptedExtBlockRefCell(ref, label); err != nil {
			return err
		}
	}

	return nil
}

func validateAcceptedExtBlockRefCell(root *cell.Cell, label string) error {
	if root == nil || root.IsSpecial() {
		return fmt.Errorf("validator block acceptance: %s cell is nil or special", label)
	}
	loader, err := root.BeginParse()
	if err != nil {
		return fmt.Errorf("validator block acceptance: begin %s: %w", label, err)
	}
	var reference tlb.ExtBlkRef
	if err = tlb.LoadFromCell(&reference, loader); err != nil {
		return fmt.Errorf("validator block acceptance: parse %s: %w", label, err)
	}

	return requireAcceptedCellEmpty(loader, label)
}

func validateAcceptedExtraCell(root *cell.Cell) error {
	if root == nil || root.IsSpecial() {
		return errors.New("validator block acceptance: block extra cell is nil or special")
	}
	loader, err := root.BeginParse()
	if err != nil {
		return fmt.Errorf("validator block acceptance: begin block extra: %w", err)
	}
	var extra tlb.BlockExtra
	if err = tlb.LoadFromCell(&extra, loader); err != nil {
		return fmt.Errorf("validator block acceptance: parse block extra: %w", err)
	}

	return requireAcceptedCellEmpty(loader, "block extra")
}

func requireAcceptedCellEmpty(loader *cell.Slice, name string) error {
	if loader.BitsLeft() != 0 || loader.RefsNum() != 0 {
		return fmt.Errorf(
			"validator block acceptance: %s has %d trailing bits and %d refs",
			name,
			loader.BitsLeft(),
			loader.RefsNum(),
		)
	}

	return nil
}

func validateAcceptedBlockHeader(id ton.BlockIDExt, block *tlb.Block) error {
	header := &block.BlockInfo
	if header.Version != 0 {
		return fmt.Errorf("validator block acceptance: unsupported block header version %d", header.Version)
	}
	if header.VertSeqnoIncr {
		return errors.New("validator block acceptance: ordinary block has vert_seqno_incr set")
	}
	if header.PrevVertRef != nil {
		return errors.New("validator block acceptance: ordinary block has a vertical predecessor")
	}
	if header.Flags&^uint32(1) != 0 {
		return fmt.Errorf("validator block acceptance: unsupported block header flags %02x", header.Flags)
	}
	if (header.Flags&1 != 0) != (header.GenSoftware != nil) {
		return errors.New("validator block acceptance: gen_software presence differs from header flags")
	}
	if err := validateAcceptedShardIdent(header.Shard); err != nil {
		return fmt.Errorf("validator block acceptance: invalid shard identifier: %w", err)
	}
	if header.PrevRef.Pruned {
		return errors.New("validator block acceptance: predecessor references are pruned")
	}

	isMasterchain := (groups.ShardID{Workchain: id.Workchain, Shard: id.Shard}).IsMasterchain()
	if header.NotMaster == isMasterchain {
		return fmt.Errorf(
			"validator block acceptance: invalid not_master flag for %s",
			storage.FormatBlockRef(id),
		)
	}
	if header.NotMaster != (header.MasterRef != nil) {
		return errors.New("validator block acceptance: masterchain reference presence differs from not_master flag")
	}

	predecessors := 1
	if header.PrevRef.Prev2 != nil {
		predecessors = 2
	}
	expectedPredecessors := 1
	if header.AfterMerge {
		expectedPredecessors = 2
	}
	if predecessors != expectedPredecessors {
		return fmt.Errorf(
			"validator block acceptance: after_merge announces %d predecessors, header contains %d",
			expectedPredecessors,
			predecessors,
		)
	}

	if isMasterchain {
		if header.AfterMerge || header.AfterSplit || header.BeforeSplit {
			return errors.New("validator block acceptance: masterchain block announces a split or merge")
		}
		if block.Extra.Custom == nil {
			return errors.New("validator block acceptance: masterchain block has no McBlockExtra")
		}
		if block.Extra.Custom.KeyBlock != header.KeyBlock {
			return errors.New("validator block acceptance: key_block flag differs between BlockInfo and McBlockExtra")
		}
		if header.KeyBlock {
			if err := validateAcceptedKeyBlockConfig(block); err != nil {
				return err
			}
		}
	} else if header.KeyBlock {
		return errors.New("validator block acceptance: non-masterchain block is marked as a key block")
	}

	return nil
}

func validateAcceptedKeyBlockConfig(block *tlb.Block) error {
	config, err := blockproof.ConfigFromKeyBlock(block)
	if err != nil {
		return fmt.Errorf("validator block acceptance: decode key block config: %w", err)
	}

	current, err := config.GetCurrentValidators()
	if err != nil {
		return fmt.Errorf("validator block acceptance: decode current validator set: %w", err)
	}
	if err = validateAcceptedValidatorSet("current", current); err != nil {
		return err
	}
	optional := []acceptedValidatorSetLoader{
		{name: "previous", load: config.GetPrevValidators},
		{name: "previous temporary", load: config.GetPrevTempValidators},
		{name: "current temporary", load: config.GetCurrentTempValidators},
		{name: "next", load: config.GetNextValidators},
		{name: "next temporary", load: config.GetNextTempValidators},
	}
	for _, item := range optional {
		set, loadErr := item.load()
		if errors.Is(loadErr, tlb.ErrBlockchainConfigParamAbsent) {
			continue
		}
		if loadErr != nil {
			return fmt.Errorf("validator block acceptance: decode %s validator set: %w", item.name, loadErr)
		}
		if err = validateAcceptedValidatorSet(item.name, set); err != nil {
			return err
		}
	}

	// Config parameter 28 deliberately defaults when absent or invalid.
	// BroadcastProofRoot still visits it, but acceptance must not reject a key
	// block solely because that optional value is malformed.
	return nil
}

func validateAcceptedValidatorSet(name string, set *tlb.ValidatorSetAny) error {
	if _, err := blockproof.TotalValidators(*set); err != nil {
		return fmt.Errorf("validator block acceptance: validate %s validator set: %w", name, err)
	}

	return nil
}

func validateAcceptedShardIdent(shard tlb.ShardIdent) error {
	prefixBits := int(shard.PrefixBits)
	if prefixBits < 0 || prefixBits > 60 {
		return fmt.Errorf("prefix length %d is outside 0..60", prefixBits)
	}
	if shard.WorkchainID == math.MinInt32 {
		return errors.New("workchain is invalid")
	}

	lowerBits := 64 - prefixBits
	lowMask := ^uint64(0)
	if lowerBits < 64 {
		lowMask = uint64(1)<<lowerBits - 1
	}
	if shard.ShardPrefix&lowMask != 0 {
		return errors.New("shard prefix has non-zero bits outside its declared length")
	}

	return nil
}
