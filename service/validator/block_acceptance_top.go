package validator

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/gton/service/p2p"
	sharddomain "github.com/xssnick/gton/service/shard"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/groups"
)

type blockAcceptanceAnchorKind uint8

const (
	blockAcceptanceAnchorLinear blockAcceptanceAnchorKind = iota + 1
	blockAcceptanceAnchorSplit
	blockAcceptanceAnchorMerge
)

type blockAcceptanceAnchor struct {
	kind   blockAcceptanceAnchorKind
	seqno  uint32
	blocks []ton.BlockIDExt
}

type acceptedProofLink struct {
	block     ton.BlockIDExt
	proofRoot *cell.Cell
	// Copy the header so a pending description does not retain the parsed
	// block's transaction and message dictionaries through a field pointer.
	header tlb.BlockHeader
	meta   *storage.BlockMeta
}

// BlockAcceptanceViewResolver produces the shard registry view that binds a
// finalized shard block to its registered ancestors. Acceptance resolves it
// lazily because the view can be temporarily unavailable, and the retry that
// follows must not repeat the parts of acceptance that already succeeded.
type BlockAcceptanceViewResolver func() (BlockAcceptanceView, error)

func currentBlockAcceptanceView(
	source collator.LocalGroupSource,
	target groups.ShardID,
) (BlockAcceptanceView, error) {
	if source == nil {
		return BlockAcceptanceView{}, fmt.Errorf(
			"%w: current validator-group view is unavailable",
			ErrBlockNotReady,
		)
	}

	snapshot, err := source.Snapshot()
	if err != nil {
		return BlockAcceptanceView{}, fmt.Errorf(
			"%w: load current validator-group view: %w",
			ErrBlockNotReady,
			err,
		)
	}
	view := BlockAcceptanceView{MasterchainBlock: snapshot.MasterchainBlock}

	// The exact active group is the common case. During a split or merge the
	// old runtime can finish concurrently with tracker reconciliation, so also
	// accept the unique registry projection that still resolves its shard.
	for i := range snapshot.Active {
		session := &snapshot.Active[i]
		if session.Shard != target || len(session.Registered) == 0 {
			continue
		}

		view.Registered = session.Registered
		if _, err = resolveBlockAcceptanceAnchor(target, view); err == nil {
			return view, nil
		}
	}

	var resolved *blockAcceptanceAnchor
	for i := range snapshot.Active {
		registered := snapshot.Active[i].Registered
		if len(registered) == 0 {
			continue
		}

		candidate := BlockAcceptanceView{
			MasterchainBlock: snapshot.MasterchainBlock,
			Registered:       registered,
		}
		anchor, resolveErr := resolveBlockAcceptanceAnchor(target, candidate)
		if resolveErr != nil {
			continue
		}
		if resolved != nil && !sameBlockAcceptanceAnchor(*resolved, anchor) {
			return BlockAcceptanceView{}, errors.New(
				"validator block acceptance: current shard registry has conflicting target projections",
			)
		}

		resolved = &anchor
		view.Registered = registered
	}
	if resolved == nil {
		return BlockAcceptanceView{}, fmt.Errorf(
			"%w: shard %d:%016x is absent from the current validator-group view",
			ErrBlockNotReady,
			target.Workchain,
			uint64(target.Shard),
		)
	}

	return view, nil
}

func sameBlockAcceptanceAnchor(left, right blockAcceptanceAnchor) bool {
	if left.kind != right.kind || left.seqno != right.seqno || len(left.blocks) != len(right.blocks) {
		return false
	}
	for i := range left.blocks {
		if !left.blocks[i].Equals(&right.blocks[i]) {
			return false
		}
	}

	return true
}

func (a *BlockAccepter) buildShardTopDescription(
	ctx context.Context,
	resolveView BlockAcceptanceViewResolver,
	prepared *preparedBlockAcceptance,
) error {
	if resolveView == nil {
		return errors.New("validator block acceptance: acceptance view resolver is absent")
	}
	view, err := resolveView()
	if err != nil {
		return err
	}
	anchor, err := resolveBlockAcceptanceAnchor(a.shard, view)
	if err != nil {
		return err
	}
	if prepared.link.block.SeqNo <= anchor.seqno {
		return nil
	}
	if prepared.link.block.SeqNo-anchor.seqno > 8 {
		return fmt.Errorf(
			"validator block acceptance: block %s needs more than eight shard proof links after registered seqno %d",
			storage.FormatBlockRef(prepared.link.block),
			anchor.seqno,
		)
	}

	links, err := a.loadAcceptedProofChain(ctx, anchor, prepared)
	if err != nil {
		return err
	}
	if err = validateAcceptedProofMasterchain(ctx, a.node, view.MasterchainBlock, links); err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}

	proofs := make([]*cell.Cell, len(links))
	for i := range links {
		proofs[i] = links[i].proofRoot
	}
	root, err := p2p.BuildShardTopBlockDescription(
		prepared.link.block,
		prepared.signaturesCell,
		proofs,
	)
	if err != nil {
		return fmt.Errorf("validator block acceptance: build shard top description: %w", err)
	}
	parsed, err := p2p.ParseShardTopBlockDescription(
		prepared.link.block,
		int32(a.catchainSeqno),
		root,
	)
	if err != nil {
		a.log.Warn().
			Err(err).
			Str("block", storage.FormatBlockRef(prepared.link.block)).
			Msg("new shard top description failed strict validation")

		return nil
	}
	if err = blockproof.CheckPreparedBlockSignatures(prepared.link.block, parsed.Signatures, a.validators); err != nil {
		a.log.Warn().
			Err(err).
			Str("block", storage.FormatBlockRef(prepared.link.block)).
			Msg("new shard top description signatures failed validation")

		return nil
	}

	if a.shardTops != nil {
		if err = a.shardTops.StoreShardTopDescription(ctx, parsed.Description, root); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			a.log.Warn().
				Err(err).
				Str("block", storage.FormatBlockRef(prepared.link.block)).
				Msg("failed to install local shard top description")
		}
	}
	return nil
}

func resolveBlockAcceptanceAnchor(
	target groups.ShardID,
	view BlockAcceptanceView,
) (blockAcceptanceAnchor, error) {
	if err := validateAcceptanceMasterchainBlock(view.MasterchainBlock); err != nil {
		return blockAcceptanceAnchor{}, err
	}

	switch len(view.Registered) {
	case 1:
		registered := &view.Registered[0]
		if err := validateAcceptanceRegisteredShard(registered); err != nil {
			return blockAcceptanceAnchor{}, err
		}

		kind := blockAcceptanceAnchorLinear
		switch {
		case registered.Shard == target:
			if registered.BeforeSplit || registered.BeforeMerge {
				return blockAcceptanceAnchor{}, errors.New(
					"validator block acceptance: linear registered shard announces a split or merge",
				)
			}
		case registered.Shard.Workchain == target.Workchain &&
			sharddomain.IsDirectChild(registered.Shard.Shard, target.Shard):
			if !registered.BeforeSplit {
				return blockAcceptanceAnchor{}, errors.New(
					"validator block acceptance: registered split parent is not marked before split",
				)
			}
			kind = blockAcceptanceAnchorSplit
		default:
			return blockAcceptanceAnchor{}, errors.New(
				"validator block acceptance: registered shard is neither the target nor its direct parent",
			)
		}

		return blockAcceptanceAnchor{
			kind:   kind,
			seqno:  registered.Block.SeqNo,
			blocks: []ton.BlockIDExt{registered.Block},
		}, nil

	case 2:
		left := &view.Registered[0]
		right := &view.Registered[1]
		if err := validateAcceptanceRegisteredShard(left); err != nil {
			return blockAcceptanceAnchor{}, err
		}
		if err := validateAcceptanceRegisteredShard(right); err != nil {
			return blockAcceptanceAnchor{}, err
		}
		leftShard, err := sharddomain.Child(target.Shard, true)
		if err != nil {
			return blockAcceptanceAnchor{}, fmt.Errorf("validator block acceptance: derive left merge child: %w", err)
		}
		rightShard, err := sharddomain.Child(target.Shard, false)
		if err != nil {
			return blockAcceptanceAnchor{}, fmt.Errorf("validator block acceptance: derive right merge child: %w", err)
		}
		if left.Shard != (groups.ShardID{Workchain: target.Workchain, Shard: leftShard}) ||
			right.Shard != (groups.ShardID{Workchain: target.Workchain, Shard: rightShard}) {
			return blockAcceptanceAnchor{}, errors.New(
				"validator block acceptance: registered merge shards are not ordered direct children",
			)
		}
		if !left.BeforeMerge || !right.BeforeMerge {
			return blockAcceptanceAnchor{}, errors.New(
				"validator block acceptance: registered merge children are not marked before merge",
			)
		}

		return blockAcceptanceAnchor{
			kind:   blockAcceptanceAnchorMerge,
			seqno:  max(left.Block.SeqNo, right.Block.SeqNo),
			blocks: []ton.BlockIDExt{left.Block, right.Block},
		}, nil

	default:
		return blockAcceptanceAnchor{}, fmt.Errorf(
			"validator block acceptance: shard has %d registered descriptions, want one or two",
			len(view.Registered),
		)
	}
}

func validateAcceptanceMasterchainBlock(block ton.BlockIDExt) error {
	if err := storage.ValidateBlockIDHashes(block); err != nil {
		return fmt.Errorf("validator block acceptance: invalid masterchain view: %w", err)
	}
	if block.Workchain != -1 || block.Shard != sharddomain.Root {
		return errors.New("validator block acceptance: acceptance view is not a masterchain block")
	}

	return nil
}

func validateAcceptanceRegisteredShard(description *groups.ShardDescription) error {
	if !description.Shard.IsValid() || description.Shard.IsMasterchain() {
		return errors.New("validator block acceptance: invalid registered shard")
	}
	if description.Block.Workchain != description.Shard.Workchain ||
		description.Block.Shard != description.Shard.Shard {
		return errors.New("validator block acceptance: registered block belongs to another shard")
	}
	if err := storage.ValidateBlockIDHashes(description.Block); err != nil {
		return fmt.Errorf("validator block acceptance: invalid registered block: %w", err)
	}

	return nil
}

func (a *BlockAccepter) loadAcceptedProofChain(
	ctx context.Context,
	anchor blockAcceptanceAnchor,
	prepared *preparedBlockAcceptance,
) ([]acceptedProofLink, error) {
	links := make([]acceptedProofLink, 0, prepared.link.block.SeqNo-anchor.seqno)
	links = append(links, prepared.link)

	for links[len(links)-1].block.SeqNo > anchor.seqno+1 {
		current := &links[len(links)-1]
		if current.header.AfterSplit || current.header.AfterMerge {
			return nil, fmt.Errorf(
				"validator block acceptance: intermediate block %s crosses a split or merge",
				storage.FormatBlockRef(current.block),
			)
		}
		if len(current.meta.PrevRefs) != 1 {
			return nil, fmt.Errorf(
				"validator block acceptance: intermediate block %s has %d predecessors",
				storage.FormatBlockRef(current.block),
				len(current.meta.PrevRefs),
			)
		}
		previous := current.meta.PrevRefs[0]
		if previous.Workchain != a.shard.Workchain || previous.Shard != a.shard.Shard ||
			previous.SeqNo+1 != current.block.SeqNo {
			return nil, fmt.Errorf(
				"validator block acceptance: block %s does not link to a direct same-shard predecessor",
				storage.FormatBlockRef(current.block),
			)
		}

		link, err := a.loadAcceptedProofLink(ctx, previous)
		if err != nil {
			return nil, err
		}
		links = append(links, link)
	}
	if err := validateAcceptedProofAnchor(anchor, &links[len(links)-1]); err != nil {
		return nil, err
	}

	return links, nil
}

// loadAcceptedProofLink reads one predecessor proof link from the node's own
// storage, the way C++ builds its shard top description from the manager that
// already holds the block. Local submission is asynchronous, so a predecessor
// this session accepted moments ago may not be stored yet; that is reported as
// ErrBlockNotReady and the acceptance retry loop waits for it. Keeping a second
// durable copy of every accepted proof only to cover that gap costs one record
// per shard block forever, which is why it is not kept.
func (a *BlockAccepter) loadAcceptedProofLink(
	ctx context.Context,
	block ton.BlockIDExt,
) (acceptedProofLink, error) {
	proofBOC, err := a.node.BlockProof(ctx, storage.ServedProofBlockLink, block)
	if errors.Is(err, storage.ErrNotFound) {
		return acceptedProofLink{}, fmt.Errorf(
			"%w: proof link for %s has not reached node storage",
			ErrBlockNotReady,
			storage.FormatBlockRef(block),
		)
	}
	if err != nil {
		return acceptedProofLink{}, fmt.Errorf(
			"validator block acceptance: load node proof link %s: %w",
			storage.FormatBlockRef(block),
			err,
		)
	}

	return parseAcceptedProofLink(block, proofBOC)
}

func parseAcceptedProofLink(block ton.BlockIDExt, proofBOC []byte) (acceptedProofLink, error) {
	parsed, err := blockproof.ParseBOC(block, proofBOC)
	if err != nil {
		return acceptedProofLink{}, fmt.Errorf(
			"validator block acceptance: parse proof link %s: %w",
			storage.FormatBlockRef(block),
			err,
		)
	}
	if parsed.Proof.Signatures != nil {
		return acceptedProofLink{}, fmt.Errorf(
			"validator block acceptance: shard proof link %s contains signatures",
			storage.FormatBlockRef(block),
		)
	}

	return acceptedProofLink{
		block:     block,
		proofRoot: parsed.Proof.Root,
		header:    parsed.Block.BlockInfo,
		meta:      parsed.Meta,
	}, nil
}

func validateAcceptedProofAnchor(anchor blockAcceptanceAnchor, oldest *acceptedProofLink) error {
	if len(oldest.meta.PrevRefs) != len(anchor.blocks) {
		return fmt.Errorf(
			"validator block acceptance: oldest proof %s has %d predecessors, registered anchor has %d",
			storage.FormatBlockRef(oldest.block),
			len(oldest.meta.PrevRefs),
			len(anchor.blocks),
		)
	}
	for i := range anchor.blocks {
		if !oldest.meta.PrevRefs[i].Equals(&anchor.blocks[i]) {
			return fmt.Errorf(
				"validator block acceptance: oldest proof %s predecessor %d differs from registered block",
				storage.FormatBlockRef(oldest.block),
				i,
			)
		}
	}

	header := &oldest.header
	switch anchor.kind {
	case blockAcceptanceAnchorLinear:
		if header.AfterSplit || header.AfterMerge {
			return errors.New("validator block acceptance: linear anchor is crossed by split or merge block")
		}
	case blockAcceptanceAnchorSplit:
		if !header.AfterSplit || header.AfterMerge {
			return errors.New("validator block acceptance: first block after registered split has invalid topology flags")
		}
	case blockAcceptanceAnchorMerge:
		if !header.AfterMerge || header.AfterSplit {
			return errors.New("validator block acceptance: first block after registered merge has invalid topology flags")
		}
	default:
		return errors.New("validator block acceptance: unknown registered anchor topology")
	}

	return nil
}

func validateAcceptedProofMasterchain(
	ctx context.Context,
	node LocalSessionNode,
	masterchain ton.BlockIDExt,
	links []acceptedProofLink,
) error {
	var newer *ton.BlockIDExt
	for i := range links {
		masterRef := links[i].header.MasterRef
		if masterRef == nil {
			return fmt.Errorf(
				"validator block acceptance: shard proof %s has no masterchain reference",
				storage.FormatBlockRef(links[i].block),
			)
		}
		block := masterchainBlockFromReference(masterRef)
		if newer != nil {
			if block.SeqNo > newer.SeqNo {
				return fmt.Errorf(
					"validator block acceptance: older shard proof %s refers to newer masterchain seqno %d",
					storage.FormatBlockRef(links[i].block),
					block.SeqNo,
				)
			}
			if block.SeqNo == newer.SeqNo && !block.Equals(newer) {
				return fmt.Errorf(
					"validator block acceptance: shard proofs refer to different masterchain blocks at seqno %d",
					block.SeqNo,
				)
			}
		}
		if err := validateAcceptedMasterchainAncestor(ctx, node, masterchain, block); err != nil {
			return fmt.Errorf(
				"validator block acceptance: validate masterchain reference of %s: %w",
				storage.FormatBlockRef(links[i].block),
				err,
			)
		}
		newer = &block
	}

	return nil
}

func validateAcceptedMasterchainAncestor(
	ctx context.Context,
	node LocalSessionNode,
	masterchain ton.BlockIDExt,
	reference ton.BlockIDExt,
) error {
	if err := storage.ValidateBlockIDHashes(reference); err != nil {
		return fmt.Errorf("invalid masterchain reference: %w", err)
	}
	if reference.SeqNo > masterchain.SeqNo {
		return fmt.Errorf(
			"%w: reference seqno %d exceeds view seqno %d",
			ErrBlockNotReady,
			reference.SeqNo,
			masterchain.SeqNo,
		)
	}
	if reference.SeqNo == masterchain.SeqNo {
		if !reference.Equals(&masterchain) {
			return fmt.Errorf("reference differs from masterchain view at seqno %d", reference.SeqNo)
		}

		return nil
	}

	resolved, err := node.LookupBlockBySeqNo(ctx, storage.BlockSeqRef{
		Workchain: -1,
		Shard:     sharddomain.Root,
		SeqNo:     reference.SeqNo,
	})
	if errors.Is(err, storage.ErrNotFound) {
		return fmt.Errorf("%w: masterchain ancestor %d is not indexed", ErrBlockNotReady, reference.SeqNo)
	}
	if err != nil {
		return fmt.Errorf("lookup masterchain ancestor %d: %w", reference.SeqNo, err)
	}
	if !resolved.Equals(&reference) {
		return fmt.Errorf("masterchain history contains another block at seqno %d", reference.SeqNo)
	}

	return nil
}

func masterchainBlockFromReference(reference *tlb.ExtBlkRef) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: -1,
		Shard:     sharddomain.Root,
		SeqNo:     reference.SeqNo,
		RootHash:  bytes.Clone(reference.RootHash),
		FileHash:  bytes.Clone(reference.FileHash),
	}
}
