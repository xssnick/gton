package service

import (
	"fmt"
	"time"

	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type PreparedBlock struct {
	ID   ton.BlockIDExt
	Kind string

	BlockBOC []byte
	ProofBOC []byte

	BlockRoot *cell.Cell
	Meta      *storage.BlockMeta
	// TODO: drop StateUpdate from long-lived prepared blocks once apply carries
	// Merkle update from/to hashes with StateUpdateToCells.
	StateUpdate               *cell.Cell
	StateUpdateToCells        storage.StateCellRecords
	StateUpdateToCellsElapsed time.Duration
	PrepareElapsed            time.Duration
	consensus                 *masterchainConsensusProof
	consensusChecked          *checkedMasterchainConsensus

	IsLink       bool
	SourcePeerID p2p.PeerID
}

type VerifiedBlock struct {
	ID   ton.BlockIDExt
	Kind string

	BlockBOC []byte
	ProofBOC []byte

	BlockRoot        *cell.Cell
	Meta             *storage.BlockMeta
	StateUpdate      *cell.Cell
	consensus        *masterchainConsensusProof
	consensusChecked *checkedMasterchainConsensus

	IsLink       bool
	SourcePeerID p2p.PeerID
}

func (b PreparedBlock) BlockRef() string {
	return storage.FormatBlockRef(b.ID)
}

func (b VerifiedBlock) BlockRef() string {
	return storage.FormatBlockRef(b.ID)
}

func (b *PreparedBlock) releaseStateUpdatePayload() {
	b.StateUpdate = nil
	b.StateUpdateToCells = storage.StateCellRecords{}
	b.StateUpdateToCellsElapsed = 0
}

func preparedBlockRoot(block PreparedBlock) (*cell.Cell, error) {
	if block.BlockRoot == nil {
		return nil, fmt.Errorf("prepared block %s has no block root", block.BlockRef())
	}
	return block.BlockRoot, nil
}

func parsePreparedBlock(block PreparedBlock) (*tlb.Block, error) {
	root, err := preparedBlockRoot(block)
	if err != nil {
		return nil, err
	}
	return storage.ParseVerifiedBlockCell(block.ID, root)
}

func verifyDownloadedBlock(downloaded p2p.DownloadedBlock) (VerifiedBlock, error) {
	if len(downloaded.BlockBOC) == 0 {
		return VerifiedBlock{}, fmt.Errorf("block %s has no block BOC", downloaded.BlockRef())
	}
	if !downloaded.VerifiedRootHash {
		return VerifiedBlock{}, fmt.Errorf("block %s root hash is not verified", downloaded.BlockRef())
	}

	root, err := downloadedBlockRoot(downloaded)
	if err != nil {
		return VerifiedBlock{}, err
	}

	var consensus *masterchainConsensusProof
	if downloaded.ID.Workchain == -1 && downloaded.ID.Shard == topShard && downloaded.Proof != nil {
		consensus, _, err = prepareMasterchainConsensusProof(downloaded.ID, downloaded.Proof, downloaded.BroadcastSignatures)
		if err != nil {
			return VerifiedBlock{}, fmt.Errorf("prepare masterchain consensus proof %s: %w", downloaded.BlockRef(), err)
		}
	}

	meta := downloaded.Meta
	if meta == nil {
		return VerifiedBlock{}, fmt.Errorf("block %s has no metadata", downloaded.BlockRef())
	}
	if downloaded.StateUpdate == nil {
		return VerifiedBlock{}, fmt.Errorf("block %s does not contain state update", storage.FormatBlockRef(downloaded.ID))
	}

	return VerifiedBlock{
		ID:           downloaded.ID,
		Kind:         downloaded.Kind,
		BlockBOC:     downloaded.BlockBOC,
		ProofBOC:     downloaded.ProofBOC,
		BlockRoot:    root,
		Meta:         meta.Clone(),
		StateUpdate:  downloaded.StateUpdate,
		consensus:    consensus,
		IsLink:       downloaded.IsLink,
		SourcePeerID: downloaded.SourcePeerID,
	}, nil
}

func prepareVerifiedBlockForApply(block VerifiedBlock) (PreparedBlock, error) {
	if block.StateUpdate == nil {
		return PreparedBlock{}, fmt.Errorf("verified block %s has no state update", block.BlockRef())
	}

	started := time.Now()
	cells, err := storage.PrepareStateUpdateCells(block.StateUpdate)
	if err != nil {
		return PreparedBlock{}, fmt.Errorf("prepare state update target cells for %s: %w", block.BlockRef(), err)
	}

	return preparedBlockWithStateCells(block, cells, time.Since(started)), nil
}

func prepareVerifiedMasterchainBlockForNextSync(prev ton.BlockIDExt, block VerifiedBlock) (PreparedBlock, error) {
	started := time.Now()
	if block.ID.Workchain != -1 || block.ID.Shard != topShard {
		return PreparedBlock{}, fmt.Errorf("next-sync block %s is not masterchain", block.BlockRef())
	}
	if block.Meta == nil || len(block.Meta.PrevRefs) != 1 {
		return PreparedBlock{}, fmt.Errorf("masterchain block %s has no single previous ref", block.BlockRef())
	}
	if !block.Meta.PrevRefs[0].Equals(&prev) {
		return PreparedBlock{}, fmt.Errorf("%w: block=%s prev=%s expected=%s", errMasterchainPrevMismatch, block.BlockRef(), storage.FormatBlockRef(block.Meta.PrevRefs[0]), storage.FormatBlockRef(prev))
	}
	if block.consensus == nil || !block.consensus.block.Equals(&block.ID) {
		return PreparedBlock{}, fmt.Errorf("masterchain block %s has no prepared consensus proof", block.BlockRef())
	}
	if !block.consensus.prevRef.Equals(&prev) {
		return PreparedBlock{}, fmt.Errorf("%w: block=%s consensus_prev=%s expected=%s", errMasterchainPrevMismatch, block.BlockRef(), storage.FormatBlockRef(block.consensus.prevRef), storage.FormatBlockRef(prev))
	}
	prepared, err := prepareVerifiedBlockForApply(block)
	if err != nil {
		return PreparedBlock{}, err
	}
	prepared.PrepareElapsed = time.Since(started)
	return prepared, nil
}

func preparedBlockWithStateCells(block VerifiedBlock, cells storage.StateCellRecords, elapsed time.Duration) PreparedBlock {
	return PreparedBlock{
		ID:                        block.ID,
		Kind:                      block.Kind,
		BlockBOC:                  block.BlockBOC,
		ProofBOC:                  block.ProofBOC,
		BlockRoot:                 block.BlockRoot,
		Meta:                      block.Meta.Clone(),
		StateUpdate:               block.StateUpdate,
		StateUpdateToCells:        cells,
		StateUpdateToCellsElapsed: elapsed,
		PrepareElapsed:            elapsed,
		consensus:                 block.consensus,
		consensusChecked:          block.consensusChecked,
		IsLink:                    block.IsLink,
		SourcePeerID:              block.SourcePeerID,
	}
}

func verifiedBlockFromPrepared(block PreparedBlock) (VerifiedBlock, error) {
	if block.StateUpdate == nil {
		return VerifiedBlock{}, fmt.Errorf("prepared block %s has no state update", block.BlockRef())
	}
	if block.BlockRoot == nil {
		return VerifiedBlock{}, fmt.Errorf("prepared block %s has no block root", block.BlockRef())
	}

	return VerifiedBlock{
		ID:               block.ID,
		Kind:             block.Kind,
		BlockBOC:         block.BlockBOC,
		ProofBOC:         block.ProofBOC,
		BlockRoot:        block.BlockRoot,
		Meta:             block.Meta.Clone(),
		StateUpdate:      block.StateUpdate,
		consensus:        block.consensus,
		consensusChecked: block.consensusChecked,
		IsLink:           block.IsLink,
		SourcePeerID:     block.SourcePeerID,
	}, nil
}

func prepareDownloadedBlockForApply(downloaded p2p.DownloadedBlock) (PreparedBlock, error) {
	started := time.Now()
	block, err := verifyDownloadedBlock(downloaded)
	if err != nil {
		return PreparedBlock{}, err
	}
	prepared, err := prepareVerifiedBlockForApply(block)
	if err != nil {
		return PreparedBlock{}, err
	}
	prepared.PrepareElapsed = time.Since(started)
	return prepared, nil
}
