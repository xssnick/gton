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
	ID ton.BlockIDExt

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
	Origin       SyncBlockOrigin
	Source       SyncBlockSource
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
	Source       SyncBlockSource
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

func parsePreparedBlock(block PreparedBlock) (*tlb.Block, error) {
	return storage.ParseVerifiedBlockCell(block.ID, block.BlockRoot)
}

func (s *SyncCoordinator) verifyDownloadedBlock(downloaded p2p.DownloadedBlock) (VerifiedBlock, error) {
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
		consensus, err = s.prepareMasterchainConsensusProof(downloaded.ID, downloaded.Proof, downloaded.SignaturesVerifiedKey)
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
	started := time.Now()
	cells, err := storage.PrepareStateUpdateCells(block.StateUpdate)
	if err != nil {
		return PreparedBlock{}, prepareStateUpdateCellsError(block, err)
	}

	return preparedBlockWithStateCells(block, cells, time.Since(started)), nil
}

func prepareStateUpdateCellsError(block VerifiedBlock, err error) error {
	return fmt.Errorf("prepare state update target cells for %s: %w", block.BlockRef(), err)
}

func prepareVerifiedMasterchainBlockForNextSync(prev ton.BlockIDExt, block VerifiedBlock) (PreparedBlock, error) {
	started := time.Now()
	if err := checkVerifiedMasterchainBlockFollows(prev, block); err != nil {
		return PreparedBlock{}, err
	}
	prepared, err := prepareVerifiedBlockForApply(block)
	if err != nil {
		return PreparedBlock{}, err
	}
	prepared.PrepareElapsed = time.Since(started)
	return prepared, nil
}

// checkVerifiedMasterchainBlockFollows is the cheap gate that must run before
// the expensive cell preparation, so a late or forked broadcast is rejected
// without paying for a state-update walk.
func checkVerifiedMasterchainBlockFollows(prev ton.BlockIDExt, block VerifiedBlock) error {
	if block.ID.Workchain != -1 || block.ID.Shard != topShard {
		return fmt.Errorf("next-sync block %s is not masterchain", block.BlockRef())
	}
	if len(block.Meta.PrevRefs) != 1 {
		return fmt.Errorf("masterchain block %s has no single previous ref", block.BlockRef())
	}
	if !block.Meta.PrevRefs[0].Equals(&prev) {
		return fmt.Errorf("%w: block=%s prev=%s expected=%s", errMasterchainPrevMismatch, block.BlockRef(), storage.FormatBlockRef(block.Meta.PrevRefs[0]), storage.FormatBlockRef(prev))
	}
	if block.consensus == nil || !block.consensus.block.Equals(&block.ID) {
		return fmt.Errorf("masterchain block %s has no prepared consensus proof", block.BlockRef())
	}
	if !block.consensus.prevRef.Equals(&prev) {
		return fmt.Errorf("%w: block=%s consensus_prev=%s expected=%s", errMasterchainPrevMismatch, block.BlockRef(), storage.FormatBlockRef(block.consensus.prevRef), storage.FormatBlockRef(prev))
	}
	return nil
}

func preparedBlockWithStateCells(block VerifiedBlock, cells storage.StateCellRecords, elapsed time.Duration) PreparedBlock {
	origin := syncBlockOriginForKind(block.Kind)
	if block.Source == SyncBlockSourceInternal {
		origin = SyncBlockOriginOther
	}

	return PreparedBlock{
		ID:                        block.ID,
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
		Origin:                    origin,
		Source:                    block.Source,
		SourcePeerID:              block.SourcePeerID,
	}
}

func (s *SyncCoordinator) prepareDownloadedBlockForApply(downloaded p2p.DownloadedBlock) (PreparedBlock, error) {
	started := time.Now()
	block, err := s.verifyDownloadedBlock(downloaded)
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
