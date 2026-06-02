package service

import (
	"fmt"

	"github.com/xssnick/gton/service/storage"
)

func preparedBlockCheckpointArtifacts(block PreparedBlock, splitDepth uint32) (*storage.ServedBlockFull, []storage.ServedBlockLink, error) {
	if len(block.BlockBOC) == 0 {
		return nil, nil, fmt.Errorf("block data is missing")
	}
	if len(block.ProofBOC) == 0 {
		return nil, nil, fmt.Errorf("block proof is missing")
	}
	if block.Meta == nil {
		return nil, nil, fmt.Errorf("block meta is missing")
	}

	// Prepared block payloads are immutable after verification; keep the same
	// backing arrays through checkpoint staging instead of duplicating packs.
	full := &storage.ServedBlockFull{
		ID:                     block.ID,
		Proof:                  block.ProofBOC,
		Block:                  block.BlockBOC,
		Meta:                   block.Meta.Clone(),
		IsLink:                 storage.ServedBlockProofIsLink(block.ID, block.IsLink),
		ArchiveShardSplitDepth: splitDepth,
	}

	var links []storage.ServedBlockLink
	if len(block.Meta.PrevRefs) > 0 {
		links = make([]storage.ServedBlockLink, 0, len(block.Meta.PrevRefs))
		for _, prev := range block.Meta.PrevRefs {
			links = append(links, storage.ServedBlockLink{Prev: prev, Next: block.ID})
		}
	}
	return full, links, nil
}
