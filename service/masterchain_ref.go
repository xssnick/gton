package service

import (
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

func setShardStateMasterchainRef(state *storage.BlockState, master ton.BlockIDExt) {
	if state == nil || state.Block.Workchain == -1 {
		return
	}

	// cppnode sets BlockHandle::masterchain_ref_block from the including master,
	// so overwrite any stale value parsed from the shard header.
	ref := cloneServiceBlockID(master)
	state.MasterchainRef = &ref
}

func setShardBlockMasterchainRef(meta *storage.BlockMeta, master ton.BlockIDExt) {
	if meta == nil || meta.ID.Workchain == -1 {
		return
	}

	// cppnode sets BlockHandle::masterchain_ref_block from the including master,
	// so overwrite any stale value parsed from the shard header.
	ref := cloneServiceBlockID(master)
	meta.MasterchainRef = &ref
}

func setPreparedShardMasterchainRef(block *PreparedBlock, master ton.BlockIDExt) {
	if block == nil {
		return
	}
	setShardBlockMasterchainRef(block.Meta, master)
}
