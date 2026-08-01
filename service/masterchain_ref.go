package service

import (
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

func setShardStateMasterchainRef(state *storage.BlockState, master ton.BlockIDExt) {
	if state.Block.Workchain == -1 {
		return
	}

	// cppnode sets BlockHandle::masterchain_ref_block from the including master,
	// so overwrite any stale value parsed from the shard header.
	ref := cloneServiceBlockID(master)
	state.MasterchainRef = &ref
}

func setShardBlockMasterchainRef(meta *storage.BlockMeta, master ton.BlockIDExt) {
	if meta.ID.Workchain == -1 {
		return
	}

	// cppnode sets BlockHandle::masterchain_ref_block from the including master,
	// so overwrite any stale value parsed from the shard header.
	meta.MasterchainRefSeqno = master.SeqNo
}
