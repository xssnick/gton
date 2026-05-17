package service

import (
	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

func setShardStateMasterchainRef(state *storage.BlockState, master ton.BlockIDExt) {
	if state == nil || state.Block.Workchain == -1 || state.MasterchainRef != nil {
		return
	}

	ref := cloneServiceBlockID(master)
	state.MasterchainRef = &ref
}

func setShardBlockMasterchainRef(meta *storage.BlockMeta, master ton.BlockIDExt) {
	if meta == nil || meta.ID.Workchain == -1 || meta.MasterchainRef != nil {
		return
	}

	ref := cloneServiceBlockID(master)
	meta.MasterchainRef = &ref
}

func setDownloadedShardMasterchainRef(block *p2p.DownloadedBlock, master ton.BlockIDExt) {
	if block == nil {
		return
	}
	setShardBlockMasterchainRef(block.Meta, master)
}
