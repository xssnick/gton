package p2p

import (
	"github.com/xssnick/gton/service/storage"
)

func (n *Node) SetBlockCacheObserver(observer BlockCacheObserver) {
	n.blockCacheObserver = observer
}

func (n *Node) nonfinalBlockCacheEnabled() bool {
	return n.blockCacheObserver != nil && n.blockCacheObserver.NonfinalBlockCacheEnabled()
}

func (n *Node) publishNonfinalDownloadedBlock(downloaded *DownloadedBlock, kind storage.LiveBlockNonfinalKind) {
	if !n.nonfinalBlockCacheEnabled() {
		return
	}
	if downloaded.ID.Workchain == -1 && downloaded.ID.Shard == topShard {
		return
	}
	if downloaded.Block == nil || len(downloaded.BlockBOC) == 0 {
		return
	}

	if err := n.blockCacheObserver.PublishNonfinalBlockArtifacts(storage.LiveBlockArtifacts{
		Block:     downloaded.ID,
		Root:      downloaded.Block,
		BlockData: downloaded.BlockBOC,
		Meta:      downloaded.Meta,
		// Extracted at broadcast/download decode; carrying it over lets the
		// non-final store skip re-parsing the block. The store decides by
		// artifact kind whether the update still needs merkle validation
		// (unsigned candidates do).
		StateUpdate: downloaded.StateUpdate,
	}, kind); err != nil {
		n.log.Debug().
			Err(err).
			Str("block", downloaded.BlockRef()).
			Msg("skip non-final live block cache update")
	}
}
