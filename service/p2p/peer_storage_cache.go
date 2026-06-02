package p2p

import (
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

func (n *Node) SetBlockCacheObserver(observer BlockCacheObserver) {
	n.blockCacheObserver = observer
}

func (n *Node) PublishLiveBlockArtifacts(artifacts storage.LiveBlockArtifacts) error {
	if n == nil || n.liveBlockCache == nil {
		return nil
	}
	return n.liveBlockCache.PublishLiveBlockArtifacts(artifacts)
}

func (n *Node) MarkLiveBlockFlushed(block ton.BlockIDExt) {
	if n == nil || n.liveBlockCache == nil {
		return
	}
	n.liveBlockCache.MarkBlockFlushed(block)
}

func (n *Node) LiveBlockCache() *storage.LiveBlockCache {
	if n == nil {
		return nil
	}
	return n.liveBlockCache
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
	}, kind); err != nil {
		n.log.Debug().
			Err(err).
			Str("block", downloaded.BlockRef()).
			Msg("skip non-final live block cache update")
	}
}
