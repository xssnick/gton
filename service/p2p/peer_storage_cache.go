package p2p

import (
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

func (n *Node) SetBlockCacheObserver(observer BlockCacheObserver) {
	n.blockCacheObserver = observer
}

func (n *Node) rememberDownloadedBlock(prev *ton.BlockIDExt, downloaded *DownloadedBlock) {
	n.rememberBlockFull(prev, downloaded, false)
}

func (n *Node) rememberDownloadedBlockAsync(prev *ton.BlockIDExt, downloaded *DownloadedBlock) {
	n.rememberBlockFullAsync(prev, downloaded, false)
}

func (n *Node) RememberVerifiedBlockFull(prev *ton.BlockIDExt, downloaded DownloadedBlock) {
	n.rememberBlockFull(prev, &downloaded, true)
}

func (n *Node) RememberVerifiedBlockFullAsync(prev *ton.BlockIDExt, downloaded DownloadedBlock) {
	n.rememberBlockFullAsync(prev, &downloaded, true)
}

func (n *Node) rememberBlockFullAsync(prev *ton.BlockIDExt, downloaded *DownloadedBlock, verified bool) {
	if downloaded == nil {
		return
	}
	if downloaded.ID.Workchain != -1 || downloaded.ID.Shard != topShard {
		return
	}
	if !verified {
		return
	}

	if n.blockCacheSlots == nil {
		n.rememberBlockFull(prev, downloaded, verified)
		return
	}

	var prevCopy *ton.BlockIDExt
	if prev != nil {
		copied := *prev
		prevCopy = &copied
	}
	downloadedCopy := *downloaded

	select {
	case n.blockCacheSlots <- struct{}{}:
	default:
		n.log.Debug().
			Str("block", downloaded.BlockRef()).
			Msg("skip block full peer cache because writer is busy")
		return
	}

	go func() {
		defer func() { <-n.blockCacheSlots }()
		n.rememberBlockFull(prevCopy, &downloadedCopy, verified)
	}()
}

func (n *Node) rememberBlockFull(prev *ton.BlockIDExt, downloaded *DownloadedBlock, verified bool) {
	writer, _ := n.peerStorage.(storage.PeerServingStorageWriter)
	if writer == nil {
		return
	}
	if downloaded == nil {
		return
	}
	if downloaded.ID.Workchain != -1 || downloaded.ID.Shard != topShard {
		return
	}
	if !verified {
		return
	}

	blockData := downloaded.BlockBOC
	if !downloaded.VerifiedFileHash {
		blockData = nil
	}

	full := &storage.ServedBlockFull{
		ID:     downloaded.ID,
		Proof:  downloaded.ProofBOC,
		Block:  blockData,
		Meta:   downloaded.Meta,
		IsLink: downloaded.IsLink,
	}
	if err := writer.SaveBlockFull(full); err != nil {
		if n.runCtx != nil && n.runCtx.Err() != nil {
			n.log.Debug().
				Err(err).
				Str("block", downloaded.BlockRef()).
				Msg("skipped storing block full during shutdown")
			return
		}
		n.log.Warn().
			Err(err).
			Str("block", downloaded.BlockRef()).
			Msg("failed to store block full")
		return
	}
	if n.blockCacheObserver != nil && len(blockData) > 0 {
		n.blockCacheObserver.MarkLiveBlockFlushed(downloaded.ID)
	}
	if prev != nil {
		if err := writer.LinkNextBlock(*prev, downloaded.ID); err != nil {
			n.log.Warn().
				Err(err).
				Str("prev", formatBlockRef(*prev)).
				Str("next", downloaded.BlockRef()).
				Msg("failed to store next block link")
		}
	}
}
