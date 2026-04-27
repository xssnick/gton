package p2p

import (
	"bytes"
	"crypto/sha256"
	"flexserver/service/storage"

	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func (n *Node) rememberDownloadedBlock(prev *ton.BlockIDExt, downloaded *DownloadedBlock) {
	writer, _ := n.peerStorage.(storage.PeerServingStorageWriter)
	if writer == nil {
		return
	}

	full := &storage.ServedBlockFull{
		ID:     downloaded.ID,
		Proof:  downloaded.ProofBOC,
		Block:  downloaded.BlockBOC,
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
	if prev != nil {
		writer.LinkNextBlock(*prev, downloaded.ID)
	}
}

func (n *Node) rememberInboundBroadcast(msg any) {
	switch data := msg.(type) {
	case tonnodeapi.BlockBroadcast:
		downloaded, err := normalizeDownloadedBlock(
			"tonNode.blockBroadcast",
			data.ID,
			append([]byte(nil), data.Proof...),
			append([]byte(nil), data.Data...),
			false,
			true,
			nil,
		)
		if err == nil {
			n.rememberDownloadedBlock(nil, downloaded)
		}
	case BlockBroadcastCompressed:
		downloaded, err := decodeCompressedBlock(DataFullCompressed{
			ID:         data.ID,
			Compressed: append([]byte(nil), data.Compressed...),
		})
		if err == nil {
			n.rememberDownloadedBlock(nil, downloaded)
		}
	case tonnodeapi.NewShardBlockBroadcast:
		raw, ok := normalizeRawBlockData(data.Block.ID, data.Block.Data)
		if !ok {
			return
		}
		if writer, _ := n.peerStorage.(storage.PeerServingStorageWriter); writer != nil {
			writer.SaveBlockData(data.Block.ID, raw)
		}
	}
}

func normalizeRawBlockData(block ton.BlockIDExt, data []byte) ([]byte, bool) {
	if len(data) == 0 {
		return nil, false
	}

	root, err := cell.FromBOC(data)
	if err != nil {
		return nil, false
	}
	rootHash := root.HashKey()
	if !bytes.Equal(rootHash[:], block.RootHash) {
		return nil, false
	}

	sum := sha256.Sum256(data)
	if !bytes.Equal(sum[:], block.FileHash) {
		return nil, false
	}
	return append([]byte(nil), data...), true
}
