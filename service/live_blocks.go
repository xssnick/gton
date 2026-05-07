package service

import (
	"flexserver/service/p2p"
	"flexserver/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type liveBlockPublisher interface {
	SetLiveBlock(block ton.BlockIDExt, root *cell.Cell, data []byte, flushed bool) error
	MarkLiveBlockFlushed(block ton.BlockIDExt)
}

func (s *Service) configureLiveBlockPublisher(publisher CurrentStatePublisher) {
	cache, ok := publisher.(liveBlockPublisher)
	if !ok || s.node == nil {
		return
	}
	s.node.SetBlockCacheObserver(cache)
}

func (s *Service) publishLiveBlock(downloaded p2p.DownloadedBlock, flushed bool) {
	cache, ok := s.liveState.(liveBlockPublisher)
	if !ok {
		return
	}

	root, err := downloadedBlockRoot(downloaded)
	if err != nil {
		s.log.Debug().
			Err(err).
			Str("block", downloaded.BlockRef()).
			Msg("skip live block cache update")
		return
	}

	blockData := downloaded.BlockBOC
	cacheFlushed := flushed
	if !downloaded.VerifiedFileHash {
		blockData = nil
		cacheFlushed = false
	}
	if err = cache.SetLiveBlock(downloaded.ID, root, blockData, cacheFlushed); err != nil {
		s.log.Debug().
			Err(err).
			Str("block", storage.FormatBlockRef(downloaded.ID)).
			Msg("skip live block cache update")
	}
}
