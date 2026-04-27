package service

import (
	"context"
	"errors"
	"flexserver/service/archive"
	"flexserver/service/storage"
	"fmt"
	"math/big"
	"math/bits"
	"os"
	"sync"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
)

const (
	nextBlockCatchUpMaxRemaining = 100_000
	maxArchiveMonitorSplitDepth  = 12
	archiveShardDownloadWorkers  = 4
)

func (s *Service) downloadShardArchives(ctx context.Context, masterchainSeqno uint32, shards []archive.ShardID) ([]*archive.Downloaded, error) {
	if len(shards) == 0 {
		return nil, nil
	}

	downloadCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	workers := archiveShardDownloadWorkers
	if len(shards) < workers {
		workers = len(shards)
	}

	type archiveFileResult struct {
		idx        int
		downloaded *archive.Downloaded
		err        error
	}

	jobs := make(chan int)
	results := make(chan archiveFileResult, len(shards))
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				downloaded, err := s.node.DownloadArchive(downloadCtx, masterchainSeqno, shards[idx], "")
				res := archiveFileResult{idx: idx, downloaded: downloaded, err: err}
				select {
				case results <- res:
				case <-downloadCtx.Done():
					if downloaded != nil {
						_ = os.Remove(downloaded.Path)
					}
					return
				}
			}
		}()
	}

	go func() {
		defer close(jobs)
		for idx := range shards {
			select {
			case jobs <- idx:
			case <-downloadCtx.Done():
				return
			}
		}
	}()
	go func() {
		wg.Wait()
		close(results)
	}()

	files := make([]*archive.Downloaded, len(shards))
	var firstErr error
	for res := range results {
		if res.err != nil {
			cancel()
			if res.downloaded != nil {
				_ = os.Remove(res.downloaded.Path)
			}
			if firstErr == nil || errors.Is(firstErr, context.Canceled) {
				firstErr = res.err
			}
			continue
		}
		files[res.idx] = res.downloaded
	}
	if firstErr != nil {
		for _, file := range files {
			if file != nil {
				_ = os.Remove(file.Path)
			}
		}
		return files, firstErr
	}
	for _, file := range files {
		if file == nil {
			for _, cleanup := range files {
				if cleanup != nil {
					_ = os.Remove(cleanup.Path)
				}
			}
			return files, archive.ErrNotAvailable
		}
	}
	return files, nil
}

func mergeImportStats(total, next *archive.ImportStats, includeSeqRange bool) {
	if total.ArchiveID == 0 && next.ArchiveID != 0 {
		total.ArchiveID = next.ArchiveID
	}
	if total.Peer == "" || total.Peer == "local-master-catchup" && next.Peer != "" {
		total.Peer = next.Peer
	}

	total.Bytes += next.Bytes
	total.Entries += next.Entries
	total.IgnoredEntries += next.IgnoredEntries
	total.Blocks += next.Blocks
	total.Proofs += next.Proofs
	total.ProofLinks += next.ProofLinks
	total.FullBlocks += next.FullBlocks
	total.Links += next.Links
	total.DownloadElapsed += next.DownloadElapsed
	total.ImportElapsed += next.ImportElapsed

	if includeSeqRange {
		if total.FirstSeqno == 0 || next.FirstSeqno != 0 && next.FirstSeqno < total.FirstSeqno {
			total.FirstSeqno = next.FirstSeqno
		}
		if next.LastSeqno > total.LastSeqno {
			total.LastSeqno = next.LastSeqno
		}
	}
}

func archivePrefixHasChangedShard(prefix archive.ShardID, nextBlocks []ton.BlockIDExt, prevBlocks map[storage.ShardKey]ton.BlockIDExt) bool {
	for _, next := range nextBlocks {
		if next.Workchain != prefix.Workchain || !archiveShardIntersects(prefix.Shard, next.Shard) {
			continue
		}

		prev, ok := prevBlocks[storage.ShardKeyFromBlock(next)]
		if !ok || !prev.Equals(&next) {
			return true
		}
	}
	return false
}

func archiveShardIntersects(left int64, right int64) bool {
	leftDepth := archiveShardPrefixLength(left)
	rightDepth := archiveShardPrefixLength(right)
	depth := leftDepth
	if rightDepth < depth {
		depth = rightDepth
	}
	if depth <= 0 {
		return true
	}

	mask := ^uint64(0) << (64 - depth)
	return uint64(left)&mask == uint64(right)&mask
}

func archiveShardPrefixLength(shard int64) int {
	value := uint64(shard)
	if value == 0 {
		return 0
	}
	return 63 - bits.TrailingZeros64(value)
}

func monitorMinSplitDepth(state *storage.BlockState, workchain int32) (uint32, error) {
	if state.Parsed == nil || state.Parsed.McStateExtra == nil {
		return 0, fmt.Errorf("masterchain state %s is missing mc_state_extra", storage.FormatBlockRef(state.Block))
	}

	var extra tlb.McStateExtra
	if err := tlb.LoadFromCell(&extra, state.Parsed.McStateExtra.BeginParse()); err != nil {
		return 0, fmt.Errorf("parse mc_state_extra for %s: %w", storage.FormatBlockRef(state.Block), err)
	}
	if extra.ConfigParams.Config.Params == nil {
		return 0, fmt.Errorf("masterchain state %s has no config params", storage.FormatBlockRef(state.Block))
	}

	cfg := tlb.BlockchainConfig{Root: extra.ConfigParams.Config.Params.AsCell()}
	workchains, err := cfg.GetWorkchains()
	if err != nil {
		return 0, err
	}
	if workchains.Workchains == nil {
		return 0, fmt.Errorf("workchains config is empty")
	}

	value, err := workchains.Workchains.LoadValueByIntKey(big.NewInt(int64(workchain)))
	if err != nil {
		return 0, err
	}
	if value.BitsLeft() < 48 && value.RefsNum() > 0 {
		value, err = value.LoadRef()
		if err != nil {
			return 0, err
		}
	}

	magic, err := value.LoadUInt(8)
	if err != nil {
		return 0, err
	}
	if magic != 0xa6 && magic != 0xa7 {
		return 0, fmt.Errorf("unsupported workchain descriptor magic 0x%x", magic)
	}
	if _, err = value.LoadUInt(32); err != nil {
		return 0, err
	}
	depth, err := value.LoadUInt(8)
	if err != nil {
		return 0, err
	}
	return uint32(depth), nil
}
