package archive

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"flexserver/service/storage"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strconv"
	"time"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	packageMagic   = 0xae8fdd01
	entryMagic     = 0x1e8b
	maxEntrySize   = 64 << 20
	blockEntryKind = "block"
	proofEntryKind = "proof"
	proofLinkKind  = "prooflink"
)

var entryNameRE = regexp.MustCompile(`^(block|proof|prooflink)_\((-?\d+),([0-9a-fA-F]{16}),(\d+)\):([0-9a-fA-F]{64}):([0-9a-fA-F]{64})$`)

type entryRef struct {
	kind string
	id   ton.BlockIDExt
}

type blockParts struct {
	id        ton.BlockIDExt
	block     []byte
	proof     []byte
	proofLink []byte
	savedFull bool
}

type ImportSink struct {
	Writer    storage.PeerServingStorageWriter
	FullBlock func(*storage.ServedBlockFull) error
}

func ImportFile(ctx context.Context, archive *Downloaded, sink ImportSink) (*ImportStats, error) {
	file, err := os.Open(archive.Path)
	if err != nil {
		return nil, fmt.Errorf("open downloaded archive: %w", err)
	}
	defer func() { _ = file.Close() }()

	stats := &ImportStats{
		MasterchainSeqno: archive.MasterchainSeqno,
		ArchiveID:        archive.ArchiveID,
		Peer:             archive.Peer,
		Bytes:            archive.Bytes,
		DownloadElapsed:  archive.DownloadElapsed,
	}
	parts := map[string]*blockParts{}
	seenBlocks := map[string]struct{}{}
	var blockIDs []ton.BlockIDExt

	started := time.Now()
	err = ReadPackage(ctx, file, func(name string, data []byte) error {
		ref, ok, err := parseEntryName(name)
		if err != nil {
			return err
		}
		if !ok {
			stats.IgnoredEntries++
			return nil
		}

		stats.Entries++
		if archive.Shard.IsMasterchain() && !archive.Shard.ContainsBlock(ref.id) {
			stats.ContainsShardBlocks = true
		}
		if archive.Shard.ContainsBlock(ref.id) {
			stats.observe(ref.id)
		}

		key := storage.BlockKey(ref.id)
		part := parts[key]
		if part == nil {
			part = &blockParts{id: ref.id}
			parts[key] = part
		}

		switch ref.kind {
		case blockEntryKind:
			stats.Blocks++
			part.block = data
			if archive.Shard.IsMasterchain() && ref.id.Workchain == -1 {
				if err = observeMasterchainBlockShards(stats, ref.id, data); err != nil {
					return err
				}
			}
			if _, seen := seenBlocks[key]; !seen {
				seenBlocks[key] = struct{}{}
				blockIDs = append(blockIDs, ref.id)
			}
		case proofEntryKind:
			stats.Proofs++
			part.proof = data
		case proofLinkKind:
			stats.ProofLinks++
			part.proofLink = data
		default:
			stats.IgnoredEntries++
			return nil
		}

		if err = flushBlockPart(sink, part, stats); err != nil {
			return err
		}
		if part.savedFull {
			delete(parts, key)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	for _, part := range parts {
		if part.savedFull {
			continue
		}
		if len(part.block) > 0 {
			sink.Writer.SaveBlockData(part.id, part.block)
		}
		if len(part.proof) > 0 {
			sink.Writer.SaveBlockProof(storage.ServedProofBlock, part.id, part.proof)
		}
		if len(part.proofLink) > 0 {
			sink.Writer.SaveBlockProof(storage.ServedProofBlockLink, part.id, part.proofLink)
		}
	}

	stats.Links = linkBlocks(sink.Writer, blockIDs)
	if archive.Shard.IsMasterchain() {
		stats.MasterchainFirstSeqno = stats.FirstSeqno
		stats.MasterchainLastSeqno = stats.LastSeqno
	}
	stats.ImportElapsed = time.Since(started)
	return stats, nil
}

func observeMasterchainBlockShards(stats *ImportStats, id ton.BlockIDExt, data []byte) error {
	if id.SeqNo < stats.LastSeqno {
		return nil
	}

	shards, err := MasterchainBlockShards(id, data)
	if err != nil {
		return err
	}
	stats.MasterchainShardBlocks = shards
	return nil
}

func MasterchainBlockShards(id ton.BlockIDExt, data []byte) ([]ton.BlockIDExt, error) {
	root, err := cell.FromBOC(data)
	if err != nil {
		return nil, nil
	}

	loader := root.BeginParse()
	magic, err := loader.LoadUInt(32)
	if err != nil || magic != 0x11ef55aa {
		return nil, nil
	}
	if _, err = loader.LoadUInt(32); err != nil {
		return nil, nil
	}
	if _, err = loader.LoadRefCell(); err != nil {
		return nil, nil
	}
	if _, err = loader.LoadRefCell(); err != nil {
		return nil, nil
	}
	if _, err = loader.LoadRefCell(); err != nil {
		return nil, nil
	}

	extraCell, err := loader.LoadRefCell()
	if err != nil {
		return nil, nil
	}
	extra := extraCell.BeginParse()
	magic, err = extra.LoadUInt(32)
	if err != nil || magic != 0x4a33f6fd {
		return nil, nil
	}
	for i := 0; i < 3; i++ {
		if _, err = extra.LoadRefCell(); err != nil {
			return nil, nil
		}
	}
	if _, err = extra.LoadSlice(512); err != nil {
		return nil, nil
	}

	custom, err := extra.LoadMaybeRef()
	if err != nil || custom == nil {
		return nil, nil
	}
	magic, err = custom.LoadUInt(16)
	if err != nil || magic != 0xcca5 {
		return nil, nil
	}
	if _, err = custom.LoadUInt(1); err != nil {
		return nil, nil
	}

	shardHashes, err := custom.LoadDict(32)
	if err != nil {
		return nil, err
	}
	loadedShards, err := ton.LoadShardsFromHashes(shardHashes, false)
	if err != nil {
		return nil, nil
	}

	shards := make([]ton.BlockIDExt, 0, len(loadedShards))
	for _, shard := range loadedShards {
		shards = append(shards, *shard)
	}
	return shards, nil
}

func flushBlockPart(sink ImportSink, part *blockParts, stats *ImportStats) error {
	if part.savedFull || len(part.block) == 0 {
		return nil
	}

	full := &storage.ServedBlockFull{
		ID:    part.id,
		Block: part.block,
		Meta:  &storage.BlockMeta{ID: part.id},
	}

	switch {
	case len(part.proof) > 0:
		full.Proof = part.proof
	case len(part.proofLink) > 0:
		full.Proof = part.proofLink
		full.IsLink = true
	default:
		return nil
	}

	if sink.FullBlock != nil {
		if err := sink.FullBlock(full); err != nil {
			return fmt.Errorf("prepare archived full block %s: %w", storage.FormatBlockRef(part.id), err)
		}
	}
	if err := sink.Writer.SaveBlockFull(full); err != nil {
		return fmt.Errorf("save archived full block %s: %w", storage.FormatBlockRef(part.id), err)
	}

	part.savedFull = true
	stats.FullBlocks++
	return nil
}

func linkBlocks(writer storage.PeerServingStorageWriter, blocks []ton.BlockIDExt) int {
	if len(blocks) < 2 {
		return 0
	}

	sort.Slice(blocks, func(i, j int) bool {
		if blocks[i].Workchain != blocks[j].Workchain {
			return blocks[i].Workchain < blocks[j].Workchain
		}
		if blocks[i].Shard != blocks[j].Shard {
			return blocks[i].Shard < blocks[j].Shard
		}
		return blocks[i].SeqNo < blocks[j].SeqNo
	})

	links := 0
	prev := blocks[0]
	for _, next := range blocks[1:] {
		if prev.Workchain == next.Workchain && prev.Shard == next.Shard && next.SeqNo == prev.SeqNo+1 {
			writer.LinkNextBlock(prev, next)
			links++
		}
		prev = next
	}
	return links
}

func ReadPackage(ctx context.Context, r io.Reader, handle func(name string, data []byte) error) error {
	var magic [4]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil {
		return fmt.Errorf("read archive magic: %w", err)
	}
	if binary.LittleEndian.Uint32(magic[:]) != packageMagic {
		return fmt.Errorf("archive package magic mismatch")
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var header [8]byte
		if _, err := io.ReadFull(r, header[:]); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("read archive entry header: %w", err)
		}

		header0 := binary.LittleEndian.Uint32(header[:4])
		if header0&0xffff != entryMagic {
			return fmt.Errorf("archive entry magic mismatch")
		}

		nameLen := int(header0 >> 16)
		dataLen := int(binary.LittleEndian.Uint32(header[4:]))
		if dataLen > maxEntrySize {
			return fmt.Errorf("archive entry %d bytes exceeds limit %d", dataLen, maxEntrySize)
		}

		name := make([]byte, nameLen)
		if _, err := io.ReadFull(r, name); err != nil {
			return fmt.Errorf("read archive entry name: %w", err)
		}

		data := make([]byte, dataLen)
		if _, err := io.ReadFull(r, data); err != nil {
			return fmt.Errorf("read archive entry data: %w", err)
		}

		if err := handle(string(name), data); err != nil {
			return err
		}
	}
}

func parseEntryName(name string) (entryRef, bool, error) {
	matches := entryNameRE.FindStringSubmatch(name)
	if matches == nil {
		return entryRef{}, false, nil
	}

	workchain, err := strconv.ParseInt(matches[2], 10, 32)
	if err != nil {
		return entryRef{}, false, fmt.Errorf("parse archive workchain from %q: %w", name, err)
	}
	shard, err := strconv.ParseUint(matches[3], 16, 64)
	if err != nil {
		return entryRef{}, false, fmt.Errorf("parse archive shard from %q: %w", name, err)
	}
	seqno, err := strconv.ParseUint(matches[4], 10, 32)
	if err != nil {
		return entryRef{}, false, fmt.Errorf("parse archive seqno from %q: %w", name, err)
	}
	rootHash, err := hex.DecodeString(matches[5])
	if err != nil {
		return entryRef{}, false, fmt.Errorf("parse archive root hash from %q: %w", name, err)
	}
	fileHash, err := hex.DecodeString(matches[6])
	if err != nil {
		return entryRef{}, false, fmt.Errorf("parse archive file hash from %q: %w", name, err)
	}

	return entryRef{
		kind: matches[1],
		id: ton.BlockIDExt{
			Workchain: int32(workchain),
			Shard:     int64(shard),
			SeqNo:     uint32(seqno),
			RootHash:  rootHash,
			FileHash:  fileHash,
		},
	}, true, nil
}
