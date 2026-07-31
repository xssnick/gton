package archive

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strconv"
	"time"

	"github.com/xssnick/gton/service/archive/packfile"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	blockEntryKind = packfile.KindBlock
	proofEntryKind = packfile.KindProof
	proofLinkKind  = packfile.KindProofLink
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

func isMasterchainBlock(block ton.BlockIDExt) bool {
	return block.Workchain == -1 && block.Shard == topShard
}

type Imported struct {
	Stats      *ImportStats
	FullBlocks []*storage.ServedBlockFull
	Links      []storage.ServedBlockLink

	PreparedBlocks map[storage.BlockRootHash]PreparedBlock
}

// ImportBytes imports an in-memory archive using the Importer's shared block
// preparation workers.
func (i *Importer) ImportBytes(ctx context.Context, archive *Downloaded, data []byte) (*Imported, error) {
	if err := i.begin(); err != nil {
		return nil, err
	}
	defer i.end()

	archiveRef := *archive
	archiveRef.Data = nil
	archiveRef.Bytes = int64(len(data))
	// The whole pack is already resident in data; parse it in place and hand
	// entries out as zero-copy sub-slices instead of re-copying every block and
	// proof out of an io.Reader. This removes the ~2x peak (full pack plus a
	// second copy of all entries) on the terabyte-scale import path. data is a
	// single-use, non-pooled download buffer, so aliasing it is safe: the
	// retained blocks keep its backing array alive via GC.
	return i.importEntries(ctx, &archiveRef, func(handle func(packfile.Entry) error) error {
		return packfile.ReadBytes(ctx, data, handle)
	})
}

// ImportStream imports an archive stream using the Importer's shared block
// preparation workers.
func (i *Importer) ImportStream(ctx context.Context, archive *Downloaded, r io.Reader) (*Imported, error) {
	if err := i.begin(); err != nil {
		return nil, err
	}
	defer i.end()

	return i.importEntries(ctx, archive, func(handle func(packfile.Entry) error) error {
		return packfile.Read(ctx, r, handle)
	})
}

func (i *Importer) importEntries(ctx context.Context, archive *Downloaded, read func(handle func(packfile.Entry) error) error) (*Imported, error) {
	stats := &ImportStats{
		MasterchainSeqno: archive.MasterchainSeqno,
		ArchiveID:        archive.ArchiveID,
		Peer:             archive.Peer,
		Bytes:            archive.Bytes,
		DownloadElapsed:  archive.DownloadElapsed,
	}
	imported := &Imported{
		Stats:          stats,
		PreparedBlocks: map[storage.BlockRootHash]PreparedBlock{},
	}
	parts := map[storage.BlockRootHash]*blockParts{}
	preparer := newImportedBlockPreparer(ctx, i.dispatcher)
	defer preparer.abort()

	started := time.Now()
	err := read(func(entry packfile.Entry) error {
		if err := preparer.err(); err != nil {
			return err
		}

		processingStarted := time.Now()
		defer func() {
			stats.ProcessingElapsed += time.Since(processingStarted)
		}()

		ref, err := parseEntryName(entry.Name)
		if errors.Is(err, storage.ErrNotFound) {
			stats.IgnoredEntries++
			return nil
		}
		if err != nil {
			return err
		}

		stats.Entries++
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
			part.block = entry.Data
		case proofEntryKind:
			stats.Proofs++
			part.proof = entry.Data
		case proofLinkKind:
			stats.ProofLinks++
			part.proofLink = entry.Data
		default:
			stats.IgnoredEntries++
			return nil
		}

		if err = flushBlockPart(preparer, part, stats); err != nil {
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

	if err = preparer.finish(imported, stats); err != nil {
		return nil, err
	}

	imported.Links = buildFullBlockLinks(imported.FullBlocks)
	stats.Links = len(imported.Links)
	if archive.Shard.IsMasterchain() {
		stats.MasterchainFirstSeqno = stats.FirstSeqno
		stats.MasterchainLastSeqno = stats.LastSeqno
	}
	stats.ImportElapsed = time.Since(started)
	return imported, nil
}

func flushBlockPart(preparer *importedBlockPreparer, part *blockParts, stats *ImportStats) error {
	if part.savedFull || len(part.block) == 0 {
		return nil
	}

	full := &storage.ServedBlockFull{
		ID:    part.id,
		Block: part.block,
		Meta:  &storage.BlockMeta{ID: part.id},
	}

	if isMasterchainBlock(part.id) {
		if len(part.proof) == 0 {
			return nil
		}
		full.Proof = part.proof
	} else {
		if len(part.proofLink) == 0 {
			return nil
		}
		full.Proof = part.proofLink
		full.IsLink = true
	}

	if err := preparer.submit(full); err != nil {
		return err
	}
	part.savedFull = true
	stats.FullBlocks++
	return nil
}

func prepareImportedBlock(id ton.BlockIDExt, data []byte) (PreparedBlock, error) {
	root, err := cell.FromBOC(data)
	if err != nil {
		return PreparedBlock{}, fmt.Errorf("parse block BOC: %w", err)
	}
	rootHash := root.HashKey()
	if !bytes.Equal(rootHash[:], id.RootHash) {
		return PreparedBlock{}, fmt.Errorf("root hash mismatch")
	}
	sum := sha256.Sum256(data)
	if !bytes.Equal(sum[:], id.FileHash) {
		return PreparedBlock{}, fmt.Errorf("file hash mismatch")
	}

	block, err := storage.ParseVerifiedBlockCell(id, root)
	if err != nil {
		return PreparedBlock{}, err
	}
	if block.StateUpdate == nil {
		return PreparedBlock{}, fmt.Errorf("block state update is missing")
	}
	if err = cell.ValidateMerkleUpdate(block.StateUpdate); err != nil {
		return PreparedBlock{}, fmt.Errorf("validate block state update: %w", err)
	}
	meta, err := storage.BuildBlockMetaFromParsedBlock(id, block)
	if err != nil {
		return PreparedBlock{}, err
	}

	updateTo, err := block.StateUpdate.PeekRef(1)
	if err != nil {
		return PreparedBlock{}, fmt.Errorf("load block state update target: %w", err)
	}
	stateRoot := updateTo.Virtualize(0)
	stateRootHash := stateRoot.HashKey(0)

	started := time.Now()
	cells, err := storage.PrepareStateUpdateCells(block.StateUpdate)
	if err != nil {
		return PreparedBlock{}, err
	}
	return PreparedBlock{
		Block:       root,
		Parsed:      block,
		Meta:        meta,
		StateUpdate: block.StateUpdate,
		State: &storage.BlockState{
			Block:         id,
			StateRootHash: append([]byte(nil), stateRootHash[:]...),
		},
		StateUpdateToCells:        cells,
		StateUpdateToCellsElapsed: time.Since(started),
	}, nil
}

func buildFullBlockLinks(blocks []*storage.ServedBlockFull) []storage.ServedBlockLink {
	if len(blocks) < 2 {
		return nil
	}

	seen := make(map[storage.BlockRootHash]struct{}, len(blocks))
	ids := make([]ton.BlockIDExt, 0, len(blocks))
	for _, block := range blocks {
		if block == nil {
			continue
		}
		key := storage.BlockKey(block.ID)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		ids = append(ids, block.ID)
	}
	return buildBlockLinks(ids)
}

func buildBlockLinks(blocks []ton.BlockIDExt) []storage.ServedBlockLink {
	if len(blocks) < 2 {
		return nil
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

	links := make([]storage.ServedBlockLink, 0, len(blocks)-1)
	prev := blocks[0]
	for _, next := range blocks[1:] {
		if prev.Workchain == next.Workchain && prev.Shard == next.Shard && next.SeqNo == prev.SeqNo+1 {
			links = append(links, storage.ServedBlockLink{Prev: prev, Next: next})
		}
		prev = next
	}
	return links
}

func parseEntryName(name string) (entryRef, error) {
	matches := entryNameRE.FindStringSubmatch(name)
	if matches == nil {
		return entryRef{}, storage.ErrNotFound
	}

	workchain, err := strconv.ParseInt(matches[2], 10, 32)
	if err != nil {
		return entryRef{}, fmt.Errorf("parse archive workchain from %q: %w", name, err)
	}
	shard, err := strconv.ParseUint(matches[3], 16, 64)
	if err != nil {
		return entryRef{}, fmt.Errorf("parse archive shard from %q: %w", name, err)
	}
	seqno, err := strconv.ParseUint(matches[4], 10, 32)
	if err != nil {
		return entryRef{}, fmt.Errorf("parse archive seqno from %q: %w", name, err)
	}
	rootHash, err := hex.DecodeString(matches[5])
	if err != nil {
		return entryRef{}, fmt.Errorf("parse archive root hash from %q: %w", name, err)
	}
	fileHash, err := hex.DecodeString(matches[6])
	if err != nil {
		return entryRef{}, fmt.Errorf("parse archive file hash from %q: %w", name, err)
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
	}, nil
}
