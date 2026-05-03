package archive

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strconv"
	"time"

	"flexserver/service/archive/packfile"
	"flexserver/service/storage"

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
	blockRef  *storage.ArtifactRef
	proofRef  *storage.ArtifactRef
	linkRef   *storage.ArtifactRef
	savedFull bool
}

type ImportSink struct {
	Writer    storage.PeerServingStorageWriter
	FullBlock func(*storage.ServedBlockFull) error
}

type Imported struct {
	Stats *ImportStats
	storage.ServedArchiveImport
	ArtifactPath string
}

func ImportFile(ctx context.Context, archive *Downloaded, sink ImportSink) (*ImportStats, error) {
	file, err := os.Open(archive.Path)
	if err != nil {
		return nil, fmt.Errorf("open downloaded archive: %w", err)
	}
	defer func() { _ = file.Close() }()

	started := time.Now()
	imported, err := ImportStream(ctx, archive, file)
	if err != nil {
		return nil, err
	}
	if err = imported.Store(sink); err != nil {
		return nil, err
	}
	imported.Stats.ImportElapsed = time.Since(started)
	return imported.Stats, nil
}

func ImportStream(ctx context.Context, archive *Downloaded, r io.Reader) (*Imported, error) {
	stats := &ImportStats{
		MasterchainSeqno: archive.MasterchainSeqno,
		ArchiveID:        archive.ArchiveID,
		Peer:             archive.Peer,
		Bytes:            archive.Bytes,
		DownloadElapsed:  archive.DownloadElapsed,
	}
	imported := &Imported{
		Stats:        stats,
		ArtifactPath: archive.Path,
	}
	parts := map[string]*blockParts{}
	seenBlocks := map[string]struct{}{}
	var blockIDs []ton.BlockIDExt

	started := time.Now()
	err := packfile.Read(ctx, r, func(entry packfile.Entry) error {
		ref, err := parseEntryName(entry.Name)
		if errors.Is(err, storage.ErrNotFound) {
			stats.IgnoredEntries++
			return nil
		}
		if err != nil {
			return err
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
			part.block = entry.Data
			part.blockRef = artifactRefFromEntry(archive.Path, entry)
			if archive.Shard.IsMasterchain() && ref.id.Workchain == -1 {
				if err = observeMasterchainBlockShards(stats, ref.id, entry.Data); err != nil {
					return err
				}
			}
			if _, seen := seenBlocks[key]; !seen {
				seenBlocks[key] = struct{}{}
				blockIDs = append(blockIDs, ref.id)
			}
		case proofEntryKind:
			stats.Proofs++
			part.proof = entry.Data
			part.proofRef = artifactRefFromEntry(archive.Path, entry)
		case proofLinkKind:
			stats.ProofLinks++
			part.proofLink = entry.Data
			part.linkRef = artifactRefFromEntry(archive.Path, entry)
		default:
			stats.IgnoredEntries++
			return nil
		}

		if err = flushBlockPart(imported, part, stats); err != nil {
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
			imported.BlockData = append(imported.BlockData, storage.ServedBlockData{
				ID:   part.id,
				Data: part.block,
				Ref:  part.blockRef,
			})
		}
		if len(part.proof) > 0 {
			imported.Proofs = append(imported.Proofs, storage.ServedBlockProof{
				Kind: storage.ServedProofBlock,
				ID:   part.id,
				Data: part.proof,
				Ref:  part.proofRef,
			})
		}
		if len(part.proofLink) > 0 {
			imported.Proofs = append(imported.Proofs, storage.ServedBlockProof{
				Kind: storage.ServedProofBlockLink,
				ID:   part.id,
				Data: part.proofLink,
				Ref:  part.linkRef,
			})
		}
	}

	imported.Links = buildBlockLinks(blockIDs)
	stats.Links = len(imported.Links)
	if archive.Shard.IsMasterchain() {
		stats.MasterchainFirstSeqno = stats.FirstSeqno
		stats.MasterchainLastSeqno = stats.LastSeqno
	}
	stats.ImportElapsed = time.Since(started)
	return imported, nil
}

func (i *Imported) SetArtifactPath(path string) {
	if i == nil {
		return
	}
	i.ArtifactPath = path
	for _, full := range i.FullBlocks {
		setArtifactPath(full.BlockRef, path)
		setArtifactPath(full.ProofRef, path)
	}
	for idx := range i.BlockData {
		setArtifactPath(i.BlockData[idx].Ref, path)
	}
	for idx := range i.Proofs {
		setArtifactPath(i.Proofs[idx].Ref, path)
	}
}

func (i *Imported) Store(sink ImportSink) error {
	if i == nil {
		return fmt.Errorf("imported archive is nil")
	}
	if sink.Writer == nil {
		return fmt.Errorf("archive import sink writer is nil")
	}
	if err := i.validateArtifactRefs(); err != nil {
		return err
	}

	for _, full := range i.FullBlocks {
		if sink.FullBlock != nil {
			if err := sink.FullBlock(full); err != nil {
				return fmt.Errorf("prepare archived full block %s: %w", storage.FormatBlockRef(full.ID), err)
			}
		}
	}
	if err := sink.Writer.SaveArchiveImport(&i.ServedArchiveImport); err != nil {
		return fmt.Errorf("save archived blocks: %w", err)
	}
	return nil
}

func (i *Imported) validateArtifactRefs() error {
	for _, full := range i.FullBlocks {
		if err := validateArtifactRef(full.BlockRef); err != nil {
			return fmt.Errorf("block artifact ref %s: %w", storage.FormatBlockRef(full.ID), err)
		}
		if err := validateArtifactRef(full.ProofRef); err != nil {
			return fmt.Errorf("proof artifact ref %s: %w", storage.FormatBlockRef(full.ID), err)
		}
	}
	for _, block := range i.BlockData {
		if err := validateArtifactRef(block.Ref); err != nil {
			return fmt.Errorf("block artifact ref %s: %w", storage.FormatBlockRef(block.ID), err)
		}
	}
	for _, proof := range i.Proofs {
		if err := validateArtifactRef(proof.Ref); err != nil {
			return fmt.Errorf("proof artifact ref %s: %w", storage.FormatBlockRef(proof.ID), err)
		}
	}
	return nil
}

func validateArtifactRef(ref *storage.ArtifactRef) error {
	if ref == nil {
		return nil
	}
	if ref.Path == "" {
		return fmt.Errorf("empty path")
	}
	return nil
}

func setArtifactPath(ref *storage.ArtifactRef, path string) {
	if ref != nil {
		ref.Path = path
	}
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

func flushBlockPart(imported *Imported, part *blockParts, stats *ImportStats) error {
	if part.savedFull || len(part.block) == 0 {
		return nil
	}

	full := &storage.ServedBlockFull{
		ID:       part.id,
		Block:    part.block,
		BlockRef: part.blockRef,
		Meta:     &storage.BlockMeta{ID: part.id},
	}

	switch {
	case len(part.proof) > 0:
		full.Proof = part.proof
		full.ProofRef = part.proofRef
	case len(part.proofLink) > 0:
		full.Proof = part.proofLink
		full.ProofRef = part.linkRef
		full.IsLink = true
	default:
		return nil
	}

	imported.FullBlocks = append(imported.FullBlocks, full)
	part.savedFull = true
	stats.FullBlocks++
	return nil
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

func artifactRefFromEntry(path string, entry packfile.Entry) *storage.ArtifactRef {
	if entry.DataSize <= 0 {
		return nil
	}
	return &storage.ArtifactRef{
		Path:   path,
		Offset: entry.DataOffset,
		Size:   entry.DataSize,
	}
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
