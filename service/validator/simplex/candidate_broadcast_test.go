package simplex

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/pierrec/lz4/v4"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestSerializeCandidateForBroadcastMatchesReferenceOrdinaryPayload(t *testing.T) {
	blockRoot := cell.BeginCell().MustStoreUInt(0x1234, 16).EndCell()
	collatedRoot := cell.BeginCell().MustStoreUInt(0x5678, 16).EndCell()
	blockBOC, err := blockRoot.ToBOCWithOptionsErr(cell.BOCSerializeOptions{
		WithCRC32C:    true,
		WithIndex:     true,
		WithCacheBits: true,
		WithIntHashes: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	collatedData, err := cell.ToBOCWithOptionsErr(
		[]*cell.Cell{collatedRoot},
		cell.BOCSerializeOptions{WithCRC32C: true},
	)
	if err != nil {
		t.Fatal(err)
	}

	fileHash := sha256.Sum256(blockBOC)
	collatedHash := sha256.Sum256(collatedData)
	rootHash := blockRoot.Hash()
	candidate := Candidate{
		Parent: Parent(CandidateID{Slot: 4, Hash: [32]byte{0x44}}),
		Leader: 2,
		Block: ton.BlockIDExt{
			Workchain: 0,
			Shard:     -1 << 63,
			SeqNo:     19,
			RootHash:  bytes.Clone(rootHash),
			FileHash:  fileHash[:],
		},
		CollatedFileHash: collatedHash,
		Signature:        bytes.Repeat([]byte{0x55}, ed25519.SignatureSize),
		Delegation: &Delegation{
			CollatorKey: bytes.Repeat([]byte{0x66}, ed25519.PublicKeySize),
			Signature:   bytes.Repeat([]byte{0x77}, ed25519.SignatureSize),
		},
	}
	candidate.ID = candidate.ComputeID(8)

	broadcast, err := SerializeCandidateForBroadcast(candidate, blockBOC, collatedData)
	if err != nil {
		t.Fatal(err)
	}
	if got := binary.LittleEndian.Uint32(broadcast.Data); got != tl.CRC(schemeCandidateBlock) {
		t.Fatalf("broadcast data constructor = %#x, want bare consensus.block", got)
	}
	if got := binary.LittleEndian.Uint32(broadcast.Data); got == idCandidateWrapped {
		t.Fatal("FEC broadcast used the resolver-only consensus.candidate wrapper")
	}

	var block ConsensusBlockData
	rest, err := tl.Parse(&block, broadcast.Data, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(rest) != 0 || block.Slot != int32(candidate.ID.Slot) ||
		!bytes.Equal(block.Signature, candidate.Signature) {
		t.Fatalf("unexpected consensus.block: %+v trailing=%d", block, len(rest))
	}
	parent, ok := block.Parent.(ton.ConsensusCandidateParent)
	if !ok {
		parentPtr, pointerOK := block.Parent.(*ton.ConsensusCandidateParent)
		if !pointerOK {
			t.Fatalf("parent type = %T", block.Parent)
		}
		parent = *parentPtr
	}
	parentID, ok := parent.ID.(ton.ConsensusCandidateID)
	if !ok {
		parentIDPtr, pointerOK := parent.ID.(*ton.ConsensusCandidateID)
		if !pointerOK {
			t.Fatalf("parent ID type = %T", parent.ID)
		}
		parentID = *parentIDPtr
	}
	if parentID.Slot != int32(candidate.Parent.ID.Slot) || !bytes.Equal(parentID.Hash, candidate.Parent.ID.Hash[:]) {
		t.Fatalf("parent = %+v", parentID)
	}

	if got := binary.LittleEndian.Uint32(block.Candidate); got != 0x4212c777 {
		t.Fatalf("compressed candidate constructor = %#x, want 0x4212c777", got)
	}
	var compressed ValidatorSessionCompressedCandidate
	rest, err = tl.Parse(&compressed, block.Candidate, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(rest) != 0 || compressed.Flags != 0 || !allZero(compressed.Source) ||
		compressed.Round != int32(candidate.Block.SeqNo) ||
		!bytes.Equal(compressed.RootHash, candidate.Block.RootHash) {
		t.Fatalf("unexpected compressed candidate: %+v trailing=%d", compressed, len(rest))
	}

	combined, err := cell.ToBOCWithOptionsErr(
		[]*cell.Cell{blockRoot, collatedRoot},
		cell.BOCSerializeOptions{WithCRC32C: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	if compressed.DecompressedSize != int32(len(combined)) {
		t.Fatalf("decompressed size = %d, want %d", compressed.DecompressedSize, len(combined))
	}
	decompressed := make([]byte, compressed.DecompressedSize)
	n, err := lz4.UncompressBlock(compressed.Data, decompressed)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(decompressed) || !bytes.Equal(decompressed, combined) {
		t.Fatal("compressed candidate does not contain the mode-2 combined multi-root BOC")
	}

	extra, err := ParseBroadcastExtra(broadcast.Extra)
	if err != nil {
		t.Fatal(err)
	}
	if extra.Slot != candidate.ID.Slot || extra.Delegation == nil ||
		!bytes.Equal(extra.Delegation.CollatorKey, candidate.Delegation.CollatorKey) ||
		!bytes.Equal(extra.Delegation.Signature, candidate.Delegation.Signature) {
		t.Fatalf("broadcast extra = %+v", extra)
	}

	wire, err := SerializeCandidate(candidate, blockBOC, collatedData)
	if err != nil {
		t.Fatal(err)
	}
	if got := binary.LittleEndian.Uint32(wire); got != idCandidateWrapped {
		t.Fatalf("serialized delegated candidate constructor = %#x, want consensus.candidate", got)
	}
	wrapped, err := ParseCandidateWrapped(wire)
	if err != nil {
		t.Fatal(err)
	}
	if wrapped.Delegation == nil ||
		!bytes.Equal(wrapped.Delegation.CollatorKey, candidate.Delegation.CollatorKey) ||
		!bytes.Equal(wrapped.Delegation.Signature, candidate.Delegation.Signature) {
		t.Fatalf("serialized candidate delegation = %+v", wrapped.Delegation)
	}

	plain := candidate
	plain.Delegation = nil
	plainWire, err := SerializeCandidate(plain, blockBOC, collatedData)
	if err != nil {
		t.Fatal(err)
	}
	plainBroadcast, err := SerializeCandidateForBroadcast(plain, blockBOC, collatedData)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(plainWire, plainBroadcast.Data) {
		t.Fatal("non-delegated Candidate::serialize differs from bare FEC payload")
	}
}

func TestSerializeCandidateForBroadcastMatchesReferenceEmptyPayload(t *testing.T) {
	candidate := Candidate{
		Parent: Parent(CandidateID{Slot: 10, Hash: [32]byte{0x11}}),
		Leader: 1,
		Empty:  true,
		Block: ton.BlockIDExt{
			Workchain: 0,
			Shard:     -1 << 63,
			SeqNo:     7,
			RootHash:  bytes.Repeat([]byte{1}, 32),
			FileHash:  bytes.Repeat([]byte{2}, 32),
		},
		Signature: bytes.Repeat([]byte{3}, ed25519.SignatureSize),
	}
	candidate.ID = candidate.ComputeID(11)

	broadcast, err := SerializeCandidateForBroadcast(candidate, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := binary.LittleEndian.Uint32(broadcast.Data); got != tl.CRC(schemeCandidateEmpty) {
		t.Fatalf("broadcast data constructor = %#x, want bare consensus.empty", got)
	}
	var empty ConsensusEmptyData
	rest, err := tl.Parse(&empty, broadcast.Data, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(rest) != 0 || empty.Slot != int32(candidate.ID.Slot) || empty.Block.SeqNo != candidate.Block.SeqNo {
		t.Fatalf("unexpected consensus.empty: %+v trailing=%d", empty, len(rest))
	}
	if got := binary.LittleEndian.Uint32(broadcast.Extra); got != idBroadcastExtraLegacy {
		t.Fatalf("plain broadcast extra constructor = %#x, want %#x", got, idBroadcastExtraLegacy)
	}

	if _, err = SerializeCandidateForBroadcast(candidate, []byte{1}, nil); err == nil ||
		!strings.Contains(err.Error(), "empty candidate carries block data") {
		t.Fatalf("empty candidate with payload error = %v", err)
	}
}

func TestSerializeCandidateForBroadcastRejectsInconsistentArtifact(t *testing.T) {
	blockRoot := cell.BeginCell().MustStoreUInt(1, 1).EndCell()
	blockBOC, err := blockRoot.ToBOCWithOptionsErr(cell.BOCSerializeOptions{
		WithCRC32C:    true,
		WithIndex:     true,
		WithCacheBits: true,
		WithIntHashes: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	collatedData, err := cell.ToBOCWithOptionsErr(
		[]*cell.Cell{cell.BeginCell().EndCell()},
		cell.BOCSerializeOptions{WithCRC32C: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	fileHash := sha256.Sum256(blockBOC)
	collatedHash := sha256.Sum256(collatedData)
	candidate := Candidate{
		Block: ton.BlockIDExt{
			Workchain: 0,
			Shard:     -1 << 63,
			SeqNo:     1,
			RootHash:  bytes.Clone(blockRoot.Hash()),
			FileHash:  fileHash[:],
		},
		CollatedFileHash: collatedHash,
	}
	candidate.ID = candidate.ComputeID(1)

	wrongID := candidate
	wrongID.ID.Hash[0] ^= 0xff
	if _, err = SerializeCandidateForBroadcast(wrongID, blockBOC, collatedData); err == nil ||
		!strings.Contains(err.Error(), "candidate ID does not match") {
		t.Fatalf("candidate ID mismatch error = %v", err)
	}

	wrongRoot := candidate
	wrongRoot.Block.RootHash = bytes.Repeat([]byte{0xff}, 32)
	wrongRoot.ID = wrongRoot.ComputeID(candidate.ID.Slot)
	if _, err = SerializeCandidateForBroadcast(wrongRoot, blockBOC, collatedData); err == nil ||
		!strings.Contains(err.Error(), "root hash does not match") {
		t.Fatalf("candidate root mismatch error = %v", err)
	}

	nonCanonicalBlock, err := blockRoot.ToBOCWithOptionsErr(cell.BOCSerializeOptions{
		WithCRC32C:    true,
		WithIndex:     true,
		WithCacheBits: true,
		WithTopHash:   true,
		WithIntHashes: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(nonCanonicalBlock, blockBOC) {
		t.Fatal("block mode fixture is unexpectedly canonical")
	}
	wrongBlockMode := candidate
	nonCanonicalBlockHash := sha256.Sum256(nonCanonicalBlock)
	wrongBlockMode.Block.FileHash = nonCanonicalBlockHash[:]
	wrongBlockMode.ID = wrongBlockMode.ComputeID(candidate.ID.Slot)
	if _, err = SerializeCandidateForBroadcast(wrongBlockMode, nonCanonicalBlock, collatedData); err == nil ||
		!strings.Contains(err.Error(), "not canonical mode 31") {
		t.Fatalf("non-canonical block BOC error = %v", err)
	}

	collatedRoots, err := cell.FromBOCMultiRoot(collatedData)
	if err != nil {
		t.Fatal(err)
	}
	nonCanonicalCollated, err := cell.ToBOCWithOptionsErr(collatedRoots, cell.BOCSerializeOptions{
		WithCRC32C: true,
		WithIndex:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(nonCanonicalCollated, collatedData) {
		t.Fatal("collated-data mode fixture is unexpectedly canonical")
	}
	wrongCollatedMode := candidate
	wrongCollatedMode.CollatedFileHash = sha256.Sum256(nonCanonicalCollated)
	wrongCollatedMode.ID = wrongCollatedMode.ComputeID(candidate.ID.Slot)
	if _, err = SerializeCandidateForBroadcast(wrongCollatedMode, blockBOC, nonCanonicalCollated); err == nil ||
		!strings.Contains(err.Error(), "not canonical mode 2") {
		t.Fatalf("non-canonical collated data error = %v", err)
	}
}

func allZero(data []byte) bool {
	for _, value := range data {
		if value != 0 {
			return false
		}
	}
	return true
}
