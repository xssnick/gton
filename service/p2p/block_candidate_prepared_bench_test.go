package p2p

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"testing"

	tnstore "github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const protocol1CandidateBenchmarkFixturePath = "../validator/collator/testdata/tvm_replay_fat_block_66519406.json"

type protocol1CandidateBenchmarkFixture struct {
	Block struct {
		Workchain int32  `json:"workchain"`
		Shard     string `json:"shard"`
		SeqNo     uint32 `json:"seqno"`
	} `json:"block"`
	BlockBOCBase64 string `json:"block_boc_base64"`
}

// BenchmarkProtocol1CandidateCacheWorker compares both the complete path from
// an already decoded Protocol-1 root and its individual handoff/worker phases.
// The legacy path reparses and rehashes the canonical BOC before metadata; the
// prepared path first detaches the reachable graph, then reuses its sealed
// root/BOC/hash binding in the worker. The fixture is a real mainnet basechain
// block with 269 transactions.
func BenchmarkProtocol1CandidateCacheWorker(b *testing.B) {
	raw, err := os.ReadFile(protocol1CandidateBenchmarkFixturePath)
	if err != nil {
		b.Fatal(err)
	}

	var fixture protocol1CandidateBenchmarkFixture
	if err = json.Unmarshal(raw, &fixture); err != nil {
		b.Fatal(err)
	}
	blockBOC, err := base64.StdEncoding.DecodeString(fixture.BlockBOCBase64)
	if err != nil {
		b.Fatal(err)
	}
	root, err := cell.FromBOC(blockBOC)
	if err != nil {
		b.Fatal(err)
	}
	sourceRoot := root
	shard, err := strconv.ParseUint(fixture.Block.Shard, 16, 64)
	if err != nil {
		b.Fatal(err)
	}
	prepared, err := tnstore.PrepareBlockCandidate(
		fixture.Block.Workchain,
		int64(shard),
		fixture.Block.SeqNo,
		root,
	)
	if err != nil {
		b.Fatal(err)
	}
	id := prepared.ID()
	blockBOC = prepared.BlockBOC()
	root = prepared.Root()

	legacyDownloaded, err := decodeRawBlockCandidateBroadcast(trustedConsensusCandidateKind, id, blockBOC)
	if err != nil {
		b.Fatalf("verify legacy benchmark path: %v", err)
	}
	preparedDownloaded, err := newParsedBlockCandidateBroadcast(trustedConsensusCandidateKind, id, blockBOC, root)
	if err != nil {
		b.Fatalf("verify prepared benchmark path: %v", err)
	}
	if !legacyDownloaded.ID.Equals(&preparedDownloaded.ID) ||
		legacyDownloaded.Block.HashKey() != preparedDownloaded.Block.HashKey() ||
		legacyDownloaded.StateUpdate.HashKey() != preparedDownloaded.StateUpdate.HashKey() ||
		!reflect.DeepEqual(legacyDownloaded.Meta, preparedDownloaded.Meta) {
		b.Fatal("legacy and prepared candidate paths produced different block metadata")
	}
	if sourceRoot == root || sourceRoot.HashKey() != root.HashKey() {
		b.Fatal("prepared handoff did not create a hash-equivalent detached graph")
	}
	canonicalOptions := cell.BOCSerializeOptions{
		WithCRC32C:    true,
		WithIndex:     true,
		WithCacheBits: true,
		WithIntHashes: true,
	}

	b.Run("detach_reachable_graph", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(blockBOC)))

		for b.Loop() {
			detached, detachErr := sourceRoot.CloneDetached()
			if detachErr != nil {
				b.Fatal(detachErr)
			}
			runtime.KeepAlive(detached)
		}

		b.ReportMetric(float64(len(blockBOC)), "candidate-bytes/op")
	})

	b.Run("legacy_full_after_consensus_parse", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(blockBOC)))

		for b.Loop() {
			artifactBOC, serializeErr := sourceRoot.ToBOCWithOptionsErr(canonicalOptions)
			if serializeErr != nil {
				b.Fatal(serializeErr)
			}
			fileHash := sha256.Sum256(artifactBOC)
			rawID := id
			rawID.FileHash = fileHash[:]
			trusted, handoffErr := NewTrustedRawBlockCandidate(rawID, artifactBOC)
			if handoffErr != nil {
				b.Fatal(handoffErr)
			}
			downloaded, decodeErr := decodeRawBlockCandidateBroadcast(
				trustedConsensusCandidateKind,
				trusted.id,
				trusted.blockBOC,
			)
			if decodeErr != nil || downloaded == nil {
				b.Fatalf("legacy full candidate: downloaded=%v err=%v", downloaded != nil, decodeErr)
			}
			runtime.KeepAlive(artifactBOC)
		}

		b.ReportMetric(float64(len(blockBOC)), "candidate-bytes/op")
	})

	b.Run("prepared_full_after_consensus_parse", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(blockBOC)))

		for b.Loop() {
			capsule, prepareErr := tnstore.PrepareBlockCandidate(
				fixture.Block.Workchain,
				int64(shard),
				fixture.Block.SeqNo,
				sourceRoot,
			)
			if prepareErr != nil {
				b.Fatal(prepareErr)
			}
			artifactBOC := capsule.BlockBOC()
			trusted, handoffErr := NewTrustedBlockCandidate(capsule)
			if handoffErr != nil {
				b.Fatal(handoffErr)
			}
			downloaded, decodeErr := newParsedBlockCandidateBroadcast(
				trustedConsensusCandidateKind,
				trusted.id,
				trusted.blockBOC,
				trusted.root,
			)
			if decodeErr != nil || downloaded == nil {
				b.Fatalf("prepared full candidate: downloaded=%v err=%v", downloaded != nil, decodeErr)
			}
			runtime.KeepAlive(artifactBOC)
		}

		b.ReportMetric(float64(len(blockBOC)), "candidate-bytes/op")
	})

	b.Run("legacy_reparse", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(blockBOC)))

		for b.Loop() {
			downloaded, decodeErr := decodeRawBlockCandidateBroadcast(
				trustedConsensusCandidateKind,
				id,
				blockBOC,
			)
			if decodeErr != nil || downloaded == nil {
				b.Fatalf("decode candidate: downloaded=%v err=%v", downloaded != nil, decodeErr)
			}
		}

		b.ReportMetric(float64(len(blockBOC)), "candidate-bytes/op")
	})

	b.Run("prepared_metadata", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(blockBOC)))

		for b.Loop() {
			downloaded, decodeErr := newParsedBlockCandidateBroadcast(
				trustedConsensusCandidateKind,
				id,
				blockBOC,
				root,
			)
			if decodeErr != nil || downloaded == nil {
				b.Fatalf("parse candidate metadata: downloaded=%v err=%v", downloaded != nil, decodeErr)
			}
		}

		b.ReportMetric(float64(len(blockBOC)), "candidate-bytes/op")
	})

	b.Run("prepared_existing_capsule_handoff_metadata", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(blockBOC)))

		for b.Loop() {
			// finishPayload retains one owned BOC in CandidateArtifact while
			// NewTrustedBlockCandidate creates the publication queue's copy.
			// Keep both live through metadata parsing to model that ownership
			// overlap rather than letting the compiler discard the first copy.
			artifactBOC := prepared.BlockBOC()
			trusted, handoffErr := NewTrustedBlockCandidate(prepared)
			if handoffErr != nil {
				b.Fatal(handoffErr)
			}
			downloaded, decodeErr := newParsedBlockCandidateBroadcast(
				trustedConsensusCandidateKind,
				trusted.id,
				trusted.blockBOC,
				trusted.root,
			)
			if decodeErr != nil || downloaded == nil {
				b.Fatalf("handoff candidate: downloaded=%v err=%v", downloaded != nil, decodeErr)
			}
			runtime.KeepAlive(artifactBOC)
		}

		b.ReportMetric(float64(len(blockBOC)), "candidate-bytes/op")
		b.ReportMetric(float64(2*len(blockBOC)), "copied-bytes/op")
	})
}
