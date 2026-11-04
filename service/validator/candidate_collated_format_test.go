package validator

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"

	"github.com/xssnick/gton/service/validator/simplex"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestCandidateGV16CollatedRoundTripAndColdRestore(t *testing.T) {
	config, key := runtimeTestConfig(0x79, &runtimeTestJournal{})
	artifact := runtimeOrdinaryArtifact(t, config, key, 0, simplex.Genesis())
	roots, err := cell.FromBOCMultiRoot(artifact.CollatedData)
	if err != nil {
		t.Fatal(err)
	}
	raw := candidateTestRawCollatedRoot(t)
	marker := cell.BeginCell().MustStoreUInt(0x4b2f36ec, 32).MustStoreSlice(raw.Hash(0), 256).EndCell()
	roots = append(roots, marker, cell.BeginCell().MustStoreUInt(collatedDataSeparatorTag, 32).EndCell(), raw)
	artifact.CollatedData, err = cell.ToBOCWithOptionsErr(roots, cell.BOCSerializeOptions{WithCRC32C: true})
	if err != nil {
		t.Fatal(err)
	}
	artifact.Candidate.CollatedFileHash = sha256.Sum256(artifact.CollatedData)
	artifact.Candidate.ID = artifact.Candidate.ComputeID(0)
	artifact.Candidate.Signature, err = simplex.SignCandidate(runtimeTestSigner{key: key}, config.SessionID, artifact.Candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := simplex.SerializeCandidate(artifact.Candidate, artifact.BlockBOC, artifact.CollatedData)
	if err != nil {
		t.Fatal(err)
	}
	broadcast, err := simplex.SerializeCandidateForBroadcast(artifact.Candidate, artifact.BlockBOC, artifact.CollatedData)
	if err != nil {
		t.Fatal(err)
	}
	block, err := cell.FromBOC(artifact.BlockBOC)
	if err != nil {
		t.Fatal(err)
	}
	combined := append([]*cell.Cell{block}, roots...)
	for _, encoding := range []string{"V1", "V2 baseline", "V2 structural"} {
		t.Run(encoding, func(t *testing.T) {
			payload := broadcast.Data
			if encoding != "V1" {
				algorithm := cell.CompressionBaselineLZ4
				if encoding == "V2 structural" {
					algorithm = cell.CompressionImprovedStructureLZ4
				}
				compressed, err := cell.CompressBOC(combined, algorithm, nil)
				if err != nil {
					t.Fatal(err)
				}
				data := candidateBroadcastBlockData(t, broadcast.Data)
				data.Candidate, err = tl.Serialize(simplex.ValidatorSessionCompressedCandidateV2{Source: make([]byte, 32), Round: int32(artifact.Candidate.Block.SeqNo), RootHash: artifact.Candidate.Block.RootHash, Data: compressed}, true)
				if err != nil {
					t.Fatal(err)
				}
				payload, err = tl.Serialize(data, true)
				if err != nil {
					t.Fatal(err)
				}
			}
			for protocol := uint8(0); protocol <= simplex.MaxProtocolVersion; protocol++ {
				t.Run(fmt.Sprintf("protocol_%d", protocol), func(t *testing.T) {
					cfg := config
					cfg.Protocol.ProtocolVersion = protocol
					codec, err := newCandidateCodec(cfg, CandidateLimits{MaxBlockBytes: 1 << 20, MaxCollatedDataBytes: 1 << 20})
					if err != nil {
						t.Fatal(err)
					}
					got, lazy, err := codec.decodeBroadcastDeferred(payload, nil, 0)
					if err != nil {
						t.Fatal(err)
					}
					assertCandidateArtifactEqual(t, got, artifact)
					lazy.releaseRoots()
					wire, hash, err := lazy.materialize()
					if err != nil {
						t.Fatal(err)
					}
					if !bytes.Equal(wire, canonical) || hash != sha256.Sum256(canonical) {
						t.Fatal("cold restore changed the canonical candidate")
					}
					restored, err := codec.decode(wire, &artifact.Candidate.ID)
					if err != nil {
						t.Fatal(err)
					}
					assertCandidateArtifactEqual(t, restored, artifact)
					if _, err = candidateGenUtime(got.CollatedData); err != nil {
						t.Fatalf("read restored timestamp: %v", err)
					}
				})
			}
		})
	}
}

func TestCandidateRejectsNonzeroBlockRoot(t *testing.T) {
	config, _ := runtimeTestConfig(0x7a, &runtimeTestJournal{})
	codec, err := newCandidateCodec(config, CandidateLimits{MaxBlockBytes: 1 << 20, MaxCollatedDataBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	raw := candidateTestRawCollatedRoot(t)
	_, err = codec.finishPayload(make([]byte, 32), 1, raw.Hash(), []*cell.Cell{raw}, 0)
	if err == nil || !strings.Contains(err.Error(), "block root has non-zero level") {
		t.Fatalf("error=%v", err)
	}
}

func TestCandidateGenerationTimeIgnoresRootsAfterSeparator(t *testing.T) {
	extra := cell.BeginCell().MustStoreUInt(consensusExtraDataTag, 32).MustStoreUInt(0, 32).MustStoreUInt(100_321, 64).EndCell()
	separator := cell.BeginCell().MustStoreUInt(collatedDataSeparatorTag, 32).EndCell()
	if _, err := candidateGenUtimeMSFromRoots([]*cell.Cell{separator, extra}); err == nil {
		t.Fatal("used a raw proof root as consensus metadata")
	}
}

func candidateTestRawCollatedRoot(t testing.TB) *cell.Cell {
	t.Helper()
	leaf := cell.BeginCell().MustStoreUInt(1, 8).EndCell()
	hidden := cell.BeginCell().MustStoreUInt(2, 8).MustStoreRef(leaf).EndCell()
	pruned, err := cell.CreatePrunedBranch(hidden, 3, 0)
	if err != nil {
		t.Fatal(err)
	}
	return cell.BeginCell().MustStoreUInt(3, 32).MustStoreRef(pruned).EndCell()
}
