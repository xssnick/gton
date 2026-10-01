package validator

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/simplex"
)

func TestCandidatePayloadValidationRootOwnership(t *testing.T) {
	fixture := loadReceiveFixture(t, fixtureFullCollated)
	sourceCells := candidateOwnershipCells(fixture.roots...)
	blockCells := candidateOwnershipCells(fixture.roots[0])
	wantBlock, wantCollated, err := receiveSerializeSerial(fixture, 0)
	if err != nil {
		t.Fatal(err)
	}

	for protocol := uint8(0); protocol <= simplex.MaxProtocolVersion; protocol++ {
		t.Run(fmt.Sprintf("protocol_%d", protocol), func(t *testing.T) {
			payload := mustFinishPayload(t, receiveFixtureCodec(t, protocol), fixture)
			if protocol == 1 {
				if payload.preparedBlock == nil || payload.blockRoot != payload.preparedBlock.Root() {
					t.Fatal("validation retained the combined arena instead of the prepared block root")
				}

				detachedCells := candidateOwnershipCells(payload.blockRoot)
				if len(detachedCells) != len(blockCells) {
					t.Fatal("validation block graph changed its reachable cell count")
				}
				for current := range detachedCells {
					if _, shared := sourceCells[current]; shared {
						t.Fatal("validation block retained a cell from the combined candidate arena")
					}
				}
			} else if payload.preparedBlock != nil || payload.blockRoot != fixture.roots[0] {
				t.Fatal("protocol without a prepared block changed its root ownership")
			}

			if len(payload.collatedRoots) != len(fixture.roots)-1 {
				t.Fatal("validation lost collated proof roots")
			}
			for i, root := range payload.collatedRoots {
				if root != fixture.roots[i+1] {
					t.Fatal("validation copied or replaced a collated proof root")
				}
			}

			if !bytes.Equal(payload.blockBOC, wantBlock) || !bytes.Equal(payload.collatedData, wantCollated) {
				t.Fatal("root ownership changed canonical candidate bytes")
			}
			if payload.rootHash != fixture.roots[0].HashKey() ||
				payload.fileHash != sha256.Sum256(wantBlock) ||
				payload.collatedFileHash != sha256.Sum256(wantCollated) {
				t.Fatal("root ownership changed candidate hashes")
			}
		})
	}
}

func TestCandidateCodecProtocolOneDetachedRootKeepsSignedIdentity(t *testing.T) {
	config, leaderKey := runtimeTestConfig(0x73, &runtimeTestJournal{})
	config.Protocol.ProtocolVersion = 1
	codec, err := newCandidateCodec(config, CandidateLimits{
		MaxBlockBytes:        1 << 20,
		MaxCollatedDataBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	artifact := runtimeOrdinaryArtifact(t, config, leaderKey, 0, simplex.Genesis())
	wire, err := simplex.SerializeCandidate(artifact.Candidate, artifact.BlockBOC, artifact.CollatedData)
	if err != nil {
		t.Fatal(err)
	}

	decoded, err := codec.decode(wire, &artifact.Candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertCandidateArtifactEqual(t, decoded, artifact)
	assertPreparedBlockRoute(t, 1, decoded)
	if decoded.validationRoots.block != decoded.preparedBlock.Root() {
		t.Fatal("decoded artifact lost the prepared block's independent root")
	}

	canonical, err := simplex.SerializeCandidate(decoded.Candidate, decoded.BlockBOC, decoded.CollatedData)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(canonical, wire) {
		t.Fatal("root detachment changed the signed canonical candidate wire")
	}
}

func candidateOwnershipCells(roots ...*cell.Cell) map[*cell.Cell]struct{} {
	seen := make(map[*cell.Cell]struct{})
	pending := append([]*cell.Cell(nil), roots...)
	for len(pending) > 0 {
		last := len(pending) - 1
		current := pending[last]
		pending = pending[:last]
		if _, exists := seen[current]; exists {
			continue
		}

		seen[current] = struct{}{}
		for i := 0; i < int(current.RefsNum()); i++ {
			pending = append(pending, current.MustPeekRef(i))
		}
	}

	return seen
}
