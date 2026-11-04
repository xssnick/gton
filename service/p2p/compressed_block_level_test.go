package p2p

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"

	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestCompressedBlockDecodersRejectNonzeroRootLevel(t *testing.T) {
	base := testPeerBlockRoot(t, 0, 17)
	var refs [4]*cell.Cell
	for i := range refs {
		refs[i] = base.MustPeekRef(i)
	}
	var err error
	refs[3], err = cell.CreatePrunedBranch(refs[3], 3, 0)
	if err != nil {
		t.Fatal(err)
	}
	root, err := base.RebuildWithRefs(refs[:])
	if err != nil {
		t.Fatal(err)
	}
	if root.Level() == 0 {
		t.Fatal("fixture has a level-zero root")
	}
	id := ton.BlockIDExt{
		Workchain: 0,
		Shard:     topShard,
		SeqNo:     17,
		RootHash:  root.Hash(),
		FileHash:  hashSimpleBroadcastPayload(serializeCompressedBlockRoot(root)),
	}
	proof := testBlockProofCell(t, id, nil)
	for _, algorithm := range []cell.CompressionAlgorithm{cell.CompressionBaselineLZ4, cell.CompressionImprovedStructureLZ4} {
		compressed, err := cell.CompressBOC([]*cell.Cell{root}, algorithm, nil)
		if err != nil {
			t.Fatal(err)
		}
		download := DataFullCompressedV2{ID: id, Proof: proof.ToBOC(), BlockCompressed: compressed, IsLink: true}
		tests := []struct {
			name   string
			decode func() error
		}{
			{"download", func() error {
				_, err := decodeCompressedBlockV2WithProofRoot(download, nil, proof)
				return err
			}},
			{"hardfork download", func() error {
				_, err := decodeCompressedBlockV2WithProofRootForHardfork(download, nil, proof, errors.New("initial decode failed"))
				return err
			}},
			{"candidate broadcast", func() error {
				_, err := decodeBlockCandidateBroadcast(tonnodeapi.NewBlockCandidateBroadcastCompressedV2{ID: id, Compressed: compressed})
				return err
			}},
			{"block broadcast", func() error {
				_, _, err := decodeBlockBroadcastCompressedV2WithProofRoot(tonnodeapi.BlockBroadcastCompressedV2{
					ID: id,
					SignatureSet: tonnodeapi.SignatureSetSimplex{
						SessionID: bytes.Repeat([]byte{0x44}, 32),
						Candidate: ton.ConsensusCandidateHashDataOrdinary{
							Block: id, CollatedFileHash: make([]byte, 32), Parent: ton.ConsensusCandidateWithoutParents{},
						},
					},
					Proof: proof.ToBOC(), DataCompressed: compressed,
				}, nil, proof, nil)
				return err
			}},
		}
		for _, test := range tests {
			t.Run(fmt.Sprintf("algorithm_%d/%s", algorithm, test.name), func(t *testing.T) {
				if err := test.decode(); err == nil || !strings.Contains(err.Error(), "root has non-zero level") {
					t.Fatalf("error = %v, want non-zero block root rejection", err)
				}
			})
		}
	}
}

func TestDownloadedMerkleProofLinkKeepsLevelZeroBlock(t *testing.T) {
	root := testPeerBlockRoot(t, 0, 19)
	proof := testPeerFullMerkleProof(t, root)
	id := ton.BlockIDExt{Workchain: 0, Shard: topShard, SeqNo: 19, RootHash: root.Hash(), FileHash: make([]byte, 32)}
	got, err := effectiveDownloadedBlockRoot(id, true, proof)
	if err != nil {
		t.Fatal(err)
	}
	if got.Level() != 0 || got.HashKey() != root.HashKey() {
		t.Fatal("valid level-zero Merkle proof link did not unwrap to its block")
	}
}
