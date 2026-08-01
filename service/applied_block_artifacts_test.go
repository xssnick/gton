package service

import (
	"testing"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
)

func TestPreparedBlockCheckpointArtifactsCanonicalProofLinkFlag(t *testing.T) {
	tests := []struct {
		name       string
		block      PreparedBlock
		wantIsLink bool
	}{
		{
			name: "master full proof stays full proof",
			block: PreparedBlock{
				ID:       testBlockID(-1, topShard, 10),
				BlockBOC: []byte{0x01},
				ProofBOC: []byte{0x02},
				Meta:     &storage.BlockMeta{},
			},
		},
		{
			name: "shard full proof is served as proof link",
			block: PreparedBlock{
				ID:       testBlockID(0, topShard, 11),
				BlockBOC: []byte{0x03},
				ProofBOC: []byte{0x04},
				Meta:     &storage.BlockMeta{},
			},
			wantIsLink: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			full, _ := preparedBlockCheckpointArtifacts(tt.block, 0)
			if full.IsLink != tt.wantIsLink {
				t.Fatalf("is link = %v, want %v", full.IsLink, tt.wantIsLink)
			}
		})
	}
}

func TestPreparedBlockCheckpointArtifactsSkipsZeroStatePrevLink(t *testing.T) {
	zero := testBlockID(0, topShard, 0)
	prev := testBlockID(0, topShard, 12)
	block := testBlockID(0, topShard, 13)

	_, links := preparedBlockCheckpointArtifacts(PreparedBlock{
		ID:       block,
		BlockBOC: []byte{0x01},
		ProofBOC: []byte{0x02},
		Meta: &storage.BlockMeta{
			PrevRefs: []ton.BlockIDExt{zero, prev},
		},
	}, 0)
	if len(links) != 1 {
		t.Fatalf("links = %+v, want one non-zero prev link", links)
	}
	if !links[0].Prev.Equals(&prev) || !links[0].Next.Equals(&block) {
		t.Fatalf("link = %s -> %s, want %s -> %s", storage.FormatBlockRef(links[0].Prev), storage.FormatBlockRef(links[0].Next), storage.FormatBlockRef(prev), storage.FormatBlockRef(block))
	}
}

func TestCheckpointArtifactsShareImmutablePayload(t *testing.T) {
	blockData := []byte{0x01, 0x02, 0x03}
	proofData := []byte{0x04, 0x05}
	block := testBlockID(0, topShard, 20)
	state := &storage.BlockState{Block: block}

	artifact, _ := preparedBlockCheckpointArtifacts(PreparedBlock{
		ID:       block,
		BlockBOC: blockData,
		ProofBOC: proofData,
		Meta:     &storage.BlockMeta{},
		IsLink:   true,
	}, 2)
	if !sameByteBacking(artifact.Block, blockData) {
		t.Fatal("checkpoint artifact copied block payload")
	}
	if !sameByteBacking(artifact.Proof, proofData) {
		t.Fatal("checkpoint artifact copied proof payload")
	}

	var states appliedStateSet
	states.rememberWithArtifacts(state, artifact, nil)
	checkpoint := states.checkpoint()
	if len(checkpoint.entries) != 1 || checkpoint.entries[0].Artifact == nil {
		t.Fatalf("checkpoint entries = %+v, want one artifact", checkpoint.entries)
	}
	if !sameByteBacking(checkpoint.entries[0].Artifact.Block, blockData) {
		t.Fatal("applied state checkpoint copied block payload")
	}
	if !sameByteBacking(checkpoint.entries[0].Artifact.Proof, proofData) {
		t.Fatal("applied state checkpoint copied proof payload")
	}
}

func sameByteBacking(left []byte, right []byte) bool {
	if len(left) == 0 || len(right) == 0 {
		return len(left) == len(right)
	}
	return &left[0] == &right[0]
}
