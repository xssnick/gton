package validator

import (
	"crypto/sha256"
	"testing"

	"github.com/pierrec/lz4/v4"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/simplex"
)

// receiveCompressedCandidate is the exact byte string decodePayload receives
// for a candidate carrying the mainnet fixture: the block and collated roots
// serialized in their canonical modes, combined, LZ4-compressed and framed as
// validatorSession.compressedCandidate by the same serializer the collator
// uses. The bare consensus.block data it sits in is what the private overlay
// delivers to decodeBroadcast.
type receiveCompressedCandidate struct {
	// broadcast is the bare consensus.block data of the private overlay.
	broadcast []byte
	// payload is the compressed candidate inside it, decodePayload's input.
	payload []byte
	// compressed is the LZ4 block inside that, decompressedSize its output.
	compressed       []byte
	decompressedSize int
}

func receiveFixtureCompressed(tb testing.TB, fixture receiveFixture) receiveCompressedCandidate {
	tb.Helper()

	blockBOC, err := receiveBlockBOC(fixture.roots[0], 0)
	if err != nil {
		tb.Fatal(err)
	}
	collatedData, err := receiveCollatedBOC(fixture.roots[1:], 0)
	if err != nil {
		tb.Fatal(err)
	}
	// Signed by the leader of receiveFixtureCodec's session, so the production
	// receive entry point accepts it: the config is a pure function of its seed.
	config, leaderKey := runtimeTestConfig(0x71, &runtimeTestJournal{})
	fileHash := sha256.Sum256(blockBOC)
	candidate := simplex.Candidate{
		Parent: simplex.Genesis(),
		Block: ton.BlockIDExt{
			Workchain: config.Shard.Workchain,
			Shard:     config.Shard.Shard,
			SeqNo:     receiveFixtureBlockSeq,
			RootHash:  fixture.rootHash,
			FileHash:  fileHash[:],
		},
		CollatedFileHash: sha256.Sum256(collatedData),
	}
	candidate.ID = candidate.ComputeID(0)
	candidate.Signature, err = simplex.SignCandidate(
		runtimeTestSigner{key: leaderKey},
		config.SessionID,
		candidate.ID,
	)
	if err != nil {
		tb.Fatal(err)
	}
	broadcast, err := simplex.SerializeCandidateForBroadcast(candidate, blockBOC, collatedData)
	if err != nil {
		tb.Fatal(err)
	}
	block := candidateBroadcastBlockData(tb, broadcast.Data)

	var compressed tl.Serializable
	if _, err = tl.ParseNoCopy(&compressed, block.Candidate, true); err != nil {
		tb.Fatal(err)
	}
	frame, ok := compressed.(simplex.ValidatorSessionCompressedCandidate)
	if !ok {
		tb.Fatalf("compressed candidate frame is %T", compressed)
	}

	return receiveCompressedCandidate{
		broadcast:        broadcast.Data,
		payload:          block.Candidate,
		compressed:       frame.Data,
		decompressedSize: int(frame.DecompressedSize),
	}
}

// BenchmarkCandidateDecodePayload is the receive-side decode of a mainnet
// candidate, broken into the stages the path pays in order — the two TL frame
// parses as the copying API measures them, the LZ4 pass, the combined-BOC
// parse — then decodePayload whole, and finally the production entry point
// ReceiveCandidate takes, decodeBroadcastDeferred, whose frame parse does not
// copy. Every stage arm starts from the bytes the previous one produced. Run
// with -benchmem.
func BenchmarkCandidateDecodePayload(b *testing.B) {
	for _, shape := range []struct {
		name  string
		shape receiveFixtureShape
	}{{"full_collated", fixtureFullCollated}, {"marker_collated", fixtureMarkerCollated}} {
		fixture := loadReceiveFixture(b, shape.shape)
		compressed := receiveFixtureCompressed(b, fixture)
		decompressed := make([]byte, compressed.decompressedSize)
		if _, err := lz4.UncompressBlock(compressed.compressed, decompressed); err != nil {
			b.Fatal(err)
		}

		b.Run(shape.name+"/parse_broadcast_frame", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := simplex.ParseCandidateData(compressed.broadcast); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(shape.name+"/parse_compressed_frame", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				var payload tl.Serializable
				if _, err := tl.Parse(&payload, compressed.payload, true); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(shape.name+"/lz4", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				out := make([]byte, compressed.decompressedSize)
				if _, err := lz4.UncompressBlock(compressed.compressed, out); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(shape.name+"/parse_boc", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := cell.FromBOCMultiRootWithOptions(decompressed, cell.BOCParseOptions{
					NoCopyPayload: true,
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
		for _, protocolVersion := range []uint8{2, 1} {
			codec := receiveFixtureCodec(b, protocolVersion)
			name := shape.name + "/" + map[uint8]string{1: "protocol1", 2: "protocol2"}[protocolVersion]
			b.Run(name+"/decode_payload", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if _, err := codec.decodePayload(compressed.payload); err != nil {
						b.Fatal(err)
					}
				}
			})
			// The production receive path: what ReceiveCandidate runs on the
			// bare frame the private overlay delivers, signature check included.
			b.Run(name+"/decode_broadcast", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if _, _, err := codec.decodeBroadcastDeferred(compressed.broadcast, nil, 0); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
