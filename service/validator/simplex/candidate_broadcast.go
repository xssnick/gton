package simplex

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/pierrec/lz4/v4"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	schemeValidatorSessionCompressedCandidate = "validatorSession.compressedCandidate flags:# src:int256 round:int root_hash:int256 " +
		"decompressed_size:int data:bytes = validatorSession.Candidate"
	schemeValidatorSessionCompressedCandidateV2 = "validatorSession.compressedCandidateV2 flags:# src:int256 round:int " +
		"root_hash:int256 data:bytes = validatorSession.Candidate"
)

// ValidatorSessionCompressedCandidate is the LZ4 candidate payload currently
// carried inside consensus.block.
type ValidatorSessionCompressedCandidate struct {
	Flags            int32  `tl:"int"`
	Source           []byte `tl:"int256"`
	Round            int32  `tl:"int"`
	RootHash         []byte `tl:"int256"`
	DecompressedSize int32  `tl:"int"`
	Data             []byte `tl:"bytes"`
}

// ValidatorSessionCompressedCandidateV2 is the improved BOC-compression form
// accepted by the current reference decoder. Candidate::serialize still emits
// the baseline form above, so this type is decode-only in consensus storage.
type ValidatorSessionCompressedCandidateV2 struct {
	Flags    int32  `tl:"int"`
	Source   []byte `tl:"int256"`
	Round    int32  `tl:"int"`
	RootHash []byte `tl:"int256"`
	Data     []byte `tl:"bytes"`
}

// CandidateBroadcast is the payload pair passed to an overlay FEC broadcast.
// Data is a bare boxed consensus.block or consensus.empty; Extra carries the
// slot and optional delegation.
type CandidateBroadcast struct {
	Data  []byte
	Extra []byte
}

// CandidateWire derives the resolver/storage representation from this FEC
// payload without decoding Data. For a non-delegated candidate the returned
// slice aliases Data; both are immutable protocol buffers.
func (b CandidateBroadcast) CandidateWire(delegation *Delegation) ([]byte, error) {
	return wrapCandidateData(b.Data, delegation)
}

func init() {
	tl.Register(ValidatorSessionCompressedCandidate{}, schemeValidatorSessionCompressedCandidate)
	tl.Register(ValidatorSessionCompressedCandidateV2{}, schemeValidatorSessionCompressedCandidateV2)
}

// SerializeCandidateForBroadcast builds the exact candidate payload the private
// overlay carries. FEC framing and source authentication are
// transport responsibilities and are deliberately not part of this function.
func SerializeCandidateForBroadcast(
	candidate Candidate,
	blockBOC []byte,
	collatedData []byte,
) (CandidateBroadcast, error) {
	wire, err := serializeCandidateData(candidate, blockBOC, collatedData)
	if err != nil {
		return CandidateBroadcast{}, err
	}

	return candidateBroadcast(candidate, wire)
}

// SerializeCandidateForBroadcastPrepared is SerializeCandidateForBroadcast for
// a producer that built the payload from its own roots. The wire it emits is
// the same bytes; only the way the payload was reached differs.
func SerializeCandidateForBroadcastPrepared(
	candidate Candidate,
	prepared *PreparedCandidate,
) (CandidateBroadcast, error) {
	wire, err := serializeCandidateDataPrepared(candidate, prepared)
	if err != nil {
		return CandidateBroadcast{}, err
	}

	return candidateBroadcast(candidate, wire)
}

func candidateBroadcast(candidate Candidate, wire []byte) (CandidateBroadcast, error) {
	extra, err := (&BroadcastExtra{Slot: candidate.ID.Slot, Delegation: candidate.Delegation}).Serialize()
	if err != nil {
		return CandidateBroadcast{}, err
	}

	return CandidateBroadcast{Data: wire, Extra: extra}, nil
}

// SerializeCandidate builds the candidate serialization. Unlike the FEC
// payload, delegated candidates carry consensus.candidate around the
// bare block/empty data. The already-serialized data is wrapped directly, so
// candidate-sized byte fields are never parsed and copied a second time.
func SerializeCandidate(candidate Candidate, blockBOC, collatedData []byte) ([]byte, error) {
	wire, err := serializeCandidateData(candidate, blockBOC, collatedData)
	if err != nil {
		return nil, err
	}

	return wrapCandidateData(wire, candidate.Delegation)
}

// SerializeCandidatePrepared is SerializeCandidate for a producer that built
// the payload from its own roots.
func SerializeCandidatePrepared(candidate Candidate, prepared *PreparedCandidate) ([]byte, error) {
	wire, err := serializeCandidateDataPrepared(candidate, prepared)
	if err != nil {
		return nil, err
	}

	return wrapCandidateData(wire, candidate.Delegation)
}

func wrapCandidateData(wire []byte, candidateDelegation *Delegation) ([]byte, error) {
	if candidateDelegation == nil {
		return wire, nil
	}
	delegation, err := tl.Serialize(delegationToTL(candidateDelegation), true)
	if err != nil {
		return nil, fmt.Errorf("simplex: serialize candidate delegation: %w", err)
	}

	result := make([]byte, 8, 8+len(wire)+len(delegation))
	binary.LittleEndian.PutUint32(result[:4], idCandidateWrapped)
	binary.LittleEndian.PutUint32(result[4:], 1)
	result = append(result, wire...)
	result = append(result, delegation...)

	return result, nil
}

func serializeCandidateData(candidate Candidate, blockBOC, collatedData []byte) ([]byte, error) {
	if err := validateCandidateIdentity(candidate); err != nil {
		return nil, err
	}
	if candidate.Empty {
		if len(blockBOC) != 0 || len(collatedData) != 0 {
			return nil, fmt.Errorf("simplex: empty candidate carries block data")
		}

		return serializeCandidateWire(emptyCandidateData(candidate))
	}
	payload, err := serializeCandidatePayload(candidate, blockBOC, collatedData)
	if err != nil {
		return nil, err
	}

	return serializeCandidateWire(blockCandidateData(candidate, payload))
}

func serializeCandidateDataPrepared(candidate Candidate, prepared *PreparedCandidate) ([]byte, error) {
	if err := validateCandidateIdentity(candidate); err != nil {
		return nil, err
	}
	if prepared == nil {
		return nil, fmt.Errorf("simplex: prepared candidate payload is absent")
	}
	payload, err := prepared.payloadFor(candidate)
	if err != nil {
		return nil, err
	}

	return serializeCandidateWire(blockCandidateData(candidate, payload))
}

func validateCandidateIdentity(candidate Candidate) error {
	if err := candidate.ValidateShape(); err != nil {
		return err
	}
	if candidate.ComputeID(candidate.ID.Slot) != candidate.ID {
		return fmt.Errorf("simplex: candidate ID does not match candidate data")
	}

	return nil
}

func emptyCandidateData(candidate Candidate) ConsensusEmptyData {
	return ConsensusEmptyData{
		Slot:      int32(candidate.ID.Slot),
		Parent:    tlCandidateID(candidate.Parent.ID),
		Block:     candidate.Block,
		Signature: candidate.Signature,
	}
}

func blockCandidateData(candidate Candidate, payload []byte) ConsensusBlockData {
	return ConsensusBlockData{
		Slot:      int32(candidate.ID.Slot),
		Parent:    parentToTL(candidate.Parent),
		Candidate: payload,
		Signature: candidate.Signature,
	}
}

func serializeCandidateWire(data any) ([]byte, error) {
	wire, err := tl.Serialize(data, true)
	if err != nil {
		return nil, fmt.Errorf("simplex: serialize candidate data: %w", err)
	}

	return wire, nil
}

func serializeCandidatePayload(candidate Candidate, blockBOC, collatedData []byte) ([]byte, error) {
	blockRoot, err := cell.FromBOC(blockBOC)
	if err != nil {
		return nil, fmt.Errorf("simplex: parse candidate block BOC: %w", err)
	}
	// The reference std_boc_serialize(root, 31) does not mark the root for a
	// top hash. Keep WithTopHash disabled to compare the actual wire bytes.
	canonicalBlock, err := isCanonicalBOC([]*cell.Cell{blockRoot}, blockBOC, cell.BOCSerializeOptions{
		WithCRC32C:    true,
		WithIndex:     true,
		WithCacheBits: true,
		WithIntHashes: true,
	})
	if err != nil {
		return nil, fmt.Errorf("simplex: serialize canonical block BOC: %w", err)
	}
	if !canonicalBlock {
		return nil, fmt.Errorf("simplex: candidate block BOC is not canonical mode 31")
	}
	if !bytes.Equal(blockRoot.Hash(), candidate.Block.RootHash) {
		return nil, fmt.Errorf("simplex: candidate block root hash does not match block BOC")
	}

	collatedRoots, err := cell.FromBOCMultiRoot(collatedData)
	if err != nil {
		return nil, fmt.Errorf("simplex: parse candidate collated data: %w", err)
	}
	canonicalCollated, err := isCanonicalBOC(
		collatedRoots,
		collatedData,
		cell.BOCSerializeOptions{WithCRC32C: true},
	)
	if err != nil {
		return nil, fmt.Errorf("simplex: serialize canonical collated data: %w", err)
	}
	if !canonicalCollated {
		return nil, fmt.Errorf("simplex: candidate collated data is not canonical mode 2")
	}

	fileHash := sha256.Sum256(blockBOC)
	if !bytes.Equal(fileHash[:], candidate.Block.FileHash) {
		return nil, fmt.Errorf("simplex: candidate file hash does not match block BOC")
	}
	if sha256.Sum256(collatedData) != candidate.CollatedFileHash {
		return nil, fmt.Errorf("simplex: collated file hash does not match collated data")
	}

	roots := make([]*cell.Cell, 1, len(collatedRoots)+1)
	roots[0] = blockRoot
	roots = append(roots, collatedRoots...)

	return compressCandidatePayload(
		candidate.Block.SeqNo,
		candidate.Block.RootHash,
		roots,
		PayloadCellHint(blockBOC, collatedData),
	)
}

// compressCandidatePayload is the reference wire algorithm of
// compress_candidate_data (payload.cpp): one mode-2 BOC over the block root
// followed by the collated roots, raw LZ4 over it, wrapped in
// validatorSession.compressedCandidate. roots carries that order already.
// cellsHint presizes the serializer's dedup structures and changes no byte of
// the output; see PayloadCellHint for where it comes from and what bounds it.
func compressCandidatePayload(seqNo uint32, rootHash []byte, roots []*cell.Cell, cellsHint int) ([]byte, error) {
	combined, err := cell.ToBOCWithOptionsErr(roots, cell.BOCSerializeOptions{
		WithCRC32C:     true,
		CellsCountHint: cellsHint,
	})
	if err != nil {
		return nil, fmt.Errorf("simplex: serialize combined candidate BOC: %w", err)
	}
	if len(combined) > math.MaxInt32 {
		return nil, fmt.Errorf("simplex: combined candidate BOC is too large")
	}

	compressed := make([]byte, lz4.CompressBlockBound(len(combined)))
	compressedSize, err := lz4.CompressBlock(combined, compressed, nil)
	if err != nil {
		return nil, fmt.Errorf("simplex: compress candidate BOC: %w", err)
	}
	compressed = compressed[:compressedSize]

	payload, err := tl.Serialize(ValidatorSessionCompressedCandidate{
		Source:           make([]byte, 32),
		Round:            int32(seqNo),
		RootHash:         rootHash,
		DecompressedSize: int32(len(combined)),
		Data:             compressed,
	}, true)
	if err != nil {
		return nil, fmt.Errorf("simplex: serialize compressed candidate: %w", err)
	}

	return payload, nil
}

type bocEqualityWriter struct {
	expected []byte
	offset   int
	equal    bool
}

func (w *bocEqualityWriter) Write(data []byte) (int, error) {
	end := w.offset + len(data)
	if end > len(w.expected) || (w.equal && !bytes.Equal(data, w.expected[w.offset:end])) {
		w.equal = false
	}
	w.offset = end
	return len(data), nil
}

func isCanonicalBOC(
	roots []*cell.Cell,
	expected []byte,
	options cell.BOCSerializeOptions,
) (bool, error) {
	writer := bocEqualityWriter{expected: expected, equal: true}
	if err := cell.WriteBOCWithOptions(&writer, roots, options); err != nil {
		return false, err
	}
	return writer.equal && writer.offset == len(expected), nil
}
