package simplex

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

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

	// payload is the buffer Data was built in, when this broadcast came from a
	// local serialization rather than off the network. It is what lets the
	// delegation wrapper be written around Data instead of around a copy of it.
	// A broadcast assembled from received bytes leaves it nil and wraps the
	// copying way.
	payload *candidatePayload
}

// CandidateWire derives the resolver/storage representation from this FEC
// payload without decoding Data. For a non-delegated candidate the returned
// slice aliases Data; both are immutable protocol buffers.
func (b CandidateBroadcast) CandidateWire(delegation *Delegation) ([]byte, error) {
	return wrapCandidateFrame(b.Data, b.payload, delegation)
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
	wire, payload, err := serializeCandidateData(candidate, blockBOC, collatedData)
	if err != nil {
		return CandidateBroadcast{}, err
	}

	return candidateBroadcast(candidate, wire, payload)
}

// SerializeCandidateForBroadcastPrepared is SerializeCandidateForBroadcast for
// a producer that built the payload from its own roots. The wire it emits is
// the same bytes; only the way the payload was reached differs.
func SerializeCandidateForBroadcastPrepared(
	candidate Candidate,
	prepared *PreparedCandidate,
) (CandidateBroadcast, error) {
	wire, payload, err := serializeCandidateDataPrepared(candidate, prepared)
	if err != nil {
		return CandidateBroadcast{}, err
	}

	return candidateBroadcast(candidate, wire, payload)
}

func candidateBroadcast(candidate Candidate, wire []byte, payload *candidatePayload) (CandidateBroadcast, error) {
	extra, err := (&BroadcastExtra{Slot: candidate.ID.Slot, Delegation: candidate.Delegation}).Serialize()
	if err != nil {
		return CandidateBroadcast{}, err
	}

	return CandidateBroadcast{Data: wire, Extra: extra, payload: payload}, nil
}

// SerializeCandidate builds the candidate serialization. Unlike the FEC
// payload, delegated candidates carry consensus.candidate around the
// bare block/empty data. The already-serialized data is wrapped directly, so
// candidate-sized byte fields are never parsed and copied a second time.
func SerializeCandidate(candidate Candidate, blockBOC, collatedData []byte) ([]byte, error) {
	wire, payload, err := serializeCandidateData(candidate, blockBOC, collatedData)
	if err != nil {
		return nil, err
	}

	return wrapCandidateFrame(wire, payload, candidate.Delegation)
}

// SerializeCandidatePrepared is SerializeCandidate for a producer that built
// the payload from its own roots.
func SerializeCandidatePrepared(candidate Candidate, prepared *PreparedCandidate) ([]byte, error) {
	wire, payload, err := serializeCandidateDataPrepared(candidate, prepared)
	if err != nil {
		return nil, err
	}

	return wrapCandidateFrame(wire, payload, candidate.Delegation)
}

// wrapCandidateData wraps bytes this package did not lay out — a re-serialized
// candidate, or one rebuilt from a decoded wire — so a delegated wrap through it
// always copies. Callers holding the buffer the payload was compressed into go
// through wrapCandidateFrame and do not.
func wrapCandidateData(wire []byte, candidateDelegation *Delegation) ([]byte, error) {
	return wrapCandidateFrame(wire, nil, candidateDelegation)
}

func wrapCandidateFrame(wire []byte, payload *candidatePayload, candidateDelegation *Delegation) ([]byte, error) {
	if candidateDelegation == nil {
		return wire, nil
	}
	delegation, err := tl.Serialize(delegationToTL(candidateDelegation), true)
	if err != nil {
		return nil, fmt.Errorf("simplex: serialize candidate delegation: %w", err)
	}
	if wrapped, ok := payload.wrapInPlace(wire, delegation); ok {
		return wrapped, nil
	}

	result := make([]byte, 8, 8+len(wire)+len(delegation))
	binary.LittleEndian.PutUint32(result[:4], idCandidateWrapped)
	binary.LittleEndian.PutUint32(result[4:], 1)
	result = append(result, wire...)
	result = append(result, delegation...)

	return result, nil
}

func serializeCandidateData(candidate Candidate, blockBOC, collatedData []byte) ([]byte, *candidatePayload, error) {
	if err := validateCandidateIdentity(candidate); err != nil {
		return nil, nil, err
	}
	if candidate.Empty {
		if len(blockBOC) != 0 || len(collatedData) != 0 {
			return nil, nil, fmt.Errorf("simplex: empty candidate carries block data")
		}
		wire, err := serializeCandidateWire(emptyCandidateData(candidate))

		return wire, nil, err
	}
	payload, err := serializeCandidatePayload(candidate, blockBOC, collatedData)
	if err != nil {
		return nil, nil, err
	}
	wire, err := serializeBlockCandidateWire(candidate, payload)

	return wire, payload, err
}

func serializeCandidateDataPrepared(candidate Candidate, prepared *PreparedCandidate) ([]byte, *candidatePayload, error) {
	if err := validateCandidateIdentity(candidate); err != nil {
		return nil, nil, err
	}
	if prepared == nil {
		return nil, nil, fmt.Errorf("simplex: prepared candidate payload is absent")
	}
	payload, err := prepared.payloadFor(candidate)
	if err != nil {
		return nil, nil, err
	}
	wire, err := serializeBlockCandidateWire(candidate, payload)

	return wire, payload, err
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

// serializeBlockCandidateWire produces consensus.block around a payload the
// compressor already placed. The reflective encoder is the fallback and not the
// route: it appends the payload into a buffer of its own, which at mainnet size
// is a 0.7 MB memcpy plus a 1.4 MB allocation on the goroutine that emits the
// broadcast — the one place on this path the background capsule's head start
// does not reach.
func serializeBlockCandidateWire(candidate Candidate, payload *candidatePayload) ([]byte, error) {
	wire, ok, err := payload.wireInPlace(candidate)
	if err != nil {
		return nil, err
	}
	if ok {
		return wire, nil
	}

	return serializeCandidateWire(blockCandidateData(candidate, payload.bytes()))
}

const (
	// compressedCandidateHeaderBytes is validatorSession.compressedCandidate up
	// to but not including its data:bytes header: ctor, flags:#, src:int256,
	// round:int, root_hash:int256, decompressed_size:int.
	compressedCandidateHeaderBytes = 4 + 4 + 32 + 4 + 32 + 4

	// candidatePayloadHeadroom and candidatePayloadTailroom are the room left
	// around a compressed payload for the frames that enclose it. Both are
	// upper bounds, and both are checked against the frame actually being
	// written before a byte of it is: a frame that does not fit falls back to
	// the copying encoder rather than truncating or overrunning. The sums they
	// bound, at the widest shape the protocol has:
	//
	//	front  8 consensus.candidate wrapper
	//	      +4 consensus.block ctor +4 slot +44 boxed parent
	//	      +8 candidate:bytes header
	//	      +compressedCandidateHeaderBytes +8 data:bytes header  = 156
	//	back   3 candidate:bytes padding
	//	      +68 signature:bytes, 1+64+3 for an ed25519 signature
	//	      +108 consensus.delegation                             = 179
	//
	// A signature longer than ed25519's is not bounded by ValidateShape and is
	// simply refused the room, which is why the check is on the frame and the
	// constants are only sized for the shapes that occur.
	candidatePayloadHeadroom = 192
	candidatePayloadTailroom = 256
)

// candidatePayload is one compressed candidate payload sitting in the buffer
// the wire will be broadcast from, with the room its two enclosing frames need
// still free on either side of it.
//
// The reference nests the payload twice more — in consensus.block, and in
// consensus.candidate when the candidate was collated under a delegation — and
// a TL bytes field is appended rather than aliased (tonutils-go/tl/bytes.go),
// so each nesting used to memcpy the whole payload into a fresh buffer. At
// mainnet size that was 2.1 MB of allocation and two 0.7 MB copies after the
// compression was already finished, the outer one on the emission goroutine.
//
// The frames are written into the reserved room instead. They cannot be laid
// out before the payload exists: a TL bytes length is one byte below 254 and
// four above it, so where a frame starts depends on how well the block
// compressed. Hence room reserved at a bound, and each frame written backwards
// from where the payload already sits.
type candidatePayload struct {
	buf   []byte
	start int
	end   int

	// wireClaimed makes the consensus.block frame a one-shot, and frame both
	// publishes that frame's bounds to whoever wraps it and is consumed by that
	// wrap. Nothing in the tree serializes one payload twice today, but two
	// candidates may legally share one — bind ties a capsule to a block, not to
	// a slot, a parent or a signature — and a second frame written into the
	// same room would rewrite the first result under the caller holding it
	// instead of producing a second one. A losing claim falls back to the
	// copying encoder: the same bytes at the old cost.
	wireClaimed atomic.Bool
	frame       atomic.Pointer[candidateFrame]
}

// candidateFrame is where a built consensus.block sits inside buf.
type candidateFrame struct {
	start int
	end   int
}

// bytes is the payload alone, which is what the copying encoder wants.
func (p *candidatePayload) bytes() []byte {
	return p.buf[p.start:p.end:p.end]
}

// wireInPlace writes consensus.block around the payload and reports whether it
// could. It reports no error for a frame that does not fit or a payload already
// framed — those are the fallback, not a failure — and an error only for a
// candidate that cannot be serialized at all.
func (p *candidatePayload) wireInPlace(candidate Candidate) ([]byte, bool, error) {
	parent, err := tl.Serialize(parentToTL(candidate.Parent), true)
	if err != nil {
		return nil, false, fmt.Errorf("simplex: serialize candidate parent: %w", err)
	}
	payloadHeader, payloadPad, err := tlBytesFraming(p.end - p.start)
	if err != nil {
		return nil, false, fmt.Errorf("simplex: frame candidate payload: %w", err)
	}
	signatureHeader, signaturePad, err := tlBytesFraming(len(candidate.Signature))
	if err != nil {
		return nil, false, fmt.Errorf("simplex: frame candidate signature: %w", err)
	}

	start := p.start - (4 + 4 + len(parent) + payloadHeader)
	end := p.end + payloadPad + signatureHeader + len(candidate.Signature) + signaturePad
	// The wrapper needs its eight bytes in front of start, and the frame must
	// be claimed last so a losing claim leaves nothing half-written.
	if start < 8 || end > len(p.buf) || !p.wireClaimed.CompareAndSwap(false, true) {
		return nil, false, nil
	}

	at := start
	at = putUint32(p.buf, at, idConsensusBlock)
	at = putUint32(p.buf, at, candidate.ID.Slot)
	at += copy(p.buf[at:], parent)
	putTLBytesHeader(p.buf, at, p.end-p.start)

	at = p.end
	clear(p.buf[at : at+payloadPad])
	at += payloadPad
	at = putTLBytesHeader(p.buf, at, len(candidate.Signature))
	at += copy(p.buf[at:], candidate.Signature)
	clear(p.buf[at : at+signaturePad])

	p.frame.Store(&candidateFrame{start: start, end: end})

	// Capped to its own length: the frames sit in a buffer with room left over,
	// and a transport that appended to a wire whose capacity ran past its end
	// would write into the room the next frame is going to use — where the old
	// encoder, which returned a buffer grown to exactly its content, would have
	// reallocated instead.
	return p.buf[start:end:end], true, nil
}

// wrapInPlace writes the consensus.candidate wrapper around a frame this
// payload built. wire is compared against that frame rather than trusted: a
// payload paired with bytes it did not produce must wrap those bytes, not the
// ones it is holding.
func (p *candidatePayload) wrapInPlace(wire, delegation []byte) ([]byte, bool) {
	if p == nil {
		return nil, false
	}
	frame := p.frame.Load()
	if frame == nil {
		return nil, false
	}
	if len(wire) != frame.end-frame.start || &wire[0] != &p.buf[frame.start] {
		return nil, false
	}
	start, end := frame.start-8, frame.end+len(delegation)
	// Clearing the frame is what makes the wrapper a one-shot, for the same
	// reason wireClaimed makes the frame one: a second delegation goes into room
	// the first wrapper's result still points at. It is cleared last, so a wrap
	// this payload refuses leaves the frame for the one it will accept.
	if start < 0 || end > len(p.buf) || !p.frame.CompareAndSwap(frame, nil) {
		return nil, false
	}

	putUint32(p.buf, start, idCandidateWrapped)
	putUint32(p.buf, start+4, 1)
	copy(p.buf[frame.end:], delegation)

	return p.buf[start:end:end], true
}

var (
	idCompressedCandidate = tl.CRC(schemeValidatorSessionCompressedCandidate)
	idConsensusBlock      = tl.CRC(schemeCandidateBlock)
)

// tlBytesFraming is the header and padding width tonutils-go's encoder gives a
// bytes field of this length: one length byte below 254, four above it, eight
// above 1<<24, then the data, then zeros to a four-byte boundary. It is
// duplicated here because writing a frame backwards from a payload that is
// already placed needs the width before the bytes are written, and the encoder
// only ever reports it by having written them. TestTLBytesFramingMatchesEncoder
// holds the two against each other, boundaries included.
func tlBytesFraming(length int) (header, pad int, err error) {
	switch {
	case length < 0:
		return 0, 0, fmt.Errorf("negative TL bytes length %d", length)
	case length < 0xFE:
		header = 1
	case length < 1<<24:
		header = 4
	case uint64(length) < uint64(1)<<32:
		header = 8
	default:
		return 0, 0, fmt.Errorf("TL bytes length %d exceeds 1<<32", length)
	}
	if rem := (header + length) % 4; rem != 0 {
		pad = 4 - rem
	}

	return header, pad, nil
}

// putTLBytesHeader writes the header tlBytesFraming measured and returns the
// offset just past it, where the data itself belongs.
func putTLBytesHeader(dst []byte, at, length int) int {
	switch {
	case length < 0xFE:
		dst[at] = byte(length)

		return at + 1
	case length < 1<<24:
		binary.LittleEndian.PutUint32(dst[at:], uint32(length)<<8|0xFE)

		return at + 4
	default:
		dst[at] = 0xFF
		binary.LittleEndian.PutUint32(dst[at+1:at+5], uint32(length))
		clear(dst[at+5 : at+8])

		return at + 8
	}
}

func putUint32(dst []byte, at int, value uint32) int {
	binary.LittleEndian.PutUint32(dst[at:], value)

	return at + 4
}

func serializeCandidatePayload(candidate Candidate, blockBOC, collatedData []byte) (*candidatePayload, error) {
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
//
// The compressed bytes are laid into the buffer they will be broadcast from,
// with the room the two enclosing frames need left free around them; see
// candidatePayload for why. The LZ4 destination is a pooled scratch rather than
// that buffer, because CompressBlockBound is a worst case the output misses by
// about a quarter — sizing the retained buffer to the bound would keep ~220 KB
// of slack alive per candidate at mainnet size, where the pooled scratch keeps
// none once the wire is built.
func compressCandidatePayload(seqNo uint32, rootHash []byte, roots []*cell.Cell, cellsHint int) (*candidatePayload, error) {
	if len(rootHash) != 32 {
		return nil, fmt.Errorf("simplex: candidate root hash is %d bytes", len(rootHash))
	}
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

	bound := lz4.CompressBlockBound(len(combined))
	scratch := acquireCompressScratch(bound)
	defer releaseCompressScratch(scratch)
	// Sliced to exactly the bound the un-pooled destination used to have, so the
	// encoder sees the same destination length whatever the pool hands back. On
	// this encoder the leftover room changes no byte and no test here holds the
	// slice — but the recycled buffer is the only input that could ever make it
	// matter, and the slice costs nothing.
	compressedSize, err := lz4.CompressBlock(combined, (*scratch)[:bound], nil)
	if err != nil {
		return nil, fmt.Errorf("simplex: compress candidate BOC: %w", err)
	}

	dataHeader, dataPad, err := tlBytesFraming(compressedSize)
	if err != nil {
		return nil, fmt.Errorf("simplex: frame compressed candidate data: %w", err)
	}
	start := candidatePayloadHeadroom - compressedCandidateHeaderBytes - dataHeader
	buf := make([]byte, candidatePayloadHeadroom+compressedSize+dataPad+candidatePayloadTailroom)

	at := start
	at = putUint32(buf, at, idCompressedCandidate)
	at = putUint32(buf, at, 0) // flags:#, no bits set
	// src:int256 is left zero, as Candidate::serialize leaves it: the reference
	// fills the source only when re-serializing a candidate it received.
	clear(buf[at : at+32])
	at += 32
	at = putUint32(buf, at, seqNo)
	at += copy(buf[at:], rootHash)
	at = putUint32(buf, at, uint32(len(combined)))
	at = putTLBytesHeader(buf, at, compressedSize)
	at += copy(buf[at:], (*scratch)[:compressedSize])
	clear(buf[at : at+dataPad])

	return &candidatePayload{buf: buf, start: start, end: at + dataPad}, nil
}

// compressScratch hands out one LZ4 destination per collating goroutine. It is
// safe to pool only because the buffer never leaves compressCandidatePayload:
// the payload is copied out of it into the buffer the wire is built in, and a
// scratch that outlived that copy would be broadcast bytes owned by a pool.
var compressScratch sync.Pool

func acquireCompressScratch(size int) *[]byte {
	if buf, _ := compressScratch.Get().(*[]byte); buf != nil && len(*buf) >= size {
		return buf
	}
	buf := make([]byte, size)

	return &buf
}

func releaseCompressScratch(buf *[]byte) {
	compressScratch.Put(buf)
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
