package validator

import (
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"

	"github.com/pierrec/lz4/v4"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	tnstore "github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
)

// CandidateLimits bounds candidate decompression before any untrusted buffer
// is allocated. The values are the max_block_size and max_collated_data_size
// fields of the active consensus config.
type CandidateLimits struct {
	MaxBlockBytes        uint32
	MaxCollatedDataBytes uint32
}

func (l CandidateLimits) validate() error {
	if l.MaxBlockBytes == 0 {
		return errors.New("validator runtime: max block bytes must be positive")
	}
	if l.MaxCollatedDataBytes == 0 {
		return errors.New("validator runtime: max collated data bytes must be positive")
	}

	return nil
}

// CandidateArtifact is the immutable decoded form used by validation and
// state resolution. The resolver retains the canonical compressed wire
// separately so persistence never requires a second compression pass.
type CandidateArtifact struct {
	Candidate    simplex.Candidate
	BlockBOC     []byte
	CollatedData []byte

	// prepared is the compressed payload of this candidate, built by whoever
	// held its cell roots: the collator for a locally built candidate, the
	// decoder for a received one. It exists only until the wire is derived from
	// it and is cleared there, so an artifact that outlives its serialization —
	// every artifact the resolver caches — never carries a second copy of the
	// payload it already stores as wire.
	//
	// The field is unexported on purpose. A prepared payload is only obtainable
	// from the roots it serializes, and an artifact reaching this package from
	// an extension-supplied Pipeline therefore cannot carry one.
	prepared *simplex.PreparedCandidate

	// digested records that Candidate.Block.FileHash and
	// Candidate.CollatedFileHash are the sha256 of BlockBOC and CollatedData.
	// It is set only where that is how those two values came to exist — the
	// decoder derives both from the payload it decompressed, the local producer
	// takes both inside the build that serialized it — so it travels with the
	// bytes rather than being asserted about them.
	//
	// Unexported for the same reason prepared is: an artifact reaching this
	// package from anywhere but those two producers cannot claim it, and the
	// validator it is handed to then re-derives the digest itself.
	digested bool

	// preparedBlock is present only for Protocol-1 received candidates. The
	// decoder already held the parsed root and serialized the canonical block;
	// carrying that immutable result lets the asynchronous node cache build
	// metadata without parsing and hashing BlockBOC again.
	preparedBlock *tnstore.PreparedBlockCandidate

	// validationRoots is the parsed payload of this candidate, from whoever
	// already held it: the network codec that decoded a received candidate, or
	// the collator that built one here. A voting resolver moves it out of the
	// retained artifact and hands it to at most one validation; observers and
	// long-lived candidate storage keep only the canonical bytes.
	validationRoots *candidateValidationRoots

	// generationTimeMS is the exact millisecond timestamp carried by the
	// consensus-extra root. generationTimeKnown is set only by code that already
	// held the roots from which CollatedData was serialized, or by the one
	// boundary that decoded CollatedData itself. Keeping the scalar lets state
	// resolution avoid deserializing the whole collated BOC for one uint64.
	//
	// Both fields are unexported so an externally assembled artifact cannot
	// claim a timestamp that is not bound to its collated file hash.
	generationTimeMS    uint64
	generationTimeKnown bool
}

type candidateValidationRoots struct {
	block    *cell.Cell
	collated []*cell.Cell
}

// PreparedBlockCandidate is the storage-ready Protocol-1 block artifact, or nil
// for candidate protocols that do not use the legacy cache route. The returned
// value and its BOC are immutable and may be retained by the publication queue.
func (a CandidateArtifact) PreparedBlockCandidate() *tnstore.PreparedBlockCandidate {
	return a.preparedBlock
}

type candidateCodec struct {
	sessionID       [32]byte
	shard           groups.ShardID
	validators      []simplex.Validator
	schedule        simplex.LeaderSchedule
	slotsPerWindow  uint32
	limits          CandidateLimits
	protocolVersion uint8
}

func newCandidateCodec(config SessionConfig, limits CandidateLimits) (*candidateCodec, error) {
	if err := limits.validate(); err != nil {
		return nil, err
	}
	if len(config.Validators) == 0 {
		return nil, errors.New("validator runtime: empty validator set")
	}
	if config.Protocol.Version != 2 || config.Protocol.ProtocolVersion > simplex.MaxProtocolVersion {
		return nil, fmt.Errorf(
			"validator runtime: unsupported simplex version %d protocol %d",
			config.Protocol.Version,
			config.Protocol.ProtocolVersion,
		)
	}
	if config.Protocol.SlotsPerLeaderWindow == 0 {
		return nil, errors.New("validator runtime: slots per leader window must be positive")
	}

	validators := make([]simplex.Validator, len(config.Validators))
	for i := range config.Validators {
		validator := &config.Validators[i]
		validators[i] = simplex.Validator{
			PublicKey: append(ed25519.PublicKey(nil), validator.PublicKey[:]...),
			Weight:    validator.Weight,
		}
	}

	return &candidateCodec{
		sessionID:  config.SessionID,
		shard:      config.Shard,
		validators: validators,
		schedule: simplex.RoundRobinSchedule{
			SlotsPerLeaderWindow: config.Protocol.SlotsPerLeaderWindow,
			Validators:           uint32(len(validators)),
		},
		slotsPerWindow:  config.Protocol.SlotsPerLeaderWindow,
		limits:          limits,
		protocolVersion: config.Protocol.ProtocolVersion,
	}, nil
}

// encodeForBroadcast derives the resolver wire and the private-overlay FEC
// payload from one compression pass. Non-delegated wire aliases broadcast.Data;
// callers and transports must treat both as immutable.
func (c *candidateCodec) encodeForBroadcast(
	artifact *CandidateArtifact,
) ([]byte, simplex.CandidateBroadcast, error) {
	if err := c.verifyCandidate(&artifact.Candidate); err != nil {
		return nil, simplex.CandidateBroadcast{}, err
	}
	// A locally built candidate arrives with its payload already compressed,
	// overlapped with signing, persistence and the scheduled broadcast wait.
	// Everything else — a recovered artifact, an empty candidate, a retry after
	// the payload was consumed — takes the full path from the BOCs.
	prepared := artifact.prepared
	artifact.prepared = nil
	var broadcast simplex.CandidateBroadcast
	var err error
	if prepared != nil {
		broadcast, err = simplex.SerializeCandidateForBroadcastPrepared(artifact.Candidate, prepared)
	} else {
		broadcast, err = simplex.SerializeCandidateForBroadcast(
			artifact.Candidate,
			artifact.BlockBOC,
			artifact.CollatedData,
		)
	}
	if err != nil {
		return nil, simplex.CandidateBroadcast{}, err
	}
	wire, err := broadcast.CandidateWire(artifact.Candidate.Delegation)
	if err != nil {
		return nil, simplex.CandidateBroadcast{}, err
	}

	return wire, broadcast, nil
}

// decode takes ownership of wire for the duration of the call. Returned BOCs
// are canonical mode-31/mode-2 serializations and are independently owned.
func (c *candidateCodec) decode(wire []byte, expected *simplex.CandidateID) (*CandidateArtifact, error) {
	return c.decodeVerified(wire, expected)
}

// decodeVerified parses, binds and signature-checks a candidate and builds no
// wire. It is the part of a decode every caller needs, and it is everything the
// receive path needs: the canonical bytes used to be built right after it, for
// every received candidate, at a combined BOC serialization plus an LZ4 pass —
// measured on the testnet validator at 3.27 s of CPU per minute, more than the
// node spent collating — and nothing on that path read them. They are built
// after the signature check now, so an unsigned forgery costs a parse and a
// verify and no more; and on the receive path they are deferred past that, to
// the first consumer that asks, by decodeDeferred or decodeBroadcastDeferred.
func (c *candidateCodec) decodeVerified(wire []byte, expected *simplex.CandidateID) (*CandidateArtifact, error) {
	wrapped, err := simplex.ParseCandidateWrapped(wire)
	if err != nil {
		return nil, fmt.Errorf("validator runtime: decode candidate: %w", err)
	}

	return c.decodeVerifiedData(wrapped.Data, wrapped.Delegation, expected)
}

// decodeBroadcast verifies the bare candidate data delivered by the private
// overlay together with the delegation already authenticated from its extra.
// The delegation is copied before it becomes part of the artifact: transport
// buffers are owned only for this callback, while a lazy canonical wire may
// retain the candidate until a later store or request.
func (c *candidateCodec) decodeBroadcast(
	payload []byte,
	delegation *simplex.Delegation,
	expectedSlot uint32,
) (*CandidateArtifact, error) {
	data, err := simplex.ParseCandidateData(payload)
	if err != nil {
		return nil, fmt.Errorf("validator runtime: decode candidate broadcast: %w", err)
	}
	artifact, _, err := c.decodeData(data, &expectedSlot)
	if err != nil {
		return nil, err
	}

	return c.verifyDecodedCandidate(artifact, cloneDelegation(delegation), nil)
}

func (c *candidateCodec) decodeVerifiedData(
	data any,
	delegation *simplex.Delegation,
	expected *simplex.CandidateID,
) (*CandidateArtifact, error) {
	artifact, _, err := c.decodeData(data, nil)
	if err != nil {
		return nil, err
	}

	return c.verifyDecodedCandidate(artifact, delegation, expected)
}

func (c *candidateCodec) verifyDecodedCandidate(
	artifact *CandidateArtifact,
	delegation *simplex.Delegation,
	expected *simplex.CandidateID,
) (*CandidateArtifact, error) {
	artifact.Candidate.Delegation = delegation
	artifact.Candidate.Leader = c.schedule.ExpectedLeader(artifact.Candidate.ID.Slot)
	if expected != nil && artifact.Candidate.ID != *expected {
		return nil, errors.New("validator runtime: candidate id mismatch")
	}
	if err := c.verifyCandidate(&artifact.Candidate); err != nil {
		return nil, err
	}

	return artifact, nil
}

func cloneDelegation(delegation *simplex.Delegation) *simplex.Delegation {
	if delegation == nil {
		return nil
	}

	return &simplex.Delegation{
		CollatorKey: append(ed25519.PublicKey(nil), delegation.CollatorKey...),
		Signature:   append([]byte(nil), delegation.Signature...),
	}
}

// decodeDeferred is the lazy canonical decode for the receive path. It returns
// the decoded artifact together with a wire that builds the canonical bytes
// and their digest on first use, so a received candidate that is never stored
// and never requested never pays for them. lazyCandidateWire says why the
// deferral is sound.
//
// An empty candidate has no payload and its wire is a few bytes, so it comes
// back already built — the lazy form would cost more than the bytes.
func (c *candidateCodec) decodeDeferred(
	wire []byte,
	expected *simplex.CandidateID,
) (*CandidateArtifact, *lazyCandidateWire, error) {
	artifact, err := c.decodeVerified(wire, expected)
	if err != nil {
		return nil, nil, err
	}

	lazy, err := deferredCandidateWire(artifact)
	if err != nil {
		return nil, nil, err
	}

	return artifact, lazy, nil
}

func (c *candidateCodec) decodeBroadcastDeferred(
	payload []byte,
	delegation *simplex.Delegation,
	expectedSlot uint32,
) (*CandidateArtifact, *lazyCandidateWire, error) {
	artifact, err := c.decodeBroadcast(payload, delegation, expectedSlot)
	if err != nil {
		return nil, nil, err
	}

	lazy, err := deferredCandidateWire(artifact)
	if err != nil {
		return nil, nil, err
	}

	return artifact, lazy, nil
}

func deferredCandidateWire(artifact *CandidateArtifact) (*lazyCandidateWire, error) {
	lazy := &lazyCandidateWire{candidate: artifact.Candidate}
	if artifact.validationRoots == nil || artifact.validationRoots.block == nil {
		lazy.once.Do(func() {
			lazy.wire, lazy.err = simplex.SerializeCandidate(artifact.Candidate, artifact.BlockBOC, artifact.CollatedData)
			if lazy.err == nil {
				lazy.hash = sha256.Sum256(lazy.wire)
			}
		})
		if lazy.err != nil {
			return nil, lazy.err
		}

		return lazy, nil
	}
	lazy.blockRoot = artifact.validationRoots.block
	lazy.collatedRoots = artifact.validationRoots.collated
	lazy.blockBOC = artifact.BlockBOC
	lazy.collatedData = artifact.CollatedData

	return lazy, nil
}

// decodeData crosses simplex candidate data's TL interface once. expectedSlot
// is supplied by the bare broadcast path so its cheap slot policy stays ahead
// of payload decoding; wrapped candidates have no separate transport slot.
func (c *candidateCodec) decodeData(
	data any,
	expectedSlot *uint32,
) (*CandidateArtifact, uint32, error) {
	var block simplex.ConsensusBlockData
	var empty simplex.ConsensusEmptyData
	blockData := false
	var slot int32
	switch candidate := data.(type) {
	case simplex.ConsensusBlockData:
		block, blockData, slot = candidate, true, candidate.Slot
	case *simplex.ConsensusBlockData:
		block, blockData, slot = *candidate, true, candidate.Slot
	case simplex.ConsensusEmptyData:
		empty, slot = candidate, candidate.Slot
	case *simplex.ConsensusEmptyData:
		empty, slot = *candidate, candidate.Slot
	default:
		return nil, 0, fmt.Errorf("validator runtime: unexpected candidate data type %T", data)
	}
	if slot < 0 {
		return nil, 0, errors.New("validator runtime: candidate slot is negative")
	}
	decodedSlot := uint32(slot)
	if expectedSlot != nil && decodedSlot != *expectedSlot {
		return nil, 0, errors.New("validator runtime: candidate broadcast slot mismatch")
	}

	if blockData {
		artifact, err := c.decodeBlock(block)
		return artifact, decodedSlot, err
	}
	artifact, err := c.decodeEmpty(empty)
	return artifact, decodedSlot, err
}

func (c *candidateCodec) decodeBlock(
	data simplex.ConsensusBlockData,
) (*CandidateArtifact, error) {
	parent, err := candidateParentFromTL(data.Parent)
	if err != nil {
		return nil, err
	}

	payload, err := c.decodePayload(data.Candidate)
	if err != nil {
		return nil, err
	}
	block := ton.BlockIDExt{
		Workchain: c.shard.Workchain,
		Shard:     c.shard.Shard,
		SeqNo:     payload.round,
		RootHash:  payload.rootHash[:],
		FileHash:  payload.fileHash[:],
	}
	candidate := simplex.Candidate{
		Parent:           parent,
		Block:            block,
		CollatedFileHash: payload.collatedFileHash,
		Signature:        data.Signature,
	}
	candidate.ID = candidate.ComputeID(uint32(data.Slot))

	return &CandidateArtifact{
		Candidate:    candidate,
		BlockBOC:     payload.blockBOC,
		CollatedData: payload.collatedData,
		// block.FileHash and candidate.CollatedFileHash immediately above are
		// payload.fileHash and payload.collatedFileHash, which decodePayload
		// took of these exact two buffers. The candidate id the signature covers
		// is computed from them here, so the digests are not merely consistent
		// with the payload — they are the only thing that made this candidate
		// identifiable at all.
		digested:      true,
		preparedBlock: payload.preparedBlock,
		validationRoots: &candidateValidationRoots{
			block:    payload.blockRoot,
			collated: payload.collatedRoots,
		},
		generationTimeMS:    payload.generationTimeMS,
		generationTimeKnown: true,
	}, nil
}

func (c *candidateCodec) decodeEmpty(data simplex.ConsensusEmptyData) (*CandidateArtifact, error) {
	parent, err := candidateIDFromTL(data.Parent)
	if err != nil {
		return nil, err
	}

	candidate := simplex.Candidate{
		Parent:    simplex.Parent(parent),
		Empty:     true,
		Block:     data.Block,
		Signature: data.Signature,
	}
	candidate.ID = candidate.ComputeID(uint32(data.Slot))

	return &CandidateArtifact{Candidate: candidate}, nil
}

type decodedCandidatePayload struct {
	round            uint32
	rootHash         [32]byte
	blockBOC         []byte
	collatedData     []byte
	fileHash         [32]byte
	collatedFileHash [32]byte
	preparedBlock    *tnstore.PreparedBlockCandidate
	blockRoot        *cell.Cell
	collatedRoots    []*cell.Cell
	generationTimeMS uint64
}

func (c *candidateCodec) decodePayload(data []byte) (decodedCandidatePayload, error) {
	var payload tl.Serializable
	rest, err := tl.Parse(&payload, data, true)
	if err != nil {
		return decodedCandidatePayload{}, fmt.Errorf("validator runtime: decode compressed candidate: %w", err)
	}
	if len(rest) != 0 {
		return decodedCandidatePayload{}, errors.New("validator runtime: trailing compressed candidate data")
	}

	maxCombined := uint64(c.limits.MaxBlockBytes) + uint64(c.limits.MaxCollatedDataBytes) + 1024
	if maxCombined > math.MaxInt {
		return decodedCandidatePayload{}, errors.New("validator runtime: candidate size limit overflows int")
	}

	// tl.Parse never yields a pointer here, but a caller may hand one over.
	// Normalizing first keeps one branch per constructor: a type switch does not
	// re-dispatch once its subject is reassigned, so this cannot be folded into
	// the switch below.
	switch pointer := payload.(type) {
	case *simplex.ValidatorSessionCompressedCandidate:
		payload = *pointer
	case *simplex.ValidatorSessionCompressedCandidateV2:
		payload = *pointer
	}

	var source, rootHash []byte
	var round int32
	var roots []*cell.Cell
	// The cell count the receiver gets for free. The combined payload holds the
	// union of the block and collated cell sets, so its declared count is a good
	// upper bound for the large block serialization. It is deliberately not
	// applied to collated data: on the marker-shaped masterchain candidate that
	// would presize a one-cell bag for the whole block. PayloadCellHint reads the
	// count off the buffer and clamps it; see simplex/candidate_payload_hint.go
	// for why a count read out of an untrusted header cannot presize an unbounded
	// table.
	var combinedCellsHint int
	switch compressed := payload.(type) {
	case simplex.ValidatorSessionCompressedCandidate:
		source, round, rootHash = compressed.Source, compressed.Round, compressed.RootHash
		if compressed.DecompressedSize < 0 || uint64(compressed.DecompressedSize) > maxCombined {
			return decodedCandidatePayload{}, errors.New("validator runtime: decompressed candidate is too large")
		}
		decompressed := make([]byte, int(compressed.DecompressedSize))
		written, decompressErr := lz4.UncompressBlock(compressed.Data, decompressed)
		if decompressErr != nil {
			return decodedCandidatePayload{}, fmt.Errorf("validator runtime: decompress candidate: %w", decompressErr)
		}
		if written != len(decompressed) {
			return decodedCandidatePayload{}, errors.New("validator runtime: decompressed candidate size mismatch")
		}
		combinedCellsHint = simplex.PayloadCellHint(decompressed, nil)
		// The parsed DAG owns immutable views into decompressed. Cell payload
		// slices keep that backing array alive through validationRoots, while
		// avoiding a second copy of every payload byte before canonical rebuild.
		roots, err = cell.FromBOCMultiRootWithOptions(decompressed, cell.BOCParseOptions{
			NoCopyPayload: true,
		})
	case simplex.ValidatorSessionCompressedCandidateV2:
		source, round, rootHash = compressed.Source, compressed.Round, compressed.RootHash
		if uint64(len(compressed.Data)) > maxCombined {
			return decodedCandidatePayload{}, errors.New("validator runtime: compressed candidate is too large")
		}
		// The structural form never materializes a combined BOC, so there is no
		// header to read a count off and the serializers size themselves as they
		// always did. Nothing in this project emits this form — it is decoded
		// because a peer may send it — so the hinted path is the one production
		// takes.
		roots, err = cell.DecompressBOC(compressed.Data, int(maxCombined), nil)
	default:
		return decodedCandidatePayload{}, fmt.Errorf("validator runtime: unexpected compressed candidate type %T", payload)
	}
	if err != nil {
		return decodedCandidatePayload{}, fmt.Errorf("validator runtime: decode combined candidate boc: %w", err)
	}

	return c.finishPayload(source, round, rootHash, roots, combinedCellsHint)
}

// finishPayload rebuilds the two canonical BOCs of a received candidate and the
// two digests that identify it. It runs inside decodeVerified, so it runs before
// the signature check on every candidate a validator is offered — about fourteen
// of them for every one it produces — and the notarization quorum cannot form
// until it is done.
//
// The two serializations run concurrently. The block half is given the
// combined cell count; the collated half reuses serializer scratch and grows to
// its actual size. candidate_receive_serialize_test.go holds byte-parity,
// allocation gates and benchmarks over both the full-proof and marker shapes.
//
// Both are safe over one already-parsed DAG whose two halves share cells:
//
//   - The hint reaches only newBOCHashIndex's initial capacity and the bag's
//     cell-list capacity (tvm/cell/serialize_options.go). The index is open
//     addressed and verifies the full hash on a fingerprint match, so which
//     entry a lookup finds cannot depend on the table size, and cell indices come
//     from DFS visit order either way. Hinted and unhinted output are the same
//     bytes, which is a gate here and not an expectation.
//   - The concurrency touches nothing on the cells. Hashes are precomputed at
//     parse (tvm/cell/proof.go calculateHashes, "for safe read parallel access
//     later"), all per-cell serializer bookkeeping lives in the bag's own
//     bocSerializeItem, and the one lazily cached field, Cell.typ, is written
//     only by resolveType(), which the serializer never calls — it reads through
//     GetType(), which computes without caching when the field is unset.
func (c *candidateCodec) finishPayload(
	source []byte,
	round int32,
	rootHash []byte,
	roots []*cell.Cell,
	combinedCellsHint int,
) (decodedCandidatePayload, error) {
	if len(source) != 32 || !allZeroBytes(source) {
		return decodedCandidatePayload{}, errors.New("validator runtime: candidate source must be zero")
	}
	if len(rootHash) != 32 {
		return decodedCandidatePayload{}, errors.New("validator runtime: candidate root hash length is invalid")
	}
	if len(roots) == 0 {
		return decodedCandidatePayload{}, errors.New("validator runtime: candidate boc is empty")
	}
	if !cellHashEquals(roots[0], rootHash) {
		return decodedCandidatePayload{}, errors.New("validator runtime: candidate root hash mismatch")
	}
	generationTimeMS, err := candidateGenUtimeMSFromRoots(roots[1:])
	if err != nil {
		return decodedCandidatePayload{}, err
	}

	// The collated half is started first and joined below, so it overlaps the
	// block half whatever branch that takes. Its size check stays ahead of its
	// digest, as it is on the block side: a candidate whose serialization
	// overflows the consensus limit is rejected without paying for a sha256 pass
	// over it.
	var collatedData []byte
	var collatedFileHash [32]byte
	var collatedErr error
	collatedDone := make(chan struct{})
	go func() {
		defer close(collatedDone)

		data, err := cell.ToBOCWithOptionsErr(
			roots[1:],
			cell.BOCSerializeOptions{WithCRC32C: true},
		)
		if err != nil {
			collatedErr = fmt.Errorf("validator runtime: serialize candidate collated data: %w", err)

			return
		}
		if uint64(len(data)) > uint64(c.limits.MaxCollatedDataBytes) {
			collatedErr = errors.New("validator runtime: candidate collated data is too large")

			return
		}
		collatedData, collatedFileHash = data, sha256.Sum256(data)
	}()

	// Protocol 1 hands the block to the node cache immediately after consensus
	// decode. Build a sealed root+BOC artifact here, while the parsed graph is in
	// hand, so that worker does not deserialize and hash the same block again.
	// Later protocols do not use that cache route and keep the allocation-free
	// direct serialization.
	var preparedBlock *tnstore.PreparedBlockCandidate
	var blockBOC []byte
	if c.protocolVersion == 1 {
		preparedBlock, err = tnstore.PrepareBlockCandidate(
			c.shard.Workchain,
			c.shard.Shard,
			uint32(round),
			roots[0],
		)
		if err != nil {
			err = fmt.Errorf("validator runtime: prepare candidate block: %w", err)
		} else {
			blockBOC = preparedBlock.BlockBOC()
		}
	} else {
		// This is the byte form emitted by reference std_boc_serialize(root, 31):
		// its root marker is not set, so WithTopHash must remain disabled.
		blockBOC, err = roots[0].ToBOCWithOptionsErr(cell.BOCSerializeOptions{
			WithCRC32C:     true,
			WithIndex:      true,
			WithCacheBits:  true,
			WithIntHashes:  true,
			CellsCountHint: combinedCellsHint,
		})
		if err != nil {
			err = fmt.Errorf("validator runtime: serialize candidate block boc: %w", err)
		}
	}

	// Both of these are finished the instant the block branch is, and neither
	// reads anything the collated goroutine writes, so they belong above the
	// join rather than after it — this function runs on every candidate a
	// validator is offered, before the signature check, and the notarization
	// quorum cannot form until it returns. Size before digest, the same order
	// the collated side uses: a block that overflows the consensus limit is
	// rejected without paying for a sha256 pass over it.
	//
	// The failure is recorded rather than returned. The goroutine writes the
	// three collated variables, so no path out of this function may leave it
	// running, and that is what keeps every return below the join.
	var fileHash [32]byte
	var blockErr error
	if err == nil {
		switch {
		case uint64(len(blockBOC)) > uint64(c.limits.MaxBlockBytes):
			blockErr = errors.New("validator runtime: candidate block is too large")
		case preparedBlock != nil:
			preparedID := preparedBlock.ID()
			copy(fileHash[:], preparedID.FileHash)
		default:
			fileHash = sha256.Sum256(blockBOC)
		}
	}

	// Joined before anything is returned: see above.
	<-collatedDone
	if err != nil {
		return decodedCandidatePayload{}, err
	}
	if blockErr != nil {
		return decodedCandidatePayload{}, blockErr
	}
	if collatedErr != nil {
		return decodedCandidatePayload{}, collatedErr
	}

	result := decodedCandidatePayload{
		round:            uint32(round),
		blockBOC:         blockBOC,
		collatedData:     collatedData,
		fileHash:         fileHash,
		collatedFileHash: collatedFileHash,
		preparedBlock:    preparedBlock,
		blockRoot:        roots[0],
		collatedRoots:    roots[1:],
		generationTimeMS: generationTimeMS,
	}
	copy(result.rootHash[:], rootHash)

	return result, nil
}

func (c *candidateCodec) verifyCandidate(candidate *simplex.Candidate) error {
	if err := candidate.ValidateShape(); err != nil {
		return fmt.Errorf("validator runtime: candidate shape: %w", err)
	}
	leader := c.schedule.ExpectedLeader(candidate.ID.Slot)
	if int(leader) >= len(c.validators) {
		return fmt.Errorf("validator runtime: leader %d is out of range", leader)
	}
	if candidate.Leader != leader {
		return fmt.Errorf("validator runtime: candidate leader %d, want %d", candidate.Leader, leader)
	}
	if candidate.ComputeID(candidate.ID.Slot) != candidate.ID {
		return errors.New("validator runtime: candidate id does not match hash data")
	}

	leaderKey := c.validators[leader].PublicKey
	if candidate.Delegation == nil {
		if !simplex.VerifyCandidateSignature(leaderKey, c.sessionID, candidate.ID, candidate.Signature) {
			return errors.New("validator runtime: candidate signature is invalid")
		}

		return nil
	}
	delegation := candidate.Delegation
	if len(delegation.CollatorKey) != ed25519.PublicKeySize {
		return errors.New("validator runtime: delegated collator key is invalid")
	}
	windowStart := candidate.ID.Slot - candidate.ID.Slot%c.slotsPerWindow
	collatorID := simplex.KeyNodeIDShort(delegation.CollatorKey)
	if !simplex.VerifyDelegationSignature(
		leaderKey,
		c.sessionID,
		windowStart,
		collatorID,
		delegation.Signature,
	) {
		return errors.New("validator runtime: delegation signature is invalid")
	}
	if !simplex.VerifyCandidateSignature(
		delegation.CollatorKey,
		c.sessionID,
		candidate.ID,
		candidate.Signature,
	) {
		return errors.New("validator runtime: delegated candidate signature is invalid")
	}

	return nil
}

func candidateParentFromTL(value any) (simplex.ParentID, error) {
	switch parent := value.(type) {
	case ton.ConsensusCandidateWithoutParents, *ton.ConsensusCandidateWithoutParents:
		return simplex.Genesis(), nil
	case ton.ConsensusCandidateParent:
		id, err := candidateIDFromTL(parent.ID)
		if err != nil {
			return simplex.ParentID{}, err
		}

		return simplex.Parent(id), nil
	case *ton.ConsensusCandidateParent:
		id, err := candidateIDFromTL(parent.ID)
		if err != nil {
			return simplex.ParentID{}, err
		}

		return simplex.Parent(id), nil
	default:
		return simplex.ParentID{}, fmt.Errorf("validator runtime: unexpected candidate parent type %T", value)
	}
}

func candidateIDFromTL(value any) (simplex.CandidateID, error) {
	var wire ton.ConsensusCandidateID
	switch id := value.(type) {
	case ton.ConsensusCandidateID:
		wire = id
	case *ton.ConsensusCandidateID:
		wire = *id
	default:
		return simplex.CandidateID{}, fmt.Errorf("validator runtime: unexpected candidate id type %T", value)
	}
	if len(wire.Hash) != 32 {
		return simplex.CandidateID{}, errors.New("validator runtime: candidate hash length is invalid")
	}

	id := simplex.CandidateID{Slot: uint32(wire.Slot)}
	copy(id.Hash[:], wire.Hash)

	return id, nil
}

func allZeroBytes(data []byte) bool {
	for _, value := range data {
		if value != 0 {
			return false
		}
	}

	return true
}

func cellHashEquals(root *cell.Cell, expected []byte) bool {
	if len(expected) != 32 {
		return false
	}
	var hash cell.Hash
	copy(hash[:], expected)

	return root.HashKey() == hash
}
