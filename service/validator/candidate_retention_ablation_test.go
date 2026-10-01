//go:build retentiondiag

package validator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"math"
	"os"
	"runtime"
	"strconv"
	"testing"
	"weak"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/validator/simplex"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type retentionAblationRoots struct {
	block    weak.Pointer[cell.Cell]
	collated weak.Pointer[cell.Cell]
}

type retentionAblationResult struct {
	HashMode           string `json:"hash_mode"`
	CacheMode          string `json:"cache_mode"`
	Candidates         int    `json:"candidates"`
	PinnedBlocks       bool   `json:"pinned_blocks"`
	BaselineHeapBytes  uint64 `json:"baseline_heap_bytes"`
	RetainedHeapBytes  uint64 `json:"retained_heap_bytes"`
	RetainedDeltaBytes int64  `json:"retained_delta_bytes"`
	PayloadBytes       int64  `json:"payload_bytes"`
	DecodedChargeBytes int64  `json:"decoded_charge_bytes"`
	DecodedDemotions   uint64 `json:"decoded_demotions"`
	DecodedBudgetBytes int64  `json:"decoded_budget_bytes"`
	LiveBlockRoots     int    `json:"live_block_roots"`
	LiveCollatedRoots  int    `json:"live_collated_roots"`
	ClosedDeltaBytes   int64  `json:"closed_delta_bytes"`
	ClosedBlockRoots   int    `json:"closed_block_roots"`
	ClosedProofRoots   int    `json:"closed_proof_roots"`
	ReleasedDeltaBytes int64  `json:"released_delta_bytes"`
	ReleasedBlockRoots int    `json:"released_block_roots"`
	OwnersDroppedDelta int64  `json:"owners_dropped_delta_bytes"`
}

// TestCandidateRetentionAblation is opt-in and must run in its own process.
// It is a retained-Go-heap counterfactual, not an RSS or throughput benchmark.
// GTON_RETENTION_ABLATION_HASH describes the binary: "interior" must be built
// using a Go overlay reverting only decodeBlock's two hash copies. Both binary
// versions otherwise execute the same production decode and resolver path.
func TestCandidateRetentionAblation(t *testing.T) {
	hashMode := os.Getenv("GTON_RETENTION_ABLATION_HASH")
	if hashMode == "" {
		t.Skip("set GTON_RETENTION_ABLATION_HASH=interior|detached in an isolated diagnostic process")
	}
	if hashMode != "interior" && hashMode != "detached" {
		t.Fatal("hash mode must be interior or detached")
	}
	cacheMode := os.Getenv("GTON_RETENTION_ABLATION_CACHE")
	if cacheMode != "keep" && cacheMode != "budget" && cacheMode != "default" {
		t.Fatal("set GTON_RETENTION_ABLATION_CACHE=keep|budget|default")
	}
	n, err := strconv.Atoi(os.Getenv("GTON_RETENTION_ABLATION_N"))
	if err != nil || n < 2 || n > 64 {
		t.Fatal("set GTON_RETENTION_ABLATION_N between 2 and 64")
	}

	wires := retentionAblationWires(t, n)
	retentionAblationWarm(t, wires[0])
	r := retentionAblationResolver(cacheMode)
	defer r.close()
	roots := make([]retentionAblationRoots, n)
	pinBlocks := os.Getenv("GTON_RETENTION_ABLATION_PIN_BLOCKS") == "1"
	var blockOwners []*cell.Cell
	if pinBlocks {
		blockOwners = make([]*cell.Cell, n)
	}
	baseline := retentionAblationHeap()

	for i, wire := range wires {
		roots[i] = retentionAblationStage(t, r, wire, blockOwners, i)
	}
	retained := retentionAblationHeap()
	liveBlock, liveCollated := retentionAblationLive(roots)
	stats := r.cacheProjection()
	wantProof := n
	if hashMode == "detached" {
		wantProof -= int(stats.DecodedDemotions)
	}
	wantBlock := wantProof
	if pinBlocks {
		wantBlock = n
	}
	if liveBlock != wantBlock || liveCollated != wantProof {
		t.Fatalf("live roots block=%d collated=%d, want block=%d proof=%d", liveBlock, liveCollated, wantBlock, wantProof)
	}
	if cacheMode == "budget" && stats.DecodedDemotions != uint64(n-1) {
		t.Fatalf("tiny cache made %d demotions, want %d", stats.DecodedDemotions, n-1)
	}
	if stats.Candidates != n {
		t.Fatalf("resolver retained %d candidate payloads, want %d", stats.Candidates, n)
	}
	assertRootCacheAccounting(t, r)

	// Exercise real canonical wire recovery only after the memory observation,
	// so materialization cannot free roots or add wire bytes before the result.
	retentionAblationCheckWire(t, r, wires[0])
	retentionAblationCheckWire(t, r, wires[n-1])
	r.close()
	closed := retentionAblationHeap()
	closedBlock, closedProof := retentionAblationLive(roots)
	wantClosedProof := 0
	if hashMode == "interior" {
		// close clears explicit roots, but intentionally preserves payloads.
		// Old identity slices therefore keep their hidden owner even afterwards.
		wantClosedProof = n
	}
	wantClosedBlock := wantClosedProof
	if pinBlocks {
		wantClosedBlock = n
	}
	if closedBlock != wantClosedBlock || closedProof != wantClosedProof {
		t.Fatalf("closed roots block=%d collated=%d, want block=%d proof=%d", closedBlock, closedProof, wantClosedBlock, wantClosedProof)
	}
	r.mu.Lock()
	for id, entry := range r.entries {
		r.releasePayloadLocked(id, entry)
	}
	r.mu.Unlock()
	released := retentionAblationHeap()
	releasedBlock, releasedProof := retentionAblationLive(roots)
	wantReleasedBlock := 0
	if pinBlocks {
		wantReleasedBlock = n
	}
	if releasedBlock != wantReleasedBlock || releasedProof != 0 {
		t.Fatalf("released payload roots block=%d collated=%d, want block=%d proof=0", releasedBlock, releasedProof, wantReleasedBlock)
	}
	runtime.KeepAlive(blockOwners)
	clear(blockOwners)
	ownersDropped := retentionAblationHeap()
	if block, collated := retentionAblationLive(roots); block != 0 || collated != 0 {
		t.Fatalf("cleared block owners still retain block=%d collated=%d", block, collated)
	}

	result := retentionAblationResult{
		HashMode: hashMode, CacheMode: cacheMode, Candidates: n, PinnedBlocks: pinBlocks,
		BaselineHeapBytes: baseline, RetainedHeapBytes: retained,
		RetainedDeltaBytes: int64(retained) - int64(baseline),
		PayloadBytes:       stats.Bytes, DecodedChargeBytes: stats.DecodedBytes,
		DecodedDemotions:   stats.DecodedDemotions,
		DecodedBudgetBytes: r.decodedBudget,
		LiveBlockRoots:     liveBlock, LiveCollatedRoots: liveCollated,
		ClosedDeltaBytes: int64(closed) - int64(baseline),
		ClosedBlockRoots: closedBlock, ClosedProofRoots: closedProof,
		ReleasedDeltaBytes: int64(released) - int64(baseline),
		ReleasedBlockRoots: releasedBlock,
		OwnersDroppedDelta: int64(ownersDropped) - int64(baseline),
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("RETENTION_ABLATION %s", encoded)
	runtime.KeepAlive(r)
	runtime.KeepAlive(wires)
	runtime.KeepAlive(roots)
}

func retentionAblationResolver(cacheMode string) *candidateResolver {
	r := newResolverForTest(newRuntimeTestStorage(), nil, 1, simplex.DefaultParams())
	r.codec.protocolVersion = 1
	switch cacheMode {
	case "keep":
		r.decodedBudget = math.MaxInt64
	case "budget":
		r.decodedBudget = 1
	}
	return r
}

func retentionAblationWires(t testing.TB, n int) [][]byte {
	t.Helper()
	fixture := loadReceiveFixture(t, fixtureFullCollated)
	compressed := receiveFixtureCompressed(t, fixture)
	codec := receiveFixtureCodec(t, 1)
	artifact, _, err := codec.decodeDeferred(compressed.broadcast, nil)
	if err != nil {
		t.Fatal(err)
	}
	config, key := runtimeTestConfig(resolverTestSessionTag, &runtimeTestJournal{})
	wires := make([][]byte, n)
	for i := range wires {
		candidate := artifact.Candidate
		candidate.ID = candidate.ComputeID(uint32(i))
		candidate.Signature, err = simplex.SignCandidate(runtimeTestSigner{key: key}, config.SessionID, candidate.ID)
		if err != nil {
			t.Fatal(err)
		}
		wires[i], err = simplex.SerializeCandidate(candidate, artifact.BlockBOC, artifact.CollatedData)
		if err != nil {
			t.Fatal(err)
		}
	}
	return wires
}

func retentionAblationWarm(t testing.TB, wire []byte) {
	t.Helper()
	r := retentionAblationResolver("budget")
	retentionAblationStage(t, r, wire, nil, 0)
	retentionAblationCheckWire(t, r, wire)
	r.close()
}

//go:noinline
func retentionAblationStage(t testing.TB, r *candidateResolver, wire []byte, blockOwners []*cell.Cell, index int) retentionAblationRoots {
	t.Helper()
	artifact, lazy, err := r.codec.decodeDeferred(wire, nil)
	if err != nil {
		t.Fatal(err)
	}
	if artifact.preparedBlock == nil {
		t.Fatal("diagnostic did not take the deployed Protocol-1 detached-block route")
	}
	parsed := lazy.roots.Load()
	if blockOwners != nil {
		// Protocol 1 exposes a detached block graph to applied state/liveview.
		// Pin only that same root, not its artifact, hashes, or collated arena.
		blockOwners[index] = parsed.block
	}
	roots := retentionAblationRoots{block: weak.Make(parsed.block), collated: weak.Make(parsed.collated[0])}
	if err := r.stageDeferred(artifact, lazy); err != nil {
		t.Fatal(err)
	}
	return roots
}

//go:noinline
func retentionAblationHeap() uint64 {
	// One GC moves pool objects to victim caches; the second removes them.
	// These forced collections run only in the isolated local diagnostic.
	runtime.GC()
	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats.HeapAlloc
}

//go:noinline
func retentionAblationLive(roots []retentionAblationRoots) (int, int) {
	var block, collated int
	for _, root := range roots {
		if root.block.Value() != nil {
			block++
		}
		if root.collated.Value() != nil {
			collated++
		}
	}
	return block, collated
}

//go:noinline
func retentionAblationCheckWire(t testing.TB, r *candidateResolver, expected []byte) {
	t.Helper()
	data, err := simplex.ParseCandidateData(expected)
	if err != nil {
		t.Fatal(err)
	}
	// The fixture has one full block payload and no delegation, so its slot is
	// the sole changing identity field between independently decoded copies.
	block, ok := data.(simplex.ConsensusBlockData)
	if !ok {
		t.Fatalf("unexpected fixture wire type %T", data)
	}
	var id simplex.CandidateID
	for candidateID := range r.entries {
		if candidateID.Slot == uint32(block.Slot) {
			id = candidateID
			break
		}
	}
	response, err := r.response(context.Background(), simplex.PeerID{}, CandidateRequest{
		SessionID: r.sessionID, ID: id, WantCandidate: true,
	})
	if err != nil || !bytes.Equal(response.CandidateWire, expected) {
		t.Fatalf("canonical candidate wire changed after cache retention: %v", err)
	}
}

type retentionCacheObservation struct {
	roots      retentionAblationRoots
	id         ton.BlockIDExt
	blockBytes int
}

type retentionCacheResult struct {
	HashMode           string `json:"hash_mode"`
	Candidates         int    `json:"candidates"`
	RetainedDeltaBytes int64  `json:"retained_delta_bytes"`
	BlockBytes         int    `json:"canonical_block_bytes"`
	LiveBlockRoots     int    `json:"live_block_roots"`
	LiveProofRoots     int    `json:"live_proof_roots"`
	FlushedDeltaBytes  int64  `json:"flushed_delta_bytes"`
}

// TestCandidateLiveBlockCacheIdentityRetention isolates the actual external ID
// owner found in accepted-block publication. The real block cache stores the
// received candidate's Block ID, BOC, and parsed metadata. Once the resolver
// releases its payload, the old ID slices can still own its full decoded graph.
// MarkBlockFlushed deletes this cache entry and must release that ownership.
// One unmodified mainnet fixture is enough: it avoids duplicate cache keys or
// fabricated distinct block IDs. This does not execute full block acceptance.
func TestCandidateLiveBlockCacheIdentityRetention(t *testing.T) {
	hashMode := os.Getenv("GTON_RETENTION_ABLATION_HASH")
	if hashMode == "" {
		t.Skip("set GTON_RETENTION_ABLATION_HASH=interior|detached in an isolated diagnostic process")
	}
	if hashMode != "interior" && hashMode != "detached" {
		t.Fatal("hash mode must be interior or detached")
	}
	wire := retentionAblationWires(t, 1)[0]
	retentionCacheWarm(t, wire)
	r := retentionAblationResolver("budget")
	defer r.close()
	cache := storage.NewLiveBlockCache(storage.DefaultLiveBlockCacheMaxBlocks, storage.DefaultLiveBlockCacheMaxBytes)
	baseline := retentionAblationHeap()
	observation := retentionCachePublish(t, r, cache, wire)
	retained := retentionAblationHeap()
	liveBlock, liveProof := retentionAblationLive([]retentionAblationRoots{observation.roots})
	wantLive := 0
	if hashMode == "interior" {
		wantLive = 1
	}
	if liveBlock != wantLive || liveProof != wantLive {
		t.Fatalf("external cache roots block=%d proof=%d, want %d", liveBlock, liveProof, wantLive)
	}
	if stats := r.cacheProjection(); stats.Candidates != 0 || stats.DecodedBytes != 0 || stats.Bytes != 0 {
		t.Fatalf("resolver still retains candidate payload: %+v", stats)
	}
	retentionCacheCheckData(t, cache, observation.id)

	cache.MarkBlockFlushed(observation.id)
	flushed := retentionAblationHeap()
	if cache.HasBlockData(observation.id) {
		t.Fatal("MarkBlockFlushed did not remove the cache entry")
	}
	if block, proof := retentionAblationLive([]retentionAblationRoots{observation.roots}); block != 0 || proof != 0 {
		t.Fatalf("flushed cache retains block=%d proof=%d roots", block, proof)
	}
	result := retentionCacheResult{
		HashMode: hashMode, Candidates: 1,
		RetainedDeltaBytes: int64(retained) - int64(baseline),
		BlockBytes:         observation.blockBytes, LiveBlockRoots: liveBlock, LiveProofRoots: liveProof,
		FlushedDeltaBytes: int64(flushed) - int64(baseline),
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("CACHE_ID_ABLATION %s", encoded)
	runtime.KeepAlive(cache)
	runtime.KeepAlive(r)
	runtime.KeepAlive(wire)
	runtime.KeepAlive(observation)
}

func retentionCacheWarm(t testing.TB, wire []byte) {
	t.Helper()
	r := retentionAblationResolver("budget")
	cache := storage.NewLiveBlockCache(storage.DefaultLiveBlockCacheMaxBlocks, storage.DefaultLiveBlockCacheMaxBytes)
	observation := retentionCachePublish(t, r, cache, wire)
	cache.MarkBlockFlushed(observation.id)
	r.close()
}

//go:noinline
func retentionCachePublish(t testing.TB, r *candidateResolver, cache *storage.LiveBlockCache, wire []byte) retentionCacheObservation {
	t.Helper()
	artifact, lazy, err := r.codec.decodeDeferred(wire, nil)
	if err != nil {
		t.Fatal(err)
	}
	parsed := lazy.roots.Load()
	if artifact.preparedBlock == nil {
		t.Fatal("diagnostic did not take the Protocol-1 detached-block route")
	}
	meta, err := storage.BuildBlockMetaFromBlockCell(artifact.Candidate.Block, parsed.block)
	if err != nil {
		t.Fatal(err)
	}
	observation := retentionCacheObservation{
		roots: retentionAblationRoots{block: weak.Make(parsed.block), collated: weak.Make(parsed.collated[0])},
		// The test's lookup/flush key must not itself hold the old interior slices.
		id: *artifact.Candidate.Block.Copy(), blockBytes: len(artifact.BlockBOC),
	}
	if err := r.stageDeferred(artifact, lazy); err != nil {
		t.Fatal(err)
	}
	if err := cache.PublishLiveBlockArtifacts(storage.LiveBlockCacheArtifacts{
		Block: artifact.Candidate.Block, BlockData: artifact.BlockBOC, Meta: meta,
	}); err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	r.releasePayloadLocked(artifact.Candidate.ID, r.entries[artifact.Candidate.ID])
	r.mu.Unlock()
	return observation
}

//go:noinline
func retentionCacheCheckData(t testing.TB, cache *storage.LiveBlockCache, id ton.BlockIDExt) {
	t.Helper()
	data, err := cache.BlockData(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(data)
	if !bytes.Equal(hash[:], id.FileHash) {
		t.Fatal("cached block bytes do not match canonical identity")
	}
	meta, err := cache.BlockMeta(context.Background(), id)
	if err != nil || !meta.ID.Equals(&id) {
		t.Fatalf("cached metadata does not preserve the block identity: %v", err)
	}
}
