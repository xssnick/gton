package collator

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
	"github.com/xssnick/gton/service/validator/simplex"
)

// builtCommitFixture is one shard candidate plus the managed session state a
// commit needs, built from the shared empty-candidate request.
type builtCommitFixture struct {
	acquisition *LocalAcquisition
	pool        *msgpool.Pool
	managed     *localAcquisitionSession
	request     BuildRequest
	artifact    CandidateArtifact
	built       *Candidate
	shardReq    ShardRequest
	genesisKey  [32]byte
}

func newBuiltCommitFixture(t *testing.T, shardReq ShardRequest) *builtCommitFixture {
	t.Helper()

	built, err := testBuilder().BuildShard(context.Background(), shardReq)
	if err != nil {
		t.Fatal(err)
	}

	pool := msgpool.New(msgpool.Config{})
	t.Cleanup(pool.Close)
	destination := blockShardIdent(shardReq.Previous.ID)
	if err = pool.Internals().ReconcileDestinations([]msgpool.ShardIdent{destination}); err != nil {
		t.Fatal(err)
	}

	sessionID := [32]byte{0x71}
	session := ActivatedSession{
		Session: Session{
			ID:         sessionID,
			Shard:      shardReq.Shard,
			Validators: []SessionValidator{{}},
		},
		Genesis: []ton.BlockIDExt{shardReq.Previous.ID},
	}
	update := SessionUpdate{SessionID: sessionID}
	activation := SessionActivation{SessionID: sessionID, Genesis: session.Genesis}
	managed := &localAcquisitionSession{
		session:    session.Session,
		branch:     openLocalTestBranch(t, pool, destination),
		activation: &activation,
		update:     update,
		candidates: make(map[simplex.CandidateID]localCandidateState),
		blocks:     make(map[[32]byte]localCandidateState),
	}
	genesisKey, err := blockRootKey(shardReq.Previous.ID)
	if err != nil {
		t.Fatal(err)
	}
	managed.blocks[genesisKey] = localCandidateState{block: shardReq.Previous}

	request := BuildRequest{Session: session, Update: update, Slot: 2}
	artifact := CandidateArtifact{
		SessionID: sessionID,
		Candidate: simplex.Candidate{
			ID:    simplex.CandidateID{Slot: request.Slot, Hash: [32]byte{0x72}},
			Block: built.ID,
		},
		BlockBOC: built.BlockBOC,
	}

	acquisition := &LocalAcquisition{
		messages: &localAcquisitionFailingMessages{pool: pool},
		sessions: map[[32]byte]*localAcquisitionSession{sessionID: managed},
	}

	return &builtCommitFixture{
		acquisition: acquisition,
		pool:        pool,
		managed:     managed,
		request:     request,
		artifact:    artifact,
		built:       built,
		shardReq:    shardReq,
		genesisKey:  genesisKey,
	}
}

func (f *builtCommitFixture) commit(t *testing.T) localCandidateState {
	t.Helper()

	if err := f.acquisition.commitCandidateLocked(
		context.Background(),
		f.managed,
		f.request,
		f.built,
		f.artifact,
	); err != nil {
		t.Fatalf("commit candidate: %v", err)
	}
	state, exists := f.managed.candidates[f.artifact.Candidate.ID]
	if !exists {
		t.Fatal("commit recorded no candidate state")
	}

	return state
}

// TestCommitReusesBuiltDerivation is the whole point of the capsule: committing
// a block this node built must not deserialize it again, and must not apply its
// state update a second time. Both are proven by identity, because an equal
// value would also be produced by doing the work.
func TestCommitReusesBuiltDerivation(t *testing.T) {
	fixture := newBuiltCommitFixture(t, emptyCandidateRequest(t))
	if fixture.built.built == nil {
		t.Fatal("the canonical build path produced no derivation capsule")
	}

	before := candidateBlockParses.Load()
	state := fixture.commit(t)
	if parses := candidateBlockParses.Load() - before; parses != 0 {
		t.Fatalf("committing our own block parsed it %d times, want none", parses)
	}
	if state.block.Block != fixture.built.built.root {
		t.Fatal("commit recorded a block root other than the one the builder serialized")
	}
	if state.block.State != fixture.built.State {
		t.Fatal("commit recorded a state other than the one the builder published")
	}
	if fixture.acquisition.builtBindMisses.Load() != 0 {
		t.Fatal("the capsule did not bind its own commit chain")
	}
}

// TestCommitBuiltAndReplayedDerivationsAgree is the differential gate. The two
// derivations must be interchangeable in everything a later build reads: the
// roots, the queue accounting, and every field of the message-pool delta the
// out-message walk produces.
func TestCommitBuiltAndReplayedDerivationsAgree(t *testing.T) {
	for _, shape := range []struct {
		name    string
		request func(testing.TB) ShardRequest
	}{
		{"first-block", func(tb testing.TB) ShardRequest { return emptyCandidateRequest(tb) }},
		{"successor", func(tb testing.TB) ShardRequest {
			return advanceCandidateRequest(tb, emptyCandidateRequest(tb))
		}},
	} {
		t.Run(shape.name, func(t *testing.T) {
			shardReq := shape.request(t)
			fixture := newBuiltCommitFixture(t, shardReq)

			fast, bound := fixture.built.built.bind(
				fixture.built.ID,
				fixture.built.State,
				fixture.built.StateUpdate,
				[]PreviousBlock{shardReq.Previous},
			)
			if !bound {
				t.Fatal("the capsule did not bind the chain it was built over")
			}
			replayed, err := deriveCommitByReplay(fixture.built, []PreviousBlock{shardReq.Previous})
			if err != nil {
				t.Fatalf("replay derivation: %v", err)
			}

			if fast.root.HashKeyAt(0) != replayed.root.HashKeyAt(0) {
				t.Fatal("block roots differ")
			}
			if fast.state.HashKeyAt(0) != replayed.state.HashKeyAt(0) {
				t.Fatal("successor state roots differ")
			}
			if fast.startLT != replayed.startLT {
				t.Fatalf("start LT %d != %d", fast.startLT, replayed.startLT)
			}
			if fast.genUTime != replayed.genUTime {
				t.Fatalf("generation time %d != %d", fast.genUTime, replayed.genUTime)
			}
			if fast.outQueueSize != replayed.outQueueSize || fast.outQueueSize != fixture.built.Stats.OutQueueSize {
				t.Fatalf(
					"queue sizes built=%d fast=%d replayed=%d",
					fixture.built.Stats.OutQueueSize,
					fast.outQueueSize,
					replayed.outQueueSize,
				)
			}
			if !equalCandidateStorageStats(fast.storageStats, replayed.storageStats) {
				t.Fatal("storage statistics differ")
			}
			if !equalCandidateExternals(fast.externals, replayed.externals) {
				t.Fatal("external feedback differs")
			}

			// The out-message walk is the one consumer that reads deep into the
			// block rather than just its hash, so it is compared entry by entry.
			source := targetShardIdent(shardReq.Shard)
			ref := msgpool.SourceRef{Seqno: fixture.built.ID.SeqNo, RootHash: [32]byte(fixture.built.ID.RootHash)}
			fastDelta, err := fixture.managed.branch.DeltaFromBlockRoot(source, ref, fast.root, fast.startLT)
			if err != nil {
				t.Fatalf("delta over the built root: %v", err)
			}
			replayDelta, err := fixture.managed.branch.DeltaFromBlockRoot(source, ref, replayed.root, replayed.startLT)
			if err != nil {
				t.Fatalf("delta over the replayed root: %v", err)
			}
			requireSameDelta(t, fastDelta, replayDelta)
		})
	}
}

func requireSameDelta(t *testing.T, fast, replayed *msgpool.InternalsDelta) {
	t.Helper()

	if len(fast.Added) != len(replayed.Added) {
		t.Fatalf("added messages %d != %d", len(fast.Added), len(replayed.Added))
	}
	if fast.AddedTotal != replayed.AddedTotal || fast.RemovedTotal != replayed.RemovedTotal {
		t.Fatalf("delta totals (%d,%d) != (%d,%d)",
			fast.AddedTotal, fast.RemovedTotal, replayed.AddedTotal, replayed.RemovedTotal)
	}
	for i := range fast.Added {
		left, right := fast.Added[i], replayed.Added[i]
		if left.Key != right.Key || left.EnqueuedLT != right.EnqueuedLT ||
			left.QueueLT != right.QueueLT || left.EnvHash != right.EnvHash {
			t.Fatalf("added message %d differs", i)
		}
		if left.Root.HashKeyAt(0) != right.Root.HashKeyAt(0) ||
			left.EnvelopeCell.HashKeyAt(0) != right.EnvelopeCell.HashKeyAt(0) {
			t.Fatalf("added message %d carries different cells", i)
		}
	}
	if len(fast.RemovedKeys) != len(replayed.RemovedKeys) {
		t.Fatalf("removed keys %d != %d", len(fast.RemovedKeys), len(replayed.RemovedKeys))
	}
	for i := range fast.RemovedKeys {
		if fast.RemovedKeys[i] != replayed.RemovedKeys[i] {
			t.Fatalf("removed key %d differs", i)
		}
	}
	if len(fast.RemovedEnvHashes) != len(replayed.RemovedEnvHashes) {
		t.Fatalf("removed envelope hashes %d != %d",
			len(fast.RemovedEnvHashes), len(replayed.RemovedEnvHashes))
	}
	for i := range fast.RemovedEnvHashes {
		if fast.RemovedEnvHashes[i] != replayed.RemovedEnvHashes[i] {
			t.Fatalf("removed envelope hash %d differs", i)
		}
	}
}

// TestNextBlockCollatedDataIdenticalOverBuiltPredecessor is the bit-exactness
// gate for the highest-stakes consumer of the reused block root: the next
// block's collated data carries a Merkle proof built from it, and those bytes go
// out on the wire and are checked by every other validator.
func TestNextBlockCollatedDataIdenticalOverBuiltPredecessor(t *testing.T) {
	base := advanceCandidateRequest(t, emptyCandidateRequest(t))
	base.Masterchain.Config.capabilities |= capFullCollatedData
	attachFullCollatedTestNeighbors(t, &base)

	first, err := testBuilder().BuildShard(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}
	if first.built == nil {
		t.Fatal("the canonical build path produced no derivation capsule")
	}
	fastDerivation, bound := first.built.bind(first.ID, first.State, first.StateUpdate, []PreviousBlock{base.Previous})
	if !bound {
		t.Fatal("the capsule did not bind the chain it was built over")
	}
	replayDerivation, err := deriveCommitByReplay(first, []PreviousBlock{base.Previous})
	if err != nil {
		t.Fatal(err)
	}

	successor := func(derivation commitDerivation) *Candidate {
		t.Helper()

		queueSize := first.Stats.OutQueueSize
		next := base
		next.Previous = PreviousBlock{
			ID:           first.ID,
			Block:        derivation.root,
			State:        derivation.state,
			OutQueueSize: &queueSize,
		}
		next.Header.GenUtime++
		next.Header.GenUtimeMS = uint64(next.Header.GenUtime) * 1_000
		// The neighbour set has to follow the predecessor: the self neighbour is
		// the block being reused, and its proofs are exactly the ones the build
		// derives from the roots under test.
		attachFullCollatedTestNeighbors(t, &next)
		candidate, buildErr := testBuilder().BuildShard(context.Background(), next)
		if buildErr != nil {
			t.Fatalf("build successor: %v", buildErr)
		}

		return candidate
	}

	overBuilt := successor(fastDerivation)
	overReplay := successor(replayDerivation)
	if !bytes.Equal(overBuilt.BlockBOC, overReplay.BlockBOC) {
		t.Fatal("the successor block differs over the reused predecessor")
	}
	if !bytes.Equal(overBuilt.CollatedData, overReplay.CollatedData) {
		t.Fatal("the successor collated data differs over the reused predecessor")
	}
	// Equality is only meaningful if the previous-block proof — the one built
	// from the reused root — is actually in there.
	roots, err := cell.FromBOCMultiRoot(overBuilt.CollatedData)
	if err != nil {
		t.Fatalf("decode successor collated data: %v", err)
	}
	proven := false
	for _, root := range roots {
		if root.GetType() != cell.MerkleProofCellType {
			continue
		}
		if _, unwrapErr := cell.UnwrapProof(root, first.ID.RootHash); unwrapErr == nil {
			proven = true
			break
		}
	}
	if !proven {
		t.Fatalf("successor collated data (%d roots, %d bytes) carries no proof of the reused block root",
			len(roots), len(overBuilt.CollatedData))
	}
}

// TestBuiltCapsuleDoesNotBindForeignChain: the capsule names the predecessors
// its update was applied over, so it cannot be spent on any other chain. The
// fallback is the ordinary replay, which produces the same answer at full price
// and says so in the counter.
func TestBuiltCapsuleDoesNotBindForeignChain(t *testing.T) {
	shardReq := emptyCandidateRequest(t)
	fixture := newBuiltCommitFixture(t, shardReq)

	foreign := shardReq.Previous
	foreign.State = fixture.built.State
	if _, bound := fixture.built.built.bind(
		fixture.built.ID,
		fixture.built.State,
		fixture.built.StateUpdate,
		[]PreviousBlock{foreign},
	); bound {
		t.Fatal("the capsule bound a chain it was not built over")
	}
	if _, bound := fixture.built.built.bind(
		fixture.built.ID,
		fixture.built.State,
		fixture.built.StateUpdate,
		[]PreviousBlock{shardReq.Previous, shardReq.Previous},
	); bound {
		t.Fatal("the capsule bound a merge chain although it was built over one predecessor")
	}
	other := fixture.built.ID
	other.RootHash = bytes.Repeat([]byte{0x5a}, 32)
	if _, bound := fixture.built.built.bind(
		other,
		fixture.built.State,
		fixture.built.StateUpdate,
		[]PreviousBlock{shardReq.Previous},
	); bound {
		t.Fatal("the capsule bound a different block id")
	}
	other = fixture.built.ID
	other.SeqNo++
	if _, bound := fixture.built.built.bind(
		other,
		fixture.built.State,
		fixture.built.StateUpdate,
		[]PreviousBlock{shardReq.Previous},
	); bound {
		t.Fatal("the capsule bound a different block sequence number")
	}
	foreignState := cell.BeginCell().MustStoreUInt(0x57a7e, 32).EndCell()
	if _, bound := fixture.built.built.bind(
		fixture.built.ID,
		foreignState,
		fixture.built.StateUpdate,
		[]PreviousBlock{shardReq.Previous},
	); bound {
		t.Fatal("the capsule bound a successor state it did not build")
	}
	foreignUpdate := cell.BeginCell().MustStoreUInt(0xadd, 32).EndCell()
	if _, bound := fixture.built.built.bind(
		fixture.built.ID,
		fixture.built.State,
		foreignUpdate,
		[]PreviousBlock{shardReq.Previous},
	); bound {
		t.Fatal("the capsule bound a state update it did not build")
	}

	// A capsule that does not bind falls back to the replay and records the miss.
	fixture.built.built.parents[0] = cell.Hash{}
	before := candidateBlockParses.Load()
	state := fixture.commit(t)
	if parses := candidateBlockParses.Load() - before; parses != 1 {
		t.Fatalf("fallback parsed the block %d times, want one", parses)
	}
	if fixture.acquisition.builtBindMisses.Load() != 1 {
		t.Fatalf("bind misses = %d, want one", fixture.acquisition.builtBindMisses.Load())
	}
	if state.block.State.HashKeyAt(0) != fixture.built.State.HashKeyAt(0) {
		t.Fatal("the replayed fallback recorded a different state")
	}
}

// TestRestoreCandidateNeverTakesBuiltPath: a candidate this node did not build
// carries no capsule and cannot acquire one, so it is replayed — and the
// verification the replay performs is still the only thing binding it.
func TestRestoreCandidateNeverTakesBuiltPath(t *testing.T) {
	shardReq := emptyCandidateRequest(t)
	fixture := newBuiltCommitFixture(t, shardReq)
	before := candidateBlockParses.Load()
	if err := fixture.acquisition.RestoreCandidate(
		context.Background(),
		fixture.request,
		fixture.artifact,
	); err != nil {
		t.Fatalf("restore candidate: %v", err)
	}
	// One parse, not two: the restore's own parse is handed to the commit rather
	// than repeated there.
	if parses := candidateBlockParses.Load() - before; parses != 1 {
		t.Fatalf("restore parsed the block %d times, want exactly one", parses)
	}
	state, exists := fixture.managed.candidates[fixture.artifact.Candidate.ID]
	if !exists {
		t.Fatal("restore recorded no candidate state")
	}
	if state.block.Block == fixture.built.built.root {
		t.Fatal("the restore path reused a capsule it must not have")
	}
	if state.block.State.HashKeyAt(0) != fixture.built.State.HashKeyAt(0) {
		t.Fatal("the restored state differs from the built one")
	}
}

func TestRestoreCandidatePreservesOutQueueSizeForNextBuild(t *testing.T) {
	shardReq := emptyCandidateRequest(t)
	shardReq.Previous.State = previousStateWithDenseOutQueue(t, shardReq.Previous.State, 1)
	queueSize := uint64(1)
	shardReq.Previous.OutQueueSize = &queueSize
	shardReq.Internals = &msgpool.Cut{More: true}

	fixture := newBuiltCommitFixture(t, shardReq)
	if fixture.built.Stats.OutQueueSize != queueSize {
		t.Fatalf("built queue size = %d, want %d", fixture.built.Stats.OutQueueSize, queueSize)
	}
	if err := fixture.acquisition.RestoreCandidate(
		context.Background(),
		fixture.request,
		fixture.artifact,
	); err != nil {
		t.Fatalf("restore candidate: %v", err)
	}

	state, exists := fixture.managed.candidates[fixture.artifact.Candidate.ID]
	if !exists {
		t.Fatal("restore recorded no candidate state")
	}
	if state.block.OutQueueSize == nil {
		t.Fatal("restored queue size is absent")
	}
	if got := *state.block.OutQueueSize; got != queueSize {
		t.Fatalf("restored queue size = %d, want %d", got, queueSize)
	}

	next := shardReq
	next.Previous = state.block
	next.Header.GenUtime++
	next.Header.GenUtimeMS = uint64(next.Header.GenUtime) * 1_000
	if _, err := testBuilder().BuildShard(context.Background(), next); err != nil {
		t.Fatalf("build over restored candidate: %v", err)
	}
}

// TestRestoreCandidateStillVerifiesACorruptedArtifact keeps the replay path's
// checks where they are: they are the only binding a from-disk tree has.
func TestRestoreCandidateStillVerifiesACorruptedArtifact(t *testing.T) {
	shardReq := emptyCandidateRequest(t)
	fixture := newBuiltCommitFixture(t, shardReq)
	corrupted := fixture.artifact
	corrupted.BlockBOC = append(bytes.Clone(corrupted.BlockBOC), 0x00)
	err := fixture.acquisition.RestoreCandidate(context.Background(), fixture.request, corrupted)
	if err == nil || !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("restore of a corrupted artifact error = %v, want ErrInvalidInput", err)
	}
	if len(fixture.managed.candidates) != 0 {
		t.Fatal("a rejected restore recorded candidate state")
	}
}

// TestBuiltBlockRootCarriesNoLiveRecorder is the permanent guard for the one
// hazard the reuse introduces. A produced block embeds a few cells that were
// read out of the predecessor — envelopes reimported from our own out-queue —
// and those cells carry the collation recorder's trace. Re-parsing the BOC used
// to launder that away by accident; now the recorder is sealed instead, so a
// retained block neither keeps the record alive nor records into it.
func TestBuiltBlockRootCarriesNoLiveRecorder(t *testing.T) {
	for _, shape := range []struct {
		name    string
		request func(testing.TB) ShardRequest
	}{
		{"first-block", func(tb testing.TB) ShardRequest { return emptyCandidateRequest(tb) }},
		{"successor", func(tb testing.TB) ShardRequest {
			return advanceCandidateRequest(tb, emptyCandidateRequest(tb))
		}},
		{"mainnet", func(tb testing.TB) ShardRequest {
			request, _ := fullCollatedMainnetCandidate(tb)

			return request
		}},
	} {
		t.Run(shape.name, func(t *testing.T) {
			shardReq := shape.request(t)
			built, err := testBuilder().BuildShard(context.Background(), shardReq)
			if err != nil {
				t.Fatal(err)
			}
			if built.built == nil {
				t.Fatal("the canonical build path produced no derivation capsule")
			}
			requireNoLiveRecorder(t, built.built.root)
			requireNoLiveRecorder(t, built.State)
		})
	}
}

func requireNoLiveRecorder(t *testing.T, root *cell.Cell) {
	t.Helper()

	seen := make(map[cell.Hash]struct{}, 8192)
	var walk func(c *cell.Cell)
	walk = func(c *cell.Cell) {
		if c == nil || t.Failed() {
			return
		}
		key := c.HashKeyAt(0)
		if _, exists := seen[key]; exists {
			return
		}
		seen[key] = struct{}{}
		// A sealed recorder returns no child trace, so descent through such a
		// cell neither records nor copies. That is the property; a lingering
		// trace pointer to a gutted recorder is inert.
		if trace := c.Trace(); trace != nil && trace.Child(0) != nil {
			t.Fatalf("cell %x still descends into a live recorder", key)
		}
		for i := range int(c.RefsNum()) {
			ref, err := c.PeekRef(i)
			if err != nil {
				t.Fatalf("peek ref %d: %v", i, err)
			}
			walk(ref)
		}
	}
	walk(root)
}

// TestCommitOverLazyPredecessorLoadsLessThanReplay is the lazy-load gate. The
// removed replay walks the resident predecessor along the update's old proof and
// re-walks its accounts for verifyPredecessor, and both materialize lazy cells;
// the reused block root is already fully materialized, because BOC
// serialization walked every reference to write it. The assertion is relative,
// so it stays meaningful as the fixture and the surrounding code move.
func TestCommitOverLazyPredecessorLoadsLessThanReplay(t *testing.T) {
	// Runs in the shared-fixture parallel batch: it reads the cached mainnet
	// workload and keeps every mutation on its own copy, holds no package-level
	// counter and derives nothing from wall-clock timing.
	t.Parallel()
	measure := func(forceReplay bool) int {
		req, _ := benchMainnetRequest(t, benchMainnetFiller)
		lazifier := newAdvLazifier()
		req.Previous.State = lazifier.root(t, req.Previous.State)

		fixture := newBuiltCommitFixture(t, req)
		lazifier.mu.Lock()
		afterBuild := lazifier.calls
		lazifier.mu.Unlock()

		if forceReplay {
			fixture.built.built.parents[0] = cell.Hash{}
		}
		fixture.commit(t)

		lazifier.mu.Lock()
		defer lazifier.mu.Unlock()

		return lazifier.calls - afterBuild
	}

	built := measure(false)
	replayed := measure(true)
	t.Logf("commit lazy loads: built derivation %d, replayed derivation %d", built, replayed)
	if built >= replayed {
		t.Fatalf("the reused derivation loaded %d cells, the replay %d: it must load strictly fewer",
			built, replayed)
	}
}
