package service

import (
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"reflect"
	"testing"
	"unsafe"

	"github.com/rs/zerolog"
	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestMasterchainValidatorCacheResetsByPrevKeyBlock(t *testing.T) {
	var cache masterchainValidatorCache

	firstKey := masterchainValidatorCacheKey{prevKeyBlockSeqno: 100, catchainSeqno: 7, validatorSetHash: 11}
	firstValidators := &blockproof.PreparedValidatorSet{}
	cache.put(firstKey, firstValidators)

	if validators, err := cache.get(firstKey); err != nil || validators != firstValidators {
		t.Fatal("expected validator cache hit before epoch reset")
	}

	secondKey := masterchainValidatorCacheKey{prevKeyBlockSeqno: 200, catchainSeqno: 8, validatorSetHash: 22}
	secondValidators := &blockproof.PreparedValidatorSet{}
	cache.put(secondKey, secondValidators)

	if _, err := cache.get(firstKey); err == nil {
		t.Fatal("expected validator cache miss after prev key block changed")
	}
	if validators, err := cache.get(secondKey); err != nil || validators != secondValidators {
		t.Fatal("expected validator cache hit for the new epoch")
	}
}

func TestMasterchainValidatorCacheKeepsEpochVariants(t *testing.T) {
	var cache masterchainValidatorCache

	firstKey := masterchainValidatorCacheKey{prevKeyBlockSeqno: 100, catchainSeqno: 7, validatorSetHash: 11}
	secondKey := masterchainValidatorCacheKey{prevKeyBlockSeqno: 100, catchainSeqno: 8, validatorSetHash: 22}
	firstValidators := &blockproof.PreparedValidatorSet{}
	secondValidators := &blockproof.PreparedValidatorSet{}

	cache.put(firstKey, firstValidators)
	cache.put(secondKey, secondValidators)

	if validators, err := cache.get(firstKey); err != nil || validators != firstValidators {
		t.Fatal("expected first validator set to stay cached inside the epoch")
	}
	if validators, err := cache.get(secondKey); err != nil || validators != secondValidators {
		t.Fatal("expected second validator set to stay cached inside the epoch")
	}
}

func TestMasterchainValidatorCacheReusesPreparedSet(t *testing.T) {
	var cache masterchainValidatorCache
	key := masterchainValidatorCacheKey{prevKeyBlockSeqno: 100, catchainSeqno: 7, validatorSetHash: 11}
	prepared := &blockproof.PreparedValidatorSet{}
	replacement := &blockproof.PreparedValidatorSet{}

	if cached := cache.put(key, prepared); cached != prepared {
		t.Fatal("expected first prepared validator set to be cached")
	}
	if cached := cache.put(key, replacement); cached != prepared {
		t.Fatal("expected repeated put to reuse the cached prepared validator set")
	}
	if cached, err := cache.get(key); err != nil || cached != prepared {
		t.Fatal("expected cache hit to return the original prepared validator set")
	}
}

func BenchmarkMasterchainValidatorCacheHit(b *testing.B) {
	key := masterchainValidatorCacheKey{
		prevKeyBlockSeqno: 100,
		catchainSeqno:     7,
		validatorSetHash:  11,
	}
	prepared := &blockproof.PreparedValidatorSet{}
	var cache masterchainValidatorCache
	cache.put(key, prepared)

	b.ReportAllocs()
	for b.Loop() {
		cached, err := cache.get(key)
		if err != nil || cached != prepared {
			b.Fatal("cache miss")
		}
	}
}

func TestMasterchainConsensusAcceptsConfiguredHardforkAfterSignatureError(t *testing.T) {
	prev := testBlockID(-1, topShard, 100)
	hardfork := testBlockID(-1, topShard, 101)
	current := testConsensusState(prev)
	svc := testServiceWithHardfork(t, hardfork)
	proof := testHardforkConsensusProof(current, hardfork, errors.New("signature check failed"))

	checked, err := svc.checkMasterchainBlockConsensusWithProof(current, proof)
	if err != nil {
		t.Fatalf("check hardfork consensus: %v", err)
	}
	if checked == nil || !checked.block.Equals(&hardfork) || !checked.current.Equals(&prev) {
		t.Fatalf("unexpected checked consensus: %+v", checked)
	}
	if !proof.hardforkChecked {
		t.Fatal("hardfork proof was not marked checked")
	}
}

func TestMasterchainConsensusRejectsUnconfiguredHardforkFallback(t *testing.T) {
	prev := testBlockID(-1, topShard, 100)
	block := testBlockID(-1, topShard, 101)
	current := testConsensusState(prev)
	cause := errors.New("signature check failed")
	svc := &SyncCoordinator{log: zerolog.Nop(), node: &p2p.Node{}}
	proof := testHardforkConsensusProof(current, block, cause)

	_, err := svc.checkMasterchainBlockConsensusWithProof(current, proof)
	if !errors.Is(err, cause) {
		t.Fatalf("error = %v, want original signature error", err)
	}
}

func TestMasterchainConsensusRejectsHardforkWithoutVerticalIncrement(t *testing.T) {
	prev := testBlockID(-1, topShard, 100)
	hardfork := testBlockID(-1, topShard, 101)
	current := testConsensusState(prev)
	svc := testServiceWithHardfork(t, hardfork)
	proof := testHardforkConsensusProof(current, hardfork, errors.New("signature check failed"))
	proof.vertSeqnoIncr = false

	_, err := svc.checkMasterchainBlockConsensusWithProof(current, proof)
	if err == nil {
		t.Fatal("expected hardfork without vert_seqno_incr to fail")
	}
}

func TestMasterchainConsensusCheckedPathDoesNotLookupHardfork(t *testing.T) {
	prev := testBlockID(-1, topShard, 100)
	block := testBlockID(-1, topShard, 101)
	current := testConsensusState(prev)
	svc := &SyncCoordinator{log: zerolog.Nop()}
	proof := testHardforkConsensusProof(current, block, nil)
	proof.signaturesChecked = true

	if _, err := svc.checkMasterchainBlockConsensusWithProof(current, proof); err != nil {
		t.Fatalf("check consensus: %v", err)
	}
}

// The prepare-stage signature preverify may mark a proof checked only after a
// passing Ed25519 batch: a validator-cache miss and a failing batch must both
// leave the proof for the full apply-time check, which owns the hardfork
// fallback and the cache population.
func TestPreverifyMasterchainConsensusSignatures(t *testing.T) {
	block := testMasterBlockID(101)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate validator key: %v", err)
	}
	prepared, err := blockproof.PrepareValidatorSet(7, []*tlb.ValidatorAddr{{
		PublicKey: tlb.SigPubKeyED25519{Key: pub},
		Weight:    1,
		ADNLAddr:  make([]byte, 32),
	}})
	if err != nil {
		t.Fatalf("prepare validator set: %v", err)
	}
	payload, err := tl.Serialize(ton.BlockID{RootHash: block.RootHash, FileHash: block.FileHash}, true)
	if err != nil {
		t.Fatalf("build signature payload: %v", err)
	}
	nodeID, err := tl.Hash(keys.PublicKeyED25519{Key: pub})
	if err != nil {
		t.Fatalf("hash validator key: %v", err)
	}
	key := masterchainValidatorCacheKey{prevKeyBlockSeqno: 1, catchainSeqno: 7, validatorSetHash: prepared.Hash()}
	proof := &masterchainConsensusProof{
		block:             block,
		validatorCacheKey: key,
		proofSignatures: &blockproof.BlockSignatureSet{
			ValidatorSignatures: blockproof.NewOrdinaryValidatorSignatureSet(7, prepared.Hash(), []ton.Signature{{
				NodeIDShort: nodeID,
				Signature:   ed25519.Sign(priv, payload),
			}}),
			DeclaredWeight: 1,
		},
	}

	svc := &SyncCoordinator{log: zerolog.Nop()}
	svc.preverifyMasterchainConsensusSignatures(proof)
	if proof.signaturesChecked {
		t.Fatal("preverify marked the proof checked on a validator cache miss")
	}

	svc.validatorCache.put(key, prepared)
	svc.preverifyMasterchainConsensusSignatures(proof)
	if !proof.signaturesChecked {
		t.Fatal("preverify did not mark a valid proof with a cached validator set")
	}

	// A failing batch must not mark: apply re-runs the full check and owns the
	// failure path.
	badProof := &masterchainConsensusProof{
		block:             block,
		validatorCacheKey: key,
		proofSignatures: &blockproof.BlockSignatureSet{
			ValidatorSignatures: blockproof.NewOrdinaryValidatorSignatureSet(7, prepared.Hash(), []ton.Signature{{
				NodeIDShort: nodeID,
				Signature:   make([]byte, 64),
			}}),
			DeclaredWeight: 1,
		},
	}
	svc.preverifyMasterchainConsensusSignatures(badProof)
	if badProof.signaturesChecked {
		t.Fatal("preverify marked the proof checked after a failing signature batch")
	}
}

func TestParsedProofCacheDeduplicatesByRootPointer(t *testing.T) {
	downloaded := mustLoadFixtureDownloadedBlock(t)
	proofRoot, err := blockproof.BroadcastProofRoot(downloaded.ID, downloaded.Block)
	if err != nil {
		t.Fatalf("build fixture proof root: %v", err)
	}
	fullRoot, proofBOC, err := blockproof.LinkFromRoot(downloaded.ID, proofRoot)
	if err != nil {
		t.Fatalf("build fixture proof link: %v", err)
	}

	var cache parsedProofCache
	first, err := cache.parse(downloaded.ID, fullRoot)
	if err != nil {
		t.Fatalf("parse fixture proof: %v", err)
	}
	second, err := cache.parse(downloaded.ID, fullRoot)
	if err != nil {
		t.Fatalf("parse fixture proof again: %v", err)
	}
	if first != second {
		t.Fatal("same proof root pointer was parsed twice")
	}

	// A re-decoded copy of the same proof is a different pointer and must be
	// parsed on its own: pointer identity is what guarantees content identity.
	reparsedRoot, err := cell.FromBOC(proofBOC)
	if err != nil {
		t.Fatalf("reparse proof BOC: %v", err)
	}
	third, err := cache.parse(downloaded.ID, reparsedRoot)
	if err != nil {
		t.Fatalf("parse re-decoded proof: %v", err)
	}
	if third == first {
		t.Fatal("different proof root pointers shared one parse result")
	}
}

func testServiceWithHardfork(tb testing.TB, block ton.BlockIDExt) *SyncCoordinator {
	tb.Helper()

	node := &p2p.Node{}
	field := testReflectField(tb, reflect.ValueOf(node).Elem(), "hardforkSet")

	hardforkSet := reflect.MakeMap(field.Type())
	hardforkSet.SetMapIndex(testHardforkSetKey(tb, field.Type().Key(), block), reflect.ValueOf(struct{}{}))
	setUnexportedReflectValue(field, hardforkSet)

	return &SyncCoordinator{log: zerolog.Nop(), node: node}
}

func testHardforkSetKey(tb testing.TB, typ reflect.Type, block ton.BlockIDExt) reflect.Value {
	tb.Helper()
	if len(block.RootHash) != 32 || len(block.FileHash) != 32 {
		tb.Fatalf("invalid test block hashes: root=%d file=%d", len(block.RootHash), len(block.FileHash))
	}

	key := reflect.New(typ).Elem()
	setUnexportedReflectValue(testReflectField(tb, key, "workchain"), reflect.ValueOf(block.Workchain))
	setUnexportedReflectValue(testReflectField(tb, key, "shard"), reflect.ValueOf(block.Shard))
	setUnexportedReflectValue(testReflectField(tb, key, "seqno"), reflect.ValueOf(block.SeqNo))

	var rootHash [32]byte
	copy(rootHash[:], block.RootHash)
	setUnexportedReflectValue(testReflectField(tb, key, "rootHash"), reflect.ValueOf(rootHash))

	var fileHash [32]byte
	copy(fileHash[:], block.FileHash)
	setUnexportedReflectValue(testReflectField(tb, key, "fileHash"), reflect.ValueOf(fileHash))

	return key
}

func testReflectField(tb testing.TB, value reflect.Value, name string) reflect.Value {
	tb.Helper()

	field := value.FieldByName(name)
	if !field.IsValid() {
		tb.Fatalf("%s has no %s field", value.Type(), name)
	}
	return field
}

func setUnexportedReflectValue(dst reflect.Value, src reflect.Value) {
	reflect.NewAt(dst.Type(), unsafe.Pointer(dst.UnsafeAddr())).Elem().Set(src)
}

func testConsensusState(block ton.BlockIDExt) *storage.BlockState {
	return &storage.BlockState{
		Block: block,
		Cell:  cell.BeginCell().MustStoreUInt(1, 1).EndCell(),
	}
}

func testHardforkConsensusProof(current *storage.BlockState, block ton.BlockIDExt, signatureErr error) *masterchainConsensusProof {
	return &masterchainConsensusProof{
		block:               block,
		prevRef:             current.Block,
		stateUpdateFromHash: current.Cell.Virtualize(0).HashKey(0),
		keyBlock:            true,
		vertSeqnoIncr:       true,
		signaturePrepareErr: signatureErr,
	}
}
