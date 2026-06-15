package service

import (
	"errors"
	"reflect"
	"testing"
	"unsafe"

	"github.com/rs/zerolog"
	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestMasterchainValidatorCacheResetsByPrevKeyBlock(t *testing.T) {
	var cache masterchainValidatorCache

	firstKey := masterchainValidatorCacheKey{prevKeyBlockSeqno: 100, catchainSeqno: 7, validatorSetHash: 11}
	firstValidators := []*tlb.ValidatorAddr{nil}
	cache.put(firstKey, firstValidators)

	if validators, ok := cache.get(firstKey); !ok || len(validators) != len(firstValidators) {
		t.Fatal("expected validator cache hit before epoch reset")
	}

	secondKey := masterchainValidatorCacheKey{prevKeyBlockSeqno: 200, catchainSeqno: 8, validatorSetHash: 22}
	secondValidators := []*tlb.ValidatorAddr{nil, nil}
	cache.put(secondKey, secondValidators)

	if _, ok := cache.get(firstKey); ok {
		t.Fatal("expected validator cache miss after prev key block changed")
	}
	if validators, ok := cache.get(secondKey); !ok || len(validators) != len(secondValidators) {
		t.Fatal("expected validator cache hit for the new epoch")
	}
}

func TestMasterchainValidatorCacheKeepsEpochVariants(t *testing.T) {
	var cache masterchainValidatorCache

	firstKey := masterchainValidatorCacheKey{prevKeyBlockSeqno: 100, catchainSeqno: 7, validatorSetHash: 11}
	secondKey := masterchainValidatorCacheKey{prevKeyBlockSeqno: 100, catchainSeqno: 8, validatorSetHash: 22}

	cache.put(firstKey, []*tlb.ValidatorAddr{nil})
	cache.put(secondKey, []*tlb.ValidatorAddr{nil, nil})

	if validators, ok := cache.get(firstKey); !ok || len(validators) != 1 {
		t.Fatal("expected first validator set to stay cached inside the epoch")
	}
	if validators, ok := cache.get(secondKey); !ok || len(validators) != 2 {
		t.Fatal("expected second validator set to stay cached inside the epoch")
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
	svc := &Service{log: zerolog.Nop(), node: &p2p.Node{}}
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
	svc := &Service{log: zerolog.Nop()}
	proof := testHardforkConsensusProof(current, block, nil)
	proof.signaturesChecked = true

	if _, err := svc.checkMasterchainBlockConsensusWithProof(current, proof); err != nil {
		t.Fatalf("check consensus: %v", err)
	}
}

func testServiceWithHardfork(tb testing.TB, block ton.BlockIDExt) *Service {
	tb.Helper()

	node := &p2p.Node{}
	field := testReflectField(tb, reflect.ValueOf(node).Elem(), "hardforkSet")

	hardforkSet := reflect.MakeMap(field.Type())
	hardforkSet.SetMapIndex(testHardforkSetKey(tb, field.Type().Key(), block), reflect.ValueOf(struct{}{}))
	setUnexportedReflectValue(field, hardforkSet)

	return &Service{log: zerolog.Nop(), node: node}
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
