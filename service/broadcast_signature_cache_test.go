package service

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"math/big"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/gton/service/p2p"
)

const (
	signatureCacheTestCatchainSeqno = uint32(7)
	signatureCacheTestSlot          = int32(11)
)

type signatureCacheTestFixture struct {
	cfg     *tlb.BlockchainConfig
	signers map[string]ed25519.PrivateKey
}

// TestBroadcastValidatorCacheIgnoresForgedFinalityFlood pins that a validator
// set reaches the broadcast cache only after signatures made with it verified.
// Shard, catchain seqno and set hash of a finality broadcast come from an
// untrusted peer, and the set hash is computable offline from the public
// config, so a flood of forged broadcasts for distinct shards must leave
// nothing behind.
func TestBroadcastValidatorCacheIgnoresForgedFinalityFlood(t *testing.T) {
	fixture := newSignatureCacheTestFixture(t, 1)
	coordinator := fixture.coordinator()

	for i := range 300 {
		shard := int64(uint64(i+1)<<52 | 1<<51)
		forged := fixture.finality(t, testBlockID(0, shard, 1<<31), false)
		if _, err := coordinator.CheckBlockFinalitySignatures(context.Background(), forged); err == nil || !strings.Contains(err.Error(), "incorrect signature") {
			t.Fatalf("forged finality broadcast %d error = %v, want an incorrect-signature rejection", i, err)
		}
	}
	if got := signatureCacheTestEntries(coordinator); got != 0 {
		t.Fatalf("forged finality flood cached %d validator sets, want none", got)
	}

	genuine := fixture.finality(t, testBlockID(0, topShard, 10), true)
	if _, err := coordinator.CheckBlockFinalitySignatures(context.Background(), genuine); err != nil {
		t.Fatalf("genuine finality broadcast rejected: %v", err)
	}
	if got := signatureCacheTestEntries(coordinator); got != 1 {
		t.Fatalf("verified finality broadcast cached %d validator sets, want one", got)
	}
}

// TestBroadcastValidatorCacheBoundsEntries pins that neither verified cache
// grows past its bound within one config root, and that the newest entry is
// kept when a cache starts over.
func TestBroadcastValidatorCacheBoundsEntries(t *testing.T) {
	var cache broadcastValidatorCache

	root := testBroadcastConfigHash(1)
	var lastSet broadcastValidatorCacheKey
	var lastFinality broadcastFinalityCacheKey
	for i := range 3 * broadcastFinalityCacheMaxEntries {
		lastSet = broadcastValidatorCacheKey{
			configRootHash:   root,
			workchain:        0,
			shard:            int64(uint64(i+1)<<40 | 1<<39),
			catchainSeqno:    7,
			validatorSetHash: uint32(i),
		}
		cache.put(lastSet, &blockproof.PreparedValidatorSet{})

		lastFinality = broadcastFinalityCacheKey{validators: lastSet, seqno: uint32(i)}
		cache.putVerifiedFinality(lastFinality)

		cache.mu.Lock()
		sets, finalities := len(cache.entries), len(cache.verifiedFinality)
		cache.mu.Unlock()
		if sets > broadcastValidatorCacheMaxEntries || finalities > broadcastFinalityCacheMaxEntries {
			t.Fatalf("after %d puts the caches hold %d validator sets and %d finalities, bounds %d and %d", i+1, sets, finalities, broadcastValidatorCacheMaxEntries, broadcastFinalityCacheMaxEntries)
		}
	}

	if _, err := cache.get(lastSet); err != nil {
		t.Fatalf("newest validator set is not cached: %v", err)
	}
	if !cache.finalityVerified(lastFinality) {
		t.Fatal("newest verified finality is not cached")
	}
}

// TestCheckBlockFinalitySignaturesAnswersVerifiedCopy pins that another
// overlay's copy of finality evidence already verified in full is answered with
// that verification's result, and that any difference in the evidence, the
// block id or the config root is verified again.
func TestCheckBlockFinalitySignaturesAnswersVerifiedCopy(t *testing.T) {
	for name, workchain := range map[string]int32{"shard": 0, "masterchain": -1} {
		t.Run(name, func(t *testing.T) {
			fixture := newSignatureCacheTestFixture(t, 4)
			coordinator := fixture.coordinator()
			block := testBlockID(workchain, topShard, 10)
			genuine := fixture.finality(t, block, true)

			first, err := coordinator.CheckBlockFinalitySignatures(context.Background(), genuine)
			if err != nil {
				t.Fatalf("genuine finality broadcast rejected: %v", err)
			}
			if got := signatureCacheTestVerifiedFinality(coordinator); got != 1 {
				t.Fatalf("verified finality evidence recorded %d times, want once", got)
			}
			second, err := coordinator.CheckBlockFinalitySignatures(context.Background(), genuine)
			if err != nil {
				t.Fatalf("copy of genuine finality broadcast rejected: %v", err)
			}
			if got := signatureCacheTestVerifiedFinality(coordinator); got != 1 {
				t.Fatalf("copy of verified finality evidence recorded %d entries, want one", got)
			}

			fresh, err := fixture.coordinator().CheckBlockFinalitySignatures(context.Background(), genuine)
			if err != nil {
				t.Fatalf("genuine finality broadcast rejected by a fresh verifier: %v", err)
			}
			for _, result := range []*p2p.BlockFinalitySignatureCheckResult{first, second} {
				if !bytes.Equal(result.SignaturesVerifiedKey, fresh.SignaturesVerifiedKey) {
					t.Fatal("finality result carries another verification key than a fresh check")
				}
				if (result.SignaturesCell == nil) != (workchain != -1) {
					t.Fatalf("finality signatures cell presence = %v for workchain %d", result.SignaturesCell != nil, workchain)
				}
				if result.SignaturesCell != nil && result.SignaturesCell.HashKey() != fresh.SignaturesCell.HashKey() {
					t.Fatal("finality signatures cell differs from a fresh check")
				}
			}

			forged := fixture.finality(t, block, false)
			if _, err = coordinator.CheckBlockFinalitySignatures(context.Background(), forged); err == nil {
				t.Fatal("forged finality evidence for a verified block was accepted")
			}
			if got := signatureCacheTestVerifiedFinality(coordinator); got != 1 {
				t.Fatalf("forged finality evidence left %d verified entries, want one", got)
			}

			// Same root and file hash, same signature set: only the block id
			// differs, which the content key of the set does not cover.
			moved := genuine
			moved.Block.SeqNo++
			if _, err = coordinator.CheckBlockFinalitySignatures(context.Background(), moved); err == nil {
				t.Fatal("finality evidence verified for one block id was accepted for another")
			}

			other := newSignatureCacheTestFixture(t, 4)
			coordinator.broadcastValidatorCache.putConfig(
				testBlockID(-1, topShard, 200),
				broadcastValidatorConfig{rootHash: other.cfg.Root.HashKey(), cfg: other.cfg},
			)
			if _, err = coordinator.CheckBlockFinalitySignatures(context.Background(), genuine); err == nil {
				t.Fatal("finality evidence verified under the previous config was accepted after the config changed")
			}
		})
	}
}

// BenchmarkCheckBlockFinalitySignaturesCopy measures a masterchain finality
// broadcast arriving through another overlay after its first copy verified.
func BenchmarkCheckBlockFinalitySignaturesCopy(b *testing.B) {
	fixture := newSignatureCacheTestFixture(b, 32)
	coordinator := fixture.coordinator()
	genuine := fixture.finality(b, testBlockID(-1, topShard, 10), true)
	if _, err := coordinator.CheckBlockFinalitySignatures(context.Background(), genuine); err != nil {
		b.Fatalf("genuine finality broadcast rejected: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := coordinator.CheckBlockFinalitySignatures(context.Background(), genuine); err != nil {
			b.Fatal(err)
		}
	}
}

func newSignatureCacheTestFixture(tb testing.TB, count int) *signatureCacheTestFixture {
	tb.Helper()

	signers := make(map[string]ed25519.PrivateKey, count)
	list := cell.NewDict(16)
	for i := range count {
		publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			tb.Fatalf("generate validator key: %v", err)
		}
		signers[string(publicKey)] = privateKey

		value := cell.BeginCell().
			MustStoreUInt(0x73, 8).
			MustStoreUInt(0x8e81278a, 32).
			MustStoreSlice(publicKey, 256).
			MustStoreUInt(1, 64).
			MustStoreSlice(publicKey, 256).
			EndCell()
		if err = list.SetIntKey(big.NewInt(int64(i)), value); err != nil {
			tb.Fatalf("set validator %d: %v", i, err)
		}
	}

	validatorSet, err := tlb.ToCell(&tlb.ValidatorSetExt{
		UTimeSince:  1,
		UTimeUntil:  2,
		Total:       uint16(count),
		Main:        uint16(count),
		TotalWeight: uint64(count),
		List:        list,
	})
	if err != nil {
		tb.Fatalf("build validator set: %v", err)
	}

	params := cell.NewDict(32)
	for param, value := range map[uint32]*cell.Cell{
		tlb.ConfigParamCatchainConfig:    broadcastValidatorTestCatchainConfig(),
		tlb.ConfigParamPrevValidators:    validatorSet,
		tlb.ConfigParamCurrentValidators: validatorSet,
		tlb.ConfigParamNextValidators:    validatorSet,
	} {
		wrapped := cell.BeginCell().MustStoreRef(value).EndCell()
		if err = params.SetIntKey(new(big.Int).SetUint64(uint64(param)), wrapped); err != nil {
			tb.Fatalf("set config param %d: %v", param, err)
		}
	}

	return &signatureCacheTestFixture{
		cfg:     &tlb.BlockchainConfig{Root: params.AsCell()},
		signers: signers,
	}
}

func (f *signatureCacheTestFixture) coordinator() *SyncCoordinator {
	coordinator := &SyncCoordinator{}
	coordinator.broadcastValidatorCache.putConfig(
		testBlockID(-1, topShard, 100),
		broadcastValidatorConfig{rootHash: f.cfg.Root.HashKey(), cfg: f.cfg},
	)
	return coordinator
}

// finality builds a final Simplex signature set for block over the validators
// the config assigns to it. Genuine evidence is signed by every one of them;
// forged evidence names the same validators with signatures nobody made.
func (f *signatureCacheTestFixture) finality(tb testing.TB, block ton.BlockIDExt, genuine bool) p2p.BlockFinalitySignatureCheck {
	tb.Helper()

	validators, err := blockproof.CurrentValidatorsForBlock(f.cfg, &block, signatureCacheTestCatchainSeqno)
	if err != nil {
		tb.Fatalf("load validators: %v", err)
	}
	prepared, err := blockproof.PrepareValidatorSet(signatureCacheTestCatchainSeqno, validators)
	if err != nil {
		tb.Fatalf("prepare validator set: %v", err)
	}

	sessionID := bytes.Repeat([]byte{0x63}, 32)
	candidate, err := tl.Serialize(ton.ConsensusCandidateHashDataOrdinary{
		Block:            block,
		CollatedFileHash: make([]byte, 32),
		Parent:           ton.ConsensusCandidateWithoutParents{},
	}, true)
	if err != nil {
		tb.Fatalf("serialize simplex candidate: %v", err)
	}
	payload, err := blockproof.SimplexSignaturePayload(block, true, sessionID, signatureCacheTestSlot, candidate)
	if err != nil {
		tb.Fatalf("build simplex signature payload: %v", err)
	}

	signatures := make([]ton.Signature, 0, len(validators))
	for _, validator := range validators {
		nodeID, err := tl.Hash(keys.PublicKeyED25519{Key: validator.PublicKey.Key})
		if err != nil {
			tb.Fatalf("hash validator key: %v", err)
		}
		signature := make([]byte, ed25519.SignatureSize)
		if genuine {
			signature = ed25519.Sign(f.signers[string(validator.PublicKey.Key)], payload)
		}
		signatures = append(signatures, ton.Signature{NodeIDShort: nodeID, Signature: signature})
	}

	return p2p.BlockFinalitySignatureCheck{
		Kind:  "tonNode.blockFinalityBroadcast",
		Block: block,
		Signatures: blockproof.NewSimplexValidatorSignatureSet(
			signatureCacheTestCatchainSeqno,
			prepared.Hash(),
			signatures,
			true,
			sessionID,
			signatureCacheTestSlot,
			candidate,
		),
	}
}

func signatureCacheTestEntries(coordinator *SyncCoordinator) int {
	coordinator.broadcastValidatorCache.mu.Lock()
	defer coordinator.broadcastValidatorCache.mu.Unlock()

	return len(coordinator.broadcastValidatorCache.entries)
}

func signatureCacheTestVerifiedFinality(coordinator *SyncCoordinator) int {
	coordinator.broadcastValidatorCache.mu.Lock()
	defer coordinator.broadcastValidatorCache.mu.Unlock()

	return len(coordinator.broadcastValidatorCache.verifiedFinality)
}
