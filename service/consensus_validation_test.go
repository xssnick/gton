package service

import (
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
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
