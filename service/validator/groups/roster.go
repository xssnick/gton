package groups

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"sort"

	"github.com/xssnick/gton/service/blockproof"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
)

// RosterInput contains every value used by validator-set roster selection.
type RosterInput struct {
	Set           ValidatorSet
	Workchain     int32
	Shard         uint64
	CatchainSeqno uint32
	Catchain      CatchainConfig
}

// TentativeValidatorSetInput contains the sets and checked block time used to
// choose the set for the next catchain session.
type TentativeValidatorSetInput struct {
	Current          ValidatorSet
	Next             *ValidatorSet
	GenUTime         uint32
	CatchainLifetime uint32
}

// ValidatorSetHashInput contains the exact ordered roster hashed into block
// signatures and proof headers.
type ValidatorSetHashInput struct {
	CatchainSeqno uint32
	Validators    []Validator
}

type weightHole struct {
	start  uint64
	weight uint64
}

const shardRosterHoleLimit = uint64(64)

// selectRoster receives validator sets produced by ParseConfig, which has
// already enforced every roster invariant.
func selectRoster(input RosterInput) []Validator {
	if input.Workchain == -1 {
		return selectMasterchainRoster(input)
	}

	return selectShardRosterWithCumulativeWeights(input, input.Set.cumulativeWeights)
}

// SelectTentativeValidatorSet applies the next-session lifetime boundary.
// Boundary arithmetic is checked instead of wrapping uint32.
func SelectTentativeValidatorSet(input TentativeValidatorSetInput) (ValidatorSet, error) {
	// Every reachable ValidatorSet comes from ParseConfig, whose parseValidatorSet
	// already enforced main in 1..total, a non-empty roster, positive weights, no
	// overflow, the 2^61 cap and a computed rather than trusted TotalWeight — and
	// it is the only thing that populates the unexported cumulativeWeights the
	// shard path below needs.
	if input.Next == nil {
		return input.Current, nil
	}
	if input.CatchainLifetime == 0 {
		return ValidatorSet{}, errors.New("catchain lifetime must be positive")
	}

	lifetime := uint64(input.CatchainLifetime)
	boundary := (uint64(input.GenUTime)/lifetime + 1) * lifetime
	if boundary > math.MaxUint32 {
		return ValidatorSet{}, fmt.Errorf("next catchain boundary %d overflows uint32", boundary)
	}
	if uint64(input.Next.Since) <= boundary {
		return *input.Next, nil
	}

	return input.Current, nil
}

// ValidatorAddrs maps a selected roster onto the TL-B validator addresses
// blockproof hashes and serializes. Both the set hash below and the block
// accepter's prepared set go through it, so the mapping — and therefore the
// set hash they compare — cannot drift apart.
func ValidatorAddrs(validators []Validator) []*tlb.ValidatorAddr {
	addrs := make([]*tlb.ValidatorAddr, len(validators))
	for i := range validators {
		validator := &validators[i]
		addrs[i] = &tlb.ValidatorAddr{
			PublicKey: tlb.SigPubKeyED25519{Key: validator.PublicKey[:]},
			Weight:    validator.Weight,
			ADNLAddr:  validator.ADNL[:],
		}
	}

	return addrs
}

// ValidatorSetHash computes gen_validator_list_hash_short from the boxed
// test0.validatorSet representation.
func ValidatorSetHash(input ValidatorSetHashInput) (uint32, error) {
	return blockproof.ValidatorSetHash(input.CatchainSeqno, ValidatorAddrs(input.Validators))
}

// MasterchainStateValidatorHash computes the validator-list hash stored in
// McStateExtra.ValidatorInfo. Unlike a block header hash, the state hash is
// intentionally boxed with catchain seqno zero, while roster shuffling still
// uses catchainSeqno.
func MasterchainStateValidatorHash(config *Config, catchainSeqno uint32) (uint32, error) {
	if config == nil {
		return 0, errors.New("validator config is absent")
	}
	// The set comes from ParseConfig, which validated it on the way in.
	roster := selectRoster(RosterInput{
		Set:           config.ActiveValidators,
		Workchain:     -1,
		Shard:         uint64(1) << 63,
		CatchainSeqno: catchainSeqno,
		Catchain:      config.Catchain,
	})
	return ValidatorSetHash(ValidatorSetHashInput{Validators: roster})
}

// PersistentOverlayMembers returns the ADNL-sorted persistent previous,
// current, and next validator union (parameters 32, 34, and 36). Validator key
// IDs within each immutable member are sorted and unique.
func (c *Config) PersistentOverlayMembers() []PersistentOverlayMember {
	return c.persistentOverlayMembers
}

type persistentOverlayIdentity struct {
	keyID [32]byte
	adnl  [32]byte
}

// buildPersistentOverlayMembers takes the already-selected persistent sets
// rather than re-deriving them from the parameter map: ParseConfig picked them
// out one call earlier and is the only caller.
func buildPersistentOverlayMembers(validatorSets []ValidatorSet) []PersistentOverlayMember {
	capacity := 0
	for i := range validatorSets {
		capacity += len(validatorSets[i].Validators)
	}
	identities := make([]persistentOverlayIdentity, 0, capacity)
	for index := range validatorSets {
		validatorSet := &validatorSets[index]
		for i := range validatorSet.Validators {
			validator := &validatorSet.Validators[i]
			adnl := validator.ADNL
			if adnl == ([32]byte{}) {
				// A validator without an explicit ADNL address is represented in
				// the persistent overlay by its public-key short ID.
				adnl = validator.PublicKeyHash
			}
			identities = append(identities, persistentOverlayIdentity{
				keyID: validator.PublicKeyHash,
				adnl:  adnl,
			})
		}
	}

	sort.Slice(identities, func(i, j int) bool {
		if cmp := bytes.Compare(identities[i].adnl[:], identities[j].adnl[:]); cmp != 0 {
			return cmp < 0
		}

		return bytes.Compare(identities[i].keyID[:], identities[j].keyID[:]) < 0
	})
	if len(identities) == 0 {
		return nil
	}

	unique := identities[:1]
	for i := 1; i < len(identities); i++ {
		if identities[i] != unique[len(unique)-1] {
			unique = append(unique, identities[i])
		}
	}

	members := make([]PersistentOverlayMember, 0, len(unique))
	for i := range unique {
		if len(members) == 0 || unique[i].adnl != members[len(members)-1].ADNL {
			members = append(members, PersistentOverlayMember{ADNL: unique[i].adnl})
		}
		last := &members[len(members)-1]
		last.ValidatorKeyIDs = append(last.ValidatorKeyIDs, unique[i].keyID)
	}

	return members
}

func selectMasterchainRoster(input RosterInput) []Validator {
	count := int(input.Set.Main)
	if count > len(input.Set.Validators) {
		count = len(input.Set.Validators)
	}
	if !input.Catchain.ShuffleMasterchain {
		return input.Set.Validators[:count]
	}

	rng := ton.NewValidatorSetPRNG(int64(input.Shard), input.Workchain, input.CatchainSeqno, nil)
	indices := make([]int, count)
	for i := 0; i < count; i++ {
		j := int(rng.NextRanged(uint64(i + 1)))
		indices[i] = indices[j]
		indices[j] = i
	}

	result := make([]Validator, count)
	for i, index := range indices {
		result[i] = input.Set.Validators[index]
	}

	return result
}

func selectShardRosterWithCumulativeWeights(input RosterInput, cumulativeWeights []uint64) []Validator {
	count := shardRosterCount(input)
	if !useShardRosterHoles(count, len(input.Set.Validators)) {
		return selectShardRosterWithFenwick(input, count)
	}

	return selectShardRosterWithHoles(input, cumulativeWeights, count)
}

func shardRosterCount(input RosterInput) uint64 {
	count := uint64(input.Catchain.ShardValidators)
	if count > uint64(len(input.Set.Validators)) {
		count = uint64(len(input.Set.Validators))
	}
	return count
}

func useShardRosterHoles(count uint64, validatorCount int) bool {
	// Keep the quadratic path only when it is bounded or cheaper than building
	// a tree for the entire validator set.
	return count <= shardRosterHoleLimit || count <= uint64(validatorCount)/count
}

func selectShardRosterWithHoles(input RosterInput, cumulativeWeights []uint64, count uint64) []Validator {
	rng := ton.NewValidatorSetPRNG(int64(input.Shard), input.Workchain, input.CatchainSeqno, nil)
	remainingWeight := input.Set.TotalWeight
	holes := make([]weightHole, 0, count)
	result := make([]Validator, 0, count)
	for range count {
		position := rng.NextRanged(remainingWeight)
		for _, hole := range holes {
			if position < hole.start {
				break
			}
			position += hole.weight
		}

		index := sort.Search(len(input.Set.Validators), func(i int) bool {
			return position < cumulativeWeights[i]+input.Set.Validators[i].Weight
		})
		validator := input.Set.Validators[index]
		remainingWeight -= validator.Weight
		result = append(result, Validator{
			PublicKey:     validator.PublicKey,
			PublicKeyHash: validator.PublicKeyHash,
			ADNL:          validator.ADNL,
			Weight:        1,
		})

		hole := weightHole{start: cumulativeWeights[index], weight: validator.Weight}
		insertAt := sort.Search(len(holes), func(i int) bool {
			return holes[i].start > hole.start
		})
		holes = append(holes, weightHole{})
		copy(holes[insertAt+1:], holes[insertAt:])
		holes[insertAt] = hole
	}

	return result
}

func selectShardRosterWithFenwick(input RosterInput, count uint64) []Validator {
	tree := newWeightFenwick(input.Set.Validators)
	rng := ton.NewValidatorSetPRNG(int64(input.Shard), input.Workchain, input.CatchainSeqno, nil)
	remainingWeight := input.Set.TotalWeight
	result := make([]Validator, 0, count)
	for range count {
		position := rng.NextRanged(remainingWeight)
		index := weightFenwickIndex(tree, position)
		validator := input.Set.Validators[index]
		remainingWeight -= validator.Weight
		removeWeightFenwick(tree, index, validator.Weight)
		result = append(result, Validator{
			PublicKey:     validator.PublicKey,
			PublicKeyHash: validator.PublicKeyHash,
			ADNL:          validator.ADNL,
			Weight:        1,
		})
	}

	return result
}

func newWeightFenwick(validators []Validator) []uint64 {
	tree := make([]uint64, len(validators)+1)
	for i := range validators {
		index := i + 1
		tree[index] += validators[i].Weight
		parent := index + (index & -index)
		if parent < len(tree) {
			tree[parent] += tree[index]
		}
	}
	return tree
}

func weightFenwickIndex(tree []uint64, position uint64) int {
	index := 0
	// position is zero-based; index becomes the first prefix strictly above it.
	for step := 1 << (bits.Len(uint(len(tree)-1)) - 1); step != 0; step >>= 1 {
		next := index + step
		if next < len(tree) && tree[next] <= position {
			position -= tree[next]
			index = next
		}
	}
	return index
}

func removeWeightFenwick(tree []uint64, index int, weight uint64) {
	index++
	for index < len(tree) {
		tree[index] -= weight
		index += index & -index
	}
}
