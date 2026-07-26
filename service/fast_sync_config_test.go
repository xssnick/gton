package service

import (
	"bytes"
	"crypto/ed25519"
	"math/big"
	"slices"
	"testing"

	"github.com/xssnick/gton/service/p2p"

	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type fastSyncConfigTestValidator struct {
	publicKey p2p.FastSyncValidatorPublicKey
	adnlID    p2p.PeerID
}

type fastSyncConfigAbsentTestCase struct {
	name   string
	params []uint32
}

type fastSyncConfigMalformedParamTestCase struct {
	name  string
	param uint32
}

// A config param this build cannot read must degrade to the default, never stop
// the caller: these are applied on the masterchain apply path, and param 30 and
// the validator sets pick a broadcast transport rather than validate anything.
// The reference node has no error path here at all.
func TestFastSyncConfigFromConfigDegradesOnMalformedParams(t *testing.T) {
	t.Parallel()

	tests := []fastSyncConfigMalformedParamTestCase{
		{name: "new consensus", param: tlb.ConfigParamNewConsensusConfig},
		{name: "previous validators", param: tlb.ConfigParamPrevValidators},
		{name: "current validators", param: tlb.ConfigParamCurrentValidators},
		{name: "next validators", param: tlb.ConfigParamNextValidators},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			value := fastSyncConfigTestMalformedValidatorSetCell(t)
			if test.param == tlb.ConfigParamNewConsensusConfig {
				value = cell.BeginCell().MustStoreUInt(0xff, 8).EndCell()
			}
			cfg := fastSyncConfigTestBlockchainConfig(t, map[uint32]*cell.Cell{
				test.param: value,
			})

			config := fastSyncConfigFromConfig(cfg)
			if config.plumtreeEnabled(-1) || config.plumtreeEnabled(0) {
				t.Fatal("malformed config enabled Plumtree")
			}
			if got := config.roster.Len(); got != 0 {
				t.Fatalf("roster from malformed config = %d entries, want 0", got)
			}
			// No roots means the policy authorizes no source at all, so
			// building it is safe rather than an error worth reporting.
			if got := config.roster.RootPublicKeyIDs(); len(got) != 0 {
				t.Fatalf("root keys from malformed config = %v, want none", got)
			}
			_ = config.plumtreePolicy()
		})
	}
}

// An unreadable param 30 must not cost the node its validator roster: the two
// are independent, and the reference node reads them independently too.
func TestFastSyncConfigFromConfigKeepsRosterWhenConsensusParamIsMalformed(t *testing.T) {
	t.Parallel()

	adnlID := fastSyncConfigTestPeerID(0x91)
	cfg := fastSyncConfigTestBlockchainConfig(t, map[uint32]*cell.Cell{
		tlb.ConfigParamNewConsensusConfig: cell.BeginCell().
			MustStoreUInt(0xff, 8).
			EndCell(),
		tlb.ConfigParamCurrentValidators: fastSyncConfigTestValidatorSetCell(
			t,
			fastSyncConfigTestValidator{
				publicKey: fastSyncConfigTestPublicKey(0x92),
				adnlID:    adnlID,
			},
		),
	})

	config := fastSyncConfigFromConfig(cfg)
	if got := config.roster.ADNLIDs(); !slices.Equal(got, []p2p.PeerID{adnlID}) {
		t.Fatalf("roster = %v, want %v", got, []p2p.PeerID{adnlID})
	}
	if config.plumtreeEnabled(-1) || config.plumtreeEnabled(0) {
		t.Fatal("malformed param 30 enabled Plumtree")
	}
}

// A consensus config variant this build does not know is the case that matters:
// it will appear on mainnet the day param 30 gains a new record, and it must
// read as "no Plumtree", exactly as the v1 record does.
func TestConsensusPlumtreeProtocolVersionIgnoresUnknownVariants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value any
		want  uint8
	}{
		{name: "absent", value: nil, want: 0},
		{name: "simplex v1", value: tlb.NewConsensusConfigSimplex{}, want: 0},
		{name: "unknown variant", value: struct{ Unknown int }{}, want: 0},
		{
			name:  "simplex v2",
			value: tlb.NewConsensusConfigSimplexV2{ProtocolVersion: 2},
			want:  2,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if got := consensusPlumtreeProtocolVersion(test.value); got != test.want {
				t.Fatalf("protocol version = %d, want %d", got, test.want)
			}
		})
	}
}

func TestFastSyncConfigFromConfigAllowsAbsentValidatorParams(t *testing.T) {
	t.Parallel()

	tests := []fastSyncConfigAbsentTestCase{
		{name: "all absent"},
		{
			name:   "only previous",
			params: []uint32{tlb.ConfigParamPrevValidators},
		},
		{
			name:   "only current",
			params: []uint32{tlb.ConfigParamCurrentValidators},
		},
		{
			name:   "only next",
			params: []uint32{tlb.ConfigParamNextValidators},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			params := map[uint32]*cell.Cell{
				tlb.ConfigParamConfigAddress: cell.BeginCell().EndCell(),
			}
			for i, param := range test.params {
				params[param] = fastSyncConfigTestValidatorSetCell(
					t,
					fastSyncConfigTestValidator{
						publicKey: fastSyncConfigTestPublicKey(byte(i + 1)),
						adnlID:    fastSyncConfigTestPeerID(byte(i + 1)),
					},
				)
			}

			config := fastSyncConfigFromConfig(
				fastSyncConfigTestBlockchainConfig(t, params),
			)
			if got := config.roster.Len(); got != len(test.params) {
				t.Fatalf("validator roster length = %d, want %d", got, len(test.params))
			}
			if config.plumtreeEnabled(-1) {
				t.Fatal("absent param 30 enabled masterchain Plumtree")
			}
			if config.plumtreeEnabled(0) {
				t.Fatal("absent param 30 enabled shard Plumtree")
			}
		})
	}
}

func TestFastSyncConfigFromConfigUnionsValidatorSets(t *testing.T) {
	t.Parallel()

	keyA := fastSyncConfigTestPublicKey(0x11)
	keyB := fastSyncConfigTestPublicKey(0x22)
	keyC := fastSyncConfigTestPublicKey(0x33)
	keyD := fastSyncConfigTestPublicKey(0x44)
	var zeroADNL p2p.PeerID
	sharedADNL := fastSyncConfigTestPeerID(0x55)
	uniqueADNL := fastSyncConfigTestPeerID(0x66)

	cfg := fastSyncConfigTestBlockchainConfig(t, map[uint32]*cell.Cell{
		tlb.ConfigParamNewConsensusConfig: fastSyncConfigTestConsensusCell(2, 1),
		tlb.ConfigParamPrevValidators: fastSyncConfigTestValidatorSetCell(
			t,
			fastSyncConfigTestValidator{publicKey: keyA, adnlID: zeroADNL},
			fastSyncConfigTestValidator{publicKey: keyB, adnlID: sharedADNL},
		),
		tlb.ConfigParamCurrentValidators: fastSyncConfigTestValidatorSetCell(
			t,
			fastSyncConfigTestValidator{publicKey: keyA, adnlID: zeroADNL},
			fastSyncConfigTestValidator{publicKey: keyC, adnlID: sharedADNL},
		),
		tlb.ConfigParamNextValidators: fastSyncConfigTestValidatorSetCell(
			t,
			fastSyncConfigTestValidator{publicKey: keyD, adnlID: uniqueADNL},
			fastSyncConfigTestValidator{publicKey: keyB, adnlID: sharedADNL},
		),
	})

	config := fastSyncConfigFromConfig(cfg)

	expectedADNL := []p2p.PeerID{
		fastSyncConfigTestValidatorID(t, keyA),
		sharedADNL,
		uniqueADNL,
	}
	fastSyncConfigTestSortPeerIDs(expectedADNL)
	if got := config.roster.ADNLIDs(); !slices.Equal(got, expectedADNL) {
		t.Fatalf("validator adnl roster = %v, want %v", got, expectedADNL)
	}

	expectedRootIDs := []p2p.PeerID{
		fastSyncConfigTestValidatorID(t, keyA),
		fastSyncConfigTestValidatorID(t, keyB),
		fastSyncConfigTestValidatorID(t, keyC),
		fastSyncConfigTestValidatorID(t, keyD),
	}
	fastSyncConfigTestSortPeerIDs(expectedRootIDs)
	if got := config.roster.RootPublicKeyIDs(); !slices.Equal(got, expectedRootIDs) {
		t.Fatalf("validator root ids = %v, want %v", got, expectedRootIDs)
	}

	if !config.plumtreeEnabled(-1) {
		t.Fatal("param 30 protocol version 2 did not enable masterchain Plumtree")
	}
	if config.plumtreeEnabled(0) {
		t.Fatal("param 30 protocol version 1 enabled shard Plumtree")
	}
}

func fastSyncConfigTestBlockchainConfig(
	t *testing.T,
	params map[uint32]*cell.Cell,
) *tlb.BlockchainConfig {
	t.Helper()

	dict := cell.NewDict(32)
	for param, value := range params {
		wrapped := cell.BeginCell().MustStoreRef(value).EndCell()
		if err := dict.SetIntKey(
			new(big.Int).SetUint64(uint64(param)),
			wrapped,
		); err != nil {
			t.Fatalf("set config param %d: %v", param, err)
		}
	}

	return &tlb.BlockchainConfig{Root: dict.AsCell()}
}

func fastSyncConfigTestValidatorSetCell(
	t *testing.T,
	validators ...fastSyncConfigTestValidator,
) *cell.Cell {
	t.Helper()

	dict := cell.NewDict(16)
	for i, validator := range validators {
		value := cell.BeginCell().
			MustStoreUInt(0x73, 8).
			MustStoreUInt(0x8e81278a, 32).
			MustStoreSlice(validator.publicKey[:], 256).
			MustStoreUInt(1, 64).
			MustStoreSlice(validator.adnlID[:], 256).
			EndCell()
		if err := dict.SetIntKey(big.NewInt(int64(i)), value); err != nil {
			t.Fatalf("set validator %d: %v", i, err)
		}
	}

	result, err := tlb.ToCell(&tlb.ValidatorSetExt{
		UTimeSince:  1,
		UTimeUntil:  2,
		Total:       uint16(len(validators)),
		Main:        uint16(len(validators)),
		TotalWeight: uint64(len(validators)),
		List:        dict,
	})
	if err != nil {
		t.Fatalf("build validator set: %v", err)
	}
	return result
}

func fastSyncConfigTestConsensusCell(
	masterchainVersion,
	shardVersion uint8,
) *cell.Cell {
	masterchain := fastSyncConfigTestSimplexV2Cell(masterchainVersion)
	shard := fastSyncConfigTestSimplexV2Cell(shardVersion)
	return cell.BeginCell().
		MustStoreUInt(0x10, 8).
		MustStoreBoolBit(true).
		MustStoreRef(masterchain).
		MustStoreBoolBit(true).
		MustStoreRef(shard).
		EndCell()
}

func fastSyncConfigTestMalformedValidatorSetCell(t *testing.T) *cell.Cell {
	t.Helper()

	validator := fastSyncConfigTestValidator{
		publicKey: fastSyncConfigTestPublicKey(1),
		adnlID:    fastSyncConfigTestPeerID(1),
	}
	dict := cell.NewDict(16)
	value := cell.BeginCell().
		MustStoreUInt(0x73, 8).
		MustStoreUInt(0x8e81278a, 32).
		MustStoreSlice(validator.publicKey[:], 256).
		MustStoreUInt(1, 64).
		MustStoreSlice(validator.adnlID[:], 256).
		EndCell()
	if err := dict.SetIntKey(big.NewInt(0), value); err != nil {
		t.Fatalf("set malformed validator: %v", err)
	}

	result, err := tlb.ToCell(&tlb.ValidatorSetExt{
		UTimeSince:  1,
		UTimeUntil:  2,
		Total:       2,
		Main:        2,
		TotalWeight: 1,
		List:        dict,
	})
	if err != nil {
		t.Fatalf("build malformed validator set: %v", err)
	}
	return result
}

func fastSyncConfigTestSimplexV2Cell(protocolVersion uint8) *cell.Cell {
	return cell.BeginCell().
		MustStoreUInt(0x22, 8).
		MustStoreUInt(0, 5).
		MustStoreUInt(uint64(protocolVersion), 2).
		MustStoreBoolBit(false).
		MustStoreUInt(4, 32).
		MustStoreDict(nil).
		EndCell()
}

func fastSyncConfigTestPublicKey(
	value byte,
) p2p.FastSyncValidatorPublicKey {
	var key p2p.FastSyncValidatorPublicKey
	for i := range key {
		key[i] = value
	}
	return key
}

func fastSyncConfigTestPeerID(value byte) p2p.PeerID {
	var id p2p.PeerID
	for i := range id {
		id[i] = value
	}
	return id
}

func fastSyncConfigTestValidatorID(
	t *testing.T,
	publicKey p2p.FastSyncValidatorPublicKey,
) p2p.PeerID {
	t.Helper()

	raw, err := tl.Hash(keys.PublicKeyED25519{
		Key: ed25519.PublicKey(publicKey[:]),
	})
	if err != nil {
		t.Fatalf("hash validator public key: %v", err)
	}
	id, err := p2p.NewPeerID(raw)
	if err != nil {
		t.Fatalf("parse validator public key id: %v", err)
	}
	return id
}

func fastSyncConfigTestSortPeerIDs(ids []p2p.PeerID) {
	slices.SortFunc(ids, func(left, right p2p.PeerID) int {
		return bytes.Compare(left[:], right[:])
	})
}
