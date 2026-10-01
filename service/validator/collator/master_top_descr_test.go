package collator

import (
	"bytes"
	"math"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// ValidateQuery rebuilds the shard descriptor from the collated TopBlockDescr
// and rejects the masterchain block on any difference, so the projection the
// acquisition pipeline supplies has to be derived from the proven header rather
// than trusted.
func TestVerifyMasterTopBlockDescrFieldsBindsDescriptorToProvenHeader(t *testing.T) {
	const (
		seqno      = uint32(7)
		genUtime   = uint32(1_700_000_000)
		regMCSeqno = uint32(3)
	)

	blockRoot := masterBuildShardBlockRoot(t, tlb.ShardIdent{WorkchainID: 0}, 0, seqno, genUtime, regMCSeqno, nil)
	rootHash := blockRoot.HashKey()
	block := ton.BlockIDExt{
		Workchain: 0,
		Shard:     math.MinInt64,
		SeqNo:     seqno,
		RootHash:  bytes.Clone(rootHash[:]),
		FileHash:  bytes.Repeat([]byte{0x5b}, 32),
	}
	fields, err := parseShardDescriptorFields(masterBuildShardDescriptor(t, block, regMCSeqno, genUtime))
	if err != nil {
		t.Fatal(err)
	}
	proofs, err := validateMasterTopBlockDescrBinding(
		masterBuildProvenTopBlockDescr(t, block, blockRoot),
		block,
		1,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err = verifyMasterTopBlockDescrFields(proofs, block, &fields); err != nil {
		t.Fatalf("projection of the proven header was rejected: %v", err)
	}

	tests := []struct {
		name   string
		tamper func(*shardDescriptorFields)
	}{
		{"logical time", func(f *shardDescriptorFields) { f.endLT++ }},
		{"generation time", func(f *shardDescriptorFields) { f.genUtime++ }},
		{"min ref mc seqno", func(f *shardDescriptorFields) { f.minRefMCSeqno++ }},
		{"catchain seqno", func(f *shardDescriptorFields) { f.nextCatchainSeqno++ }},
		{"split intent", func(f *shardDescriptorFields) { f.wantSplit = !f.wantSplit }},
		{"collected fees", func(f *shardDescriptorFields) {
			f.fees = tlb.CurrencyCollection{Coins: tlb.FromNanoTONU(1)}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tampered := fields
			test.tamper(&tampered)
			if err := verifyMasterTopBlockDescrFields(proofs, block, &tampered); err == nil {
				t.Fatal("descriptor field diverging from the proven header was accepted")
			}
		})
	}
}

func TestValidateMasterTopBlockDescrBinding(t *testing.T) {
	block := testBlockID(0, math.MinInt64, 7, 0x51)
	descriptor := masterBuildTopBlockDescr(t, block)
	if _, err := validateMasterTopBlockDescrBinding(descriptor, block, 1); err != nil {
		t.Fatal(err)
	}

	other := *block.Copy()
	other.RootHash[0] ^= 1
	if _, err := validateMasterTopBlockDescrBinding(descriptor, other, 1); err == nil {
		t.Fatal("TopBlockDescr for a different block was accepted")
	}
	if _, err := validateMasterTopBlockDescrBinding(descriptor, block, 2); err == nil {
		t.Fatal("TopBlockDescr shorter than the transition was accepted")
	}
	longer := shardTopInboxTestTopBlockDescr(t, block, 3)
	proofs, err := validateMasterTopBlockDescrBinding(longer, block, 2)
	if err != nil {
		t.Fatalf("TopBlockDescr retained history was rejected: %v", err)
	}
	if len(proofs) != 2 {
		t.Fatalf("selected proof prefix length = %d, want 2", len(proofs))
	}
}

func TestValidateMasterTopBlockDescrBindingRejectsOrdinaryProof(t *testing.T) {
	block := testBlockID(0, math.MinInt64, 7, 0x61)
	valid := masterBuildTopBlockDescr(t, block)
	loader := valid.MustBeginParse()
	bits := loader.BitsLeft()
	prefix, err := loader.LoadSlice(uint(bits))
	if err != nil {
		t.Fatal(err)
	}
	malformed := cell.BeginCell().
		MustStoreSlice(prefix, uint(bits)).
		MustStoreRef(cell.BeginCell().EndCell()).
		EndCell()
	if _, err = validateMasterTopBlockDescrBinding(malformed, block, 1); err == nil {
		t.Fatal("ordinary proof link was accepted")
	}
}

// TestParseMasterTopBlockDescrEnvelopeRejectsNonCanonicalShardIdent pins the
// ShardIdent bounds of crypto/block/block-parse.cpp:2113-2124, which
// tlb.ShardIdent itself does not apply. Both collator call sites happen to
// reject these encodings today through their target comparison, but that is two
// call frames away in each of them.
func TestParseMasterTopBlockDescrEnvelopeRejectsNonCanonicalShardIdent(t *testing.T) {
	descriptor := func(prefixBits uint64, workchain int64, prefix uint64) *cell.Cell {
		chain := cell.BeginCell().MustStoreRef(cell.BeginCell().EndCell()).EndCell()
		return cell.BeginCell().
			MustStoreUInt(masterTopBlockDescrTag, 8).
			MustStoreUInt(0, 2).
			MustStoreUInt(prefixBits, 6).
			MustStoreInt(workchain, 32).
			MustStoreUInt(prefix, 64).
			MustStoreUInt(7, 32).
			MustStoreSlice(bytes.Repeat([]byte{0x11}, 32), 256).
			MustStoreSlice(bytes.Repeat([]byte{0x22}, 32), 256).
			MustStoreBoolBit(false).
			MustStoreUInt(1, 8).
			MustStoreRef(chain).
			EndCell()
	}

	if _, err := parseMasterTopBlockDescrEnvelope(descriptor(1, 0, 0)); err != nil {
		t.Fatalf("canonical shard ident rejected: %v", err)
	}

	tests := []struct {
		name       string
		prefixBits uint64
		workchain  int64
		prefix     uint64
	}{
		{name: "prefix length above 60", prefixBits: 61},
		{name: "invalid workchain", workchain: int64(invalidWorkchainID)},
		{name: "root shard with a prefix", prefix: 1},
		{name: "prefix below its length", prefixBits: 1, prefix: 1 << 61},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := parseMasterTopBlockDescrEnvelope(
				descriptor(test.prefixBits, test.workchain, test.prefix),
			); err == nil {
				t.Fatal("non-canonical shard ident was accepted")
			}
		})
	}
}
