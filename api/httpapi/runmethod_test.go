package httpapi

import (
	"bytes"
	"encoding/json"
	"math/big"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
	"github.com/xssnick/tonutils-go/tvm/tuple"
)

func TestRunMethodIDHashesStringAndAcceptsInteger(t *testing.T) {
	id, apiErr := runMethodID(requestParams{body: map[string]json.RawMessage{
		"method": json.RawMessage(`"seqno"`),
	}})
	if apiErr != nil {
		t.Fatalf("runMethodID string: %v", apiErr.message)
	}
	if want := tlb.MethodNameHash("seqno"); id != want {
		t.Fatalf("method id = %d, want %d", id, want)
	}

	id, apiErr = runMethodID(requestParams{body: map[string]json.RawMessage{
		"method": json.RawMessage(`123`),
	}})
	if apiErr != nil {
		t.Fatalf("runMethodID integer: %v", apiErr.message)
	}
	if id != 123 {
		t.Fatalf("method id = %d, want 123", id)
	}
}

func TestLegacyStackValuesAcceptToncenterShapes(t *testing.T) {
	root := cell.BeginCell().MustStoreUInt(0xabc, 16).EndCell()
	encoded := bocBase64(root)

	values, apiErr := legacyStackValues(requestParams{body: map[string]json.RawMessage{
		"stack": json.RawMessage(`[
			["num", "-0x2a"],
			["tvm.Cell", "` + encoded + `"],
			["tvm.Slice", {"bytes":"` + encoded + `"}],
			["tuple", {"@type":"tvm.tuple","elements":[{"@type":"tvm.stackEntryNumber","number":{"@type":"tvm.numberDecimal","number":"7"}}]}]
		]`),
	}})
	if apiErr != nil {
		t.Fatalf("legacyStackValues: %v", apiErr.message)
	}
	if len(values) != 4 {
		t.Fatalf("values len = %d, want 4", len(values))
	}
	if got := values[0].(*big.Int); got.Int64() != -42 {
		t.Fatalf("number = %s, want -42", got)
	}
	rootHash := root.Hash()
	if got := values[1].(*cell.Cell); !bytes.Equal(got.Hash()[:], rootHash[:]) {
		t.Fatalf("cell hash mismatch")
	}
	if got := values[2].(*cell.Slice).MustToCell(); !bytes.Equal(got.Hash()[:], rootHash[:]) {
		t.Fatalf("slice cell hash mismatch")
	}

	tup := values[3].(tuple.Tuple)
	if tup.Len() != 1 {
		t.Fatalf("tuple len = %d, want 1", tup.Len())
	}
	item, err := tup.Index(0)
	if err != nil {
		t.Fatalf("tuple index: %v", err)
	}
	if got := item.(*big.Int); got.Int64() != 7 {
		t.Fatalf("tuple item = %s, want 7", got)
	}
}

func TestTypedStackEntriesMatchRunGetMethodStdSchema(t *testing.T) {
	root := cell.BeginCell().MustStoreUInt(0xabc, 16).EndCell()
	slice, err := root.BeginParse()
	if err != nil {
		t.Fatalf("root.BeginParse: %v", err)
	}
	tup := tuple.NewTupleValue(big.NewInt(7))

	entries := typedStackEntries([]any{big.NewInt(85143), root, slice, tup})
	if len(entries) != 4 {
		t.Fatalf("entries len = %d, want 4", len(entries))
	}
	if entries[0].Type != tvmStackEntryNumber || entries[0].Number.Number != "85143" {
		t.Fatalf("unexpected number entry: %+v", entries[0])
	}
	if entries[1].Type != tvmStackEntryCell || entries[1].Cell.Type != tvmCellType || entries[1].Cell.Bytes == "" {
		t.Fatalf("unexpected cell entry: %+v", entries[1])
	}
	if entries[2].Type != tvmStackEntrySlice || entries[2].Slice.Type != tvmSliceType || entries[2].Slice.Bytes == "" {
		t.Fatalf("unexpected slice entry: %+v", entries[2])
	}
	if entries[3].Type != tvmStackEntryTuple || entries[3].Tuple.Type != tvmTupleType {
		t.Fatalf("unexpected tuple entry: %+v", entries[3])
	}
	if entries[3].Tuple.Elements[0].Number.Number != "7" {
		t.Fatalf("tuple number = %s, want 7", entries[3].Tuple.Elements[0].Number.Number)
	}
}

func TestLegacyStackEntriesFormatNumbersAsHex(t *testing.T) {
	entries := legacyStackEntries([]any{big.NewInt(85143), big.NewInt(-42)})
	if entries[0][0] != "num" || entries[0][1] != "0x14c97" {
		t.Fatalf("unexpected positive entry: %#v", entries[0])
	}
	if entries[1][0] != "num" || entries[1][1] != "-0x2a" {
		t.Fatalf("unexpected negative entry: %#v", entries[1])
	}
}
