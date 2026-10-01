package collator

import (
	"errors"
	"fmt"
	"testing"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

type semanticOutDescriptorEncoding struct {
	name string
	root *cell.Cell
}

// Descriptor values are borrowed suffixes of dictionary leaves. The old parser
// repacked each suffix before decoding it; the new parser consumes it directly.
func TestSemanticOutDescriptorBorrowedSliceParity(t *testing.T) {
	envelope := semanticDispatchBatchEnvelope(t, [32]byte{1}, 10, false)
	transaction := cell.BeginCell().MustStoreUInt(0, 8).EndCell()
	reimport := cell.BeginCell().MustStoreUInt(1, 8).EndCell()
	encodings := []semanticOutDescriptorEncoding{
		{"external", cell.BeginCell().MustStoreUInt(0, 3).MustStoreRef(envelope.message).MustStoreRef(transaction).EndCell()},
		{"new", cell.BeginCell().MustStoreUInt(1, 3).MustStoreRef(envelope.root).MustStoreRef(transaction).EndCell()},
		{"immediate", cell.BeginCell().MustStoreUInt(2, 3).MustStoreRef(envelope.root).MustStoreRef(transaction).MustStoreRef(reimport).EndCell()},
		{"transit", cell.BeginCell().MustStoreUInt(3, 3).MustStoreRef(envelope.root).MustStoreRef(reimport).EndCell()},
		{"dequeue immediate", cell.BeginCell().MustStoreUInt(4, 3).MustStoreRef(envelope.root).MustStoreRef(reimport).EndCell()},
		{"new deferred", cell.BeginCell().MustStoreUInt(20, 5).MustStoreRef(envelope.root).MustStoreRef(transaction).EndCell()},
		{"deferred transit", cell.BeginCell().MustStoreUInt(21, 5).MustStoreRef(envelope.root).MustStoreRef(reimport).EndCell()},
		{"dequeue", cell.BeginCell().MustStoreUInt(12, 4).MustStoreRef(envelope.root).MustStoreUInt(7_000, 63).EndCell()},
		{"short dequeue", cell.BeginCell().MustStoreUInt(13, 4).MustStoreSlice(envelope.root.Hash(), 256).MustStoreInt(0, 32).MustStoreUInt(0, 64).MustStoreUInt(7_000, 64).EndCell()},
		{"transit request", cell.BeginCell().MustStoreUInt(7, 3).MustStoreRef(envelope.root).MustStoreRef(reimport).EndCell()},
	}
	key := [32]byte(envelope.message.HashKey())
	for _, encoding := range encodings {
		t.Run(encoding.name, func(t *testing.T) {
			check := func(root *cell.Cell, reject bool) {
				t.Helper()
				for _, materialize := range []bool{true, false} {
					borrowed := borrowedSemanticDescriptor(root)
					_, err := parseSemanticOutDescriptorMaterialization(borrowed, key, materialize)
					if (err != nil) != reject || err != nil && !errors.Is(err, ErrInvalidInput) {
						t.Fatalf("materialize=%v bits=%d refs=%d: %v, want reject=%v", materialize, root.BitsSize(), root.RefsNum(), err, reject)
					}
				}
			}
			check(encoding.root, false)
			// Each truncated bit/ref prefix must remain a rejection. This covers
			// constructor suffixes, dequeue fields and required envelope bindings.
			for bits := uint(0); bits < encoding.root.BitsSize(); bits++ {
				loader := encoding.root.MustBeginParse()
				builder := cell.BeginCell().MustStoreSlice(loader.MustLoadSlice(bits), bits)
				for ref := uint(0); ref < encoding.root.RefsNum(); ref++ {
					builder.MustStoreRef(encoding.root.MustPeekRef(int(ref)))
				}
				check(builder.EndCell(), true)
			}
			for refs := uint(0); refs < encoding.root.RefsNum(); refs++ {
				loader := encoding.root.MustBeginParse()
				bits := encoding.root.BitsSize()
				builder := cell.BeginCell().MustStoreSlice(loader.MustLoadSlice(bits), bits)
				for ref := uint(0); ref < refs; ref++ {
					builder.MustStoreRef(encoding.root.MustPeekRef(int(ref)))
				}
				check(builder.EndCell(), true)
			}
			check(encoding.root.ToBuilder().MustStoreUInt(1, 1).EndCell(), true)
			check(encoding.root.ToBuilder().MustStoreRef(transaction).EndCell(), true)
			read := func(materialize bool) *cell.Cell {
				borrowed := borrowedSemanticDescriptor(encoding.root)
				source := borrowed.BaseCell()
				usage := cell.NewReadSet(source)
				borrowed.SetTrace(usage.Trace())
				// The dictionary walk already opened the containing leaf.
				usage.Record(source)
				if _, err := parseSemanticOutDescriptorMaterialization(borrowed, key, materialize); err != nil {
					t.Fatal(err)
				}
				proof, err := usage.Proof()
				if err != nil {
					t.Fatal(err)
				}
				return proof
			}
			if !equalCell(read(true), read(false)) {
				t.Fatal("direct parsing changed descriptor proof reads")
			}
		})
	}

	pruned, err := cell.CreatePrunedBranch(envelope.root, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	bad := cell.BeginCell().MustStoreUInt(1, 3).MustStoreRef(pruned).MustStoreRef(transaction).EndCell()
	for _, materialize := range []bool{true, false} {
		if _, err := parseSemanticOutDescriptorMaterialization(borrowedSemanticDescriptor(bad), key, materialize); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("pruned envelope materialize=%v: %v", materialize, err)
		}
	}
}

func borrowedSemanticDescriptor(root *cell.Cell) cell.Slice {
	builder := cell.BeginCell().MustStoreUInt(0x1fff, 17)
	builder.MustStoreBuilder(root.ToBuilder())
	value := builder.EndCell().MustBeginParse()
	value.MustLoadUInt(17)
	return *value
}

func parseSemanticOutDescriptorMaterialization(value cell.Slice, key [32]byte, materialize bool) (*semanticOutDescriptor, error) {
	if materialize {
		if _, err := value.ToCell(); err != nil {
			return nil, fmt.Errorf("%w: materialize descriptor: %v", ErrInvalidInput, err)
		}
	}
	return parseSemanticOutDescriptor(value, key, nil)
}
