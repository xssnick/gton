package pebblestore

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"strings"
	"testing"
)

// TestHotKeyPrefixesAreUnique parses keys.go and asserts that no two hot-DB key
// prefixes share a byte value. A duplicate is not a cosmetic problem: two
// features then write into the same keyspace, and a prefix iterator of one
// decodes the other's records — a live example bricked node startup, because
// the pack-journal recovery iterator returns its decode error straight out of
// Open, so the data directory could never be opened again.
//
// The list is derived from the source rather than hand-maintained so a prefix
// added later cannot silently escape the check.
func TestHotKeyPrefixesAreUnique(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "keys.go", nil, 0)
	if err != nil {
		t.Fatalf("parse keys.go: %v", err)
	}

	owner := map[byte]string{}
	found := 0
	ast.Inspect(file, func(n ast.Node) bool {
		spec, ok := n.(*ast.ValueSpec)
		if !ok {
			return true
		}
		for i, name := range spec.Names {
			if !strings.HasPrefix(name.Name, "hotPrefix") || i >= len(spec.Values) {
				continue
			}
			lit, ok := spec.Values[i].(*ast.CompositeLit)
			if !ok || len(lit.Elts) != 1 {
				continue
			}
			basic, ok := lit.Elts[0].(*ast.BasicLit)
			if !ok || basic.Kind != token.INT {
				continue
			}
			value, err := strconv.ParseUint(basic.Value, 0, 8)
			if err != nil {
				t.Fatalf("%s has a non-byte prefix %s", name.Name, basic.Value)
			}
			found++
			if previous, clash := owner[byte(value)]; clash {
				t.Fatalf("hot key prefix 0x%02X is used by both %s and %s", value, previous, name.Name)
			}
			owner[byte(value)] = name.Name
		}
		return true
	})

	if found < 20 {
		t.Fatalf("only %d hot key prefixes parsed, the extraction is broken", found)
	}
}
