package validator

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"strings"
	"testing"
)

// The self route is the only conversion that must carry the collator's parsed
// roots. A candidate this process built is validated here like any other — the
// leader is not exempt — and the collator still holds the trees both BOCs were
// serialized from, so dropping them on the way in makes this node parse a
// megabyte of its own output straight back. The delegated route deliberately
// does not carry them: that runtime has no voter and never validates, and
// attachPayloadLocked would discard them on arrival.
//
// This is an AST guard for the same reason TestCandidateArtifactConversionsCarryPreparedPayload
// is one: collator.CandidateArtifact keeps its roots unexported, which is what
// makes them provably the trees the emitted bytes came from, so a test in this
// package cannot build an artifact that carries a pair and cannot observe the
// propagation at run time. The regression it catches is the same too — a
// dropped field on a struct conversion changes no behaviour, only cost.
func TestSelfCandidateRouteCarriesTheCollatorValidationRoots(t *testing.T) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(info fs.FileInfo) bool {
		return !strings.HasSuffix(info.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parse package: %v", err)
	}
	pkg := pkgs["validator"]
	if pkg == nil {
		t.Fatal("package validator was not parsed")
	}

	var routes, carriers int
	for _, file := range pkg.Files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || preparedFuncQualifiedName(fn) != "LocalSessionBackend.routeCandidate" {
				continue
			}
			routes++
			reads := false
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				if selector, isSelector := node.(*ast.SelectorExpr); isSelector &&
					selector.Sel.Name == "ValidationRoots" {
					reads = true
				}
				literal, isLiteral := node.(*ast.CompositeLit)
				if !isLiteral {
					return true
				}
				name, isName := literal.Type.(*ast.Ident)
				if !isName || name.Name != "CandidateArtifact" {
					return true
				}
				if _, converts := preparedConversionSource(literal); !converts {
					return true
				}
				if artifactFieldValue(literal, "validationRoots") == "<absent>" {
					t.Errorf(
						"the self route at %s converts a collator artifact without carrying its "+
							"validationRoots, so this node re-parses the block it just built",
						fset.Position(literal.Pos()),
					)

					return true
				}
				carriers++

				return true
			})
			if !reads {
				t.Error("the self route never reads the collator artifact's ValidationRoots")
			}
		}
	}
	if routes != 1 || carriers != 1 {
		t.Fatalf("found %d self routes converting %d artifacts, want 1 and 1", routes, carriers)
	}
}
