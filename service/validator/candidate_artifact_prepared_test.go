package validator

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/simplex"
)

// preparedFromArtifactBOCs builds the capsule a producer would have handed over
// with this artifact. It goes through the BOCs because a test cannot reach the
// collator's roots, but the result is the same object the producer builds: the
// capsule binds to roots, and these are the roots those BOCs serialize.
func preparedFromArtifactBOCs(t *testing.T, artifact *CandidateArtifact) *simplex.PreparedCandidate {
	t.Helper()

	blockRoot, err := cell.FromBOC(artifact.BlockBOC)
	if err != nil {
		t.Fatal(err)
	}
	collatedRoots, err := cell.FromBOCMultiRoot(artifact.CollatedData)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := simplex.PrepareCandidate(
		artifact.Candidate.Block.SeqNo,
		blockRoot,
		collatedRoots,
		sha256.Sum256(artifact.BlockBOC),
		sha256.Sum256(artifact.CollatedData),
		simplex.PayloadCellHint(artifact.BlockBOC, artifact.CollatedData),
	)
	if err != nil {
		t.Fatal(err)
	}

	return prepared
}

// preparedConversionSites pins every function in this package that converts a
// collator.CandidateArtifact into the validator's own CandidateArtifact.
//
// Both are producer routes for a candidate this process built: the self route
// hands the artifact to a local leader window, the delegated route hands it to
// a standalone collator's observer runtime. Neither involves a wire hop — the
// collator↔validator link carries only pleaseCollatePrepare/pleaseCollate — so
// in both cases the compressed broadcast payload has already been built from
// the very roots the block was serialized from.
//
// The delegated site used to omit the capsule. Nothing failed: the runtime
// silently fell back to rebuilding the payload from bytes, which parses both
// BOCs back into cell trees, re-serializes each one to prove it is canonical,
// and compresses the combined BOC a second time — while the first compression's
// result was still sitting in the collator's artifact, unread. A dropped field
// on a struct conversion is invisible to the compiler and to every behavioural
// test, because the fallback produces identical bytes; only its cost differs.
// That is the regression class this test exists to catch.
var preparedConversionSites = []string{
	"ConsensusObserver.BroadcastCandidate",
	"LocalSessionBackend.routeCandidate",
}

// TestCandidateArtifactConversionsCarryPreparedPayload asserts that every
// composite literal which copies a collator artifact's BlockBOC also copies its
// prepared payload and its digest provenance.
//
// It is an AST guard rather than a behavioural test because
// collator.CandidateArtifact.prepared is unexported: a test in this package
// cannot construct a collator artifact that carries a capsule, so it cannot
// observe the propagation at run time from here. The unexported field is
// deliberate — it is what makes a capsule provably a serialization of the roots
// the producer built — so the guard adapts to the type rather than the type to
// the guard.
func TestCandidateArtifactConversionsCarryPreparedPayload(t *testing.T) {
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

	found := make(map[string]struct{})
	for _, file := range pkg.Files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			where := preparedFuncQualifiedName(fn)
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				literal, isLiteral := node.(*ast.CompositeLit)
				if !isLiteral {
					return true
				}
				name, isName := literal.Type.(*ast.Ident)
				if !isName || name.Name != "CandidateArtifact" {
					return true
				}
				source, converts := preparedConversionSource(literal)
				if !converts {
					return true
				}
				found[where] = struct{}{}
				for field, accessor := range map[string]string{
					"prepared": ".Prepared()",
					"digested": ".Digested()",
				} {
					if got, want := artifactFieldValue(literal, field), source+accessor; got != want {
						t.Errorf(
							"%s at %s converts %s but sets %s to %q, want %q",
							where,
							fset.Position(literal.Pos()),
							source,
							field,
							got,
							want,
						)
					}
				}

				return true
			})
		}
	}

	var sites []string
	for where := range found {
		sites = append(sites, where)
	}
	sort.Strings(sites)
	want := append([]string(nil), preparedConversionSites...)
	sort.Strings(want)
	if strings.Join(sites, ",") != strings.Join(want, ",") {
		t.Fatalf(
			"collator artifact conversion sites = %v, want %v; a new site must carry Prepared() "+
				"and be listed in preparedConversionSites",
			sites,
			want,
		)
	}
}

// preparedConversionSource reports the identifier a literal copies a collator
// artifact's BlockBOC from. A literal that does not copy BlockBOC from another
// artifact is not a conversion: the codec's decode path builds its fields from
// a freshly decoded payload instead, and an empty candidate carries no BOCs.
func preparedConversionSource(literal *ast.CompositeLit) (string, bool) {
	for _, element := range literal.Elts {
		field, ok := element.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		key, isKey := field.Key.(*ast.Ident)
		if !isKey || key.Name != "BlockBOC" {
			continue
		}
		selector, isSelector := field.Value.(*ast.SelectorExpr)
		if !isSelector || selector.Sel.Name != "BlockBOC" {
			continue
		}
		source, isIdent := selector.X.(*ast.Ident)
		if !isIdent {
			continue
		}

		return source.Name, true
	}

	return "", false
}

// artifactFieldValue renders the accessor call one unexported provenance field
// of a converting literal is assigned, or a description of why it is not one.
func artifactFieldValue(literal *ast.CompositeLit, name string) string {
	for _, element := range literal.Elts {
		field, ok := element.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		key, isKey := field.Key.(*ast.Ident)
		if !isKey || key.Name != name {
			continue
		}
		call, isCall := field.Value.(*ast.CallExpr)
		if !isCall || len(call.Args) != 0 {
			return "<not a no-argument call>"
		}
		selector, isSelector := call.Fun.(*ast.SelectorExpr)
		if !isSelector {
			return "<not a method call>"
		}
		receiver, isIdent := selector.X.(*ast.Ident)
		if !isIdent {
			return "<receiver is not an identifier>"
		}

		return receiver.Name + "." + selector.Sel.Name + "()"
	}

	return "<absent>"
}

// benchmarkPreparedArtifact builds a candidate whose BOCs are the size of a
// busy mainnet block, so the parse, canonicity re-serialization and compression
// the capsule skips are measured at the scale they actually run at. The tiny
// fixture the other codec benchmarks use is dominated by fixed costs.
func benchmarkPreparedArtifact(
	b *testing.B,
	config SessionConfig,
	privateKey ed25519.PrivateKey,
	blockCells int,
	collatedCells int,
) *CandidateArtifact {
	b.Helper()

	blockRoot := benchmarkWideCell(0xb10c, blockCells)
	blockBOC, err := blockRoot.ToBOCWithOptionsErr(cell.BOCSerializeOptions{
		WithCRC32C:    true,
		WithIndex:     true,
		WithCacheBits: true,
		WithIntHashes: true,
	})
	if err != nil {
		b.Fatal(err)
	}
	extra := cell.BeginCell().
		MustStoreUInt(consensusExtraDataTag, 32).
		MustStoreUInt(0, 32).
		MustStoreUInt(uint64(time.Now().UnixMilli()), 64).
		EndCell()
	collatedRoots := []*cell.Cell{extra, benchmarkWideCell(0xc011a, collatedCells)}
	collatedData, err := cell.ToBOCWithOptionsErr(collatedRoots, cell.BOCSerializeOptions{WithCRC32C: true})
	if err != nil {
		b.Fatal(err)
	}

	fileHash := sha256.Sum256(blockBOC)
	candidate := simplex.Candidate{
		Parent: simplex.Genesis(),
		Leader: 0,
		Block: ton.BlockIDExt{
			Workchain: config.Shard.Workchain,
			Shard:     config.Shard.Shard,
			SeqNo:     1,
			RootHash:  blockRoot.Hash(),
			FileHash:  fileHash[:],
		},
		CollatedFileHash: sha256.Sum256(collatedData),
	}
	candidate.ID = candidate.ComputeID(0)
	candidate.Signature, err = simplex.SignCandidate(
		runtimeTestSigner{key: privateKey},
		config.SessionID,
		candidate.ID,
	)
	if err != nil {
		b.Fatal(err)
	}

	return &CandidateArtifact{Candidate: candidate, BlockBOC: blockBOC, CollatedData: collatedData}
}

// benchmarkWideCell builds a tree of distinct cells; distinct payloads keep the
// BOC writer from deduplicating them into one.
func benchmarkWideCell(tag uint64, cells int) *cell.Cell {
	if cells <= 1 {
		return cell.BeginCell().MustStoreUInt(tag, 64).EndCell()
	}

	leaves := make([]*cell.Cell, 0, cells)
	for i := range cells {
		leaves = append(leaves, cell.BeginCell().
			MustStoreUInt(tag, 64).
			MustStoreUInt(uint64(i), 64).
			MustStoreSlice(bytes.Repeat([]byte{byte(i), byte(i >> 8)}, 40), 640).
			EndCell())
	}
	for len(leaves) > 1 {
		next := make([]*cell.Cell, 0, (len(leaves)+3)/4)
		for start := 0; start < len(leaves); start += 4 {
			builder := cell.BeginCell().MustStoreUInt(tag^uint64(start), 64)
			for _, child := range leaves[start:min(start+4, len(leaves))] {
				builder.MustStoreRef(child)
			}
			next = append(next, builder.EndCell())
		}
		leaves = next
	}

	return leaves[0]
}

// BenchmarkCandidateCodecEncodeDelegatedRoute measures what the delegated route
// used to pay per slot. "full" is the path a dropped capsule forced: parse both
// BOCs back into cell trees, re-serialize each to prove it is canonical, hash
// both, then compress the combined BOC — while the collator's own compression
// of the same roots had already run and been discarded. "prepared" is the path
// the self route has always taken.
func BenchmarkCandidateCodecEncodeDelegatedRoute(b *testing.B) {
	config, leaderKey := runtimeTestConfig(0x64, &runtimeTestJournal{})
	codec, err := newCandidateCodec(config, CandidateLimits{
		MaxBlockBytes:        1 << 22,
		MaxCollatedDataBytes: 1 << 22,
	})
	if err != nil {
		b.Fatal(err)
	}
	artifact := benchmarkPreparedArtifact(b, config, leaderKey, 6850, 4330)
	collatorKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0xc8}, ed25519.SeedSize))
	collatorPublic := collatorKey.Public().(ed25519.PublicKey)
	artifact.Candidate.Delegation = &simplex.Delegation{CollatorKey: collatorPublic}
	artifact.Candidate.Delegation.Signature, err = simplex.SignDelegation(
		runtimeTestSigner{key: leaderKey},
		config.SessionID,
		0,
		simplex.KeyNodeIDShort(collatorPublic),
	)
	if err != nil {
		b.Fatal(err)
	}
	artifact.Candidate.Signature, err = simplex.SignCandidate(
		runtimeTestSigner{key: collatorKey},
		config.SessionID,
		artifact.Candidate.ID,
	)
	if err != nil {
		b.Fatal(err)
	}
	payload := int64(len(artifact.BlockBOC) + len(artifact.CollatedData))
	b.Logf("block %d B, collated %d B", len(artifact.BlockBOC), len(artifact.CollatedData))

	b.Run("full", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(payload)
		for range b.N {
			attempt := *artifact
			if _, _, err = codec.encodeForBroadcast(&attempt); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("prepared", func(b *testing.B) {
		prepared := preparedFromBenchmarkArtifact(b, artifact)
		b.ReportAllocs()
		b.SetBytes(payload)
		b.ResetTimer()
		for range b.N {
			attempt := *artifact
			attempt.prepared = prepared
			if _, _, err = codec.encodeForBroadcast(&attempt); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func preparedFromBenchmarkArtifact(b *testing.B, artifact *CandidateArtifact) *simplex.PreparedCandidate {
	b.Helper()

	blockRoot, err := cell.FromBOC(artifact.BlockBOC)
	if err != nil {
		b.Fatal(err)
	}
	collatedRoots, err := cell.FromBOCMultiRoot(artifact.CollatedData)
	if err != nil {
		b.Fatal(err)
	}
	prepared, err := simplex.PrepareCandidate(
		artifact.Candidate.Block.SeqNo,
		blockRoot,
		collatedRoots,
		sha256.Sum256(artifact.BlockBOC),
		sha256.Sum256(artifact.CollatedData),
		simplex.PayloadCellHint(artifact.BlockBOC, artifact.CollatedData),
	)
	if err != nil {
		b.Fatal(err)
	}

	return prepared
}

func preparedFuncQualifiedName(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return fn.Name.Name
	}

	receiver := fn.Recv.List[0].Type
	if star, isStar := receiver.(*ast.StarExpr); isStar {
		receiver = star.X
	}
	if name, isName := receiver.(*ast.Ident); isName {
		return name.Name + "." + fn.Name.Name
	}

	return fn.Name.Name
}
