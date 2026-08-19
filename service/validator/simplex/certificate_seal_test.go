package simplex

import (
	"crypto/ed25519"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"slices"
	"strings"
	"testing"
)

// sealConstructionSites pins every place a VerifiedCertificate can come into
// existence, by enclosing function.
//
// sealCertificate is the single funnel; Engine.seal is the attestation wrapper
// around it and is therefore pinned too. The doc comment on VerifiedCertificate
// enumerates exactly these, and a fifth construction site added without
// updating that enumeration is the failure this test exists to catch — not a
// hypothetical one: the whole point of the type is that a holder may skip the
// Ed25519 quorum pass, so a site that seals without verifying silently converts
// a forged quorum into an accepted block.
var (
	sealCertificateCallers = []string{
		"CertificateVerifier.Verify",
		"Engine.seal",
		"Engine.verifyCertificate",
		"ValidateBootstrap",
	}
	engineSealCallers = []string{
		"Engine.Start",
		"Engine.onVoteApplied",
	}
)

func TestVerifiedCertificateHasOnlyTheDocumentedConstructors(t *testing.T) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(info fs.FileInfo) bool {
		return !strings.HasSuffix(info.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parse package: %v", err)
	}
	pkg := pkgs["simplex"]
	if pkg == nil {
		t.Fatal("package simplex was not parsed")
	}

	var literals []string
	var sealCalls []string
	var engineSealCalls []string
	for name, file := range pkg.Files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			where := funcQualifiedName(fn)
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch node := n.(type) {
				case *ast.CompositeLit:
					// The zero literal is inert by construction — IsZero is true
					// and CertificateBinding.Check rejects it — so only populated
					// ones are construction.
					if ident, isIdent := node.Type.(*ast.Ident); isIdent &&
						ident.Name == "VerifiedCertificate" && len(node.Elts) > 0 {
						literals = append(literals, fmt.Sprintf("%s in %s", where, shortFile(name)))
					}
				case *ast.CallExpr:
					switch callee := node.Fun.(type) {
					case *ast.Ident:
						if callee.Name == "sealCertificate" {
							sealCalls = append(sealCalls, where)
						}
					case *ast.SelectorExpr:
						if callee.Sel.Name == "seal" {
							if recv, isIdent := callee.X.(*ast.Ident); isIdent && recv.Name == "e" {
								engineSealCalls = append(engineSealCalls, where)
							}
						}
					}
				}

				return true
			})
		}
	}

	// Exactly one: the funnel's own.
	if !slices.Equal(literals, []string{"sealCertificate in certificate_verify.go"}) {
		t.Fatalf("populated VerifiedCertificate literals = %v, want only the one in sealCertificate", literals)
	}
	slices.Sort(sealCalls)
	if !slices.Equal(sealCalls, sealCertificateCallers) {
		t.Fatalf("sealCertificate callers = %v, want %v (update the VerifiedCertificate doc comment too)", sealCalls, sealCertificateCallers)
	}
	slices.Sort(engineSealCalls)
	if !slices.Equal(engineSealCalls, engineSealCallers) {
		t.Fatalf("Engine.seal callers = %v, want %v", engineSealCalls, engineSealCallers)
	}
}

func funcQualifiedName(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return fn.Name.Name
	}
	recv := fn.Recv.List[0].Type
	if star, ok := recv.(*ast.StarExpr); ok {
		recv = star.X
	}
	if ident, ok := recv.(*ast.Ident); ok {
		return ident.Name + "." + fn.Name.Name
	}

	return fn.Name.Name
}

func shortFile(path string) string {
	if i := strings.LastIndexByte(path, '/'); i >= 0 {
		return path[i+1:]
	}

	return path
}

// TestZeroVerifiedCertificateIsInert: the zero value is the only
// VerifiedCertificate a consumer outside this package can produce, and it must
// prove nothing.
func TestZeroVerifiedCertificateIsInert(t *testing.T) {
	var zero VerifiedCertificate
	if !zero.IsZero() || zero.Certificate() != nil {
		t.Fatal("zero verified certificate is not zero")
	}
	if zero.Vote() != (Vote{}) || zero.Vote().Kind == VoteNotarize {
		t.Fatal("zero verified certificate carries a usable vote")
	}
	if !zero.Binding().IsZero() {
		t.Fatal("zero verified certificate carries a binding")
	}

	validators, _ := testValidators(4)
	binding, err := NewCertificateBinding([32]byte{0x11}, validators)
	if err != nil {
		t.Fatal(err)
	}
	if err = binding.Check(zero); err == nil {
		t.Fatal("an unverified certificate was accepted by a real binding")
	}
	// And the zero binding accepts nothing either, so a consumer that forgot to
	// derive one fails closed rather than open.
	var zeroBinding CertificateBinding
	sealed, err := VerifyCertificate([32]byte{0x11}, validators, testCertificateFor(t, validators, nil, SkipVote(1)))
	if err == nil {
		if err = zeroBinding.Check(sealed); err == nil {
			t.Fatal("the zero binding accepted a certificate")
		}
	}
}

// TestCertificateBindingSeparatesSessionsAndRosters is the property the block
// accepter depends on: a seal is only a proof relative to one (session, roster)
// pair.
func TestCertificateBindingSeparatesSessionsAndRosters(t *testing.T) {
	validators, keys := testValidators(4)
	sessionA := [32]byte{0xa1}
	sessionB := [32]byte{0xb2}

	vote := NotarizeVote(CandidateID{Slot: 5, Hash: [32]byte{0x77}})
	sealed, err := VerifyCertificate(sessionA, validators, testCertificateFor(t, validators, keys, vote))
	if err != nil {
		t.Fatalf("verify certificate: %v", err)
	}

	bindingA, err := NewCertificateBinding(sessionA, validators)
	if err != nil {
		t.Fatal(err)
	}
	if err = bindingA.Check(sealed); err != nil {
		t.Fatalf("seal rejected by its own binding: %v", err)
	}

	bindingB, err := NewCertificateBinding(sessionB, validators)
	if err != nil {
		t.Fatal(err)
	}
	if err = bindingB.Check(sealed); err == nil {
		t.Fatal("a certificate verified for one session sealed for another")
	}

	otherRoster := slices.Clone(validators)
	otherRoster[2].Weight++
	bindingWeights, err := NewCertificateBinding(sessionA, otherRoster)
	if err != nil {
		t.Fatal(err)
	}
	if err = bindingWeights.Check(sealed); err == nil {
		t.Fatal("a certificate verified against one roster sealed for another weighting")
	}

	reordered := slices.Clone(validators)
	reordered[0], reordered[1] = reordered[1], reordered[0]
	bindingOrder, err := NewCertificateBinding(sessionA, reordered)
	if err != nil {
		t.Fatal(err)
	}
	if err = bindingOrder.Check(sealed); err == nil {
		t.Fatal("a certificate verified against one roster order sealed for another")
	}

	shorter, err := NewCertificateBinding(sessionA, validators[:3])
	if err != nil {
		t.Fatal(err)
	}
	if err = shorter.Check(sealed); err == nil {
		t.Fatal("a certificate verified against one roster sealed for a prefix of it")
	}
}

// TestEngineSelfSealedCertificateVerifies is the honesty check on the one seal
// that is an attestation rather than a verification (Engine.seal, reached from
// onVoteApplied): the certificate the engine assembles from applied votes is
// re-verified here from the outside, exactly as an untrusted one would be. A
// future third caller of applyVote that skips signature verification fails
// this.
func TestEngineSelfSealedCertificateVerifies(t *testing.T) {
	env := newTestEnv(t, withLocal(0))
	env.start()

	id := candID(1, 0x42)
	vote := NotarizeVote(id)
	for i := range testValidatorCount {
		env.deliverVote(i, vote)
	}
	env.eng.FlushVerify()

	if len(env.hooks.notarizedSeals) != 1 {
		t.Fatalf("notarized seals = %d, want one self-built certificate", len(env.hooks.notarizedSeals))
	}
	sealed := env.hooks.notarizedSeals[0]
	if sealed.IsZero() || sealed.Vote() != vote {
		t.Fatalf("self-built certificate carries %s, want %s", sealed.Vote(), vote)
	}
	if err := env.eng.binding().Check(sealed); err != nil {
		t.Fatalf("self-built certificate is not sealed for its own session: %v", err)
	}

	// The seal claims a verified quorum. Prove it independently.
	if _, err := VerifyCertificate(env.session, env.vals, sealed.Certificate()); err != nil {
		t.Fatalf("engine-attested certificate does not verify: %v", err)
	}
	env.requireNoFatal()
}

// TestNewEngineRejectsForeignSigner closes the gap the seal opens: with block
// acceptance no longer re-verifying, a signer that does not hold the key the
// roster lists for us would produce blocks rejected by the network instead of
// failing locally.
func TestNewEngineRejectsForeignSigner(t *testing.T) {
	vals, keys := testValidators(4)
	foreign := testKey(99)

	base := func(signer Signer) Config {
		return Config{
			SessionID:            [32]byte{0x5a},
			Validators:           vals,
			LocalIndex:           1,
			SlotsPerLeaderWindow: 4,
			Journal:              NewMemoryJournal(len(vals)),
			Transport:            &fakeTransport{},
			Hooks:                acceptingHooks{},
			Signer:               signer,
		}
	}

	if _, err := NewEngine(base(newTestSigner(foreign))); err == nil {
		t.Fatal("an engine accepted a signer holding an unrelated key")
	}
	// Another validator's key of the same session is just as wrong.
	if _, err := NewEngine(base(newTestSigner(keys[2]))); err == nil {
		t.Fatal("an engine accepted the signer of another validator")
	}
	if _, err := NewEngine(base(newTestSigner(keys[1]))); err != nil {
		t.Fatalf("an engine rejected its own signer: %v", err)
	}

	// An observer has no key to probe.
	observer := base(nil)
	observer.LocalIndex = ObserverIndex
	if _, err := NewEngine(observer); err != nil {
		t.Fatalf("observer engine rejected: %v", err)
	}
}

// testCertificateFor builds a quorum certificate over the given vote. A nil
// keys slice yields an unsigned certificate, which must not verify.
func testCertificateFor(t *testing.T, validators []Validator, keys []ed25519.PrivateKey, vote Vote) *Certificate {
	t.Helper()

	cert := &Certificate{Vote: vote}
	for i := range validators {
		signature := make([]byte, ed25519.SignatureSize)
		if keys != nil {
			signature = ed25519.Sign(keys[i], DataToSign([32]byte{0xa1}, VoteBytes(vote)))
		}
		cert.Signatures = append(cert.Signatures, VoteSignature{
			ValidatorIndex: uint32(i),
			Signature:      signature,
		})
	}

	return cert
}

// TestEngineSealsWithWhatItVerifiesAgainst pins the agreement between the two
// halves of every engine seal: the session id and roster the quorum is checked
// under, and the binding stamped on the result.
//
// They used to be four sibling Engine fields that agreed only because NewEngine
// filled all of them from one config — nothing said so, and nothing would have
// noticed one of them being sourced differently later. A seal is a claim other
// components trust in place of re-verifying the quorum, so the claim has to name
// what was actually checked: an engine sealing for a session or a roster it did
// not verify under would hand out proofs of nothing, and the accepter that
// consumes them (BlockAccepter, which runs the weight check only) could not
// tell.
//
// The assertion is deliberately made from the engine's own verification inputs
// rather than from the test's config: it holds the two together, wherever they
// come from.
func TestEngineSealsWithWhatItVerifiesAgainst(t *testing.T) {
	env := newTestEnv(t, withLocal(0))
	env.start()

	want, err := NewCertificateBinding(env.eng.sessionID(), env.eng.validators())
	if err != nil {
		t.Fatalf("derive binding from the engine's verification inputs: %v", err)
	}
	if env.eng.binding() != want {
		t.Fatal("the engine seals under a binding that is not the one derived " +
			"from the session id and roster it verifies against")
	}

	// The same statement from the outside: a verifier built from what the engine
	// verifies against accepts what the engine seals, and its binding is the one
	// the engine stamps.
	verifier, err := NewCertificateVerifier(env.eng.sessionID(), env.eng.validators())
	if err != nil {
		t.Fatalf("build verifier from the engine's verification inputs: %v", err)
	}
	if verifier.Binding() != env.eng.binding() {
		t.Fatal("an independently built verifier disagrees with the engine's seal")
	}
	if verifier.threshold != env.eng.threshold() {
		t.Fatalf("engine quorum threshold %d, independently derived %d",
			env.eng.threshold(), verifier.threshold)
	}

	id := candID(1, 0x42)
	vote := NotarizeVote(id)
	for i := range testValidatorCount {
		env.deliverVote(i, vote)
	}
	env.eng.FlushVerify()
	if len(env.hooks.notarizedSeals) != 1 {
		t.Fatalf("notarized seals = %d, want one", len(env.hooks.notarizedSeals))
	}
	if err = verifier.Binding().Check(env.hooks.notarizedSeals[0]); err != nil {
		t.Fatalf("the emitted seal does not carry the derived binding: %v", err)
	}
	env.requireNoFatal()
}
