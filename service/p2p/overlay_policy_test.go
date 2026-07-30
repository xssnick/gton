package p2p

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestOverlayPolicyMatrix is the per-kind truth table for every behavioural
// predicate. It exists so a changed predicate body shows up as a diff in one
// readable place instead of as a behaviour change in whichever subsystem
// happened to ask the question.
func TestOverlayPolicyMatrix(t *testing.T) {
	tests := []struct {
		name                          string
		fn                            func(*overlaySpec) bool
		public, customFixed, fastSync bool
	}{
		{"seedsFromFixedNodes", (*overlaySpec).seedsFromFixedNodes, false, true, true},
		{"restrictsPeerIDs", (*overlaySpec).restrictsPeerIDs, false, true, false},
		{"adoptsInboundPeers", (*overlaySpec).adoptsInboundPeers, false, true, false},
		{"membersArePermanent", (*overlaySpec).membersArePermanent, false, true, false},
		{"peersStartPending", (*overlaySpec).peersStartPending, true, false, false},
		{"hasDirectoryTier", (*overlaySpec).hasDirectoryTier, true, false, false},
		{"shufflesSeedOrder", (*overlaySpec).shufflesSeedOrder, false, false, true},
		{"rosterSizesPeerLimit", (*overlaySpec).rosterSizesPeerLimit, false, true, false},
		{"usesDedicatedQueryPeers", (*overlaySpec).usesDedicatedQueryPeers, false, true, false},
		{"usesV1PeerGossip", (*overlaySpec).usesV1PeerGossip, true, false, false},
		{"usesFastSyncPeerGossip", (*overlaySpec).usesFastSyncPeerGossip, false, false, true},
		{"runsFixedPeerProbes", (*overlaySpec).runsFixedPeerProbes, false, true, false},
		{"statusListsWholeRoster", (*overlaySpec).statusListsWholeRoster, false, true, false},
		{"followsShardLifecycle", (*overlaySpec).followsShardLifecycle, true, false, false},
		{"persistsPeerCache", (*overlaySpec).persistsPeerCache, true, false, false},
		{"usesFastSyncRoster", (*overlaySpec).usesFastSyncRoster, false, false, true},
		{"probesPeersPeriodically", (*overlaySpec).probesPeersPeriodically, true, true, true},
		{"preferredAsQuerySource", (*overlaySpec).preferredAsQuerySource, false, true, false},
		{"privatePeerRoster", (*overlaySpec).privatePeerRoster, false, true, false},
		{"enforcesAcceptQueries", (*overlaySpec).enforcesAcceptQueries, false, true, false},
		{"probeDrivenQueryReadiness", (*overlaySpec).probeDrivenQueryReadiness, false, true, false},
		{"preservesQueryCandidateOrder", (*overlaySpec).preservesQueryCandidateOrder, false, true, false},
		{"pingsPeerOnQueryTimeout", (*overlaySpec).pingsPeerOnQueryTimeout, false, false, true},
		{"drawsRandomQueryFallback", (*overlaySpec).drawsRandomQueryFallback, true, false, false},
		{"restrictsBroadcastKinds", (*overlaySpec).restrictsBroadcastKinds, false, false, true},
		{"dropsSelfSourcedBroadcasts", (*overlaySpec).dropsSelfSourcedBroadcasts, false, true, false},
		{"dropsIHRBroadcasts", (*overlaySpec).dropsIHRBroadcasts, false, true, false},
		{"authorizesBroadcastSenders", (*overlaySpec).authorizesBroadcastSenders, false, true, false},
		{"relaysFECBroadcasts", (*overlaySpec).relaysFECBroadcasts, true, false, true},
		{"usesPlumtree", (*overlaySpec).usesPlumtree, true, false, true},
		{"usesTwoStepDelivery", (*overlaySpec).usesTwoStepDelivery, false, true, true},
		{"alwaysTwoStepFEC", (*overlaySpec).alwaysTwoStepFEC, false, true, false},
		{"twoStepFECUnlessPlanOptsOut", (*overlaySpec).twoStepFECUnlessPlanOptsOut, false, false, true},
		{"floodsWholeRoster", (*overlaySpec).floodsWholeRoster, false, true, false},
		{"pacesFECBursts", (*overlaySpec).pacesFECBursts, false, false, true},
		{"originatesLocalBroadcasts", (*overlaySpec).originatesLocalBroadcasts, false, true, false},
		{"servesPublicIngress", (*overlaySpec).servesPublicIngress, true, false, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, kind := range []struct {
				kind overlayKind
				name string
				want bool
			}{
				{overlayKindPublicShard, "public", test.public},
				{overlayKindCustomFixed, "customFixed", test.customFixed},
				{overlayKindFastSync, "fastSync", test.fastSync},
			} {
				// Every configuration flag is on, so what the matrix shows is
				// the kind term alone.
				spec := overlaySpec{
					Kind:              kind.kind,
					RandomPeers:       true,
					SendQueries:       true,
					AcceptQueries:     true,
					QueryCapabilities: true,
				}
				if got := test.fn(&spec); got != kind.want {
					t.Fatalf("%s on %s overlay = %v, want %v", test.name, kind.name, got, kind.want)
				}
			}
		})
	}
}

// TestOverlayPolicyHonoursSpecFlags pins the predicates whose answer is not
// kind alone. Folding a per-instance configuration term into a kind-only answer
// is the one way this file can silently change behaviour, because the flags and
// the kind both zero-value to the public arm.
func TestOverlayPolicyHonoursSpecFlags(t *testing.T) {
	public := overlaySpec{Kind: overlayKindPublicShard}
	if public.usesV1PeerGossip() {
		t.Fatal("usesV1PeerGossip must stay false while RandomPeers is unset")
	}
	if public.probesPeersPeriodically() {
		t.Fatal("probesPeersPeriodically must stay false while QueryCapabilities is unset")
	}
	public.RandomPeers = true
	public.QueryCapabilities = true
	if !public.usesV1PeerGossip() || !public.probesPeersPeriodically() {
		t.Fatal("public overlay must gossip and probe once its flags are set")
	}

	custom := overlaySpec{Kind: overlayKindCustomFixed}
	if custom.preferredAsQuerySource() {
		t.Fatal("preferredAsQuerySource must stay false while SendQueries is unset")
	}
	if custom.probesPeersPeriodically() {
		t.Fatal("probesPeersPeriodically must stay false while SendQueries is unset")
	}
	custom.SendQueries = true
	if !custom.preferredAsQuerySource() || !custom.probesPeersPeriodically() {
		t.Fatal("custom overlay must be a query source and probe once SendQueries is set")
	}

	// FastSync sets SendQueries and RandomPeers too; only the kind term keeps
	// it out of the custom-fixed query preference and the v1 gossip exchange.
	fastSync := overlaySpec{
		Kind:        overlayKindFastSync,
		SendQueries: true,
		RandomPeers: true,
	}
	if fastSync.preferredAsQuerySource() {
		t.Fatal("FastSync must not outrank the public pool as a query source")
	}
	if fastSync.usesV1PeerGossip() {
		t.Fatal("FastSync speaks getRandomPeersV2 only")
	}
}

func TestOverlayProbeCadenceIsArmed(t *testing.T) {
	for _, kind := range []overlayKind{
		overlayKindPublicShard,
		overlayKindCustomFixed,
		overlayKindFastSync,
	} {
		cadence := overlayProbeCadence[kind]
		if cadence.min <= 0 || cadence.jitter <= 0 {
			t.Fatalf("kind %d cadence {min:%v jitter:%v} must be armed", kind, cadence.min, cadence.jitter)
		}

		spec := overlaySpec{Kind: kind}
		for range 32 {
			delay := spec.nextQueryProbeDelay()
			if delay < cadence.min || delay >= cadence.min+cadence.jitter {
				t.Fatalf(
					"kind %d probe delay %v outside [%v, %v)",
					kind, delay, cadence.min, cadence.min+cadence.jitter,
				)
			}
		}
	}
}

func TestOverlayProbeCadenceMatchesPreviousConstants(t *testing.T) {
	for _, test := range []struct {
		kind        overlayKind
		min, jitter time.Duration
	}{
		{overlayKindPublicShard, peerPingMinDelay, peerPingJitter},
		{overlayKindCustomFixed, customQueryProbeMinDelay, customQueryProbeJitter},
		{overlayKindFastSync, fastSyncPingMinDelay, fastSyncPingJitter},
	} {
		if got := overlayProbeCadence[test.kind]; got.min != test.min || got.jitter != test.jitter {
			t.Fatalf(
				"kind %d cadence = {%v %v}, want {%v %v}",
				test.kind, got.min, got.jitter, test.min, test.jitter,
			)
		}
	}
}

// TestOverlayKindStaysInPolicyFile is what keeps this refactor from decaying.
// Without it the next feature adds one more `s.spec.Kind == ...` somewhere in
// the package and nothing complains until the count is back where it started.
//
// It matches identifiers rather than the text of a comparison, so `switch
// spec.Kind`, `!=`, argument position and assignment are all caught. Two writes
// are legitimate and allowed: the `Kind:` value in a builder's composite
// literal, and the const declarations themselves.
func TestOverlayKindStaysInPolicyFile(t *testing.T) {
	kindConstants := map[string]struct{}{
		"overlayKindPublicShard": {},
		"overlayKindCustomFixed": {},
		"overlayKindFastSync":    {},
	}

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}

	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() ||
			filepath.Ext(name) != ".go" ||
			strings.HasSuffix(name, "_test.go") ||
			name == "overlay_policy.go" {
			continue
		}

		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}

		specNames := specValueNames(file)

		allowed := map[ast.Node]struct{}{}
		ast.Inspect(file, func(node ast.Node) bool {
			switch node := node.(type) {
			case *ast.KeyValueExpr:
				if key, ok := node.Key.(*ast.Ident); ok && key.Name == "Kind" {
					allowed[node.Value] = struct{}{}
				}
			case *ast.ValueSpec:
				for _, ident := range node.Names {
					allowed[ident] = struct{}{}
				}
			}
			return true
		})

		ast.Inspect(file, func(node ast.Node) bool {
			switch node := node.(type) {
			case *ast.Ident:
				if _, isKind := kindConstants[node.Name]; !isKind {
					return true
				}
				if _, ok := allowed[ast.Node(node)]; ok {
					return true
				}
				t.Errorf(
					"%s: %s names %s outside overlay_policy.go — add a named predicate there instead",
					name, fset.Position(node.Pos()), node.Name,
				)
			case *ast.SelectorExpr:
				if node.Sel.Name == "Kind" && isSpecExpr(node.X, specNames) {
					t.Errorf(
						"%s: %s reads spec.Kind outside overlay_policy.go — add a named predicate there instead",
						name, fset.Position(node.Pos()),
					)
				}
			}
			return true
		})
	}
}

// isSpecExpr reports whether an expression roots in an overlaySpec, so the
// guard above only flags `.Kind` reads on a spec. The package has unrelated
// Kind fields (DownloadedBlock, the broadcast caches, the plumtree sender) that
// a blanket `.Kind` match would report as false positives.
//
// names carries the local variables that hold a spec, so copying it out under
// another name does not slip past — `sp := s.spec; table[sp.Kind]` names no
// kind constant and would otherwise be invisible to both halves of the guard.
func isSpecExpr(expr ast.Expr, names map[string]struct{}) bool {
	switch node := expr.(type) {
	case *ast.Ident:
		if node.Name == "spec" {
			return true
		}
		_, ok := names[node.Name]
		return ok
	case *ast.SelectorExpr:
		return node.Sel.Name == "spec"
	case *ast.StarExpr:
		return isSpecExpr(node.X, names)
	case *ast.ParenExpr:
		return isSpecExpr(node.X, names)
	}
	return false
}

// specValueNames collects the identifiers in one file that hold an overlaySpec:
// parameters and results declared as overlaySpec, and variables assigned from
// an expression that is already known to be one. It iterates to a fixed point
// so a chain of copies is still recognised.
func specValueNames(file *ast.File) map[string]struct{} {
	names := map[string]struct{}{}

	isSpecType := func(expr ast.Expr) bool {
		if star, ok := expr.(*ast.StarExpr); ok {
			expr = star.X
		}
		ident, ok := expr.(*ast.Ident)
		return ok && ident.Name == "overlaySpec"
	}

	ast.Inspect(file, func(node ast.Node) bool {
		fields, ok := node.(*ast.FieldList)
		if !ok {
			return true
		}
		for _, field := range fields.List {
			if !isSpecType(field.Type) {
				continue
			}
			for _, ident := range field.Names {
				names[ident.Name] = struct{}{}
			}
		}
		return true
	})

	for {
		grew := false
		record := func(targets []ast.Expr, values []ast.Expr) {
			if len(targets) != len(values) {
				return
			}
			for i, target := range targets {
				ident, ok := target.(*ast.Ident)
				if !ok || ident.Name == "_" {
					continue
				}
				if _, known := names[ident.Name]; known {
					continue
				}
				if isSpecExpr(values[i], names) {
					names[ident.Name] = struct{}{}
					grew = true
				}
			}
		}

		ast.Inspect(file, func(node ast.Node) bool {
			switch node := node.(type) {
			case *ast.AssignStmt:
				record(node.Lhs, node.Rhs)
			case *ast.ValueSpec:
				targets := make([]ast.Expr, 0, len(node.Names))
				for _, ident := range node.Names {
					targets = append(targets, ident)
				}
				if isSpecType(node.Type) {
					for _, ident := range node.Names {
						if _, known := names[ident.Name]; !known {
							names[ident.Name] = struct{}{}
							grew = true
						}
					}
				}
				record(targets, node.Values)
			}
			return true
		})

		if !grew {
			return names
		}
	}
}
