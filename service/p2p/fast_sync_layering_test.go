package p2p

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

// fastSyncCoreFiles are the FastSync protocol rules: shard routing and roster
// identity, certificate validation and member slots, and the peer registry.
// They are a pure state machine — every entry point takes the clock as a
// parameter — and the rest of FastSync (fast_sync_overlay.go,
// fast_sync_certificate*.go) is the glue that runs them against a live Node.
//
// The three files below could live in their own package: their only dependency
// on the rest of p2p is PeerID. They deliberately do not, because the query
// limiter cannot follow them (it switches over twenty TL query types defined
// across peer_queries.go, protocol.go, state_overlay.go and peer_next_blocks.go)
// and because six test files that would stay behind depend on the
// certificate-forging helpers that would leave, which would mean publishing an
// API for forging member certificates.
//
// TestFastSyncCoreStaysSelfContained is what a package boundary would have
// bought instead: the compiler cannot enforce the separation inside one
// package, so this does.
var fastSyncCoreFiles = []string{
	"fast_sync_model.go",
	"fast_sync_membership.go",
	"fast_sync_peer_runtime.go",
}

func TestFastSyncCoreStaysSelfContained(t *testing.T) {
	// Each rule is something a package boundary would have made impossible.
	bannedIdents := map[string]string{
		"Node":                "the core must not reach the node; that is what the glue in fast_sync_overlay.go is for",
		"overlaySubscription": "the core must not know about overlay subscriptions",
		"overlayPeer":         "the core owns its own peer descriptors and must not use the transport's",
		"overlaySpec":         "the core must not read overlay configuration",
	}
	bannedPackages := map[string]string{
		"context": "the core is synchronous; cancellation belongs to the glue",
		"net":     "the core must not touch the network",
		"adnl":    "the core must not touch a transport",
	}

	for _, name := range fastSyncCoreFiles {
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}

		for _, spec := range file.Imports {
			path := strings.Trim(spec.Path.Value, `"`)
			base := path
			if idx := strings.LastIndex(base, "/"); idx >= 0 {
				base = base[idx+1:]
			}
			if reason, banned := bannedPackages[base]; banned {
				t.Errorf("%s imports %q — %s", name, path, reason)
			}
			if strings.Contains(path, "gton/service") {
				t.Errorf("%s imports %q — the core must not depend on the node's own packages", name, path)
			}
		}

		ast.Inspect(file, func(node ast.Node) bool {
			switch node := node.(type) {
			case *ast.Ident:
				if reason, banned := bannedIdents[node.Name]; banned {
					t.Errorf("%s: %s names %s — %s", name, fset.Position(node.Pos()), node.Name, reason)
				}
			case *ast.GoStmt:
				t.Errorf(
					"%s: %s starts a goroutine — the core is synchronous so its tests stay deterministic",
					name, fset.Position(node.Pos()),
				)
			case *ast.SelectorExpr:
				pkg, ok := node.X.(*ast.Ident)
				if !ok || pkg.Name != "time" || node.Sel.Name != "Now" {
					return true
				}
				t.Errorf(
					"%s: %s calls time.Now() — every core entry point takes `now time.Time` so callers control the clock",
					name, fset.Position(node.Pos()),
				)
			}
			return true
		})
	}
}
