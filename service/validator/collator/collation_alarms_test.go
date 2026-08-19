package collator

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/xssnick/gton/service/validator/groups"
)

// collationAlarmObserver counts the two producer alarms and ignores everything
// else. The other methods exist because CollationObserver is a bounded-enum
// interface and a partial implementation would not compile.
type collationAlarmObserver struct {
	alarms map[CollationAlarm]int
	chains map[CollationAlarm]MetricChain
}

func newCollationAlarmObserver() *collationAlarmObserver {
	return &collationAlarmObserver{
		alarms: map[CollationAlarm]int{},
		chains: map[CollationAlarm]MetricChain{},
	}
}

func (o *collationAlarmObserver) ObserveCollationAlarm(chain MetricChain, alarm CollationAlarm) {
	o.alarms[alarm]++
	o.chains[alarm] = chain
}

func (o *collationAlarmObserver) AddCollationBuildInflight(MetricChain, int)                {}
func (o *collationAlarmObserver) ObserveCollationBuild(CollationBuildObservation)           {}
func (o *collationAlarmObserver) ObserveCollationStage(CollationStageObservation)           {}
func (o *collationAlarmObserver) ObserveCollationCandidate(CandidateObservation)            {}
func (o *collationAlarmObserver) ObserveCandidateProduction(CandidateProductionObservation) {}
func (o *collationAlarmObserver) ObserveCollationRetry(MetricChain, ProductionRetryReason)  {}
func (o *collationAlarmObserver) AddCollationWindowInflight(MetricChain, int)               {}
func (o *collationAlarmObserver) ObserveCollationWindow(WindowObservation)                  {}
func (o *collationAlarmObserver) ObserveScheduleLateness(MetricChain, ScheduleEvent, time.Duration) {
}
func (o *collationAlarmObserver) ObserveCollationDeadline(MetricChain, CollationDeadline, DeadlineAction) {
}

// TestCollationAlarmsFireOnTheirOwnFaults pins what the two alarms answer to.
// Both conditions are invisible in every other series — a short collated proof
// rides on a build that SUCCEEDED, and a mandatory-dequeue overflow is one
// error among all the reasons a build fails — so an alarm that fired on the
// wrong input or stayed silent on the right one would leave the operator with
// exactly what the incident left them with: peers rejecting our blocks for a
// pruned branch, and nothing here saying why.
func TestCollationAlarmsFireOnTheirOwnFaults(t *testing.T) {
	request := BuildRequest{
		Session: ActivatedSession{Session: Session{Shard: groups.ShardID{Workchain: 0, Shard: -1 << 63}}},
		Slot:    7,
	}

	for _, tc := range []struct {
		name      string
		candidate *Candidate
		buildErr  error
		want      map[CollationAlarm]int
	}{
		{
			name:      "an ordinary successful build",
			candidate: &Candidate{},
			want:      map[CollationAlarm]int{},
		},
		{
			name: "a block shipped with a short collated proof",
			candidate: &Candidate{Stats: Stats{
				ProcessedScanTraceError: "trace predecessor inbound queue scan to (1,ab): bad envelope",
			}},
			want: map[CollationAlarm]int{CollationAlarmShortCollatedProof: 1},
		},
		{
			name:     "a slot lost to the mandatory dequeue",
			buildErr: fmt.Errorf("%w: shard 0:8000000000000000 dequeued 12", ErrMandatoryDequeueOverflow),
			want:     map[CollationAlarm]int{CollationAlarmMandatoryDequeueOverflow: 1},
		},
		{
			name:     "any other failed build",
			buildErr: errors.New("context canceled"),
			want:     map[CollationAlarm]int{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			observer := newCollationAlarmObserver()
			acquisition := &LocalAcquisition{log: zerolog.Nop(), collationObserver: observer}

			acquisition.reportCollationAlarms(
				MetricChainShardchain, request, tc.candidate, tc.buildErr)

			for _, alarm := range []CollationAlarm{
				CollationAlarmShortCollatedProof,
				CollationAlarmMandatoryDequeueOverflow,
			} {
				if got := observer.alarms[alarm]; got != tc.want[alarm] {
					t.Fatalf("%s alarms = %d, want %d", alarm, got, tc.want[alarm])
				}
				if tc.want[alarm] != 0 && observer.chains[alarm] != MetricChainShardchain {
					t.Fatalf("%s was counted against %v", alarm, observer.chains[alarm])
				}
			}
		})
	}
}

// packageSourceFile resolves one source file of THIS package by name, against
// the directory of this test's own source rather than against the process
// working directory.
//
// An AST gate that hands a bare file name to parser.ParseFile validates whatever
// file happens to sit in the cwd. Under a plain `go test` that is the package
// directory and the gate works by coincidence; under `go test -c` and a
// hand-run binary, under a -exec wrapper, or from any harness that chdirs, it
// parses something else or nothing at all. A structural gate that silently
// agrees with the wrong file is worse than no gate, because it still reports
// PASS. runtime.Caller(0) is taken on this helper, so the anchor is this file
// and not the caller's.
func packageSourceFile(tb testing.TB, name string) string {
	tb.Helper()

	_, self, _, ok := runtime.Caller(0)
	if !ok {
		tb.Fatal("cannot locate this test file to anchor the package source path")
	}
	path := filepath.Join(filepath.Dir(self), name)
	if _, err := os.Stat(path); err != nil {
		tb.Fatalf("resolve package source %s: %v", name, err)
	}

	return path
}

// TestBuildCandidateReportsAlarmsOnEveryCandidateReturn pins both alarms at
// their CALL SITES. The test above drives reportCollationAlarms directly, so
// deleting the calls from BuildCandidate leaves the whole collator suite green
// — which is how an alarm nobody exercises gets removed by a refactor that
// meant no harm.
//
// It is structural rather than behavioural because the two branches are not
// equally reachable from a test: the shardchain branch needs a full acquisition
// with a store, a group snapshot and a masterchain view, and the masterchain
// branch additionally needs a masterchain predecessor that can overflow its own
// mandatory drain. What IS checkable, and is what a refactor breaks, is the
// shape: every path in BuildCandidate that hands a candidate and a build error
// back to the caller reports the alarms first, with those same two values.
//
// The rule is deliberately stated over the RETURNS rather than over the calls,
// so it also catches the other direction — a third branch added later that
// returns a candidate without reporting.
//
// Two things it used to miss are pinned below. It checked only that a call to
// reportCollationAlarms stood there, not what it was passed, so a call degraded
// to constants — a fixed chain, a nil candidate, a nil error — satisfied it
// while reporting nothing about the build that just happened. And being a
// source-text match it agreed with a file rather than with the binary, so the
// method's signature could move out from under it; the compile-time reference
// below is what ties the two together, since it stops compiling if the
// signature this test asserts the shape of is no longer the one that exists.
func TestBuildCandidateReportsAlarmsOnEveryCandidateReturn(t *testing.T) {
	const (
		file     = "local_acquisition_build.go"
		function = "BuildCandidate"
		alarms   = "reportCollationAlarms"
	)

	// The signature the argument check below is written against, bound to the
	// compiled method. A change to it fails here at build time rather than
	// leaving the AST rule quietly matching a call that means something else.
	var report func(*LocalAcquisition, MetricChain, BuildRequest, *Candidate, error)
	report = (*LocalAcquisition).reportCollationAlarms
	if report == nil {
		t.Fatal("reportCollationAlarms is not a method of *LocalAcquisition")
	}

	parsed, err := parser.ParseFile(token.NewFileSet(), packageSourceFile(t, file), nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", file, err)
	}
	var body *ast.BlockStmt
	for _, declaration := range parsed.Decls {
		declared, ok := declaration.(*ast.FuncDecl)
		if ok && declared.Name.Name == function && declared.Body != nil {
			body = declared.Body
			break
		}
	}
	if body == nil {
		t.Fatalf("%s declares no function %s", file, function)
	}

	reported, returns := 0, 0
	ast.Inspect(body, func(node ast.Node) bool {
		block, ok := node.(*ast.BlockStmt)
		if !ok {
			return true
		}
		for i, statement := range block.List {
			if !returnsCandidateAndBuildError(statement) {
				continue
			}
			returns++
			if i == 0 {
				t.Fatalf("%s returns a candidate and a build error as the first statement of a "+
					"block in %s, so nothing reported the alarms", function, file)
			}
			args, ok := methodCallArgs(block.List[i-1], alarms)
			if !ok {
				t.Fatalf("%s returns a candidate and a build error without reporting the alarms first "+
					"(statement %d of a block in %s)", function, i, file)
			}
			// The arguments, not just the call: the alarms must be reported about
			// the build that is being returned. Constants here would satisfy a
			// call-site-only rule while reporting nothing.
			if want := []string{"chain", "request", "candidate", "buildErr"}; !identArgs(args, want) {
				t.Fatalf("%s reports the alarms with %s, want %v — the values it is about to return",
					function, renderArgs(args), want)
			}
			reported++
		}
		return true
	})
	// One per chain. The count is asserted so that deleting a whole branch,
	// alarm and return together, is not silently accepted as compliance.
	if returns != 2 {
		t.Fatalf("%s has %d candidate returns, want one per chain", function, returns)
	}
	if reported != returns {
		t.Fatalf("%d of %d candidate returns report the alarms", reported, returns)
	}
}

// returnsCandidateAndBuildError matches `return candidate, buildErr`, the shape
// both chains of BuildCandidate end on.
func returnsCandidateAndBuildError(statement ast.Stmt) bool {
	returned, ok := statement.(*ast.ReturnStmt)
	if !ok || len(returned.Results) != 2 {
		return false
	}
	first, firstOK := returned.Results[0].(*ast.Ident)
	second, secondOK := returned.Results[1].(*ast.Ident)

	return firstOK && secondOK && first.Name == "candidate" && second.Name == "buildErr"
}

// methodCallArgs matches a bare `x.name(...)` statement and returns its
// arguments.
func methodCallArgs(statement ast.Stmt, name string) ([]ast.Expr, bool) {
	expression, ok := statement.(*ast.ExprStmt)
	if !ok {
		return nil, false
	}
	call, ok := expression.X.(*ast.CallExpr)
	if !ok {
		return nil, false
	}
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != name {
		return nil, false
	}

	return call.Args, true
}

// identArgs reports whether the call was passed exactly these identifiers. A
// literal, a nil, a conversion or a reordering all fail it.
func identArgs(args []ast.Expr, want []string) bool {
	if len(args) != len(want) {
		return false
	}
	for i, arg := range args {
		ident, ok := arg.(*ast.Ident)
		if !ok || ident.Name != want[i] {
			return false
		}
	}

	return true
}

func renderArgs(args []ast.Expr) string {
	rendered := make([]string, 0, len(args))
	for _, arg := range args {
		if ident, ok := arg.(*ast.Ident); ok {
			rendered = append(rendered, ident.Name)
			continue
		}
		rendered = append(rendered, fmt.Sprintf("%T", arg))
	}

	return "(" + strings.Join(rendered, ", ") + ")"
}
