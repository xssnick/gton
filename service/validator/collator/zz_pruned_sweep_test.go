package collator

import (
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

// zzPrunedSites collects, across the whole package's tests, every place that
// parses a pruned branch as if it were content. Parsing one never fails — the
// boundary's own payload decodes as data and optional fields read as absent — so
// a reader that walks past what a proof carries produces a plausible zero value
// here and a hard rejection on the far side.
var (
	zzPrunedMu    sync.Mutex
	zzPrunedSites = map[string]int{}
)

func TestMain(m *testing.M) {
	if os.Getenv("PRUNED_SWEEP") == "" {
		os.Exit(runPackageTests(m))
	}

	cell.SetPrunedParseDetector(func(*cell.Cell) {
		var pcs [32]uintptr
		n := runtime.Callers(3, pcs[:])
		frames := runtime.CallersFrames(pcs[:n])
		var chain []string
		for {
			f, more := frames.Next()
			name := f.Function
			if i := strings.LastIndex(name, "/"); i >= 0 {
				name = name[i+1:]
			}
			if !strings.Contains(f.File, "/tvm/cell/") && !strings.Contains(name, "testing.tRunner") && !strings.Contains(name, "runtime.goexit") {
				chain = append(chain, name)
			}
			if len(chain) == 7 || !more {
				break
			}
		}
		zzPrunedMu.Lock()
		zzPrunedSites[strings.Join(chain, " <- ")]++
		zzPrunedMu.Unlock()
	})

	code := runPackageTests(m)

	zzPrunedMu.Lock()
	keys := make([]string, 0, len(zzPrunedSites))
	for k := range zzPrunedSites {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return zzPrunedSites[keys[i]] > zzPrunedSites[keys[j]] })
	fmt.Printf("PRUNED-SWEEP distinct sites: %d\n", len(keys))
	for _, k := range keys {
		fmt.Printf("PRUNED-SWEEP %8d  %s\n", zzPrunedSites[k], k)
	}
	zzPrunedMu.Unlock()

	os.Exit(code)
}
