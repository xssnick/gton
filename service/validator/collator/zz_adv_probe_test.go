package collator

import (
	"context"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type advLazifier struct {
	records map[cell.Hash][]byte

	mu            sync.Mutex
	calls         int
	firstSite     map[string]int
	allSites      map[string]int
	seen          map[cell.Hash]bool
	siteOfCell    map[cell.Hash]string
	warmFromOther map[string]int
}

func newAdvLazifier() *advLazifier {
	return &advLazifier{
		records:       map[cell.Hash][]byte{},
		firstSite:     map[string]int{},
		allSites:      map[string]int{},
		seen:          map[cell.Hash]bool{},
		siteOfCell:    map[cell.Hash]string{},
		warmFromOther: map[string]int{},
	}
}

func (l *advLazifier) index(tb testing.TB, c *cell.Cell) {
	tb.Helper()
	if c == nil {
		return
	}
	h := c.HashKey()
	if _, ok := l.records[h]; ok {
		return
	}
	rec, err := storage.CellRecordFromCell(c)
	if err != nil {
		tb.Fatalf("cell record: %v", err)
	}
	l.records[h] = storage.EncodeCellRecord(rec)
	for i := 0; i < int(c.RefsNum()); i++ {
		r, err := c.PeekRef(i)
		if err != nil {
			tb.Fatalf("peek ref: %v", err)
		}
		l.index(tb, r)
	}
}

func advSite() string {
	var pcs [64]uintptr
	n := runtime.Callers(3, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	var lastCollator string
	for {
		f, more := frames.Next()
		name := f.Function
		if strings.Contains(name, "/service/validator/collator.") {
			short := name[strings.LastIndex(name, ".")+1:]
			if !strings.HasPrefix(short, "func") {
				return "collator." + short
			}
			if lastCollator == "" {
				lastCollator = "collator.<closure>"
			}
		}
		if !more {
			break
		}
	}
	if lastCollator != "" {
		return lastCollator
	}
	return "unknown"
}

// advPhase returns the collation phase frame.
func advPhase() string {
	var pcs [64]uintptr
	n := runtime.Callers(3, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	last := "none"
	for {
		f, more := frames.Next()
		name := f.Function
		if strings.Contains(name, "/service/validator/collator.") {
			short := name[strings.LastIndex(name, ".")+1:]
			switch short {
			case "cleanupOutQueue", "processDispatchQueue", "processInternals",
				"processExternals", "processNewMessages", "updateProcessedInfo",
				"finishAccounts", "finish", "prepare":
				last = short
			}
		}
		if !more {
			break
		}
	}
	return last
}

func (l *advLazifier) loader() cell.LazyCellLoader {
	var self cell.LazyCellLoader
	self = func(h cell.Hash) (*cell.Cell, error) {
		site := advPhase() + " | " + advSite()
		l.mu.Lock()
		l.calls++
		l.allSites[site]++
		if !l.seen[h] {
			l.seen[h] = true
			l.firstSite[site]++
			l.siteOfCell[h] = site
		} else if l.siteOfCell[h] != site {
			l.warmFromOther[site]++
		}
		l.mu.Unlock()

		enc, ok := l.records[h]
		if !ok {
			return nil, cell.ErrLazyRefNotFound
		}
		return storage.DecodeLazyCellRecordTrusted(h[:], enc, self)
	}
	return self
}

func (l *advLazifier) root(tb testing.TB, c *cell.Cell) *cell.Cell {
	tb.Helper()
	l.index(tb, c)
	h := c.HashKey()
	out, err := storage.DecodeLazyCellRecordTrusted(h[:], l.records[h], l.loader())
	if err != nil {
		tb.Fatalf("lazy root: %v", err)
	}
	return out
}

func TestZZAdvProbeDistinctBySite(t *testing.T) {
	req, _ := benchMainnetRequest(t, benchMainnetFiller)

	lz := newAdvLazifier()
	req.Previous.State = lz.root(t, req.Previous.State)

	builder := testBuilder()
	if _, err := builder.BuildShard(context.Background(), req); err != nil {
		t.Fatalf("build: %v", err)
	}

	lz.mu.Lock()
	defer lz.mu.Unlock()
	t.Logf("TOTAL calls %d  DISTINCT %d", lz.calls, len(lz.seen))

	type kv struct {
		k          string
		first, all int
		warm       int
	}
	var list []kv
	for k, v := range lz.allSites {
		list = append(list, kv{k, lz.firstSite[k], v, lz.warmFromOther[k]})
	}
	sort.Slice(list, func(i, j int) bool { return list[i].first > list[j].first })
	t.Logf("%-70s %8s %8s %8s", "phase | site", "FIRST", "calls", "warm-by-other")
	for _, e := range list {
		t.Logf("%-70s %8d %8d %8d", e.k, e.first, e.all, e.warm)
	}
}
