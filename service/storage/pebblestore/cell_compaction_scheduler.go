package pebblestore

import (
	"sort"
	"sync"

	"github.com/cockroachdb/pebble/v2"
)

type cellCompactionController struct {
	mu         sync.Mutex
	cond       *sync.Cond
	paused     bool
	maxRunning int
	running    int
	foreground int
	schedulers map[*cellCompactionScheduler]struct{}
}

type cellCompactionScheduler struct {
	controller   *cellCompactionController
	db           pebble.DBForCompaction
	running      int
	unregistered bool
}

type cellCompactionGrantCandidate struct {
	scheduler *cellCompactionScheduler
	waiting   pebble.WaitingCompaction
}

func newCellCompactionController(maxRunning int) *cellCompactionController {
	if maxRunning < 1 {
		maxRunning = 1
	}
	controller := &cellCompactionController{
		paused:     true,
		maxRunning: maxRunning,
		schedulers: map[*cellCompactionScheduler]struct{}{},
	}
	controller.cond = sync.NewCond(&controller.mu)
	return controller
}

func (c *cellCompactionController) newScheduler() *cellCompactionScheduler {
	return &cellCompactionScheduler{controller: c}
}

func (c *cellCompactionController) start() {
	c.mu.Lock()
	c.paused = false
	c.mu.Unlock()
	go c.tryGrant()
}

func (c *cellCompactionController) reserve(s *cellCompactionScheduler) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.paused || s.unregistered || c.running >= c.effectiveMaxRunningLocked() {
		return false
	}
	c.running++
	s.running++
	return true
}

func (c *cellCompactionController) release(s *cellCompactionScheduler) {
	c.mu.Lock()
	if c.running > 0 {
		c.running--
	}
	if s.running > 0 {
		s.running--
	}
	c.cond.Broadcast()
	c.mu.Unlock()

	go c.tryGrant()
}

func (c *cellCompactionController) tryGrant() {
	for {
		candidates := c.grantCandidates()
		if len(candidates) == 0 {
			return
		}

		granted := false
		for _, candidate := range candidates {
			scheduler := candidate.scheduler
			if !c.reserve(scheduler) {
				continue
			}
			if scheduler.db.Schedule(scheduler) {
				granted = true
				continue
			}
			c.release(scheduler)
		}
		if !granted {
			return
		}
	}
}

func (c *cellCompactionController) grantCandidates() []cellCompactionGrantCandidate {
	schedulers := c.snapshotGrantSchedulers()
	if len(schedulers) == 0 {
		return nil
	}

	candidates := make([]cellCompactionGrantCandidate, 0, len(schedulers))
	for _, scheduler := range schedulers {
		waiting, ok := scheduler.waitingCompaction()
		if !ok {
			continue
		}
		candidates = append(candidates, cellCompactionGrantCandidate{
			scheduler: scheduler,
			waiting:   waiting,
		})
	}

	sort.Slice(candidates, func(i, j int) bool {
		return cellCompactionCandidateLess(candidates[i], candidates[j])
	})
	return candidates
}

func (c *cellCompactionController) snapshotGrantSchedulers() []*cellCompactionScheduler {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.paused || c.running >= c.effectiveMaxRunningLocked() {
		return nil
	}

	schedulers := make([]*cellCompactionScheduler, 0, len(c.schedulers))
	for scheduler := range c.schedulers {
		if scheduler.unregistered || scheduler.db == nil {
			continue
		}
		schedulers = append(schedulers, scheduler)
	}
	return schedulers
}

func cellCompactionCandidateLess(left, right cellCompactionGrantCandidate) bool {
	if left.waiting.Optional != right.waiting.Optional {
		return !left.waiting.Optional
	}
	if left.waiting.Priority != right.waiting.Priority {
		return left.waiting.Priority > right.waiting.Priority
	}
	return left.waiting.Score > right.waiting.Score
}

func (c *cellCompactionController) beginForegroundRead() func() {
	c.mu.Lock()
	c.foreground++
	c.cond.Broadcast()
	c.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			c.mu.Lock()
			if c.foreground > 0 {
				c.foreground--
			}
			c.mu.Unlock()

			go c.tryGrant()
		})
	}
}

func (c *cellCompactionController) effectiveMaxRunningLocked() int {
	if c.foreground > 0 {
		maxRunning := c.maxRunning / 2
		if maxRunning < 1 {
			return 1
		}
		return maxRunning
	}
	return c.maxRunning
}

func (s *cellCompactionScheduler) Register(_ int, db pebble.DBForCompaction) {
	c := s.controller
	c.mu.Lock()
	s.db = db
	c.schedulers[s] = struct{}{}
	c.mu.Unlock()
}

func (s *cellCompactionScheduler) Unregister() {
	c := s.controller
	c.mu.Lock()
	s.unregistered = true
	delete(c.schedulers, s)
	for s.running > 0 {
		c.cond.Wait()
	}
	c.mu.Unlock()
}

func (s *cellCompactionScheduler) TrySchedule() (bool, pebble.CompactionGrantHandle) {
	if !s.controller.reserve(s) {
		return false, nil
	}
	return true, s
}

func (s *cellCompactionScheduler) UpdateGetAllowedWithoutPermission() {
	go s.controller.tryGrant()
}

func (s *cellCompactionScheduler) waitingCompaction() (pebble.WaitingCompaction, bool) {
	s.controller.mu.Lock()
	db := s.db
	unregistered := s.unregistered
	s.controller.mu.Unlock()
	if db == nil || unregistered {
		return pebble.WaitingCompaction{}, false
	}

	waiting, compaction := db.GetWaitingCompaction()
	return compaction, waiting
}

func (s *cellCompactionScheduler) Started() {}

func (s *cellCompactionScheduler) MeasureCPU(pebble.CompactionGoroutineKind) {}

func (s *cellCompactionScheduler) CumulativeStats(pebble.CompactionGrantHandleStats) {}

func (s *cellCompactionScheduler) Done() {
	s.controller.release(s)
}
