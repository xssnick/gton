package service

import (
	"sync/atomic"

	"github.com/xssnick/gton/service/storage"
)

type lazyCellLoadCounters struct {
	stateWindow atomic.Uint64
}

func (c *lazyCellLoadCounters) observeStateWindow() {
	if c != nil {
		c.stateWindow.Add(1)
	}
}

func (c *lazyCellLoadCounters) snapshot() []storage.LazyCellLoadMetric {
	if c == nil {
		return nil
	}
	return []storage.LazyCellLoadMetric{
		{Layer: storage.LazyCellLoadLayerStateWindow, Count: c.stateWindow.Load()},
	}
}

func (s *Service) LazyCellLoadMetrics() []storage.LazyCellLoadMetric {
	return s.lazyCellLoads.snapshot()
}
