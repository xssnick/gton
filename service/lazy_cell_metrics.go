package service

import (
	"sync/atomic"

	"github.com/xssnick/gton/service/storage"
)

type lazyCellLoadCounters struct {
	stateWindow atomic.Uint64
}

func (c *lazyCellLoadCounters) observeStateWindow() {
	c.stateWindow.Add(1)
}

func (c *lazyCellLoadCounters) snapshot() []storage.LazyCellLoadMetric {
	return []storage.LazyCellLoadMetric{
		{Layer: storage.LazyCellLoadLayerStateWindow, Count: c.stateWindow.Load()},
	}
}
