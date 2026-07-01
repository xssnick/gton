package pebblestore

import (
	"sync/atomic"

	"github.com/xssnick/gton/service/storage"
)

type lazyCellLoadCounters struct {
	decodedCache atomic.Uint64
	pebble       atomic.Uint64
}

func (c *lazyCellLoadCounters) observeDecodedCache() {
	c.decodedCache.Add(1)
}

func (c *lazyCellLoadCounters) observePebble() {
	c.pebble.Add(1)
}

func (c *lazyCellLoadCounters) snapshot() []storage.LazyCellLoadMetric {
	return []storage.LazyCellLoadMetric{
		{Layer: storage.LazyCellLoadLayerDecodedCache, Count: c.decodedCache.Load()},
		{Layer: storage.LazyCellLoadLayerPebble, Count: c.pebble.Load()},
	}
}
