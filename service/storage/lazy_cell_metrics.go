package storage

const (
	LazyCellLoadLayerStateWindow  = "state_window"
	LazyCellLoadLayerDecodedCache = "decoded_cache"
	LazyCellLoadLayerPebble       = "pebble"
)

type LazyCellLoadMetric struct {
	Layer string
	Count uint64
}
