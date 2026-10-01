package storage

// Where a lazy cell load was answered from. The layers are disjoint and every
// load lands in exactly one of them, so a stacked graph over these adds up to
// the total load rate.
//
// The first three are OBSERVED — the code knows which branch it took. The last
// three are INFERRED from how long the store read took, because neither pebble
// nor the kernel tells a caller which of them served a particular Get. The
// thresholds come from tiers measured on the deployment's own disk (see
// LazyCellLoadDiskThreshold), and the inference is cross-checked by
// gton_storage_lazy_cell_disk_read_bytes_total, which counts real block-layer
// traffic and needs no inference at all: if the disk layer is busy while that
// counter is flat, the thresholds have drifted and want re-measuring.
const (
	// LazyCellLoadLayerStateWindow is a load the resident state window answered
	// without reaching the cell store at all.
	LazyCellLoadLayerStateWindow = "state_window"
	// LazyCellLoadLayerDecodedCache is a hit in the in-process decoded cell
	// cache: no store read, no decode.
	LazyCellLoadLayerDecodedCache = "decoded_cache"
	// LazyCellLoadLayerRecordCache is a hit in the encoded cell record cache:
	// no store read, but the record still gets decoded.
	LazyCellLoadLayerRecordCache = "record_cache"
	// LazyCellLoadLayerBlockCache is a store read fast enough that pebble served
	// it from its own block cache.
	LazyCellLoadLayerBlockCache = "block_cache"
	// LazyCellLoadLayerPageCache is a store read that missed pebble's block
	// cache but was served by the OS without touching the device.
	LazyCellLoadLayerPageCache = "page_cache"
	// LazyCellLoadLayerDisk is a store read slow enough to have reached the
	// device.
	LazyCellLoadLayerDisk = "disk"
)

// LazyCellLoadLayers lists every layer in cost order, cheapest first. Callers
// that render a stack should follow this order so the graph reads as a cost
// gradient.
var LazyCellLoadLayers = []string{
	LazyCellLoadLayerStateWindow,
	LazyCellLoadLayerDecodedCache,
	LazyCellLoadLayerRecordCache,
	LazyCellLoadLayerBlockCache,
	LazyCellLoadLayerPageCache,
	LazyCellLoadLayerDisk,
}

type LazyCellLoadMetric struct {
	Layer string
	Count uint64
}
