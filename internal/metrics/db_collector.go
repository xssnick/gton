package metrics

import (
	"context"
	"sort"
	"strconv"
	"time"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/storage/pebblestore"

	"github.com/prometheus/client_golang/prometheus"
)

type dbCollector struct {
	metrics *Metrics

	available                 *prometheus.Desc
	generation                *prometheus.Desc
	cacheBytes                *prometheus.Desc
	cacheRequests             *prometheus.Desc
	diskBytes                 *prometheus.Desc
	liveBytes                 *prometheus.Desc
	readAmp                   *prometheus.Desc
	l0Files                   *prometheus.Desc
	l0Sublevels               *prometheus.Desc
	compactionDebtBytes       *prometheus.Desc
	compactionsInProgress     *prometheus.Desc
	compactionInProgressBytes *prometheus.Desc
	memtableBytes             *prometheus.Desc
	tableIters                *prometheus.Desc
	flushes                   *prometheus.Desc
	ingests                   *prometheus.Desc
	readCells                 *prometheus.Desc
	writtenCells              *prometheus.Desc
	lazyCellLoads             *prometheus.Desc

	recordCacheEntries          *prometheus.Desc
	recordCacheBytes            *prometheus.Desc
	recordCacheInserts          *prometheus.Desc
	recordCacheDeclined         *prometheus.Desc
	recordCacheSalvageTruncated *prometheus.Desc
}

func newDBCollector(metrics *Metrics, namespace string) prometheus.Collector {
	generationLabels := []string{"generation"}
	shardLabels := []string{"generation", "shard"}
	return &dbCollector{
		metrics: metrics,
		available: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "storage", "db_status_available"),
			"Whether storage DB status was available during the scrape.",
			nil,
			nil,
		),
		generation: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "storage", "cell_db_generation"),
			"Open cell DB generation id by stable generation role.",
			generationLabels,
			nil,
		),
		cacheBytes: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "storage", "cell_db_cache_bytes"),
			"Cell DB cache size by cache kind.",
			[]string{"generation", "cache"},
			nil,
		),
		cacheRequests: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "storage", "cell_db_cache_requests_total"),
			"Cell DB block cache requests by result.",
			[]string{"generation", "result"},
			nil,
		),
		diskBytes: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "storage", "cell_db_disk_bytes"),
			"Cell DB disk space usage.",
			shardLabels,
			nil,
		),
		liveBytes: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "storage", "cell_db_live_bytes"),
			"Cell DB live table size.",
			shardLabels,
			nil,
		),
		readAmp: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "storage", "cell_db_read_amp"),
			"Cell DB maximum read amplification.",
			shardLabels,
			nil,
		),
		l0Files: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "storage", "cell_db_l0_files"),
			"Cell DB L0 file count.",
			shardLabels,
			nil,
		),
		l0Sublevels: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "storage", "cell_db_l0_sublevels"),
			"Cell DB L0 sublevel count.",
			shardLabels,
			nil,
		),
		compactionDebtBytes: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "storage", "cell_db_compaction_debt_bytes"),
			"Cell DB estimated compaction debt.",
			shardLabels,
			nil,
		),
		compactionsInProgress: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "storage", "cell_db_compactions_in_progress"),
			"Cell DB compactions currently in progress.",
			shardLabels,
			nil,
		),
		compactionInProgressBytes: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "storage", "cell_db_compaction_in_progress_bytes"),
			"Cell DB compaction bytes currently in progress.",
			shardLabels,
			nil,
		),
		memtableBytes: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "storage", "cell_db_memtable_bytes"),
			"Cell DB memtable size.",
			shardLabels,
			nil,
		),
		tableIters: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "storage", "cell_db_table_iters"),
			"Cell DB open table iterators.",
			shardLabels,
			nil,
		),
		flushes: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "storage", "cell_db_flushes_total"),
			"Cell DB flush count.",
			shardLabels,
			nil,
		),
		ingests: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "storage", "cell_db_ingests_total"),
			"Cell DB ingest count.",
			shardLabels,
			nil,
		),
		readCells: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "storage", "cell_db_read_cells_total"),
			"Cell records successfully read from Cell DB.",
			shardLabels,
			nil,
		),
		writtenCells: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "storage", "cell_db_written_cells_total"),
			"Cell records successfully written to Cell DB.",
			shardLabels,
			nil,
		),
		lazyCellLoads: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "storage", "lazy_cell_loads_total"),
			"Successful lazy state cell loads by serving layer.",
			[]string{"layer"},
			nil,
		),
		recordCacheEntries: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "storage", "record_cache_entries"),
			"Live entries resident in the encoded cell record cache.",
			nil,
			nil,
		),
		recordCacheBytes: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "storage", "record_cache_bytes"),
			"Encoded cell record cache memory by kind: resident (live entry bytes), capacity (arena budget), index (derived index overhead).",
			[]string{"kind"},
			nil,
		),
		recordCacheInserts: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "storage", "record_cache_inserts_total"),
			"Records inserted into the encoded cell record cache.",
			nil,
			nil,
		),
		recordCacheDeclined: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "storage", "record_cache_declined_inserts_total"),
			"Record cache inserts declined because no index slot was placeable within the probe budget.",
			nil,
			nil,
		),
		recordCacheSalvageTruncated: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "storage", "record_cache_salvage_truncated_total"),
			"Region rotations whose CLOCK salvage hit the 50% re-append budget and dropped remaining hot entries.",
			nil,
			nil,
		),
	}
}

func (c *dbCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.available
	ch <- c.generation
	ch <- c.cacheBytes
	ch <- c.cacheRequests
	ch <- c.diskBytes
	ch <- c.liveBytes
	ch <- c.readAmp
	ch <- c.l0Files
	ch <- c.l0Sublevels
	ch <- c.compactionDebtBytes
	ch <- c.compactionsInProgress
	ch <- c.compactionInProgressBytes
	ch <- c.memtableBytes
	ch <- c.tableIters
	ch <- c.flushes
	ch <- c.ingests
	ch <- c.readCells
	ch <- c.writtenCells
	ch <- c.lazyCellLoads
	ch <- c.recordCacheEntries
	ch <- c.recordCacheBytes
	ch <- c.recordCacheInserts
	ch <- c.recordCacheDeclined
	ch <- c.recordCacheSalvageTruncated
}

func (c *dbCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	status, err := c.metrics.dbStatusReader(ctx)
	if err != nil {
		ch <- prometheus.MustNewConstMetric(c.available, prometheus.GaugeValue, 0)
		return
	}

	ch <- prometheus.MustNewConstMetric(c.available, prometheus.GaugeValue, 1)
	c.collectLazyCellLoads(ch, status.LazyCellLoads)
	c.collectRecordCache(ch, status.RecordCache)
	for _, generation := range status.CellGenerations {
		c.collectGeneration(ch, generation)
	}
}

// collectRecordCache exports the record cache's own series. A disabled cache
// (nil status) exports nothing: absent series say "tier off" where zeros would
// say "tier idle", and the record_cache LAYER of lazy_cell_loads_total is
// seeded to zero from the canonical layer list either way.
func (c *dbCollector) collectRecordCache(ch chan<- prometheus.Metric, status *pebblestore.RecordCacheStatus) {
	if status == nil {
		return
	}

	ch <- prometheus.MustNewConstMetric(c.recordCacheEntries, prometheus.GaugeValue, float64(status.Entries))
	ch <- prometheus.MustNewConstMetric(c.recordCacheBytes, prometheus.GaugeValue, float64(status.ResidentBytes), "resident")
	ch <- prometheus.MustNewConstMetric(c.recordCacheBytes, prometheus.GaugeValue, float64(status.CapacityBytes), "capacity")
	ch <- prometheus.MustNewConstMetric(c.recordCacheBytes, prometheus.GaugeValue, float64(status.IndexBytes), "index")
	ch <- prometheus.MustNewConstMetric(c.recordCacheInserts, prometheus.CounterValue, float64(status.Inserts))
	ch <- prometheus.MustNewConstMetric(c.recordCacheDeclined, prometheus.CounterValue, float64(status.Declined))
	ch <- prometheus.MustNewConstMetric(c.recordCacheSalvageTruncated, prometheus.CounterValue, float64(status.SalvageTruncated))
}

func (c *dbCollector) collectLazyCellLoads(ch chan<- prometheus.Metric, dbLoads []storage.LazyCellLoadMetric) {
	// Seeded from the canonical layer list so every layer is exported even at
	// zero — a stack with a series that appears only once it is non-zero reads
	// as a change in shape rather than a change in value, and adding a layer
	// here cannot be forgotten.
	counts := make(map[string]uint64, len(storage.LazyCellLoadLayers))
	for _, layer := range storage.LazyCellLoadLayers {
		counts[layer] = 0
	}
	for _, metric := range c.metrics.lazyCellLoads() {
		if metric.Layer != "" {
			counts[metric.Layer] += metric.Count
		}
	}
	for _, metric := range dbLoads {
		if metric.Layer != "" {
			counts[metric.Layer] += metric.Count
		}
	}

	layers := make([]string, 0, len(counts))
	for layer := range counts {
		layers = append(layers, layer)
	}
	sort.Strings(layers)

	for _, layer := range layers {
		ch <- prometheus.MustNewConstMetric(c.lazyCellLoads, prometheus.CounterValue, float64(counts[layer]), layer)
	}
}

func (c *dbCollector) collectGeneration(ch chan<- prometheus.Metric, generation pebblestore.CellDBGenerationStatus) {
	generationLabel := cellDBGenerationLabel(generation.Role)
	if generationLabel == "" {
		return
	}

	ch <- prometheus.MustNewConstMetric(c.generation, prometheus.GaugeValue, float64(generation.ID), generationLabel)
	ch <- prometheus.MustNewConstMetric(c.cacheBytes, prometheus.GaugeValue, float64(generation.Cache.BlockCacheSize), generationLabel, "block")
	ch <- prometheus.MustNewConstMetric(c.cacheBytes, prometheus.GaugeValue, float64(generation.Cache.FileCacheSize), generationLabel, "file")
	ch <- prometheus.MustNewConstMetric(c.cacheRequests, prometheus.CounterValue, float64(generation.Cache.BlockCacheHits), generationLabel, "hit")
	ch <- prometheus.MustNewConstMetric(c.cacheRequests, prometheus.CounterValue, float64(generation.Cache.BlockCacheMisses), generationLabel, "miss")

	for _, shard := range generation.Shards {
		c.collectShard(ch, generationLabel, strconv.Itoa(shard.Shard), shard)
	}
	c.collectShard(ch, generationLabel, "total", generation.Total)
}

func cellDBGenerationLabel(role string) string {
	switch role {
	case "active", "pending":
		return role
	default:
		return ""
	}
}

func (c *dbCollector) collectShard(ch chan<- prometheus.Metric, generation string, shardLabel string, shard pebblestore.CellDBShardStatus) {
	ch <- prometheus.MustNewConstMetric(c.diskBytes, prometheus.GaugeValue, float64(shard.DiskSize), generation, shardLabel)
	ch <- prometheus.MustNewConstMetric(c.liveBytes, prometheus.GaugeValue, float64(shard.LiveSize), generation, shardLabel)
	ch <- prometheus.MustNewConstMetric(c.readAmp, prometheus.GaugeValue, float64(shard.ReadAmp), generation, shardLabel)
	ch <- prometheus.MustNewConstMetric(c.l0Files, prometheus.GaugeValue, float64(shard.L0Files), generation, shardLabel)
	ch <- prometheus.MustNewConstMetric(c.l0Sublevels, prometheus.GaugeValue, float64(shard.L0Sublevels), generation, shardLabel)
	ch <- prometheus.MustNewConstMetric(c.compactionDebtBytes, prometheus.GaugeValue, float64(shard.CompactionDebt), generation, shardLabel)
	ch <- prometheus.MustNewConstMetric(c.compactionsInProgress, prometheus.GaugeValue, float64(shard.CompactionsInProgress), generation, shardLabel)
	ch <- prometheus.MustNewConstMetric(c.compactionInProgressBytes, prometheus.GaugeValue, float64(shard.CompactionInProgressSize), generation, shardLabel)
	ch <- prometheus.MustNewConstMetric(c.memtableBytes, prometheus.GaugeValue, float64(shard.MemTableSize), generation, shardLabel)
	ch <- prometheus.MustNewConstMetric(c.tableIters, prometheus.GaugeValue, float64(shard.TableIters), generation, shardLabel)
	ch <- prometheus.MustNewConstMetric(c.flushes, prometheus.CounterValue, float64(shard.Flushes), generation, shardLabel)
	ch <- prometheus.MustNewConstMetric(c.ingests, prometheus.CounterValue, float64(shard.Ingests), generation, shardLabel)
	ch <- prometheus.MustNewConstMetric(c.readCells, prometheus.CounterValue, float64(shard.ReadCells), generation, shardLabel)
	ch <- prometheus.MustNewConstMetric(c.writtenCells, prometheus.CounterValue, float64(shard.WrittenCells), generation, shardLabel)
}
