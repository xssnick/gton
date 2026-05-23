package metrics

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/xssnick/gton/service/storage/pebblestore"

	"github.com/prometheus/client_golang/prometheus"
)

type dbCollector struct {
	metrics *Metrics

	available                 *prometheus.Desc
	cacheBytes                *prometheus.Desc
	cacheRequests             *prometheus.Desc
	fileCacheTables           *prometheus.Desc
	diskBytes                 *prometheus.Desc
	liveBytes                 *prometheus.Desc
	liveTables                *prometheus.Desc
	readAmp                   *prometheus.Desc
	l0Files                   *prometheus.Desc
	l0Sublevels               *prometheus.Desc
	l0Bytes                   *prometheus.Desc
	compactionDebtBytes       *prometheus.Desc
	compactionsInProgress     *prometheus.Desc
	compactionInProgressBytes *prometheus.Desc
	memtableBytes             *prometheus.Desc
	memtableCount             *prometheus.Desc
	tableIters                *prometheus.Desc
	flushes                   *prometheus.Desc
	ingests                   *prometheus.Desc
	readCells                 *prometheus.Desc
	writtenCells              *prometheus.Desc
}

func newDBCollector(metrics *Metrics, namespace string) prometheus.Collector {
	generationLabels := []string{"generation", "role"}
	shardLabels := []string{"generation", "role", "shard"}
	return &dbCollector{
		metrics: metrics,
		available: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "storage", "db_status_available"),
			"Whether storage DB status was available during the scrape.",
			nil,
			nil,
		),
		cacheBytes: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "storage", "cell_db_cache_bytes"),
			"Cell DB cache size by cache kind.",
			[]string{"generation", "role", "cache"},
			nil,
		),
		cacheRequests: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "storage", "cell_db_cache_requests_total"),
			"Cell DB block cache requests by result.",
			[]string{"generation", "role", "result"},
			nil,
		),
		fileCacheTables: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "storage", "cell_db_file_cache_tables"),
			"Cell DB file cache table count.",
			generationLabels,
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
		liveTables: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "storage", "cell_db_live_tables"),
			"Cell DB live table count.",
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
		l0Bytes: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "storage", "cell_db_l0_bytes"),
			"Cell DB L0 table size.",
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
		memtableCount: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "storage", "cell_db_memtable_count"),
			"Cell DB memtable count.",
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
	}
}

func (c *dbCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.available
	ch <- c.cacheBytes
	ch <- c.cacheRequests
	ch <- c.fileCacheTables
	ch <- c.diskBytes
	ch <- c.liveBytes
	ch <- c.liveTables
	ch <- c.readAmp
	ch <- c.l0Files
	ch <- c.l0Sublevels
	ch <- c.l0Bytes
	ch <- c.compactionDebtBytes
	ch <- c.compactionsInProgress
	ch <- c.compactionInProgressBytes
	ch <- c.memtableBytes
	ch <- c.memtableCount
	ch <- c.tableIters
	ch <- c.flushes
	ch <- c.ingests
	ch <- c.readCells
	ch <- c.writtenCells
}

func (c *dbCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	status, err := c.metrics.dbStatus(ctx)
	if errors.Is(err, errMetricReaderNotConfigured) {
		return
	}
	if err != nil {
		ch <- prometheus.MustNewConstMetric(c.available, prometheus.GaugeValue, 0)
		return
	}

	ch <- prometheus.MustNewConstMetric(c.available, prometheus.GaugeValue, 1)
	for _, generation := range status.CellGenerations {
		c.collectGeneration(ch, generation)
	}
}

func (c *dbCollector) collectGeneration(ch chan<- prometheus.Metric, generation pebblestore.CellDBGenerationStatus) {
	generationID := strconv.FormatUint(generation.ID, 10)
	role := fallbackLabel(generation.Role)
	ch <- prometheus.MustNewConstMetric(c.cacheBytes, prometheus.GaugeValue, float64(generation.Cache.BlockCacheSize), generationID, role, "block")
	ch <- prometheus.MustNewConstMetric(c.cacheBytes, prometheus.GaugeValue, float64(generation.Cache.FileCacheSize), generationID, role, "file")
	ch <- prometheus.MustNewConstMetric(c.cacheRequests, prometheus.CounterValue, float64(generation.Cache.BlockCacheHits), generationID, role, "hit")
	ch <- prometheus.MustNewConstMetric(c.cacheRequests, prometheus.CounterValue, float64(generation.Cache.BlockCacheMisses), generationID, role, "miss")
	ch <- prometheus.MustNewConstMetric(c.fileCacheTables, prometheus.GaugeValue, float64(generation.Cache.FileCacheTableCount), generationID, role)

	for _, shard := range generation.Shards {
		c.collectShard(ch, generationID, role, strconv.Itoa(shard.Shard), shard)
	}
	c.collectShard(ch, generationID, role, "total", generation.Total)
}

func (c *dbCollector) collectShard(ch chan<- prometheus.Metric, generation string, role string, shardLabel string, shard pebblestore.CellDBShardStatus) {
	ch <- prometheus.MustNewConstMetric(c.diskBytes, prometheus.GaugeValue, float64(shard.DiskSize), generation, role, shardLabel)
	ch <- prometheus.MustNewConstMetric(c.liveBytes, prometheus.GaugeValue, float64(shard.LiveSize), generation, role, shardLabel)
	ch <- prometheus.MustNewConstMetric(c.liveTables, prometheus.GaugeValue, float64(shard.LiveTables), generation, role, shardLabel)
	ch <- prometheus.MustNewConstMetric(c.readAmp, prometheus.GaugeValue, float64(shard.ReadAmp), generation, role, shardLabel)
	ch <- prometheus.MustNewConstMetric(c.l0Files, prometheus.GaugeValue, float64(shard.L0Files), generation, role, shardLabel)
	ch <- prometheus.MustNewConstMetric(c.l0Sublevels, prometheus.GaugeValue, float64(shard.L0Sublevels), generation, role, shardLabel)
	ch <- prometheus.MustNewConstMetric(c.l0Bytes, prometheus.GaugeValue, float64(shard.L0Size), generation, role, shardLabel)
	ch <- prometheus.MustNewConstMetric(c.compactionDebtBytes, prometheus.GaugeValue, float64(shard.CompactionDebt), generation, role, shardLabel)
	ch <- prometheus.MustNewConstMetric(c.compactionsInProgress, prometheus.GaugeValue, float64(shard.CompactionsInProgress), generation, role, shardLabel)
	ch <- prometheus.MustNewConstMetric(c.compactionInProgressBytes, prometheus.GaugeValue, float64(shard.CompactionInProgressSize), generation, role, shardLabel)
	ch <- prometheus.MustNewConstMetric(c.memtableBytes, prometheus.GaugeValue, float64(shard.MemTableSize), generation, role, shardLabel)
	ch <- prometheus.MustNewConstMetric(c.memtableCount, prometheus.GaugeValue, float64(shard.MemTableCount), generation, role, shardLabel)
	ch <- prometheus.MustNewConstMetric(c.tableIters, prometheus.GaugeValue, float64(shard.TableIters), generation, role, shardLabel)
	ch <- prometheus.MustNewConstMetric(c.flushes, prometheus.CounterValue, float64(shard.Flushes), generation, role, shardLabel)
	ch <- prometheus.MustNewConstMetric(c.ingests, prometheus.CounterValue, float64(shard.Ingests), generation, role, shardLabel)
	ch <- prometheus.MustNewConstMetric(c.readCells, prometheus.CounterValue, float64(shard.ReadCells), generation, role, shardLabel)
	ch <- prometheus.MustNewConstMetric(c.writtenCells, prometheus.CounterValue, float64(shard.WrittenCells), generation, role, shardLabel)
}
