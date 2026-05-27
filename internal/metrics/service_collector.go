package metrics

import (
	"errors"
	"fmt"
	"time"

	"github.com/xssnick/gton/service"
	"github.com/xssnick/gton/service/blocksync"
	"github.com/xssnick/gton/service/storage"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/xssnick/tonutils-go/ton"
)

const topShard = int64(-1 << 63)

type serviceCollector struct {
	metrics *Metrics

	syncLagSeconds        *prometheus.Desc
	syncBlockUTimeSeconds *prometheus.Desc
	syncLocalSeqno        *prometheus.Desc
	syncNetworkSeqno      *prometheus.Desc
	syncGapBlocks         *prometheus.Desc
	syncRecentTPS         *prometheus.Desc
	syncRecentTx          *prometheus.Desc
	syncRecentComplete    *prometheus.Desc
	serviceBackgroundTask *prometheus.Desc

	p2pOverlayPeers     *prometheus.Desc
	p2pOverlayNeighbors *prometheus.Desc
	p2pQueueItems       *prometheus.Desc
	p2pQueueBytes       *prometheus.Desc
	p2pQueueMaxItems    *prometheus.Desc
	p2pQueueMaxBytes    *prometheus.Desc
	p2pQueuePushed      *prometheus.Desc
	p2pQueueDropped     *prometheus.Desc
	p2pBroadcasts       *prometheus.Desc
	p2pBroadcastDrops   *prometheus.Desc
	p2pRebroadcastSent  *prometheus.Desc
	p2pRebroadcastDrop  *prometheus.Desc
	p2pFECActiveStreams *prometheus.Desc
	p2pFECActiveBytes   *prometheus.Desc
	p2pFECDelivered     *prometheus.Desc
	p2pFECDropped       *prometheus.Desc
	p2pFECEvicted       *prometheus.Desc
	p2pFECCompleted     *prometheus.Desc
	p2pFECDeliveredHits *prometheus.Desc

	blocksyncQueueItems    *prometheus.Desc
	blocksyncQueueCapacity *prometheus.Desc
	blocksyncQueueDropped  *prometheus.Desc
	blocksyncChains        *prometheus.Desc
}

func newServiceCollector(metrics *Metrics, namespace string) prometheus.Collector {
	return &serviceCollector{
		metrics: metrics,
		syncLagSeconds: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "sync", "lag_seconds"),
			"Local chain lag in seconds by chain and shard, computed at scrape time.",
			[]string{"chain", "shard"},
			nil,
		),
		syncBlockUTimeSeconds: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "sync", "block_utime_seconds"),
			"Unix timestamp of the local block generation time by chain and shard.",
			[]string{"chain", "shard"},
			nil,
		),
		syncLocalSeqno: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "sync", "local_seqno"),
			"Local synchronized block seqno by chain and shard.",
			[]string{"chain", "shard"},
			nil,
		),
		syncNetworkSeqno: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "sync", "network_seqno"),
			"Latest observed network block seqno by chain and shard.",
			[]string{"chain", "shard"},
			nil,
		),
		syncGapBlocks: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "sync", "gap_blocks"),
			"Block gap between latest observed network block and local synchronized block.",
			[]string{"chain", "shard"},
			nil,
		),
		syncRecentTPS: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "sync", "recent_tps"),
			"Recent local TPS over the service status window.",
			nil,
			nil,
		),
		syncRecentTx: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "sync", "recent_transactions"),
			"Recent local transaction count over the service status window.",
			nil,
			nil,
		),
		syncRecentComplete: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "sync", "recent_tps_complete"),
			"Whether the recent TPS status window was fully available.",
			nil,
			nil,
		),
		serviceBackgroundTask: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "service", "background_task"),
			"Current background service task as a labeled gauge.",
			[]string{"task"},
			nil,
		),
		p2pOverlayPeers: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "p2p", "overlay_peers"),
			"Known overlay peers by overlay and liveness.",
			[]string{"overlay", "state"},
			nil,
		),
		p2pOverlayNeighbors: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "p2p", "overlay_neighbours"),
			"Active overlay neighbours by overlay and liveness.",
			[]string{"overlay", "state"},
			nil,
		),
		p2pQueueItems: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "p2p", "queue_items"),
			"Current P2P queue length.",
			[]string{"queue"},
			nil,
		),
		p2pQueueBytes: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "p2p", "queue_bytes"),
			"Current P2P queue estimated bytes.",
			[]string{"queue"},
			nil,
		),
		p2pQueueMaxItems: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "p2p", "queue_max_items"),
			"Configured P2P queue item limit.",
			[]string{"queue"},
			nil,
		),
		p2pQueueMaxBytes: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "p2p", "queue_max_bytes"),
			"Configured P2P queue byte limit.",
			[]string{"queue"},
			nil,
		),
		p2pQueuePushed: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "p2p", "queue_pushed_total"),
			"Total accepted P2P queue pushes.",
			[]string{"queue"},
			nil,
		),
		p2pQueueDropped: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "p2p", "queue_dropped_total"),
			"Total dropped P2P queue pushes.",
			[]string{"queue"},
			nil,
		),
		p2pBroadcasts: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "p2p", "broadcasts_total"),
			"Total P2P broadcasts accepted or successfully rebroadcasted by type.",
			[]string{"direction", "overlay", "kind"},
			nil,
		),
		p2pBroadcastDrops: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "p2p", "broadcast_dropped_total"),
			"Total inbound P2P broadcasts dropped before acceptance by type and reason.",
			[]string{"overlay", "kind", "reason"},
			nil,
		),
		p2pRebroadcastSent: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "p2p", "rebroadcast_sent_total"),
			"Total successful P2P rebroadcast sends.",
			[]string{"queue"},
			nil,
		),
		p2pRebroadcastDrop: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "p2p", "rebroadcast_dropped_total"),
			"Total P2P rebroadcast messages dropped before a successful send.",
			[]string{"queue"},
			nil,
		),
		p2pFECActiveStreams: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "p2p", "fec_receiver_active_streams"),
			"Current active inbound FEC broadcast decoder streams by overlay.",
			[]string{"overlay"},
			nil,
		),
		p2pFECActiveBytes: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "p2p", "fec_receiver_active_bytes"),
			"Current estimated bytes reserved by inbound FEC broadcast decoder streams by overlay.",
			[]string{"overlay"},
			nil,
		),
		p2pFECDelivered: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "p2p", "fec_receiver_delivered_cache_items"),
			"Current delivered inbound FEC broadcast cache items by overlay.",
			[]string{"overlay"},
			nil,
		),
		p2pFECDropped: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "p2p", "fec_receiver_dropped_total"),
			"Total inbound FEC broadcast streams dropped by receiver budget by overlay.",
			[]string{"overlay"},
			nil,
		),
		p2pFECEvicted: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "p2p", "fec_receiver_evicted_total"),
			"Total inbound FEC broadcast streams evicted by receiver cleanup by overlay.",
			[]string{"overlay"},
			nil,
		),
		p2pFECCompleted: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "p2p", "fec_receiver_completed_total"),
			"Total inbound FEC broadcasts decoded and delivered by overlay.",
			[]string{"overlay"},
			nil,
		),
		p2pFECDeliveredHits: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "p2p", "fec_receiver_delivered_cache_hits_total"),
			"Total late inbound FEC parts skipped by delivered broadcast cache by overlay.",
			[]string{"overlay"},
			nil,
		),
		blocksyncQueueItems: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "blocksync", "queue_items"),
			"Current block sync queue length.",
			[]string{"queue"},
			nil,
		),
		blocksyncQueueCapacity: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "blocksync", "queue_capacity"),
			"Block sync queue capacity.",
			[]string{"queue"},
			nil,
		),
		blocksyncQueueDropped: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "blocksync", "queue_dropped_total"),
			"Total dropped block sync queue items.",
			[]string{"queue"},
			nil,
		),
		blocksyncChains: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "blocksync", "chains"),
			"Block sync chain count by state.",
			[]string{"state"},
			nil,
		),
	}
}

func (c *serviceCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.syncLagSeconds
	ch <- c.syncBlockUTimeSeconds
	ch <- c.syncLocalSeqno
	ch <- c.syncNetworkSeqno
	ch <- c.syncGapBlocks
	ch <- c.syncRecentTPS
	ch <- c.syncRecentTx
	ch <- c.syncRecentComplete
	ch <- c.serviceBackgroundTask
	ch <- c.p2pOverlayPeers
	ch <- c.p2pOverlayNeighbors
	ch <- c.p2pQueueItems
	ch <- c.p2pQueueBytes
	ch <- c.p2pQueueMaxItems
	ch <- c.p2pQueueMaxBytes
	ch <- c.p2pQueuePushed
	ch <- c.p2pQueueDropped
	ch <- c.p2pBroadcasts
	ch <- c.p2pBroadcastDrops
	ch <- c.p2pRebroadcastSent
	ch <- c.p2pRebroadcastDrop
	ch <- c.p2pFECActiveStreams
	ch <- c.p2pFECActiveBytes
	ch <- c.p2pFECDelivered
	ch <- c.p2pFECDropped
	ch <- c.p2pFECEvicted
	ch <- c.p2pFECCompleted
	ch <- c.p2pFECDeliveredHits
	ch <- c.blocksyncQueueItems
	ch <- c.blocksyncQueueCapacity
	ch <- c.blocksyncQueueDropped
	ch <- c.blocksyncChains
}

func (c *serviceCollector) Collect(ch chan<- prometheus.Metric) {
	snapshot, err := c.metrics.serviceStatus()
	if errors.Is(err, errMetricReaderNotConfigured) {
		return
	}
	if err != nil {
		return
	}

	now := time.Now()
	c.collectSync(ch, snapshot, now)
	c.collectP2P(ch, snapshot)
	c.collectBlockSync(ch, snapshot.BlockSync)
}

func (c *serviceCollector) collectSync(ch chan<- prometheus.Metric, snapshot service.StatusSnapshot, now time.Time) {
	c.collectChain(ch, "masterchain", "masterchain", snapshot.LocalMasterchain, snapshot.LatestMasterchain, snapshot.LocalMasterchainUtime, now)
	if len(snapshot.LocalBasechainShards) == 0 {
		c.collectChain(ch, "shardchain", "basechain", snapshot.LocalBasechain, snapshot.LatestBasechain, snapshot.LocalBasechainUtime, now)
	} else {
		latest := latestBasechainShardsByKey(snapshot)
		for _, shard := range snapshot.LocalBasechainShards {
			network := latest[storage.ShardKeyFromBlock(shard.Block)]
			c.collectChain(ch, "shardchain", shardLabel(shard.Block), &shard.Block, network, shard.Utime, now)
		}
	}

	if snapshot.RecentTPS.WindowMasters > 0 {
		ch <- prometheus.MustNewConstMetric(c.syncRecentTPS, prometheus.GaugeValue, snapshot.RecentTPS.TPS)
		ch <- prometheus.MustNewConstMetric(c.syncRecentTx, prometheus.GaugeValue, float64(snapshot.RecentTPS.Transactions))
		complete := 0.0
		if snapshot.RecentTPS.Complete {
			complete = 1
		}
		ch <- prometheus.MustNewConstMetric(c.syncRecentComplete, prometheus.GaugeValue, complete)
	}

	task := snapshot.BackgroundTask
	if task == "" {
		task = "idle"
	}
	ch <- prometheus.MustNewConstMetric(c.serviceBackgroundTask, prometheus.GaugeValue, 1, task)
}

func (c *serviceCollector) collectChain(ch chan<- prometheus.Metric, chain string, shard string, local *ton.BlockIDExt, network *ton.BlockIDExt, utime int64, now time.Time) {
	if local != nil {
		ch <- prometheus.MustNewConstMetric(c.syncLocalSeqno, prometheus.GaugeValue, float64(local.SeqNo), chain, shard)
	}
	if network != nil {
		ch <- prometheus.MustNewConstMetric(c.syncNetworkSeqno, prometheus.GaugeValue, float64(network.SeqNo), chain, shard)
	}
	if local != nil && network != nil {
		gap := uint32(0)
		if network.SeqNo > local.SeqNo {
			gap = network.SeqNo - local.SeqNo
		}
		ch <- prometheus.MustNewConstMetric(c.syncGapBlocks, prometheus.GaugeValue, float64(gap), chain, shard)
	}
	if utime <= 0 {
		return
	}
	ch <- prometheus.MustNewConstMetric(c.syncBlockUTimeSeconds, prometheus.GaugeValue, float64(utime), chain, shard)
	lag := now.Unix() - utime
	if lag < 0 {
		lag = 0
	}
	ch <- prometheus.MustNewConstMetric(c.syncLagSeconds, prometheus.GaugeValue, float64(lag), chain, shard)
}

func (c *serviceCollector) collectP2P(ch chan<- prometheus.Metric, snapshot service.StatusSnapshot) {
	for _, overlay := range snapshot.Overlays {
		ch <- prometheus.MustNewConstMetric(c.p2pOverlayPeers, prometheus.GaugeValue, float64(overlay.KnownPeers), overlay.Name, "known")
		ch <- prometheus.MustNewConstMetric(c.p2pOverlayPeers, prometheus.GaugeValue, float64(overlay.AliveKnownPeers), overlay.Name, "alive")
		ch <- prometheus.MustNewConstMetric(c.p2pOverlayNeighbors, prometheus.GaugeValue, float64(overlay.ActiveNeighbours), overlay.Name, "active")
		ch <- prometheus.MustNewConstMetric(c.p2pOverlayNeighbors, prometheus.GaugeValue, float64(overlay.AliveNeighbours), overlay.Name, "alive")
	}

	for _, queue := range snapshot.Queues {
		ch <- prometheus.MustNewConstMetric(c.p2pQueueItems, prometheus.GaugeValue, float64(queue.Items), queue.Name)
		ch <- prometheus.MustNewConstMetric(c.p2pQueueBytes, prometheus.GaugeValue, float64(queue.Bytes), queue.Name)
		ch <- prometheus.MustNewConstMetric(c.p2pQueueMaxItems, prometheus.GaugeValue, float64(queue.MaxItems), queue.Name)
		ch <- prometheus.MustNewConstMetric(c.p2pQueueMaxBytes, prometheus.GaugeValue, float64(queue.MaxBytes), queue.Name)
		ch <- prometheus.MustNewConstMetric(c.p2pQueuePushed, prometheus.CounterValue, float64(queue.Pushed), queue.Name)
		ch <- prometheus.MustNewConstMetric(c.p2pQueueDropped, prometheus.CounterValue, float64(queue.Dropped), queue.Name)
	}

	for _, rebroadcast := range snapshot.Rebroadcast {
		ch <- prometheus.MustNewConstMetric(c.p2pRebroadcastSent, prometheus.CounterValue, float64(rebroadcast.Sent), rebroadcast.Queue)
		ch <- prometheus.MustNewConstMetric(c.p2pRebroadcastDrop, prometheus.CounterValue, float64(rebroadcast.Dropped), rebroadcast.Queue)
	}

	for _, fec := range snapshot.FECReceivers {
		ch <- prometheus.MustNewConstMetric(c.p2pFECActiveStreams, prometheus.GaugeValue, float64(fec.ActiveStreams), fec.Overlay)
		ch <- prometheus.MustNewConstMetric(c.p2pFECActiveBytes, prometheus.GaugeValue, float64(fec.ActiveBytes), fec.Overlay)
		ch <- prometheus.MustNewConstMetric(c.p2pFECDelivered, prometheus.GaugeValue, float64(fec.DeliveredBroadcasts), fec.Overlay)
		ch <- prometheus.MustNewConstMetric(c.p2pFECDropped, prometheus.CounterValue, float64(fec.DroppedTotal), fec.Overlay)
		ch <- prometheus.MustNewConstMetric(c.p2pFECEvicted, prometheus.CounterValue, float64(fec.EvictedTotal), fec.Overlay)
		ch <- prometheus.MustNewConstMetric(c.p2pFECCompleted, prometheus.CounterValue, float64(fec.CompletedTotal), fec.Overlay)
		ch <- prometheus.MustNewConstMetric(c.p2pFECDeliveredHits, prometheus.CounterValue, float64(fec.DeliveredCacheHitsTotal), fec.Overlay)
	}

	for _, broadcast := range snapshot.Broadcasts {
		ch <- prometheus.MustNewConstMetric(c.p2pBroadcasts, prometheus.CounterValue, float64(broadcast.Count), broadcast.Direction, broadcast.Overlay, broadcast.Kind)
	}
	for _, drop := range snapshot.BroadcastDrops {
		ch <- prometheus.MustNewConstMetric(c.p2pBroadcastDrops, prometheus.CounterValue, float64(drop.Count), drop.Overlay, drop.Kind, drop.Reason)
	}
}

func (c *serviceCollector) collectBlockSync(ch chan<- prometheus.Metric, snapshot blocksync.StatusSnapshot) {
	ch <- prometheus.MustNewConstMetric(c.blocksyncQueueItems, prometheus.GaugeValue, float64(snapshot.OutputQueueItems), "output")
	ch <- prometheus.MustNewConstMetric(c.blocksyncQueueCapacity, prometheus.GaugeValue, float64(snapshot.OutputQueueCapacity), "output")
	ch <- prometheus.MustNewConstMetric(c.blocksyncQueueItems, prometheus.GaugeValue, float64(snapshot.ShardDescriptionQueueItems), "shard_description")
	ch <- prometheus.MustNewConstMetric(c.blocksyncQueueCapacity, prometheus.GaugeValue, float64(snapshot.ShardDescriptionQueueCapacity), "shard_description")
	ch <- prometheus.MustNewConstMetric(c.blocksyncQueueDropped, prometheus.CounterValue, float64(snapshot.ShardDescriptionDropped), "shard_description")
	ch <- prometheus.MustNewConstMetric(c.blocksyncChains, prometheus.GaugeValue, float64(snapshot.Chains), "total")
	ch <- prometheus.MustNewConstMetric(c.blocksyncChains, prometheus.GaugeValue, float64(snapshot.BusyChains), "busy")
	ch <- prometheus.MustNewConstMetric(c.blocksyncChains, prometheus.GaugeValue, float64(snapshot.PendingChains), "pending")
}

func latestBasechainShardsByKey(snapshot service.StatusSnapshot) map[storage.ShardKey]*ton.BlockIDExt {
	latest := make(map[storage.ShardKey]*ton.BlockIDExt, len(snapshot.LatestBasechainShards))
	for _, block := range snapshot.LatestBasechainShards {
		blockCopy := block
		latest[storage.ShardKeyFromBlock(block)] = &blockCopy
	}
	if len(latest) == 0 && snapshot.LatestBasechain != nil {
		latest[storage.ShardKeyFromBlock(*snapshot.LatestBasechain)] = snapshot.LatestBasechain
	}
	return latest
}

func shardLabel(block ton.BlockIDExt) string {
	if block.Shard == topShard {
		return "basechain"
	}
	return fmt.Sprintf("%016x", uint64(block.Shard))
}
