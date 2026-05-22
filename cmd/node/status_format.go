package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	service2 "github.com/xssnick/gton/service"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/storage/pebblestore"
	"github.com/xssnick/tonutils-go/ton"
)

func formatStatus(snapshot service2.StatusSnapshot, showPeers bool) string {
	return formatStatusWithNow(snapshot, showPeers, time.Now())
}

func formatDBStatus(status pebblestore.DBStatus) string {
	var b strings.Builder

	fmt.Fprintf(&b, "DB Status\n")
	fmt.Fprintf(&b, "\n")
	fmt.Fprintf(&b, "Meta DB\n")
	if status.Meta == nil {
		fmt.Fprintf(&b, "  unavailable\n")
	} else {
		formatDBCacheStatus(&b, status.Meta.Cache)
		formatDBMetricsHeader(&b, "db")
		formatDBMetricsStatus(&b, "meta", status.Meta.DB)
	}
	fmt.Fprintf(&b, "\n")
	fmt.Fprintf(&b, "Cell DB\n")
	if len(status.CellGenerations) == 0 {
		fmt.Fprintf(&b, "  unavailable\n")
		return b.String()
	}

	for _, generation := range status.CellGenerations {
		role := fallbackString(generation.Role, "open")
		fmt.Fprintf(&b, "  generation %d role=%s", generation.ID, role)
		if validBlockID(generation.Origin) {
			fmt.Fprintf(&b, " origin=%s", formatBlock(&generation.Origin))
		}
		fmt.Fprintf(&b, "\n")
		formatDBCacheStatus(&b, generation.Cache)
		if reason := formatCellGenerationSwitchWait(generation); reason != "" {
			fmt.Fprintf(&b, "    switch_wait %s\n", reason)
		}
		formatDBMetricsHeader(&b, "shard")
		for _, shard := range generation.Shards {
			formatDBMetricsStatus(&b, fmt.Sprintf("%d", shard.Shard), shard)
		}
		formatDBMetricsStatus(&b, "total", generation.Total)
	}

	return b.String()
}

func formatCellGenerationSwitchWait(generation pebblestore.CellDBGenerationStatus) string {
	if generation.Role != "pending" || generation.Total.ReadAmp <= service2.CellGenerationSwitchMaxReadAmp {
		return ""
	}

	return fmt.Sprintf(
		"pending read_amp=%d > %d, waiting for compaction debt=%s comp=%d/%s l0=%d/%d %s",
		generation.Total.ReadAmp,
		service2.CellGenerationSwitchMaxReadAmp,
		formatDBBytes(generation.Total.CompactionDebt),
		generation.Total.CompactionsInProgress,
		formatDBBytesInt(generation.Total.CompactionInProgressSize),
		generation.Total.L0Files,
		generation.Total.L0Sublevels,
		formatDBBytesInt(generation.Total.L0Size),
	)
}

func formatDBCacheStatus(b *strings.Builder, cache pebblestore.CellDBCacheStatus) {
	fmt.Fprintf(
		b,
		"    cache block=%s file=%s file_tables=%d block_hit=%s\n",
		formatDBBytes(uint64(cache.BlockCacheSize)),
		formatDBBytes(uint64(cache.FileCacheSize)),
		cache.FileCacheTableCount,
		formatDBCacheHitRate(cache.BlockCacheHits, cache.BlockCacheMisses),
	)
}

func formatDBMetricsHeader(b *strings.Builder, label string) {
	fmt.Fprintf(b, "    %-5s %9s %9s %7s %5s %9s %9s %9s %10s %10s %8s\n",
		label, "disk", "live", "tables", "amp", "l0 f/s", "l0 size", "debt", "comp", "mem", "fl/ing")
}

func formatDBMetricsStatus(b *strings.Builder, label string, shard pebblestore.CellDBShardStatus) {
	fmt.Fprintf(b, "    %-5s %9s %9s %7d %5d %9s %9s %9s %10s %10s %8s\n",
		label,
		formatDBBytes(shard.DiskSize),
		formatDBBytes(shard.LiveSize),
		shard.LiveTables,
		shard.ReadAmp,
		fmt.Sprintf("%d/%d", shard.L0Files, shard.L0Sublevels),
		formatDBBytesInt(shard.L0Size),
		formatDBBytes(shard.CompactionDebt),
		fmt.Sprintf("%d/%s", shard.CompactionsInProgress, formatDBBytesInt(shard.CompactionInProgressSize)),
		fmt.Sprintf("%s/%d", formatDBBytes(shard.MemTableSize), shard.MemTableCount),
		fmt.Sprintf("%d/%d", shard.Flushes, shard.Ingests),
	)
}

func formatDBCacheHitRate(hits int64, misses int64) string {
	total := hits + misses
	if total <= 0 {
		return "n/a"
	}
	return fmt.Sprintf("%.1f%%", float64(hits)*100/float64(total))
}

func formatDBBytesInt(value int64) string {
	if value <= 0 {
		return "0B"
	}
	return formatDBBytes(uint64(value))
}

func formatDBBytes(value uint64) string {
	units := []string{"B", "KiB", "MiB", "GiB", "TiB", "PiB"}
	size := float64(value)
	unit := 0
	for size >= 1024 && unit+1 < len(units) {
		size /= 1024
		unit++
	}
	if unit == 0 {
		return fmt.Sprintf("%dB", value)
	}
	if size >= 100 {
		return fmt.Sprintf("%.0f%s", size, units[unit])
	}
	return fmt.Sprintf("%.1f%s", size, units[unit])
}

func formatStatusWithNow(snapshot service2.StatusSnapshot, showPeers bool, now time.Time) string {
	var b strings.Builder

	totalNeighbours := 0
	totalAliveNeighbours := 0
	totalKnownPeers := 0
	totalAliveKnownPeers := 0
	for _, overlay := range snapshot.Overlays {
		totalNeighbours += overlay.ActiveNeighbours
		totalAliveNeighbours += overlay.AliveNeighbours
		totalKnownPeers += overlay.KnownPeers
		totalAliveKnownPeers += overlay.AliveKnownPeers
	}

	fmt.Fprintf(&b, "Status\n")
	fmt.Fprintf(&b, "\n")
	fmt.Fprintf(&b, "Node\n")
	fmt.Fprintf(&b, "  %-20s %s\n", "listen", fallbackString(snapshot.ListenAddr, "<client-mode>"))
	fmt.Fprintf(&b, "  %-20s %s\n", "latest masterchain", formatBlock(snapshot.LatestMasterchain))
	fmt.Fprintf(&b, "  %-20s %s\n", "latest basechain", formatBlock(snapshot.LatestBasechain))
	fmt.Fprintf(&b, "  %-20s %d\n", "overlays", len(snapshot.Overlays))
	fmt.Fprintf(&b, "  %-20s %d / %d alive\n", "known peers", totalAliveKnownPeers, totalKnownPeers)
	fmt.Fprintf(&b, "  %-20s %d / %d alive\n", "neighbours", totalAliveNeighbours, totalNeighbours)

	formatChainLagStatus(&b, snapshot, now)

	if showPeers {
		fmt.Fprintf(&b, "\n")
		fmt.Fprintf(&b, "Overlays\n")
		if len(snapshot.Overlays) == 0 {
			fmt.Fprintf(&b, "  none\n")
			return b.String()
		}
		for _, overlay := range snapshot.Overlays {
			fmt.Fprintf(&b, "  %s\n", overlay.Name)
			fmt.Fprintf(&b, "    %-18s %d / %d alive\n", "known peers", overlay.AliveKnownPeers, overlay.KnownPeers)
			fmt.Fprintf(&b, "    %-18s %d / %d alive\n", "neighbours", overlay.AliveNeighbours, overlay.ActiveNeighbours)
			if len(overlay.Neighbours) == 0 {
				fmt.Fprintf(&b, "    no active neighbours\n")
				continue
			}
			fmt.Fprintf(&b, "    %-5s %-12s %6s %8s  %s\n", "alive", "last ok", "fail", "score", "addr")
			for _, peer := range overlay.Neighbours {
				fmt.Fprintf(
					&b,
					"    %-5s %-12s %6d %8.1f  %s\n",
					formatBool(peer.Alive),
					formatSince(peer.LastSuccessAt),
					peer.FailedQueries,
					peer.Unreliability,
					peer.Addr,
				)
			}
		}
	}

	return b.String()
}

func formatChainLagStatus(b *strings.Builder, snapshot service2.StatusSnapshot, now time.Time) {
	fmt.Fprintf(b, "\n")
	fmt.Fprintf(b, "Chain Lag\n")
	formatChainLag(
		b,
		"masterchain",
		snapshot.LatestMasterchain,
		snapshot.LocalMasterchain,
		localMasterchainStatus(snapshot),
		snapshot.LocalMasterchainUtime,
		snapshot.LocalMasterchainTx,
		snapshot.LocalMasterchainHasTx,
		now,
	)
	formatBasechainLagStatus(b, snapshot, now)
	formatRecentTPSStatus(b, snapshot.RecentTPS)
}

func formatBasechainLagStatus(b *strings.Builder, snapshot service2.StatusSnapshot, now time.Time) {
	if len(snapshot.LocalBasechainShards) == 0 {
		formatChainLag(
			b,
			"basechain",
			snapshot.LatestBasechain,
			snapshot.LocalBasechain,
			localBasechainStatus(snapshot),
			snapshot.LocalBasechainUtime,
			snapshot.LocalBasechainTx,
			snapshot.LocalBasechainHasTx,
			now,
		)
		return
	}

	latest := latestBasechainShardsByKey(snapshot)
	for _, shard := range snapshot.LocalBasechainShards {
		var network *ton.BlockIDExt
		if block, ok := latest[storage.ShardKeyFromBlock(shard.Block)]; ok {
			network = &block
		}
		formatChainLag(
			b,
			formatBasechainShardName(shard.Block),
			network,
			&shard.Block,
			localBasechainStatus(snapshot),
			shard.Utime,
			shard.Transactions,
			shard.HasTransactions,
			now,
		)
	}
}

func formatRecentTPSStatus(b *strings.Builder, snapshot service2.StatusTPSSnapshot) {
	if snapshot.WindowMasters == 0 {
		return
	}
	if !snapshot.Complete {
		fmt.Fprintf(b, "  %-12s window_masters=%d tx=unknown duration=unknown tps=unknown\n", "tps", snapshot.WindowMasters)
		return
	}

	fmt.Fprintf(
		b,
		"  %-12s window_masters=%d tx=%d duration=%s tps=%.2f\n",
		"tps",
		snapshot.WindowMasters,
		snapshot.Transactions,
		formatLagSeconds(snapshot.DurationSeconds),
		snapshot.TPS,
	)
}

func latestBasechainShardsByKey(snapshot service2.StatusSnapshot) map[storage.ShardKey]ton.BlockIDExt {
	latest := make(map[storage.ShardKey]ton.BlockIDExt, len(snapshot.LatestBasechainShards))
	for _, block := range snapshot.LatestBasechainShards {
		latest[storage.ShardKeyFromBlock(block)] = block
	}
	if len(latest) == 0 && snapshot.LatestBasechain != nil {
		latest[storage.ShardKeyFromBlock(*snapshot.LatestBasechain)] = *snapshot.LatestBasechain
	}
	return latest
}

func formatBasechainShardName(block ton.BlockIDExt) string {
	if block.Shard == topShard {
		return "basechain"
	}
	return fmt.Sprintf("basechain/%016x", uint64(block.Shard))
}

func formatChainLag(
	b *strings.Builder,
	name string,
	network *ton.BlockIDExt,
	local *ton.BlockIDExt,
	localMissing string,
	localUtime int64,
	localTransactions uint32,
	hasLocalTransactions bool,
	now time.Time,
) {
	if local == nil {
		fmt.Fprintf(b, "  %-12s %s latest=%s\n", name, localMissing, formatBlockSeq(network))
		return
	}

	fmt.Fprintf(
		b,
		"  %-12s local=%s latest=%s lag_seconds=%s block_time=%s tx=%s\n",
		name,
		formatBlockSeq(local),
		formatBlockSeq(network),
		formatLocalLagSeconds(now, localUtime),
		formatBlockUtime(localUtime),
		formatTransactionCount(localTransactions, hasLocalTransactions),
	)
}

func formatTransactionCount(count uint32, known bool) string {
	if !known {
		return "unknown"
	}
	return strconv.FormatUint(uint64(count), 10)
}

func formatLocalLagSeconds(now time.Time, blockUtime int64) string {
	if blockUtime <= 0 {
		return "unknown"
	}
	return formatLagSeconds(now.Unix() - blockUtime)
}

func formatLagSeconds(delta int64) string {
	return fmt.Sprintf("%ds", delta)
}

func formatBlockUtime(blockUtime int64) string {
	if blockUtime <= 0 {
		return "unknown"
	}
	return time.Unix(blockUtime, 0).UTC().Format(time.RFC3339)
}

func localMasterchainStatus(snapshot service2.StatusSnapshot) string {
	if snapshot.LocalStateError != "" {
		return "local state error: " + snapshot.LocalStateError
	}
	return "no local current state"
}

func localBasechainStatus(snapshot service2.StatusSnapshot) string {
	if snapshot.LocalStateError != "" {
		return "local state error: " + snapshot.LocalStateError
	}
	if snapshot.LocalStateLoaded {
		return "no local basechain state"
	}
	return "no local current state"
}

func formatBool(value bool) string {
	if value {
		return "yes"
	}
	return "no"
}

func formatBlock(block *ton.BlockIDExt) string {
	if block == nil {
		return "<none>"
	}
	return fmt.Sprintf("wc=%d shard=%016x seqno=%d", block.Workchain, uint64(block.Shard), block.SeqNo)
}

func formatBlockSeq(block *ton.BlockIDExt) string {
	if block == nil {
		return "<none>"
	}
	return fmt.Sprintf("%d", block.SeqNo)
}

func formatSince(ts time.Time) string {
	if ts.IsZero() {
		return "never"
	}
	return time.Since(ts).Round(time.Second).String() + " ago"
}

func fallbackString(value string, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
