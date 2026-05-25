# Flexserver Prometheus Metrics

Flexserver exposes metrics on `/metrics` when metrics are enabled in the node
configuration:

```json
{
  "metrics": {
    "enabled": true,
    "listen_addr": "127.0.0.1:9090",
    "namespace": "gton"
  }
}
```

All project-specific metrics use the configured `metrics.namespace` prefix. The
default namespace is `gton`, so metric names use the `gton_` prefix unless
configured otherwise. Histograms expose the standard Prometheus `_bucket`,
`_sum`, and `_count` series. Counters expose a `_total` suffix.

## Liteserver

These metrics describe inbound liteserver query load, latency, and result mix.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `gton_liteserver_queries_total` | counter | `method`, `response`, `error_code` | Number of handled liteserver queries. `error_code="0"` means the response was not a `ton.LSError`. |
| `gton_liteserver_inflight_queries` | gauge | none | Number of liteserver queries currently being handled. |
| `gton_liteserver_query_duration_seconds` | histogram | `method`, `response`, `error_code` | Total query duration, including `waitMasterchainSeqno` wait time. |
| `gton_liteserver_query_handler_duration_seconds` | histogram | `method`, `response`, `error_code` | Query handler duration without `waitMasterchainSeqno` wait time. Use this for normal handler latency SLOs. |
| `gton_liteserver_query_wait_seconds` | histogram | `method`, `response`, `error_code` | Time spent waiting for `waitMasterchainSeqno`. This series is emitted only for queries that actually waited. |

Useful examples:

```promql
sum(rate(gton_liteserver_queries_total[5m])) by (method)
sum(rate(gton_liteserver_queries_total{error_code!="0"}[5m])) by (method, error_code)
histogram_quantile(0.95, sum(rate(gton_liteserver_query_handler_duration_seconds_bucket[5m])) by (le, method))
```

## Sync Freshness

These gauges are collected from the current service status at scrape time, so
lag keeps moving forward even if no new block has been published.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `gton_sync_lag_seconds` | gauge | `chain`, `shard` | Current wall-clock lag from the local block generation time. |
| `gton_sync_block_utime_seconds` | gauge | `chain`, `shard` | Unix timestamp of the local block generation time. |
| `gton_sync_local_seqno` | gauge | `chain`, `shard` | Local synchronized block seqno. |
| `gton_sync_network_seqno` | gauge | `chain`, `shard` | Latest observed network block seqno. |
| `gton_sync_gap_blocks` | gauge | `chain`, `shard` | Difference between latest observed network seqno and local seqno. |
| `gton_sync_recent_tps` | gauge | none | Recent local TPS over the status window. |
| `gton_sync_recent_transactions` | gauge | none | Transaction count in the recent TPS window. |
| `gton_sync_recent_tps_complete` | gauge | none | `1` when the recent TPS window had all required block data, `0` otherwise. |
| `gton_service_background_task` | gauge | `task` | Current exclusive background service task. Exactly one task label is exported with value `1`. |

Label notes:

- `chain` is usually `masterchain` or `shardchain`.
- `shard="masterchain"` is the masterchain head.
- `shard="basechain"` is the unsplit basechain shard.
- Split basechain shards use their 16-digit shard id as `shard`.
- `task` is one of `idle`, `serializing state`, `migrating cell DB`, `pruning persistent states`, or `pruning archives`.

Useful examples:

```promql
max(gton_sync_lag_seconds) by (chain)
max(gton_sync_gap_blocks) by (chain)
max(gton_service_background_task) by (task)
```

## Sync Pipeline

These metrics describe block download, block application, and current-state
checkpoint persistence.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `gton_sync_blocks_total` | counter | `pipeline`, `chain`, `source`, `result`, `catch_up` | Number of blocks processed by sync pipelines. |
| `gton_sync_block_origins_total` | counter | `pipeline`, `chain`, `origin`, `result`, `catch_up` | Number of synchronized blocks grouped by origin: `broadcast`, `download`, `stored`, or `other`. |
| `gton_sync_block_download_duration_seconds` | histogram | `pipeline`, `chain`, `source`, `result`, `catch_up` | Block download duration. |
| `gton_sync_block_apply_duration_seconds` | histogram | `pipeline`, `chain`, `result` | Block apply or block processing duration. |
| `gton_sync_checkpoints_total` | counter | `mode`, `result` | Number of current-state checkpoints. |
| `gton_sync_persist_duration_seconds` | histogram | `mode`, `result` | Time spent writing a current-state checkpoint. |
| `gton_sync_persist_queue_seconds` | histogram | `mode`, `result` | Time spent waiting before a current-state checkpoint write can run. |

Common label values:

- `pipeline`: `blocksync`, `next_block`, `next_block_bootstrap`.
- `chain`: `masterchain`, `shardchain`, or `workchain_<id>`.
- `source`: `broadcast`, `broadcast_queue`, `broadcast_candidate`, `broadcast_cache`, `peer_catch_up`, `peer_probe`, `next_block`, `indexed`, `next_description`, `stored`, `unknown`.
- `origin`: `broadcast`, `download`, `stored`, `other`, or `unknown`.
- `result`: `success`, `miss`, `timeout`, `canceled`, `retry`, or `error`.
- `catch_up`: `true` or `false`.
- `mode`: `next_block_async`, `next_block_sync`.

Useful examples:

```promql
sum(rate(gton_sync_blocks_total{result="error"}[5m])) by (pipeline, chain)
sum(rate(gton_sync_blocks_total{result!="success"}[5m])) by (pipeline, chain, result)
sum(rate(gton_sync_block_origins_total{pipeline=~"next_block.*",result="success"}[5m])) by (pipeline, chain, origin)
histogram_quantile(0.95, sum(rate(gton_sync_block_download_duration_seconds_bucket[5m])) by (le, pipeline, source))
histogram_quantile(0.95, sum(rate(gton_sync_persist_duration_seconds_bucket[5m])) by (le, mode))
```

## P2P And Blocksync Load

These metrics show peer availability and backpressure in the broadcast and block
sync queues.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `gton_p2p_overlay_peers` | gauge | `overlay`, `state` | Known overlay peer count. `state` is `known` or `alive`. |
| `gton_p2p_overlay_neighbours` | gauge | `overlay`, `state` | Active neighbour count. `state` is `active` or `alive`. |
| `gton_p2p_queue_items` | gauge | `queue` | Current number of queued P2P items. |
| `gton_p2p_queue_bytes` | gauge | `queue` | Estimated bytes held by a P2P queue. |
| `gton_p2p_queue_max_items` | gauge | `queue` | Configured item limit for a P2P queue. |
| `gton_p2p_queue_max_bytes` | gauge | `queue` | Configured byte limit for a P2P queue. |
| `gton_p2p_queue_pushed_total` | counter | `queue` | Number of accepted pushes into a P2P queue. |
| `gton_p2p_queue_dropped_total` | counter | `queue` | Number of rejected pushes into a P2P queue. |
| `gton_p2p_broadcasts_total` | counter | `direction`, `overlay`, `kind` | Number of P2P broadcasts accepted or successfully rebroadcasted by type. |
| `gton_p2p_rebroadcast_sent_total` | counter | `queue` | Number of successful P2P rebroadcast sends. |
| `gton_p2p_rebroadcast_dropped_total` | counter | `queue` | Number of P2P rebroadcast messages dropped before a successful send. |
| `gton_blocksync_queue_items` | gauge | `queue` | Current block sync queue length. |
| `gton_blocksync_queue_capacity` | gauge | `queue` | Block sync queue capacity. |
| `gton_blocksync_queue_dropped_total` | counter | `queue` | Number of dropped block sync queue items. |
| `gton_blocksync_chains` | gauge | `state` | Number of tracked block sync chains by state. |

Common `queue` values:

- P2P queues: `broadcast`, `rebroadcast`, `local_rebroadcast`.
- Blocksync queues: `output`, `shard_description`.

Common `direction` values for `gton_p2p_broadcasts_total` are `accepted` and
`rebroadcasted`.

Useful examples:

```promql
sum(rate(gton_p2p_queue_dropped_total[5m])) by (queue)
gton_p2p_queue_items / gton_p2p_queue_max_items
sum(gton_p2p_overlay_neighbours{state="alive"}) by (overlay)
```

## Storage

Storage metrics are collected from Pebble cell DB status. They are useful for
explaining sync lag caused by write stalls, compaction debt, cache misses, or L0
growth.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `gton_storage_db_status_available` | gauge | none | `1` when DB status was collected successfully, `0` when collection failed. |
| `gton_storage_archive_packages` | gauge | none | Number of archive `.pack` files under the archive package directory. |
| `gton_storage_archive_package_bytes` | gauge | none | Total regular-file bytes under the archive package directory. |
| `gton_storage_persistent_state_masters` | gauge | none | Number of distinct masterchain seqnos represented by persistent state files. |
| `gton_storage_persistent_state_bytes` | gauge | none | Total regular-file bytes under the persistent state directory. |
| `gton_storage_cell_db_generation` | gauge | `generation` | Numeric cell DB generation id for the stable generation label. |
| `gton_storage_cell_db_cache_bytes` | gauge | `generation`, `cache` | Cell DB cache size. `cache` is `block` or `file`. |
| `gton_storage_cell_db_cache_requests_total` | counter | `generation`, `result` | Block cache requests. `result` is `hit` or `miss`. |
| `gton_storage_cell_db_file_cache_tables` | gauge | `generation` | Number of file cache tables. |
| `gton_storage_cell_db_disk_bytes` | gauge | `generation`, `shard` | Disk space used by cell DB tables. |
| `gton_storage_cell_db_live_bytes` | gauge | `generation`, `shard` | Live table bytes. |
| `gton_storage_cell_db_live_tables` | gauge | `generation`, `shard` | Live table count. |
| `gton_storage_cell_db_read_amp` | gauge | `generation`, `shard` | Read amplification estimate. |
| `gton_storage_cell_db_l0_files` | gauge | `generation`, `shard` | Number of L0 files. |
| `gton_storage_cell_db_l0_sublevels` | gauge | `generation`, `shard` | Number of L0 sublevels. |
| `gton_storage_cell_db_l0_bytes` | gauge | `generation`, `shard` | Bytes in L0 tables. |
| `gton_storage_cell_db_compaction_debt_bytes` | gauge | `generation`, `shard` | Estimated compaction debt. |
| `gton_storage_cell_db_compactions_in_progress` | gauge | `generation`, `shard` | Number of compactions currently running. |
| `gton_storage_cell_db_compaction_in_progress_bytes` | gauge | `generation`, `shard` | Bytes currently being compacted. |
| `gton_storage_cell_db_memtable_bytes` | gauge | `generation`, `shard` | Memtable bytes. |
| `gton_storage_cell_db_memtable_count` | gauge | `generation`, `shard` | Memtable count. |
| `gton_storage_cell_db_table_iters` | gauge | `generation`, `shard` | Open table iterator count. |
| `gton_storage_cell_db_flushes_total` | counter | `generation`, `shard` | Pebble flush count. |
| `gton_storage_cell_db_ingests_total` | counter | `generation`, `shard` | Pebble ingest count. |
| `gton_storage_cell_db_read_cells_total` | counter | `generation`, `shard` | Cell records successfully read from the cell DB. |
| `gton_storage_cell_db_written_cells_total` | counter | `generation`, `shard` | Cell records successfully written to the cell DB. |

Label notes:

- `generation` is a stable cell DB generation role: `active` or `pending`.
- `shard` is a cell DB shard number, plus a synthetic `total` shard.

Useful examples:

```promql
max(gton_storage_cell_db_compaction_debt_bytes{shard="total"}) by (generation)
max(gton_storage_cell_db_generation) by (generation)
max(gton_storage_cell_db_l0_files{shard="total"}) by (generation)
sum(rate(gton_storage_cell_db_cache_requests_total[5m])) by (result)
sum(rate(gton_storage_cell_db_read_cells_total[1m])) by (generation, shard)
sum(rate(gton_storage_cell_db_written_cells_total[1m])) by (generation, shard)
```

## Go And Process Metrics

Flexserver also registers the standard Prometheus Go and process collectors in
the same registry. Expect metrics such as:

- `go_goroutines`
- `go_memstats_*`
- `go_gc_duration_seconds`
- `process_cpu_seconds_total`
- `process_resident_memory_bytes`
- `process_open_fds`

These metrics describe runtime and process health rather than TON-specific
behavior.
