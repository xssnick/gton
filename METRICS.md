# Flexserver Prometheus Metrics

Flexserver exposes metrics on `/metrics` when metrics are enabled in the node
configuration:

```json
{
  "metrics": {
    "enabled": true,
    "listen_addr": "127.0.0.1:9090"
  }
}
```

All project-specific metrics use the `flexserver_` prefix. Histograms expose the
standard Prometheus `_bucket`, `_sum`, and `_count` series. Counters expose a
`_total` suffix.

## Liteserver

These metrics describe inbound liteserver query load, latency, and result mix.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `flexserver_liteserver_queries_total` | counter | `method`, `response`, `error_code` | Number of handled liteserver queries. `error_code="0"` means the response was not a `ton.LSError`. |
| `flexserver_liteserver_inflight_queries` | gauge | none | Number of liteserver queries currently being handled. |
| `flexserver_liteserver_query_duration_seconds` | histogram | `method`, `response`, `error_code` | Total query duration, including `waitMasterchainSeqno` wait time. |
| `flexserver_liteserver_query_handler_duration_seconds` | histogram | `method`, `response`, `error_code` | Query handler duration without `waitMasterchainSeqno` wait time. Use this for normal handler latency SLOs. |
| `flexserver_liteserver_query_wait_seconds` | histogram | `method`, `response`, `error_code` | Time spent waiting for `waitMasterchainSeqno`. This series is emitted only for queries that actually waited. |

Useful examples:

```promql
sum(rate(flexserver_liteserver_queries_total[5m])) by (method)
sum(rate(flexserver_liteserver_queries_total{error_code!="0"}[5m])) by (method, error_code)
histogram_quantile(0.95, sum(rate(flexserver_liteserver_query_handler_duration_seconds_bucket[5m])) by (le, method))
```

## Sync Freshness

These gauges are collected from the current service status at scrape time, so
lag keeps moving forward even if no new block has been published.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `flexserver_sync_lag_seconds` | gauge | `chain`, `shard` | Current wall-clock lag from the local block generation time. |
| `flexserver_sync_block_utime_seconds` | gauge | `chain`, `shard` | Unix timestamp of the local block generation time. |
| `flexserver_sync_local_seqno` | gauge | `chain`, `shard` | Local synchronized block seqno. |
| `flexserver_sync_network_seqno` | gauge | `chain`, `shard` | Latest observed network block seqno. |
| `flexserver_sync_gap_blocks` | gauge | `chain`, `shard` | Difference between latest observed network seqno and local seqno. |
| `flexserver_sync_last_publish_timestamp_seconds` | gauge | none | Unix timestamp of the last published current state. |
| `flexserver_sync_recent_tps` | gauge | none | Recent local TPS over the status window. |
| `flexserver_sync_recent_transactions` | gauge | none | Transaction count in the recent TPS window. |
| `flexserver_sync_recent_tps_complete` | gauge | none | `1` when the recent TPS window had all required block data, `0` otherwise. |

Label notes:

- `chain` is usually `masterchain` or `shardchain`.
- `shard="masterchain"` is the masterchain head.
- `shard="basechain"` is the unsplit basechain shard.
- Split basechain shards use their 16-digit shard id as `shard`.

Useful examples:

```promql
max(flexserver_sync_lag_seconds) by (chain)
max(flexserver_sync_gap_blocks) by (chain)
time() - flexserver_sync_last_publish_timestamp_seconds
```

## Sync Pipeline

These metrics describe block download, block application, and current-state
checkpoint persistence.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `flexserver_sync_blocks_total` | counter | `pipeline`, `chain`, `source`, `result`, `catch_up` | Number of blocks processed by sync pipelines. |
| `flexserver_sync_block_download_duration_seconds` | histogram | `pipeline`, `chain`, `source`, `result`, `catch_up` | Block download duration. |
| `flexserver_sync_block_apply_duration_seconds` | histogram | `pipeline`, `chain`, `result` | Block apply or block processing duration. |
| `flexserver_sync_checkpoints_total` | counter | `mode`, `result` | Number of current-state checkpoints. |
| `flexserver_sync_persist_duration_seconds` | histogram | `mode`, `result` | Time spent writing a current-state checkpoint. |
| `flexserver_sync_persist_queue_seconds` | histogram | `mode`, `result` | Time spent waiting before a current-state checkpoint write can run. |

Common label values:

- `pipeline`: `blocksync`, `next_block`, `next_block_bootstrap`.
- `chain`: `masterchain`, `shardchain`, or `workchain_<id>`.
- `source`: `broadcast`, `catch_up`, `queue`, `probe`, `next_block`, `indexed`, `next_description`, `unknown`.
- `result`: `success` or `error`.
- `catch_up`: `true` or `false`.
- `mode`: `next_block_async`, `next_block_sync`.

Useful examples:

```promql
sum(rate(flexserver_sync_blocks_total{result="error"}[5m])) by (pipeline, chain)
histogram_quantile(0.95, sum(rate(flexserver_sync_block_download_duration_seconds_bucket[5m])) by (le, pipeline, source))
histogram_quantile(0.95, sum(rate(flexserver_sync_persist_duration_seconds_bucket[5m])) by (le, mode))
```

## P2P And Blocksync Load

These metrics show peer availability and backpressure in the broadcast and block
sync queues.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `flexserver_p2p_overlay_peers` | gauge | `overlay`, `state` | Known overlay peer count. `state` is `known` or `alive`. |
| `flexserver_p2p_overlay_neighbours` | gauge | `overlay`, `state` | Active neighbour count. `state` is `active` or `alive`. |
| `flexserver_p2p_queue_items` | gauge | `queue` | Current number of queued P2P items. |
| `flexserver_p2p_queue_bytes` | gauge | `queue` | Estimated bytes held by a P2P queue. |
| `flexserver_p2p_queue_max_items` | gauge | `queue` | Configured item limit for a P2P queue. |
| `flexserver_p2p_queue_max_bytes` | gauge | `queue` | Configured byte limit for a P2P queue. |
| `flexserver_p2p_queue_pushed_total` | counter | `queue` | Number of accepted pushes into a P2P queue. |
| `flexserver_p2p_queue_dropped_total` | counter | `queue` | Number of rejected pushes into a P2P queue. |
| `flexserver_blocksync_queue_items` | gauge | `queue` | Current block sync queue length. |
| `flexserver_blocksync_queue_capacity` | gauge | `queue` | Block sync queue capacity. |
| `flexserver_blocksync_queue_dropped_total` | counter | `queue` | Number of dropped block sync queue items. |
| `flexserver_blocksync_chains` | gauge | `state` | Number of tracked block sync chains by state. |

Common `queue` values:

- P2P queues: `broadcast`, `rebroadcast`, `local_rebroadcast`.
- Blocksync queues: `output`, `shard_description`.

Useful examples:

```promql
sum(rate(flexserver_p2p_queue_dropped_total[5m])) by (queue)
flexserver_p2p_queue_items / flexserver_p2p_queue_max_items
sum(flexserver_p2p_overlay_neighbours{state="alive"}) by (overlay)
```

## Storage

Storage metrics are collected from Pebble cell DB status. They are useful for
explaining sync lag caused by write stalls, compaction debt, cache misses, or L0
growth.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `flexserver_storage_db_status_available` | gauge | none | `1` when DB status was collected successfully, `0` when collection failed. |
| `flexserver_storage_cell_db_cache_bytes` | gauge | `generation`, `role`, `cache` | Cell DB cache size. `cache` is `block` or `file`. |
| `flexserver_storage_cell_db_cache_requests_total` | counter | `generation`, `role`, `result` | Block cache requests. `result` is `hit` or `miss`. |
| `flexserver_storage_cell_db_file_cache_tables` | gauge | `generation`, `role` | Number of file cache tables. |
| `flexserver_storage_cell_db_disk_bytes` | gauge | `generation`, `role`, `shard` | Disk space used by cell DB tables. |
| `flexserver_storage_cell_db_live_bytes` | gauge | `generation`, `role`, `shard` | Live table bytes. |
| `flexserver_storage_cell_db_live_tables` | gauge | `generation`, `role`, `shard` | Live table count. |
| `flexserver_storage_cell_db_read_amp` | gauge | `generation`, `role`, `shard` | Read amplification estimate. |
| `flexserver_storage_cell_db_l0_files` | gauge | `generation`, `role`, `shard` | Number of L0 files. |
| `flexserver_storage_cell_db_l0_sublevels` | gauge | `generation`, `role`, `shard` | Number of L0 sublevels. |
| `flexserver_storage_cell_db_l0_bytes` | gauge | `generation`, `role`, `shard` | Bytes in L0 tables. |
| `flexserver_storage_cell_db_compaction_debt_bytes` | gauge | `generation`, `role`, `shard` | Estimated compaction debt. |
| `flexserver_storage_cell_db_compactions_in_progress` | gauge | `generation`, `role`, `shard` | Number of compactions currently running. |
| `flexserver_storage_cell_db_compaction_in_progress_bytes` | gauge | `generation`, `role`, `shard` | Bytes currently being compacted. |
| `flexserver_storage_cell_db_memtable_bytes` | gauge | `generation`, `role`, `shard` | Memtable bytes. |
| `flexserver_storage_cell_db_memtable_count` | gauge | `generation`, `role`, `shard` | Memtable count. |
| `flexserver_storage_cell_db_table_iters` | gauge | `generation`, `role`, `shard` | Open table iterator count. |
| `flexserver_storage_cell_db_flushes_total` | counter | `generation`, `role`, `shard` | Pebble flush count. |
| `flexserver_storage_cell_db_ingests_total` | counter | `generation`, `role`, `shard` | Pebble ingest count. |

Label notes:

- `generation` is the cell DB generation id.
- `role` is usually `active`, `candidate`, or `open`.
- `shard` is a cell DB shard number, plus a synthetic `total` shard.

Useful examples:

```promql
max(flexserver_storage_cell_db_compaction_debt_bytes{shard="total"}) by (generation, role)
max(flexserver_storage_cell_db_l0_files{shard="total"}) by (generation, role)
sum(rate(flexserver_storage_cell_db_cache_requests_total[5m])) by (result)
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
