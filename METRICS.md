# GTON Prometheus Metrics

GTON exposes metrics on `/metrics` when metrics are enabled in the node
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
| `gton_liteserver_queries_total` | counter | `method`, `response`, `error_code`, `reason` | Number of handled liteserver queries. `error_code="0"` means the response was not a `ton.LSError`; `ton.LSError` with protocol code `0` is exported as `error_code="unspecified"`. Successful queries use `reason="none"`; unclassified errors use `reason="unspecified"`. |
| `gton_liteserver_inflight_queries` | gauge | none | Number of liteserver queries currently being handled. |
| `gton_liteserver_query_duration_seconds` | histogram | `method`, `response`, `error_code`, `reason` | Total query duration, including `waitMasterchainSeqno` wait time. |
| `gton_liteserver_query_handler_duration_seconds` | histogram | `method`, `response`, `error_code`, `reason` | Query handler duration without `waitMasterchainSeqno` wait time. Use this for normal handler latency SLOs. |
| `gton_liteserver_query_wait_seconds` | histogram | `method`, `response`, `error_code`, `reason` | Time spent waiting for `waitMasterchainSeqno`. This series is emitted only for queries that actually waited. |

Useful examples:

```promql
sum(rate(gton_liteserver_queries_total[5m])) by (method)
sum(rate(gton_liteserver_queries_total{error_code!="0"}[5m])) by (method, error_code, reason)
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
| `gton_sync_gap_blocks` | gauge | `chain`, `shard` | Difference between latest observed network seqno and local seqno, clamped to `0` when local is ahead. |
| `gton_sync_recent_tps` | gauge | none | Average live applied-block TPS over the last 10 seconds, using block `GenUTime`. Omitted while the window is incomplete. |
| `gton_sync_recent_transactions` | gauge | none | Live applied-block transaction count in the 10-second `GenUTime` window. Omitted while the window is incomplete. |
| `gton_sync_recent_tps_complete` | gauge | none | `1` after the 10-second live window has been fully observed, `0` during warm-up or after dropped/invalid block samples. |
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
| `gton_sync_block_prepare_duration_seconds` | histogram | `pipeline`, `chain`, `shard`, `source`, `result`, `catch_up` | Post-download block processing duration, including validation, consensus checks, and state-cell preparation after the block is dequeued; excludes network download, queue wait, and state apply. |
| `gton_sync_block_apply_duration_seconds` | histogram | `pipeline`, `chain`, `result` | Block state transition duration for state-apply pipelines. |
| `gton_sync_master_shards_obtain_duration_seconds` | histogram | `pipeline`, `stage`, `result`, `catch_up` | Time spent obtaining a master block or the shard blocks needed for one master transition; excludes state apply and checkpoint persistence. |
| `gton_sync_checkpoints_total` | counter | `mode`, `result` | Number of current-state checkpoints. |
| `gton_sync_persist_duration_seconds` | histogram | `mode`, `result` | Time spent writing a current-state checkpoint. |
| `gton_sync_persist_queue_seconds` | histogram | `mode`, `result` | Time spent waiting before a current-state checkpoint write can run. |
| `gton_sync_checkpoint_stage_duration_seconds` | histogram | `mode`, `stage`, `result` | Current-state checkpoint duration split by artifact wait, cell prewrite wait, cell flush, artifact fsync, and metadata sync stages. |

Common label values:

- `pipeline`: `blocksync`, `next_block`, `next_block_bootstrap`.
- `chain`: `masterchain`, `shardchain`, or `workchain_<id>`.
- `shard`: `masterchain`, `basechain`, or a 16-digit shard id.
- `source`: `broadcast`, `broadcast_queue`, `broadcast_candidate`, `broadcast_cache`, `broadcast_hint`, `queue`, `peer_catch_up`, `peer_probe`, `next_block`, `indexed`, `next_description`, `stored`, `unknown`.
- `origin`: `broadcast`, `download`, `stored`, `other`, or `unknown`.
- `stage`: `master` or `shards`.
- `result`: `success`, `miss`, `timeout`, `canceled`, `retry`, or `error`.
- `catch_up`: `true` or `false`.
- `mode`: `next_block_async`, `next_block_sync`.
- checkpoint `stage`: `wait_artifacts`, `wait_cell_prewrite`, `flush_prewrite_cells`, `write_cells`, `flush_cells`, `sync_artifacts`, or `metadata_sync`.

Useful examples:

```promql
sum(rate(gton_sync_blocks_total{result="error"}[5m])) by (pipeline, chain)
sum(rate(gton_sync_blocks_total{result!="success"}[5m])) by (pipeline, chain, result)
sum(rate(gton_sync_block_origins_total{pipeline=~"next_block.*",result="success"}[5m])) by (pipeline, chain, origin)
histogram_quantile(0.95, sum(rate(gton_sync_block_download_duration_seconds_bucket{result="success"}[5m])) by (le, pipeline, chain, source))
histogram_quantile(0.95, sum(rate(gton_sync_block_prepare_duration_seconds_bucket{result="success"}[5m])) by (le, pipeline, chain, shard))
histogram_quantile(0.95, sum(rate(gton_sync_block_apply_duration_seconds_bucket{result="success",pipeline!="blocksync"}[5m])) by (le, pipeline, chain))
histogram_quantile(0.95, sum(rate(gton_sync_persist_duration_seconds_bucket[5m])) by (le, mode))
histogram_quantile(0.95, sum(rate(gton_sync_checkpoint_stage_duration_seconds_bucket[5m])) by (le, mode, stage))
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
| `gton_p2p_queue_dropped_total` | counter | `queue` | Number of rejected pushes into a P2P queue. |
| `gton_p2p_broadcasts_total` | counter | `direction`, `overlay`, `kind`, `delivery` | Number of P2P broadcasts received, accepted, or successfully sent through the app-level rebroadcast queue by type and delivery mode. |
| `gton_p2p_broadcast_dropped_total` | counter | `overlay`, `kind`, `reason` | Number of inbound P2P broadcasts dropped before acceptance by type and reason; duplicate payload rebroadcasts rejected by existing seen/dedupe guards use `reason="seen"`. |
| `gton_p2p_broadcast_pipeline_stage_duration_seconds` | histogram | `stage`, `kind`, `delivery`, `result` | Inbound broadcast hot-path stage latency. Stages include `fec_decode`, `classify`, `candidate_decode`, `shard_desc_validate`, `block_broadcast_signature_check`, `hot_cache_notify`, `exact_pop`, `decode_async` (block decode on the bounded decode pool), and `decode_inline` (block decode on the transport receive goroutine). `block_broadcast_signature_check` is the validator-signature pass over a full block broadcast, run on the transport thread *before* the payload decode is enqueued (the reference ordering: `validate_block_broadcast_signatures` gates `obtain_state_for_decompression`), so an unverified payload never buys a decode worker. It is measured separately but is contained in `classify`, which the relay/trust decision keeps on that thread anyway. `decode_inline` covers two cases: payload kinds that cannot be offloaded because their validator signatures are only verifiable after the decode (`tonNode.blockBroadcastCompressed`, i.e. v1 — the only kind whose signature check is sampled after its decode), and payloads the decode pool refused. Compare it against `decode_async` per `kind`: a rising `decode_inline` rate on `tonNode.blockBroadcastCompressedV2` or `tonNode.blockBroadcast` is the pool-undersized signal, while v1 traffic is expected there. |
| `gton_p2p_rebroadcast_sent_total` | counter | `queue` | Number of successful app-level P2P rebroadcast queue sends. |
| `gton_p2p_rebroadcast_dropped_total` | counter | `queue` | Number of app-level P2P rebroadcast queue messages dropped before a successful send. |
| `gton_p2p_broadcast_relay_sent_total` | counter | `overlay`, `delivery` | Number of successful overlay-level broadcast relay sends. `delivery="fec"` counts FEC part sends; `delivery="simple"` counts simple broadcast sends. |
| `gton_p2p_broadcast_relay_failed_total` | counter | `overlay`, `delivery` | Number of failed overlay-level broadcast relay sends. `delivery="fec"` counts FEC part send failures; `delivery="simple"` counts simple broadcast send failures. |
| `gton_p2p_plumtree_fec_parts_received_total` | counter | `overlay`, `source` | Number of verified Plumtree FEC parts accepted into the decoder. `source` is `direct` for eager push delivery or `recovery` for a successful repair response. |
| `gton_p2p_plumtree_messages_received_total` | counter | `overlay`, `type` | Number of parsed inbound Plumtree protocol messages. |
| `gton_blocksync_queue_items` | gauge | `queue` | Current block sync queue length. |
| `gton_blocksync_queue_capacity` | gauge | `queue` | Block sync queue capacity. |
| `gton_blocksync_queue_dropped_total` | counter | `queue` | Number of dropped block sync queue items. |
| `gton_blocksync_chains` | gauge | `state` | Number of tracked block sync chains by state. |

Common `queue` values:

- P2P queues: `broadcast`, `rebroadcast`, `local_rebroadcast`.
- Blocksync queues: `output`, `shard_description`.

Common `direction` values for `gton_p2p_broadcasts_total` are `received_roster`,
`received_unlisted`, `accepted`, and `queue_rebroadcasted`. The two `received_*`
series are counted after the overlay signature/FEC checks and distinguish the
immediate transport peer from the broadcast signer. An unlisted peer is kept as
an ingress transport but is not promoted into the overlay routing roster.
Overlay-level relay sends are exported separately via
`gton_p2p_broadcast_relay_sent_total`.

Common `delivery` values are `simple`, `fec`, `two_step`, and `plumtree`.
Inbound QUIC Plumtree broadcasts use `delivery="plumtree"`. For
`direction="queue_rebroadcasted"`, the label describes the actual outgoing
broadcast mode.

Common `type` values for `gton_p2p_plumtree_messages_received_total` are
`simple`, `fec`, `ihave`, `prune`, `useful`, `stats_push`, and `repair_query`.
Direct messages are counted after their TL framing is parsed, before semantic
acceptance or deduplication. Repair queries are counted after the QUIC query
router has parsed them.

Common `reason` values for `gton_p2p_broadcast_dropped_total` include `seen`,
`invalid_payload`, `decode_failed`, `signature_parse_failed`, and
`signature_check_failed`. Block broadcasts at or below the highest locally
applied seqno of their chain use `already_applied` — the masterchain seqno for
masterchain blocks, and the applied top block of that exact shard for shard
blocks (a shard whose prefix has just changed through a split or a merge is not
gated until the next committed masterchain state names it).

`gton_p2p_broadcast_pipeline_stage_duration_seconds` intentionally has no block
id, root hash, file hash, or peer label. `stage="fec_decode"` is measured inside
the overlay FEC/two-step decoder before the gton broadcast classifier runs.

Useful examples:

```promql
sum(rate(gton_p2p_queue_dropped_total[5m])) by (queue)
gton_p2p_queue_items / gton_p2p_queue_max_items
sum(gton_p2p_overlay_neighbours{state="alive"}) by (overlay)
sum(rate(gton_p2p_plumtree_fec_parts_received_total[5m])) by (overlay, source)
sum(rate(gton_p2p_plumtree_messages_received_total[5m])) by (overlay, type)
histogram_quantile(0.95, sum(rate(gton_p2p_broadcast_pipeline_stage_duration_seconds_bucket[5m])) by (le, stage, delivery, kind))
```

## Storage

Storage metrics are collected from Pebble cell DB status. They are useful for
explaining sync lag caused by write stalls, compaction debt, cache misses, or L0
growth.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `gton_storage_db_status_available` | gauge | none | `1` when DB status was collected successfully, `0` when collection failed. |
| `gton_storage_archive_package_bytes` | gauge | none | Total bytes used by archive `.pack` files under the archive package directory. |
| `gton_storage_persistent_state_bytes` | gauge | none | Total bytes used by recognized persistent state files under the persistent state directory. |
| `gton_storage_cell_db_generation` | gauge | `generation` | Numeric cell DB generation id for the stable generation label. |
| `gton_storage_cell_db_cache_bytes` | gauge | `generation`, `cache` | Cell DB cache size. `cache` is `block` or `file`. |
| `gton_storage_cell_db_cache_requests_total` | counter | `generation`, `result` | Block cache requests. `result` is `hit` or `miss`. |
| `gton_storage_cell_db_disk_bytes` | gauge | `generation`, `shard` | Disk space used by cell DB tables. |
| `gton_storage_cell_db_live_bytes` | gauge | `generation`, `shard` | Live table bytes. |
| `gton_storage_cell_db_read_amp` | gauge | `generation`, `shard` | Read amplification estimate. |
| `gton_storage_cell_db_l0_files` | gauge | `generation`, `shard` | Number of L0 files. |
| `gton_storage_cell_db_l0_sublevels` | gauge | `generation`, `shard` | Number of L0 sublevels. |
| `gton_storage_cell_db_compaction_debt_bytes` | gauge | `generation`, `shard` | Estimated compaction debt. |
| `gton_storage_cell_db_compactions_in_progress` | gauge | `generation`, `shard` | Number of compactions currently running. |
| `gton_storage_cell_db_compaction_in_progress_bytes` | gauge | `generation`, `shard` | Bytes currently being compacted. |
| `gton_storage_cell_db_memtable_bytes` | gauge | `generation`, `shard` | Memtable bytes. |
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

GTON also registers the standard Prometheus Go and process collectors in
the same registry. Expect metrics such as:

- `go_goroutines`
- `go_memstats_*`
- `go_gc_duration_seconds`
- `process_cpu_seconds_total`
- `process_resident_memory_bytes`
- `process_open_fds`

These metrics describe runtime and process health rather than TON-specific
behavior.
