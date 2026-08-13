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

## Validator

Validator metrics are registered by the validator extension stack. All label
sets are bounded and pre-bound during extension construction; candidate IDs,
block hashes, session IDs, peers, shard IDs, and error strings are never metric
labels. When metrics are disabled, the new stage clocks are skipped entirely.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `gton_validator_validations_total` | counter | `chain`, `kind`, `origin`, `result` | Finished candidate validations. |
| `gton_validator_validations_inflight` | gauge | `chain`, `kind`, `origin` | Candidate validations currently running. |
| `gton_validator_validation_decision_duration_seconds` | histogram | `chain`, `kind`, `origin`, `result` | Time until the semantic validation decision, before `ValidAfter` waiting. |
| `gton_validator_validation_ready_duration_seconds` | histogram | `chain`, `kind`, `origin`, `result` | End-to-end time until the validation continuation is ready to vote. |
| `gton_validator_validation_stage_duration_seconds` | histogram | `chain`, `stage` | Runtime and deterministic-core stage duration. `backend` contains the nested `core_*` stages. |
| `gton_validator_validation_attempt_duration_seconds` | histogram | `chain`, `result` | One backend validation attempt. |
| `gton_validator_validation_retries_total` | counter | `chain`, `reason` | Retries caused by unavailable local state or an attempt deadline. |
| `gton_validator_candidate_size_bytes` | histogram | `chain`, `part` | Candidate block and collated-data payload sizes entering validation. |

Common values:

- `chain`: `masterchain`, `shardchain`.
- `kind`: `block`, `empty`.
- `origin`: `local`, `remote`; local means the candidate leader is this
  session's validator identity.
- validation `result`: `success`, `rejected`, `canceled`, `deadline`, `error`.
- attempt `result`: `success`, `rejected`, `not_ready`, `canceled`, `deadline`,
  `error`.
- `stage`: `load_candidate`, `resolve_parent`, `min_block_interval_wait`,
  `backend`, `retry_wait`, `valid_after_wait`, `core_restore_state`,
  `core_master_view`, `core_chain_inputs`, `core_decode`, `core_transition`.
- retry `reason`: `state_not_ready`, `attempt_deadline`.

Useful examples:

```promql
sum(rate(gton_validator_validations_total[5m])) by (chain, result)
histogram_quantile(0.95, sum(rate(gton_validator_validation_decision_duration_seconds_bucket{result="success"}[5m])) by (le, chain))
histogram_quantile(0.95, sum(rate(gton_validator_validation_ready_duration_seconds_bucket{result="success"}[5m])) by (le, chain))
histogram_quantile(0.95, sum(rate(gton_validator_validation_stage_duration_seconds_bucket[5m])) by (le, chain, stage))
sum(rate(gton_validator_validation_retries_total[5m])) by (chain, reason)
```

The C++ reference records whole-query `validateblock` and `collate` wall time
through `PerfWarningTimer`/manager perf stats, plus generic per-operation CPU
ticks. These gton histograms retain the same whole-operation boundaries and add
the stage split needed to tell state waiting, deterministic verification, and
`ValidAfter` apart.

## Collator

Collator metrics are registered once by the shared validator/collator extension
stack. `mode="validator"` is the integrated collator; `mode="standalone"` is
the standalone collator. The two roles can coexist without collector-name
collisions or dynamic label binding in candidate processing.

The `origin` label separates the two things this node does with a candidate.
`origin="collation"` covers blocks it produced, counted from the collator's own
Stats. `origin="validation"` covers blocks another validator produced and this
node accepted; those counts come out of the semantic replay, which already walks
the descriptors and transactions to verify them, so reporting them costs no
extra pass over the block. Rejected candidates are not reported — a block that
failed verification has no shape worth charting.

The two are defined to line up: `kind="external"` counts `msg_import_ext`
descriptors, which collation writes once per included external, and
`kind="internal"` counts `msg_import_fin` plus `msg_import_tr`, the pair it
writes for an internal taken off a queue. Metrics without an `origin` label are
collation-only, because they measure work only a producer performs.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `gton_collator_builds_total` | counter | `mode`, `chain`, `result` | Candidate build attempts. |
| `gton_collator_builds_inflight` | gauge | `mode`, `chain` | Builds currently acquiring inputs or collating. |
| `gton_collator_build_duration_seconds` | histogram | `mode`, `chain`, `result` | Whole build future: acquisition, core collation, and serialization. |
| `gton_collator_stage_duration_seconds` | histogram | `mode`, `chain`, `stage` | Non-overlapping acquisition and producer stages. |
| `gton_collator_candidates_total` | counter | `mode`, `chain`, `kind` | Newly signed block or empty candidates. |
| `gton_collator_candidate_production_duration_seconds` | histogram | `mode`, `chain`, `kind`, `result` | Candidate production through persistence, state commit, schedule wait, and emission; recovered candidates are a separate kind. |
| `gton_collator_candidate_size_bytes` | histogram | `mode`, `chain`, `origin`, `part` | Block and collated-data sizes. `origin="collation"` is a block this node built; `origin="validation"` is one it accepted from another validator. |
| `gton_collator_candidate_transactions` | histogram | `mode`, `chain`, `origin` | Transactions in a non-empty candidate, by `origin`. |
| `gton_collator_candidate_gas_used` | histogram | `mode`, `chain` | Gas used by a produced block. Collation only: gas is a producer's own metering and is not recoverable from a block. |
| `gton_collator_candidate_messages` | histogram | `mode`, `chain`, `origin`, `kind` | External and imported internal messages in a candidate, by `origin`. |
| `gton_collator_candidate_out_queue_messages` | histogram | `mode`, `chain` | Resulting outbound queue size. |
| `gton_collator_external_wait_duration_seconds` | histogram | `mode`, `chain` | Time waiting for ready external-message batches. |
| `gton_collator_external_batches` | histogram | `mode`, `chain` | Ready external batches consumed. |
| `gton_collator_external_stop_total` | counter | `mode`, `chain`, `reason` | External-message phase stop reason. |
| `gton_collator_load_class_total` | counter | `mode`, `chain`, `class` | Produced candidates by load class. |
| `gton_collator_overload_total` | counter | `mode`, `chain`, `reason` | Produced candidates by overload cause. |
| `gton_collator_deadline_events_total` | counter | `mode`, `chain`, `deadline`, `action` | Soft/hard producer deadline decisions. |
| `gton_collator_retries_total` | counter | `mode`, `chain`, `reason` | Retried producer windows. |
| `gton_collator_schedule_lateness_seconds` | histogram | `mode`, `chain`, `event` | Positive lateness at build-start or broadcast slot boundaries. |
| `gton_collator_windows_inflight` | gauge | `mode`, `chain` | Producer windows currently running. |
| `gton_collator_windows_total` | counter | `mode`, `chain`, `result` | Finished producer windows. |
| `gton_collator_window_duration_seconds` | histogram | `mode`, `chain`, `result` | Whole producer-window duration, including retries. |

Common values:

- `result`: `success`, `error`, `canceled`, `deadline`.
- candidate `kind`: `block`, `empty`, `recovered`.
- `stage`: `acquire_inputs`, `core`, `resolve_state`, `restore`, `sign`,
  `persist`, `commit`, `broadcast_wait`, `emit`.
- deadline/action: `soft` or `hard`; `wait`, `emit_empty`, or `abort`.
- schedule `event`: `build_start`, `broadcast`.
- retry `reason`: `not_ready`, `other`.
- external stop `reason`: `unknown`, `ready_drained`, `soft_limit`, `deadline`,
  `attempt_limit`.
- load `class`: `underload`, `normal`, `soft`, `medium`, `hard`, `unknown`.
- overload `reason`: `none`, `block_limit`, `force_split_queue`,
  `long_collation`, `unknown`.

The standalone extension additionally collects its bounded controller and
storage status at scrape time. The read is outside collation and has a one
second timeout.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `gton_collator_status_available` | gauge | `mode` | `1` when standalone status collection succeeded. |
| `gton_collator_controller_state` | gauge | `mode`, `state` | Controller lifecycle booleans. |
| `gton_collator_controller_sessions` | gauge | `mode`, `state` | Active, future, backend, and observer session counts. |
| `gton_collator_status_windows` | gauge | `mode`, `state` | Active and retrying backend windows. |
| `gton_collator_status_windows_total` | counter | `mode`, `result` | Backend completed and failed window counters. |
| `gton_collator_storage_records` | gauge | `mode`, `type` | Session, window, pending-window, and candidate records. |
| `gton_collator_storage_pending_writes` | gauge | `mode` | Writes awaiting completion. |
| `gton_collator_storage_db_bytes` | gauge | `mode`, `type` | Pebble disk, live, WAL, memtable, and compaction-debt bytes. |
| `gton_collator_storage_db_read_amp` | gauge | `mode` | Pebble read amplification. |
| `gton_collator_storage_db_l0_files` | gauge | `mode` | Pebble L0 file count. |
| `gton_collator_storage_db_l0_sublevels` | gauge | `mode` | Pebble L0 sublevel count. |
| `gton_collator_storage_db_compactions_in_progress` | gauge | `mode` | Active Pebble compactions. |
| `gton_collator_last_completed_timestamp_seconds` | gauge | `mode` | Last completed standalone window Unix timestamp. |

Useful examples:

```promql
histogram_quantile(0.95, sum(rate(gton_collator_build_duration_seconds_bucket{mode="validator",result="success"}[5m])) by (le, chain))
histogram_quantile(0.95, sum(rate(gton_collator_stage_duration_seconds_bucket{mode="standalone"}[5m])) by (le, chain, stage))
histogram_quantile(0.95, sum(rate(gton_collator_schedule_lateness_seconds_bucket[5m])) by (le, mode, chain, event))
sum(rate(gton_collator_deadline_events_total[5m])) by (mode, chain, deadline, action)
sum(rate(gton_collator_retries_total[5m])) by (mode, chain, reason)
```

Ready-to-import dashboards are provided in `validator_metrics.json` (validator
plus its integrated collator) and `collator_metrics.json` (standalone collator
only).

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
- `source`: `broadcast`, `broadcast_queue`, `broadcast_candidate`, `broadcast_cache`, `broadcast_hint`, `queue`, `peer_catch_up`, `peer_probe`, `next_block`, `indexed`, `next_description`, `stored`, `internal`, `unknown`.
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
- Blocksync queues: `output`, `internal`, `shard_description`.

Common `direction` values for `gton_p2p_broadcasts_total` are `received`,
`accepted`, and `queue_rebroadcasted`. The `received` series are counted after
the transport has reconstructed and parsed the broadcast, before application
acceptance, for every delivery mode. Peer roster membership is deliberately not
encoded in this metric. Overlay-level relay sends are exported separately via
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
