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
| `gton_validator_validation_stage_duration_seconds` | histogram | `chain`, `stage` | Non-overlapping validation runtime stages. |
| `gton_validator_validation_semantic_stage_duration_seconds` | histogram | `chain`, `stage` | Nested deterministic stages inside `semantic_validation`. |
| `gton_validator_validation_task_duration_seconds` | histogram | `chain`, `result` | The single semantic-validation task for a candidate. |
| `gton_validator_candidate_size_bytes` | histogram | `chain`, `part` | Candidate block and collated-data payload sizes entering validation. |
| `gton_validator_candidate_cache_entries` | gauge | `chain`, `state` | Consensus candidate cache entries by whether their payload is still in memory. |
| `gton_validator_candidate_cache_bytes` | gauge | `chain` | Candidate wire, block, and collated-data bytes retained by live consensus sessions. |
| `gton_validator_candidate_retention_capped_total` | counter | `chain` | Finalizations whose candidate retention pruned past the lineage the local producer still needs. |
| `gton_validator_candidate_persist_failures_total` | counter | `chain` | Failed durable writes of a candidate produced by this node. |
| `gton_validator_self_rejected_candidates_total` | counter | `chain` | Candidates this node produced and then rejected in its own validation. Always a defect on this node. |
| `gton_validator_chain_tip_wait_backstops_total` | counter | `chain` | Predecessor reads that waited past the backstop for a block to become readable. |
| `gton_validator_session_spec_rejections_total` | counter | `chain`, `role`, `reason` | Transitions of a local validator or observer session specification into rejection. |
| `gton_validator_consensus_slot` | gauge | `chain` | Present consensus slot of the newest live session on this chain. |
| `gton_validator_consensus_finalized_slot` | gauge | `chain` | Highest finalized consensus slot. |
| `gton_validator_consensus_last_finalization_timestamp_seconds` | gauge | `chain` | Unix time of the last observed finalization; its age is the liveness signal. |
| `gton_validator_consensus_slots_finalized_total` | counter | `chain` | Slots finalized by the consensus engine. |
| `gton_validator_consensus_standstills_total` | counter | `chain` | Standstill alarms fired by the consensus engine. |
| `gton_validator_consensus_certificates_total` | counter | `chain`, `kind` | Certificates stored, split by vote kind. |
| `gton_validator_consensus_certificate_signatures_total` | counter | `chain`, `kind` | Signatures carried by those certificates; divided by the certificate count it is the mean quorum size. |
| `gton_validator_consensus_votes_total` | counter | `chain`, `outcome` | Local voting decisions by outcome. |
| `gton_validator_consensus_first_block_timeout_seconds` | gauge | `chain` | Live leader-window first-block timeout, as scaled by the skip ladder. |
| `gton_validator_consensus_validator_last_signed_slot` | gauge | `chain`, `validator_index` | Highest slot at which each validator's signature appeared in a stored certificate of the current session. |
| `gton_validator_consensus_lineage_anchor_slot` | gauge | `chain` | Slot the last completed leader-window lineage walk reached. |
| `gton_validator_consensus_retention_lag_slots` | gauge | `chain` | Finalized slot minus that anchor. Diagnostic only; nothing is bounded by it. |
| `gton_validator_consensus_retained_payloads` | gauge | `chain` | Candidate payloads the session retains — the quantity the retention budget bounds. |
| `gton_validator_consensus_retention_budget_bytes` | gauge | `chain` | Payload bytes the session may retain below the fixed margin. |
| `gton_validator_consensus_retention_capped` | gauge | `chain` | 1 while retention is pruning below what the local producer asked to keep. |
| `gton_validator_lineage_walk_candidates` | histogram | `chain` | Candidates visited by one leader-window lineage walk. |
| `gton_validator_lineage_walk_duration_seconds` | histogram | `chain`, `result` | Duration of that walk, including the anchor state load. |
| `gton_validator_lineage_walk_steps_total` | counter | `chain`, `source` | Walk steps by where the candidate had to come from. |

Common values:

- `chain`: `masterchain`, `shardchain`.
- `kind`: `block`, `empty`.
- `origin`: `local`, `remote`; local means the candidate leader is this
  session's validator identity.
- validation `result`: `success`, `rejected`, `canceled`, `deadline`, `error`.
- validation-task `result`: `success`, `rejected`, `not_ready`, `canceled`, `deadline`,
  `error`.
- session-spec `role`: `validator`, `observer`.
- session-spec rejection `reason`: `unsupported_simplex_protocol`,
  `missing_simplex_config`, `missing_blockchain_config`,
  `local_validator_key_not_found`, `invalid_session_spec`.
- runtime `stage`: `load_candidate`, `resolve_parent`,
  `wait_min_block_interval`, `semantic_validation`, `wait_valid_after`.
- semantic `stage`: `restore_state`, `prepare_master_view`,
  `resolve_chain_inputs`, `decode_candidate`, `verify_transition`,
  `wait_inputs`.
  `restore_state` covers the single parse of the candidate — its BOC decode,
  the exact header and extra re-parse, the header-versus-id checks, the
  collated data, and the successor state applied on top of the predecessor.
  `decode_candidate` is what only the masterchain config can decide: size
  limits, the state update validation, the block dictionaries, the value flow
  and the candidate state views. The block file hash, the exact block parts and
  the header checks moved from `decode_candidate` into `restore_state` when the
  two stages became one capsule, so a small, constant part of the second stage
  now shows up in the first.
  `wait_inputs` is passive, event-driven waiting for the exact block/state or
  validator-group snapshot published by sync. Validation does not start a
  download. Its duration is subtracted from the working stage in which the
  dependency was requested, so the semantic-stage graph remains a real stack.
- candidate cache `state`: `retained` (payload in memory), `released` (identity
  and certificate only).
- certificate `kind`: `notarize`, `finalize`, `skip`.
- vote `outcome`: `cast` (own votes signed and sent), `accepted`, `rejected`,
  `abstained`.
- lineage walk `result`: `ok`, `error`.
- lineage step `source`: `memory` (payload still resident), `storage` (released
  payload read back from the candidate store), `peer` (no durable copy, so the
  network is asked).

There is no validation retry counter or retry-wait stage. One candidate creates
one cancellable validation task. Missing exact inputs suspend that task on the
live-view/group event and resume its current stage; the candidate BOC, collated
proof, and already prepared transition are not parsed again. If the 60-second
query deadline expires, Simplex abstains instead of turning local lag into a
candidate rejection.

The `consensus_*` series are the consensus state itself, taken from values the
Simplex engine already computes. They are collected at scrape time from the
runner's published snapshot and never post onto the consensus loop, so a scrape
cannot block behind the goroutine whose stall it is being scraped to explain.
The gauges describe a position and therefore come from the newest live session
of each chain (two overlap only across a rotation); the counters are
accumulated by difference and survive the session that produced them, including
its retirement.

Liveness reads off `time() - gton_validator_consensus_last_finalization_timestamp_seconds`.
A chain that is settling slots without producing blocks shows up as
`consensus_certificates_total{kind="skip"}` rising while
`consensus_slots_finalized_total` is flat, and
`consensus_first_block_timeout_seconds` climbing is the skip ladder scaling the
leader-window timeout. `consensus_validator_last_signed_slot` is the only
signal that names a validator that has gone quiet before the session stops
finalizing; it resets per session, because after a rotation the index is a
different node, and it is not exported for rosters above 64 members. Comparing
it against `consensus_slot` needs an explicit matcher — the two carry different
label sets, so the subtraction has to be joined `on (chain)`, as the example
below does.

Every gauge in this group comes from the session registered most recently for
its chain, which during a rotation is the incoming one. That is registration
order and deliberately not the slot: slots restart at zero per session, so the
outgoing session is the one with the larger slot for as long as the two overlap.
The counters, unlike the gauges, are the sum over every session the process has
run.

`consensus_retention_lag_slots` is reported for diagnosis and bounds nothing.
It is the slot distance between the finalized slot and the producer's anchor,
and skipped slots inflate it on their own: what the retention budget bounds is
`consensus_retained_payloads` and the bytes behind them, because those are the
memory. Those two and the anchor are read live from the session's resolvers, so
they keep moving while a session is not finalizing — which is the interval they
exist for. `consensus_retention_budget_bytes` is the first term of the ceiling
and not the whole of it: the fixed 8 s margin and every candidate at or above
the finalized tip sit outside the budget by design (see
`service/validator/retention.go`). Crossing the budget is a degradation point,
not a limit — payloads are released and the next read comes from the candidate
store, which is what `lineage_walk_steps_total{source="storage"}` counts.
`consensus_retention_capped` at 1, together with
`lineage_walk_steps_total{source="storage"}` rising, is the retention floor
having given up and the leader window paying storage reads —
`lineage_walk_candidates` against `consensus_retention_lag_slots` then says
whether that is a real production backlog (the two agree) or a run of skipped
slots (the lag is far larger).

`candidate_retention_capped_total` counts the finalizations spent in that
state, which makes the condition a rate rather than an event.

`candidate_persist_failures_total` counts durable writes of a candidate this
node produced that did not commit. The write is submitted by the producer and
awaited by the voter, so on a validator session a nonzero rate is accompanied by
failed notarize votes for the same slots. On an observer session — the delegated
and standalone collator composition, which has no voter — nothing else observes
it: the candidate was broadcast and the peers that received it can still finalize
it, but this node cannot serve it back or resume it after a restart. Any nonzero
rate there is a storage fault to act on, and it is logged at Error.

`chain_tip_wait_backstops_total` counts predecessor reads that waited past the
30-second backstop without the block becoming readable. It is an alarm, not a
rate. A candidate's parent is read through the live view, and every shard block
this session finalizes is published into the live view at acceptance — before its
database commit, because waiting for that commit is what cost 28.1% of a measured
18,047 s run in lost basechain finality. The wait that remains is edge-triggered:
it re-reads on each publication and never polls. So a firing backstop means one of
two things. Either the node is genuinely catching up — crash replay, or a block
this node did not finalize itself, where the wait is expected and bounded by the
shard client — or some path made a state readable without raising the live view's
artifacts signal, which would otherwise turn a slow-but-correct wait into a silent
hang. The accompanying Warn line names the block and how long the read waited.

`self_rejected_candidates_total` counts candidates this node built and then
refused in its own validation, on the branch that mirrors the reference
node's `LOG(ERROR) << "BUG! Candidate ... is self-rejected"`
(validator/consensus/block-validator.cpp:112-117). It can only mean a
collator/validator asymmetry inside this process: the two halves disagree about
the same bytes, so this node produces blocks the network will not take and does
not vote for them itself. Nothing else reports it — the candidate merely never
gets our vote, and every rejection log that names it belongs to a peer — so any
nonzero rate is an incident, and the accompanying Error line carries the
underlying reason the counter cannot.

It is scoped to the local route by the leader index, the same predicate the
reference uses. `candidate.Leader` is pinned to the slot's expected leader and
verified against that leader's signature before validation starts, and an
observer session has no voting identity at all, so a remote candidate cannot
raise it. A delegated window does count: the block was produced by a collator
this node authorized and published under its leadership.

Useful examples:

```promql
sum(rate(gton_validator_validations_total[5m])) by (chain, result)
histogram_quantile(0.95, sum(rate(gton_validator_validation_decision_duration_seconds_bucket{result="success"}[5m])) by (le, chain))
histogram_quantile(0.95, sum(rate(gton_validator_validation_ready_duration_seconds_bucket{result="success"}[5m])) by (le, chain))
histogram_quantile(0.95, sum(rate(gton_validator_validation_stage_duration_seconds_bucket[5m])) by (le, chain, stage))
histogram_quantile(0.95, sum(rate(gton_validator_validation_semantic_stage_duration_seconds_bucket[5m])) by (le, chain, stage))
sum(rate(gton_validator_session_spec_rejections_total[5m])) by (chain, role, reason)
sum(gton_validator_candidate_cache_bytes) by (chain)
sum(rate(gton_validator_candidate_retention_capped_total[5m])) by (chain)
sum(rate(gton_validator_candidate_persist_failures_total[5m])) by (chain)
sum(rate(gton_validator_self_rejected_candidates_total[5m])) by (chain)
sum(rate(gton_validator_chain_tip_wait_backstops_total[5m])) by (chain)
time() - max(gton_validator_consensus_last_finalization_timestamp_seconds) by (chain)
sum(rate(gton_validator_consensus_certificates_total[5m])) by (chain, kind)
sum(rate(gton_validator_consensus_certificate_signatures_total[5m])) by (chain, kind)
  / sum(rate(gton_validator_consensus_certificates_total[5m])) by (chain, kind)
max(gton_validator_consensus_first_block_timeout_seconds) by (chain)
max by (chain, validator_index) (
  gton_validator_consensus_slot
    - on (chain) group_right gton_validator_consensus_validator_last_signed_slot)
sum(rate(gton_validator_lineage_walk_steps_total[5m])) by (chain, source)
histogram_quantile(0.95, sum(rate(gton_validator_lineage_walk_candidates_bucket[5m])) by (le, chain))
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
| `gton_collator_build_duration_seconds` | histogram | `mode`, `chain`, `result` | Whole build future: input acquisition, active candidate assembly, external-message waiting, and serialization. |
| `gton_collator_stage_duration_seconds` | histogram | `mode`, `chain`, `stage` | Non-overlapping acquisition and producer stages. |
| `gton_collator_candidates_total` | counter | `mode`, `chain`, `kind` | Newly signed block or empty candidates. |
| `gton_collator_candidate_production_duration_seconds` | histogram | `mode`, `chain`, `kind`, `result` | Candidate production through persistence, state commit, schedule wait, and emission; recovered candidates are a separate kind. |
| `gton_collator_candidate_size_bytes` | histogram | `mode`, `chain`, `origin`, `part` | Block and collated-data sizes. `origin="collation"` is a block this node built; `origin="validation"` is one it accepted from another validator. |
| `gton_collator_candidate_transactions` | histogram | `mode`, `chain`, `origin` | Transactions in a non-empty candidate, by `origin`. |
| `gton_collator_candidate_gas_used` | histogram | `mode`, `chain` | Gas used by a produced block. Collation only: gas is a producer's own metering and is not recoverable from a block. |
| `gton_collator_candidate_messages` | histogram | `mode`, `chain`, `origin`, `kind` | External and imported internal messages in a candidate, by `origin`. |
| `gton_collator_candidate_out_queue_messages` | histogram | `mode`, `chain` | Resulting outbound queue size. |
| `gton_collator_candidate_queue_cleaned_messages` | histogram | `mode`, `chain` | Outbound queue entries removed by the cleanup phase. |
| `gton_collator_queue_cleanup_stop_total` | counter | `mode`, `chain`, `reason` | Why the NEIGHBOUR half of out-queue cleanup stopped: `exhausted`, `block_full` or `budget`. A rising `budget` share means the wall-clock cleanup budget is binding. It says nothing about the predecessor's own half, which is drained to exhaustion under every reason — leaving one of our own processed entries queued produces a block every validator rejects. When that drain cannot fit the block limits the collation fails instead of truncating, and that is reported by `gton_collator_alarms_total{alarm="mandatory_dequeue_overflow"}`, not here. |
| `gton_collator_external_batches` | histogram | `mode`, `chain` | Ready external batches consumed. |
| `gton_collator_external_stop_total` | counter | `mode`, `chain`, `reason` | External-message phase stop reason. |
| `gton_collator_load_class_total` | counter | `mode`, `chain`, `class` | Produced candidates by load class. |
| `gton_collator_overload_total` | counter | `mode`, `chain`, `reason` | Produced candidates by overload cause. |
| `gton_collator_deadline_events_total` | counter | `mode`, `chain`, `deadline`, `action` | Soft/hard producer deadline decisions. |
| `gton_collator_retries_total` | counter | `mode`, `chain`, `reason` | Retried producer windows. |
| `gton_collator_alarms_total` | counter | `mode`, `chain`, `alarm` | Producer faults that need an operator, not a trend line: `short_collated_proof` and `mandatory_dequeue_overflow`. Any nonzero rate is a defect on this node. |
| `gton_collator_schedule_lateness_seconds` | histogram | `mode`, `chain`, `event` | Positive lateness at build-start or broadcast slot boundaries. |
| `gton_collator_windows_inflight` | gauge | `mode`, `chain` | Producer windows currently running. |
| `gton_collator_windows_total` | counter | `mode`, `chain`, `result` | Finished producer windows. |
| `gton_collator_window_duration_seconds` | histogram | `mode`, `chain`, `result` | Whole producer-window duration, including retries. |

Common values:

- `result`: `success`, `error`, `canceled`, `deadline`, `not_ready`.
  `not_ready` is a build that stopped because a local input had not arrived
  yet. It is not a failure: the window producer retries it on its own schedule
  and counts the wait in `gton_collator_retries_total{reason="not_ready"}`. It
  has its own result because it used to be reported as `error`, where it was
  99.7% of everything under that label and made a 0.2% real build-failure rate
  read as 39%. The validator-engine-compatible collation counters ignore it
  entirely: the attempt that eventually runs is the one they count.
- candidate `kind`: `block`, `empty`, `recovered`.
- `alarm`: `short_collated_proof`, `mandatory_dequeue_overflow`.
- `stage`, in the order a produced candidate passes through them:
  `acquire_inputs`, `assemble_candidate`, `wait_external_messages`,
  `resolve_candidate_state`, `restore_candidate`, `sign_candidate`,
  `commit_candidate_state`, `wait_broadcast_slot`, `persist_candidate`,
  `deliver_candidate`.
  `persist_candidate` is not the candidate marker's whole synced commit. The
  producer submits that write before `commit_candidate_state` and only waits for
  it here, at the last instant where the marker still has to be durable before
  the candidate can reach anyone, so the two stages in between overlap it and
  this stage reports what is left. On a healthy device that residue is near zero
  where the whole commit used to read as some 13 ms; the fsync still happens, and
  the property it protects — no candidate is emitted for a slot whose marker is
  not durable — is unchanged.
  What the stage measures now is the amount by which the write outran the work
  that was overlapping it, which is exactly the part that costs the producer
  anything. It grows when the store cannot keep up, and
  `gton_collator_storage_pending_writes` together with the
  `gton_collator_storage_db_*` gauges is where that pressure is visible on its
  own. The whole cost including the overlapped part remains inside
  `gton_collator_candidate_production_duration_seconds`.
- deadline/action: `soft` or `hard`; `wait`, `emit_empty`, `abort`, or
  `wait_no_empty`. `wait_no_empty` is a soft deadline the producer had to keep
  waiting through because the session has not finalized for
  `NoEmptyBlocksOnErrTimeout` and an empty block is therefore forbidden — the
  producer is not slow, it is not allowed to end the wait, and that is a
  different fault from an ordinary `wait`.
- schedule `event`: `build_start`, `broadcast`.
- retry `reason`: `not_ready`, `other`.
- external stop `reason`: `unknown`, `ready_drained`, `soft_limit`, `deadline`,
  `attempt_limit`.
- load `class`: `underload`, `normal`, `soft`, `medium`, `hard`, `unknown`.
- overload `reason`: `none`, `block_limit`, `force_split_queue`,
  `long_collation`, `unknown`.

`alarms_total` is the one collator counter that is never a workload
characteristic. Both members are conditions this node can detect about its own
output and nothing downstream can, and neither shows up anywhere else in this
table.

`alarm="short_collated_proof"` means a block SHIPPED with a collated proof this
node knows does not cover the predecessor-queue scan a proof-backed validator
will run over it, because the replay that widens the proof stopped on a verdict
about a predecessor entry. The build succeeded, so it is counted as an ordinary
candidate in `builds_total` and `candidates_total`; what the network sees is
peers rejecting the block for `augmented dict has special cells in tree
structure`, which names a cell and not a cause. The Error line accompanying the
counter carries the real reason, and it is the only place that reason exists.
The block is very likely to be rejected, so a sustained rate is a shard that is
producing and losing every block.

`alarm="mandatory_dequeue_overflow"` is the opposite shape: no block at all. The
predecessor left more already-processed own-shard queue entries than one block
can dequeue, and that dequeue is not optional — a block missing it is rejected by
every validator on a resident state — so the collation fails and loses the slot
rather than shipping something invalid or something over the block limits. It
appears in `builds_total{result="error"}` indistinguishably from a cancelled
slot or a bad input, which is why it has its own counter. Ordinary traffic
cannot produce it: an own entry offered to a block is dequeued by that same
block. It points at a preceding run of blocks whose own drain stopped early, at
a restored or imported predecessor, or at block limits configured below what the
shard's own queue needs — and the shard will report it every slot until the
population fits.

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
sum by (stage) (rate(gton_collator_stage_duration_seconds_sum{stage=~"acquire_inputs|assemble_candidate|wait_external_messages"}[5m]))
  / scalar(sum(rate(gton_collator_build_duration_seconds_count[5m])))
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
| `gton_storage_lazy_cell_loads_total` | counter | `layer` | Lazy cell loads by the layer that answered them. The layers are disjoint and every load lands in exactly one, so a stacked graph over `layer` sums to the total load rate. |
| `gton_storage_record_cache_entries` | gauge | — | Live entries resident in the encoded cell record cache. Absent when the tier is disabled (`cell_record_cache_bytes: 0`). |
| `gton_storage_record_cache_bytes` | gauge | `kind` | Record cache memory: `resident` (live entry bytes, header included), `capacity` (the configured arena budget) and `index` (the derived lookup index on top of it). Absent when the tier is disabled. |
| `gton_storage_record_cache_inserts_total` | counter | — | Records inserted into the record cache. |
| `gton_storage_record_cache_declined_inserts_total` | counter | — | Inserts declined because no index slot could be placed within the 16-slot probe budget. A cache declines instead of growing; a persistently high rate against inserts means the index is undersized for the record mix. |
| `gton_storage_record_cache_salvage_truncated_total` | counter | — | Region rotations whose CLOCK salvage hit the mandatory 50% re-append budget and dropped the remaining hot entries. The budget is the live-lock guard; this counter is how often it actually bit. |

Label notes:

- `generation` is a stable cell DB generation role: `active` or `pending`.
- `shard` is a cell DB shard number, plus a synthetic `total` shard.
- `layer` is where a lazy cell load was answered, cheapest first: `state_window`,
  `decoded_cache`, `record_cache`, `block_cache`, `page_cache`, `disk`.

**How much of `layer` is observed and how much is inferred.** `state_window`,
`decoded_cache` and `record_cache` are OBSERVED — the code knows which branch it
took (a `record_cache` load is a hit in the encoded record tier: no store read,
but the record still gets decoded). The three store-read layers are inferred from how long the read took, because neither
pebble nor the kernel tells a caller which of them served a particular Get. The
cut points are the geometric midpoints between tiers measured on the
deployment's own disk (pebble block cache 26.3 us, OS page cache 90.3 us, device
1.585 ms, giving 50 us and 400 us), and they live in
`service/storage/pebblestore/lazy_cell_metrics.go` so a different disk can
re-cut them after measuring its own tiers. Nothing re-cuts them automatically: a
self-tuning threshold would reclassify history and make two days of graph
incomparable.

Because the bottom three are inferred, read them against a signal that is not.
`gton_storage_cell_db_cache_requests_total{result="miss"}` is pebble's own
accounting of block-cache misses and needs no inference: if the `block_cache`
layer dominates while that counter climbs, or `disk` climbs while it is flat,
the thresholds have drifted from this deployment's hardware and want
re-measuring. The layer split is a decomposition of one true total, not five
independent measurements — the total is exact regardless of where the cut points
sit.

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
