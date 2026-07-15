# gton

<img align="right" width="512px" src="https://github.com/user-attachments/assets/b7ec1bc3-6cb0-4e93-bcf0-6c019e295147">

`gton` is a Go implementation of a TON full node with a liteserver API. It does not implement validator functionality. The node is designed to be an efficient API access point for services, backends of projects, indexers, wallets, and other infrastructure that needs fast synchronization and stable data serving under heavy load.

The project focuses on:

- Fast block sync and live updates;
- Memory efficiency
- Liteserver optimized for live data availability, with smart cache;
- LSM Pebble-based storage with a sharded cell DB
- Runtime metrics for sync, storage, p2p, and liteserver observability.

The project is under active development. Storage format and configuration may change without backward compatibility.

Telegram group: [@gtonnode](https://t.me/gtonnode)

## Hardware Requirements

- Minimum RAM: `48 GB`.
- Recommended RAM: `64 GB`.
- Minimum CPU: `8` cores.
- Minimum disk: `512 GB SSD`.

## Running

Build the node binary:

```bash
go build -o gton-node ./cmd/node
```

Or download from [releases](https://github.com/xssnick/gton/releases).

The first run creates `config.json` and exits:

```bash
./gton-node
```

Review the generated config, enable the liteserver and metrics if needed, then start the node again:

If `global.config.json` is missing, the node downloads it automatically. To replace the global config before startup:

```bash
./gton-node --global-config https://ton-blockchain.github.io/global.config.json
```

The generated config defaults to:

- ADNL: `0.0.0.0:30303`;
- DHT: `0.0.0.0:30304`;
- storage directory: `data`;
- liteserver: disabled, non-final mode disabled, listen address `0.0.0.0:7445`.

Open the external ADNL/DHT (UDP) ports on your firewall and make sure `adnl.external_addr` points to the node's public address. If you enable the liteserver for external clients, open its port too (TCP).

After startup node will sync with the latest blockchain state. First it will download `state` and then blocks to apply them. This could take around 1 - 3 hours depending on your hardware and chain.

## CLI Flags

The main binary is `./cmd/node`.

```bash
./gton-node [flags]
```

Supported flags:

| Flag | Description |
| --- | --- |
| `--config <path>` | Path to the JSON config. Defaults to `config.json`. |
| `--ls-pubkey` | Print the liteserver public key (base64) and exit. |
| `--adnl-id` | Print the ADNL id derived from `adnl.key` (base64) and exit. |
| `--version` | Print the build version and exit. |
| `--verbosity <level>` | Global log verbosity: `trace`, `debug`, `info`, `warn`, `error`. |
| `--log-types <list>` | Per-category log verbosity overrides, for example `liteserver=debug,p2p=warn`. |
| `--log-json` | Write logs as JSON instead of pretty console output. |
| `--log-file <path>` | Also write logs to this rotating file. Disabled by default. |
| `--log-file-max-size <mb>` | Rotate the log file after it reaches this size. Defaults to `100`. |
| `--log-file-max-backups <n>` | Maximum rotated log files to keep. Defaults to `10`, `0` keeps all. |
| `--log-file-max-age <days>` | Maximum days to keep rotated log files. Defaults to `30`, `0` keeps all. |
| `--log-file-compress` | Compress rotated log files. |
| `--global-config <url>` | Download global config from the URL and replace the file from `ton.global_config_path` before startup. |
| `--pprof-addr <addr>` | Enable `net/http/pprof`, for example `127.0.0.1:6060`. |
| `--skip-cfg-check` | Continue startup after creating missing config file without manually reviewing it first. |
| `--archive-checkpoint-period <duration>` | Maximum current-state checkpoint interval during archive catch-up. Defaults to `2m`. |
| `--archive-prefetch-windows <n>` | Archive import window prefetch depth. Defaults to `2`. |

## Using Custom Overlays and Non-Final Blocks

Use this when the node must receive broadcasts from a private/custom overlay and optionally expose pending non-final shard blocks through its liteserver.

### 1. Configure `custom_overlays`

Custom overlays are configured directly in `config.json`. The local node must be listed in `nodes`; otherwise it will ignore the overlay. Set `block_sender=true` if this node should send block broadcasts to the overlay, and `msg_sender=true` if it should send external messages.

Minimal `custom_overlays` example:

```json
{
  "custom_overlays": [{
    "name": "private-a",
    "nodes": [{
      "adnl_id": "<base64 32-byte ADNL id>",
      "msg_sender": true,
      "msg_sender_priority": 0,
      "block_sender": true
    }],
    "sender_shards": [],
    "skip_public_msg_send": false
  }]
}
```

`sender_shards` may be empty to send all shards. To limit sending to a shard, add entries with `workchain` and `shard`:

```json
"sender_shards": [{
  "workchain": 0,
  "shard": -9223372036854775808
}]
```

`skip_public_msg_send` skips public-overlay external message broadcasting only when the message is actually sent through a matching custom overlay.

### 2. Enable non-final liteserver data

Set `liteserver.non_final_enabled=true` in the node config and restart:

```json
{
  "liteserver": {
    "enabled": true,
    "non_final_enabled": true
  }
}
```

Non-final data is kept in memory only. It is not applied to persistent state or written to storage, and it is dropped when the corresponding final shard blocks arrive or cache limits are reached. Supported liteserver methods use it transparently when the request references a pending non-final block; regular final-block requests do not touch the non-final cache. `getValidatorGroups` is not supported for non-final blocks.

## Console Commands

After startup, the process reads commands from stdin. This is useful for manual diagnostics and maintenance without a separate RPC control interface.

| Command | Description                                                                                                                                   |
| --- |-----------------------------------------------------------------------------------------------------------------------------------------------|
| `status` | Prints a short sync, p2p, liteserver, and TPS status.                                                                                         |
| `status full` | Prints extended status with peer and overlay details.                                                                                         |
| `status db` | Prints Pebble/meta DB and cell DB generation status: cache, disk, L0, compaction, memtable, and read/write rates.                             |
| `serialize <masterchain_seqno>` | Starts persistent state serialization for the given masterchain seqno.                                                                        |
| `serialize cancel` | Cancels the current persistent state serialization.                                                                                           |
| `migrate <masterchain_seqno>` | Starts cell DB generation migration from the persistent state of the given masterchain block. Migration is used for cell db size optimization |
| `migrate stop` | Stops the current cell DB generation migration.                                                                                               |

Example:

```text
status
status db
serialize 48500000
migrate 48500000
```

## Background Routine

After startup, the node runs a background maintenance routine. It keeps stored data useful for serving requests while controlling disk usage. Only one heavy maintenance job runs at a time, so the node does not run several large cleanup or rebuild tasks at once.

The routine can run these jobs:

##### Persistent state serialization

Creates snapshot files with the full chain state at selected masterchain key blocks. These files let the node serve state downloads to other nodes and provide a stable point for later storage maintenance.

##### Cell database migration

Builds a fresh cell database from a serialized state snapshot, catches it up to the current chain state, and then switches to it. This keeps the state database compact and lets the node remove old state data after it is no longer needed.

##### Persistent state cleanup

Deletes expired state snapshot files while keeping recent snapshots and any snapshot still needed by an unfinished migration. This saves disk space without breaking state serving or an in-progress database migration.

##### Archive cleanup

Removes old archive packages and related block metadata after the configured archive retention period. This keeps archive storage inside the configured history window instead of growing forever.

## Archival liteserver

Use archival liteserver mode when the node should start from the zero state, import archive data forward, keep archive packages forever, and serve liteserver requests over the full stored history.

Enable it by setting `ton.sync_before=-1` and keeping both storage retention knobs disabled:

```json
{
  "ton": {
    "sync_before": -1,
    "state_ttl": 0,
    "archive_ttl": 0
  },
  "liteserver": {
    "enabled": true
  }
}
```

In this mode startup rejects non-zero `state_ttl` or `archive_ttl`. `state_ttl=0` keeps the current-state cell database generation, and `archive_ttl=0` disables archive package pruning. Disk usage grows with the archived history, so size the storage volume for long-term retention before enabling this mode.

## Configuration

The config is a JSON file. If it does not exist, the node generates a new file with ADNL/DHT/liteserver keys and default values, then exits so you can review it.

Simplified example:

```json
{
  "ton": {
    "global_config_path": "global.config.json",
    "sync_before": 14400,
    "sync_until": 0,
    "state_ttl": 172800,
    "archive_ttl": 604800,
    "next_checkpoint_blocks": 200,
    "archive_checkpoint_blocks": 2000,
    "checkpoint_bytes": 268435456,
    "sync_backpressure_windows": 4
  },
  "adnl": {
    "key": "<base64 ed25519 seed>",
    "listen_addr": "0.0.0.0:30303",
    "external_addr": "203.0.113.10:30303"
  },
  "dht": {
    "key": "<base64 ed25519 seed>",
    "listen_addr": "0.0.0.0:30304"
  },
  "liteserver": {
    "enabled": true,
    "non_final_enabled": false,
    "key": "<base64 ed25519 seed>",
    "listen_addr": "0.0.0.0:7445",
    "master_block_cache": 128,
    "shard_block_cache": 1024,
    "allow_duplicate_externals": false,
    "send_message_broadcast_bytes_per_second": 0,
    "send_message_broadcast_max_delay_ms": 100,
    "send_message_broadcast_fanout": 5
  },
  "storage": {
    "dir": "data",
    "cell_total_cache_size": 8589934592,
    "decoded_cell_cache_enabled": true,
    "decoded_cell_cache_shards": 64,
    "decoded_cell_cache_bytes_per_entry": 16384,
    "decoded_cell_cache_min_entries": 65536,
    "decoded_cell_cache_max_entries": 1048576,
    "cell_shard_memtable_size": 268435456,
    "cell_memtable_stop_writes_threshold": 4,
    "large_boc_shard_read_workers": 2,
    "persistent_state_large_boc_batch_size": 524288,
    "state_serialize_one_pass": false,
    "artifact_file_max_open": 512
  },
  "metrics": {
    "enabled": true,
    "listen_addr": "127.0.0.1:9090",
    "namespace": "gton"
  },
  "custom_overlays": [],
  "disable_state_serialization": false
}
```

### `ton`

| Field | Description |
| --- | --- |
| `global_config_path` | Path to the TON global config. If the file is missing, it is downloaded during startup. |
| `sync_before` | Minimum persistent state age for initial sync, in seconds. Defaults to `14400` (4 hours). Set to `-1` only for Archival liteserver mode. |
| `sync_until` | Optional UNIX block time cutoff. Defaults to `0` (disabled). When set, the node syncs only up to blocks not newer than this time, then switches p2p offline while keeping liteserver data available. |
| `state_ttl` | Current-state TTL for cell generation rotation, in seconds. Defaults to `172800` (2 days). Set to `0` to disable automatic cell DB generation rotation and keep the current-state DB generation forever. Required to be `0` when `sync_before=-1`. |
| `archive_ttl` | How long archive packages are kept, in seconds. Defaults to `604800` (7 days). Set to `0` to keep archives forever. Required to be `0` when `sync_before=-1`. |
| `next_checkpoint_blocks` | Current-state checkpoint frequency during next-block sync, in masterchain blocks. Defaults to `200`. |
| `archive_checkpoint_blocks` | Current-state checkpoint frequency during archive catch-up. |
| `checkpoint_bytes` | Pending checkpoint data threshold in bytes. Once reached, sync schedules a checkpoint. Defaults to `268435456` (256 MiB). |
| `sync_backpressure_windows` | Number of checkpoint windows allowed to accumulate while a checkpoint is still persisting before sync backpressure waits. Defaults to `4`. |

### `adnl`

| Field | Description |
| --- | --- |
| `key` | Base64-encoded Ed25519 seed for the ADNL key. Generated automatically. |
| `listen_addr` | Local address for the p2p ADNL listener. Empty value switches p2p to client mode. |
| `external_addr` | Public `ip:port` announced to other peers. |

### `dht`

| Field | Description |
| --- | --- |
| `key` | Base64-encoded Ed25519 seed for the DHT key. Generated automatically. |
| `listen_addr` | Local address for the DHT listener. Empty value disables DHT server mode. |

### `liteserver`

| Field | Description |
| --- | --- |
| `enabled` | Enables the liteserver. |
| `non_final_enabled` | Enables in-memory non-final shard block visibility for supported liteserver methods. Defaults to `false`. |
| `key` | Base64-encoded Ed25519 seed for the liteserver key. Required when `enabled=true`. |
| `listen_addr` | Liteserver listener address. Defaults to `0.0.0.0:7445`. |
| `master_block_cache` | Live cache size for masterchain blocks. |
| `shard_block_cache` | Live cache size for shard blocks. |
| `allow_duplicate_externals` | When `true`, duplicate external messages submitted through liteserver `sendMessage` are checked and broadcast again. Defaults to `false`; duplicates are accepted as a successful no-op. |
| `send_message_broadcast_bytes_per_second` | Leaky-bucket capacity for external message broadcast traffic. `0` disables the limit. |
| `send_message_broadcast_max_delay_ms` | Maximum pacing delay before `sendMessage` is rejected with `reason="broadcast_capacity"`. Defaults to `100`; set to `0` for no backlog. |
| `send_message_broadcast_fanout` | Number of active public-overlay peers selected for external message broadcasts originated by liteserver `sendMessage`. Defaults to `5` when omitted or set to `0`; valid range is `3` to `20`. When the node is lagged, the local-send fanout is halved, with a minimum of `1`. Inbound overlay rebroadcast fanout is unchanged. |

### `storage`

| Field | Description |
| --- | --- |
| `dir` | Pebble storage directory. |
| `cell_total_cache_size` | Total cache budget for the cell DB, in bytes. Defaults to `8589934592` (8 GiB). |
| `decoded_cell_cache_enabled` | Enables the in-process decoded lazy-cell cache. Defaults to `true`. |
| `decoded_cell_cache_shards` | Number of LRU shards in the decoded lazy-cell cache. Defaults to `64`. |
| `decoded_cell_cache_bytes_per_entry` | Estimated bytes per decoded cell cache entry used to derive capacity from `cell_total_cache_size`. Defaults to `16384`. |
| `decoded_cell_cache_min_entries` | Minimum decoded cell cache entries. Defaults to `65536`. |
| `decoded_cell_cache_max_entries` | Maximum decoded cell cache entries. Defaults to `1048576`. |
| `cell_shard_memtable_size` | Memtable size for one cell DB shard, in bytes. |
| `cell_memtable_stop_writes_threshold` | Pebble stop-writes threshold for memtables. |
| `large_boc_shard_read_workers` | Per-cell-DB-shard parallel readers for large-BOC state serialization loads. Defaults to `2`; `0` uses the default. |
| `persistent_state_large_boc_batch_size` | Large-BOC serialization batch size for persistent state files, in cells. Defaults to `524288`; `0` uses the default. |
| `state_serialize_one_pass` | Forces persistent state serialization to use one-pass large-BOC serialization for every state part. Defaults to `false`. |
| `artifact_file_max_open` | Open-file limit for block/state artifacts. |

### `metrics`

| Field | Description |
| --- | --- |
| `enabled` | Enables the Prometheus endpoint. |
| `listen_addr` | HTTP listener address for `/metrics`, for example `127.0.0.1:9090`. |
| `namespace` | Prometheus metric prefix. Defaults to `gton`; must match `[A-Za-z_][A-Za-z0-9_]*`. |

### `custom_overlays`

List of private overlay definitions. Empty by default. Each overlay has:

| Field | Description |
| --- | --- |
| `name` | Custom overlay name used to derive the overlay id. |
| `nodes` | Fixed overlay members. The local ADNL id must be present here to join the overlay. |
| `sender_shards` | Optional shard filter for block and external-message sending. Empty means all shards. |
| `skip_public_msg_send` | When `true`, skips public-overlay external message broadcasting after the message is sent through this custom overlay. |

Each `nodes` entry has:

| Field | Description |
| --- | --- |
| `adnl_id` | Base64-encoded 32-byte ADNL id. |
| `msg_sender` | Allows this node to send external-message broadcasts. |
| `msg_sender_priority` | Message sender priority preserved from the overlay config. |
| `block_sender` | Allows this node to send block broadcasts. |

### `disable_state_serialization`

When `true`, automatic persistent state serialization is disabled. Manual serialization through the `serialize` console command is still a separate service operation.

## Metrics

To enable the Prometheus endpoint, add:

```json
{
  "metrics": {
    "enabled": true,
    "listen_addr": "127.0.0.1:9090",
    "namespace": "gton"
  }
}
```

Metrics are exposed at:

```text
http://127.0.0.1:9090/metrics
```

The exported metrics cover liteserver latency, sync lag, block download/apply, checkpoint persistence, p2p queues, rebroadcasting, blocksync, and Pebble/cell DB status. The full metric list and PromQL examples are documented in [METRICS.md](METRICS.md). A Grafana dashboard is available in `metrics.json`.

## Extensions

The node can call a statically linked extension from a custom node binary.

The extension is compiled into the custom binary, so node builds do not need cgo. A custom binary imports `github.com/xssnick/gton`, parses its own CLI/config files, and passes typed startup values to `gton.RunNode`:

```go
package main

import (
    "context"
    "flag"
    "os"

    "github.com/xssnick/gton"
    nodeconfig "github.com/xssnick/gton/cmd/node/config"
    "github.com/rs/zerolog"
    "github.com/xssnick/tonutils-go/liteclient"
    "my/project/extension"
)

func main() {
    flags := flag.NewFlagSet("my-node", flag.ExitOnError)
    configPath := flags.String("config", nodeconfig.DefaultPath, "path to node config JSON")
    flags.Parse(os.Args[1:])

    cfg, err := nodeconfig.Load(*configPath)
    if err != nil {
        panic(err)
    }

    ctx := context.Background()
    runtimeOpts, err := cfg.RuntimeOptions(gton.DefaultNodeOptions())
    if err != nil {
        panic(err)
    }

    globalConfigPath := runtimeOpts.GlobalConfigPath
    if _, err = nodeconfig.EnsureGlobalConfig(ctx, globalConfigPath, nodeconfig.DefaultGlobalConfigURL, false); err != nil {
        panic(err)
    }
    globalConfig, err := liteclient.GetConfigFromFile(globalConfigPath)
    if err != nil {
        panic(err)
    }

    opts := runtimeOpts.Node
    opts.GlobalConfig = globalConfig
    opts.Logger = zerolog.New(os.Stdout).Level(zerolog.InfoLevel).With().Timestamp().Logger()
    opts.Extension = extension.New

    err = gton.RunNode(ctx, opts)
    if err != nil {
        panic(err)
    }
}
```

Custom binaries can load the node config themselves and keep extension config in
a separate file:

```go
nodeCfg, err := nodeconfig.Load("node.json")
if err != nil {
    panic(err)
}
runtimeOpts, err := nodeCfg.RuntimeOptions(gton.DefaultNodeOptions())
if err != nil {
    panic(err)
}
globalConfig, err := liteclient.GetConfigFromFile(runtimeOpts.GlobalConfigPath)
if err != nil {
    panic(err)
}
extensionCfg, err := extension.LoadConfig("extension.json")
if err != nil {
    panic(err)
}

opts := runtimeOpts.Node
opts.GlobalConfig = globalConfig
opts.Logger = zerolog.New(os.Stdout).Level(zerolog.InfoLevel).With().Timestamp().Logger()
opts.Extension = extension.NewFactory(extensionCfg)

err = gton.RunNode(context.Background(), opts)
```

See `example_extension/` for a complete static apply logger and live transaction
replay checker example. The example is a separate Go module with a local
`replace` to the repository root, so it does not become part of the main
module's `./...` package set.

The extension factory must have this signature:

```go
func New(node hooks.Node) (hooks.Extension, error)
```

`hooks.Node` gives the extension a small capability surface:
`Network` can send external messages, `Store` is a read-only live view backed by the same store/cache layer used by the liteserver, and `Logger` is a zerolog logger with `source=extension`. It does not expose block download methods or storage modifiers.

The returned value receives hook events through:

```go
type Extension interface {
    Start(context.Context) error
    Close(context.Context) error
    OnBlockApplied(context.Context, BlockAppliedEvent) error
    OnExternalMessage(context.Context, ExternalMessageEvent) error
    OnBlockReceived(context.Context, BlockReceivedEvent) error
}
```

`OnBlockApplied` is called after a block state update is applied and before that block flow can continue to checkpoint or persist. Block apply hook delivery is at least once: the same block apply call may be repeated if the node crashes or is hard-stopped before the block flow is fully persisted/checkpointed. If the method returns an error, the node retries the same event after a short delay and does not advance that block flow until the extension returns `nil`. This retry only blocks the affected block dependency branch; shard block apply is parallel, so other independent branches may continue and may call the same extension concurrently.

`Start` is called after the node services are started. `Close` is called during shutdown and should block until the extension exits or the context is done. Extensions with no background work should return `nil`.

`OnExternalMessage` is called after an external message is accepted by TVM emulation and before it is rebroadcast further. `event.IsLocal` is `true` when the message came from this node's local API path and `false` for overlay broadcasts. If the method returns an error, that external message is dropped without retry.

`OnBlockReceived` is called when a block is received from broadcasts, downloads, or archive imports. `event.IsSigned` is `true` for signed block artifacts and downloaded/imported full blocks, and `false` for shard block candidates. If the method returns an error, the node logs it and continues the block flow.

Extension implementations must be thread-safe. They must also treat every object received in hook events as borrowed for the duration of the call only. Do not retain event fields, cells, metadata, state pointers, or payload slices after the hook returns. If an extension needs data later, it must copy or serialize the exact data it owns before returning.

`BlockAppliedEvent` includes block/proof BOC payloads, block root cell, block metadata, applied state roots, and the inclusion masterchain reference/state for shard blocks. The block id is available as `event.Meta.ID`. `InclusionMasterRef` and `InclusionMasterState` are `nil` for masterchain blocks. The node does not resolve execution masterchain data on the apply hot path. Extensions that need the execution master reference should parse `tlb.Block` from `BlockRoot`; state can then be taken from the event roots when it is already present or loaded through the normal read-only store APIs.

`BlockReceivedEvent` includes the received block/proof BOC payloads, block root cell, block metadata, and `IsSigned`. The block id is available as `event.Meta.ID`.

A minimal extension looks like:

```go
package extension

import (
    "context"

    "github.com/xssnick/gton/service/hooks"
)

type extension struct{}

func New(node hooks.Node) (hooks.Extension, error) {
    return extension{}, nil
}

func (extension) Start(ctx context.Context) error {
    return nil
}

func (extension) Close(ctx context.Context) error {
    return nil
}

func (extension) OnBlockApplied(ctx context.Context, event hooks.BlockAppliedEvent) error {
    return nil
}

func (extension) OnExternalMessage(ctx context.Context, event hooks.ExternalMessageEvent) error {
    return nil
}

func (extension) OnBlockReceived(ctx context.Context, event hooks.BlockReceivedEvent) error {
    return nil
}
```

Build a custom node binary from the wrapper package:

```bash
cd example_extension
CGO_ENABLED=0 go build -o gton-node-with-extensions .
```
