# gton

`gton` is a Go implementation of a TON full node with a liteserver API. It does not implement validator functionality. The node is designed to be an efficient API access point for services, backend projects, indexers, wallets, and other infrastructure that needs fast synchronization and stable data serving under heavy load.

The project focuses on:

- Fast block sync and live updates;
- Memory efficiency
- Liteserver optimized for live data availability, with smart cache;
- LSM Pebble-based storage with a sharded cell DB
- Runtime metrics for sync, storage, p2p, and liteserver observability.

The project is under active development. Storage format and configuration may change without backward compatibility.

## Running

Build the node binary:

```bash
go build -o gton-node ./cmd/node
```

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
- liteserver: disabled, listen address `0.0.0.0:7445`.

Open the external ADNL/DHT ports on your firewall and make sure `adnl.external_addr` points to the node's public address. If you enable the liteserver for external clients, open its port too.

## CLI Flags

The main binary is `./cmd/node`.

```bash
./gton-node [flags]
```

Supported flags:

| Flag | Description |
| --- | --- |
| `--config <path>` | Path to the JSON config. Defaults to `config.json`. |
| `--log-level <level>` | Global log level: `trace`, `debug`, `info`, `warn`, `error`. |
| `--log-levels <list>` | Per-category log overrides, for example `liteserver=debug,p2p=warn`. |
| `--log-json` | Write logs as JSON instead of pretty console output. |
| `--global-config <url>` | Download global config from the URL and replace the file from `ton.global_config_path` before startup. |
| `--pprof-addr <addr>` | Enable `net/http/pprof`, for example `127.0.0.1:6060`. |
| `--from-zero` | Verify the initial key-block chain from zerostate instead of the `init_block` from global config. |
| `--archive-checkpoint-period <duration>` | Maximum current-state checkpoint interval during archive catch-up. Defaults to `2m`. |
| `--archive-prefetch-windows <n>` | Archive import window prefetch depth. Defaults to `8`. |

## Console Commands

After startup, the process reads commands from stdin. This is useful for manual diagnostics and maintenance without a separate RPC control interface.

| Command | Description |
| --- | --- |
| `status` | Prints a short sync, p2p, liteserver, and TPS status. |
| `status full` | Prints extended status with peer and overlay details. |
| `status db` | Prints Pebble/meta DB and cell DB generation status: cache, disk, L0, compaction, memtable, and read/write rates. |
| `serialize <masterchain_seqno>` | Starts persistent state serialization for the given masterchain seqno. |
| `serialize <masterchain_seqno> basechain` | Serializes only the basechain persistent state for the given masterchain seqno. |
| `serialize cancel` | Cancels the current persistent state serialization. |
| `migrate <masterchain_seqno>` | Starts cell DB generation migration from the persistent state of the given masterchain block. |
| `migrate stop` | Stops the current cell DB generation migration. |

Example:

```text
status
status db
serialize 48500000 basechain
migrate 48500000
```

## Configuration

The config is a JSON file. If it does not exist, the node generates a new file with ADNL/DHT/liteserver keys and default values, then exits so you can review it.

Simplified example:

```json
{
  "ton": {
    "global_config_path": "global.config.json",
    "sync_before": 3600,
    "state_ttl": 259200,
    "archive_ttl": 604800,
    "next_checkpoint_blocks": 600,
    "archive_checkpoint_blocks": 2000,
    "checkpoint_bytes": 1073741824
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
    "key": "<base64 ed25519 seed>",
    "listen_addr": "0.0.0.0:7445",
    "master_block_cache": 128,
    "shard_block_cache": 4096
  },
  "storage": {
    "dir": "data",
    "cell_total_cache_size": 8589934592,
    "cell_shard_memtable_size": 268435456,
    "cell_memtable_stop_writes_threshold": 4,
    "artifact_file_max_open": 512
  },
  "metrics": {
    "enabled": true,
    "listen_addr": "127.0.0.1:9090",
    "namespace": "gton"
  },
  "disable_state_serialization": false
}
```

### `ton`

| Field | Description |
| --- | --- |
| `global_config_path` | Path to the TON global config. If the file is missing, it is downloaded during startup. |
| `sync_before` | Minimum persistent state age for initial sync, in seconds. Defaults to `3600`. |
| `state_ttl` | Current-state TTL for cell generation rotation, in seconds. Defaults to `259200` (3 days). |
| `archive_ttl` | How long archive packages are kept, in seconds. Defaults to `604800` (7 days). |
| `next_checkpoint_blocks` | Current-state checkpoint frequency during next-block sync, in masterchain blocks. |
| `archive_checkpoint_blocks` | Current-state checkpoint frequency during archive catch-up. |
| `checkpoint_bytes` | Pending checkpoint data threshold in bytes. Once reached, sync applies more pressure to persist a checkpoint. |

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
| `key` | Base64-encoded Ed25519 seed for the liteserver key. Required when `enabled=true`. |
| `listen_addr` | Liteserver listener address. Defaults to `0.0.0.0:7445`. |
| `master_block_cache` | Live cache size for masterchain blocks. |
| `shard_block_cache` | Live cache size for shard blocks. |

### `storage`

| Field | Description |
| --- | --- |
| `dir` | Pebble storage directory. |
| `cell_total_cache_size` | Total cache budget for the cell DB, in bytes. Defaults to `8589934592` (8 GiB). |
| `cell_shard_memtable_size` | Memtable size for one cell DB shard, in bytes. |
| `cell_memtable_stop_writes_threshold` | Pebble stop-writes threshold for memtables. |
| `artifact_file_max_open` | Open-file limit for block/state artifacts. |

### `metrics`

| Field | Description |
| --- | --- |
| `enabled` | Enables the Prometheus endpoint. |
| `listen_addr` | HTTP listener address for `/metrics`, for example `127.0.0.1:9090`. |
| `namespace` | Prometheus metric prefix. Defaults to `gton`; must match `[A-Za-z_][A-Za-z0-9_]*`. |

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