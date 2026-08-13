# gton-lab

`gton-lab` is a server-resident, JSON-first localnet harness. It does not know
about a particular host or attempt directory: all paths, processes, tmux
sessions, and node arguments belong in its config.

Commands:

```text
gton-lab preflight --config lab.json
gton-lab status --config lab.json
gton-lab deploy --config lab.json --binary ./gton-node
gton-lab run --config lab.json --scenario topology-cycle
gton-lab run --config lab.json --scenario full-cycle
gton-lab report --run ./lab-runs/<run>
gton-lab trim-logs --config lab.json --keep-bytes 16777216 [--apply]
```

Stdout is one machine-readable JSON value. Diagnostics are zerolog JSON on
stderr; set `GTON_LAB_LOG_FORMAT=console` for an interactive console writer.

Deployment is deliberately sequential and has no automatic rollback. For each
configured Go node it kills only that node's tmux session, recreates it, and
waits for a fresh masterchain event before moving to the next node. A start
command should make the node the pane process, for example:

```json
{
  "name": "go-0",
  "kind": "go",
  "log_path": "/srv/localnet/node-0/lab.jsonl",
  "tmux_session": "localnet-go-0",
  "start_command": [
    "/bin/bash",
    "-lc",
    "cd /srv/localnet/node-0 && exec {binary} --config config.json --data-dir data --global-config-file global.json"
  ]
}
```

Unless the command overrides them, deploy appends `--verbosity info`, JSON file
logging, and 100 MiB/5-file compressed rotation. Use a dedicated `log_path` for
the harness; do not point it at an unbounded historical console log.

`trim-logs` is a dry-run unless `--apply` is explicit. Apply archives only the
bounded tail of each exact configured regular file, then truncates that same
inode. It never deletes attempt directories and refuses to run during a load.

For parallel load, set `sender_count` and put `{sender}` in `state_path`.
Optional setup runs senders sequentially because they share the funding-wallet
sequence; only the load phase fans out. `topology_timeout` bounds post-load
full-cycle observation independently of the normal `settle` delay.

Use `topology-cycle` on a weak lab host when jetton load is only the split
trigger. A sender which launched, submitted every batch (`failed_batches=0`),
and timed out only while confirming transfers is reported as advisory
`load_delivery_incomplete`; topology and parity polling continues. Process
launch failures, unknown sender failures, and any failed batch remain hard.
The report keeps `load_delivery_outcome` separate from
`consensus_topology_verdict` and requires the ordered cycle
linear → split → both children → rotation → merge → after-merge → linear,
plus collation on every required node, validation on every required Go node,
and a C++ candidate hash validated by Go. `full-cycle` and its `all` alias keep
strict delivery semantics.
