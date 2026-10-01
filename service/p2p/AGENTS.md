# P2P package guide

This file supplements the repository root `AGENTS.md` for changes under
`service/p2p`.

## Ownership map

- `node.go` owns P2P dependency composition and the `Node` state declaration.
  `node_lifecycle.go` owns startup, shutdown, goroutine joining, and inbound
  draining. Keep protocol workflows in their domain files instead of growing
  either file.
- `node_config.go`, `dht_runtime.go`, and `chain_heads.go` own global-config
  validation, DHT startup, and observed-chain-head state respectively.
- `peer_pool.go` owns pooled ADNL/RLDP transports and endpoint retention.
- `internal/eventdedupe` owns the bounded TTL/LRU recent-key cache. It has no
  P2P or `Node` dependency; use the concrete cache directly without wrappers.
- `internal/peerroute` owns learned QUIC endpoints, retry state, background
  dial single-flight, and route-table eviction. Consumers must use its public
  methods; do not expose its mutexes or atomics through aliases.
- `internal/fastsync` owns FastSync membership, certificate slot arbitration,
  descriptor admission, liveness, and random-peer state. These owners are
  synchronous and concrete: they have no Node, transport, callback, or
  goroutine lifecycle. Keep `ID` concrete on their hot methods; generic
  fixed-size ID acceptance belongs only on cold construction boundaries.
- Root `internal/extmsg` owns external-message wire parsing, address keys, and
  transport-capacity/offline errors. P2P must not import
  `service/externalmsg`; that package owns admission and TVM checker policy.
- Root `internal/shardstate` owns split shard-state cell encoding and merge
  rules. Use its cells directly without conversion or copies; P2P must not
  import the state synchronization workflow for codec operations.
  `service/shard` separately owns validated shard-ID math and containment.
- `overlay_spec.go` owns public/custom overlay specifications and shard-prefix
  helpers.
- `subscription_registry.go` owns subscription registration and snapshots.
- `runtime_callbacks.go` is the one-time pre-Start binding boundary to chain
  components. Bind independent owners through the named fields of
  `RuntimeCallbacks`; `Options` must not contain a second callback path. Never
  recreate a mega-interface implemented by one god object.
- `storage.go` owns P2P's two storage contracts. `PeerStorage` is the required
  peer-serving read boundary; `StateArtifactStorage` is the optional local
  state import/persistence boundary. Pass both explicitly from the composition
  root when one concrete store implements both. Do not add a full-storage
  fallback, merge competing options, or add `Close` to either contract.

## Extraction rule

Create a package under `internal` only when the extracted engine:

1. owns meaningful state and invariants;
2. depends on narrow inputs rather than `*p2p.Node`;
3. can be tested without constructing a node; and
4. does not require aliases, forwarding methods, or conversion glue.

Moving files alone is not a refactor. The following boundaries intentionally
remain in `p2p` until their invariants can be separated without callback bags,
lock exposure, protocol glue, or copied hot-path data:

- FastSync membership and peer-descriptor state are already isolated in
  `internal/fastsync`. The remaining overlay reconciliation still shares one
  `subscriptionsMx` transaction across the normal/FastSync maps, subscription
  state, and public broadcast-receiver generations. Extracting that
  orchestration would expose locks or introduce transaction closures.
- Peer selection and endpoint-use accounting share `peerUseMx`, pooled-peer
  lock ordering, live subscription state, and per-peer statistics. A generic
  ranking package would require snapshots/adapters and add allocations on
  download/query paths.
- Plumtree runtime owns and joins its loop, outbound workers, and repairs. Its
  wire-facing state still sends through live overlay subscriptions and QUIC
  paths and uses public P2P protocol types. Extracting it now would replace
  direct calls with a callback bag or conversion glue.
- FastSync overlay reconciliation atomically changes subscription membership
  and receiver generations, while certificate import still uses peer-pool and
  Node lifecycle. The synchronous core is independent; orchestration is not.
- ADNL/RLDP/QUIC transport callbacks terminate directly in detached-query,
  subscription, peer-pool, route, and inbound-drain state. Moving transport
  would force P2P protocol types and lifecycle callbacks across the boundary.
- Archive and state downloads share endpoint leases, peer statistics,
  subscriptions, storage/domain types, and node shutdown. Split those
  ownership transactions first; do not wrap the current Node calls.
- Bounded queues and payload caches stay package-local because their elements
  contain P2P types. Moving them would require aliases or conversions without
  producing an autonomous state owner.

## Protocol compatibility

- Types registered with `tl.Register` stay public.
- Do not move public TL types, add aliases, or create forwarding packages in a
  regular refactor. Do that only at an explicitly planned breaking boundary.
- Keep wire behavior compatible with `cppnode`; use it as the reference when a
  protocol decision is ambiguous.

## Extension boundary

- Core P2P code must not import extension packages or branch on extension
  implementations. Extensions may depend on the stable P2P API; dependency
  direction is always extension -> core.
- Bind optional extension behavior at the composition root through an existing
  narrow public contract. Do not add extension-specific fields, fallbacks, TL
  types, or lifecycle decisions to `Node`.

## Concurrency

- A state owner hides its locks and atomics behind semantic methods.
- Every spawned goroutine has an identifiable lifecycle owner and is joined
  before that owner's transports, sessions, or caches are closed.
- `plumtreeRuntime.Close` closes repair admission, cancels, joins its loop,
  outbound workers, and repairs, and only then closes the engine. Repairs must
  never be put back on `Node.runAsync`.
- `Node.BindRuntimeCallbacks` is one-shot and must finish before `Node.Start`.

## Hot paths

- Broadcast decode/dispatch, route lookup, peer selection, QUIC envelope
  handling, and Plumtree forwarding must remain allocation-stable.
- Do not copy broadcast payloads or replace direct engine calls with reflection
  or generic glue.
- Run affected benchmarks with `-benchmem -count=6`; a package extraction is
  not complete if it measurably regresses throughput or allocations.

## Verification

Run focused tests first:

```sh
go test ./service/p2p/internal/eventdedupe ./service/p2p/internal/fastsync ./service/p2p/internal/peerroute ./service/p2p -count=1
go test -race ./service/p2p/internal/eventdedupe ./service/p2p/internal/fastsync ./service/p2p/internal/peerroute ./service/p2p -count=1
go test ./service/p2p/internal/eventdedupe -run '^$' -bench '^BenchmarkCache' -benchmem -count=6
go test ./service/p2p/internal/fastsync -run '^$' -bench '^BenchmarkFastSync' -benchmem -count=6
go test ./service/p2p -run '^$' -bench '^BenchmarkPeerRouteTableGetHit$' -benchmem -count=6
```

Use the repository-wide test commands from the root guide before handing off
a cross-package change.
