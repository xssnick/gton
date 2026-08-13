# Service package guide

This file supplements the repository root `AGENTS.md` for changes under
`service`.

## Dependency direction

Core service code may expose narrow contracts for integrations, but it must
not import extension implementations or their released SDK:

```text
api/*, service/validator, external extensions
                    -> composition root / service contracts
                    -> core service, p2p, state, storage
```

The released `service/hooks` package is an external SDK. Conversion between
its event types and core event types belongs in the module composition root
(`extensions.go`), never in this package. Built-in API servers are ordinary
root-owned servers, not extensions.

Keep this direction explicit in package APIs and code review; do not introduce
core imports of extension packages to make application wiring compile.

## Component ownership

- `StatusTracker` owns current/applied status snapshots, recent TPS, and lazy
  cell-load counters. It owns and joins its TPS goroutine.
- `BroadcastAdmission` owns the live-versus-flushed circuit breaker and has no
  goroutines.
- `StateLifecycle` owns durable-state serialization, checkpoint gates,
  prewriters, cell loaders, cell generations, and their goroutines.
- `MaintenanceRunner` owns maintenance scheduling, its graceful-shutdown
  context, and exclusive background task policy.
- `ArchiveRunner` owns archive catch-up/import session workflow; a per-run type
  owns scoped workers and joins them before closing its session/importer.
- `blocksync.Service` owns broadcast ingestion, gap filling, its chain workers,
  and the goroutine that runs them. The composition root starts and joins it
  directly.
- `SyncCoordinator` consumes the block stream and owns the transition pipeline,
  current-state publication, shard preparation, and overlay reconciliation.

Components are stored in named fields. Do not use embedding or method
promotion to conceal cross-component access. A component must not retain a
pointer to the old aggregate service. At a real construction cycle, use a
one-shot binding of consumer-owned narrow interfaces before Start.

## API placement

- Put a method on the state owner whose invariant it changes.
- Cross-component operations use named interfaces defined beside the
  consumer. Split them by workflow; do not create another broad `serviceCore`
  interface.
- Do not leave forwarding methods on an aggregate type for status,
  maintenance, serialization, migration, or admission. The composition root
  passes the owning component directly to P2P, metrics, and console code.
- Absence is `(value, error)` with `storage.ErrNotFound`, never
  `(value, ok, error)`.

## Storage and shard boundaries

- Define storage interfaces beside the consumer. P2P peer serving, state
  serialization, cell-generation rotation, HTTP reads, and storage lifecycle
  are separate contracts; do not pass `storage.Storage` merely because the
  Pebble implementation satisfies every one of them.
- Select cells once with `ActiveCells` or `Cells(generation)`, then keep the
  returned `storage.CellGeneration` for that workflow. Exact generations are
  always non-zero; never reintroduce zero as a hidden "active" sentinel.
- Storage `Close` belongs to the composition root and concrete store. Do not
  add lifecycle ownership to consumer read/write interfaces.
- `service/shard` owns raw TON shard-ID validation, prefix length,
  containment, intersection, parent/child, and prefix normalization. Shard
  zero is invalid. Consumers layer workchain and protocol-specific maximum
  depth policy above the domain package instead of duplicating `bits` or TLB
  shard arithmetic.

## Goroutine lifecycle

- `Start` is idempotent per component.
- `Wait` joins only goroutines owned by that component.
- `blocksync.Service` is started and joined directly by the composition root;
  do not launch `blocksync.Run` through `SyncCoordinator.runAsync`.
- A scoped archive/next-sync/migration run cancels and joins its workers before
  releasing the data, session, importer, or store generation they use.
- Test-only scheduling controls and hooks belong in `_test.go`, not production
  structs.

## Hot-path budget

- Treat broadcast decode, peer routing, block prepare/apply, state-cell lookup,
  next-sync, and checkpoint staging as performance contracts.
- Component extraction may add a direct pointer dereference, but must not add
  payload copies, per-block heap leases, reflection, closure allocation, or
  interface dispatch inside per-cell loops.
- Share immutable BOCs, cell trees, state metadata, and slices through internal
  flows. Copy only at an ownership or mutation boundary.
- Measure changed hot paths with `-benchmem` using multiple runs. Preserve
  allocation counts unless a measured tradeoff is explicitly justified.

## Physical package splits

Keep component extraction inside package `service` until its contracts no
longer mention parent-only types such as `PreparedBlock` or internal caches.
Only then move a whole workflow into `blocksync`, `archive`, or `state`. Do not
export internals, add aliases, or create forwarding packages merely to move
files.

## Verification

Run the narrow owner tests while editing, then:

```sh
go test ./service -count=1
go test ./service/p2p/internal/peerroute ./service/p2p -count=1
go test ./... -count=1
go test ./service ./service/p2p -run '^$' -bench . -benchmem -count=6
```

Use `-race` for any component whose locks, atomics, queues, or goroutine
ownership changed.
