// Package msgpool accumulates the messages a collator feeds into new
// blocks.
//
// The pool is organized in two sections with different truth models:
//
//   - Externals (implemented): user messages validated by the ingress
//     layers. AddExternal accumulates them, SelectForBlock hands a bounded
//     batch in collation order (priority levels descending, fair seeded
//     ordering within a level).
//     The concrete set is node-local by design: the protocol only verifies
//     that included externals are valid, and different nodes may pick
//     different sets.
//
//   - Internals (fed by Feed from applied blocks, in both the validator
//     and the standalone-collator deployment): a derived,
//     pre-ordered view of the inbound internal message stream — per-source
//     out-queue runs advanced by finalized block deltas or reseeded from
//     state, extended past finality by
//     speculative candidate overlays, and served by Cut only for an exact
//     collation context (visible source positions, candidate tip,
//     processed skip) in canonical (lt, hash) import order.
//     The view is exact by construction (seeds read the state, deltas are
//     consensus-validated block transformations); the only waiting is the
//     readiness barrier and the only recovery is a reseed from state. The
//     full-queue-size counter cross-checked against the size states store
//     detects construction bugs at runtime; rebuild-equivalence tests
//     (seed(prev)+delta == seed(cur)) pin the construction down in CI.
//
// Deliberately out of scope: admission (TVM accept emulation and size
// limits run on the ingress layers before AddExternal), re-broadcast
// concerns, transaction execution (messages born inside the block being
// collated cannot exist ahead of it; the gas-driven cut belongs to the
// collator). Every pool operation is one short critical section.
//
// Unlike the Simplex journal, the pool is RAM-only: externals are
// re-delivered by the network after a restart, internals are re-derived
// from state.
package msgpool

import (
	"errors"
	"time"

	"github.com/rs/zerolog"
)

const (
	DefaultMempoolLimit                   = 8192
	DefaultMempoolBytesLimit              = 256 << 20
	DefaultPerAddressLimit                = 256
	DefaultTTL                            = 10 * time.Minute
	DefaultIncludedRetryDelay             = 10 * time.Second
	DefaultAccountRejectRetryDelay        = time.Second
	DefaultAccountRejectRetryLimit uint32 = 3
)

// Config assembles a Pool.
type Config struct {
	// MempoolLimit caps pooled messages per priority level.
	MempoolLimit int
	// MempoolBytesLimit caps the total raw size of pooled messages across
	// all priorities; 0 uses 256 MiB.
	MempoolBytesLimit int64
	// PerAddressLimit caps pooled messages per destination and priority.
	PerAddressLimit int

	// TTL is the pooled-message lifetime; a non-positive value uses 10
	// minutes. Idle background expiry runs for the built-in SystemClock;
	// injected clocks are reclaimed on pool operations so deterministic
	// clocks do not need wall-time goroutines.
	TTL time.Duration
	// IncludedRetryDelay is how long an external accepted into a candidate
	// stays unavailable while that candidate either reaches the applied chain
	// or loses. Zero uses ten seconds.
	IncludedRetryDelay time.Duration
	// AccountRejectRetryDelay is how long an account-rejected external stays
	// unavailable before the pool offers it to a collator again. Zero uses one
	// second.
	AccountRejectRetryDelay time.Duration
	// AccountRejectRetryLimit bounds delayed retries during one pool lifetime.
	// Zero uses three retries after the initial attempt.
	AccountRejectRetryLimit uint32

	// Clock is optional; nil means the system clock.
	Clock Clock
	// Logger is optional; nil discards logs.
	Logger *zerolog.Logger
}

func (c *Config) applyDefaults() {
	if c.MempoolLimit == 0 {
		c.MempoolLimit = DefaultMempoolLimit
	}
	if c.MempoolBytesLimit == 0 {
		c.MempoolBytesLimit = DefaultMempoolBytesLimit
	}
	if c.PerAddressLimit == 0 {
		c.PerAddressLimit = DefaultPerAddressLimit
	}
	if c.TTL <= 0 {
		c.TTL = DefaultTTL
	}
	if c.IncludedRetryDelay <= 0 {
		c.IncludedRetryDelay = DefaultIncludedRetryDelay
	}
	if c.AccountRejectRetryDelay == 0 {
		c.AccountRejectRetryDelay = DefaultAccountRejectRetryDelay
	}
	if c.AccountRejectRetryLimit == 0 {
		c.AccountRejectRetryLimit = DefaultAccountRejectRetryLimit
	}
	if c.Clock == nil {
		c.Clock = SystemClock{}
	}
	if c.Logger == nil {
		nop := zerolog.Nop()
		c.Logger = &nop
	}
}

// Clock abstracts time for deterministic tests.
type Clock interface {
	Now() time.Time
}

// SystemClock uses time.Now.
type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now() }

var (
	// ErrClosed rejects operations on a closed pool.
	ErrClosed = errors.New("msgpool: pool is closed")
	// ErrExternalCapacity reports that an otherwise valid external did not fit
	// one of the pool's count, byte, or per-address admission limits.
	ErrExternalCapacity = errors.New("msgpool: external capacity exceeded")
	// ErrInvalidExternalSize rejects a missing or invalid serialized BOC size.
	ErrInvalidExternalSize = errors.New("msgpool: invalid external serialized size")
	// ErrInvalidExternalOutcome rejects feedback the pool cannot interpret.
	ErrInvalidExternalOutcome = errors.New("msgpool: invalid external outcome")
)

// Stats is a snapshot of the pool counters.
type Stats struct {
	// DedupSkipped counts submissions answered from the raw-hash index.
	DedupSkipped  uint64
	PriorityBumps uint64

	Added           uint64
	OverflowMempool uint64
	OverflowBytes   uint64
	OverflowAddress uint64
	Expired         uint64

	InvalidDeleted      uint64
	IncludedQuarantined uint64
	IncludedReleased    uint64
	RejectedDelayed     uint64
	RejectedRetried     uint64
	RejectedExhausted   uint64
	RejectedPressure    uint64
	StaleFeedback       uint64

	AppliedRequested uint64
	AppliedDeleted   uint64

	Pooled      int   // current message count across priorities
	PooledBytes int64 // current serialized message bytes across priorities
}
