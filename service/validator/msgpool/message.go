package msgpool

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// ShardIdent identifies a shard by workchain and marked shard prefix.
type ShardIdent struct {
	Workchain int32
	Shard     uint64
}

// ShardAll is the full-workchain shard id.
const ShardAll = uint64(1) << 63

// Contains reports whether the shard covers the given account prefix.
func (s ShardIdent) Contains(workchain int32, prefix uint64) bool {
	if s.Workchain != workchain {
		return false
	}
	low := s.Shard & (^s.Shard + 1)
	mask := ^(2*low - 1) // wraps to "cover all" for ShardAll
	return (prefix^s.Shard)&mask == 0
}

// ExternalRef identifies one selection generation of a pooled message.
// Feedback for an older generation is ignored after another collation result
// has already advanced the message.
type ExternalRef struct {
	Hash       [32]byte
	Generation uint64
}

// ExternalOutcome describes what happened to a selected external during
// collation.
type ExternalOutcome uint8

const (
	// ExternalIncluded was accepted into a candidate. It stays pooled until an
	// applied block proves inclusion.
	ExternalIncluded ExternalOutcome = iota + 1
	// ExternalInvalid cannot be retried and is removed immediately.
	ExternalInvalid
	// ExternalNotAccepted was rejected by the destination account. It is
	// delayed and may be offered again in a later generation.
	ExternalNotAccepted
	// ExternalSkippedLimit was not executed because the candidate hit a limit.
	ExternalSkippedLimit
)

// ExternalFeedback binds a collation outcome to the exact selection
// generation that produced it.
type ExternalFeedback struct {
	Ref     ExternalRef
	Outcome ExternalOutcome
}

// ExternalSnapshot is the read-only view handed to a collator. Hash is a
// value for convenient routing and result correlation; the remaining payload
// is exposed through accessors so callers cannot mutate the pooled record.
type ExternalSnapshot struct {
	Hash [32]byte

	message    *ExternalMessage
	generation uint64
	expiresAt  int64
}

// Reference returns the immutable identity to use in Complete feedback.
func (m ExternalSnapshot) Reference() ExternalRef {
	return ExternalRef{Hash: m.message.Hash, Generation: m.generation}
}

// Root returns the message root. Cell trees are treated as immutable
// throughout the validator.
func (m ExternalSnapshot) Root() *cell.Cell { return m.message.Root }

// Parsed returns an owned decoded ext-in header. It is rebuilt from the root
// so mutable address and state-init objects cannot alias pool state.
func (m ExternalSnapshot) Parsed() (*tlb.ExternalMessage, error) {
	return parseExtIn(m.message.Root)
}

// NormalizedHash returns the hash used by applied-block cleanup.
func (m ExternalSnapshot) NormalizedHash() [32]byte { return m.message.HashNorm }

// Workchain returns the destination workchain.
func (m ExternalSnapshot) Workchain() int32 { return m.message.Workchain }

// Address returns the destination account address.
func (m ExternalSnapshot) Address() [32]byte { return m.message.Addr }

// AddressPrefix returns the first 64 address bits used for shard routing.
func (m ExternalSnapshot) AddressPrefix() uint64 { return m.message.AddrPrefix }

// ExternalMessage is the pool-owned representation of an external message.
type ExternalMessage struct {
	// Size is the serialized length of the message, the only quantity the
	// memory budget needs. The received BOC itself is not retained: the cell
	// tree is the authoritative payload and everything downstream — collation
	// included — reads Root.
	Size int
	// Root is the message root cell.
	Root *cell.Cell

	// Hash is the representation hash of the root cell. Complete feedback uses
	// Reference so it is scoped to the selected generation.
	Hash [32]byte
	// HashNorm is the normalized message hash used by applied-block cleanup.
	HashNorm [32]byte

	// Workchain and Addr identify the destination account.
	Workchain int32
	Addr      [32]byte
	// AddrPrefix is the first 64 bits of the destination address, used for
	// shard routing.
	AddrPrefix uint64
}

// addrKey identifies a destination account.
type addrKey struct {
	wc   int32
	addr [32]byte
}

func (m *ExternalMessage) key() addrKey {
	return addrKey{wc: m.Workchain, addr: m.Addr}
}

func (m *ExternalMessage) snapshot(generation uint64, expiresAt int64) ExternalSnapshot {
	return ExternalSnapshot{
		Hash:       m.Hash,
		message:    m,
		generation: generation,
		expiresAt:  expiresAt,
	}
}

// newExternalMessage assembles the pooled view of a message the ingress layer
// already deserialized (and emulated): routing fields and the normalized
// hash. parsed may be nil, then the header is decoded from root — a cheap
// slice walk, no BOC work. size is the received serialized length; 0 means the
// caller does not have it and the budget figure is estimated from the tree.
func newExternalMessage(size int, root *cell.Cell, parsed *tlb.ExternalMessage) (*ExternalMessage, error) {
	if parsed == nil {
		var err error
		if parsed, err = parseExtIn(root); err != nil {
			return nil, err
		}
	} else if err := checkExtIn(parsed); err != nil {
		return nil, err
	}

	if size == 0 {
		size = estimateBOCSize(root)
	}
	m := &ExternalMessage{
		Size:      size,
		Root:      root,
		Workchain: parsed.DstAddr.Workchain(),
	}
	m.Hash = root.HashKey()
	copy(m.Addr[:], parsed.DstAddr.Data())
	if err := RewriteAnycast(m.Addr[:], parsed.DstAddr); err != nil {
		return nil, err
	}
	m.AddrPrefix = binary.BigEndian.Uint64(m.Addr[:8])
	copy(m.HashNorm[:], parsed.NormalizedHash())

	return m, nil
}

// estimateBOCSize approximates the serialized length of a resident cell tree
// without serializing it: every unique cell costs its two descriptor bytes,
// its data bytes and one index byte per reference. The number only feeds the
// mempool byte budget — a heuristic admission cap — so the BOC header and the
// exact index width are not worth a ToBOC round trip and the copy it returns.
// Uniqueness is by cell identity, which is what deserialization produces for a
// shared subtree, so a wide DAG cannot make the walk blow up.
func estimateBOCSize(root *cell.Cell) int {
	visited := map[*cell.Cell]struct{}{}
	stack := []*cell.Cell{root}
	total := 0
	for len(stack) > 0 {
		current := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if _, seen := visited[current]; seen {
			continue
		}
		visited[current] = struct{}{}

		refs := int(current.RefsNum())
		total += 2 + int((current.BitsSize()+7)/8) + refs
		for index := range refs {
			ref, err := current.PeekRef(index)
			if err != nil {
				// An unresolvable reference cannot be charged for; the budget
				// is a heuristic and assembly fails elsewhere on real garbage.
				continue
			}
			stack = append(stack, ref)
		}
	}

	return total
}

func checkExtIn(parsed *tlb.ExternalMessage) error {
	// The destination must be a plain std address so the normalized-hash
	// rebuild serializes it back bit-for-bit.
	if parsed.DstAddr.Type() != address.StdAddress || parsed.DstAddr.BitsLen() != 256 {
		return errors.New("msgpool: external message destination is not a std 256-bit address")
	}
	return nil
}

// parseExtIn decodes and validates the ext_in_msg_info header of a message
// cell.
func parseExtIn(root *cell.Cell) (*tlb.ExternalMessage, error) {
	loader, err := root.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("msgpool: cannot parse external message: %w", err)
	}
	magic, err := loader.PreloadUInt(2)
	if err != nil || magic != 0b10 {
		return nil, errors.New("msgpool: external message must begin with ext_in_msg_info$10")
	}
	var parsed tlb.ExternalMessage
	if err = tlb.LoadFromCell(&parsed, loader); err != nil {
		return nil, fmt.Errorf("msgpool: cannot parse external message: %w", err)
	}
	if err = checkExtIn(&parsed); err != nil {
		return nil, err
	}
	return &parsed, nil
}

// normHashOfMessageCell computes the normalized hash of an ext-in message
// cell (applied-block cleanup over InMsgDescr entries).
func normHashOfMessageCell(root *cell.Cell) ([32]byte, error) {
	parsed, err := parseExtIn(root)
	if err != nil {
		return [32]byte{}, err
	}

	var out [32]byte
	copy(out[:], parsed.NormalizedHash())
	return out, nil
}
