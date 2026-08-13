package msgpool

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/xssnick/tonutils-go/address"
)

// RoutingAddressBits is the (workchain, prefix) routing address width.
const RoutingAddressBits = 96

// AccountPrefix is the routable head of an account address: the workchain
// and the first 64 address bits (with anycast rewrite applied).
type AccountPrefix struct {
	Workchain int32
	Prefix    uint64
}

// ContainsPrefix reports whether the shard covers the account prefix.
func (s ShardIdent) ContainsPrefix(p AccountPrefix) bool {
	return s.Contains(p.Workchain, p.Prefix)
}

// QueueKey is one out-msg-queue dictionary key: the 32-bit next-hop
// workchain, the 64-bit next-hop address prefix and the 256-bit message
// hash.
type QueueKey [44]byte

// MakeQueueKey assembles the out-queue key of a message routed through the
// next hop.
func MakeQueueKey(nextHop AccountPrefix, messageHash [32]byte) QueueKey {
	var key QueueKey
	binary.BigEndian.PutUint32(key[:4], uint32(nextHop.Workchain))
	binary.BigEndian.PutUint64(key[4:12], nextHop.Prefix)
	copy(key[12:], messageHash[:])
	return key
}

// NextHop returns the next-hop prefix half of the key.
func (k QueueKey) NextHop() AccountPrefix {
	return AccountPrefix{
		Workchain: int32(binary.BigEndian.Uint32(k[:4])),
		Prefix:    binary.BigEndian.Uint64(k[4:12]),
	}
}

// MsgHash returns the message-hash half of the key.
func (k QueueKey) MsgHash() [32]byte {
	var hash [32]byte
	copy(hash[:], k[12:])
	return hash
}

// AccountPrefixFromAddress extracts the routable prefix of a message
// destination or source address.
func AccountPrefixFromAddress(addr *address.Address) (AccountPrefix, error) {
	switch addr.Type() {
	case address.StdAddress:
		if addr.BitsLen() != 256 || len(addr.Data()) != 32 {
			return AccountPrefix{}, errors.New("msgpool: malformed standard account address")
		}
	case address.VarAddress:
		workchain := addr.Workchain()
		if workchain == 0 || workchain == address.MasterchainID ||
			(workchain >= -128 && workchain <= 127 && addr.BitsLen() == 256) {
			return AccountPrefix{}, errors.New("msgpool: non-canonical variable account address")
		}
	default:
		return AccountPrefix{}, errors.New("msgpool: account address has no routable prefix")
	}
	if addr.BitsLen() < 64 || uint(len(addr.Data())*8) < addr.BitsLen() {
		return AccountPrefix{}, errors.New("msgpool: account address data is shorter than its bit length")
	}

	var prefixBytes [8]byte
	copy(prefixBytes[:], addr.Data()[:8])
	if err := RewriteAnycast(prefixBytes[:], addr); err != nil {
		return AccountPrefix{}, err
	}

	return AccountPrefix{
		Workchain: addr.Workchain(),
		Prefix:    binary.BigEndian.Uint64(prefixBytes[:]),
	}, nil
}

// RewriteAnycast applies the address anycast rewrite prefix to dst in place.
// A nil anycast is a no-op.
func RewriteAnycast(dst []byte, addr *address.Address) error {
	anycast := addr.Anycast()
	if anycast == nil {
		return nil
	}

	depth := anycast.Depth()
	if depth == 0 || depth > 30 {
		return fmt.Errorf("msgpool: invalid anycast depth %d", depth)
	}
	rewriteBytes := anycast.Prefix()
	rewriteLen := (depth + 7) / 8
	if uint(len(rewriteBytes)) != rewriteLen {
		return errors.New("msgpool: anycast prefix length does not match its depth")
	}

	wholeBytes := depth / 8
	copy(dst[:wholeBytes], rewriteBytes[:wholeBytes])
	if remaining := depth % 8; remaining != 0 {
		mask := byte(0xff << (8 - remaining))
		dst[wholeBytes] = dst[wholeBytes]&^mask | rewriteBytes[wholeBytes]&mask
	}
	return nil
}

// InterpolatePrefix rewrites the first dstBits bits of the source routing
// address with the destination's — the intermediate-address resolution of
// hypercube routing.
func InterpolatePrefix(src, dst AccountPrefix, dstBits int) AccountPrefix {
	switch {
	case dstBits <= 0:
		return src
	case dstBits >= RoutingAddressBits:
		return dst
	case dstBits >= 32:
		mask := ^uint64(0) >> (dstBits - 32)
		return AccountPrefix{
			Workchain: dst.Workchain,
			Prefix:    dst.Prefix&^mask | src.Prefix&mask,
		}
	default:
		mask := ^uint32(0) >> dstBits
		return AccountPrefix{
			Workchain: int32(uint32(dst.Workchain)&^mask | uint32(src.Workchain)&mask),
			Prefix:    src.Prefix,
		}
	}
}
