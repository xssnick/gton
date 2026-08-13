package collator

import (
	"fmt"
	"math/bits"

	"github.com/xssnick/gton/service/validator/msgpool"

	"github.com/xssnick/tonutils-go/address"
)

const routingAddressBits = msgpool.RoutingAddressBits

func accountPrefixFromAddress(addr *address.Address) (msgpool.AccountPrefix, error) {
	prefix, err := msgpool.AccountPrefixFromAddress(addr)
	if err != nil {
		return msgpool.AccountPrefix{}, fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}
	return prefix, nil
}

func accountIDFromAddress(addr *address.Address) ([32]byte, error) {
	if addr.Type() != address.StdAddress || addr.BitsLen() != 256 || len(addr.Data()) != 32 {
		return [32]byte{}, fmt.Errorf("%w: account address is not a canonical standard address", ErrInvalidInput)
	}

	var accountID [32]byte
	copy(accountID[:], addr.Data())
	if err := msgpool.RewriteAnycast(accountID[:], addr); err != nil {
		return [32]byte{}, fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}
	return accountID, nil
}

func performHypercubeRouting(src, dst msgpool.AccountPrefix, current msgpool.ShardIdent, usedDstBits int) (int, int, error) {
	transit := msgpool.InterpolatePrefix(src, dst, usedDstBits)
	if !current.ContainsPrefix(transit) {
		return 0, 0, fmt.Errorf("%w: transit address is outside the current shard", ErrInvalidInput)
	}
	if current.ContainsPrefix(dst) {
		return routingAddressBits, routingAddressBits, nil
	}
	if transit.Workchain == address.MasterchainID || dst.Workchain == address.MasterchainID {
		return usedDstBits, routingAddressBits, nil
	}
	if transit.Workchain != dst.Workchain {
		return usedDstBits, 32, nil
	}

	lower := current.Shard & (current.Shard - 1)
	upper := current.Shard | (current.Shard - 1)
	diff := dst.Prefix ^ transit.Prefix
	matching := bits.LeadingZeros64(diff) &^ 3
	mask := ^uint64(0) >> matching
	// Route in four-bit hops until the next hop leaves the current shard.
	for {
		mask >>= 4
		hop := transit.Prefix ^ (diff &^ mask)
		matching += 4
		if hop < lower || hop > upper {
			break
		}
	}

	return 28 + matching, 32 + matching, nil
}
