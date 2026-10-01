package extmsg

import (
	"errors"
	"fmt"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const maxBOCCells = 1 << 17

// ParseMessage decodes the transport representation of an external message.
// Admission and TVM policy belong to service/externalmsg and intentionally do
// not participate in this low-level wire-format boundary.
func ParseMessage(data []byte) (*cell.Cell, *tlb.ExternalMessage, error) {
	if len(data) == 0 {
		return nil, nil, errors.New("external message is empty")
	}

	root, err := cell.FromBOCWithOptions(data, cell.BOCParseOptions{
		MaxCells: maxBOCCells,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("cannot parse external message BOC: %w", err)
	}

	var msg tlb.ExternalMessage
	loader, err := root.BeginParse()
	if err != nil {
		return nil, nil, fmt.Errorf("cannot parse external message: %w", err)
	}
	magic, err := loader.PreloadUInt(2)
	if err != nil || magic != 0b10 {
		return nil, nil, errors.New("external message must begin with ext_in_msg_info$10")
	}
	if err = tlb.LoadFromCell(&msg, loader); err != nil {
		return nil, nil, fmt.Errorf("cannot parse external message: %w", err)
	}
	if msg.DstAddr == nil {
		return nil, nil, errors.New("external message has no destination address")
	}
	if msg.DstAddr.Type() != address.StdAddress || msg.DstAddr.BitsLen() != 256 {
		return nil, nil, errors.New("external message destination address is not a std 256-bit address")
	}

	return root, &msg, nil
}

func AddressKeyFor(addr *address.Address) AddressKey {
	key := AddressKey{Workchain: addr.Workchain()}
	copy(key.Account[:], addr.Data())
	return key
}
