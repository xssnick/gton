package liteserver

import (
	"context"
	"fmt"

	"github.com/xssnick/gton/internal/extmsg"
	admission "github.com/xssnick/gton/service/externalmsg"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	maxExternalMessageBroadcastDataSize = 16 << 20
)

func (s *Server) checkExternalMessage(ctx context.Context, data []byte, msgCell *cell.Cell, msg *tlb.ExternalMessage) (admission.CheckResult, error) {
	if s.messageChecker != nil {
		return s.messageChecker.CheckExternalMessage(ctx, data, msgCell, msg)
	}
	return s.externalMessageChecker.Check(ctx, data, msgCell, msg)
}

func (s *Server) checkExternalMessageAddressLimit(key extmsg.AddressKey) error {
	return externalMessageAddressLimitError(key, s.externalMessageLimiter.Check(key, s.now()))
}

func (s *Server) addExternalMessageAddressLimit(key extmsg.AddressKey) error {
	return externalMessageAddressLimitError(key, s.externalMessageLimiter.Add(key, s.now()))
}

func (s *Server) dropExternalMessageAddressLimit(key extmsg.AddressKey) {
	s.externalMessageLimiter.Remove(key, s.now())
}

func externalMessageAddressKey(addr *address.Address) extmsg.AddressKey {
	key := extmsg.AddressKey{Workchain: addr.Workchain()}
	copy(key.Account[:], addr.Data())
	return key
}

func externalMessageAddressLimitError(key extmsg.AddressKey, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w %d:%x", err, key.Workchain, key.Account[:])
}
