package liteserver

import (
	"context"

	admission "github.com/xssnick/gton/service/externalmsg"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	maxExternalMessageBroadcastDataSize = admission.MaxBroadcastDataSize
)

func (s *Server) checkExternalMessage(ctx context.Context, data []byte, msgCell *cell.Cell, msg *tlb.ExternalMessage) (admission.CheckResult, error) {
	if s.messageChecker != nil {
		return s.messageChecker.CheckExternalMessage(ctx, data, msgCell, msg)
	}
	return s.externalMessageChecker.Check(ctx, data, msgCell, msg)
}

func (s *Server) newExternalMessageSender() (*admission.Sender, error) {
	return admission.NewSender(admission.SenderOptions{
		Check:          s.checkExternalMessage,
		Broadcast:      s.messageSender.SendCheckedExternalMessage,
		AllowDuplicate: s.allowDuplicateExternals,
		Now:            s.now,
		Cache:          s.sendMessageCache,
		Limiter:        s.externalMessageLimiter,
	})
}
