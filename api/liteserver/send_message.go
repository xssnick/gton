package liteserver

import admission "github.com/xssnick/gton/service/externalmsg"

func (s *Server) newExternalMessageSender() (*admission.Sender, error) {
	return admission.NewSender(admission.SenderOptions{
		Check:          s.externalMessageCheck,
		Broadcast:      s.messageSender.SendCheckedExternalMessage,
		AllowDuplicate: s.allowDuplicateExternals,
		Now:            s.now,
		Cache:          s.sendMessageCache,
		Limiter:        s.externalMessageLimiter,
	})
}
