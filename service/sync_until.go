package service

import (
	"context"
	"fmt"

	"github.com/xssnick/gton/service/storage"
)

func (s *Service) syncUntilEnabled() bool {
	return s.syncUntil != 0
}

func (s *Service) syncUntilFrozen() bool {
	return s.syncUntilEnabled() && s.node.IsOffline()
}

func (s *Service) checkCurrentBeforeSyncUntil(ctx context.Context, current *storage.CurrentState) error {
	if !s.syncUntilEnabled() || current == nil {
		return nil
	}

	utime := blockStateUtime(ctx, s.storage, &current.Masterchain)
	if utime == 0 || utime <= int64(s.syncUntil) {
		return nil
	}
	return fmt.Errorf(
		"current masterchain block is newer than ton.sync_until: current=%s current_utime=%d sync_until=%d",
		storage.FormatBlockRef(current.Masterchain.Block),
		utime,
		s.syncUntil,
	)
}

func (s *Service) preparedMasterBlockAfterSyncUntil(block PreparedBlock) bool {
	if !s.syncUntilEnabled() {
		return false
	}
	return block.Meta.GenUTime > s.syncUntil
}

func (s *Service) enterSyncUntilOffline(current *storage.CurrentState, next PreparedBlock) {
	event := s.log.Info().
		Uint32("sync_until", s.syncUntil)
	if current != nil {
		event.Str("current", storage.FormatBlockRef(current.Masterchain.Block))
	}
	if next.Meta != nil {
		event.
			Str("next", storage.FormatBlockRef(next.ID)).
			Uint32("next_utime", next.Meta.GenUTime)
	}
	event.Msg("sync_until reached, switching p2p node offline")

	reason := fmt.Sprintf("ton.sync_until=%d reached", s.syncUntil)
	if current != nil {
		reason = fmt.Sprintf("ton.sync_until=%d reached at %s", s.syncUntil, storage.FormatBlockRef(current.Masterchain.Block))
	}
	if next.Meta != nil {
		reason = fmt.Sprintf("ton.sync_until=%d reached before %s utime=%d", s.syncUntil, storage.FormatBlockRef(next.ID), next.Meta.GenUTime)
	}
	s.node.EnterOffline(reason)
	s.wakeServiceMaintenance()
}
