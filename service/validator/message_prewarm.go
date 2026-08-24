package validator

import (
	"github.com/xssnick/gton/service/validator/msgpool"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

type pooledAccountPrewarmKey struct {
	workchain int32
	account   [32]byte
}

type pooledInternalsPrewarmSeen struct {
	accounts  map[pooledAccountPrewarmKey]struct{}
	envelopes map[cell.Hash]struct{}
}

// prewarmPooledInternals schedules full destination-account and exact message
// envelope record warming after messages enter a committed run. seen spans
// every routed destination of one source update, because overlapping shard
// views may share messages.
func (s *Service) prewarmPooledInternals(
	messages []*msgpool.InternalMessage,
	seen *pooledInternalsPrewarmSeen,
) {
	if s.accountPrewarmer == nil {
		return
	}

	for _, message := range messages {
		if !message.DestinationPrewarmable {
			continue
		}
		key := pooledAccountPrewarmKey{
			workchain: message.DestinationWorkchain,
			account:   message.DestinationAccount,
		}
		if _, exists := seen.accounts[key]; exists {
			continue
		}

		seen.accounts[key] = struct{}{}
		s.accountPrewarmer.EnqueueAccount(key.workchain, key.account)
	}

	for _, message := range messages {
		root := cell.Hash(message.EnvHash)
		if _, exists := seen.envelopes[root]; exists {
			continue
		}

		seen.envelopes[root] = struct{}{}
		s.accountPrewarmer.EnqueueRoot(root)
	}
}
