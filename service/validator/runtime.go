package validator

import (
	"sync"

	"github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/msgpool"
)

var _ collator.LocalGroupSource = (*GroupTracker)(nil)
var _ collator.GroupTracker = (*GroupTracker)(nil)
var _ collator.LocalMessageSource = (*msgpool.Pool)(nil)

// SharedRuntimeOptions configures one validator or standalone-collator
// workflow's shared group and message state.
type SharedRuntimeOptions struct {
	Groups   groups.TrackerOptions
	Messages msgpool.Config
}

// Runtime owns group and message resources shared inside one workflow. A
// validator and its local collator share one Runtime; an independent
// standalone collator receives another. Its lifecycle belongs to the
// composition root, after all consumers have stopped.
type Runtime struct {
	Groups   *GroupTracker
	Messages *msgpool.Pool

	closeOnce sync.Once
}

// NewRuntime constructs shared group and message state. The group tracker is
// a stable handle: it buffers masterchain events until the first consumer
// completes bootstrap and publishes the concrete groups.Tracker behind it.
func NewRuntime(options SharedRuntimeOptions) (*Runtime, error) {
	tracker, err := newGroupTracker(options.Groups)
	if err != nil {
		return nil, err
	}

	return &Runtime{
		Groups:   tracker,
		Messages: msgpool.New(options.Messages),
	}, nil
}

// Close stops resources owned by Runtime. Consumers must already be stopped.
func (r *Runtime) Close() {
	r.closeOnce.Do(r.Messages.Close)
}
