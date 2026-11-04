package validator

import (
	"context"
	"errors"
	"time"

	"github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/simplex"
)

var errDelegationPreparationUnavailable = errors.New("delegation preparation unavailable")

type firstDelegationBackend interface {
	scheduleFirstDelegation(context.Context)
}

type delegationDelivery struct {
	cancel context.CancelFunc
}

// scheduleFirstDelegation runs only for a journal which has not announced its
// first window. The prepared backend owns this work until Close, including
// before the consensus runtime is activated.
func (b *LocalSessionBackend) scheduleFirstDelegation(ctx context.Context) {
	b.controlMu.Lock()
	defer b.controlMu.Unlock()
	if b.closed.Load() || b.productionMode != collator.ProductionModeDelegated || b.validator == nil || b.validator.Index != 0 {
		return
	}
	if _, exists := b.deliveries[0]; exists {
		return
	}
	workerCtx, delivery := b.newDelegationDeliveryLocked(ctx, 0)
	b.deliveryWorkers.Add(1)
	go func() {
		defer b.deliveryWorkers.Done()
		defer b.finishDelegationDelivery(0, delivery)
		if !b.config.ExpectedStart.IsZero() {
			if !waitDelegationRetry(workerCtx, b.config.ExpectedStart.Add(-5*time.Second)) {
				return
			}
		}
		b.prepareAndDeliverDelegation(workerCtx, 0)
	}()
}

// newDelegationDeliveryLocked creates a backend-owned context. A caller's
// preparation/window deadline must not terminate already durable authority.
func (b *LocalSessionBackend) newDelegationDeliveryLocked(ctx context.Context, start uint32) (context.Context, *delegationDelivery) {
	if b.deliveries == nil {
		b.deliveries = make(map[uint32]*delegationDelivery)
	}
	workerCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	delivery := &delegationDelivery{cancel: cancel}
	b.deliveries[start] = delivery
	return workerCtx, delivery
}

func (b *LocalSessionBackend) retryDelegationLocked(ctx context.Context, authorization DelegationAuthorization, nextAttempt time.Time) {
	if _, exists := b.deliveries[authorization.StartSlot]; exists {
		return
	}
	workerCtx, delivery := b.newDelegationDeliveryLocked(ctx, authorization.StartSlot)
	b.deliveryWorkers.Add(1)
	go func() {
		defer b.deliveryWorkers.Done()
		defer b.finishDelegationDelivery(authorization.StartSlot, delivery)
		if waitDelegationRetry(workerCtx, nextAttempt) {
			b.deliverDelegation(workerCtx, authorization)
		}
	}()
}

func (b *LocalSessionBackend) retryDelegationPreparationLocked(ctx context.Context, start uint32, nextAttempt time.Time) {
	if _, exists := b.deliveries[start]; exists {
		return
	}
	workerCtx, delivery := b.newDelegationDeliveryLocked(ctx, start)
	b.deliveryWorkers.Add(1)
	go func() {
		defer b.deliveryWorkers.Done()
		defer b.finishDelegationDelivery(start, delivery)
		if waitDelegationRetry(workerCtx, nextAttempt) {
			b.prepareAndDeliverDelegation(workerCtx, start)
		}
	}()
}

func (b *LocalSessionBackend) prepareAndDeliverDelegation(ctx context.Context, start uint32) {
	for ctx.Err() == nil {
		b.controlMu.Lock()
		started := b.closed.Load() || (b.update.HasCurrentWindow && b.update.CurrentWindowStart >= start)
		b.controlMu.Unlock()
		if started {
			return
		}
		nextAttempt := time.Now().Add(delegationDeliveryTimeout)
		prepareCtx, cancel := context.WithDeadline(ctx, nextAttempt)
		authorization, err := b.delegationAuthorization(prepareCtx, start)
		cancel()
		if err == nil {
			b.deliverDelegation(ctx, authorization)
			return
		}
		if !errors.Is(err, errDelegationPreparationUnavailable) {
			if ctx.Err() == nil {
				b.log.Error().Err(err).Uint32("window", start).Msg("failed to prepare delegation authority")
			}
			return
		}
		if !waitDelegationRetry(ctx, nextAttempt) {
			return
		}
	}
}

func (b *LocalSessionBackend) finishDelegationDelivery(start uint32, delivery *delegationDelivery) {
	delivery.cancel()
	b.controlMu.Lock()
	if b.deliveries[start] == delivery {
		delete(b.deliveries, start)
	}
	b.controlMu.Unlock()
}

func (b *LocalSessionBackend) cancelStartedDelegationsLocked(start uint32) {
	for slot, delivery := range b.deliveries {
		if slot <= start {
			delivery.cancel()
		}
	}
}

func (b *LocalSessionBackend) deliverDelegation(ctx context.Context, authorization DelegationAuthorization) {
	for ctx.Err() == nil {
		b.controlMu.Lock()
		started := b.closed.Load() || (b.update.HasCurrentWindow && b.update.CurrentWindowStart >= authorization.StartSlot)
		b.controlMu.Unlock()
		if started {
			return
		}
		nextAttempt := time.Now().Add(delegationDeliveryTimeout)
		if err := b.commitDelegation(ctx, authorization); err == nil || errors.Is(err, collator.ErrAlreadyDelegated) {
			return
		}
		if !waitDelegationRetry(ctx, nextAttempt) {
			return
		}
	}
}

func (b *LocalSessionBackend) commitDelegation(ctx context.Context, authorization DelegationAuthorization) error {
	deliveryCtx, cancel := context.WithTimeout(ctx, delegationDeliveryTimeout)
	defer cancel()
	return b.collator.CommitDelegation(deliveryCtx, collator.WindowRequest{
		SessionID:  b.config.SessionID,
		SourceADNL: b.config.Identity.ADNLID,
		PleaseCollate: simplex.ConsensusPleaseCollate{
			WindowStartSlot: int32(authorization.StartSlot),
			Signature:       authorization.Signature,
		},
	})
}

func waitDelegationRetry(ctx context.Context, until time.Time) bool {
	timer := time.NewTimer(max(time.Until(until), 0))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return ctx.Err() == nil
	}
}
