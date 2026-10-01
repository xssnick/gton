package collator

import (
	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
)

type prewarmAccountKey struct {
	workchain int32
	account   [32]byte
}

// pooledPrewarmLimit bounds how far into a freshly seeded run the prewarm
// hints reach: the configured capacity when there is one, otherwise a few
// blocks' worth of admissions.
func (a *LocalAcquisition) pooledPrewarmLimit() int {
	if a.accountPrewarmCapacity > 0 {
		return a.accountPrewarmCapacity
	}
	return pooledPrewarmDefaultLimit
}

// pooledPrewarmDefaultLimit is about three full blocks of inbound internals.
const pooledPrewarmDefaultLimit = 1024

// prewarmExternalInputs starts every valid local destination in the batch
// before the first external transaction executes. Pool admission already
// schedules the destination in the background; this immediate hint promotes
// pending work and resolves the current live root again when the account has
// advanced before this candidate.
func (c *collation) prewarmExternalInputs(inputs []ExternalInput) {
	if c.req.accountPrewarmer == nil {
		return
	}

	for i := range inputs {
		message := inputs[i].message.Message()
		if message.MsgType != tlb.MsgTypeExternalIn {
			continue
		}
		destination := message.AsExternalIn().DstAddr
		prefix, err := accountPrefixFromAddress(destination)
		if err != nil || !c.shard.ContainsPrefix(prefix) {
			continue
		}
		c.prewarmImmediateAccount(destination)
	}
}

// prewarmGeneratedOutputs starts cache work while the collator records the
// transaction descriptors. Only local outputs that may still execute in this
// block are useful this early; forced-enqueue and incomplete-input paths are
// deliberately left to their normal routing flow.
func (c *collation) prewarmGeneratedOutputs(result *tvm.TransactionExecutionResult) {
	if c.req.accountPrewarmer == nil || c.blockFull || c.haveUnprocessedDispatchQueue ||
		c.req.internalsIncomplete() {
		return
	}

	for _, output := range result.OutMessages {
		if output.Msg.MsgType != tlb.MsgTypeInternal {
			continue
		}

		destination := output.Msg.AsInternal().DstAddr
		prefix, err := accountPrefixFromAddress(destination)
		if err != nil || !c.shard.ContainsPrefix(prefix) {
			continue
		}
		c.prewarmImmediateAccount(destination)
	}
}

// prewarmImmediateAccount gives an external or generated same-block
// destination a priority hint, falling back to the normal bounded look-ahead
// when every immediate slot is occupied. Accounts already opened by this
// collation are resident in lanes and need no storage prewarm.
func (c *collation) prewarmImmediateAccount(destination *address.Address) {
	if c.req.accountPrewarmer == nil {
		return
	}

	account, err := accountIDFromAddress(destination)
	if err != nil {
		return
	}
	if c.lanes[account] != nil {
		return
	}

	key := prewarmAccountKey{workchain: destination.Workchain(), account: account}
	if _, exists := c.prewarmedAccounts[key]; exists {
		return
	}
	scheduled := c.req.accountPrewarmer.PrewarmAccountNow(key.workchain, key.account)
	if !scheduled {
		scheduled = c.req.accountPrewarmer.EnqueueAccount(key.workchain, key.account)
	}
	if !scheduled {
		return
	}
	if c.prewarmedAccounts == nil {
		c.prewarmedAccounts = make(map[prewarmAccountKey]struct{})
	}
	c.prewarmedAccounts[key] = struct{}{}
}
