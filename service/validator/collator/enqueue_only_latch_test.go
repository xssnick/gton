package collator

import "testing"

// A block's generated messages are delivered in the same block until the
// normal limit fills, and enqueued after that. The reference keeps that
// decision per block and only ever raises it (collator.cpp:4843-4860); we used
// to recompute it per pass from blockFull, which escalateToMediumMark clears —
// so a pass could enqueue a self-directed message at lt L and a later pass
// deliver one immediately at lt L+2, carrying the ProcessedInfo claim past the
// enqueued message. Both validators refuse that block, ours included: on the
// stand it surfaced as a self-rejected candidate.
func TestEnqueueOnlyIsStickyAcrossNewMessagePasses(t *testing.T) {
	roomy := blockLimits{
		bytes:        limitThresholds{1, 1 << 30, 1 << 31, 1 << 32},
		gas:          limitThresholds{1, 1 << 30, 1 << 31, 1 << 32},
		ltDelta:      limitThresholds{1, 1 << 30, 1 << 31, 1 << 32},
		collatedData: limitThresholds{1, 1 << 30, 1 << 31, 1 << 32},
	}
	c := &collation{limits: newBlockLimitStatus(roomy, 0, nil, 0, 0)}
	if err := c.processNewMessages(false); err != nil {
		t.Fatal(err)
	}
	if c.enqueueOnlyLatched {
		t.Fatal("a delivering pass latched enqueue-only")
	}
	if err := c.processNewMessages(true); err != nil {
		t.Fatal(err)
	}
	if !c.enqueueOnlyLatched {
		t.Fatal("an enqueue-only pass did not latch")
	}
	// The mark being raised clears blockFull — the very sequence that produced
	// the self-rejected block — and the next pass must stay enqueue-only.
	c.blockFull = true
	c.escalateToMediumMark()
	if err := c.processNewMessages(c.blockFull); err != nil {
		t.Fatal(err)
	}
	if !c.enqueueOnlyLatched {
		t.Fatal("the latch was lowered by a later pass")
	}
}
