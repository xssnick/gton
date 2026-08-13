package collator

import "testing"

// These are the collator options every node is expected to run with; drifting
// from them changes how fast DispatchQueue drains relative to other collators
// in the same shard.
func TestReferenceDispatchPolicyMatchesReferenceOptions(t *testing.T) {
	policy := ReferenceDispatchPolicy()
	if !policy.DeferringEnabled {
		t.Fatal("deferring is disabled")
	}
	if policy.DeferMessagesAfter != 10 {
		t.Fatalf("defer messages after = %d, want 10", policy.DeferMessagesAfter)
	}
	if policy.DeferOutQueueSizeLimit != 2048 {
		t.Fatalf("defer out queue size limit = %d, want 2048", policy.DeferOutQueueSizeLimit)
	}
	if policy.Phase2MaxTotal != 150 || policy.Phase2MaxPerInitiator != 20 {
		t.Fatalf("phase 2 limits = %d/%d, want 150/20", policy.Phase2MaxTotal, policy.Phase2MaxPerInitiator)
	}
	if policy.Phase3MaxTotal != 150 {
		t.Fatalf("phase 3 total = %d, want 150", policy.Phase3MaxTotal)
	}
	if !policy.Phase3AdaptivePerInitiator || policy.Phase3MaxPerInitiator != 0 {
		t.Fatal("phase 3 per-initiator limit is not the canonical queue-size rule")
	}

	request := collationRequest{randSeed: [32]byte{1}, dispatch: policy}
	if err := validateCollationRequest(&request); err != nil {
		t.Fatalf("reference policy rejected by request validation: %v", err)
	}
}
