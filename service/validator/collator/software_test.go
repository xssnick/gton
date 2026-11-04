package collator

import "testing"

func TestSupportedSoftwareMatchesReferenceCollator(t *testing.T) {
	software := SupportedSoftware()
	if software.Version != 17 {
		t.Fatalf("supported version = %d, want 17", software.Version)
	}
	if software.Capabilities != 0x3ee {
		t.Fatalf("supported capabilities = %#x, want 0x3ee", software.Capabilities)
	}
}
