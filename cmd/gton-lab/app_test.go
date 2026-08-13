package main

import (
	"bytes"
	"context"
	"testing"
)

func TestRunCLIUsageErrors(t *testing.T) {
	var output bytes.Buffer
	exitCode, err := runCLI(context.Background(), nil, &output)
	if exitCode != 2 || err == nil {
		t.Fatalf("exit=%d err=%v", exitCode, err)
	}
	if output.Len() != 0 {
		t.Fatalf("stdout = %q", output.String())
	}
}
