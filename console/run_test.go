package console_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/xssnick/gton/console"
)

type errorReader struct {
	err error
}

func (r errorReader) Read([]byte) (int, error) {
	return 0, r.err
}

type errorWriter struct {
	err error
}

func (w errorWriter) Write([]byte) (int, error) {
	return 0, w.err
}

type shortWriter struct{}

func (shortWriter) Write(p []byte) (int, error) {
	return len(p) - 1, nil
}

func TestRunDispatchesCommandsAndContinuesAfterErrors(t *testing.T) {
	commandErr := errors.New("command failed")
	var registry console.Registry

	if err := registry.Register("print", func(_ context.Context, args []string) (string, error) {
		return strings.Join(args, " "), nil
	}); err != nil {
		t.Fatalf("register print: %v", err)
	}
	if err := registry.Register("fail", func(context.Context, []string) (string, error) {
		return "", commandErr
	}); err != nil {
		t.Fatalf("register fail: %v", err)
	}

	var output bytes.Buffer
	lines := []string{}
	errorsSeen := []error{}
	err := console.Run(
		context.Background(),
		&registry,
		strings.NewReader("\nprint First Value\nfail\nmissing ARG\nprint Second\n"),
		&output,
		func(line string, err error) {
			lines = append(lines, line)
			errorsSeen = append(errorsSeen, err)
		},
	)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if output.String() != "First Value\nSecond\n" {
		t.Fatalf("output = %q, want %q", output.String(), "First Value\nSecond\n")
	}
	if len(lines) != 2 || lines[0] != "fail" || lines[1] != "missing ARG" {
		t.Fatalf("error lines = %q, want [fail missing ARG]", lines)
	}
	if !errors.Is(errorsSeen[0], commandErr) {
		t.Fatalf("first command error = %v, want %v", errorsSeen[0], commandErr)
	}
	if !errors.Is(errorsSeen[1], console.ErrNotFound) {
		t.Fatalf("second command error = %v, want ErrNotFound", errorsSeen[1])
	}
}

func TestRunDoesNotAddASecondNewline(t *testing.T) {
	var registry console.Registry
	if err := registry.Register("multiline", func(context.Context, []string) (string, error) {
		return "first\nsecond\n", nil
	}); err != nil {
		t.Fatalf("register multiline: %v", err)
	}

	var output bytes.Buffer
	if err := console.Run(
		context.Background(),
		&registry,
		strings.NewReader("multiline\n"),
		&output,
		nil,
	); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if output.String() != "first\nsecond\n" {
		t.Fatalf("output = %q, want %q", output.String(), "first\nsecond\n")
	}
}

func TestRunReturnsScannerError(t *testing.T) {
	inputErr := errors.New("read failed")
	input := io.MultiReader(strings.NewReader("unknown\n"), errorReader{err: inputErr})
	var registry console.Registry

	err := console.Run(context.Background(), &registry, input, io.Discard, nil)
	if !errors.Is(err, inputErr) {
		t.Fatalf("Run error = %v, want %v", err, inputErr)
	}
}

func TestRunBoundsLineSize(t *testing.T) {
	var registry console.Registry
	input := strings.NewReader(strings.Repeat("x", 128*1024) + "\n")

	err := console.Run(context.Background(), &registry, input, io.Discard, nil)
	if err == nil || !strings.Contains(err.Error(), "scan console input") {
		t.Fatalf("Run error = %v, want scanner line-size error", err)
	}
}

func TestRunReturnsWriterErrors(t *testing.T) {
	var registry console.Registry
	if err := registry.Register("write", func(context.Context, []string) (string, error) {
		return "output", nil
	}); err != nil {
		t.Fatalf("register write: %v", err)
	}

	t.Run("writer error", func(t *testing.T) {
		writeErr := errors.New("write failed")
		err := console.Run(
			context.Background(),
			&registry,
			strings.NewReader("write\n"),
			errorWriter{err: writeErr},
			nil,
		)
		if !errors.Is(err, writeErr) {
			t.Fatalf("Run error = %v, want %v", err, writeErr)
		}
	})

	t.Run("short write", func(t *testing.T) {
		err := console.Run(
			context.Background(),
			&registry,
			strings.NewReader("write\n"),
			shortWriter{},
			nil,
		)
		if !errors.Is(err, io.ErrShortWrite) {
			t.Fatalf("Run error = %v, want io.ErrShortWrite", err)
		}
	})
}

func TestRunCancellationIsClean(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reader, writer := io.Pipe()
	var registry console.Registry
	done := make(chan error, 1)

	go func() {
		done <- console.Run(ctx, &registry, reader, io.Discard, nil)
	}()

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run error = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not stop after context cancellation")
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("close pipe writer: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("close pipe reader: %v", err)
	}
}
