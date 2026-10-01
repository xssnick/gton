package console_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/xssnick/gton/console"
)

func TestRegistryRegisterNormalizesPath(t *testing.T) {
	var registry console.Registry
	var received []string

	err := registry.Register("  StAtUs\tDB  ", func(_ context.Context, args []string) (string, error) {
		received = args

		return "db", nil
	})
	if err != nil {
		t.Fatalf("register command: %v", err)
	}

	output, err := registry.Execute(context.Background(), "STATUS db MixedCase ARG")
	if err != nil {
		t.Fatalf("execute command: %v", err)
	}
	if output != "db" {
		t.Fatalf("output = %q, want %q", output, "db")
	}
	if expected := []string{"MixedCase", "ARG"}; !slices.Equal(received, expected) {
		t.Fatalf("handler args = %q, want %q", received, expected)
	}

	err = registry.Register("status db", func(context.Context, []string) (string, error) {
		return "", nil
	})
	if err == nil {
		t.Fatal("duplicate registration error = nil, want error")
	}
}

func TestRegistryExplicitHandlerReplacesDefaultIndependentOfOrder(t *testing.T) {
	orders := []struct {
		name          string
		registerFirst func(*console.Registry) error
		registerLast  func(*console.Registry) error
	}{
		{
			name: "default then explicit",
			registerFirst: func(registry *console.Registry) error {
				return registry.RegisterDefault("debug collator", fixedConsoleOutput("default"))
			},
			registerLast: func(registry *console.Registry) error {
				return registry.Register("debug collator", fixedConsoleOutput("explicit"))
			},
		},
		{
			name: "explicit then default",
			registerFirst: func(registry *console.Registry) error {
				return registry.Register("debug collator", fixedConsoleOutput("explicit"))
			},
			registerLast: func(registry *console.Registry) error {
				return registry.RegisterDefault("debug collator", fixedConsoleOutput("default"))
			},
		},
	}
	for _, order := range orders {
		t.Run(order.name, func(t *testing.T) {
			var registry console.Registry
			if err := order.registerFirst(&registry); err != nil {
				t.Fatal(err)
			}
			if err := order.registerLast(&registry); err != nil {
				t.Fatal(err)
			}

			output, err := registry.Execute(context.Background(), "debug collator")
			if err != nil {
				t.Fatal(err)
			}
			if output != "explicit" {
				t.Fatalf("output = %q, want explicit", output)
			}
		})
	}
}

func fixedConsoleOutput(output string) console.Handler {
	return func(context.Context, []string) (string, error) {
		return output, nil
	}
}

func TestRegistryRegisterRejectsInvalidValues(t *testing.T) {
	var registry console.Registry
	handler := func(context.Context, []string) (string, error) {
		return "", nil
	}

	t.Run("empty path", func(t *testing.T) {
		if err := registry.Register(" \t\n ", handler); err == nil {
			t.Fatal("Register error = nil, want error")
		}
	})

	t.Run("nil handler", func(t *testing.T) {
		if err := registry.Register("status", nil); err == nil {
			t.Fatal("Register error = nil, want error")
		}
	})
}

func TestRegistryExecuteUsesLongestPrefix(t *testing.T) {
	var registry console.Registry
	var called string

	if err := registry.Register("status", func(_ context.Context, args []string) (string, error) {
		called = "status"

		return args[0], nil
	}); err != nil {
		t.Fatalf("register status: %v", err)
	}
	if err := registry.Register("status validator", func(_ context.Context, args []string) (string, error) {
		called = "validator"

		return args[0], nil
	}); err != nil {
		t.Fatalf("register status validator: %v", err)
	}

	output, err := registry.Execute(context.Background(), "StAtUs VaLiDaToR GroupOne")
	if err != nil {
		t.Fatalf("execute nested command: %v", err)
	}
	if called != "validator" || output != "GroupOne" {
		t.Fatalf("nested dispatch = (%q, %q), want (%q, %q)", called, output, "validator", "GroupOne")
	}

	output, err = registry.Execute(context.Background(), "STATUS Other")
	if err != nil {
		t.Fatalf("execute parent command: %v", err)
	}
	if called != "status" || output != "Other" {
		t.Fatalf("parent dispatch = (%q, %q), want (%q, %q)", called, output, "status", "Other")
	}
}

func TestRegistryExecuteReturnsErrNotFound(t *testing.T) {
	var registry console.Registry

	for _, line := range []string{"", "  \t ", "missing argument"} {
		_, err := registry.Execute(context.Background(), line)
		if !errors.Is(err, console.ErrNotFound) {
			t.Fatalf("Execute(%q) error = %v, want ErrNotFound", line, err)
		}
	}
}

func TestRegistryExecuteReleasesLockBeforeHandler(t *testing.T) {
	var registry console.Registry

	if err := registry.Register("extend", func(context.Context, []string) (string, error) {
		err := registry.Register("added", func(context.Context, []string) (string, error) {
			return "added", nil
		})
		if err != nil {
			return "", err
		}

		return "extended", nil
	}); err != nil {
		t.Fatalf("register extend: %v", err)
	}

	if _, err := registry.Execute(context.Background(), "extend"); err != nil {
		t.Fatalf("execute extend: %v", err)
	}
	output, err := registry.Execute(context.Background(), "added")
	if err != nil {
		t.Fatalf("execute added: %v", err)
	}
	if output != "added" {
		t.Fatalf("output = %q, want %q", output, "added")
	}
}

func TestRegistryExecuteReturnsHandlerError(t *testing.T) {
	commandErr := errors.New("command failed")
	var registry console.Registry

	if err := registry.Register("fail", func(context.Context, []string) (string, error) {
		return "", commandErr
	}); err != nil {
		t.Fatalf("register fail: %v", err)
	}

	_, err := registry.Execute(context.Background(), "fail")
	if !errors.Is(err, commandErr) {
		t.Fatalf("Execute error = %v, want %v", err, commandErr)
	}
}
