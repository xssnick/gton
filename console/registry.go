package console

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

// ErrNotFound is returned when no handler matches a command.
var ErrNotFound = errors.New("console: command not found")

// Handler executes a command with the arguments that follow its registered
// path.
type Handler func(context.Context, []string) (string, error)

type registration struct {
	handler   Handler
	isDefault bool
}

// Registry stores console command handlers. Its zero value is ready to use.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]registration
	maxDepth int
}

// Register adds a handler for a case-insensitive, whitespace-delimited command
// path.
func (r *Registry) Register(path string, handler Handler) error {
	return r.register(path, handler, false)
}

// RegisterDefault installs an optional handler unless an explicit handler
// owns the same path. A later Register replaces the default, making extension
// registration order irrelevant when a generic in-process capability and a
// role-specific extension expose the same command.
func (r *Registry) RegisterDefault(path string, handler Handler) error {
	return r.register(path, handler, true)
}

func (r *Registry) register(path string, handler Handler, isDefault bool) error {
	if handler == nil {
		return errors.New("console: handler is nil")
	}

	tokens := strings.Fields(path)
	if len(tokens) == 0 {
		return errors.New("console: command path is empty")
	}
	for i, token := range tokens {
		tokens[i] = strings.ToLower(token)
	}
	key := strings.Join(tokens, " ")

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.handlers == nil {
		r.handlers = make(map[string]registration)
	}
	if existing, exists := r.handlers[key]; exists {
		switch {
		case isDefault:
			return nil
		case !existing.isDefault:
			return fmt.Errorf("console: command %q is already registered", key)
		}
	}

	r.handlers[key] = registration{handler: handler, isDefault: isDefault}
	if len(tokens) > r.maxDepth {
		r.maxDepth = len(tokens)
	}

	return nil
}

// Execute dispatches a line to the handler with the longest matching command
// path. Command matching is case-insensitive; unmatched arguments retain their
// original case.
func (r *Registry) Execute(ctx context.Context, line string) (string, error) {
	tokens := strings.Fields(line)
	if len(tokens) == 0 {
		return "", ErrNotFound
	}

	var handler Handler
	matched := 0

	r.mu.RLock()
	depth := min(len(tokens), r.maxDepth)
	normalized := make([]string, depth)
	for i, token := range tokens[:depth] {
		normalized[i] = strings.ToLower(token)
	}
	for size := depth; size > 0; size-- {
		candidate := strings.Join(normalized[:size], " ")
		if registered := r.handlers[candidate]; registered.handler != nil {
			handler = registered.handler
			matched = size
			break
		}
	}
	r.mu.RUnlock()

	if handler == nil {
		return "", ErrNotFound
	}

	return handler(ctx, tokens[matched:])
}
