package console

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"
)

type scanResult struct {
	line string
	err  error
}

// Run reads and executes newline-delimited commands until input reaches EOF or
// the context is canceled. Command errors are reported to handleError and do
// not stop the runner. Scanner and writer errors are returned. Cancellation
// cannot interrupt a Read already blocked inside an arbitrary caller-owned
// Reader; Run still returns immediately and that scanner exits when Read next
// yields. The node's stdin reader naturally ends with the process.
func Run(
	ctx context.Context,
	registry *Registry,
	in io.Reader,
	out io.Writer,
	handleError func(string, error),
) error {
	scanCtx, cancelScan := context.WithCancel(ctx)
	defer cancelScan()

	results := scan(scanCtx, in)
	for {
		select {
		case <-ctx.Done():
			return nil
		case result, open := <-results:
			if !open {
				return nil
			}
			if ctx.Err() != nil {
				return nil
			}
			if result.err != nil {
				return fmt.Errorf("scan console input: %w", result.err)
			}
			if strings.TrimSpace(result.line) == "" {
				continue
			}

			output, err := registry.Execute(ctx, result.line)
			if ctx.Err() != nil {
				return nil
			}
			if err != nil {
				if handleError != nil {
					handleError(result.line, err)
				}

				continue
			}
			if output == "" {
				continue
			}

			if err = writeOutput(out, output); err != nil {
				return err
			}
		}
	}
}

func scan(ctx context.Context, in io.Reader) <-chan scanResult {
	const maxLineBytes = 64 * 1024

	results := make(chan scanResult)

	go func() {
		defer close(results)

		scanner := bufio.NewScanner(in)
		scanner.Buffer(make([]byte, 4*1024), maxLineBytes)
		for scanner.Scan() {
			select {
			case results <- scanResult{line: scanner.Text()}:
			case <-ctx.Done():
				return
			}
		}
		if err := scanner.Err(); err != nil {
			select {
			case results <- scanResult{err: err}:
			case <-ctx.Done():
			}
		}
	}()

	return results
}

func writeOutput(out io.Writer, output string) error {
	if !strings.HasSuffix(output, "\n") {
		output += "\n"
	}

	written, err := io.WriteString(out, output)
	if err != nil {
		return fmt.Errorf("write console output: %w", err)
	}
	if written != len(output) {
		return fmt.Errorf("write console output: %w", io.ErrShortWrite)
	}

	return nil
}
