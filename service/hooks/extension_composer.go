package hooks

import (
	"context"
	"errors"
	"fmt"
)

var _ Extension = composedExtension(nil)

// ExtensionComposer builds one Extension from several extension factories.
type ExtensionComposer []ExtensionFactory

func (c ExtensionComposer) New(node Node) (Extension, error) {
	extensions := make(composedExtension, 0, len(c))
	for idx, factory := range c {
		if factory == nil {
			return nil, fmt.Errorf("extension factory %d is nil", idx)
		}

		extension, err := factory(node)
		if err != nil {
			return nil, fmt.Errorf("initialize extension %d: %w", idx, err)
		}
		if extension == nil {
			return nil, fmt.Errorf("extension factory %d returned nil extension", idx)
		}

		extensions = append(extensions, extension)
	}
	return extensions, nil
}

type composedExtension []Extension

func (m composedExtension) OnBlockApplied(ctx context.Context, event BlockAppliedEvent) error {
	for idx, extension := range m {
		if err := extension.OnBlockApplied(ctx, event); err != nil {
			return fmt.Errorf("extension %d OnBlockApplied: %w", idx, err)
		}
	}
	return nil
}

func (m composedExtension) OnExternalMessage(ctx context.Context, event ExternalMessageEvent) error {
	for idx, extension := range m {
		if err := extension.OnExternalMessage(ctx, event); err != nil {
			return fmt.Errorf("extension %d OnExternalMessage: %w", idx, err)
		}
	}
	return nil
}

func (m composedExtension) OnBlockReceived(ctx context.Context, event BlockReceivedEvent) error {
	for idx, extension := range m {
		if err := extension.OnBlockReceived(ctx, event); err != nil {
			return fmt.Errorf("extension %d OnBlockReceived: %w", idx, err)
		}
	}
	return nil
}

func (m composedExtension) Start(ctx context.Context) error {
	for idx, extension := range m {
		if err := extension.Start(ctx); err != nil {
			return errors.Join(
				fmt.Errorf("start extension %d: %w", idx, err),
				m.closeStarted(ctx, idx),
			)
		}
	}
	return nil
}

func (m composedExtension) Close(ctx context.Context) error {
	return m.closeStarted(ctx, len(m))
}

func (m composedExtension) closeStarted(ctx context.Context, before int) error {
	var err error
	for idx := before - 1; idx >= 0; idx-- {
		if closeErr := m[idx].Close(ctx); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close extension %d: %w", idx, closeErr))
		}
	}
	return err
}
