package hooks

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/xssnick/gton/service/p2p"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

var _ Extension = (*composedExtension)(nil)
var _ ShardTopBlockDescriptionObserver = (*composedExtensionWithShardTops)(nil)

// ExtensionComposer builds one Extension from several extension factories.
type ExtensionComposer []ExtensionFactory

func (c ExtensionComposer) New(node Node) (Extension, error) {
	composed := &composedExtension{
		extensions: make([]Extension, 0, len(c)),
	}
	shardTopObservers := make([]composedShardTopObserver, 0, len(c))
	for idx, factory := range c {
		if factory == nil {
			return nil, errors.Join(
				fmt.Errorf("extension factory %d is nil", idx),
				composed.closeBefore(context.Background(), len(composed.extensions)),
			)
		}

		extension, err := factory(node)
		if err != nil {
			return nil, errors.Join(
				fmt.Errorf("initialize extension %d: %w", idx, err),
				composed.closeBefore(context.Background(), len(composed.extensions)),
			)
		}
		if extension == nil {
			return nil, errors.Join(
				fmt.Errorf("extension factory %d returned nil extension", idx),
				composed.closeBefore(context.Background(), len(composed.extensions)),
			)
		}

		composed.extensions = append(composed.extensions, extension)
		if observer, ok := extension.(ShardTopBlockDescriptionObserver); ok {
			shardTopObservers = append(shardTopObservers, composedShardTopObserver{
				index:    idx,
				observer: observer,
			})
		}
	}

	if len(shardTopObservers) == 0 {
		return composed, nil
	}

	return &composedExtensionWithShardTops{
		composedExtension: composed,
		observers:         shardTopObservers,
	}, nil
}

type composedExtension struct {
	extensions []Extension

	closeOnce sync.Once
	closeGate chan struct{}
	closeNext int
}

func (m *composedExtension) OnBlockApplied(ctx context.Context, event BlockAppliedEvent) error {
	for idx, extension := range m.extensions {
		if err := extension.OnBlockApplied(ctx, event); err != nil {
			return fmt.Errorf("extension %d OnBlockApplied: %w", idx, err)
		}
	}
	return nil
}

func (m *composedExtension) OnExternalMessage(ctx context.Context, event ExternalMessageEvent) error {
	for idx, extension := range m.extensions {
		if err := extension.OnExternalMessage(ctx, event); err != nil {
			return fmt.Errorf("extension %d OnExternalMessage: %w", idx, err)
		}
	}
	return nil
}

func (m *composedExtension) OnBlockReceived(ctx context.Context, event BlockReceivedEvent) error {
	for idx, extension := range m.extensions {
		if err := extension.OnBlockReceived(ctx, event); err != nil {
			return fmt.Errorf("extension %d OnBlockReceived: %w", idx, err)
		}
	}
	return nil
}

func (m *composedExtension) Start(ctx context.Context) error {
	for idx, extension := range m.extensions {
		if err := extension.Start(ctx); err != nil {
			return fmt.Errorf("start extension %d: %w", idx, err)
		}
	}
	return nil
}

func (m *composedExtension) Close(ctx context.Context) error {
	m.closeOnce.Do(func() {
		m.closeGate = make(chan struct{}, 1)
		m.closeGate <- struct{}{}
		m.closeNext = len(m.extensions)
	})
	select {
	case <-m.closeGate:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { m.closeGate <- struct{}{} }()

	for m.closeNext > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}

		idx := m.closeNext - 1
		if err := m.extensions[idx].Close(ctx); err != nil {
			return fmt.Errorf("close extension %d: %w", idx, err)
		}
		m.closeNext = idx
	}

	return nil
}

func (m *composedExtension) closeBefore(ctx context.Context, before int) error {
	var err error
	for idx := before - 1; idx >= 0; idx-- {
		if closeErr := m.extensions[idx].Close(ctx); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close extension %d: %w", idx, closeErr))
		}
	}
	return err
}

type composedShardTopObserver struct {
	index    int
	observer ShardTopBlockDescriptionObserver
}

type composedExtensionWithShardTops struct {
	*composedExtension
	observers []composedShardTopObserver
}

func (m *composedExtensionWithShardTops) OnShardTopBlockDescription(
	ctx context.Context,
	description *p2p.ShardBlockDescription,
	root *cell.Cell,
) error {
	var joined error
	for _, item := range m.observers {
		if err := item.observer.OnShardTopBlockDescription(ctx, description, root); err != nil {
			joined = errors.Join(joined, fmt.Errorf("extension %d OnShardTopBlockDescription: %w", item.index, err))
		}
	}

	return joined
}
