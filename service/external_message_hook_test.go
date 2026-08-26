package service

import (
	"context"
	"testing"

	"github.com/xssnick/gton/service/hooks"
	"github.com/xssnick/gton/service/p2p"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type externalMessageRecordingExtension struct {
	events []hooks.ExternalMessageEvent
}

func (*externalMessageRecordingExtension) Start(context.Context) error {
	return nil
}

func (*externalMessageRecordingExtension) Close(context.Context) error {
	return nil
}

func (*externalMessageRecordingExtension) OnBlockApplied(context.Context, hooks.BlockAppliedEvent) error {
	return nil
}

func (e *externalMessageRecordingExtension) OnExternalMessage(_ context.Context, event hooks.ExternalMessageEvent) error {
	e.events = append(e.events, event)
	return nil
}

func (*externalMessageRecordingExtension) OnBlockReceived(context.Context, hooks.BlockReceivedEvent) error {
	return nil
}

func TestExternalMessageHookPreservesSourceMetadata(t *testing.T) {
	tests := []struct {
		name     string
		isLocal  bool
		priority int
		invoke   func(context.Context, *Service, p2p.ExternalMessageEvent, *cell.Cell, *tlb.ExternalMessage) error
	}{
		{
			name:     "checked admission",
			isLocal:  true,
			priority: 11,
			invoke: func(ctx context.Context, svc *Service, source p2p.ExternalMessageEvent, root *cell.Cell, message *tlb.ExternalMessage) error {
				source.Root = root
				source.Message = message
				return svc.AcceptCheckedExternalMessage(ctx, source)
			},
		},
		{
			name:     "unchecked admission result",
			isLocal:  false,
			priority: 17,
			invoke: func(ctx context.Context, svc *Service, source p2p.ExternalMessageEvent, root *cell.Cell, message *tlb.ExternalMessage) error {
				return svc.runExternalMessageHooks(ctx, source, root, message)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			extension := &externalMessageRecordingExtension{}
			svc := &Service{
				externalMessageHooks: &externalMessageHookRunner{
					log:       zerolog.Nop(),
					extension: extension,
				},
			}
			root := cell.BeginCell().MustStoreUInt(1, 1).EndCell()
			message := &tlb.ExternalMessage{}
			source := p2p.ExternalMessageEvent{
				IsLocal:  tt.isLocal,
				Priority: tt.priority,
			}

			if err := tt.invoke(t.Context(), svc, source, root, message); err != nil {
				t.Fatalf("run external message hook: %v", err)
			}
			if len(extension.events) != 1 {
				t.Fatalf("external message hook events = %d, want 1", len(extension.events))
			}
			event := extension.events[0]
			if event.IsLocal != tt.isLocal {
				t.Fatalf("hook IsLocal = %v, want %v", event.IsLocal, tt.isLocal)
			}
			if event.Priority != tt.priority {
				t.Fatalf("hook priority = %d, want %d", event.Priority, tt.priority)
			}
			if event.MessageRoot != root || event.MessageParsed != message {
				t.Fatal("hook parsed external message data mismatch")
			}
		})
	}
}
