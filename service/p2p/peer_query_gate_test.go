package p2p

import (
	"context"
	"reflect"
	"strings"
	"testing"

	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/ton"
)

func TestDispatchCustomFixedQueryGate(t *testing.T) {
	node := newTestNode(t)
	block := ton.BlockIDExt{
		Workchain: -1,
		Shard:     topShard,
		SeqNo:     1,
		RootHash:  make([]byte, 32),
		FileHash:  make([]byte, 32),
	}

	tests := []struct {
		name          string
		acceptQueries bool
		query         any
		want          any
		wantError     string
	}{
		{
			name:          "ping when queries disabled",
			acceptQueries: false,
			query:         overlay.Ping{},
			want:          overlay.Pong{},
		},
		{
			name:          "random peers when queries disabled",
			acceptQueries: false,
			query:         overlay.GetRandomPeers{},
			wantError:     "overlay is private",
		},
		{
			name:          "capabilities when queries disabled",
			acceptQueries: false,
			query:         GetCapabilities{},
			wantError:     "this node does not accept queries",
		},
		{
			name:          "application query when queries disabled",
			acceptQueries: false,
			query:         tonnodeapi.DownloadBlockFull{Block: block},
			wantError:     "this node does not accept queries",
		},
		{
			name:          "ping when queries enabled",
			acceptQueries: true,
			query:         overlay.Ping{},
			want:          overlay.Pong{},
		},
		{
			name:          "random peers when queries enabled",
			acceptQueries: true,
			query:         overlay.GetRandomPeers{},
			wantError:     "overlay is private",
		},
		{
			name:          "capabilities when queries enabled",
			acceptQueries: true,
			query:         GetCapabilities{},
			want:          Capabilities{},
		},
		{
			name:          "application query when queries enabled",
			acceptQueries: true,
			query:         tonnodeapi.DownloadBlockFull{Block: block},
			want:          tonnodeapi.DataFullEmpty{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sub := testOverlaySubscription(&overlaySubscription{
				node: node,
				spec: overlaySpec{
					Name:              "custom.private",
					Kind:              overlayKindCustomFixed,
					AcceptQueries:     tt.acceptQueries,
					ProtoVersionMajor: shardchainProtoVersionMajor,
					ProtoVersionMinor: shardchainProtoVersionMinor,
				},
				log: discardLogger(),
			})

			response, err := sub.dispatchPeerQuery(context.Background(), tt.query)
			if tt.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantError) {
					t.Fatalf("error = %v, want containing %q", err, tt.wantError)
				}
				if response != nil {
					t.Fatalf("response = %T, want nil", response)
				}
				return
			}
			if err != nil {
				t.Fatalf("dispatch query: %v", err)
			}
			if reflect.TypeOf(response) != reflect.TypeOf(tt.want) {
				t.Fatalf("response = %T, want %T", response, tt.want)
			}
		})
	}
}
