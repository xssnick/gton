package p2p

import (
	"context"
	"errors"
	"testing"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type runtimeCompressedState struct{}

func (*runtimeCompressedState) StateRootForCompressedBlock(context.Context, ton.BlockIDExt) (*cell.Cell, error) {
	return nil, storage.ErrNotFound
}

func (*runtimeCompressedState) RememberCompressedBlockState(*storage.BlockState) bool {
	return false
}

type runtimeSyncLag struct{}

func (*runtimeSyncLag) SyncLagSeconds() (int64, error) {
	return 0, nil
}

type runtimeSignatureVerifier struct{}

func (*runtimeSignatureVerifier) CheckBlockBroadcastSignatures(context.Context, BlockBroadcastSignatureCheck) error {
	return nil
}

func (*runtimeSignatureVerifier) CheckBlockFinalitySignatures(context.Context, BlockFinalitySignatureCheck) (*BlockFinalitySignatureCheckResult, error) {
	return nil, nil
}

func (*runtimeSignatureVerifier) ValidateShardDescriptionBroadcast(context.Context, ShardDescriptionSignatureCheck) (*ShardBlockDescription, error) {
	return nil, nil
}

type runtimeBroadcastAdmission struct{}

func (*runtimeBroadcastAdmission) CanAcceptBroadcast(BroadcastAdmissionRequest) bool {
	return true
}

type runtimeExternalMessageAdmission struct{}

func (*runtimeExternalMessageAdmission) AcceptExternalMessage(context.Context, ExternalMessageEvent) error {
	return nil
}

func (*runtimeExternalMessageAdmission) AcceptCheckedExternalMessage(context.Context, ExternalMessageEvent) error {
	return nil
}

type runtimeBlockReceivedObserver struct{}

func (*runtimeBlockReceivedObserver) ObserveBlockReceived(context.Context, BlockReceivedEvent) {}

var _ CompressedBlockStateProvider = (*runtimeCompressedState)(nil)
var _ SyncLagProvider = (*runtimeSyncLag)(nil)
var _ BroadcastSignatureVerifier = (*runtimeSignatureVerifier)(nil)
var _ BroadcastAdmission = (*runtimeBroadcastAdmission)(nil)
var _ ExternalMessageAdmission = (*runtimeExternalMessageAdmission)(nil)
var _ BlockReceivedObserver = (*runtimeBlockReceivedObserver)(nil)

func TestBindRuntimeCallbacksKeepsComponentOwnership(t *testing.T) {
	compressedState := &runtimeCompressedState{}
	syncLag := &runtimeSyncLag{}
	signatureVerifier := &runtimeSignatureVerifier{}
	broadcastAdmission := &runtimeBroadcastAdmission{}
	externalMessages := &runtimeExternalMessageAdmission{}
	blocks := &runtimeBlockReceivedObserver{}

	node := &Node{}
	err := node.BindRuntimeCallbacks(RuntimeCallbacks{
		CompressedState:          compressedState,
		SyncLag:                  syncLag,
		SignatureVerifier:        signatureVerifier,
		BroadcastAdmission:       broadcastAdmission,
		ExternalMessageAdmission: externalMessages,
		BlockReceivedObserver:    blocks,
	})
	if err != nil {
		t.Fatalf("bind runtime callbacks: %v", err)
	}

	if node.compressedState != compressedState || node.syncLag != syncLag ||
		node.signatureVerifier != signatureVerifier || node.broadcastAdmission != broadcastAdmission ||
		node.externalMessageAdmission != externalMessages || node.blockReceivedObserver != blocks {
		t.Fatal("runtime callbacks were not bound to their individual owners")
	}
	if err := node.BindRuntimeCallbacks(RuntimeCallbacks{}); !errors.Is(err, ErrRuntimeCallbacksBound) {
		t.Fatalf("second bind error = %v, want %v", err, ErrRuntimeCallbacksBound)
	}
}

func TestBindRuntimeCallbacksRejectsStartedNode(t *testing.T) {
	node := &Node{}
	node.sealRuntimeCallbacks()

	if err := node.BindRuntimeCallbacks(RuntimeCallbacks{}); !errors.Is(err, ErrRuntimeCallbacksBound) {
		t.Fatalf("bind after start error = %v, want %v", err, ErrRuntimeCallbacksBound)
	}
}
