package p2p

import "errors"

var ErrRuntimeCallbacksBound = errors.New("p2p runtime callbacks are already bound or the node has started")

// BindRuntimeCallbacks completes the constructor cycle between P2P and the
// chain services. It may be called exactly once and only before Start.
func (n *Node) BindRuntimeCallbacks(callbacks RuntimeCallbacks) error {
	n.runtimeCallbacksMu.Lock()
	defer n.runtimeCallbacksMu.Unlock()

	if n.runtimeCallbacksBound || n.runtimeCallbacksSealed {
		return ErrRuntimeCallbacksBound
	}

	n.compressedState = callbacks.CompressedState
	n.syncLag = callbacks.SyncLag
	n.signatureVerifier = callbacks.SignatureVerifier
	n.broadcastAdmission = callbacks.BroadcastAdmission
	n.externalMessageAdmission = callbacks.ExternalMessageAdmission
	n.blockReceivedObserver = callbacks.BlockReceivedObserver
	n.runtimeCallbacksBound = true

	return nil
}

func (n *Node) sealRuntimeCallbacks() {
	n.runtimeCallbacksMu.Lock()
	n.runtimeCallbacksSealed = true
	n.runtimeCallbacksMu.Unlock()
}
