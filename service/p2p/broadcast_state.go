package p2p

func (n *Node) SetMasterchainBroadcastSignatureVerifier(verifier MasterchainBroadcastSignatureVerifier) {
	if n == nil {
		return
	}

	n.signatureVerifierMx.Lock()
	n.signatureVerifier = verifier
	n.signatureVerifierMx.Unlock()
}

func (n *Node) masterchainBroadcastSignatureVerifier() MasterchainBroadcastSignatureVerifier {
	if n == nil {
		return nil
	}

	n.signatureVerifierMx.RLock()
	verifier := n.signatureVerifier
	n.signatureVerifierMx.RUnlock()
	return verifier
}

func (n *Node) NotifyCompressedBlockStateReady() {
	if n == nil {
		return
	}

	n.stateReadyMx.Lock()
	if n.stateReadyNotify != nil {
		close(n.stateReadyNotify)
	}
	n.stateReadyNotify = make(chan struct{})
	n.stateReadyMx.Unlock()
}

func (n *Node) compressedBlockStateReadyNotify() <-chan struct{} {
	if n == nil {
		return nil
	}

	n.stateReadyMx.Lock()
	if n.stateReadyNotify == nil {
		n.stateReadyNotify = make(chan struct{})
	}
	ch := n.stateReadyNotify
	n.stateReadyMx.Unlock()
	return ch
}
