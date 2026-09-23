package p2p

// SetChainBroadcastsEnabled suspends chain broadcast reception and relay during
// archive catch-up. Queries and extension-owned private overlays stay active.
func (n *Node) SetChainBroadcastsEnabled(enabled bool) {
	n = n.chainNode()
	if n.chainBroadcastsPaused.Swap(!enabled) == !enabled {
		return
	}

	for _, network := range []*Node{n, n.privateNetwork} {
		if network == nil {
			continue
		}
		network.subscriptionsMx.Lock()
		for _, sub := range network.subscriptions {
			if sub.spec.isPrivateOverlay() {
				continue
			}
			sub.mx.Lock()
			sub.broadcastReceiver.SetActive(!sub.removed && !sub.inactive && !sub.chainBroadcastsPaused())
			sub.broadcastTargetsGen.Add(1)
			sub.mx.Unlock()
		}
		network.subscriptionsMx.Unlock()
	}

	n.log.Info().Bool("enabled", enabled).Msg("updated chain broadcast participation")
}

func (s *overlaySubscription) chainBroadcastsPaused() bool {
	return !s.spec.isPrivateOverlay() && s.node.chainNode().chainBroadcastsPaused.Load()
}

func (n *Node) canAcceptBroadcast(kind string, local bool) bool {
	if n.chainBroadcastsPaused.Load() {
		return false
	}
	if n.broadcastAdmission == nil {
		return true
	}
	if local && broadcastAdmissionExternalKind(kind) {
		return true
	}
	return n.broadcastAdmission.CanAcceptBroadcast(BroadcastAdmissionRequest{
		Kind:  kind,
		Local: local,
	})
}

func broadcastAdmissionExternalKind(kind string) bool {
	switch kind {
	case "tonNode.externalMessageBroadcast", "tonNode.ihrMessageBroadcast":
		return true
	default:
		return false
	}
}
