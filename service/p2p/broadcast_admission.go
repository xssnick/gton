package p2p

func (n *Node) canAcceptBroadcast(kind string, local bool) bool {
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

func (n *Node) rebroadcastBlockedByAdmission(req *rebroadcastRequest) bool {
	if req == nil {
		return true
	}
	return !n.canAcceptBroadcast(req.kind, req.local)
}

func broadcastAdmissionExternalKind(kind string) bool {
	switch kind {
	case "tonNode.externalMessageBroadcast", "tonNode.ihrMessageBroadcast":
		return true
	default:
		return false
	}
}
