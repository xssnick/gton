package p2p

import "context"

func (n *Node) observeBlockReceived(ctx context.Context, downloaded *DownloadedBlock, isSigned bool) {
	if !n.blockReceivedHooks || n.blockReceivedObserver == nil {
		return
	}

	n.blockReceivedObserver.ObserveBlockReceived(ctx, BlockReceivedEvent{
		IsSigned:   isSigned,
		Downloaded: downloaded,
	})
}

// observeBroadcastBlockReceived is not gated on extension hooks: the service
// observer also uses broadcast arrivals to pre-prepare shard blocks ahead of
// the master commit, hooks or not.
func (n *Node) observeBroadcastBlockReceived(ctx context.Context, downloaded *DownloadedBlock, isSigned bool) {
	if n.blockReceivedObserver == nil {
		return
	}

	n.blockReceivedObserver.ObserveBlockReceived(ctx, BlockReceivedEvent{
		IsSigned:      isSigned,
		FromBroadcast: true,
		Downloaded:    downloaded,
	})
}

func (n *Node) observeDownloadedBlockReceived(ctx context.Context, downloaded *DownloadedBlock) {
	n.observeBlockReceived(ctx, downloaded, true)
}

func blockReceivedHooksEnabled(observer BlockReceivedObserver) bool {
	return observer != nil && observer.BlockReceivedHooksEnabled()
}
