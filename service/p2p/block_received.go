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

func (n *Node) observeDownloadedBlockReceived(ctx context.Context, downloaded *DownloadedBlock) {
	n.observeBlockReceived(ctx, downloaded, true)
}

func blockReceivedHooksEnabled(observer BlockReceivedObserver) bool {
	return observer != nil && observer.BlockReceivedHooksEnabled()
}
