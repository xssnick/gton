package p2p

import "context"

type inboundBroadcastIngest struct {
	msg any
}

func (n *Node) runInboundIngestLoop(ctx context.Context) {
	for {
		job, ok := n.ingestQueue.Pop(ctx)
		if !ok {
			return
		}

		n.rememberInboundBroadcast(job.msg)
	}
}
