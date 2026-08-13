package p2p

import (
	"context"
	"fmt"
	"strings"

	storage2 "github.com/xssnick/gton/service/storage"
)

type nodeLifecycleState uint8

const (
	nodeLifecycleNew nodeLifecycleState = iota
	nodeLifecycleStarting
	nodeLifecycleRunning
	nodeLifecycleStopping
	nodeLifecycleStopped
	nodeLifecycleFailed
)

func (n *Node) Start(ctx context.Context) error {
	startDone, startOwner, err := n.beginStart()
	if err != nil {
		return err
	}
	if !startOwner {
		if startDone == nil {
			return nil
		}
		<-startDone

		n.lifecycleMu.Lock()
		err = n.startErr
		n.lifecycleMu.Unlock()
		return err
	}

	n.sealRuntimeCallbacks()
	gatewayStarted, err := n.start(ctx)
	return n.finishStart(gatewayStarted, err)
}

func (n *Node) beginStart() (<-chan struct{}, bool, error) {
	n.lifecycleMu.Lock()
	defer n.lifecycleMu.Unlock()

	switch n.lifecycleState {
	case nodeLifecycleNew:
		n.lifecycleState = nodeLifecycleStarting
		n.startDone = make(chan struct{})
		return n.startDone, true, nil
	case nodeLifecycleStarting:
		return n.startDone, false, nil
	case nodeLifecycleRunning:
		return nil, false, nil
	case nodeLifecycleStopping:
		if n.startDone != nil {
			return n.startDone, false, nil
		}
		return nil, false, ErrOffline
	case nodeLifecycleStopped:
		return nil, false, ErrOffline
	case nodeLifecycleFailed:
		return nil, false, n.startErr
	default:
		panic("invalid p2p node lifecycle state")
	}
}

func (n *Node) start(ctx context.Context) (bool, error) {
	cfg := n.globalConfig
	if cfg == nil {
		return false, fmt.Errorf("global config is required")
	}

	zeroBlock := blockIDFromConfig(cfg.Validator.ZeroState)
	if zeroBlock.Workchain != -1 || zeroBlock.Shard != topShard || zeroBlock.SeqNo != 0 || !storage2.BlockIDHashesKnown(zeroBlock) {
		return false, fmt.Errorf("global config contains invalid zero_state")
	}

	hardforks, hardforkSet, err := hardforksFromConfig(cfg.Validator.Hardforks)
	if err != nil {
		return false, err
	}

	initBlock, err := initBlockFromConfig(cfg.Validator.InitBlock, zeroBlock, hardforks)
	if err != nil {
		return false, err
	}

	runCtx, runCancel := context.WithCancel(ctx)
	n.lifecycleMu.Lock()
	n.runCtx = runCtx
	n.runCancel = runCancel
	stopping := n.lifecycleState == nodeLifecycleStopping
	n.lifecycleMu.Unlock()
	if stopping {
		runCancel()
	}
	if err = runCtx.Err(); err != nil {
		return false, err
	}
	n.zeroStateFileHash = append([]byte(nil), zeroBlock.FileHash...)
	n.zeroStateBlock = zeroBlock
	n.initBlock = initBlock
	n.hardforkSet = hardforkSet
	if len(hardforks) > 0 {
		n.log.Info().
			Int("hardforks", len(hardforks)).
			Str("init_block", storage2.FormatBlockRef(initBlock)).
			Msg("loaded hardforks from global config")
	}
	specs, err := buildOverlaySpecs(n.zeroStateFileHash)
	if err != nil {
		return false, err
	}
	customSpecs, err := buildCustomOverlaySpecs(n.zeroStateFileHash, n.customOverlays, n.localID)
	if err != nil {
		return false, err
	}
	if len(n.customOverlays) > 0 {
		n.log.Info().
			Int("configured", len(n.customOverlays)).
			Int("local", len(customSpecs)).
			Msg("loaded custom overlay config")
	}
	specs = append(specs, customSpecs...)
	subscriptions := make([]*overlaySubscription, 0, len(specs))
	for _, spec := range specs {
		sub, err := n.getOrCreateSubscription(spec)
		if err != nil {
			return false, err
		}
		subscriptions = append(subscriptions, sub)
	}

	n.gateway.SetConnectionHandler(n.handleInboundPeer)
	n.quicGateway.SetConnectionHandler(n.handleInboundQUICPeer)
	if err = n.startGateway(); err != nil {
		if n.quicServeDone != nil {
			_ = n.closeQUICGateway()
		}
		return false, fmt.Errorf("start network gateways: %w", err)
	}

	if err = n.startDHT(runCtx, cfg); err != nil {
		return true, err
	}
	if err = runCtx.Err(); err != nil {
		return true, err
	}
	if err = n.checkQUICServer(); err != nil {
		return true, err
	}

	for _, sub := range subscriptions {
		n.startSubscription(sub)
	}
	n.runAsync(func() {
		n.runEventLoop(runCtx)
	})
	n.runAsync(func() {
		n.blockBroadcasts.run(runCtx)
	})
	n.runAsync(func() {
		n.runShardBroadcastCacheJanitor(runCtx)
	})
	n.runAsync(func() {
		n.runSubscriptionLifecycleLoop(runCtx)
	})
	n.runAsync(func() {
		n.runQUICIdleSweepLoop(runCtx)
	})
	n.startQUICMonitor(runCtx)
	if n.isPublicServer() {
		n.runAsync(func() {
			n.runAnnounceLoop(runCtx)
		})
	}

	n.lifecycleWG.Add(1)
	go func() {
		defer n.lifecycleWG.Done()
		<-runCtx.Done()
		n.stop()
	}()

	return true, nil
}

func (n *Node) finishStart(gatewayStarted bool, startErr error) error {
	n.lifecycleMu.Lock()
	if startErr == nil && n.lifecycleState == nodeLifecycleStarting {
		n.lifecycleState = nodeLifecycleRunning
		n.networkStarted.Store(true)
		n.startErr = nil
		close(n.startDone)
		n.startDone = nil
		n.lifecycleMu.Unlock()
		return nil
	}

	terminalState := nodeLifecycleFailed
	terminalErr := startErr
	if n.lifecycleState == nodeLifecycleStopping {
		terminalState = nodeLifecycleStopped
		terminalErr = ErrOffline
	} else {
		n.lifecycleState = nodeLifecycleStopping
	}
	n.lifecycleMu.Unlock()

	n.completeShutdown(terminalState, terminalErr, gatewayStarted)
	return terminalErr
}

func (n *Node) runAsync(fn func()) bool {
	n.asyncMx.Lock()
	if n.asyncStopped {
		n.asyncMx.Unlock()
		return false
	}
	n.wg.Add(1)
	n.asyncMx.Unlock()
	go func() {
		defer n.wg.Done()
		fn()
	}()
	return true
}

func (n *Node) Wait() {
	<-n.stopped
	n.lifecycleWG.Wait()
}

func (n *Node) EnterOffline(reason string) {
	n.enterOffline(reason, false)
}

// enterOfflineFailure is EnterOffline for an unexpected subsystem death. The
// failure flag is raised before the offline transition, and the failure signal
// is published only after its reason is ready for observers.
func (n *Node) enterOfflineFailure(reason string) {
	n.enterOffline(reason, true)
}

func (n *Node) enterOffline(reason string, failed bool) {
	if strings.TrimSpace(reason) == "" {
		reason = "offline mode requested"
	}
	if failed {
		n.offlineFailed.Store(true)
	}
	if !n.offline.CompareAndSwap(false, true) {
		return
	}

	n.offlineReasonMu.Lock()
	n.offlineReason = reason
	n.offlineReasonMu.Unlock()

	if failed {
		n.onceFailed.Do(func() {
			close(n.failed)
		})
	}

	n.log.Info().
		Str("reason", reason).
		Msg("entering p2p offline mode")
	n.stop()
}

func (n *Node) IsOffline() bool {
	return n.offline.Load()
}

// OfflineFailed reports whether the node went offline because something broke,
// as opposed to the deliberate stop at ton.sync_until.
func (n *Node) OfflineFailed() bool {
	return n.offlineFailed.Load()
}

// Failed is closed when the node stops because a subsystem died. The deliberate
// ton.sync_until stop never closes it: there the process is meant to keep
// serving the frozen state.
func (n *Node) Failed() <-chan struct{} {
	return n.failed
}

func (n *Node) OfflineReason() string {
	n.offlineReasonMu.RLock()
	defer n.offlineReasonMu.RUnlock()
	return n.offlineReason
}

func (n *Node) stop() {
	n.lifecycleMu.Lock()
	state := n.lifecycleState
	switch state {
	case nodeLifecycleNew:
		n.lifecycleState = nodeLifecycleStopping
	case nodeLifecycleStarting:
		n.lifecycleState = nodeLifecycleStopping
		startDone := n.startDone
		cancel := n.runCancel
		n.lifecycleMu.Unlock()
		if cancel != nil {
			cancel()
		}
		<-startDone
		return
	case nodeLifecycleRunning:
		n.lifecycleState = nodeLifecycleStopping
	case nodeLifecycleStopping:
		stopped := n.stopped
		n.lifecycleMu.Unlock()
		<-stopped
		return
	case nodeLifecycleStopped, nodeLifecycleFailed:
		n.lifecycleMu.Unlock()
		return
	default:
		n.lifecycleMu.Unlock()
		panic("invalid p2p node lifecycle state")
	}
	cancel := n.runCancel
	n.lifecycleMu.Unlock()

	if cancel != nil {
		cancel()
	}
	n.completeShutdown(nodeLifecycleStopped, ErrOffline, state == nodeLifecycleRunning)
}

func (n *Node) completeShutdown(
	terminalState nodeLifecycleState,
	startErr error,
	gatewayStarted bool,
) {
	n.onceStop.Do(func() {
		n.lifecycleMu.Lock()
		cancel := n.runCancel
		n.lifecycleMu.Unlock()
		if cancel != nil {
			cancel()
		}

		n.stopAcceptingInbound()

		if gatewayStarted {
			if n.dhtClient != nil {
				n.dhtClient.Close()
			}
			if n.dhtServer != nil {
				_ = n.dhtServer.Close()
			}
			if n.dhtGateway != nil {
				_ = n.dhtGateway.Close()
			}
			_ = n.gateway.Close()
			_ = n.closeQUICGateway()
		}
		n.networkStarted.Store(false)

		n.eventQueue.Close()
		n.blockBroadcasts.queue.Close()
		n.closeSubscriptions()

		n.asyncMx.Lock()
		n.asyncStopped = true
		n.asyncMx.Unlock()
		n.wg.Wait()
		n.inboundWG.Wait()

		if n.stateFilesDir != "" {
			_ = removeIncompleteStateFiles(n.stateFilesDir)
		}

		n.lifecycleMu.Lock()
		n.lifecycleState = terminalState
		if n.startDone != nil {
			n.startErr = startErr
			close(n.startDone)
			n.startDone = nil
		}
		close(n.stopped)
		n.lifecycleMu.Unlock()
	})
}

func (n *Node) stopAcceptingInbound() {
	n.inboundMx.Lock()
	n.inboundStopping = true
	n.inboundMx.Unlock()
}

func (n *Node) beginInbound() bool {
	n.inboundMx.Lock()
	defer n.inboundMx.Unlock()

	if n.inboundStopping {
		return false
	}
	n.inboundWG.Add(1)
	return true
}

func (n *Node) finishInbound() {
	n.inboundWG.Done()
}
