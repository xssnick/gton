package main

import (
	"context"
	"fmt"
	"sync"

	service2 "github.com/xssnick/gton/service"
	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

var (
	_ p2p.CompressedBlockStateProvider = (*serviceP2PCallbacks)(nil)
	_ p2p.SyncLagProvider              = (*serviceP2PCallbacks)(nil)
	_ p2p.BroadcastSignatureVerifier   = (*serviceP2PCallbacks)(nil)
	_ p2p.BroadcastAdmission           = (*serviceP2PCallbacks)(nil)
)

type serviceP2PCallbacks struct {
	mu  sync.RWMutex
	svc *service2.Service
}

func (c *serviceP2PCallbacks) set(svc *service2.Service) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.svc = svc
}

func (c *serviceP2PCallbacks) service() (*service2.Service, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.svc == nil {
		return nil, fmt.Errorf("service p2p callbacks are not initialized: %w", storage.ErrNotFound)
	}
	return c.svc, nil
}

func (c *serviceP2PCallbacks) StateRootForCompressedBlock(ctx context.Context, block ton.BlockIDExt) (*cell.Cell, error) {
	svc, err := c.service()
	if err != nil {
		return nil, err
	}
	return svc.StateRootForCompressedBlock(ctx, block)
}

func (c *serviceP2PCallbacks) SyncLagSeconds() (int64, error) {
	svc, err := c.service()
	if err != nil {
		return 0, err
	}
	return svc.SyncLagSeconds()
}

func (c *serviceP2PCallbacks) CheckBlockBroadcastSignatures(ctx context.Context, req p2p.BlockBroadcastSignatureCheck) error {
	svc, err := c.service()
	if err != nil {
		return err
	}
	return svc.CheckBlockBroadcastSignatures(ctx, req)
}

func (c *serviceP2PCallbacks) ValidateShardDescriptionBroadcast(ctx context.Context, req p2p.ShardDescriptionSignatureCheck) (*p2p.ShardBlockDescription, error) {
	svc, err := c.service()
	if err != nil {
		return nil, err
	}
	return svc.ValidateShardDescriptionBroadcast(ctx, req)
}

func (c *serviceP2PCallbacks) CanAcceptBroadcast(req p2p.BroadcastAdmissionRequest) bool {
	svc, err := c.service()
	if err != nil {
		return false
	}
	return svc.CanAcceptBroadcast(req)
}
