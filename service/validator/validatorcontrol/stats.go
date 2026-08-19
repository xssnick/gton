package validatorcontrol

import (
	"context"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"

	"github.com/xssnick/gton/service/validator/blockstats"
	"github.com/xssnick/tonutils-go/ton"
)

func (s *Server) stats(ctx context.Context) (Stats, error) {
	current, err := s.state.CurrentState(ctx)
	if err != nil {
		return Stats{}, err
	}
	if current == nil {
		return Stats{}, fmt.Errorf("masterchain state is not loaded")
	}

	masterchainBlock, err := formatBlockID(current.Masterchain.Block)
	if err != nil {
		return Stats{}, err
	}

	masterchainBlockTime := uint64(0)
	if current.Masterchain.Parsed != nil {
		masterchainBlockTime = uint64(current.Masterchain.Parsed.GenUTime)
	} else {
		meta, err := s.state.BlockMeta(ctx, current.Masterchain.Block)
		if err != nil {
			return Stats{}, fmt.Errorf("load masterchain block metadata: %w", err)
		}
		if meta == nil {
			return Stats{}, fmt.Errorf("masterchain block metadata is not loaded")
		}
		masterchainBlockTime = uint64(meta.GenUTime)
	}

	now := uint32(time.Now().Unix())
	masterchainSeqno := current.Masterchain.Block.SeqNo
	blockStats := s.blockStats.BlockStats()
	stats := []OneStat{
		{Key: "unixtime", Value: strconv.FormatUint(uint64(now), 10)},
		{Key: "masterchainblock", Value: masterchainBlock},
		{Key: "masterchainblocktime", Value: strconv.FormatUint(masterchainBlockTime, 10)},
		{Key: "gcmasterchainblock", Value: masterchainBlock},
		{Key: "keymasterchainblock", Value: masterchainBlock},
		{Key: "knownkeymasterchainblock", Value: masterchainBlock},
		{Key: "rotatemasterchainblock", Value: masterchainBlock},
		{Key: "shardclientmasterchainseqno", Value: strconv.FormatUint(uint64(current.ShardClientSeqno), 10)},
		{Key: "stateserializermasterchainseqno", Value: strconv.FormatUint(uint64(masterchainSeqno), 10)},
		{Key: "stateserializerenabled", Value: "true"},
		{Key: "last_deleted_mc_state", Value: strconv.FormatUint(uint64(masterchainSeqno), 10)},
		{Key: "total.collated_blocks.master", Value: formatBlockStats(blockStats.Collated.Master)},
		{Key: "total.collated_blocks.shard", Value: formatBlockStats(blockStats.Collated.Shard)},
		{Key: "total.validated_blocks.master", Value: formatBlockStats(blockStats.Validated.Master)},
		{Key: "total.validated_blocks.shard", Value: formatBlockStats(blockStats.Validated.Shard)},
		{Key: "start_time", Value: strconv.FormatInt(s.startedAt.Unix(), 10)},
	}

	return Stats{Stats: stats}, nil
}

func formatBlockStats(counter blockstats.Counter) string {
	return "ok:" + strconv.FormatUint(counter.OK, 10) + " error:" + strconv.FormatUint(counter.Error, 10)
}

func formatBlockID(block ton.BlockIDExt) (string, error) {
	if len(block.RootHash) != 32 {
		return "", fmt.Errorf("masterchain root hash must be 32 bytes, got %d", len(block.RootHash))
	}
	if len(block.FileHash) != 32 {
		return "", fmt.Errorf("masterchain file hash must be 32 bytes, got %d", len(block.FileHash))
	}

	return fmt.Sprintf(
		"(%d,%016x,%d):%s:%s",
		block.Workchain,
		uint64(block.Shard),
		block.SeqNo,
		hex.EncodeToString(block.RootHash),
		hex.EncodeToString(block.FileHash),
	), nil
}
