package validatorcontrol

import (
	"context"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"

	"github.com/xssnick/tonutils-go/ton"
)

func (s *Server) stats(ctx context.Context) (Stats, error) {
	current, err := s.state.CurrentState(ctx)
	if err != nil {
		return Stats{}, err
	}
	if current.Masterchain.Parsed == nil {
		return Stats{}, fmt.Errorf("masterchain state metadata is not loaded")
	}

	masterchainBlock, err := formatBlockID(current.Masterchain.Block)
	if err != nil {
		return Stats{}, err
	}

	now := uint32(time.Now().Unix())
	masterchainSeqno := current.Masterchain.Block.SeqNo
	stats := []OneStat{
		{Key: "unixtime", Value: strconv.FormatUint(uint64(now), 10)},
		{Key: "masterchainblock", Value: masterchainBlock},
		{Key: "masterchainblocktime", Value: strconv.FormatUint(uint64(current.Masterchain.Parsed.GenUTime), 10)},
		{Key: "gcmasterchainblock", Value: masterchainBlock},
		{Key: "keymasterchainblock", Value: masterchainBlock},
		{Key: "knownkeymasterchainblock", Value: masterchainBlock},
		{Key: "rotatemasterchainblock", Value: masterchainBlock},
		{Key: "shardclientmasterchainseqno", Value: strconv.FormatUint(uint64(current.ShardClientSeqno), 10)},
		{Key: "stateserializermasterchainseqno", Value: strconv.FormatUint(uint64(masterchainSeqno), 10)},
		{Key: "stateserializerenabled", Value: "true"},
		{Key: "last_deleted_mc_state", Value: strconv.FormatUint(uint64(masterchainSeqno), 10)},
		{Key: "start_time", Value: strconv.FormatInt(s.startedAt.Unix(), 10)},
	}

	return Stats{Stats: stats}, nil
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
