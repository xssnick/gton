package p2p

import (
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"os"
	"strings"

	storage2 "github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/ton"
)

func prepareStateFilesDir(dir string) (string, error) {
	if strings.TrimSpace(dir) == "" {
		return "", fmt.Errorf("state files dir is empty")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	if err := removeIncompleteStateFiles(dir); err != nil {
		return "", err
	}
	return dir, nil
}

func privateKeyOrGenerate(key ed25519.PrivateKey) (ed25519.PrivateKey, error) {
	if len(key) == 0 {
		_, priv, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			return nil, err
		}
		return priv, nil
	}

	if len(key) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("expected %d bytes, got %d", ed25519.PrivateKeySize, len(key))
	}

	return append(ed25519.PrivateKey(nil), key...), nil
}

func (n *Node) ZeroStateBlock() (ton.BlockIDExt, error) {
	if n.zeroStateBlock.Workchain != -1 || n.zeroStateBlock.Shard != topShard || n.zeroStateBlock.SeqNo != 0 || !storage2.BlockIDHashesKnown(n.zeroStateBlock) {
		return ton.BlockIDExt{}, storage2.ErrNotFound
	}
	return n.zeroStateBlock, nil
}

func (n *Node) InitBlock() (ton.BlockIDExt, error) {
	if n.initBlock.Workchain != -1 || n.initBlock.Shard != topShard || !storage2.BlockIDHashesKnown(n.initBlock) {
		return ton.BlockIDExt{}, storage2.ErrNotFound
	}
	return n.initBlock, nil
}

func (n *Node) IsHardfork(block ton.BlockIDExt) bool {
	key, ok := blockIDFullKeyFromBlock(block)
	if !ok {
		return false
	}
	_, ok = n.hardforkSet[key]
	return ok
}

func blockIDFromConfig(block liteclient.ConfigBlock) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: block.Workchain,
		Shard:     block.Shard,
		SeqNo:     block.SeqNo,
		RootHash:  append([]byte(nil), block.RootHash...),
		FileHash:  append([]byte(nil), block.FileHash...),
	}
}

type blockIDFullKey struct {
	workchain int32
	shard     int64
	seqno     uint32
	rootHash  [32]byte
	fileHash  [32]byte
}

func blockIDFullKeyFromBlock(block ton.BlockIDExt) (blockIDFullKey, bool) {
	if !storage2.BlockIDHashesKnown(block) {
		return blockIDFullKey{}, false
	}

	var key blockIDFullKey
	key.workchain = block.Workchain
	key.shard = block.Shard
	key.seqno = block.SeqNo
	copy(key.rootHash[:], block.RootHash)
	copy(key.fileHash[:], block.FileHash)
	return key, true
}

func hardforksFromConfig(blocks []liteclient.ConfigBlock) ([]ton.BlockIDExt, map[blockIDFullKey]struct{}, error) {
	if len(blocks) == 0 {
		return nil, nil, nil
	}

	hardforks := make([]ton.BlockIDExt, 0, len(blocks))
	active := make([]bool, 0, len(blocks))
	for _, block := range blocks {
		hardfork := blockIDFromConfig(block)
		if hardfork.Workchain != -1 || hardfork.Shard != topShard {
			return nil, nil, fmt.Errorf("global config contains non-masterchain hardfork")
		}
		if !storage2.BlockIDHashesKnown(hardfork) {
			return nil, nil, fmt.Errorf("global config contains invalid hardfork")
		}

		for i := range hardforks {
			if active[i] && hardforks[i].SeqNo >= hardfork.SeqNo {
				active[i] = false
			}
		}
		hardforks = append(hardforks, hardfork)
		active = append(active, true)
	}

	filtered := hardforks[:0]
	for i := range hardforks {
		if active[i] {
			filtered = append(filtered, hardforks[i])
		}
	}
	hardforks = filtered
	if len(hardforks) == 0 {
		return nil, nil, nil
	}

	set := make(map[blockIDFullKey]struct{}, len(hardforks))
	for _, hardfork := range hardforks {
		key, _ := blockIDFullKeyFromBlock(hardfork)
		set[key] = struct{}{}
	}
	return hardforks, set, nil
}

func initBlockFromConfig(block liteclient.ConfigBlock, zeroBlock ton.BlockIDExt, hardforks []ton.BlockIDExt) (ton.BlockIDExt, error) {
	initBlock := zeroBlock
	if emptyConfigBlock(block) {
		return latestInitBlockForHardforks(initBlock, hardforks), nil
	}

	initBlock = blockIDFromConfig(block)
	if initBlock.Workchain != -1 || initBlock.Shard != topShard || !storage2.BlockIDHashesKnown(initBlock) {
		return ton.BlockIDExt{}, fmt.Errorf("global config contains invalid init_block")
	}
	return latestInitBlockForHardforks(initBlock, hardforks), nil
}

func latestInitBlockForHardforks(initBlock ton.BlockIDExt, hardforks []ton.BlockIDExt) ton.BlockIDExt {
	for _, hardfork := range hardforks {
		if hardfork.SeqNo > initBlock.SeqNo {
			initBlock = hardfork
		}
	}
	return initBlock
}

func emptyConfigBlock(block liteclient.ConfigBlock) bool {
	return block.Workchain == 0 &&
		block.Shard == 0 &&
		block.SeqNo == 0 &&
		len(block.RootHash) == 0 &&
		len(block.FileHash) == 0
}
