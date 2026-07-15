package httpapi

import (
	"context"
	"fmt"
	"sort"

	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	blockHeaderType      = "blocks.header"
	shardsType           = "blocks.shards"
	consensusBlockType   = "ext.blocks.consensusBlock"
	outMsgQueueSizesType = "blocks.outMsgQueueSizes"
	outMsgQueueSizeType  = "blocks.outMsgQueueSize"
	configInfoType       = "configInfo"
	tvmCellType          = "tvm.cell"
	masterchainShard     = int64(-1 << 63)
	outMsgQueueSizeLimit = uint64(8000)
)

type blockHeader struct {
	Type                   string       `json:"@type"`
	ID                     blockIDExt   `json:"id"`
	GlobalID               int32        `json:"global_id"`
	Version                uint32       `json:"version"`
	AfterMerge             bool         `json:"after_merge"`
	AfterSplit             bool         `json:"after_split"`
	BeforeSplit            bool         `json:"before_split"`
	WantMerge              bool         `json:"want_merge"`
	WantSplit              bool         `json:"want_split"`
	ValidatorListHashShort uint32       `json:"validator_list_hash_short"`
	CatchainSeqno          uint32       `json:"catchain_seqno"`
	MinRefMCSeqno          uint32       `json:"min_ref_mc_seqno"`
	IsKeyBlock             bool         `json:"is_key_block"`
	PrevKeyBlockSeqno      uint32       `json:"prev_key_block_seqno"`
	StartLT                string       `json:"start_lt"`
	EndLT                  string       `json:"end_lt"`
	GenUTime               uint32       `json:"gen_utime"`
	VertSeqno              uint32       `json:"vert_seqno,omitempty"`
	PrevBlocks             []blockIDExt `json:"prev_blocks"`
}

type shardsInfo struct {
	Type   string       `json:"@type"`
	Shards []blockIDExt `json:"shards"`
}

type consensusBlock struct {
	Type      string `json:"@type"`
	Seqno     uint32 `json:"consensus_block"`
	Timestamp uint32 `json:"timestamp"`
}

type outMsgQueueSizes struct {
	Type      string            `json:"@type"`
	Shards    []outMsgQueueSize `json:"shards"`
	SizeLimit uint64            `json:"ext_msg_queue_size_limit"`
}

type outMsgQueueSize struct {
	Type string     `json:"@type"`
	ID   blockIDExt `json:"id"`
	Size uint64     `json:"size"`
}

type configInfo struct {
	Type   string  `json:"@type"`
	Config tvmCell `json:"config"`
}

type tvmCell struct {
	Type  string `json:"@type"`
	Bytes string `json:"bytes"`
}

func (s *Server) handleConsensusBlock(ctx context.Context, _ requestParams) (any, *apiError) {
	block, _, genUTime, err := s.store.CurrentMasterchainInfo(ctx)
	if err != nil {
		return nil, internalError("cannot obtain a valid masterchain state")
	}
	if genUTime == 0 {
		genUTime = uint32(s.now().Unix())
	}

	return consensusBlock{
		Type:      consensusBlockType,
		Seqno:     block.SeqNo,
		Timestamp: genUTime,
	}, nil
}

func (s *Server) handleLookupBlock(ctx context.Context, params requestParams) (any, *apiError) {
	workchain, apiErr := params.requiredInt32("workchain")
	if apiErr != nil {
		return nil, apiErr
	}
	shard, apiErr := params.requiredInt64("shard")
	if apiErr != nil {
		return nil, apiErr
	}

	block, apiErr := s.lookupBlock(ctx, workchain, shard, params)
	if apiErr != nil {
		return nil, apiErr
	}
	return blockIDExtFromTON(block), nil
}

func (s *Server) handleBlockHeader(ctx context.Context, params requestParams) (any, *apiError) {
	id, apiErr := s.blockIDFromParams(ctx, params)
	if apiErr != nil {
		return nil, apiErr
	}

	block, apiErr := s.loadBlock(ctx, id)
	if apiErr != nil {
		return nil, apiErr
	}
	return blockHeaderFromBlock(id, block), nil
}

func (s *Server) handleShards(ctx context.Context, params requestParams) (any, *apiError) {
	seqno, apiErr := params.requiredUint32("seqno")
	if apiErr != nil {
		return nil, apiErr
	}

	block, err := s.store.LookupBlockBySeqNo(ctx, storage.BlockSeqRef{
		Workchain: masterchainWorkchain,
		Shard:     masterchainShard,
		SeqNo:     seqno,
	})
	if err != nil {
		return nil, internalError("cannot lookup masterchain block: " + err.Error())
	}

	fragments, err := s.store.BlockFragments(ctx, block)
	if err != nil {
		return nil, internalError("cannot load masterchain state: " + err.Error())
	}
	extra, err := fragments.McStateExtra()
	if err != nil {
		return nil, internalError("cannot unpack masterchain state: " + err.Error())
	}

	shards, err := shardBlocksFromMcStateExtra(extra)
	if err != nil {
		return nil, internalError("cannot load shard blocks: " + err.Error())
	}
	return shardsInfo{Type: shardsType, Shards: shards}, nil
}

func (s *Server) handleOutMsgQueueSize(ctx context.Context, _ requestParams) (any, *apiError) {
	current, err := s.store.CurrentState(ctx)
	if err != nil {
		return nil, internalError("cannot obtain current masterchain state: " + err.Error())
	}

	blocks := make([]ton.BlockIDExt, 0, 1+len(current.Shards))
	blocks = append(blocks, current.Masterchain.Block)
	for _, key := range storage.SortedShardKeys(current.Shards) {
		blocks = append(blocks, current.Shards[key].Block)
	}

	shards := make([]outMsgQueueSize, 0, len(blocks))
	for _, block := range blocks {
		fragments, err := s.store.BlockFragments(ctx, block)
		if err != nil {
			return nil, internalError("cannot load state: " + err.Error())
		}
		size, err := loadOutMsgQueueSize(fragments.StateRoot())
		if err != nil {
			return nil, internalError("cannot load out msg queue size: " + err.Error())
		}

		shards = append(shards, outMsgQueueSize{
			Type: outMsgQueueSizeType,
			ID:   blockIDExtFromTON(block),
			Size: size,
		})
	}

	return outMsgQueueSizes{
		Type:      outMsgQueueSizesType,
		Shards:    shards,
		SizeLimit: outMsgQueueSizeLimit,
	}, nil
}

func (s *Server) handleConfigParam(ctx context.Context, params requestParams) (any, *apiError) {
	configID, apiErr := configParamID(params)
	if apiErr != nil {
		return nil, apiErr
	}

	root, apiErr := s.configRoot(ctx, params)
	if apiErr != nil {
		return nil, apiErr
	}

	param := tlb.BlockchainConfig{Root: root}.Get(configID)
	return configInfoFromCell(param), nil
}

func configParamID(params requestParams) (int32, *apiError) {
	configID, hasConfigID, apiErr := params.optionalInt32("config_id")
	if apiErr != nil {
		return 0, apiErr
	}
	param, hasParam, apiErr := params.optionalInt32("param")
	if apiErr != nil {
		return 0, apiErr
	}
	if hasConfigID {
		return configID, nil
	}
	if hasParam {
		return param, nil
	}
	return 0, validationError("failed to validate request: only one of config_id or param should be specified")
}

func (s *Server) handleConfigAll(ctx context.Context, params requestParams) (any, *apiError) {
	root, apiErr := s.configRoot(ctx, params)
	if apiErr != nil {
		return nil, apiErr
	}
	return configInfoFromCell(root), nil
}

func (s *Server) blockIDFromParams(ctx context.Context, params requestParams) (ton.BlockIDExt, *apiError) {
	workchain, apiErr := params.requiredInt32("workchain")
	if apiErr != nil {
		return ton.BlockIDExt{}, apiErr
	}
	shard, apiErr := params.requiredInt64("shard")
	if apiErr != nil {
		return ton.BlockIDExt{}, apiErr
	}
	seqno, apiErr := params.requiredUint32("seqno")
	if apiErr != nil {
		return ton.BlockIDExt{}, apiErr
	}

	rootHash, hasRootHash, apiErr := optionalHashParam(params, "root_hash")
	if apiErr != nil {
		return ton.BlockIDExt{}, apiErr
	}
	fileHash, hasFileHash, apiErr := optionalHashParam(params, "file_hash")
	if apiErr != nil {
		return ton.BlockIDExt{}, apiErr
	}
	if hasRootHash != hasFileHash {
		return ton.BlockIDExt{}, validationError("failed to parse request: root_hash and file_hash must be specified together")
	}
	if hasRootHash {
		id := ton.BlockIDExt{Workchain: workchain, Shard: shard, SeqNo: seqno, RootHash: rootHash, FileHash: fileHash}
		if !blockproof.IsFullBlockID(id) {
			return ton.BlockIDExt{}, validationError("failed to parse request: invalid block id")
		}
		return id, nil
	}

	id, err := s.store.LookupBlockBySeqNo(ctx, storage.BlockSeqRef{Workchain: workchain, Shard: shard, SeqNo: seqno})
	if err != nil {
		return ton.BlockIDExt{}, internalError("cannot lookup block: " + err.Error())
	}
	return id, nil
}

func (s *Server) lookupBlock(ctx context.Context, workchain int32, shard int64, params requestParams) (ton.BlockIDExt, *apiError) {
	seqno, hasSeqno, apiErr := params.optionalUint32("seqno")
	if apiErr != nil {
		return ton.BlockIDExt{}, apiErr
	}
	lt, hasLT, apiErr := params.optionalUint64("lt")
	if apiErr != nil {
		return ton.BlockIDExt{}, apiErr
	}
	utime, hasUTime, apiErr := params.optionalUint32("unixtime")
	if apiErr != nil {
		return ton.BlockIDExt{}, apiErr
	}

	selectors := 0
	for _, ok := range []bool{hasSeqno, hasLT, hasUTime} {
		if ok {
			selectors++
		}
	}
	if selectors != 1 {
		return ton.BlockIDExt{}, validationError("failed to parse request: exactly one of seqno, lt or unixtime must be specified")
	}

	key := storage.BlockHistoryKey{Workchain: workchain, Shard: shard}
	var (
		block ton.BlockIDExt
		err   error
	)
	switch {
	case hasSeqno:
		block, err = s.store.LookupBlockBySeqNo(ctx, storage.BlockSeqRef{Workchain: workchain, Shard: shard, SeqNo: seqno})
	case hasLT:
		block, err = s.store.LookupBlockByLT(ctx, key, lt)
	default:
		block, err = s.store.LookupBlockByUnixTime(ctx, key, utime)
	}
	if err != nil {
		return ton.BlockIDExt{}, internalError("cannot lookup block: " + err.Error())
	}
	return block, nil
}

func (s *Server) loadBlock(ctx context.Context, id ton.BlockIDExt) (tlb.Block, *apiError) {
	root, err := s.store.BlockRoot(ctx, id)
	if err != nil {
		return tlb.Block{}, internalError("cannot load block: " + err.Error())
	}

	loader, err := root.BeginParse()
	if err != nil {
		return tlb.Block{}, internalError("cannot unpack block: " + err.Error())
	}

	var block tlb.Block
	if err = tlb.LoadFromCell(&block, loader); err != nil {
		return tlb.Block{}, internalError("cannot unpack block: " + err.Error())
	}
	return block, nil
}

func (s *Server) configRoot(ctx context.Context, params requestParams) (*cell.Cell, *apiError) {
	seqno, hasSeqno, apiErr := params.optionalUint32("seqno")
	if apiErr != nil {
		return nil, apiErr
	}

	var block ton.BlockIDExt
	var err error
	if hasSeqno {
		block, err = s.store.LookupBlockBySeqNo(ctx, storage.BlockSeqRef{
			Workchain: masterchainWorkchain,
			Shard:     masterchainShard,
			SeqNo:     seqno,
		})
	} else {
		block, _, _, err = s.store.CurrentMasterchainInfo(ctx)
	}
	if err != nil {
		return nil, internalError("cannot load masterchain info: " + err.Error())
	}

	fragments, err := s.store.BlockFragments(ctx, block)
	if err != nil {
		return nil, internalError("cannot load masterchain state: " + err.Error())
	}
	extra, err := fragments.McStateExtra()
	if err != nil {
		return nil, internalError("cannot unpack masterchain state: " + err.Error())
	}
	if extra.ConfigParams.Config.Params == nil {
		return nil, internalError("cannot load config: config dictionary is missing")
	}
	return extra.ConfigParams.Config.Params.AsCell(), nil
}

func blockHeaderFromBlock(id ton.BlockIDExt, block tlb.Block) blockHeader {
	info := block.BlockInfo
	return blockHeader{
		Type:                   blockHeaderType,
		ID:                     blockIDExtFromTON(id),
		GlobalID:               block.GlobalID,
		Version:                info.Version,
		AfterMerge:             info.AfterMerge,
		AfterSplit:             info.AfterSplit,
		BeforeSplit:            info.BeforeSplit,
		WantMerge:              info.WantMerge,
		WantSplit:              info.WantSplit,
		ValidatorListHashShort: info.GenValidatorListHashShort,
		CatchainSeqno:          info.GenCatchainSeqno,
		MinRefMCSeqno:          info.MinRefMcSeqno,
		IsKeyBlock:             info.KeyBlock,
		PrevKeyBlockSeqno:      info.PrevKeyBlockSeqno,
		StartLT:                fmt.Sprintf("%d", info.StartLt),
		EndLT:                  fmt.Sprintf("%d", info.EndLt),
		GenUTime:               info.GenUtime,
		VertSeqno:              info.VertSeqNo,
		PrevBlocks:             previousBlocks(id, info),
	}
}

func previousBlocks(id ton.BlockIDExt, info tlb.BlockHeader) []blockIDExt {
	shards := previousBlockShards(id.Shard, info)
	blocks := make([]blockIDExt, 0, 2)
	blocks = append(blocks, blockIDExtFromExtRef(id.Workchain, shards[0], info.PrevRef.Prev1))
	if info.PrevRef.Prev2 != nil {
		blocks = append(blocks, blockIDExtFromExtRef(id.Workchain, shards[1], *info.PrevRef.Prev2))
	}
	return blocks
}

func previousBlockShards(shard int64, info tlb.BlockHeader) [2]int64 {
	if info.AfterMerge && info.PrevRef.Prev2 != nil {
		return [2]int64{
			int64(tlb.ShardID(uint64(shard)).GetChild(true)),
			int64(tlb.ShardID(uint64(shard)).GetChild(false)),
		}
	}
	if info.AfterSplit {
		parent := int64(tlb.ShardID(uint64(shard)).GetParent())
		return [2]int64{parent, parent}
	}
	return [2]int64{shard, shard}
}

func blockIDExtFromExtRef(workchain int32, shard int64, ref tlb.ExtBlkRef) blockIDExt {
	return blockIDExtFromTON(ton.BlockIDExt{
		Workchain: workchain,
		Shard:     shard,
		SeqNo:     ref.SeqNo,
		RootHash:  ref.RootHash,
		FileHash:  ref.FileHash,
	})
}

func shardBlocksFromMcStateExtra(extra *tlb.McStateExtra) ([]blockIDExt, error) {
	if extra == nil || extra.ShardHashes == nil {
		return []blockIDExt{}, nil
	}

	entries, err := extra.ShardHashes.LoadAll()
	if err != nil {
		return nil, err
	}

	shards := make([]blockIDExt, 0)
	for _, entry := range entries {
		workchain, err := entry.Key.LoadInt(32)
		if err != nil {
			return nil, err
		}

		root, err := entry.Value.LoadRefCell()
		if err != nil {
			return nil, err
		}
		if err = appendShardBlocks(&shards, int32(workchain), root, 0, 0); err != nil {
			return nil, err
		}
	}

	sort.Slice(shards, func(i, j int) bool {
		if shards[i].Workchain != shards[j].Workchain {
			return shards[i].Workchain < shards[j].Workchain
		}
		return shards[i].Shard < shards[j].Shard
	})
	return shards, nil
}

func appendShardBlocks(out *[]blockIDExt, workchain int32, node *cell.Cell, prefix uint64, depth int) error {
	loader, err := node.BeginParse()
	if err != nil {
		return err
	}

	tag, err := loader.LoadUInt(1)
	if err != nil {
		return err
	}
	if tag == 0 {
		if depth > 63 {
			return fmt.Errorf("invalid shard depth")
		}
		shard := prefix | (uint64(1) << uint(63-depth))
		block, err := blockproof.BlockIDFromShardLeaf(workchain, int64(shard), node)
		if err != nil {
			return err
		}
		*out = append(*out, blockIDExtFromTON(block))
		return nil
	}
	if loader.BitsLeft() != 0 || loader.RefsNum() != 2 {
		return fmt.Errorf("invalid shard hash tree fork")
	}

	left, err := loader.LoadRefCell()
	if err != nil {
		return err
	}
	right, err := loader.LoadRefCell()
	if err != nil {
		return err
	}
	if err = appendShardBlocks(out, workchain, left, prefix, depth+1); err != nil {
		return err
	}
	return appendShardBlocks(out, workchain, right, prefix|(uint64(1)<<uint(63-depth)), depth+1)
}

func loadOutMsgQueueSize(stateRoot *cell.Cell) (uint64, error) {
	loader, err := stateRoot.BeginParse()
	if err != nil {
		return 0, err
	}

	var state tlb.ShardStateUnsplit
	if err = tlb.LoadFromCell(&state, loader); err != nil {
		return 0, err
	}
	if state.OutMsgQueueInfo == nil {
		return 0, fmt.Errorf("state is missing out_msg_queue_info")
	}

	queueLoader, err := state.OutMsgQueueInfo.BeginParse()
	if err != nil {
		return 0, err
	}

	var info tlb.OutMsgQueueInfo
	if err = tlb.LoadFromCell(&info, queueLoader); err != nil {
		return 0, err
	}
	if info.Extra == nil || info.Extra.OutQueueSize == nil {
		return 0, fmt.Errorf("no out_msg_queue_size in shard state")
	}
	return *info.Extra.OutQueueSize, nil
}

func configInfoFromCell(root *cell.Cell) configInfo {
	bytes := ""
	if root != nil {
		bytes = bocBase64(root)
	}
	return configInfo{
		Type: configInfoType,
		Config: tvmCell{
			Type:  tvmCellType,
			Bytes: bytes,
		},
	}
}

func optionalHashParam(params requestParams, name string) ([]byte, bool, *apiError) {
	raw, ok, apiErr := params.optionalNonEmptyString(name)
	if apiErr != nil || !ok {
		return nil, ok, apiErr
	}

	hash, err := parseHash(raw)
	if err != nil {
		return nil, false, hashParamError(params, name, raw)
	}
	return hash, true, nil
}
