package liteserver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"math/big"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/gton/internal/extmsg"
	"github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
	execop "github.com/xssnick/tonutils-go/tvm/op/exec"
	funcsop "github.com/xssnick/tonutils-go/tvm/op/funcs"
	stackop "github.com/xssnick/tonutils-go/tvm/op/stack"
	"github.com/xssnick/tonutils-go/tvm/tuple"
	vmcore "github.com/xssnick/tonutils-go/tvm/vm"
)

func TestHandleMasterchainInfoExtModeZero(t *testing.T) {
	block := testBlockID(t, 10, cell.BeginCell().MustStoreUInt(1, 8).EndCell())
	stateRoot := bytes.Repeat([]byte{0x11}, 32)
	store := &fakeStore{
		current: &storage.CurrentState{
			Masterchain: storage.BlockState{
				Block:         block,
				StateRootHash: stateRoot,
				Parsed:        &tlb.ShardStateUnsplit{GenUTime: 12345},
			},
		},
	}

	srv := testServer(store)
	resp := srv.handleQuery(context.Background(), ton.GetMasterchainInfoExt{Mode: 0})

	info, ok := resp.(ton.MasterchainInfoExt)
	if !ok {
		t.Fatalf("response type = %T, want ton.MasterchainInfoExt", resp)
	}
	if info.Mode != 0 || info.Version != DefaultVersion || info.Capabilities != DefaultCapabilities {
		t.Fatalf("unexpected version response: %+v", info)
	}
	if info.Last == nil || !blockIDEqual(*info.Last, block) {
		t.Fatalf("unexpected last block: %+v", info.Last)
	}
	if !bytes.Equal(info.StateRootHash, stateRoot) {
		t.Fatalf("unexpected state root")
	}
	if info.LastUTime != 12345 {
		t.Fatalf("LastUTime = %d, want 12345", info.LastUTime)
	}
	if info.Now != 1700000000 {
		t.Fatalf("Now = %d, want 1700000000", info.Now)
	}
}

func TestHandleMasterchainInfoExtSupportsShardClientStateMode(t *testing.T) {
	block := testBlockID(t, 10, cell.BeginCell().MustStoreUInt(1, 8).EndCell())
	stateRoot := bytes.Repeat([]byte{0x22}, 32)
	store := &fakeStore{
		current: &storage.CurrentState{
			Masterchain: storage.BlockState{Block: block, StateRootHash: stateRoot},
		},
	}
	srv := testServer(store)

	resp := srv.handleQuery(context.Background(), ton.GetMasterchainInfoExt{Mode: 0x80000000})
	info, ok := resp.(ton.MasterchainInfoExt)
	if !ok {
		t.Fatalf("high-bit response type = %T, want ton.MasterchainInfoExt", resp)
	}
	if info.Mode != 0x80000000 {
		t.Fatalf("mode = %d, want echoed high-bit mode", info.Mode)
	}

	resp = srv.handleQuery(context.Background(), ton.GetMasterchainInfoExt{Mode: getMasterchainInfoExtShardClientState})
	info, ok = resp.(ton.MasterchainInfoExt)
	if !ok {
		t.Fatalf("shard-client mode response type = %T, want ton.MasterchainInfoExt", resp)
	}
	if info.Mode != getMasterchainInfoExtShardClientState {
		t.Fatalf("mode = %d, want shard-client state mode", info.Mode)
	}
	if info.Last == nil || !blockIDEqual(*info.Last, block) {
		t.Fatalf("unexpected shard-client last block: %+v", info.Last)
	}

	resp = srv.handleQuery(context.Background(), ton.GetMasterchainInfoExt{Mode: 2})
	errResp, ok := resp.(ton.LSError)
	if !ok {
		t.Fatalf("unsupported mode response type = %T, want ton.LSError", resp)
	}
	if errResp.Code != errCodeProtoViolation {
		t.Fatalf("error code = %d, want %d", errResp.Code, errCodeProtoViolation)
	}
}

func TestHandleNonfinalQueriesReturnNotAllowed(t *testing.T) {
	block := testBlockID(t, 10, cell.BeginCell().MustStoreUInt(1, 8).EndCell())
	candidateID := &ton.NonfinalCandidateID{
		BlockID:          cloneBlockID(block),
		Creator:          bytes.Repeat([]byte{0x11}, 32),
		CollatedDataHash: bytes.Repeat([]byte{0x22}, 32),
	}
	queries := []tl.Serializable{
		ton.NonfinalGetValidatorGroups{},
		ton.NonfinalGetCandidate{ID: candidateID},
		ton.NonfinalGetPendingShardBlocks{},
	}

	srv := testServer(&fakeStore{})
	for _, query := range queries {
		resp, ok := srv.handleQuery(context.Background(), query).(ton.LSError)
		if !ok {
			t.Fatalf("%T response type = %T, want ton.LSError", query, resp)
		}
		if resp.Code != errCodeUnspecified {
			t.Fatalf("%T error code = %d, want %d", query, resp.Code, errCodeUnspecified)
		}
		if resp.Text != "query is not allowed" {
			t.Fatalf("%T error text = %q", query, resp.Text)
		}
	}
}

func TestHandleGetBlockDataReturnsStoredPayload(t *testing.T) {
	id := testBlockID(t, 3, cell.BeginCell().MustStoreUInt(0xaa, 8).EndCell())
	payload := []byte{0xde, 0xad, 0xbe, 0xef}
	store := &fakeStore{
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(id): payload,
		},
	}
	srv := testServer(store)

	resp := srv.handleQuery(context.Background(), ton.GetBlockData{ID: cloneBlockID(id)})
	data, ok := resp.(ton.BlockData)
	if !ok {
		t.Fatalf("response type = %T, want ton.BlockData", resp)
	}
	if data.ID == nil || !blockIDEqual(*data.ID, id) {
		t.Fatalf("unexpected block id: %+v", data.ID)
	}
	if !bytes.Equal(data.Payload, payload) {
		t.Fatalf("payload = %x, want %x", data.Payload, payload)
	}
}

func TestHandleSendMessageForwardsExternalBOC(t *testing.T) {
	accountID := bytes.Repeat([]byte{0x11}, 32)
	addr := address.NewAddress(0, 0xff, accountID)
	code := testCodeFromBuilders(t,
		stackop.DROP().Serialize(),
		stackop.DROP().Serialize(),
		stackop.DROP().Serialize(),
		stackop.DROP().Serialize(),
		stackop.DROP().Serialize(),
		funcsop.ACCEPT().Serialize(),
	)
	data := cell.BeginCell().EndCell()
	base := ton.BlockIDExt{Workchain: masterchainID, Shard: masterchainShard, SeqNo: 3}
	stateRoot, _ := testMasterStateWithActiveAccount(t, base, accountID, code, data)
	block, blockRoot := testBlockForState(t, masterchainID, masterchainShard, 3, stateRoot)
	store := &fakeStore{
		current: &storage.CurrentState{
			Masterchain: storage.BlockState{Block: block, StateRootHash: stateRoot.Hash(0), Cell: stateRoot},
		},
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(block): testBlockBOC(blockRoot),
		},
		blockStates: map[storage.BlockRootHash]*storage.BlockState{
			storage.BlockKey(block): {Block: block, StateRootHash: stateRoot.Hash(0), Cell: stateRoot},
		},
	}
	sender := &fakeMessageSender{}
	srv := testServer(store)
	srv.messageSender = sender

	msg, err := tlb.ToCell(&tlb.ExternalMessage{
		DstAddr: addr,
		Body:    cell.BeginCell().EndCell(),
	})
	if err != nil {
		t.Fatalf("build external message: %v", err)
	}
	body := msg.ToBOC()
	resp := srv.handleQuery(context.Background(), ton.SendMessage{Body: body})

	status, ok := resp.(ton.SendMessageStatus)
	if !ok {
		t.Fatalf("response type = %T, want ton.SendMessageStatus: %+v", resp, resp)
	}
	if status.Status != 1 {
		t.Fatalf("status = %d, want 1", status.Status)
	}
	if !bytes.Equal(sender.body, body) {
		t.Fatalf("forwarded body = %x, want %x", sender.body, body)
	}
	if want := externalMessageAddressKey(addr); sender.address != want {
		t.Fatalf("forwarded address key = %#v, want %#v", sender.address, want)
	}
	if sender.count != 1 {
		t.Fatalf("send count = %d, want 1", sender.count)
	}

	dup := srv.handleQuery(context.Background(), ton.SendMessage{Body: body})
	lsErr, ok := dup.(ton.LSError)
	if !ok {
		t.Fatalf("duplicate response type = %T, want ton.LSError: %+v", dup, dup)
	}
	if lsErr.Text != "cannot send external message : duplicate message" {
		t.Fatalf("duplicate error text = %q", lsErr.Text)
	}
	if sender.count != 1 {
		t.Fatalf("duplicate was forwarded, send count = %d", sender.count)
	}
}

func TestHandleSendMessageWithLiveStoreCachesBlockFragments(t *testing.T) {
	accountID := bytes.Repeat([]byte{0x41}, 32)
	addr := address.NewAddress(0, 0xff, accountID)
	code := testCodeFromBuilders(t,
		stackop.DROP().Serialize(),
		stackop.DROP().Serialize(),
		stackop.DROP().Serialize(),
		stackop.DROP().Serialize(),
		stackop.DROP().Serialize(),
		funcsop.ACCEPT().Serialize(),
	)
	data := cell.BeginCell().EndCell()
	base := ton.BlockIDExt{Workchain: masterchainID, Shard: masterchainShard, SeqNo: 4}
	stateRoot, _ := testMasterStateWithActiveAccount(t, base, accountID, code, data)
	block, blockRoot := testBlockForState(t, masterchainID, masterchainShard, base.SeqNo, stateRoot)
	blockState := storage.BlockState{Block: block, StateRootHash: stateRoot.Hash(0), Cell: stateRoot}
	backing := &fakeStore{
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(block): testBlockBOC(blockRoot),
		},
		blockStates: map[storage.BlockRootHash]*storage.BlockState{
			storage.BlockKey(block): &blockState,
		},
	}
	live := NewLiveStore(backing)
	if err := live.PublishLiveBlockArtifacts(storage.LiveBlockArtifacts{
		Block:     block,
		Root:      blockRoot,
		BlockData: testBlockBOC(blockRoot),
		State:     &blockState,
	}); err != nil {
		t.Fatalf("publish live block artifacts: %v", err)
	}
	if live.cachedBlockFragments(block) == nil {
		t.Fatal("live block fragments were not prepared on publish")
	}
	live.SetLiveCurrentState(&storage.CurrentState{Masterchain: blockState})

	sender := &fakeMessageSender{}
	srv := testServer(live)
	srv.messageSender = sender

	for i := 0; i < 2; i++ {
		msg, err := tlb.ToCell(&tlb.ExternalMessage{
			DstAddr: addr,
			Body:    cell.BeginCell().MustStoreUInt(uint64(i), 8).EndCell(),
		})
		if err != nil {
			t.Fatalf("build external message %d: %v", i, err)
		}

		resp := srv.handleQuery(context.Background(), ton.SendMessage{Body: msg.ToBOC()})
		status, ok := resp.(ton.SendMessageStatus)
		if !ok {
			t.Fatalf("response %d type = %T, want ton.SendMessageStatus: %+v", i, resp, resp)
		}
		if status.Status != 1 {
			t.Fatalf("status %d = %d, want 1", i, status.Status)
		}
	}

	if sender.count != 2 {
		t.Fatalf("send count = %d, want 2", sender.count)
	}
	if backing.loadStateCalls != 0 {
		t.Fatalf("backing state loads = %d, want 0", backing.loadStateCalls)
	}
}

func TestLiveStoreCurrentAccountBlocksUsesSpecificLiveShard(t *testing.T) {
	accountID := bytes.Repeat([]byte{0x42}, 32)
	candidates := storage.AccountShardCandidates(0, accountID)
	if len(candidates) < 2 {
		t.Fatal("expected account shard candidates")
	}

	masterState, masterRoot := benchmarkBlockState(t, masterchainID, masterchainShard, 10)
	topShardState, topShardRoot := benchmarkBlockState(t, 0, candidates[0], 20)
	specificShardState, specificShardRoot := benchmarkBlockState(t, 0, candidates[len(candidates)-1], 21)
	master := masterState.Block
	specificShard := specificShardState.Block
	live := NewLiveStore(&fakeStore{})
	if err := live.publishLiveBlockData(masterState.Block, masterRoot, testBlockBOC(masterRoot), true); err != nil {
		t.Fatalf("publish live master block: %v", err)
	}
	if err := live.publishLiveBlockData(topShardState.Block, topShardRoot, testBlockBOC(topShardRoot), true); err != nil {
		t.Fatalf("publish live top shard block: %v", err)
	}
	if err := live.publishLiveBlockData(specificShardState.Block, specificShardRoot, testBlockBOC(specificShardRoot), true); err != nil {
		t.Fatalf("publish live specific shard block: %v", err)
	}
	live.SetLiveCurrentState(&storage.CurrentState{
		Masterchain: masterState,
		Shards: map[storage.ShardKey]storage.BlockState{
			storage.ShardKeyFromBlock(topShardState.Block):      topShardState,
			storage.ShardKeyFromBlock(specificShardState.Block): specificShardState,
		},
	})

	blocks, err := live.CurrentAccountBlocks(context.Background(), 0, accountID)
	if err != nil {
		t.Fatalf("current account blocks: %v", err)
	}
	if !blockIDEqual(blocks.Master, master) {
		t.Fatalf("master block = %+v, want %+v", blocks.Master, master)
	}
	if !blockIDEqual(blocks.Account, specificShard) {
		t.Fatalf("account block = %+v, want %+v", blocks.Account, specificShard)
	}

	masterBlocks, err := live.CurrentAccountBlocks(context.Background(), masterchainID, accountID)
	if err != nil {
		t.Fatalf("masterchain account blocks: %v", err)
	}
	if !blockIDEqual(masterBlocks.Account, master) {
		t.Fatalf("masterchain account block = %+v, want %+v", masterBlocks.Account, master)
	}
}

func TestLiveStoreCurrentAccountBlocksUsesStoredCurrentInit(t *testing.T) {
	accountID := bytes.Repeat([]byte{0x43}, 32)
	candidates := storage.AccountShardCandidates(0, accountID)
	masterStateRoot := cell.BeginCell().MustStoreUInt(0x43, 8).EndCell()
	master, masterRoot := testBlockForState(t, masterchainID, masterchainShard, 10, masterStateRoot)
	masterData := testBlockBOC(masterRoot)
	master = testBlockIDForData(masterchainID, masterchainShard, master.SeqNo, masterRoot, masterData)
	shardStateRoot := cell.BeginCell().MustStoreUInt(0x44, 8).EndCell()
	shard, shardRoot := testBlockForState(t, 0, candidates[len(candidates)-1], 20, shardStateRoot)
	shardData := testBlockBOC(shardRoot)
	shard = testBlockIDForData(0, candidates[len(candidates)-1], shard.SeqNo, shardRoot, shardData)
	backing := &fakeStore{
		current: &storage.CurrentState{
			Masterchain: storage.BlockState{Block: master, StateRootHash: masterStateRoot.Hash(0)},
			Shards: map[storage.ShardKey]storage.BlockState{
				storage.ShardKeyFromBlock(shard): {Block: shard, StateRootHash: shardStateRoot.Hash(0)},
			},
		},
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(master): masterData,
			storage.BlockKey(shard):  shardData,
		},
		blockStates: map[storage.BlockRootHash]*storage.BlockState{
			storage.BlockKey(master): {Block: master, StateRootHash: masterStateRoot.Hash(0), Cell: masterStateRoot},
			storage.BlockKey(shard):  {Block: shard, StateRootHash: shardStateRoot.Hash(0), Cell: shardStateRoot},
		},
	}
	live := NewLiveStore(backing)

	blocks, err := live.CurrentAccountBlocks(context.Background(), 0, accountID)
	if err != nil {
		t.Fatalf("current account blocks after init: %v", err)
	}
	if !blockIDEqual(blocks.Master, master) || !blockIDEqual(blocks.Account, shard) {
		t.Fatalf("current account blocks = %+v, want master=%+v account=%+v", blocks, master, shard)
	}

	backing.current = nil
	backing.currentCalls = 0
	blocks, err = live.CurrentAccountBlocks(context.Background(), 0, accountID)
	if err != nil {
		t.Fatalf("current account blocks after backing removal: %v", err)
	}
	if backing.currentCalls != 0 {
		t.Fatalf("backing current calls after init = %d, want 0", backing.currentCalls)
	}
}

func TestHandleSendMessageRejectsUnacceptedExternalMessage(t *testing.T) {
	accountID := bytes.Repeat([]byte{0x12}, 32)
	addr := address.NewAddress(0, 0xff, accountID)
	code := testCodeFromBuilders(t,
		stackop.DROP().Serialize(),
		stackop.DROP().Serialize(),
		stackop.DROP().Serialize(),
		stackop.DROP().Serialize(),
		stackop.DROP().Serialize(),
	)
	base := ton.BlockIDExt{Workchain: masterchainID, Shard: masterchainShard, SeqNo: 3}
	stateRoot, _ := testMasterStateWithActiveAccount(t, base, accountID, code, cell.BeginCell().EndCell())
	block, blockRoot := testBlockForState(t, masterchainID, masterchainShard, 3, stateRoot)
	store := &fakeStore{
		current: &storage.CurrentState{
			Masterchain: storage.BlockState{Block: block, StateRootHash: stateRoot.Hash(0), Cell: stateRoot},
		},
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(block): testBlockBOC(blockRoot),
		},
		blockStates: map[storage.BlockRootHash]*storage.BlockState{
			storage.BlockKey(block): {Block: block, StateRootHash: stateRoot.Hash(0), Cell: stateRoot},
		},
	}
	sender := &fakeMessageSender{}
	srv := testServer(store)
	srv.messageSender = sender

	msg, err := tlb.ToCell(&tlb.ExternalMessage{
		DstAddr: addr,
		Body:    cell.BeginCell().EndCell(),
	})
	if err != nil {
		t.Fatalf("build external message: %v", err)
	}

	resp := srv.handleQuery(context.Background(), ton.SendMessage{Body: msg.ToBOC()})
	lsErr, ok := resp.(ton.LSError)
	if !ok {
		t.Fatalf("response type = %T, want ton.LSError: %+v", resp, resp)
	}
	if !strings.Contains(lsErr.Text, "external message was not accepted") {
		t.Fatalf("error text = %q", lsErr.Text)
	}
	if !strings.HasPrefix(lsErr.Text, "cannot apply external message to current state : ") {
		t.Fatalf("error text = %q, want C++ prefix", lsErr.Text)
	}
	if sender.body != nil {
		t.Fatalf("unaccepted message was forwarded: %x", sender.body)
	}
}

func TestHandleSendMessageNotConfiguredUsesCppPrefix(t *testing.T) {
	srv := testServer(&fakeStore{})

	resp := srv.handleQuery(context.Background(), ton.SendMessage{Body: []byte{0xb5, 0xee, 0x9c, 0x72}})
	lsErr, ok := resp.(ton.LSError)
	if !ok {
		t.Fatalf("response type = %T, want ton.LSError: %+v", resp, resp)
	}
	if lsErr.Text != "cannot apply external message to current state : not configured" {
		t.Fatalf("error text = %q", lsErr.Text)
	}
}

func TestSendMessageBroadcastCapacityErrorReason(t *testing.T) {
	got := sendMessageBroadcastErrorReason(extmsg.ErrExternalBroadcastCapacityExceeded)
	if got != sendMessageErrorReasonBroadcastCapacity {
		t.Fatalf("broadcast error reason = %q, want %q", got, sendMessageErrorReasonBroadcastCapacity)
	}
}

func TestSendMessageTVMTraceLogIncludesBlockAndC7(t *testing.T) {
	var out bytes.Buffer
	logger := zerolog.New(&out).Level(zerolog.WarnLevel)
	srv := testServer(&fakeStore{})
	srv.log = logger

	addr := address.NewAddress(0, 0xff, bytes.Repeat([]byte{0x33}, 32))
	master := ton.BlockIDExt{
		Workchain: masterchainID,
		Shard:     masterchainShard,
		SeqNo:     11,
		RootHash:  bytes.Repeat([]byte{0x11}, 32),
		FileHash:  bytes.Repeat([]byte{0x12}, 32),
	}
	shard := ton.BlockIDExt{
		Workchain: 0,
		Shard:     0x4000000000000000,
		SeqNo:     22,
		RootHash:  bytes.Repeat([]byte{0x21}, 32),
		FileHash:  bytes.Repeat([]byte{0x22}, 32),
	}

	stackBefore := vmcore.NewStack()
	if err := stackBefore.PushInt(big.NewInt(1)); err != nil {
		t.Fatalf("push trace stack before: %v", err)
	}
	stackAfter := vmcore.NewStack()
	if err := stackAfter.PushInt(big.NewInt(2)); err != nil {
		t.Fatalf("push trace stack after: %v", err)
	}

	srv.logSendMessageTVMTrace(
		errors.New("rejected"),
		false,
		[]vmcore.TraceStep{
			{Step: 1, Opcode: "PUSHINT 1", GasRemaining: 999, Stack: stackBefore},
			{Step: 2, Opcode: "THROWIF", GasRemaining: 990, Stack: stackAfter},
		},
		addr,
		master,
		shard,
		runMethodShardHeader{GenUTime: 100, GenLT: 200},
		runMethodShardHeader{GenUTime: 300, GenLT: 400},
		tvm.CheckExternalMessageAcceptedConfig{Now: 300, BlockLT: 400, LogicalTime: 402},
		13,
	)

	log := out.String()
	for _, want := range []string{
		`"accepted":false`,
		`"vm_trace_steps":2`,
		`"master_seqno":11`,
		`"master_gen_utime":100`,
		`"master_gen_lt":200`,
		`"execution_seqno":22`,
		`"execution_gen_utime":300`,
		`"execution_gen_lt":400`,
		`"global_version":13`,
		`"c7_now":300`,
		`"c7_block_lt":400`,
		`"c7_logical_time":402`,
		`#1 PUSHINT 1 gas=999\ns0 = 1 [int]`,
		`#2 THROWIF gas=990\ns0 = 2 [int]`,
	} {
		if !strings.Contains(log, want) {
			t.Fatalf("trace log missing %s in %s", want, log)
		}
	}
}

func TestHandleSendMessageRejectsInvalidExternalMessageLikeCpp(t *testing.T) {
	srv := testServer(&fakeStore{})
	srv.messageSender = &fakeMessageSender{}

	resp := srv.handleQuery(context.Background(), ton.SendMessage{Body: cell.BeginCell().EndCell().ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false})})
	lsErr, ok := resp.(ton.LSError)
	if !ok {
		t.Fatalf("response type = %T, want ton.LSError: %+v", resp, resp)
	}
	if lsErr.Code != errCodeUnspecified {
		t.Fatalf("error code = %d, want %d", lsErr.Code, errCodeUnspecified)
	}
	want := "cannot apply external message to current state : external message must begin with ext_in_msg_info$10"
	if lsErr.Text != want {
		t.Fatalf("error text = %q, want %q", lsErr.Text, want)
	}
}

func TestHandleSendMessageRejectsAddressOverLimitBeforeTVM(t *testing.T) {
	accountID := bytes.Repeat([]byte{0x31}, 32)
	addr := address.NewAddress(0, 0xff, accountID)
	srv := testServer(&fakeStore{})
	srv.messageSender = &fakeMessageSender{}

	key := externalMessageAddressKey(addr)
	now := srv.now()
	for i := 0; i < extmsg.DefaultAddressLimit; i++ {
		if err := srv.externalMessageLimiter.Add(key, now); err != nil {
			t.Fatalf("preload limiter %d: %v", i, err)
		}
	}

	msg, err := tlb.ToCell(&tlb.ExternalMessage{
		DstAddr:   addr,
		ImportFee: tlb.ZeroCoins,
		Body:      cell.BeginCell().EndCell(),
	})
	if err != nil {
		t.Fatalf("build external message: %v", err)
	}

	resp := srv.handleQuery(context.Background(), ton.SendMessage{Body: msg.ToBOC()})
	lsErr, ok := resp.(ton.LSError)
	if !ok {
		t.Fatalf("response type = %T, want ton.LSError: %+v", resp, resp)
	}
	if !strings.Contains(lsErr.Text, "too many external messages to address") {
		t.Fatalf("error text = %q", lsErr.Text)
	}
	if sender := srv.messageSender.(*fakeMessageSender); sender.count != 0 {
		t.Fatalf("rate-limited message was forwarded, send count = %d", sender.count)
	}
}

func TestHandleSendMessageRejectsOversizeBeforeTVM(t *testing.T) {
	srv := testServer(&fakeStore{})
	sender := &fakeMessageSender{}
	srv.messageSender = sender

	body := make([]byte, maxExternalMessageBroadcastDataSize+1)
	resp := srv.handleQuery(context.Background(), ton.SendMessage{Body: body})
	lsErr, ok := resp.(ton.LSError)
	if !ok {
		t.Fatalf("response type = %T, want ton.LSError: %+v", resp, resp)
	}

	want := "cannot apply external message to current state : external message is too large: 16777217"
	if lsErr.Text != want {
		t.Fatalf("error text = %q, want %q", lsErr.Text, want)
	}
	if sender.count != 0 {
		t.Fatalf("oversize message was forwarded, send count = %d", sender.count)
	}

	again := srv.handleQuery(context.Background(), ton.SendMessage{Body: body})
	againErr, ok := again.(ton.LSError)
	if !ok {
		t.Fatalf("second response type = %T, want ton.LSError: %+v", again, again)
	}
	if againErr.Text != want {
		t.Fatalf("second error text = %q, want %q", againErr.Text, want)
	}
}

func TestQueryObservationMarksUnspecifiedLSErrorAsError(t *testing.T) {
	observation := queryObservationFromResponse(
		ton.LSError{Code: errCodeUnspecified, Text: "cannot apply external message to current state : External message was not accepted"},
		queryLogTiming{query: "SendMessage", errorReason: sendMessageErrorReasonTVMRejected, duration: time.Millisecond},
	)

	if !observation.Error {
		t.Fatal("expected LSError observation to be marked as error")
	}
	if observation.ErrorCode != errCodeUnspecified {
		t.Fatalf("error code = %d, want %d", observation.ErrorCode, errCodeUnspecified)
	}
	if observation.Method != "SendMessage" {
		t.Fatalf("method = %q, want SendMessage", observation.Method)
	}
	if observation.ErrorReason != sendMessageErrorReasonTVMRejected {
		t.Fatalf("error reason = %q, want %q", observation.ErrorReason, sendMessageErrorReasonTVMRejected)
	}
}

func TestHandleStandaloneQueryWithTimingReportsSendMessageReason(t *testing.T) {
	srv := testServer(&fakeStore{})

	resp, timing := srv.handleStandaloneQueryWithTiming(context.Background(), ton.SendMessage{Body: []byte{0xb5, 0xee, 0x9c, 0x72}})
	if _, ok := resp.(ton.LSError); !ok {
		t.Fatalf("response type = %T, want ton.LSError: %+v", resp, resp)
	}
	if timing.errorReason != sendMessageErrorReasonNotConfigured {
		t.Fatalf("error reason = %q, want %q", timing.errorReason, sendMessageErrorReasonNotConfigured)
	}
}

func TestLiveStoreServesPendingBlockBeforeStorageFlush(t *testing.T) {
	root := cell.BeginCell().MustStoreUInt(0xbb, 8).EndCell()
	payload := testBlockBOC(root)
	id := testBlockIDForData(0, int64(0x4000000000000000), 5, root, payload)
	live := NewLiveStore(&fakeStore{}, LiveStoreOptions{MasterBlockCache: 0, ShardBlockCache: 0})

	if err := live.publishLiveBlockData(id, root, payload, false); err != nil {
		t.Fatalf("set live block: %v", err)
	}

	cachedRoot, err := live.BlockRoot(context.Background(), id)
	if err != nil {
		t.Fatalf("load cached root: %v", err)
	}
	if cachedRoot != root {
		t.Fatal("expected cached cell tree to be returned")
	}

	resp := testServer(live).handleQuery(context.Background(), ton.GetBlockData{ID: cloneBlockID(id)})
	data, ok := resp.(ton.BlockData)
	if !ok {
		t.Fatalf("response type = %T, want ton.BlockData: %+v", resp, resp)
	}
	if !bytes.Equal(data.Payload, payload) {
		t.Fatalf("payload = %x, want %x", data.Payload, payload)
	}
}

func TestLiveStoreDoesNotPublishInvalidArtifactsToSharedBlockCache(t *testing.T) {
	shared := storage.NewLiveBlockCache(8)
	live := NewLiveStore(&fakeStore{}, LiveStoreOptions{LiveBlockCache: shared})

	expectedRoot := cell.BeginCell().MustStoreUInt(0x31, 8).EndCell()
	wrongRoot := cell.BeginCell().MustStoreUInt(0x32, 8).EndCell()
	block := testBlockIDForRoot(0, int64(0x4000000000000000), 33, expectedRoot)

	err := live.PublishLiveBlockArtifacts(storage.LiveBlockArtifacts{
		Block:     block,
		Root:      wrongRoot,
		BlockData: testBlockBOC(wrongRoot),
	})
	if err == nil {
		t.Fatal("invalid live artifact was accepted")
	}
	if _, err = shared.BlockData(context.Background(), block); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("shared cache block data error = %v, want ErrNotFound", err)
	}
}

func TestParseTrustedBlockBOCReadsMode31LazyBOC(t *testing.T) {
	child := cell.BeginCell().MustStoreUInt(0xcd, 8).EndCell()
	root := cell.BeginCell().MustStoreUInt(0xab, 8).MustStoreRef(child).EndCell()
	payload := root.ToBOCWithOptions(cell.BOCSerializeOptions{
		WithCRC32C:    true,
		WithIndex:     true,
		WithCacheBits: true,
		WithTopHash:   true,
		WithIntHashes: true,
	})
	id := testBlockIDForData(0, int64(0x4000000000000000), 6, root, payload)

	parsed, err := parseTrustedBlockBOC(id, payload)
	if err != nil {
		t.Fatalf("parse trusted block boc: %v", err)
	}
	if !bytes.Equal(parsed.Hash(), root.Hash()) {
		t.Fatalf("root hash = %x, want %x", parsed.Hash(), root.Hash())
	}

	parsedChild, err := parsed.MustBeginParse().LoadRefCell()
	if err != nil {
		t.Fatalf("load child ref: %v", err)
	}
	if !bytes.Equal(parsedChild.Hash(), child.Hash()) {
		t.Fatalf("child hash = %x, want %x", parsedChild.Hash(), child.Hash())
	}
}

func TestParseTrustedBlockBOCReadsBOCWithoutStoredRootHashes(t *testing.T) {
	child := cell.BeginCell().MustStoreUInt(0xcd, 8).EndCell()
	root := cell.BeginCell().MustStoreUInt(0xab, 8).MustStoreRef(child).EndCell()
	payload := root.ToBOCWithOptions(cell.BOCSerializeOptions{
		WithCRC32C:    true,
		WithIndex:     true,
		WithCacheBits: true,
	})
	id := testBlockIDForData(0, int64(0x4000000000000000), 7, root, payload)

	parsed, err := parseTrustedBlockBOC(id, payload)
	if err != nil {
		t.Fatalf("parse trusted block boc: %v", err)
	}
	if !bytes.Equal(parsed.Hash(), root.Hash()) {
		t.Fatalf("root hash = %x, want %x", parsed.Hash(), root.Hash())
	}
}

func TestLiveStoreKeepsPendingBlocksOverLimitUntilFlush(t *testing.T) {
	live := NewLiveStore(&fakeStore{}, LiveStoreOptions{MasterBlockCache: 1, ShardBlockCache: 1})
	var ids []ton.BlockIDExt

	for seqno := uint32(1); seqno <= 3; seqno++ {
		root := cell.BeginCell().MustStoreUInt(uint64(seqno), 8).EndCell()
		payload := testBlockBOC(root)
		id := testBlockIDForData(0, int64(0x4000000000000000), seqno, root, payload)
		if err := live.publishLiveBlockData(id, root, payload, false); err != nil {
			t.Fatalf("set pending live block %d: %v", seqno, err)
		}
		ids = append(ids, id)
	}

	for _, id := range ids {
		if _, err := live.BlockData(context.Background(), id); err != nil {
			t.Fatalf("pending block %d was trimmed before flush: %v", id.SeqNo, err)
		}
	}

	for _, id := range ids {
		live.MarkLiveBlockFlushed(id)
	}

	if _, err := live.BlockData(context.Background(), ids[0]); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("oldest flushed block error = %v, want ErrNotFound", err)
	}
	if _, err := live.BlockData(context.Background(), ids[1]); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("second flushed block error = %v, want ErrNotFound", err)
	}
	if _, err := live.BlockData(context.Background(), ids[2]); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("latest flushed block error = %v, want ErrNotFound", err)
	}
}

func TestLiveStoreKeepsUnflushedArtifactsUnderSharedCachePressure(t *testing.T) {
	shared := storage.NewLiveBlockCache(1)
	live := NewLiveStore(&fakeStore{}, LiveStoreOptions{
		MasterBlockCache: 0,
		ShardBlockCache:  0,
		LiveBlockCache:   shared,
	})

	firstRoot := cell.BeginCell().MustStoreUInt(0x41, 8).EndCell()
	firstData := testBlockBOC(firstRoot)
	first := testBlockIDForData(masterchainID, masterchainShard, 41, firstRoot, firstData)
	firstProof := []byte("first live proof")
	if err := live.PublishLiveBlockArtifacts(storage.LiveBlockArtifacts{
		Block:     first,
		Root:      firstRoot,
		BlockData: firstData,
		Proofs: []storage.LiveBlockProofArtifact{{
			Kind: storage.ServedProofBlock,
			Data: firstProof,
		}},
	}); err != nil {
		t.Fatalf("publish first live artifacts: %v", err)
	}

	live.SetLiveCurrentState(&storage.CurrentState{
		Masterchain: storage.BlockState{Block: first},
	})

	secondRoot := cell.BeginCell().MustStoreUInt(0x42, 8).EndCell()
	secondData := testBlockBOC(secondRoot)
	second := testBlockIDForData(masterchainID, masterchainShard, 42, secondRoot, secondData)
	if err := live.PublishLiveBlockArtifacts(storage.LiveBlockArtifacts{
		Block:     second,
		Root:      secondRoot,
		BlockData: secondData,
	}); err != nil {
		t.Fatalf("publish second live artifacts: %v", err)
	}

	gotData, err := live.BlockData(context.Background(), first)
	if err != nil {
		t.Fatalf("current block data was evicted before flush: %v", err)
	}
	if !bytes.Equal(gotData, firstData) {
		t.Fatalf("current block data = %x, want %x", gotData, firstData)
	}
	gotProof, err := live.BlockProof(context.Background(), storage.ServedProofBlock, first)
	if err != nil {
		t.Fatalf("current block proof was evicted before flush: %v", err)
	}
	if !bytes.Equal(gotProof, firstProof) {
		t.Fatalf("current block proof = %x, want %x", gotProof, firstProof)
	}

	live.MarkLiveBlockFlushed(first)
	if _, err = live.BlockData(context.Background(), first); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("flushed current block data error = %v, want ErrNotFound", err)
	}
}

func TestLiveStoreReplacingFlushedBlockKeepsTrimCountStable(t *testing.T) {
	live := NewLiveStore(&fakeStore{}, LiveStoreOptions{MasterBlockCache: 0, ShardBlockCache: 2})

	firstRoot := cell.BeginCell().MustStoreUInt(0x31, 8).EndCell()
	firstData := testBlockBOC(firstRoot)
	first := testBlockIDForData(0, int64(0x4000000000000000), 31, firstRoot, firstData)

	secondRoot := cell.BeginCell().MustStoreUInt(0x32, 8).EndCell()
	secondData := testBlockBOC(secondRoot)
	second := testBlockIDForData(0, int64(0x4000000000000000), 32, secondRoot, secondData)

	if err := live.publishLiveBlockData(first, firstRoot, firstData, true); err != nil {
		t.Fatalf("set first live block: %v", err)
	}
	if err := live.publishLiveBlockData(first, firstRoot, firstData, true); err != nil {
		t.Fatalf("replace first live block: %v", err)
	}
	if err := live.publishLiveBlockData(second, secondRoot, secondData, true); err != nil {
		t.Fatalf("set second live block: %v", err)
	}

	if _, err := live.BlockData(context.Background(), first); err != nil {
		t.Fatalf("replaced first block should stay cached: %v", err)
	}
	if _, err := live.BlockData(context.Background(), second); err != nil {
		t.Fatalf("second block should stay cached: %v", err)
	}
}

func TestLiveStoreZeroStateReadsStorage(t *testing.T) {
	id := ton.BlockIDExt{
		Workchain: masterchainID,
		Shard:     masterchainShard,
		SeqNo:     0,
		RootHash:  bytes.Repeat([]byte{0x11}, 32),
		FileHash:  bytes.Repeat([]byte{0x22}, 32),
	}
	data := []byte{0xde, 0xad}
	live := NewLiveStore(&fakeStore{
		zeroStates: map[storage.BlockRootHash][]byte{
			storage.BlockKey(id): data,
		},
	})

	got, err := live.ZeroState(context.Background(), id)
	if err != nil {
		t.Fatalf("zero state: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("zero state data = %x, want %x", got, data)
	}

	if _, err = live.ZeroState(context.Background(), ton.BlockIDExt{Workchain: masterchainID, Shard: masterchainShard, SeqNo: 0}); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("missing zero state error = %v, want ErrNotFound", err)
	}
}

func TestLiveStorePublishesCurrentOnlyAfterShardDataFlushed(t *testing.T) {
	current, masterRoot, masterData := testCurrentStateWithLiveBlock(t, 21)
	shardRoot := cell.BeginCell().MustStoreUInt(0x21, 8).EndCell()
	shardData := testBlockBOC(shardRoot)
	shard := testBlockIDForData(0, masterchainShard, 21, shardRoot, shardData)
	current.Shards = map[storage.ShardKey]storage.BlockState{
		storage.ShardKeyFromBlock(shard): {
			Block: shard,
		},
	}

	store := &fakeStore{}
	live := NewLiveStore(store)
	live.SetLiveCurrentState(current)
	if _, err := live.CurrentState(context.Background()); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("current state before shard data = %v, want ErrNotFound", err)
	}

	if err := live.publishLiveBlockData(shard, shardRoot, shardData, false); err != nil {
		t.Fatalf("set unflushed shard block: %v", err)
	}
	if _, err := live.CurrentState(context.Background()); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("current state before shard flush = %v, want ErrNotFound", err)
	}

	live.MarkLiveBlockFlushed(shard)
	if _, err := live.CurrentState(context.Background()); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("current state before master flush = %v, want ErrNotFound", err)
	}

	if err := live.publishLiveBlockData(current.Masterchain.Block, masterRoot, masterData, true); err != nil {
		t.Fatalf("set flushed master block: %v", err)
	}
	got, err := live.CurrentState(context.Background())
	if err != nil {
		t.Fatalf("current state after master and shard flush: %v", err)
	}
	if got.Masterchain.Block.SeqNo != current.Masterchain.Block.SeqNo {
		t.Fatalf("published master seqno = %d, want %d", got.Masterchain.Block.SeqNo, current.Masterchain.Block.SeqNo)
	}
}

func TestLiveStorePublishesCurrentFromFlushedMarkersWithoutPayload(t *testing.T) {
	current, _, masterData := testCurrentStateWithLiveBlock(t, 23)
	shardRoot := cell.BeginCell().MustStoreUInt(0x23, 8).EndCell()
	shardData := testBlockBOC(shardRoot)
	shard := testBlockIDForData(0, masterchainShard, 23, shardRoot, shardData)
	current.Shards = map[storage.ShardKey]storage.BlockState{
		storage.ShardKeyFromBlock(shard): {
			Block:         shard,
			StateRootHash: shardRoot.Hash(0),
		},
	}

	store := &fakeStore{
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(current.Masterchain.Block): masterData,
			storage.BlockKey(shard):                     shardData,
		},
	}
	live := NewLiveStore(store)
	live.SetLiveCurrentState(current)
	if _, err := live.CurrentState(context.Background()); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("current state before markers = %v, want ErrNotFound", err)
	}

	for _, state := range []storage.BlockState{current.Masterchain, current.Shards[storage.ShardKeyFromBlock(shard)]} {
		if err := live.PublishLiveBlockArtifacts(storage.LiveBlockArtifacts{
			Block:           state.Block,
			Meta:            storage.BuildBlockMetaFromState(state),
			State:           &state,
			ArtifactFlushed: true,
			StateFlushed:    true,
		}); err != nil {
			t.Fatalf("publish marker %s: %v", storage.FormatBlockRef(state.Block), err)
		}
	}

	got, err := live.CurrentState(context.Background())
	if err != nil {
		t.Fatalf("current state after markers: %v", err)
	}
	if got.Masterchain.Block.SeqNo != current.Masterchain.Block.SeqNo {
		t.Fatalf("published master seqno = %d, want %d", got.Masterchain.Block.SeqNo, current.Masterchain.Block.SeqNo)
	}

	data, err := live.BlockData(context.Background(), current.Masterchain.Block)
	if err != nil {
		t.Fatalf("load marker-backed master block data: %v", err)
	}
	if !bytes.Equal(data, masterData) {
		t.Fatal("master block data was not loaded from backing store")
	}
	if store.blockDataCalls == 0 {
		t.Fatal("expected block data lookup to hit backing store")
	}
}

func TestLiveStorePinsCurrentBlocksOverCacheLimit(t *testing.T) {
	current, masterRoot, masterData := testCurrentStateWithLiveBlock(t, 22)
	shardRoot := cell.BeginCell().MustStoreUInt(0x22, 8).EndCell()
	shardData := testBlockBOC(shardRoot)
	shard := testBlockIDForData(0, masterchainShard, 22, shardRoot, shardData)
	current.Shards = map[storage.ShardKey]storage.BlockState{
		storage.ShardKeyFromBlock(shard): {Block: shard},
	}

	live := NewLiveStore(&fakeStore{}, LiveStoreOptions{MasterBlockCache: 0, ShardBlockCache: 0})
	live.SetLiveCurrentState(current)
	if err := live.publishLiveBlockData(current.Masterchain.Block, masterRoot, masterData, true); err != nil {
		t.Fatalf("set live master block: %v", err)
	}
	if err := live.publishLiveBlockData(shard, shardRoot, shardData, true); err != nil {
		t.Fatalf("set live shard block: %v", err)
	}

	if _, err := live.CurrentState(context.Background()); err != nil {
		t.Fatalf("current should be published with pinned blocks: %v", err)
	}
	if _, err := live.BlockData(context.Background(), current.Masterchain.Block); err != nil {
		t.Fatalf("current master block was trimmed: %v", err)
	}
	if _, err := live.BlockData(context.Background(), shard); err != nil {
		t.Fatalf("current shard block was trimmed: %v", err)
	}
}

func TestLiveStoreStoredCurrentInitRequiresReadableBlocks(t *testing.T) {
	stateRoot := cell.BeginCell().MustStoreUInt(0x23, 8).EndCell()
	block, root := testBlockForState(t, masterchainID, masterchainShard, 23, stateRoot)
	data := testBlockBOC(root)
	block = testBlockIDForData(masterchainID, masterchainShard, block.SeqNo, root, data)
	current := &storage.CurrentState{
		Masterchain: storage.BlockState{
			Block:         block,
			StateRootHash: stateRoot.Hash(0),
		},
	}
	store := &fakeStore{current: current}
	live := NewLiveStore(store)

	if _, err := live.CurrentState(context.Background()); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("current state before block data = %v, want ErrNotFound", err)
	}

	store.blocks = map[storage.BlockRootHash][]byte{
		storage.BlockKey(current.Masterchain.Block): data,
	}
	live = NewLiveStore(store)
	if _, err := live.CurrentState(context.Background()); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("current state before state cells = %v, want ErrNotFound", err)
	}

	store.blockStates = map[storage.BlockRootHash]*storage.BlockState{
		storage.BlockKey(current.Masterchain.Block): {
			Block:         current.Masterchain.Block,
			StateRootHash: stateRoot.Hash(0),
			Cell:          stateRoot,
		},
	}

	live = NewLiveStore(store)
	got, err := live.CurrentState(context.Background())
	if err != nil {
		t.Fatalf("current state after init: %v", err)
	}
	if !blockIDEqual(got.Masterchain.Block, current.Masterchain.Block) {
		t.Fatalf("current block = %+v, want %+v", got.Masterchain.Block, current.Masterchain.Block)
	}

	store.current = nil
	got, err = live.CurrentState(context.Background())
	if err != nil {
		t.Fatalf("current state after backing removal: %v", err)
	}
	if !blockIDEqual(got.Masterchain.Block, current.Masterchain.Block) {
		t.Fatalf("in-memory current block = %+v, want %+v", got.Masterchain.Block, current.Masterchain.Block)
	}
}

func TestLiveStoreCurrentMasterchainInfoUsesStoredCurrentInit(t *testing.T) {
	stateRoot := cell.BeginCell().MustStoreUInt(0x25, 8).EndCell()
	block, root := testBlockForState(t, masterchainID, masterchainShard, 25, stateRoot)
	data := testBlockBOC(root)
	block = testBlockIDForData(masterchainID, masterchainShard, block.SeqNo, root, data)
	stateRootHash := stateRoot.Hash(0)
	current := &storage.CurrentState{
		Masterchain: storage.BlockState{
			Block:         block,
			StateRootHash: stateRootHash,
		},
	}
	store := &fakeStore{current: current}
	live := NewLiveStore(store)

	if _, _, _, err := live.CurrentMasterchainInfo(context.Background()); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("masterchain info before block data = %v, want ErrNotFound", err)
	}

	store.blocks = map[storage.BlockRootHash][]byte{
		storage.BlockKey(current.Masterchain.Block): data,
	}
	live = NewLiveStore(store)
	if _, _, _, err := live.CurrentMasterchainInfo(context.Background()); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("masterchain info before state cells = %v, want ErrNotFound", err)
	}

	key := storage.BlockKey(current.Masterchain.Block)
	store.blockStates = map[storage.BlockRootHash]*storage.BlockState{
		key: {
			Block:         current.Masterchain.Block,
			StateRootHash: stateRootHash,
			Cell:          stateRoot,
		},
	}
	store.metas = map[storage.BlockRootHash]*storage.BlockMeta{
		key: {
			ID:       current.Masterchain.Block,
			GenUTime: 12345,
		},
	}

	live = NewLiveStore(store)
	store.current = nil

	got, gotStateRoot, gotUTime, err := live.CurrentMasterchainInfo(context.Background())
	if err != nil {
		t.Fatalf("masterchain info after init: %v", err)
	}
	if !blockIDEqual(got, current.Masterchain.Block) {
		t.Fatalf("masterchain info block = %+v, want %+v", got, current.Masterchain.Block)
	}
	if !bytes.Equal(gotStateRoot, stateRootHash) {
		t.Fatalf("masterchain info state root = %x, want %x", gotStateRoot, stateRootHash)
	}
	if gotUTime != 12345 {
		t.Fatalf("masterchain info utime = %d, want 12345", gotUTime)
	}
}

func TestLiveStoreCurrentMasterchainInfoUsesLiveMetaUTime(t *testing.T) {
	stateRoot := cell.BeginCell().MustStoreUInt(0x26, 8).EndCell()
	block, root := testBlockForState(t, masterchainID, masterchainShard, 26, stateRoot)
	data := testBlockBOC(root)
	block = testBlockIDForData(masterchainID, masterchainShard, block.SeqNo, root, data)
	stateRootHash := stateRoot.Hash(0)

	live := NewLiveStore(&fakeStore{})
	if err := live.PublishLiveBlockArtifacts(storage.LiveBlockArtifacts{
		Block: block,
		Root:  root,
		State: &storage.BlockState{
			Block:         block,
			StateRootHash: stateRootHash,
			Cell:          stateRoot,
		},
		BlockData: data,
		Meta: &storage.BlockMeta{
			ID:            block,
			StateRootHash: stateRootHash,
			GenUTime:      12346,
		},
		ArtifactFlushed: true,
		StateFlushed:    true,
	}); err != nil {
		t.Fatalf("publish live artifact: %v", err)
	}
	live.SetLiveCurrentState(&storage.CurrentState{
		Masterchain: storage.BlockState{
			Block:         block,
			StateRootHash: stateRootHash,
		},
	})

	got, gotStateRoot, gotUTime, err := live.CurrentMasterchainInfo(context.Background())
	if err != nil {
		t.Fatalf("live masterchain info: %v", err)
	}
	if !blockIDEqual(got, block) {
		t.Fatalf("live masterchain info block = %+v, want %+v", got, block)
	}
	if !bytes.Equal(gotStateRoot, stateRootHash) {
		t.Fatalf("live masterchain info state root = %x, want %x", gotStateRoot, stateRootHash)
	}
	if gotUTime != 12346 {
		t.Fatalf("live masterchain info utime = %d, want 12346", gotUTime)
	}
}

func TestLiveStoreCurrentMasterchainInfoRefreshesLiveMetaUTime(t *testing.T) {
	stateRoot := cell.BeginCell().MustStoreUInt(0x27, 8).EndCell()
	block, root := testBlockForState(t, masterchainID, masterchainShard, 27, stateRoot)
	data := testBlockBOC(root)
	block = testBlockIDForData(masterchainID, masterchainShard, block.SeqNo, root, data)
	stateRootHash := stateRoot.Hash(0)

	live := NewLiveStore(&fakeStore{})
	if err := live.PublishLiveBlockArtifacts(storage.LiveBlockArtifacts{
		Block:           block,
		Root:            root,
		BlockData:       data,
		ArtifactFlushed: true,
	}); err != nil {
		t.Fatalf("publish live artifact without meta: %v", err)
	}
	live.SetLiveCurrentState(&storage.CurrentState{
		Masterchain: storage.BlockState{
			Block:         block,
			StateRootHash: stateRootHash,
		},
	})

	if _, _, gotUTime, err := live.CurrentMasterchainInfo(context.Background()); err != nil || gotUTime != 0 {
		t.Fatalf("live masterchain info before meta: utime=%d err=%v, want 0 nil", gotUTime, err)
	}

	if err := live.PublishLiveBlockArtifacts(storage.LiveBlockArtifacts{
		Block:     block,
		Root:      root,
		BlockData: data,
		Meta: &storage.BlockMeta{
			ID:            block,
			StateRootHash: stateRootHash,
			GenUTime:      12347,
		},
		ArtifactFlushed: true,
	}); err != nil {
		t.Fatalf("publish live artifact with meta: %v", err)
	}

	got, gotStateRoot, gotUTime, err := live.CurrentMasterchainInfo(context.Background())
	if err != nil {
		t.Fatalf("live masterchain info after meta: %v", err)
	}
	if !blockIDEqual(got, block) {
		t.Fatalf("live masterchain info block = %+v, want %+v", got, block)
	}
	if !bytes.Equal(gotStateRoot, stateRootHash) {
		t.Fatalf("live masterchain info state root = %x, want %x", gotStateRoot, stateRootHash)
	}
	if gotUTime != 12347 {
		t.Fatalf("live masterchain info utime = %d, want 12347", gotUTime)
	}
}

func TestLiveStoreWaitUsesStoredCurrentInit(t *testing.T) {
	stateRoot := cell.BeginCell().MustStoreUInt(0x24, 8).EndCell()
	block, root := testBlockForState(t, masterchainID, masterchainShard, 24, stateRoot)
	data := testBlockBOC(root)
	block = testBlockIDForData(masterchainID, masterchainShard, block.SeqNo, root, data)
	shardRoot := cell.BeginCell().MustStoreUInt(0x25, 8).EndCell()
	shard, shardBlockRoot := testBlockForState(t, 0, masterchainShard, 7, shardRoot)
	shardData := testBlockBOC(shardBlockRoot)
	shard = testBlockIDForData(0, masterchainShard, shard.SeqNo, shardBlockRoot, shardData)
	current := &storage.CurrentState{
		Masterchain: storage.BlockState{
			Block:         block,
			StateRootHash: stateRoot.Hash(0),
		},
		Shards: map[storage.ShardKey]storage.BlockState{
			storage.ShardKeyFromBlock(shard): {
				Block:         shard,
				StateRootHash: shardRoot.Hash(0),
			},
		},
	}
	store := &fakeStore{current: current}
	live := NewLiveStore(store)

	err := live.WaitMasterchainSeqno(context.Background(), current.Masterchain.Block.SeqNo, time.Millisecond)
	if !errors.Is(err, errWaitMasterchainTimeout) {
		t.Fatalf("wait before block data = %v, want timeout", err)
	}

	store.blocks = map[storage.BlockRootHash][]byte{
		storage.BlockKey(current.Masterchain.Block): data,
	}
	store.blockStates = map[storage.BlockRootHash]*storage.BlockState{
		storage.BlockKey(current.Masterchain.Block): {
			Block:         current.Masterchain.Block,
			StateRootHash: stateRoot.Hash(0),
			Cell:          stateRoot,
		},
	}

	live = NewLiveStore(store)
	err = live.WaitMasterchainSeqno(context.Background(), current.Masterchain.Block.SeqNo, time.Millisecond)
	if !errors.Is(err, errWaitMasterchainTimeout) {
		t.Fatalf("wait before shard data = %v, want timeout", err)
	}

	store.blocks[storage.BlockKey(shard)] = shardData
	store.blockStates[storage.BlockKey(shard)] = &storage.BlockState{
		Block:         shard,
		StateRootHash: shardRoot.Hash(0),
		Cell:          shardRoot,
	}
	live = NewLiveStore(store)
	store.current = nil

	if err = live.WaitMasterchainSeqno(context.Background(), current.Masterchain.Block.SeqNo, time.Second); err != nil {
		t.Fatalf("wait after init: %v", err)
	}
}

func TestLiveStoreLookupSeqnoUsesCurrentStateBeforeStorage(t *testing.T) {
	current, root, data := testCurrentStateWithLiveBlock(t, 11)
	store := &fakeStore{}
	live := NewLiveStore(store)
	if err := live.publishLiveBlockData(current.Masterchain.Block, root, data, true); err != nil {
		t.Fatalf("set live master block: %v", err)
	}
	live.SetLiveCurrentState(current)

	key := storage.BlockHistoryKey{Workchain: masterchainID, Shard: masterchainShard}
	got, err := live.LookupBlockBySeqNo(context.Background(), key, 11)
	if err != nil {
		t.Fatalf("lookup live seqno: %v", err)
	}
	if !blockIDEqual(got, current.Masterchain.Block) {
		t.Fatalf("lookup block = %+v, want %+v", got, current.Masterchain.Block)
	}
	if store.seqLookupCalls != 0 {
		t.Fatalf("storage seq lookup calls = %d, want 0", store.seqLookupCalls)
	}

	if _, err = live.LookupBlockBySeqNo(context.Background(), key, 12); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("missing seqno error = %v, want ErrNotFound", err)
	}
	if store.seqLookupCalls != 0 {
		t.Fatalf("storage seq lookup calls = %d, want 0", store.seqLookupCalls)
	}
}

func TestLiveStoreLookupSeqnoUsesPendingBlockBeforeStorage(t *testing.T) {
	root := cell.BeginCell().MustStoreUInt(0xcc, 8).EndCell()
	payload := testBlockBOC(root)
	id := testBlockIDForData(0, int64(0x4000000000000000), 15, root, payload)
	store := &fakeStore{}
	live := NewLiveStore(store, LiveStoreOptions{MasterBlockCache: 0, ShardBlockCache: 0})
	if err := live.publishLiveBlockData(id, root, payload, false); err != nil {
		t.Fatalf("set live block: %v", err)
	}

	key := storage.BlockHistoryKey{Workchain: id.Workchain, Shard: id.Shard}
	got, err := live.LookupBlockBySeqNo(context.Background(), key, id.SeqNo)
	if err != nil {
		t.Fatalf("lookup pending seqno: %v", err)
	}
	if !blockIDEqual(got, id) {
		t.Fatalf("lookup block = %+v, want %+v", got, id)
	}
	if store.seqLookupCalls != 0 {
		t.Fatalf("storage seq lookup calls = %d, want 0", store.seqLookupCalls)
	}
}

func TestLiveStoreLookupLTAndUnixTimeUseLiveMetaBeforeStorage(t *testing.T) {
	stateRoot := cell.BeginCell().MustStoreUInt(0xdd, 8).EndCell()
	id, root := testBlockForState(t, 0, masterchainShard, 16, stateRoot)
	store := &fakeStore{}
	live := NewLiveStore(store, LiveStoreOptions{MasterBlockCache: 0, ShardBlockCache: 0})
	if err := live.publishLiveBlockData(id, root, nil, false); err != nil {
		t.Fatalf("set live block: %v", err)
	}

	key := storage.BlockHistoryKey{Workchain: id.Workchain, Shard: id.Shard}
	got, err := live.LookupBlockByLT(context.Background(), key, 50)
	if err != nil {
		t.Fatalf("lookup live lt: %v", err)
	}
	if !blockIDEqual(got, id) {
		t.Fatalf("lt lookup block = %+v, want %+v", got, id)
	}
	if store.ltLookupCalls != 0 {
		t.Fatalf("storage lt lookup calls = %d, want 0", store.ltLookupCalls)
	}

	got, err = live.LookupBlockByUnixTime(context.Background(), key, 1000)
	if err != nil {
		t.Fatalf("lookup live utime: %v", err)
	}
	if !blockIDEqual(got, id) {
		t.Fatalf("utime lookup block = %+v, want %+v", got, id)
	}
	if store.utimeLookupCalls != 0 {
		t.Fatalf("storage utime lookup calls = %d, want 0", store.utimeLookupCalls)
	}
}

func TestLiveStoreHistoryIndexesMatchLookupRules(t *testing.T) {
	live := NewLiveStore(&fakeStore{})
	key := storage.BlockHistoryKey{Workchain: 0, Shard: int64(0x4000000000000000)}
	first := testLiveStoreIndexBlock(1, key)
	second := testLiveStoreIndexBlock(2, key)

	live.mu.Lock()
	live.putBlockLocked(storage.BlockKey(first), &liveBlock{
		id:              first,
		meta:            &storage.BlockMeta{ID: first, StartLT: 1, EndLT: 100, GenUTime: 1000},
		artifactFlushed: true,
	})
	live.putBlockLocked(storage.BlockKey(second), &liveBlock{
		id:              second,
		meta:            &storage.BlockMeta{ID: second, StartLT: 150, EndLT: 200, GenUTime: 1001},
		artifactFlushed: true,
	})
	live.mu.Unlock()

	got, err := live.LookupBlockByLT(context.Background(), key, 75)
	if err != nil {
		t.Fatalf("lookup live lt: %v", err)
	}
	if !blockIDEqual(got, first) {
		t.Fatalf("lt lookup block = %+v, want %+v", got, first)
	}

	got, err = live.LookupBlockByLT(context.Background(), key, 125)
	if err != nil {
		t.Fatalf("lookup live lt ceil: %v", err)
	}
	if !blockIDEqual(got, second) {
		t.Fatalf("lt ceil lookup block = %+v, want %+v", got, second)
	}

	if _, err = live.LookupBlockByLT(context.Background(), key, 201); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("future lt lookup error = %v, want ErrNotFound", err)
	}

	got, err = live.LookupBlockByUnixTime(context.Background(), key, 1000)
	if err != nil {
		t.Fatalf("lookup live unix time: %v", err)
	}
	if !blockIDEqual(got, first) {
		t.Fatalf("unix time lookup block = %+v, want %+v", got, first)
	}

	got, err = live.LookupBlockByUnixTime(context.Background(), key, 1001)
	if err != nil {
		t.Fatalf("lookup live unix time ceil: %v", err)
	}
	if !blockIDEqual(got, second) {
		t.Fatalf("unix time ceil lookup block = %+v, want %+v", got, second)
	}

	if _, err = live.LookupBlockByUnixTime(context.Background(), key, 1002); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("future unix time lookup error = %v, want ErrNotFound", err)
	}
}

func TestLiveStoreLookupBlockByAccountLTUsesLowerBoundShardPath(t *testing.T) {
	live := NewLiveStore(&fakeStore{})
	account := bytes.Repeat([]byte{0x40}, 32)
	shards := storage.AccountShardCandidates(0, account)
	if len(shards) < 3 {
		t.Fatalf("account shard candidates = %d, want at least 3", len(shards))
	}

	topKey := storage.BlockHistoryKey{Workchain: 0, Shard: shards[0]}
	pathKey := storage.BlockHistoryKey{Workchain: 0, Shard: shards[2]}
	topBlock := testLiveStoreIndexBlock(50, topKey)
	pathBlock := testLiveStoreIndexBlock(20, pathKey)

	live.mu.Lock()
	live.putBlockLocked(storage.BlockKey(topBlock), &liveBlock{
		id:              topBlock,
		meta:            &storage.BlockMeta{ID: topBlock, StartLT: 1, EndLT: 500},
		artifactFlushed: true,
	})
	live.putBlockLocked(storage.BlockKey(pathBlock), &liveBlock{
		id:              pathBlock,
		meta:            &storage.BlockMeta{ID: pathBlock, StartLT: 150, EndLT: 200},
		artifactFlushed: true,
	})
	live.mu.Unlock()

	got, err := live.LookupBlockByAccountLT(context.Background(), 0, account, 125)
	if err != nil {
		t.Fatalf("lookup account lt lower-bound: %v", err)
	}
	if !blockIDEqual(got, pathBlock) {
		t.Fatalf("account lt lower-bound block = %+v, want %+v", got, pathBlock)
	}

	if _, err = live.LookupBlockByAccountLT(context.Background(), 0, account, 501); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("future account lt lookup error = %v, want ErrNotFound", err)
	}
}

func TestLiveStoreTrimRemovesHistoryIndexEntries(t *testing.T) {
	store := &fakeStore{}
	live := NewLiveStore(store, LiveStoreOptions{MasterBlockCache: 0, ShardBlockCache: 2})
	key := storage.BlockHistoryKey{Workchain: 0, Shard: int64(0x4000000000000000)}
	blocks := make([]ton.BlockIDExt, 0, 3)

	live.mu.Lock()
	for seqno := uint32(1); seqno <= 3; seqno++ {
		block := testLiveStoreIndexBlock(seqno, key)
		live.putBlockLocked(storage.BlockKey(block), &liveBlock{
			id: block,
			meta: &storage.BlockMeta{
				ID:       block,
				StartLT:  uint64(seqno*100 + 1),
				EndLT:    uint64(seqno*100 + 100),
				GenUTime: 1000 + seqno,
			},
			artifactFlushed: true,
		})
		live.trimBlocksLocked(liveBlockShard)
		blocks = append(blocks, block)
	}
	live.mu.Unlock()

	got, err := live.LookupBlockByLT(context.Background(), key, 150)
	if err != nil {
		t.Fatalf("lookup lt before first retained live block: %v", err)
	}
	if !blockIDEqual(got, blocks[1]) {
		t.Fatalf("lt lookup before first retained block = %+v, want %+v", got, blocks[1])
	}
	if store.ltLookupCalls != 0 {
		t.Fatalf("storage lt lookup calls = %d, want 0", store.ltLookupCalls)
	}

	got, err = live.LookupBlockByLT(context.Background(), key, 350)
	if err != nil {
		t.Fatalf("lookup retained live lt: %v", err)
	}
	if !blockIDEqual(got, blocks[2]) {
		t.Fatalf("lt lookup block = %+v, want %+v", got, blocks[2])
	}

	got, err = live.LookupBlockByUnixTime(context.Background(), key, 1001)
	if err != nil {
		t.Fatalf("lookup unix time before first retained live block: %v", err)
	}
	if !blockIDEqual(got, blocks[1]) {
		t.Fatalf("unix time lookup before first retained block = %+v, want %+v", got, blocks[1])
	}
	if store.utimeLookupCalls != 0 {
		t.Fatalf("storage unix time lookup calls = %d, want 0", store.utimeLookupCalls)
	}

	got, err = live.LookupBlockByUnixTime(context.Background(), key, 1003)
	if err != nil {
		t.Fatalf("lookup retained live unix time: %v", err)
	}
	if !blockIDEqual(got, blocks[2]) {
		t.Fatalf("unix time lookup block = %+v, want %+v", got, blocks[2])
	}
}

func TestLiveStoreStateMethodsUseCurrentStateBeforeStorage(t *testing.T) {
	stateRoot := cell.BeginCell().MustStoreUInt(0xee, 8).EndCell()
	id, blockRoot := testBlockForState(t, masterchainID, masterchainShard, 17, stateRoot)
	blockData := testBlockBOC(blockRoot)
	id = testBlockIDForData(masterchainID, masterchainShard, id.SeqNo, blockRoot, blockData)
	current := &storage.CurrentState{
		Masterchain: storage.BlockState{
			Block:         id,
			StateRootHash: stateRoot.Hash(0),
			Cell:          stateRoot,
		},
	}
	store := &fakeStore{}
	live := NewLiveStore(store)
	if err := live.publishLiveBlockData(id, blockRoot, blockData, true); err != nil {
		t.Fatalf("set live master block: %v", err)
	}
	live.SetLiveCurrentState(current)

	state, err := live.BlockState(context.Background(), id)
	if err != nil {
		t.Fatalf("load live block state: %v", err)
	}
	if state.Cell != stateRoot {
		t.Fatal("expected current state cell to be returned")
	}

	root, err := live.LoadStateCellTree(context.Background(), id, stateRoot.Hash(0))
	if err != nil {
		t.Fatalf("load live state cell tree: %v", err)
	}
	if root != stateRoot {
		t.Fatal("expected live state cell tree to be returned")
	}
	if store.blockStateCalls != 0 {
		t.Fatalf("storage block state calls = %d, want 0", store.blockStateCalls)
	}

	wrongRoot := cell.BeginCell().MustStoreUInt(0xef, 8).EndCell().Hash(0)
	if _, err = live.LoadStateCellTree(context.Background(), id, wrongRoot); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("wrong live state root error = %v, want ErrNotFound", err)
	}

	meta, err := live.BlockMeta(context.Background(), id)
	if err != nil {
		t.Fatalf("load live block meta: %v", err)
	}
	if !bytes.Equal(meta.StateRootHash, stateRoot.Hash(0)) {
		t.Fatalf("meta state root = %x, want %x", meta.StateRootHash, stateRoot.Hash(0))
	}
}

func TestLiveStorePublishedArtifactsAreAuthoritative(t *testing.T) {
	stateRoot := cell.BeginCell().MustStoreUInt(0xee, 8).EndCell()
	id, blockRoot := testBlockForState(t, masterchainID, masterchainShard, 18, stateRoot)
	blockData := testBlockBOC(blockRoot)
	id = testBlockIDForData(masterchainID, masterchainShard, id.SeqNo, blockRoot, blockData)
	proofData := []byte("live proof")
	store := &fakeStore{
		metas: map[storage.BlockRootHash]*storage.BlockMeta{
			storage.BlockKey(id): {
				ID:       id,
				GenUTime: 99,
				Flags:    storage.BlockMetaHasProofKeyBlock,
			},
		},
		proofs: map[fakeProofKey][]byte{
			fakeProofKeyForBlock(storage.ServedProofKeyBlock, id): []byte("stored proof"),
		},
	}
	live := NewLiveStore(store)

	if err := live.PublishLiveBlockArtifacts(storage.LiveBlockArtifacts{
		Block:     id,
		Root:      blockRoot,
		BlockData: blockData,
		Meta: &storage.BlockMeta{
			ID:            id,
			GenUTime:      1000,
			StateRootHash: stateRoot.Hash(0),
		},
		State: &storage.BlockState{
			Block:         id,
			StateRootHash: stateRoot.Hash(0),
			Cell:          stateRoot,
		},
		Proofs: []storage.LiveBlockProofArtifact{{
			Kind: storage.ServedProofBlock,
			Data: proofData,
		}},
		ArtifactFlushed: true,
		StateFlushed:    true,
	}); err != nil {
		t.Fatalf("publish live artifacts: %v", err)
	}

	meta, err := live.BlockMeta(context.Background(), id)
	if err != nil {
		t.Fatalf("live block meta: %v", err)
	}
	if meta.GenUTime != 1000 || !meta.Has(storage.BlockMetaHasProofBlock) || meta.Has(storage.BlockMetaHasProofKeyBlock) {
		t.Fatalf("unexpected live meta: %+v", meta)
	}
	if store.blockMetaCalls != 0 {
		t.Fatalf("storage block meta calls = %d, want 0", store.blockMetaCalls)
	}

	proof, err := live.BlockProof(context.Background(), storage.ServedProofBlock, id)
	if err != nil {
		t.Fatalf("live proof: %v", err)
	}
	if !bytes.Equal(proof, proofData) {
		t.Fatalf("proof = %q, want %q", proof, proofData)
	}
	if len(store.proofCalls) != 0 {
		t.Fatalf("storage proof calls = %d, want 0", len(store.proofCalls))
	}
}

func TestLiveStoreDoesNotReadFutureMasterBlockFromBacking(t *testing.T) {
	currentRoot := cell.BeginCell().MustStoreUInt(0x10, 8).EndCell()
	currentID, currentBlockRoot := testBlockForState(t, masterchainID, masterchainShard, 10, currentRoot)
	currentData := testBlockBOC(currentBlockRoot)
	currentID = testBlockIDForData(masterchainID, masterchainShard, currentID.SeqNo, currentBlockRoot, currentData)

	futureRoot := cell.BeginCell().MustStoreUInt(0x11, 8).EndCell()
	futureID, futureBlockRoot := testBlockForState(t, masterchainID, masterchainShard, 11, futureRoot)
	futureData := testBlockBOC(futureBlockRoot)
	futureID = testBlockIDForData(masterchainID, masterchainShard, futureID.SeqNo, futureBlockRoot, futureData)

	store := &fakeStore{
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(futureID): futureData,
		},
	}
	live := NewLiveStore(store)
	if err := live.PublishLiveBlockArtifacts(storage.LiveBlockArtifacts{
		Block:           currentID,
		Root:            currentBlockRoot,
		BlockData:       currentData,
		Meta:            &storage.BlockMeta{ID: currentID, StateRootHash: currentRoot.Hash(0)},
		State:           &storage.BlockState{Block: currentID, StateRootHash: currentRoot.Hash(0), Cell: currentRoot},
		ArtifactFlushed: true,
		StateFlushed:    true,
	}); err != nil {
		t.Fatalf("publish current live artifacts: %v", err)
	}
	live.SetLiveCurrentState(&storage.CurrentState{
		Masterchain: storage.BlockState{
			Block:         currentID,
			StateRootHash: currentRoot.Hash(0),
			Cell:          currentRoot,
		},
	})

	if _, err := live.BlockData(context.Background(), futureID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("future block data error = %v, want ErrNotFound", err)
	}
	if store.blockDataCalls != 0 {
		t.Fatalf("storage block data calls = %d, want 0", store.blockDataCalls)
	}
}

func TestLiveStoreKeepsRecentStateCellsUntilBlockTrimmed(t *testing.T) {
	type liveState struct {
		current   *storage.CurrentState
		stateRoot *cell.Cell
		blockRoot *cell.Cell
		blockData []byte
	}

	makeState := func(seqno uint32) liveState {
		stateRoot := cell.BeginCell().MustStoreUInt(uint64(seqno), 32).EndCell()
		_, blockRoot := testBlockForState(t, masterchainID, masterchainShard, seqno, stateRoot)
		blockData := testBlockBOC(blockRoot)
		id := testBlockIDForData(masterchainID, masterchainShard, seqno, blockRoot, blockData)
		return liveState{
			current: &storage.CurrentState{
				Masterchain: storage.BlockState{
					Block:         id,
					StateRootHash: stateRoot.Hash(0),
					Cell:          stateRoot,
				},
			},
			stateRoot: stateRoot,
			blockRoot: blockRoot,
			blockData: blockData,
		}
	}

	live := NewLiveStore(&fakeStore{}, LiveStoreOptions{MasterBlockCache: 2, ShardBlockCache: 0})
	first := makeState(30)
	second := makeState(31)
	third := makeState(32)

	for _, state := range []liveState{first, second} {
		if err := live.publishLiveBlockData(state.current.Masterchain.Block, state.blockRoot, state.blockData, true); err != nil {
			t.Fatalf("set live block %d: %v", state.current.Masterchain.Block.SeqNo, err)
		}
		live.SetLiveCurrentState(state.current)
		live.MarkLiveCurrentStateFlushed(state.current)
	}

	root, err := live.LoadStateCellTree(context.Background(), first.current.Masterchain.Block, first.stateRoot.Hash(0))
	if err != nil {
		t.Fatalf("load recent live state cell tree: %v", err)
	}
	if root != first.stateRoot {
		t.Fatal("expected recent live state cell tree to be returned")
	}
	if err = live.publishLiveBlockData(third.current.Masterchain.Block, third.blockRoot, third.blockData, true); err != nil {
		t.Fatalf("set live block %d: %v", third.current.Masterchain.Block.SeqNo, err)
	}
	live.SetLiveCurrentState(third.current)
	live.MarkLiveCurrentStateFlushed(third.current)

	if _, err = live.LoadStateCellTree(context.Background(), first.current.Masterchain.Block, first.stateRoot.Hash(0)); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("trimmed live state error = %v, want ErrNotFound", err)
	}
}

func TestLiveStoreDoesNotTrimUnflushedLiveState(t *testing.T) {
	stateRoot := cell.BeginCell().MustStoreUInt(33, 32).EndCell()
	_, blockRoot := testBlockForState(t, masterchainID, masterchainShard, 33, stateRoot)
	blockData := testBlockBOC(blockRoot)
	id := testBlockIDForData(masterchainID, masterchainShard, 33, blockRoot, blockData)
	state := storage.BlockState{
		Block:         id,
		StateRootHash: stateRoot.Hash(0),
		Cell:          stateRoot,
	}

	live := NewLiveStore(&fakeStore{}, LiveStoreOptions{MasterBlockCache: 0, ShardBlockCache: 0})
	if err := live.PublishLiveBlockArtifacts(storage.LiveBlockArtifacts{
		Block:           id,
		Root:            blockRoot,
		BlockData:       blockData,
		State:           &state,
		ArtifactFlushed: true,
	}); err != nil {
		t.Fatalf("publish live artifacts: %v", err)
	}

	if _, err := live.BlockData(context.Background(), id); err != nil {
		t.Fatalf("unflushed live state block data should stay cached: %v", err)
	}
	if _, err := live.LoadStateCellTree(context.Background(), id, stateRoot.Hash(0)); err != nil {
		t.Fatalf("unflushed live state should stay cached: %v", err)
	}

	live.MarkLiveCurrentStateFlushed(&storage.CurrentState{Masterchain: state})
	if _, err := live.BlockData(context.Background(), id); err != nil {
		t.Fatalf("state flush should not clear live block data: %v", err)
	}
	live.MarkLiveBlockFlushed(id)
	if _, err := live.BlockData(context.Background(), id); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("artifact-flushed block data error = %v, want ErrNotFound", err)
	}
}

func TestLiveStoreTrimsCheckpointFlushedHistoricalState(t *testing.T) {
	stateRoot := cell.BeginCell().MustStoreUInt(34, 32).EndCell()
	_, blockRoot := testBlockForState(t, masterchainID, masterchainShard, 34, stateRoot)
	blockData := testBlockBOC(blockRoot)
	id := testBlockIDForData(masterchainID, masterchainShard, 34, blockRoot, blockData)
	state := storage.BlockState{
		Block:         id,
		StateRootHash: stateRoot.Hash(0),
		Cell:          stateRoot,
	}

	live := NewLiveStore(&fakeStore{}, LiveStoreOptions{MasterBlockCache: 0, ShardBlockCache: 0})
	if err := live.PublishLiveBlockArtifacts(storage.LiveBlockArtifacts{
		Block:           id,
		Root:            blockRoot,
		BlockData:       blockData,
		State:           &state,
		ArtifactFlushed: true,
	}); err != nil {
		t.Fatalf("publish live artifacts: %v", err)
	}

	if _, err := live.LoadStateCellTree(context.Background(), id, stateRoot.Hash(0)); err != nil {
		t.Fatalf("unflushed historical live state should stay cached: %v", err)
	}

	live.MarkLiveBlockStatesFlushed([]ton.BlockIDExt{id})
	if _, err := live.LoadStateCellTree(context.Background(), id, stateRoot.Hash(0)); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("checkpoint-flushed historical state error = %v, want ErrNotFound", err)
	}
	if _, err := live.BlockData(context.Background(), id); err != nil {
		t.Fatalf("state flush should not clear historical block data: %v", err)
	}
	live.MarkLiveBlockFlushed(id)
	if _, err := live.BlockData(context.Background(), id); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("artifact-flushed historical block data error = %v, want ErrNotFound", err)
	}
}

func TestLiveStoreCheckpointFlushDoesNotRememberMissingBlocks(t *testing.T) {
	root := cell.BeginCell().MustStoreUInt(35, 32).EndCell()
	missing := testBlockID(t, 35, root)
	live := NewLiveStore(&fakeStore{})

	live.MarkLiveBlockStatesFlushed([]ton.BlockIDExt{missing})
	if len(live.flushed) != 0 {
		t.Fatalf("missing checkpoint-flushed blocks remembered = %d, want 0", len(live.flushed))
	}
}

func TestLiveStoreBlockRootAcceptsTrustedStoredRootWithMode31BOC(t *testing.T) {
	stateRoot := cell.BeginCell().MustStoreUInt(0xab, 8).EndCell()
	id, blockRoot := testBlockForState(t, masterchainID, masterchainShard, 18, stateRoot)
	data := testBlockBOC(blockRoot)
	store := &fakeStore{
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(id): data,
		},
	}
	live := NewLiveStore(store)

	root, err := live.BlockRoot(context.Background(), id)
	if err != nil {
		t.Fatalf("load live block root: %v", err)
	}
	if root.HashKey() != blockRoot.HashKey() {
		t.Fatal("unexpected block root")
	}

	got, err := live.BlockData(context.Background(), id)
	if err != nil {
		t.Fatalf("load trusted block data: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("unexpected trusted block data")
	}
}

func TestLiveStoreBlockDataDeduplicatesColdMiss(t *testing.T) {
	stateRoot := cell.BeginCell().MustStoreUInt(0xcc, 8).EndCell()
	id, blockRoot := testBlockForState(t, masterchainID, masterchainShard, 19, stateRoot)
	data := testBlockBOC(blockRoot)
	store := &blockingBlockDataStore{
		fakeStore: fakeStore{
			blocks: map[storage.BlockRootHash][]byte{
				storage.BlockKey(id): data,
			},
		},
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	live := NewLiveStore(store)

	const callers = 16
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	wg.Add(callers)
	for range callers {
		go func() {
			defer wg.Done()
			got, err := live.BlockData(context.Background(), id)
			if err != nil {
				errs <- err
				return
			}
			if !bytes.Equal(got, data) {
				errs <- errors.New("unexpected block data")
				return
			}
			errs <- nil
		}()
	}

	<-store.started
	close(store.release)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("load block data: %v", err)
		}
	}

	if calls := store.blockDataCalls(); calls != 1 {
		t.Fatalf("unexpected cold store calls %d", calls)
	}
}

func TestLookupBlockBySeqnoReturnsBlockHeader(t *testing.T) {
	stateRoot := cell.BeginCell().MustStoreUInt(0xbb, 8).EndCell()
	id, root := testBlockForState(t, masterchainID, masterchainShard, 7, stateRoot)
	store := &fakeStore{
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(id): testBlockBOC(root),
		},
		seqLookupByKey: map[fakeSeqLookupKey]ton.BlockIDExt{
			fakeSeqKey(storage.BlockHistoryKey{Workchain: id.Workchain, Shard: id.Shard}, id.SeqNo): id,
		},
	}
	srv := testServer(store)

	resp := srv.handleQuery(context.Background(), ton.LookupBlock{
		Mode: 1,
		ID: &ton.BlockInfoShort{
			Workchain: id.Workchain,
			Shard:     id.Shard,
			Seqno:     int32(id.SeqNo),
		},
	})
	header, ok := resp.(ton.BlockHeader)
	if !ok {
		t.Fatalf("response type = %T, want ton.BlockHeader", resp)
	}
	if header.ID == nil || !blockIDEqual(*header.ID, id) {
		t.Fatalf("unexpected block id: %+v", header.ID)
	}
	if store.seqLookupCalls != 1 {
		t.Fatalf("seq lookup calls = %d, want 1", store.seqLookupCalls)
	}

	body := mustUnwrapProof(t, header.HeaderProof, id.RootHash)
	assertRefType(t, body, 0, cell.OrdinaryCellType)
	assertRefType(t, body, 1, cell.PrunedCellType)
	assertRefType(t, body, 2, cell.PrunedCellType)
	assertRefType(t, body, 3, cell.PrunedCellType)
}

func TestGetBlockHeaderMode16IncludesBlockExtraLikeCpp(t *testing.T) {
	id, root := testMasterBlockWithShardHashes(t, 12, cell.NewDict(32))
	store := &fakeStore{
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(id): testBlockBOC(root),
		},
	}
	srv := testServer(store)

	resp := srv.handleQuery(context.Background(), ton.GetBlockHeader{
		ID:   cloneBlockID(id),
		Mode: 16,
	})
	header, ok := resp.(ton.BlockHeader)
	if !ok {
		t.Fatalf("response type = %T, want ton.BlockHeader: %+v", resp, resp)
	}

	body := mustUnwrapProof(t, header.HeaderProof, id.RootHash)
	assertRefType(t, body, 3, cell.OrdinaryCellType)
}

func TestAccountPrunedProofSkipsStateInitRefsLikeCpp(t *testing.T) {
	code := cell.BeginCell().
		MustStoreUInt(0xCA, 8).
		MustStoreRef(cell.BeginCell().MustStoreUInt(0x01, 8).EndCell()).
		EndCell()
	data := cell.BeginCell().
		MustStoreUInt(0xDA, 8).
		MustStoreRef(cell.BeginCell().MustStoreUInt(0x02, 8).EndCell()).
		EndCell()
	account, err := tlb.ToCell(&tlb.AccountState{
		IsValid: true,
		Address: address.MustParseAddr("EQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAM9c"),
		StorageInfo: tlb.StorageInfo{
			StorageUsed: tlb.StorageUsed{
				CellsUsed: big.NewInt(0),
				BitsUsed:  big.NewInt(0),
			},
			StorageExtra: tlb.StorageExtraNone{},
		},
		AccountStorage: tlb.AccountStorage{
			Status:  tlb.AccountStatusActive,
			Balance: tlb.FromNanoTONU(0),
			StateInit: &tlb.StateInit{
				Code: code,
				Data: data,
			},
		},
	})
	if err != nil {
		t.Fatalf("build account: %v", err)
	}

	proof, err := accountPrunedProof(account)
	if err != nil {
		t.Fatalf("create account proof: %v", err)
	}
	body, err := cell.UnwrapProof(proof, account.Hash(0))
	if err != nil {
		t.Fatalf("unwrap proof: %v", err)
	}
	assertRefType(t, body, 0, cell.PrunedCellType)
	assertRefType(t, body, 1, cell.PrunedCellType)
}

func TestLookupBlockRejectsUnsupportedSelector(t *testing.T) {
	srv := testServer(&fakeStore{})

	resp := srv.handleQuery(context.Background(), ton.LookupBlock{
		Mode: 3,
		ID:   &ton.BlockInfoShort{Workchain: 0, Shard: masterchainShard, Seqno: 1},
	})
	errResp, ok := resp.(ton.LSError)
	if !ok {
		t.Fatalf("response type = %T, want ton.LSError", resp)
	}
	if errResp.Code != errCodeProtoViolation {
		t.Fatalf("error code = %d, want %d", errResp.Code, errCodeProtoViolation)
	}
	if errResp.Text != "exactly one of mode.0, mode.1 and mode.2 bits must be set" {
		t.Fatalf("error text = %q", errResp.Text)
	}
}

func TestGetShardBlockProofReturnsEmptyForMasterchain(t *testing.T) {
	id := ton.BlockIDExt{
		Workchain: masterchainID,
		Shard:     masterchainShard,
		SeqNo:     7,
		RootHash:  bytes.Repeat([]byte{0x11}, 32),
		FileHash:  bytes.Repeat([]byte{0x22}, 32),
	}
	srv := testServer(&fakeStore{})

	resp := srv.handleQuery(context.Background(), ton.GetShardBlockProof{ID: cloneBlockID(id)})
	proof, ok := resp.(ton.ShardBlockProof)
	if !ok {
		t.Fatalf("response type = %T, want ton.ShardBlockProof", resp)
	}
	if proof.MasterchainID == nil || !blockIDEqual(*proof.MasterchainID, id) {
		t.Fatalf("masterchain id = %+v, want %s", proof.MasterchainID, storage.FormatBlockRef(id))
	}
	if len(proof.Links) != 0 {
		t.Fatalf("links = %d, want 0", len(proof.Links))
	}
}

func TestGetShardBlockProofBuildsMasterToShardLink(t *testing.T) {
	shardState := cell.BeginCell().MustStoreUInt(0x55, 8).EndCell()
	shard, _ := testBlockForState(t, 0, masterchainShard, 15, shardState)
	master, masterRoot := testMasterBlockWithShardHashes(t, 16, testShardHashes(t, shard))
	store := &fakeStore{
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(master): testBlockBOC(masterRoot),
		},
		metas: map[storage.BlockRootHash]*storage.BlockMeta{
			storage.BlockKey(shard): {ID: shard, MasterchainRefSeqno: master.SeqNo},
		},
		seqLookupByKey: map[fakeSeqLookupKey]ton.BlockIDExt{
			fakeSeqKey(storage.BlockHistoryKey{Workchain: masterchainID, Shard: masterchainShard}, master.SeqNo): master,
		},
	}
	srv := testServer(store)

	resp := srv.handleQuery(context.Background(), ton.GetShardBlockProof{ID: cloneBlockID(shard)})
	proof, ok := resp.(ton.ShardBlockProof)
	if !ok {
		t.Fatalf("response type = %T, want ton.ShardBlockProof: %+v", resp, resp)
	}
	if proof.MasterchainID == nil || !blockIDEqual(*proof.MasterchainID, master) {
		t.Fatalf("masterchain id = %+v, want %s", proof.MasterchainID, storage.FormatBlockRef(master))
	}
	if len(proof.Links) != 1 || proof.Links[0].ID == nil || !blockIDEqual(*proof.Links[0].ID, shard) {
		t.Fatalf("unexpected links: %+v", proof.Links)
	}

	body := mustUnwrapProof(t, proof.Links[0].Proof, master.RootHash)
	assertRefType(t, body, 0, cell.OrdinaryCellType)
	assertRefType(t, body, 3, cell.OrdinaryCellType)
}

func TestGetShardBlockProofResolvesSeqOnlyMasterRef(t *testing.T) {
	shardState := cell.BeginCell().MustStoreUInt(0x56, 8).EndCell()
	shard, _ := testBlockForState(t, 0, masterchainShard, 15, shardState)
	master, masterRoot := testMasterBlockWithShardHashes(t, 16, testShardHashes(t, shard))
	store := &fakeStore{
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(master): testBlockBOC(masterRoot),
		},
		metas: map[storage.BlockRootHash]*storage.BlockMeta{
			storage.BlockKey(shard): {ID: shard, MasterchainRefSeqno: master.SeqNo},
		},
		seqLookupByKey: map[fakeSeqLookupKey]ton.BlockIDExt{
			fakeSeqKey(storage.BlockHistoryKey{Workchain: masterchainID, Shard: masterchainShard}, master.SeqNo): master,
		},
	}
	srv := testServer(store)

	resp := srv.handleQuery(context.Background(), ton.GetShardBlockProof{ID: cloneBlockID(shard)})
	proof, ok := resp.(ton.ShardBlockProof)
	if !ok {
		t.Fatalf("response type = %T, want ton.ShardBlockProof: %+v", resp, resp)
	}
	if proof.MasterchainID == nil || !blockIDEqual(*proof.MasterchainID, master) {
		t.Fatalf("masterchain id = %+v, want %s", proof.MasterchainID, storage.FormatBlockRef(master))
	}
	if len(proof.Links) != 1 || proof.Links[0].ID == nil || !blockIDEqual(*proof.Links[0].ID, shard) {
		t.Fatalf("unexpected links: %+v", proof.Links)
	}
	if store.seqLookupCalls != 1 {
		t.Fatalf("seq lookup calls = %d, want 1", store.seqLookupCalls)
	}
}

func TestLiveStoreCurrentShardMetaUsesInclusionMasterRef(t *testing.T) {
	shardState := cell.BeginCell().MustStoreUInt(0x55, 8).EndCell()
	shard, shardRoot := testBlockForState(t, 0, masterchainShard, 15, shardState)
	master, masterRoot := testMasterBlockWithShardHashes(t, 16, testShardHashes(t, shard))

	live := NewLiveStore(&fakeStore{})
	if err := live.publishLiveBlockData(shard, shardRoot, testBlockBOC(shardRoot), true); err != nil {
		t.Fatalf("set live shard block: %v", err)
	}
	if err := live.publishLiveBlockData(master, masterRoot, testBlockBOC(masterRoot), true); err != nil {
		t.Fatalf("set live master block: %v", err)
	}

	live.SetLiveCurrentState(&storage.CurrentState{
		Masterchain: storage.BlockState{Block: master},
		Shards: map[storage.ShardKey]storage.BlockState{
			storage.ShardKeyFromBlock(shard): {
				Block:          shard,
				StateRootHash:  shardState.Hash(0),
				MasterchainRef: cloneBlockID(master),
				Cell:           shardState,
			},
		},
	})

	meta, err := live.BlockMeta(context.Background(), shard)
	if err != nil {
		t.Fatalf("load live shard meta: %v", err)
	}
	if meta.MasterchainRefSeqno != master.SeqNo {
		t.Fatalf("masterchain ref seqno = %d, want %d", meta.MasterchainRefSeqno, master.SeqNo)
	}

	resp := testServer(live).handleQuery(context.Background(), ton.GetShardBlockProof{ID: cloneBlockID(shard)})
	proof, ok := resp.(ton.ShardBlockProof)
	if !ok {
		t.Fatalf("response type = %T, want ton.ShardBlockProof: %+v", resp, resp)
	}
	if proof.MasterchainID == nil || !blockIDEqual(*proof.MasterchainID, master) {
		t.Fatalf("proof masterchain id = %+v, want %s", proof.MasterchainID, storage.FormatBlockRef(master))
	}
	if len(proof.Links) != 1 || proof.Links[0].ID == nil || !blockIDEqual(*proof.Links[0].ID, shard) {
		t.Fatalf("unexpected links: %+v", proof.Links)
	}
}

func TestGetBlockProofForwardIncludesSignaturesFromFullProof(t *testing.T) {
	catchainConfig := cell.BeginCell().MustStoreUInt(0x28, 8).EndCell()
	validatorConfig := cell.BeginCell().MustStoreUInt(0x34, 8).EndCell()
	keyCustom := testMcBlockExtraWithConfig(t, map[int32]*cell.Cell{
		int32(tlb.ConfigParamCatchainConfig):    catchainConfig,
		int32(tlb.ConfigParamCurrentValidators): validatorConfig,
	})
	keyStateRoot := testMasterStateWithOldBlocks(t, ton.BlockIDExt{Workchain: masterchainID, Shard: masterchainShard, SeqNo: 5}, nil)
	keyID, keyRoot := testMasterBlockForStateWithPrevKey(t, 5, 5, true, keyStateRoot, keyCustom)
	baseID := ton.BlockIDExt{Workchain: masterchainID, Shard: masterchainShard, SeqNo: 6}
	stateRoot := testMasterStateWithOldBlocks(t, baseID, []testOldMasterBlock{{id: keyID, isKey: true, endLT: 600}})
	targetID, targetRoot := testMasterBlockForStateWithPrevKey(t, 6, 5, false, stateRoot, nil)

	store := testBlockProofStore(t, keyID, keyRoot, targetID, targetRoot, stateRoot, testBlockProofSignatures(t))
	srv := testServer(store)

	resp := srv.handleQuery(context.Background(), ton.GetBlockProof{
		Mode:        0x1001,
		KnownBlock:  cloneBlockID(keyID),
		TargetBlock: cloneBlockID(targetID),
	})
	proof, ok := resp.(ton.PartialBlockProof)
	if !ok {
		t.Fatalf("response type = %T, want ton.PartialBlockProof: %+v", resp, resp)
	}
	if !proof.Complete || proof.From == nil || proof.To == nil || !blockIDEqual(*proof.From, keyID) || !blockIDEqual(*proof.To, targetID) {
		t.Fatalf("unexpected proof envelope: %+v", proof)
	}
	if len(proof.Steps) != 1 {
		t.Fatalf("steps = %d, want 1", len(proof.Steps))
	}

	step, ok := proof.Steps[0].(ton.BlockLinkForward)
	if !ok {
		t.Fatalf("step type = %T, want ton.BlockLinkForward", proof.Steps[0])
	}
	if step.SignatureSet == nil {
		t.Fatal("forward link has no signature set")
	}
	if _, ok = step.SignatureSet.(ton.SignatureSetOrdinary); !ok {
		t.Fatalf("signature set type = %T, want ton.SignatureSetOrdinary", step.SignatureSet)
	}
	if step.ToKeyBlock {
		t.Fatal("target is not a key block")
	}

	_ = mustUnwrapProof(t, step.DestProof, targetID.RootHash)
	configBody := mustUnwrapProof(t, step.ConfigProof, keyID.RootHash)
	assertRefType(t, configBody, 3, cell.OrdinaryCellType)
}

func TestGetBlockProofForwardDestProofUsesVirtualizedStoredProofRoot(t *testing.T) {
	catchainConfig := cell.BeginCell().MustStoreUInt(0x28, 8).EndCell()
	validatorConfig := cell.BeginCell().MustStoreUInt(0x34, 8).EndCell()
	keyCustom := testMcBlockExtraWithConfig(t, map[int32]*cell.Cell{
		int32(tlb.ConfigParamCatchainConfig):    catchainConfig,
		int32(tlb.ConfigParamCurrentValidators): validatorConfig,
	})
	keyStateRoot := testMasterStateWithOldBlocks(t, ton.BlockIDExt{Workchain: masterchainID, Shard: masterchainShard, SeqNo: 5}, nil)
	keyID, keyRoot := testMasterBlockForStateWithPrevKey(t, 5, 5, true, keyStateRoot, keyCustom)
	baseID := ton.BlockIDExt{Workchain: masterchainID, Shard: masterchainShard, SeqNo: 6}
	stateRoot := testMasterStateWithOldBlocks(t, baseID, []testOldMasterBlock{{id: keyID, isKey: true, endLT: 600}})
	targetID, targetRoot := testMasterBlockForStateWithPrevKey(t, 6, 5, false, stateRoot, nil)
	targetProof := testBlockStateRootProofEnvelopeBOC(t, targetID, targetRoot, testBlockProofSignatures(t))

	store := &fakeStore{
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(keyID):    testBlockBOC(keyRoot),
			storage.BlockKey(targetID): testBlockBOC(targetRoot),
		},
		proofs: map[fakeProofKey][]byte{
			fakeProofKeyForBlock(storage.ServedProofKeyBlock, keyID): testBlockProofEnvelopeBOC(t, keyID, keyRoot, testBlockProofSignatures(t)),
			fakeProofKeyForBlock(storage.ServedProofBlock, targetID): targetProof,
		},
		blockStates: map[storage.BlockRootHash]*storage.BlockState{
			storage.BlockKey(targetID): {Block: targetID, StateRootHash: stateRoot.Hash(0), Cell: stateRoot},
		},
		metas: map[storage.BlockRootHash]*storage.BlockMeta{
			storage.BlockKey(keyID):    {ID: keyID, Flags: storage.BlockMetaIsKeyBlock | storage.BlockMetaHasProofKeyBlock},
			storage.BlockKey(targetID): {ID: targetID, StateRootHash: stateRoot.Hash(0), Flags: storage.BlockMetaHasProofBlock},
		},
	}
	srv := testServer(store)

	resp := srv.handleQuery(context.Background(), ton.GetBlockProof{
		Mode:        0x1001,
		KnownBlock:  cloneBlockID(keyID),
		TargetBlock: cloneBlockID(targetID),
	})
	proof, ok := resp.(ton.PartialBlockProof)
	if !ok {
		t.Fatalf("response type = %T, want ton.PartialBlockProof: %+v", resp, resp)
	}
	if len(proof.Steps) != 1 {
		t.Fatalf("steps = %d, want 1", len(proof.Steps))
	}
	step, ok := proof.Steps[0].(ton.BlockLinkForward)
	if !ok {
		t.Fatalf("step type = %T, want ton.BlockLinkForward", proof.Steps[0])
	}

	assertBOCRootLevelZero(t, step.DestProof)
	_ = mustUnwrapProof(t, step.DestProof, targetID.RootHash)
}

func TestBlockHeaderProofBOCVirtualizesNonZeroLevelSourceRoot(t *testing.T) {
	stateRoot := cell.BeginCell().MustStoreUInt(0xbb, 8).EndCell()
	id, root := testBlockForState(t, masterchainID, masterchainShard, 7, stateRoot)
	storedProof, err := blockStateRootProof(root)
	if err != nil {
		t.Fatalf("create block state root proof: %v", err)
	}
	sourceRoot, err := cell.UnwrapProof(storedProof, id.RootHash)
	if err != nil {
		t.Fatalf("unwrap stored proof: %v", err)
	}
	if sourceRoot.Level() == 0 {
		t.Fatal("test source root should have non-zero level")
	}

	headerProof, err := blockHeaderProofBOC(sourceRoot, id, 0)
	if err != nil {
		t.Fatalf("create header proof: %v", err)
	}

	assertBOCRootLevelZero(t, headerProof)
	_ = mustUnwrapProof(t, headerProof, id.RootHash)
}

func TestGetBlockProofBackToKeyThenForwardIncludesSignatures(t *testing.T) {
	keyCustom := testMcBlockExtraWithConfig(t, map[int32]*cell.Cell{
		int32(tlb.ConfigParamCatchainConfig):    cell.BeginCell().EndCell(),
		int32(tlb.ConfigParamCurrentValidators): cell.BeginCell().EndCell(),
	})
	keyStateRoot := testMasterStateWithOldBlocks(t, ton.BlockIDExt{Workchain: masterchainID, Shard: masterchainShard, SeqNo: 5}, nil)
	keyID, keyRoot := testMasterBlockForStateWithPrevKey(t, 5, 5, true, keyStateRoot, keyCustom)

	knownBase := ton.BlockIDExt{Workchain: masterchainID, Shard: masterchainShard, SeqNo: 6}
	knownStateRoot := testMasterStateWithOldBlocks(t, knownBase, []testOldMasterBlock{{id: keyID, isKey: true, endLT: 600}})
	knownID, knownRoot := testMasterBlockForStateWithPrevKey(t, 6, 5, false, knownStateRoot, nil)

	targetBase := ton.BlockIDExt{Workchain: masterchainID, Shard: masterchainShard, SeqNo: 7}
	targetStateRoot := testMasterStateWithOldBlocks(t, targetBase, []testOldMasterBlock{
		{id: keyID, isKey: true, endLT: 600},
		{id: knownID, isKey: false, endLT: 700},
	})
	targetID, targetRoot := testMasterBlockForStateWithPrevKey(t, 7, 5, false, targetStateRoot, nil)

	store := &fakeStore{
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(keyID):    testBlockBOC(keyRoot),
			storage.BlockKey(knownID):  testBlockBOC(knownRoot),
			storage.BlockKey(targetID): testBlockBOC(targetRoot),
		},
		proofs: map[fakeProofKey][]byte{
			fakeProofKeyForBlock(storage.ServedProofKeyBlock, keyID): testBlockProofEnvelopeBOC(t, keyID, keyRoot, testBlockProofSignatures(t)),
			fakeProofKeyForBlock(storage.ServedProofBlock, knownID):  testBlockProofEnvelopeBOC(t, knownID, knownRoot, testBlockProofSignatures(t)),
			fakeProofKeyForBlock(storage.ServedProofBlock, targetID): testBlockProofEnvelopeBOC(t, targetID, targetRoot, testBlockProofSignatures(t)),
		},
		blockStates: map[storage.BlockRootHash]*storage.BlockState{
			storage.BlockKey(knownID):  {Block: knownID, StateRootHash: knownStateRoot.Hash(0), Cell: knownStateRoot},
			storage.BlockKey(targetID): {Block: targetID, StateRootHash: targetStateRoot.Hash(0), Cell: targetStateRoot},
		},
		metas: map[storage.BlockRootHash]*storage.BlockMeta{
			storage.BlockKey(keyID):    {ID: keyID, Flags: storage.BlockMetaIsKeyBlock | storage.BlockMetaHasProofKeyBlock},
			storage.BlockKey(knownID):  {ID: knownID, StateRootHash: knownStateRoot.Hash(0), Flags: storage.BlockMetaHasProofBlock},
			storage.BlockKey(targetID): {ID: targetID, StateRootHash: targetStateRoot.Hash(0), Flags: storage.BlockMetaHasProofBlock},
		},
	}
	srv := testServer(store)

	resp := srv.handleQuery(context.Background(), ton.GetBlockProof{
		Mode:        0x1001,
		KnownBlock:  cloneBlockID(knownID),
		TargetBlock: cloneBlockID(targetID),
	})
	proof, ok := resp.(ton.PartialBlockProof)
	if !ok {
		t.Fatalf("response type = %T, want ton.PartialBlockProof: %+v", resp, resp)
	}
	if !proof.Complete || len(proof.Steps) != 2 {
		t.Fatalf("unexpected proof chain: %+v", proof)
	}
	if _, ok = proof.Steps[0].(ton.BlockLinkBackward); !ok {
		t.Fatalf("first step type = %T, want ton.BlockLinkBackward", proof.Steps[0])
	}
	forward, ok := proof.Steps[1].(ton.BlockLinkForward)
	if !ok {
		t.Fatalf("second step type = %T, want ton.BlockLinkForward", proof.Steps[1])
	}
	if forward.SignatureSet == nil {
		t.Fatal("forward link after backward has no signature set")
	}
	if _, ok = forward.SignatureSet.(ton.SignatureSetOrdinary); !ok {
		t.Fatalf("signature set type = %T, want ton.SignatureSetOrdinary", forward.SignatureSet)
	}
}

func TestStoredMasterProofExportsKeyBlockLinkFromFullProof(t *testing.T) {
	stateRoot := testMasterStateWithOldBlocks(t, ton.BlockIDExt{Workchain: masterchainID, Shard: masterchainShard, SeqNo: 10}, nil)
	custom := testMcBlockExtraWithConfig(t, map[int32]*cell.Cell{
		int32(tlb.ConfigParamCatchainConfig):    cell.BeginCell().EndCell(),
		int32(tlb.ConfigParamCurrentValidators): cell.BeginCell().EndCell(),
	})
	keyID, keyRoot := testMasterBlockForStateWithPrevKey(t, 10, 10, true, stateRoot, custom)
	fullProof := testBlockProofEnvelopeBOC(t, keyID, keyRoot, testBlockProofSignatures(t))
	store := &fakeStore{
		proofs: map[fakeProofKey][]byte{
			fakeProofKeyForBlock(storage.ServedProofKeyBlock, keyID): fullProof,
		},
		metas: map[storage.BlockRootHash]*storage.BlockMeta{
			storage.BlockKey(keyID): {
				ID:    keyID,
				Flags: storage.BlockMetaIsKeyBlock | storage.BlockMetaHasProofKeyBlock,
			},
		},
	}

	data, err := testServer(store).storedMasterProof(context.Background(), keyID, true)
	if err != nil {
		t.Fatalf("stored master proof: %v", err)
	}
	proofRoot, err := cell.FromBOC(data)
	if err != nil {
		t.Fatalf("parse proof link: %v", err)
	}
	loader, err := proofRoot.BeginParse()
	if err != nil {
		t.Fatalf("begin proof link: %v", err)
	}
	var parsed testBlockProofEnvelope
	if err = tlb.LoadFromCell(&parsed, loader); err != nil {
		t.Fatalf("load proof link: %v", err)
	}
	gotRoot, err := cell.UnwrapProofVirtualized(parsed.Root, keyID.RootHash)
	if err != nil {
		t.Fatalf("unwrap proof link: %v", err)
	}
	if parsed.Signatures != nil {
		t.Fatal("proof link kept validator signatures")
	}
	if !bytes.Equal(gotRoot.Hash(0), keyID.RootHash) {
		t.Fatalf("root hash = %x, want %x", gotRoot.Hash(0), keyID.RootHash)
	}
	if len(store.proofCalls) != 1 || store.proofCalls[0] != storage.ServedProofKeyBlock {
		t.Fatalf("proof calls = %+v, want key-block full proof read", store.proofCalls)
	}
}

func TestStoredMasterProofUsesStoredLinkWhenFullProofIsAbsent(t *testing.T) {
	stateRoot := testMasterStateWithOldBlocks(t, ton.BlockIDExt{Workchain: masterchainID, Shard: masterchainShard, SeqNo: 10}, nil)
	custom := testMcBlockExtraWithConfig(t, map[int32]*cell.Cell{
		int32(tlb.ConfigParamCatchainConfig):    cell.BeginCell().EndCell(),
		int32(tlb.ConfigParamCurrentValidators): cell.BeginCell().EndCell(),
	})
	keyID, keyRoot := testMasterBlockForStateWithPrevKey(t, 10, 10, true, stateRoot, custom)
	linkProof := testBlockProofEnvelopeBOC(t, keyID, keyRoot, nil)
	store := &fakeStore{
		proofs: map[fakeProofKey][]byte{
			fakeProofKeyForBlock(storage.ServedProofKeyBlockLink, keyID): linkProof,
		},
		metas: map[storage.BlockRootHash]*storage.BlockMeta{
			storage.BlockKey(keyID): {
				ID:    keyID,
				Flags: storage.BlockMetaIsKeyBlock | storage.BlockMetaHasProofKeyBlockLink,
			},
		},
	}

	gotRoot, parsed, err := testServer(store).storedMasterProofRoot(context.Background(), keyID, true)
	if err != nil {
		t.Fatalf("stored master proof root: %v", err)
	}
	if parsed.Proof.Signatures != nil {
		t.Fatal("stored proof link has validator signatures")
	}
	if !bytes.Equal(gotRoot.Hash(0), keyID.RootHash) {
		t.Fatalf("root hash = %x, want %x", gotRoot.Hash(0), keyID.RootHash)
	}
	if len(store.proofCalls) != 1 || store.proofCalls[0] != storage.ServedProofKeyBlockLink {
		t.Fatalf("proof calls = %+v, want key-block link proof read", store.proofCalls)
	}
}

func TestStoredMasterProofAcceptsStoredMasterLinkWithSignatures(t *testing.T) {
	stateRoot := testMasterStateWithOldBlocks(t, ton.BlockIDExt{Workchain: masterchainID, Shard: masterchainShard, SeqNo: 10}, nil)
	custom := testMcBlockExtraWithConfig(t, map[int32]*cell.Cell{
		int32(tlb.ConfigParamCatchainConfig):    cell.BeginCell().EndCell(),
		int32(tlb.ConfigParamCurrentValidators): cell.BeginCell().EndCell(),
	})
	keyID, keyRoot := testMasterBlockForStateWithPrevKey(t, 10, 10, true, stateRoot, custom)
	linkProof := testBlockProofEnvelopeBOC(t, keyID, keyRoot, testBlockProofSignatures(t))
	store := &fakeStore{
		proofs: map[fakeProofKey][]byte{
			fakeProofKeyForBlock(storage.ServedProofKeyBlockLink, keyID): linkProof,
		},
		metas: map[storage.BlockRootHash]*storage.BlockMeta{
			storage.BlockKey(keyID): {
				ID:    keyID,
				Flags: storage.BlockMetaIsKeyBlock | storage.BlockMetaHasProofKeyBlockLink,
			},
		},
	}

	gotRoot, parsed, err := testServer(store).storedMasterProofRoot(context.Background(), keyID, true)
	if err != nil {
		t.Fatalf("stored master proof root: %v", err)
	}
	if parsed.Proof.Signatures == nil {
		t.Fatal("stored master proof link signatures were not parsed")
	}
	if !bytes.Equal(gotRoot.Hash(0), keyID.RootHash) {
		t.Fatalf("root hash = %x, want %x", gotRoot.Hash(0), keyID.RootHash)
	}
}

func TestStoredMasterProofDoesNotFallbackFromKeyBlockToOrdinaryProof(t *testing.T) {
	keyID := ton.BlockIDExt{
		Workchain: masterchainID,
		Shard:     masterchainShard,
		SeqNo:     10,
		RootHash:  bytes.Repeat([]byte{0x10}, 32),
		FileHash:  bytes.Repeat([]byte{0x11}, 32),
	}
	store := &fakeStore{
		proofs: map[fakeProofKey][]byte{
			fakeProofKeyForBlock(storage.ServedProofBlock, keyID): {0x01},
		},
		metas: map[storage.BlockRootHash]*storage.BlockMeta{
			storage.BlockKey(keyID): {
				ID:    keyID,
				Flags: storage.BlockMetaIsKeyBlock | storage.BlockMetaHasProofBlock,
			},
		},
	}

	_, err := testServer(store).storedMasterProof(context.Background(), keyID, true)
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("stored master proof error = %v, want ErrNotFound", err)
	}
	if len(store.proofCalls) != 0 {
		t.Fatalf("proof calls = %+v, want no ordinary proof lookup", store.proofCalls)
	}
}

func TestGetBlockProofForwardRejectsMissingEmbeddedSignatures(t *testing.T) {
	keyCustom := testMcBlockExtraWithConfig(t, map[int32]*cell.Cell{
		int32(tlb.ConfigParamCatchainConfig):    cell.BeginCell().EndCell(),
		int32(tlb.ConfigParamCurrentValidators): cell.BeginCell().EndCell(),
	})
	keyStateRoot := testMasterStateWithOldBlocks(t, ton.BlockIDExt{Workchain: masterchainID, Shard: masterchainShard, SeqNo: 5}, nil)
	keyID, keyRoot := testMasterBlockForStateWithPrevKey(t, 5, 5, true, keyStateRoot, keyCustom)
	baseID := ton.BlockIDExt{Workchain: masterchainID, Shard: masterchainShard, SeqNo: 6}
	stateRoot := testMasterStateWithOldBlocks(t, baseID, []testOldMasterBlock{{id: keyID, isKey: true, endLT: 600}})
	targetID, targetRoot := testMasterBlockForStateWithPrevKey(t, 6, 5, false, stateRoot, nil)

	store := testBlockProofStore(t, keyID, keyRoot, targetID, targetRoot, stateRoot, nil)
	srv := testServer(store)

	resp := srv.handleQuery(context.Background(), ton.GetBlockProof{
		Mode:        0x1001,
		KnownBlock:  cloneBlockID(keyID),
		TargetBlock: cloneBlockID(targetID),
	})
	errResp, ok := resp.(ton.LSError)
	if !ok {
		t.Fatalf("response type = %T, want ton.LSError: %+v", resp, resp)
	}
	if !strings.Contains(errResp.Text, "validator signatures") {
		t.Fatalf("error text = %q, want validator signatures", errResp.Text)
	}
}

func TestGetBlockProofModeTargets(t *testing.T) {
	baseID := ton.BlockIDExt{Workchain: masterchainID, Shard: masterchainShard, SeqNo: 7}
	stateRoot := testMasterStateWithOldBlocks(t, baseID, nil)
	id, root := testMasterBlockForStateWithPrevKey(t, 7, 0, false, stateRoot, nil)
	store := &fakeStore{
		current: &storage.CurrentState{
			Masterchain: storage.BlockState{Block: id, StateRootHash: stateRoot.Hash(0), Cell: stateRoot},
		},
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(id): testBlockBOC(root),
		},
		blockStates: map[storage.BlockRootHash]*storage.BlockState{
			storage.BlockKey(id): {Block: id, StateRootHash: stateRoot.Hash(0), Cell: stateRoot},
		},
		metas: map[storage.BlockRootHash]*storage.BlockMeta{
			storage.BlockKey(id): {ID: id, StateRootHash: stateRoot.Hash(0)},
		},
	}
	srv := testServer(store)

	tests := []ton.GetBlockProof{
		{Mode: 0, KnownBlock: cloneBlockID(id)},
		{Mode: 2, KnownBlock: cloneBlockID(id)},
		{Mode: 1, KnownBlock: cloneBlockID(id), TargetBlock: cloneBlockID(id)},
		{Mode: 0x1001, KnownBlock: cloneBlockID(id), TargetBlock: cloneBlockID(id)},
	}
	for _, query := range tests {
		resp := srv.handleQuery(context.Background(), query)
		proof, ok := resp.(ton.PartialBlockProof)
		if !ok {
			t.Fatalf("mode %#x response type = %T, want ton.PartialBlockProof: %+v", query.Mode, resp, resp)
		}
		if !proof.Complete || proof.From == nil || proof.To == nil || !blockIDEqual(*proof.From, id) || !blockIDEqual(*proof.To, id) || len(proof.Steps) != 0 {
			t.Fatalf("mode %#x unexpected proof: %+v", query.Mode, proof)
		}
	}
}

func TestLookupBlockWithProofBuildsShardLinks(t *testing.T) {
	shardState := cell.BeginCell().MustStoreUInt(0x66, 8).EndCell()
	shard, shardRoot := testBlockForState(t, 0, masterchainShard, 21, shardState)
	master, masterRoot := testMasterBlockWithShardHashes(t, 22, testShardHashes(t, shard))
	store := &fakeStore{
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(master): testBlockBOC(masterRoot),
			storage.BlockKey(shard):  testBlockBOC(shardRoot),
		},
		metas: map[storage.BlockRootHash]*storage.BlockMeta{
			storage.BlockKey(shard): {ID: shard, MasterchainRefSeqno: master.SeqNo},
		},
		seqLookupByKey: map[fakeSeqLookupKey]ton.BlockIDExt{
			fakeSeqKey(storage.BlockHistoryKey{Workchain: shard.Workchain, Shard: shard.Shard}, shard.SeqNo):     shard,
			fakeSeqKey(storage.BlockHistoryKey{Workchain: masterchainID, Shard: masterchainShard}, master.SeqNo): master,
		},
	}
	srv := testServer(store)

	resp := srv.handleQuery(context.Background(), ton.LookupBlockWithProof{
		Mode:      1,
		ID:        &ton.BlockInfoShort{Workchain: shard.Workchain, Shard: shard.Shard, Seqno: int32(shard.SeqNo)},
		MCBlockID: cloneBlockID(master),
	})
	result, ok := resp.(ton.LookupBlockResult)
	if !ok {
		t.Fatalf("response type = %T, want ton.LookupBlockResult: %+v", resp, resp)
	}
	if result.ID == nil || !blockIDEqual(*result.ID, shard) {
		t.Fatalf("id = %+v, want %s", result.ID, storage.FormatBlockRef(shard))
	}
	if result.MCBlockID == nil || !blockIDEqual(*result.MCBlockID, master) {
		t.Fatalf("mc id = %+v, want %s", result.MCBlockID, storage.FormatBlockRef(master))
	}
	if len(result.ShardLinks) != 1 || result.ShardLinks[0].ID == nil || !blockIDEqual(*result.ShardLinks[0].ID, shard) {
		t.Fatalf("unexpected shard links: %+v", result.ShardLinks)
	}
	if len(result.ClientMCStateProof) != 0 || len(result.MCBlockProof) != 0 || len(result.PrevHeader) != 0 {
		t.Fatalf("unexpected optional proofs: client=%d mc=%d prev=%d", len(result.ClientMCStateProof), len(result.MCBlockProof), len(result.PrevHeader))
	}
	if store.seqLookupCalls != 2 {
		t.Fatalf("seq lookup calls = %d, want 2", store.seqLookupCalls)
	}

	headerBody := mustUnwrapProof(t, result.Header, shard.RootHash)
	assertRefType(t, headerBody, 0, cell.OrdinaryCellType)
	linkBody := mustUnwrapProof(t, result.ShardLinks[0].Proof, master.RootHash)
	assertRefType(t, linkBody, 3, cell.OrdinaryCellType)
}

func TestLookupBlockWithProofIncludesPrevHeaderForLTLookup(t *testing.T) {
	prevState := cell.BeginCell().MustStoreUInt(0x65, 8).EndCell()
	prev, prevRoot := testBlockForState(t, 0, masterchainShard, 20, prevState)
	shardState := cell.BeginCell().MustStoreUInt(0x66, 8).EndCell()
	shard, shardRoot := testBlockForState(t, 0, masterchainShard, 21, shardState)
	master, masterRoot := testMasterBlockWithShardHashes(t, 22, testShardHashes(t, shard))
	key := storage.BlockHistoryKey{Workchain: shard.Workchain, Shard: shard.Shard}
	store := &fakeStore{
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(master): testBlockBOC(masterRoot),
			storage.BlockKey(prev):   testBlockBOC(prevRoot),
			storage.BlockKey(shard):  testBlockBOC(shardRoot),
		},
		metas: map[storage.BlockRootHash]*storage.BlockMeta{
			storage.BlockKey(shard): {ID: shard, MasterchainRefSeqno: master.SeqNo, PrevRefs: []ton.BlockIDExt{prev}},
		},
		ltLookup: map[fakeLTLookupKey]ton.BlockIDExt{
			fakeLTKey(key, 123): shard,
		},
		seqLookupByKey: map[fakeSeqLookupKey]ton.BlockIDExt{
			fakeSeqKey(storage.BlockHistoryKey{Workchain: masterchainID, Shard: masterchainShard}, master.SeqNo): master,
		},
	}
	srv := testServer(store)

	resp := srv.handleQuery(context.Background(), ton.LookupBlockWithProof{
		Mode:      2,
		ID:        &ton.BlockInfoShort{Workchain: shard.Workchain, Shard: shard.Shard},
		MCBlockID: cloneBlockID(master),
		LT:        123,
	})
	result, ok := resp.(ton.LookupBlockResult)
	if !ok {
		t.Fatalf("response type = %T, want ton.LookupBlockResult: %+v", resp, resp)
	}
	if store.ltLookupCalls != 1 {
		t.Fatalf("lt lookup calls = %d, want 1", store.ltLookupCalls)
	}
	if len(result.PrevHeader) == 0 {
		t.Fatal("expected previous header proof")
	}
	prevHeaderBody := mustUnwrapProof(t, result.PrevHeader, prev.RootHash)
	assertRefType(t, prevHeaderBody, 0, cell.OrdinaryCellType)
}

func TestAccountStateReturnsAccountCellAndProofRoots(t *testing.T) {
	accountID := bytes.Repeat([]byte{0x33}, 32)
	stateID := ton.BlockIDExt{Workchain: masterchainID, Shard: masterchainShard, SeqNo: 9}
	stateRoot, wantAccount := testShardStateWithAccount(t, stateID, accountID)
	id, blockRoot := testBlockForState(t, masterchainID, masterchainShard, 9, stateRoot)

	store := &fakeStore{
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(id): testBlockBOC(blockRoot),
		},
		blockStates: map[storage.BlockRootHash]*storage.BlockState{
			storage.BlockKey(id): {
				Block:         id,
				StateRootHash: stateRoot.Hash(0),
				Cell:          stateRoot,
			},
		},
	}
	srv := testServer(store)

	resp := srv.handleQuery(context.Background(), ton.GetAccountState{
		ID: cloneBlockID(id),
		Account: ton.AccountID{
			Workchain: masterchainID,
			ID:        accountID,
		},
	})

	accountState, ok := resp.(ton.AccountState)
	if !ok {
		t.Fatalf("response type = %T, want ton.AccountState", resp)
	}
	if accountState.State == nil {
		t.Fatal("expected account state cell")
	}
	if len(accountState.Proof) != 2 {
		t.Fatalf("proof roots = %d, want 2", len(accountState.Proof))
	}
	blockProofBody, err := cell.UnwrapProof(accountState.Proof[0], id.RootHash)
	if err != nil {
		t.Fatalf("unwrap block proof: %v", err)
	}
	assertRefType(t, blockProofBody, 0, cell.OrdinaryCellType)
	assertRefType(t, blockProofBody, 2, cell.MerkleUpdateCellType)
	assertRefType(t, blockProofBody, 3, cell.PrunedCellType)

	if accountState.State.HashKey() != wantAccount.HashKey() {
		t.Fatal("returned account state hash mismatch")
	}
}

func TestAccountStateWithMasterReferenceResolvesBasechainShard(t *testing.T) {
	accountID := bytes.Repeat([]byte{0x33}, 32)
	shardStateRoot, wantAccount := testShardStateWithAccount(t, ton.BlockIDExt{Workchain: 0, Shard: masterchainShard, SeqNo: 15}, accountID)
	shardBlock, shardRoot := testBlockForState(t, 0, masterchainShard, 15, shardStateRoot)
	nextValidatorShard := int64(tlb.ShardID(uint64(shardBlock.Shard)).GetChild(true))

	shardHashes := testShardHashesWithNextValidatorShard(t, shardBlock, nextValidatorShard)
	masterStateRoot := testMasterStateWithShardHashes(t, ton.BlockIDExt{Workchain: masterchainID, Shard: masterchainShard, SeqNo: 16}, shardHashes)
	masterBlock, masterRoot := testBlockForState(t, masterchainID, masterchainShard, 16, masterStateRoot)

	store := &fakeStore{
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(masterBlock): testBlockBOC(masterRoot),
			storage.BlockKey(shardBlock):  testBlockBOC(shardRoot),
		},
		blockStates: map[storage.BlockRootHash]*storage.BlockState{
			storage.BlockKey(masterBlock): {Block: masterBlock, StateRootHash: masterStateRoot.Hash(0), Cell: masterStateRoot},
			storage.BlockKey(shardBlock):  {Block: shardBlock, StateRootHash: shardStateRoot.Hash(0), Cell: shardStateRoot},
		},
	}
	srv := testServer(store)

	resp := srv.handleQuery(context.Background(), ton.GetAccountState{
		ID: cloneBlockID(masterBlock),
		Account: ton.AccountID{
			Workchain: 0,
			ID:        accountID,
		},
	})

	accountState, ok := resp.(ton.AccountState)
	if !ok {
		t.Fatalf("response type = %T, want ton.AccountState: %+v", resp, resp)
	}
	if !blockIDEqual(*accountState.Shard, shardBlock) {
		t.Fatalf("resolved shard = %s, want %s", storage.FormatBlockRef(*accountState.Shard), storage.FormatBlockRef(shardBlock))
	}
	if accountState.State == nil || accountState.State.HashKey() != wantAccount.HashKey() {
		t.Fatal("returned account state mismatch")
	}
	if len(accountState.ShardProof) != 2 || len(accountState.Proof) != 2 {
		t.Fatalf("proof lengths shard=%d state=%d, want 2/2", len(accountState.ShardProof), len(accountState.Proof))
	}
}

func TestAccountStateWithLiveStoreCachesBlockFragments(t *testing.T) {
	accountID := bytes.Repeat([]byte{0x34}, 32)
	shardStateRoot, wantAccount := testShardStateWithAccount(t, ton.BlockIDExt{Workchain: 0, Shard: masterchainShard, SeqNo: 15}, accountID)
	shardBlock, shardRoot := testBlockForState(t, 0, masterchainShard, 15, shardStateRoot)
	shardHashes := testShardHashes(t, shardBlock)
	masterStateRoot := testMasterStateWithShardHashes(t, ton.BlockIDExt{Workchain: masterchainID, Shard: masterchainShard, SeqNo: 16}, shardHashes)
	masterBlock, masterRoot := testBlockForState(t, masterchainID, masterchainShard, 16, masterStateRoot)

	backing := &fakeStore{
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(masterBlock): testBlockBOC(masterRoot),
			storage.BlockKey(shardBlock):  testBlockBOC(shardRoot),
		},
		blockStates: map[storage.BlockRootHash]*storage.BlockState{
			storage.BlockKey(masterBlock): {Block: masterBlock, StateRootHash: masterStateRoot.Hash(0), Cell: masterStateRoot},
			storage.BlockKey(shardBlock):  {Block: shardBlock, StateRootHash: shardStateRoot.Hash(0), Cell: shardStateRoot},
		},
	}
	srv := testServer(NewLiveStore(backing))

	for range 2 {
		resp := srv.handleQuery(context.Background(), ton.GetAccountState{
			ID: cloneBlockID(masterBlock),
			Account: ton.AccountID{
				Workchain: 0,
				ID:        accountID,
			},
		})

		accountState, ok := resp.(ton.AccountState)
		if !ok {
			t.Fatalf("response type = %T, want ton.AccountState: %+v", resp, resp)
		}
		if accountState.State == nil || accountState.State.HashKey() != wantAccount.HashKey() {
			t.Fatal("returned account state mismatch")
		}
	}

	if backing.loadStateCalls != 2 {
		t.Fatalf("state loads = %d, want 2", backing.loadStateCalls)
	}
}

func TestAccountStateUsesLocalShardAliasWithSameHash(t *testing.T) {
	accountID := bytes.Repeat([]byte{0x33}, 32)
	shardStateRoot, wantAccount := testShardStateWithAccount(t, ton.BlockIDExt{Workchain: 0, Shard: masterchainShard, SeqNo: 15}, accountID)
	shardBlock, shardRoot := testBlockForState(t, 0, masterchainShard, 15, shardStateRoot)
	localShard := shardBlock
	localShard.Shard = int64(tlb.ShardID(uint64(shardBlock.Shard)).GetChild(true))

	shardHashes := testShardHashes(t, shardBlock)
	masterStateRoot := testMasterStateWithShardHashes(t, ton.BlockIDExt{Workchain: masterchainID, Shard: masterchainShard, SeqNo: 16}, shardHashes)
	masterBlock, masterRoot := testBlockForState(t, masterchainID, masterchainShard, 16, masterStateRoot)

	store := &fakeStore{
		current: &storage.CurrentState{
			Masterchain: storage.BlockState{Block: masterBlock, StateRootHash: masterStateRoot.Hash(0), Cell: masterStateRoot},
			Shards: map[storage.ShardKey]storage.BlockState{
				storage.ShardKeyFromBlock(localShard): {Block: localShard, StateRootHash: shardStateRoot.Hash(0), Cell: shardStateRoot},
			},
		},
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(masterBlock): testBlockBOC(masterRoot),
			storage.BlockKey(shardBlock):  testBlockBOC(shardRoot),
		},
		blockStates: map[storage.BlockRootHash]*storage.BlockState{
			storage.BlockKey(masterBlock): {Block: masterBlock, StateRootHash: masterStateRoot.Hash(0), Cell: masterStateRoot},
			storage.BlockKey(shardBlock):  {Block: shardBlock, StateRootHash: shardStateRoot.Hash(0), Cell: shardStateRoot},
		},
	}
	srv := testServer(store)

	resp := srv.handleQuery(context.Background(), ton.GetAccountState{
		ID: cloneBlockID(masterBlock),
		Account: ton.AccountID{
			Workchain: 0,
			ID:        accountID,
		},
	})

	accountState, ok := resp.(ton.AccountState)
	if !ok {
		t.Fatalf("response type = %T, want ton.AccountState: %+v", resp, resp)
	}
	if !blockIDEqual(*accountState.Shard, shardBlock) {
		t.Fatalf("response shard = %s, want %s", storage.FormatBlockRef(*accountState.Shard), storage.FormatBlockRef(shardBlock))
	}
	if accountState.State == nil || accountState.State.HashKey() != wantAccount.HashKey() {
		t.Fatal("returned account state mismatch")
	}
}

func testRunMethodParamsBOC(t testing.TB, params *cell.Cell) []byte {
	t.Helper()
	return params.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false})
}

func TestRunSmcMethodExecutesGetMethodAndReturnsProofs(t *testing.T) {
	accountID := bytes.Repeat([]byte{0x35}, 32)
	base := ton.BlockIDExt{Workchain: masterchainID, Shard: masterchainShard, SeqNo: 10}
	code := testCodeFromBuilders(t,
		stackop.POP(0).Serialize(),
		stackop.PUSHINT(big.NewInt(7)).Serialize(),
		execop.RET().Serialize(),
	)
	stateRoot, accountCell := testMasterStateWithActiveAccount(t, base, accountID, code, cell.BeginCell().EndCell())
	id, blockRoot := testBlockForState(t, masterchainID, masterchainShard, 10, stateRoot)
	params := tlb.NewStack()
	paramsCell, err := params.ToCell()
	if err != nil {
		t.Fatalf("build params stack: %v", err)
	}
	store := &fakeStore{
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(id): testBlockBOC(blockRoot),
		},
		blockStates: map[storage.BlockRootHash]*storage.BlockState{
			storage.BlockKey(id): {Block: id, StateRootHash: stateRoot.Hash(0), Cell: stateRoot},
		},
	}
	srv := testServer(store)

	resp := srv.handleQuery(context.Background(), ton.RunSmcMethod{
		Mode:     7 | 8 | 32,
		ID:       cloneBlockID(id),
		Account:  ton.AccountID{Workchain: masterchainID, ID: accountID},
		MethodID: tlb.MethodNameHash("answer"),
		Params:   testRunMethodParamsBOC(t, paramsCell),
	})

	result, ok := resp.(ton.RunMethodResult)
	if !ok {
		t.Fatalf("response type = %T, want ton.RunMethodResult: %+v", resp, resp)
	}
	if result.ExitCode != 0 {
		t.Fatalf("exit code = %d, want 0", result.ExitCode)
	}
	if result.ID == nil || !blockIDEqual(*result.ID, id) || result.ShardBlock == nil || !blockIDEqual(*result.ShardBlock, id) {
		t.Fatalf("unexpected block ids: id=%+v shard=%+v", result.ID, result.ShardBlock)
	}
	if len(result.Proof) != 2 || result.StateProof == nil {
		t.Fatalf("expected account proofs, got proof=%d state=%v", len(result.Proof), result.StateProof)
	}
	if _, err = cell.UnwrapProof(result.StateProof, accountCell.Hash(0)); err != nil {
		t.Fatalf("unwrap account proof: %v", err)
	}
	if result.InitC7 == nil {
		t.Fatal("expected init c7")
	}
	c7Value, err := tlb.ParseStackValue(result.InitC7.MustBeginParse())
	if err != nil {
		t.Fatalf("parse init c7: %v", err)
	}
	c7Outer, ok := c7Value.([]any)
	if !ok || len(c7Outer) != 1 {
		t.Fatalf("unexpected c7 outer value: %#v", c7Value)
	}
	c7Inner, ok := c7Outer[0].([]any)
	if !ok || len(c7Inner) != 18 {
		t.Fatalf("unexpected c7 inner value: %#v", c7Outer[0])
	}

	var stack tlb.Stack
	if result.Result == nil {
		t.Fatal("expected result stack")
	}
	if err = stack.LoadFromCell(result.Result.MustBeginParse()); err != nil {
		t.Fatalf("load result stack: %v", err)
	}
	value, err := stack.Pop()
	if err != nil {
		t.Fatalf("pop result: %v", err)
	}
	intValue, ok := value.(*big.Int)
	if !ok || intValue.Int64() != 7 {
		t.Fatalf("result value = %#v, want 7", value)
	}
}

func TestRunSmcMethodWithLiveStoreCachesBlockFragments(t *testing.T) {
	accountID := bytes.Repeat([]byte{0x38}, 32)
	base := ton.BlockIDExt{Workchain: masterchainID, Shard: masterchainShard, SeqNo: 12}
	code := testCodeFromBuilders(t,
		stackop.POP(0).Serialize(),
		stackop.PUSHINT(big.NewInt(9)).Serialize(),
		execop.RET().Serialize(),
	)
	stateRoot, _ := testMasterStateWithActiveAccount(t, base, accountID, code, cell.BeginCell().EndCell())
	id, blockRoot := testBlockForState(t, masterchainID, masterchainShard, base.SeqNo, stateRoot)
	paramsCell, err := tlb.NewStack().ToCell()
	if err != nil {
		t.Fatalf("build params stack: %v", err)
	}
	backing := &fakeStore{
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(id): testBlockBOC(blockRoot),
		},
		blockStates: map[storage.BlockRootHash]*storage.BlockState{
			storage.BlockKey(id): {Block: id, StateRootHash: stateRoot.Hash(0), Cell: stateRoot},
		},
	}
	srv := testServer(NewLiveStore(backing))

	for range 2 {
		resp := srv.handleQuery(context.Background(), ton.RunSmcMethod{
			Mode:     4,
			ID:       cloneBlockID(id),
			Account:  ton.AccountID{Workchain: masterchainID, ID: accountID},
			MethodID: tlb.MethodNameHash("answer"),
			Params:   testRunMethodParamsBOC(t, paramsCell),
		})

		result, ok := resp.(ton.RunMethodResult)
		if !ok {
			t.Fatalf("response type = %T, want ton.RunMethodResult: %+v", resp, resp)
		}
		if result.ExitCode != 0 {
			t.Fatalf("exit code = %d, want 0", result.ExitCode)
		}
	}

	if backing.loadStateCalls != 1 {
		t.Fatalf("state loads = %d, want 1", backing.loadStateCalls)
	}
}

func TestLiveBlockFragmentsCachesAccountProofOnly(t *testing.T) {
	accountID := bytes.Repeat([]byte{0x39}, 32)
	base := ton.BlockIDExt{Workchain: masterchainID, Shard: masterchainShard, SeqNo: 12}
	code := testCodeFromBuilders(t,
		stackop.POP(0).Serialize(),
		stackop.PUSHINT(big.NewInt(9)).Serialize(),
		execop.RET().Serialize(),
	)
	stateRoot, _ := testMasterStateWithActiveAccount(t, base, accountID, code, cell.BeginCell().EndCell())
	id, blockRoot := testBlockForState(t, masterchainID, masterchainShard, base.SeqNo, stateRoot)

	fragments, err := buildLiveBlockFragments(id, blockRoot, stateRoot)
	if err != nil {
		t.Fatalf("build fragments: %v", err)
	}

	proof, state, err := fragments.accountProof(accountID, false)
	if err != nil {
		t.Fatalf("first account proof: %v", err)
	}
	cachedProof, cachedState, err := fragments.accountProof(accountID, false)
	if err != nil {
		t.Fatalf("cached account proof: %v", err)
	}

	if len(proof) != 2 || len(cachedProof) != 2 {
		t.Fatalf("proof lengths = %d/%d, want 2/2", len(proof), len(cachedProof))
	}
	if proof[1] != cachedProof[1] {
		t.Fatal("account proof was rebuilt instead of reused")
	}
	if state == nil || cachedState == nil {
		t.Fatal("account state is missing")
	}

	prunedProof, prunedState, err := fragments.accountProof(accountID, true)
	if err != nil {
		t.Fatalf("pruned account proof: %v", err)
	}
	if proof[1] != prunedProof[1] {
		t.Fatal("account proof cache should be shared by pruned and full state responses")
	}
	if prunedState == nil || !prunedState.IsSpecial() || prunedState.GetType() != cell.MerkleProofCellType {
		t.Fatalf("pruned account state is not a merkle proof: %v", prunedState)
	}
	if len(fragments.accountProofs) != 1 {
		t.Fatalf("cached account proofs = %d, want 1", len(fragments.accountProofs))
	}
}

func TestRunMethodStackKeepsCppArgumentOrder(t *testing.T) {
	first := cell.BeginCell().MustStoreUInt(0xaa, 8).EndCell().MustBeginParse()
	second := cell.BeginCell().MustStoreUInt(0xbb, 8).EndCell()

	params := tlb.NewStack()
	params.Push(second)
	params.Push(first)
	paramsCell, err := params.ToCell()
	if err != nil {
		t.Fatalf("build params stack: %v", err)
	}

	stack, err := runMethodStack(123, testRunMethodParamsBOC(t, paramsCell))
	if err != nil {
		t.Fatalf("runMethodStack: %v", err)
	}

	method, err := stack.PopInt()
	if err != nil {
		t.Fatalf("pop method id: %v", err)
	}
	if method.Int64() != 123 {
		t.Fatalf("method id = %s, want 123", method)
	}

	gotSecond, err := stack.PopCell()
	if err != nil {
		t.Fatalf("pop second arg: %v", err)
	}
	if !bytes.Equal(gotSecond.Hash(), second.Hash()) {
		t.Fatalf("second arg hash = %x, want %x", gotSecond.Hash(), second.Hash())
	}

	gotFirst, err := stack.PopSlice()
	if err != nil {
		t.Fatalf("pop first arg: %v", err)
	}
	if !bytes.Equal(gotFirst.MustToCell().Hash(), first.MustToCell().Hash()) {
		t.Fatalf("first arg hash mismatch")
	}
}

func TestRunSmcMethodExecutesLibraryReferenceCode(t *testing.T) {
	accountID := bytes.Repeat([]byte{0x37}, 32)
	base := ton.BlockIDExt{Workchain: masterchainID, Shard: masterchainShard, SeqNo: 14}
	code := testCodeFromBuilders(t,
		stackop.POP(0).Serialize(),
		stackop.PUSHINT(big.NewInt(11)).Serialize(),
		execop.RET().Serialize(),
	)
	codeRef, err := cell.BeginCell().
		MustStoreUInt(uint64(cell.LibraryCellType), 8).
		MustStoreSlice(code.Hash(), 256).
		EndCellSpecial(true)
	if err != nil {
		t.Fatalf("build library code ref: %v", err)
	}

	stateRoot, _ := testMasterStateWithActiveAccountAndLibraries(t, base, accountID, codeRef, cell.BeginCell().EndCell(), []*cell.Cell{code})
	id, blockRoot := testBlockForState(t, masterchainID, masterchainShard, base.SeqNo, stateRoot)
	paramsCell, err := tlb.NewStack().ToCell()
	if err != nil {
		t.Fatalf("build params stack: %v", err)
	}
	store := &fakeStore{
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(id): testBlockBOC(blockRoot),
		},
		blockStates: map[storage.BlockRootHash]*storage.BlockState{
			storage.BlockKey(id): {Block: id, StateRootHash: stateRoot.Hash(0), Cell: stateRoot},
		},
	}

	resp := testServer(store).handleQuery(context.Background(), ton.RunSmcMethod{
		Mode:     4,
		ID:       cloneBlockID(id),
		Account:  ton.AccountID{Workchain: masterchainID, ID: accountID},
		MethodID: tlb.MethodNameHash("answer"),
		Params:   testRunMethodParamsBOC(t, paramsCell),
	})

	result, ok := resp.(ton.RunMethodResult)
	if !ok {
		t.Fatalf("response type = %T, want ton.RunMethodResult: %+v", resp, resp)
	}
	if result.ExitCode != 0 {
		t.Fatalf("exit code = %d, want 0", result.ExitCode)
	}
	var stack tlb.Stack
	if err = stack.LoadFromCell(result.Result.MustBeginParse()); err != nil {
		t.Fatalf("load result stack: %v", err)
	}
	value, err := stack.Pop()
	if err != nil {
		t.Fatalf("pop result: %v", err)
	}
	intValue, ok := value.(*big.Int)
	if !ok || intValue.Int64() != 11 {
		t.Fatalf("result value = %#v, want 11", value)
	}
}

func TestRunSmcMethodExecutesAccountLibraryReferenceCodeWithoutProof(t *testing.T) {
	accountID := bytes.Repeat([]byte{0x3a}, 32)
	base := ton.BlockIDExt{Workchain: masterchainID, Shard: masterchainShard, SeqNo: 16}
	code := testCodeFromBuilders(t,
		stackop.POP(0).Serialize(),
		stackop.PUSHINT(big.NewInt(13)).Serialize(),
		execop.RET().Serialize(),
	)
	codeRef, err := cell.BeginCell().
		MustStoreUInt(uint64(cell.LibraryCellType), 8).
		MustStoreSlice(code.Hash(), 256).
		EndCellSpecial(true)
	if err != nil {
		t.Fatalf("build library code ref: %v", err)
	}

	stateRoot, _ := testMasterStateWithActiveAccountStateLibraries(t, base, accountID, codeRef, cell.BeginCell().EndCell(), []*cell.Cell{code})
	id, blockRoot := testBlockForState(t, masterchainID, masterchainShard, base.SeqNo, stateRoot)
	paramsCell, err := tlb.NewStack().ToCell()
	if err != nil {
		t.Fatalf("build params stack: %v", err)
	}
	store := &fakeStore{
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(id): testBlockBOC(blockRoot),
		},
		blockStates: map[storage.BlockRootHash]*storage.BlockState{
			storage.BlockKey(id): {Block: id, StateRootHash: stateRoot.Hash(0), Cell: stateRoot},
		},
	}

	resp := testServer(store).handleQuery(context.Background(), ton.RunSmcMethod{
		Mode:     4,
		ID:       cloneBlockID(id),
		Account:  ton.AccountID{Workchain: masterchainID, ID: accountID},
		MethodID: tlb.MethodNameHash("answer"),
		Params:   testRunMethodParamsBOC(t, paramsCell),
	})

	result, ok := resp.(ton.RunMethodResult)
	if !ok {
		t.Fatalf("response type = %T, want ton.RunMethodResult: %+v", resp, resp)
	}
	if result.ExitCode != 0 {
		t.Fatalf("exit code = %d, want 0", result.ExitCode)
	}
	if result.StateProof != nil {
		t.Fatal("state proof should not be built when mode.1 is not requested")
	}
	var stack tlb.Stack
	if err = stack.LoadFromCell(result.Result.MustBeginParse()); err != nil {
		t.Fatalf("load result stack: %v", err)
	}
	value, err := stack.Pop()
	if err != nil {
		t.Fatalf("pop result: %v", err)
	}
	intValue, ok := value.(*big.Int)
	if !ok || intValue.Int64() != 13 {
		t.Fatalf("result value = %#v, want 13", value)
	}
}

func TestRunSmcMethodReturnsContractNotInitializedForMissingAccount(t *testing.T) {
	accountID := bytes.Repeat([]byte{0x36}, 32)
	base := ton.BlockIDExt{Workchain: masterchainID, Shard: masterchainShard, SeqNo: 11}
	stateRoot := testMasterStateWithConfig(t, base, nil)
	id, blockRoot := testBlockForState(t, masterchainID, masterchainShard, 11, stateRoot)
	params := tlb.NewStack()
	paramsCell, err := params.ToCell()
	if err != nil {
		t.Fatalf("build params stack: %v", err)
	}
	store := &fakeStore{
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(id): testBlockBOC(blockRoot),
		},
		blockStates: map[storage.BlockRootHash]*storage.BlockState{
			storage.BlockKey(id): {Block: id, StateRootHash: stateRoot.Hash(0), Cell: stateRoot},
		},
	}
	srv := testServer(store)

	resp := srv.handleQuery(context.Background(), ton.RunSmcMethod{
		Mode:     7,
		ID:       cloneBlockID(id),
		Account:  ton.AccountID{Workchain: masterchainID, ID: accountID},
		MethodID: tlb.MethodNameHash("missing"),
		Params:   testRunMethodParamsBOC(t, paramsCell),
	})

	result, ok := resp.(ton.RunMethodResult)
	if !ok {
		t.Fatalf("response type = %T, want ton.RunMethodResult: %+v", resp, resp)
	}
	if result.ExitCode != ton.ErrCodeContractNotInitialized {
		t.Fatalf("exit code = %d, want %d", result.ExitCode, ton.ErrCodeContractNotInitialized)
	}
	if len(result.Proof) != 2 {
		t.Fatalf("proof roots = %d, want 2", len(result.Proof))
	}
	if result.StateProof != nil || result.Result != nil {
		t.Fatalf("unexpected state proof/result for missing account")
	}
}

func TestRunSmcMethodRejectsUnsupportedMode(t *testing.T) {
	srv := testServer(&fakeStore{})

	resp := srv.handleQuery(context.Background(), ton.RunSmcMethod{Mode: 0x40})
	errResp, ok := resp.(ton.LSError)
	if !ok {
		t.Fatalf("response type = %T, want ton.LSError", resp)
	}
	if errResp.Code != errCodeProtoViolation {
		t.Fatalf("error code = %d, want %d", errResp.Code, errCodeProtoViolation)
	}
}

func TestRunSmcMethodRejectsOversizedParams(t *testing.T) {
	params := tlb.NewStack()
	params.Push(testLargeCell(t, 600))
	paramsCell, err := params.ToCell()
	if err != nil {
		t.Fatalf("build params stack: %v", err)
	}
	paramsBOC := testRunMethodParamsBOC(t, paramsCell)
	if got := len(paramsBOC); got < runMethodMaxParamBytes {
		t.Fatalf("params boc size = %d, want at least %d", got, runMethodMaxParamBytes)
	}

	srv := testServer(&fakeStore{})
	resp := srv.handleQuery(context.Background(), ton.RunSmcMethod{Params: paramsBOC})
	errResp, ok := resp.(ton.LSError)
	if !ok {
		t.Fatalf("response type = %T, want ton.LSError", resp)
	}
	if errResp.Code != errCodeProtoViolation {
		t.Fatalf("error code = %d, want %d", errResp.Code, errCodeProtoViolation)
	}
}

func TestRunMethodConfigBuildsCppC7Extras(t *testing.T) {
	master := ton.BlockIDExt{
		Workchain: masterchainID,
		Shard:     masterchainShard,
		SeqNo:     123,
		RootHash:  bytes.Repeat([]byte{0x12}, 32),
		FileHash:  bytes.Repeat([]byte{0x13}, 32),
	}
	code := cell.BeginCell().MustStoreUInt(0x9abc, 16).EndCell()
	stateRoot := testMasterStateWithConfig(t, master, map[int32]*cell.Cell{
		int32(tlb.ConfigParamGlobalVersion):        testGlobalVersionCell(t, 13),
		int32(tlb.ConfigParamGlobalID):             cell.BeginCell().MustStoreUInt(0x12345678, 32).EndCell(),
		int32(tlb.ConfigParamStoragePrices):        testStoragePricesConfigCell(t),
		int32(tlb.ConfigParamPrecompiledContracts): testPrecompiledConfigCell(t, code.Hash(), 777),
	})

	cfg, err := runMethodConfig(master, stateRoot, 1500, code)
	if err != nil {
		t.Fatalf("run method config: %v", err)
	}

	prevBlocks, ok := cfg.PrevBlocks.(tuple.Tuple)
	if !ok || prevBlocks.Len() != 3 {
		t.Fatalf("prev blocks = %#v, want tuple len 3", cfg.PrevBlocks)
	}
	lastBlocks := testTupleAt(t, prevBlocks, 0)
	if lastBlocks.Len() != 16 {
		t.Fatalf("last mc blocks len = %d, want 16", lastBlocks.Len())
	}
	if seqno := testBlockIDTupleSeqno(t, testTupleAt(t, lastBlocks, 0)); seqno != 123 {
		t.Fatalf("current mc seqno = %d, want 123", seqno)
	}
	if seqno := testBlockIDTupleSeqno(t, testTupleAt(t, lastBlocks, 15)); seqno != 108 {
		t.Fatalf("oldest included mc seqno = %d, want 108", seqno)
	}
	if seqno := testBlockIDTupleSeqno(t, testTupleAt(t, prevBlocks, 1)); seqno != 123 {
		t.Fatalf("last key block seqno = %d, want current key block 123", seqno)
	}
	lastBlocks100 := testTupleAt(t, prevBlocks, 2)
	if seqno := testBlockIDTupleSeqno(t, testTupleAt(t, lastBlocks100, 0)); seqno != 100 {
		t.Fatalf("first 100-step mc seqno = %d, want 100", seqno)
	}
	if seqno := testBlockIDTupleSeqno(t, testTupleAt(t, lastBlocks100, 1)); seqno != 0 {
		t.Fatalf("second 100-step mc seqno = %d, want 0", seqno)
	}

	unpacked, ok := cfg.Unpacked.(tuple.Tuple)
	if !ok || unpacked.Len() != 7 {
		t.Fatalf("unpacked config = %#v, want tuple len 7", cfg.Unpacked)
	}
	storagePrices := testSliceAt(t, unpacked, 0)
	if got, err := storagePrices.LoadUInt(16); err != nil || got != 0x1000 {
		t.Fatalf("storage price marker = (%x, %v), want 1000", got, err)
	}
	globalID := testSliceAt(t, unpacked, 1)
	if got, err := globalID.LoadUInt(32); err != nil || got != 0x12345678 {
		t.Fatalf("global id marker = (%x, %v), want 12345678", got, err)
	}

	if cfg.Precompiled == nil || cfg.Precompiled.Uint64() != 777 {
		t.Fatalf("precompiled gas = %#v, want 777", cfg.Precompiled)
	}
}

func TestRunMethodConfigRejectsOldGlobalVersion(t *testing.T) {
	master := ton.BlockIDExt{
		Workchain: masterchainID,
		Shard:     masterchainShard,
		SeqNo:     10,
		RootHash:  bytes.Repeat([]byte{0x21}, 32),
		FileHash:  bytes.Repeat([]byte{0x22}, 32),
	}
	stateRoot := testMasterStateWithConfig(t, master, map[int32]*cell.Cell{
		int32(tlb.ConfigParamGlobalVersion): testGlobalVersionCell(t, 12),
	})

	_, err := runMethodConfig(master, stateRoot, 1500, nil)
	if err == nil || !strings.Contains(err.Error(), "unsupported global version 12") {
		t.Fatalf("run method config error = %v, want unsupported global version", err)
	}
}

func TestLoadStateRootUsesStateHashFromMeta(t *testing.T) {
	stateRoot := cell.BeginCell().MustStoreUInt(0xee, 8).EndCell()
	id, blockRoot := testBlockForState(t, masterchainID, masterchainShard, 19, stateRoot)
	wrongState := cell.BeginCell().MustStoreUInt(0xff, 8).EndCell()
	store := &fakeStore{
		current: &storage.CurrentState{
			Masterchain: storage.BlockState{Block: id, StateRootHash: wrongState.Hash(0), Cell: wrongState},
		},
		metas: map[storage.BlockRootHash]*storage.BlockMeta{
			storage.BlockKey(id): {ID: id, StateRootHash: stateRoot.Hash(0)},
		},
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(id): testBlockBOC(blockRoot),
		},
		blockStates: map[storage.BlockRootHash]*storage.BlockState{
			storage.BlockKey(id): {Block: id, StateRootHash: wrongState.Hash(0), Cell: wrongState},
		},
		stateRoots: map[string]*cell.Cell{
			string(stateRoot.Hash(0)): stateRoot,
		},
	}
	srv := testServer(store)

	root, err := srv.loadStateRoot(context.Background(), id)
	if err != nil {
		t.Fatalf("load state root: %v", err)
	}
	if root != stateRoot {
		t.Fatal("expected state root selected by metadata hash")
	}
	if store.currentCalls != 0 || store.blockStateCalls != 0 || store.blockMetaCalls != 1 || store.blockDataCalls != 0 {
		t.Fatalf("unexpected calls: current=%d block_state=%d block_meta=%d block_data=%d", store.currentCalls, store.blockStateCalls, store.blockMetaCalls, store.blockDataCalls)
	}
}

func TestLoadStateRootWithBlockRootRejectsMetaMismatch(t *testing.T) {
	stateRoot := cell.BeginCell().MustStoreUInt(0xee, 8).EndCell()
	id, blockRoot := testBlockForState(t, masterchainID, masterchainShard, 20, stateRoot)
	wrongState := cell.BeginCell().MustStoreUInt(0xff, 8).EndCell()
	store := &fakeStore{
		metas: map[storage.BlockRootHash]*storage.BlockMeta{
			storage.BlockKey(id): {ID: id, StateRootHash: wrongState.Hash(0)},
		},
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(id): testBlockBOC(blockRoot),
		},
		stateRoots: map[string]*cell.Cell{
			string(wrongState.Hash(0)): wrongState,
		},
	}
	srv := testServer(store)

	_, _, err := srv.loadStateRootWithBlockRoot(context.Background(), id)
	if err == nil || !strings.Contains(err.Error(), "state root hash mismatch") {
		t.Fatalf("load state root error = %v, want mismatch", err)
	}
}

func TestBlockFragmentsSkipsBlockMetaOnSuccessfulLoad(t *testing.T) {
	accountID := bytes.Repeat([]byte{0x51}, 32)
	stateRoot, _ := testShardStateWithAccount(t, ton.BlockIDExt{Workchain: 0, Shard: masterchainShard, SeqNo: 21}, accountID)
	id, blockRoot := testBlockForState(t, 0, masterchainShard, 21, stateRoot)
	store := &fakeStore{
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(id): testBlockBOC(blockRoot),
		},
		blockStates: map[storage.BlockRootHash]*storage.BlockState{
			storage.BlockKey(id): {Block: id, StateRootHash: stateRoot.Hash(0), Cell: stateRoot},
		},
	}
	srv := testServer(store)

	fragments, err := srv.blockFragments(context.Background(), id)
	if err != nil {
		t.Fatalf("block fragments: %v", err)
	}
	if fragments == nil || fragments.stateRoot != stateRoot {
		t.Fatal("unexpected fragments state root")
	}
	if store.blockMetaCalls != 0 {
		t.Fatalf("block meta calls = %d, want 0", store.blockMetaCalls)
	}
}

func TestHandleGetStateReturnsSerializedState(t *testing.T) {
	stateRoot := cell.BeginCell().MustStoreUInt(0xee, 8).EndCell()
	id, blockRoot := testBlockForState(t, masterchainID, masterchainShard, 5, stateRoot)
	store := &fakeStore{
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(id): testBlockBOC(blockRoot),
		},
		blockStates: map[storage.BlockRootHash]*storage.BlockState{
			storage.BlockKey(id): {
				Block:         id,
				StateRootHash: stateRoot.Hash(0),
				Cell:          stateRoot,
			},
		},
	}
	srv := testServer(store)

	resp := srv.handleQuery(context.Background(), ton.GetState{ID: cloneBlockID(id)})
	state, ok := resp.(ton.BlockState)
	if !ok {
		t.Fatalf("response type = %T, want ton.BlockState", resp)
	}
	if state.ID == nil || !blockIDEqual(*state.ID, id) {
		t.Fatalf("unexpected block id: %+v", state.ID)
	}
	if !bytes.Equal(state.RootHash, stateRoot.Hash(0)) {
		t.Fatal("state root hash mismatch")
	}
	if len(state.FileHash) != 32 || len(state.Data) == 0 {
		t.Fatalf("expected file hash and data, got file_hash=%x data_len=%d", state.FileHash, len(state.Data))
	}
}

func TestHandleGetStateRejectsLargeSeqnoLikeCpp(t *testing.T) {
	id := testBlockID(t, 1001, cell.BeginCell().EndCell())
	srv := testServer(&fakeStore{})

	resp := srv.handleQuery(context.Background(), ton.GetState{ID: cloneBlockID(id)})
	errResp, ok := resp.(ton.LSError)
	if !ok {
		t.Fatalf("response type = %T, want ton.LSError", resp)
	}
	if errResp.Code != errCodeInternal {
		t.Fatalf("error code = %d, want %d", errResp.Code, errCodeInternal)
	}
}

func TestHandleStateReturnsOriginalZeroStateBytes(t *testing.T) {
	id := ton.BlockIDExt{
		Workchain: masterchainID,
		Shard:     masterchainShard,
		SeqNo:     0,
		RootHash:  bytes.Repeat([]byte{0x11}, 32),
		FileHash:  bytes.Repeat([]byte{0x22}, 32),
	}
	data := []byte{0xaa, 0xbb, 0xcc}
	store := &fakeStore{
		zeroStates: map[storage.BlockRootHash][]byte{
			storage.BlockKey(id): data,
		},
	}
	srv := testServer(store)

	resp := srv.handleQuery(context.Background(), ton.GetState{ID: cloneBlockID(id)})

	state, ok := resp.(ton.BlockState)
	if !ok {
		t.Fatalf("response type = %T, want ton.BlockState: %+v", resp, resp)
	}
	if !bytes.Equal(state.Data, data) {
		t.Fatalf("data = %x, want %x", state.Data, data)
	}
	if !bytes.Equal(state.RootHash, id.RootHash) || !bytes.Equal(state.FileHash, id.FileHash) {
		t.Fatalf("hashes = %x/%x, want id hashes", state.RootHash, state.FileHash)
	}
}

func TestHandleStateZeroStateDoesNotFallbackToBlockState(t *testing.T) {
	stateRoot := cell.BeginCell().MustStoreUInt(0xee, 8).EndCell()
	id, blockRoot := testBlockForState(t, masterchainID, masterchainShard, 0, stateRoot)
	id.FileHash = bytes.Repeat([]byte{0x01}, 32)
	store := &fakeStore{
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(id): testBlockBOC(blockRoot),
		},
		blockStates: map[storage.BlockRootHash]*storage.BlockState{
			storage.BlockKey(id): {
				Block:         id,
				StateRootHash: stateRoot.Hash(0),
				Cell:          stateRoot,
			},
		},
	}
	srv := testServer(store)

	resp := srv.handleQuery(context.Background(), ton.GetState{ID: cloneBlockID(id)})
	lsErr, ok := resp.(ton.LSError)
	if !ok {
		t.Fatalf("response type = %T, want ton.LSError: %+v", resp, resp)
	}
	if lsErr.Code != errCodeNotReady {
		t.Fatalf("error code = %d, want %d", lsErr.Code, errCodeNotReady)
	}
	if !strings.Contains(lsErr.Text, "cannot load zero state") {
		t.Fatalf("error text = %q", lsErr.Text)
	}
}

func TestHandleConfigParamsReturnsTwoProofRoots(t *testing.T) {
	base := ton.BlockIDExt{Workchain: masterchainID, Shard: masterchainShard, SeqNo: 7}
	stateRoot := testMasterStateWithConfig(t, base, map[int32]*cell.Cell{
		15: cell.BeginCell().MustStoreUInt(0x15, 8).EndCell(),
	})
	id, blockRoot := testBlockForState(t, masterchainID, masterchainShard, base.SeqNo, stateRoot)
	store := &fakeStore{
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(id): testBlockBOC(blockRoot),
		},
		blockStates: map[storage.BlockRootHash]*storage.BlockState{
			storage.BlockKey(id): {Block: id, StateRootHash: stateRoot.Hash(0), Cell: stateRoot},
		},
	}
	srv := testServer(store)

	resp := srv.handleQuery(context.Background(), ton.GetConfigParams{
		Mode:    0,
		BlockID: cloneBlockID(id),
		Params:  []int32{15},
	})
	cfg, ok := resp.(ton.ConfigAll)
	if !ok {
		t.Fatalf("response type = %T, want ton.ConfigAll: %+v", resp, resp)
	}
	if len(cfg.StateProof) == 0 || len(cfg.ConfigProof) == 0 {
		t.Fatal("expected state and config proofs")
	}
	if cfg.Mode != 0 || cfg.ID == nil || !blockIDEqual(*cfg.ID, id) {
		t.Fatalf("unexpected config response: %+v", cfg)
	}
}

func TestHandleConfigParamsModeIncludesCapabilitiesParam(t *testing.T) {
	base := ton.BlockIDExt{Workchain: masterchainID, Shard: masterchainShard, SeqNo: 8}
	version, err := tlb.ToCell(&tlb.GlobalVersion{Version: 13, Capabilities: 0x55})
	if err != nil {
		t.Fatalf("build global version: %v", err)
	}
	stateRoot := testMasterStateWithConfig(t, base, map[int32]*cell.Cell{
		int32(tlb.ConfigParamGlobalVersion): version,
	})
	id, blockRoot := testBlockForState(t, masterchainID, masterchainShard, base.SeqNo, stateRoot)
	store := &fakeStore{
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(id): testBlockBOC(blockRoot),
		},
		blockStates: map[storage.BlockRootHash]*storage.BlockState{
			storage.BlockKey(id): {Block: id, StateRootHash: stateRoot.Hash(0), Cell: stateRoot},
		},
	}
	srv := testServer(store)

	resp := srv.handleQuery(context.Background(), ton.GetConfigParams{
		Mode:    int32(configModeNeedCapabilities),
		BlockID: cloneBlockID(id),
	})
	cfg, ok := resp.(ton.ConfigAll)
	if !ok {
		t.Fatalf("response type = %T, want ton.ConfigAll: %+v", resp, resp)
	}
	if cfg.Mode != int(configModeNeedCapabilities) {
		t.Fatalf("mode = %d, want %d", cfg.Mode, configModeNeedCapabilities)
	}
	got := configProofParamCell(t, stateRoot, cfg.ConfigProof, int32(tlb.ConfigParamGlobalVersion))
	if !bytes.Equal(got.Hash(0), version.Hash(0)) {
		t.Fatalf("global version hash = %x, want %x", got.Hash(0), version.Hash(0))
	}
}

func TestHandleConfigParamsStateInfoFlagsNeedConfigInfoPath(t *testing.T) {
	base := ton.BlockIDExt{Workchain: masterchainID, Shard: masterchainShard, SeqNo: 8}
	param := cell.BeginCell().MustStoreUInt(0x15, 8).EndCell()
	stateRoot := testMasterStateWithConfig(t, base, map[int32]*cell.Cell{15: param})
	id, blockRoot := testBlockForState(t, masterchainID, masterchainShard, base.SeqNo, stateRoot)
	store := &fakeStore{
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(id): testBlockBOC(blockRoot),
		},
		blockStates: map[storage.BlockRootHash]*storage.BlockState{
			storage.BlockKey(id): {Block: id, StateRootHash: stateRoot.Hash(0), Cell: stateRoot},
		},
	}
	srv := testServer(store)

	resp := srv.handleQuery(context.Background(), ton.GetConfigParams{
		Mode:    int32(configModeNeedLibraries),
		BlockID: cloneBlockID(id),
		Params:  []int32{15},
	})
	cfg, ok := resp.(ton.ConfigAll)
	if !ok {
		t.Fatalf("response type = %T, want ton.ConfigAll: %+v", resp, resp)
	}
	if cfg.Mode != int(configModeNeedLibraries) {
		t.Fatalf("mode = %d, want %d", cfg.Mode, configModeNeedLibraries)
	}
	if got := configProofParamCell(t, stateRoot, cfg.ConfigProof, 15); !bytes.Equal(got.Hash(0), param.Hash(0)) {
		t.Fatalf("param hash = %x, want %x", got.Hash(0), param.Hash(0))
	}
}

func TestHandleConfigParamsStateInfoFlagsIncludeRequestedStateRoots(t *testing.T) {
	base := ton.BlockIDExt{Workchain: masterchainID, Shard: masterchainShard, SeqNo: 9}
	lib := cell.BeginCell().MustStoreUInt(0x6c, 8).EndCell()
	accountID := bytes.Repeat([]byte{0x31}, 32)
	stateRoot, _ := testMasterStateWithActiveAccountAndLibraries(t, base, accountID, cell.BeginCell().EndCell(), cell.BeginCell().EndCell(), []*cell.Cell{lib})
	id, blockRoot := testBlockForState(t, masterchainID, masterchainShard, base.SeqNo, stateRoot)
	store := &fakeStore{
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(id): testBlockBOC(blockRoot),
		},
		blockStates: map[storage.BlockRootHash]*storage.BlockState{
			storage.BlockKey(id): {Block: id, StateRootHash: stateRoot.Hash(0), Cell: stateRoot},
		},
	}
	srv := testServer(store)

	resp := srv.handleQuery(context.Background(), ton.GetConfigParams{
		Mode:    int32(configModeNeedLibraries | configModeNeedAccountsRoot),
		BlockID: cloneBlockID(id),
	})
	cfg, ok := resp.(ton.ConfigAll)
	if !ok {
		t.Fatalf("response type = %T, want ton.ConfigAll: %+v", resp, resp)
	}

	body := mustUnwrapProof(t, cfg.ConfigProof, stateRoot.Hash(0))
	assertRefType(t, body, 1, cell.OrdinaryCellType)
	assertRefType(t, body, 2, cell.OrdinaryCellType)

	shardBlock := ton.BlockIDExt{
		Workchain: 0,
		Shard:     masterchainShard,
		SeqNo:     5,
		RootHash:  bytes.Repeat([]byte{0x51}, 32),
		FileHash:  bytes.Repeat([]byte{0x52}, 32),
	}
	shardStateRoot := testMasterStateWithShardHashes(t, base, testShardHashes(t, shardBlock))
	shardID, shardBlockRoot := testBlockForState(t, masterchainID, masterchainShard, base.SeqNo, shardStateRoot)
	store.blocks[storage.BlockKey(shardID)] = testBlockBOC(shardBlockRoot)
	store.blockStates[storage.BlockKey(shardID)] = &storage.BlockState{Block: shardID, StateRootHash: shardStateRoot.Hash(0), Cell: shardStateRoot}

	resp = srv.handleQuery(context.Background(), ton.GetConfigParams{
		Mode:    int32(configModeNeedShardHashes),
		BlockID: cloneBlockID(shardID),
	})
	cfg, ok = resp.(ton.ConfigAll)
	if !ok {
		t.Fatalf("shard hashes response type = %T, want ton.ConfigAll: %+v", resp, resp)
	}

	body = mustUnwrapProof(t, cfg.ConfigProof, shardStateRoot.Hash(0))
	extra, err := loadMcStateExtraPrefix(body)
	if err != nil {
		t.Fatalf("load master state extra from proof: %v", err)
	}
	if extra.ShardHashes == nil || extra.ShardHashes.AsCell().GetType() != cell.OrdinaryCellType {
		t.Fatal("shard hashes root is not included in config proof")
	}
}

func TestHandleConfigParamsNeedPrevBlocksAddsCapabilitiesAndProof(t *testing.T) {
	base := ton.BlockIDExt{Workchain: masterchainID, Shard: masterchainShard, SeqNo: 207}
	version, err := tlb.ToCell(&tlb.GlobalVersion{Version: 13})
	if err != nil {
		t.Fatalf("build global version: %v", err)
	}
	stateRoot := testMasterStateWithConfig(t, base, map[int32]*cell.Cell{
		int32(tlb.ConfigParamGlobalVersion): version,
	})
	id, blockRoot := testBlockForState(t, masterchainID, masterchainShard, base.SeqNo, stateRoot)
	store := &fakeStore{
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(id): testBlockBOC(blockRoot),
		},
		blockStates: map[storage.BlockRootHash]*storage.BlockState{
			storage.BlockKey(id): {Block: id, StateRootHash: stateRoot.Hash(0), Cell: stateRoot},
		},
	}
	srv := testServer(store)

	resp := srv.handleQuery(context.Background(), ton.GetConfigParams{
		Mode:    int32(configModeNeedPrevBlocks),
		BlockID: cloneBlockID(id),
	})
	cfg, ok := resp.(ton.ConfigAll)
	if !ok {
		t.Fatalf("response type = %T, want ton.ConfigAll: %+v", resp, resp)
	}
	wantMode := int(configModeNeedPrevBlocks | configModeNeedCapabilities)
	if cfg.Mode != wantMode {
		t.Fatalf("mode = %d, want %d", cfg.Mode, wantMode)
	}
	if seqno := configProofPrevBlockSeqno(t, stateRoot, cfg.ConfigProof, 206); seqno != 206 {
		t.Fatalf("prev block seqno = %d, want 206", seqno)
	}
	if seqno := configProofPrevBlockSeqno(t, stateRoot, cfg.ConfigProof, 200); seqno != 200 {
		t.Fatalf("prev block 100-step seqno = %d, want 200", seqno)
	}
	if seqno := configProofPrevBlockSeqno(t, stateRoot, cfg.ConfigProof, 0); seqno != 0 {
		t.Fatalf("zerostate seqno = %d, want 0", seqno)
	}
	if got := configProofParamCell(t, stateRoot, cfg.ConfigProof, int32(tlb.ConfigParamGlobalVersion)); !bytes.Equal(got.Hash(0), version.Hash(0)) {
		t.Fatalf("global version hash = %x, want %x", got.Hash(0), version.Hash(0))
	}
}

func TestHandleConfigParamsPreviousKeyBlockMode(t *testing.T) {
	keyParam := cell.BeginCell().MustStoreUInt(0x42, 8).EndCell()
	keyID, keyRoot := testKeyBlockWithConfig(t, 5, map[int32]*cell.Cell{42: keyParam})
	blockID, blockRoot := testMasterBlockWithPrevKey(t, 9, 5)
	store := &fakeStore{
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(keyID):   testBlockBOC(keyRoot),
			storage.BlockKey(blockID): testBlockBOC(blockRoot),
		},
		seqLookupByKey: map[fakeSeqLookupKey]ton.BlockIDExt{
			fakeSeqKey(storage.BlockHistoryKey{Workchain: masterchainID, Shard: masterchainShard}, keyID.SeqNo): keyID,
		},
	}
	srv := testServer(store)

	resp := srv.handleQuery(context.Background(), ton.GetConfigParams{
		Mode:    0x8000,
		BlockID: cloneBlockID(blockID),
		Params:  []int32{42},
	})

	cfg, ok := resp.(ton.ConfigAll)
	if !ok {
		t.Fatalf("response type = %T, want ton.ConfigAll: %+v", resp, resp)
	}
	if cfg.ID == nil || !blockIDEqual(*cfg.ID, keyID) {
		t.Fatalf("config id = %+v, want key block %s", cfg.ID, storage.FormatBlockRef(keyID))
	}
	if cfg.Mode != 0x8000 {
		t.Fatalf("mode = %d, want 0x8000", cfg.Mode)
	}
	if len(cfg.StateProof) != 0 || len(cfg.ConfigProof) == 0 {
		t.Fatalf("state/config proof lengths = %d/%d, want empty state proof and config proof", len(cfg.StateProof), len(cfg.ConfigProof))
	}
	if store.seqLookupCalls != 1 {
		t.Fatalf("seq lookup calls = %d, want 1", store.seqLookupCalls)
	}
}

func TestHandleConfigParamsPreviousKeyBlockDoesNotAddCapabilities(t *testing.T) {
	keyParam := cell.BeginCell().MustStoreUInt(0x51, 8).EndCell()
	keyID, keyRoot := testKeyBlockWithConfig(t, 6, map[int32]*cell.Cell{51: keyParam})
	blockID, blockRoot := testMasterBlockWithPrevKey(t, 10, 6)
	store := &fakeStore{
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(keyID):   testBlockBOC(keyRoot),
			storage.BlockKey(blockID): testBlockBOC(blockRoot),
		},
		seqLookupByKey: map[fakeSeqLookupKey]ton.BlockIDExt{
			fakeSeqKey(storage.BlockHistoryKey{Workchain: masterchainID, Shard: masterchainShard}, keyID.SeqNo): keyID,
		},
	}
	srv := testServer(store)

	mode := configModePreviousKeyBlock | configModeNeedPrevBlocks
	resp := srv.handleQuery(context.Background(), ton.GetConfigParams{
		Mode:    int32(mode),
		BlockID: cloneBlockID(blockID),
		Params:  []int32{51},
	})
	cfg, ok := resp.(ton.ConfigAll)
	if !ok {
		t.Fatalf("response type = %T, want ton.ConfigAll: %+v", resp, resp)
	}
	if cfg.Mode != int(mode) {
		t.Fatalf("mode = %d, want %d", cfg.Mode, mode)
	}
	if got := keyBlockConfigProofParamCell(t, keyRoot, cfg.ConfigProof, 51); !bytes.Equal(got.Hash(0), keyParam.Hash(0)) {
		t.Fatalf("key config param hash = %x, want %x", got.Hash(0), keyParam.Hash(0))
	}
}

func TestHandleGetShardInfoReturnsDescriptorFromMasterState(t *testing.T) {
	shardBlock := ton.BlockIDExt{
		Workchain: 0,
		Shard:     masterchainShard,
		SeqNo:     77,
		RootHash:  bytes.Repeat([]byte{0x77}, 32),
		FileHash:  bytes.Repeat([]byte{0x88}, 32),
	}
	shardHashes := testShardHashes(t, shardBlock)
	masterBase := ton.BlockIDExt{Workchain: masterchainID, Shard: masterchainShard, SeqNo: 13}
	stateRoot := testMasterStateWithShardHashes(t, masterBase, shardHashes)
	master, blockRoot := testBlockForState(t, masterchainID, masterchainShard, 13, stateRoot)
	store := &fakeStore{
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(master): testBlockBOC(blockRoot),
		},
		blockStates: map[storage.BlockRootHash]*storage.BlockState{
			storage.BlockKey(master): {Block: master, StateRootHash: stateRoot.Hash(0), Cell: stateRoot},
		},
	}
	srv := testServer(store)

	resp := srv.handleQuery(context.Background(), ton.GetShardInfo{
		ID:        cloneBlockID(master),
		Workchain: shardBlock.Workchain,
		Shard:     shardBlock.Shard,
		Exact:     true,
	})
	info, ok := resp.(ton.ShardInfo)
	if !ok {
		t.Fatalf("response type = %T, want ton.ShardInfo: %+v", resp, resp)
	}
	if info.ShardBlock == nil || !blockIDEqual(*info.ShardBlock, shardBlock) {
		t.Fatalf("unexpected shard block: %+v", info.ShardBlock)
	}
	if len(info.ShardProof) != 2 || info.ShardDescription == nil {
		t.Fatalf("expected shard proof and descriptor, got proof=%d descr=%v", len(info.ShardProof), info.ShardDescription)
	}
}

func TestShardHashesProofFollowsRequestedShardPath(t *testing.T) {
	rootShard := tlb.ShardID(1 << 63)
	leftShard := int64(rootShard.GetChild(true))
	rightShard := int64(rootShard.GetChild(false))
	left := ton.BlockIDExt{
		Workchain: 0,
		Shard:     leftShard,
		SeqNo:     78,
		RootHash:  bytes.Repeat([]byte{0x78}, 32),
		FileHash:  bytes.Repeat([]byte{0x87}, 32),
	}
	right := ton.BlockIDExt{
		Workchain: 0,
		Shard:     rightShard,
		SeqNo:     79,
		RootHash:  bytes.Repeat([]byte{0x79}, 32),
		FileHash:  bytes.Repeat([]byte{0x97}, 32),
	}

	base := ton.BlockIDExt{Workchain: masterchainID, Shard: masterchainShard, SeqNo: 14}
	stateRoot := testMasterStateWithShardHashes(t, base, testForkedShardHashes(t, left, right))

	proof, err := shardHashesProof(stateRoot, left.Workchain, 1, false)
	if err != nil {
		t.Fatalf("create shard hashes proof: %v", err)
	}
	body, err := cell.UnwrapProofVirtualized(proof, stateRoot.Hash(0))
	if err != nil {
		t.Fatalf("unwrap shard hashes proof: %v", err)
	}
	extra, err := mcStateExtra(body)
	if err != nil {
		t.Fatalf("load master state extra from proof: %v", err)
	}

	got, _, err := shardInfoFromHashes(extra.ShardHashes, left.Workchain, left.Shard, true)
	if err != nil {
		t.Fatalf("load selected shard from proof: %v", err)
	}
	if !blockIDEqual(got, left) {
		t.Fatalf("selected shard = %s, want %s", storage.FormatBlockRef(got), storage.FormatBlockRef(left))
	}

	if _, _, err = shardInfoFromHashes(extra.ShardHashes, right.Workchain, right.Shard, true); err == nil {
		t.Fatal("expected sibling shard branch to stay pruned")
	}
}

func TestHandleGetLibrariesSortsDedupsAndLimits(t *testing.T) {
	base := ton.BlockIDExt{Workchain: masterchainID, Shard: masterchainShard, SeqNo: 8}
	libs := make([]*cell.Cell, 18)
	hashes := make([][]byte, 0, len(libs)+1)
	for i := range libs {
		libs[i] = cell.BeginCell().MustStoreUInt(uint64(i+1), 16).EndCell()
		hashes = append(hashes, libs[i].Hash())
	}
	stateRoot := testMasterStateWithLibraries(t, base, libs)
	block, blockRoot := testBlockForState(t, masterchainID, masterchainShard, 8, stateRoot)
	store := &fakeStore{
		current: &storage.CurrentState{
			Masterchain: storage.BlockState{Block: block, StateRootHash: stateRoot.Hash(0), Cell: stateRoot},
		},
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(block): testBlockBOC(blockRoot),
		},
		blockStates: map[storage.BlockRootHash]*storage.BlockState{
			storage.BlockKey(block): {Block: block, StateRootHash: stateRoot.Hash(0), Cell: stateRoot},
		},
	}
	srv := testServer(store)

	resp := srv.handleQuery(context.Background(), ton.GetLibraries{LibraryList: append(hashes, hashes[0])})
	result, ok := resp.(ton.LibraryResult)
	if !ok {
		t.Fatalf("response type = %T, want ton.LibraryResult", resp)
	}
	if len(result.Result) != 16 {
		t.Fatalf("libraries returned = %d, want 16", len(result.Result))
	}
	for _, entry := range result.Result {
		if len(entry.Hash) != 32 || len(entry.Data) == 0 {
			t.Fatalf("bad library entry: %+v", entry)
		}
	}
}

func TestHandleGetLibrariesWithProofModeTwoOmitsLibraryData(t *testing.T) {
	base := ton.BlockIDExt{Workchain: masterchainID, Shard: masterchainShard, SeqNo: 8}
	lib := cell.BeginCell().MustStoreUInt(0x1234, 16).EndCell()
	stateRoot := testMasterStateWithLibraries(t, base, []*cell.Cell{lib})
	block, blockRoot := testBlockForState(t, masterchainID, masterchainShard, 8, stateRoot)
	store := &fakeStore{
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(block): testBlockBOC(blockRoot),
		},
		blockStates: map[storage.BlockRootHash]*storage.BlockState{
			storage.BlockKey(block): {Block: block, StateRootHash: stateRoot.Hash(0), Cell: stateRoot},
		},
	}
	srv := testServer(store)

	resp := srv.handleQuery(context.Background(), ton.GetLibrariesWithProof{
		ID:          &block,
		Mode:        2,
		LibraryList: [][]byte{lib.Hash()},
	})
	result, ok := resp.(ton.LibraryResultWithProof)
	if !ok {
		t.Fatalf("response type = %T, want ton.LibraryResultWithProof: %+v", resp, resp)
	}
	if result.Mode != 2 || len(result.Result) != 1 {
		t.Fatalf("mode/result = %d/%d, want 2/1", result.Mode, len(result.Result))
	}
	if !bytes.Equal(result.Result[0].Hash, lib.Hash()) {
		t.Fatalf("library hash mismatch")
	}
	if len(result.Result[0].Data) != 0 {
		t.Fatalf("library data len = %d, want 0", len(result.Result[0].Data))
	}
	if len(result.StateProof) == 0 || len(result.DataProof) == 0 {
		t.Fatalf("proofs are missing: state=%d data=%d", len(result.StateProof), len(result.DataProof))
	}
}

func TestListBlockTransactionsAndGetOneTransaction(t *testing.T) {
	account := bytes.Repeat([]byte{0x55}, 32)
	tx1 := cell.BeginCell().MustStoreUInt(0x1111, 16).EndCell()
	tx2 := cell.BeginCell().MustStoreUInt(0x2222, 16).EndCell()
	root := testBlockWithTransactions(t, 0, masterchainShard, account, map[uint64]*cell.Cell{
		10: tx1,
		20: tx2,
	})
	id := testBlockIDForRoot(0, masterchainShard, 1, root)
	store := &fakeStore{
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(id): testBlockBOC(root),
		},
	}
	srv := testServer(store)

	resp := srv.handleQuery(context.Background(), ton.ListBlockTransactions{
		ID:    cloneBlockID(id),
		Mode:  7 | 32,
		Count: 1,
	})
	list, ok := resp.(ton.BlockTransactions)
	if !ok {
		t.Fatalf("response type = %T, want ton.BlockTransactions: %+v", resp, resp)
	}
	if len(list.TransactionIds) != 1 || !list.Incomplete {
		t.Fatalf("unexpected transaction list: %+v", list)
	}
	if list.TransactionIds[0].LT != 10 || !bytes.Equal(list.TransactionIds[0].Account, account) {
		t.Fatalf("unexpected transaction id: %+v", list.TransactionIds[0])
	}
	if list.TransactionIds[0].Flags != 7|32 {
		t.Fatalf("transaction id flags = %d, want %d", list.TransactionIds[0].Flags, 7|32)
	}
	if list.Proof == nil {
		t.Fatal("expected transaction list proof")
	}
	listProofBody, err := cell.UnwrapProof(list.Proof, id.RootHash)
	if err != nil {
		t.Fatalf("unwrap transaction list proof: %v", err)
	}
	assertRefType(t, listProofBody, 0, cell.PrunedCellType)
	assertRefType(t, listProofBody, 1, cell.PrunedCellType)
	assertRefType(t, listProofBody, 2, cell.PrunedCellType)
	assertRefType(t, listProofBody, 3, cell.OrdinaryCellType)

	resp = srv.handleQuery(context.Background(), ton.ListBlockTransactions{
		ID:    cloneBlockID(id),
		Mode:  7 | 128,
		Count: 2,
		After: &ton.TransactionID3{Account: account, LT: 10},
	})
	list, ok = resp.(ton.BlockTransactions)
	if !ok {
		t.Fatalf("after response type = %T, want ton.BlockTransactions: %+v", resp, resp)
	}
	if len(list.TransactionIds) != 1 || list.TransactionIds[0].LT != 20 || list.Incomplete {
		t.Fatalf("unexpected after transaction list: %+v", list)
	}
	if list.TransactionIds[0].Flags != 7|128 {
		t.Fatalf("after transaction id flags = %d, want %d", list.TransactionIds[0].Flags, 7|128)
	}

	resp = srv.handleQuery(context.Background(), ton.ListBlockTransactions{
		ID:    cloneBlockID(id),
		Mode:  7 | 64,
		Count: 2,
	})
	list, ok = resp.(ton.BlockTransactions)
	if !ok {
		t.Fatalf("reverse response type = %T, want ton.BlockTransactions: %+v", resp, resp)
	}
	if len(list.TransactionIds) != 2 || list.TransactionIds[0].LT != 20 || list.TransactionIds[1].LT != 10 || !list.Incomplete {
		t.Fatalf("unexpected reverse transaction list: %+v", list)
	}

	resp = srv.handleQuery(context.Background(), ton.GetOneTransaction{
		ID:    cloneBlockID(id),
		AccID: &ton.AccountID{Workchain: 0, ID: account},
		LT:    20,
	})
	info, ok := resp.(ton.TransactionInfo)
	if !ok {
		t.Fatalf("response type = %T, want ton.TransactionInfo", resp)
	}
	if len(info.Proof) == 0 || len(info.Transaction) == 0 {
		t.Fatalf("expected proof and transaction, got proof=%d tx=%d", len(info.Proof), len(info.Transaction))
	}
	txProofBody := mustUnwrapProof(t, info.Proof, id.RootHash)
	assertRefType(t, txProofBody, 0, cell.PrunedCellType)
	assertRefType(t, txProofBody, 1, cell.PrunedCellType)
	assertRefType(t, txProofBody, 2, cell.PrunedCellType)
	assertRefType(t, txProofBody, 3, cell.OrdinaryCellType)
	loaded, err := cell.FromBOC(info.Transaction)
	if err != nil {
		t.Fatalf("load tx boc: %v", err)
	}
	if loaded.HashKey() != tx2.HashKey() {
		t.Fatal("returned transaction hash mismatch")
	}
}

func TestGetTransactionsTraversesPreviousChain(t *testing.T) {
	account := bytes.Repeat([]byte{0x55}, 32)
	oldTx := testTransactionWithPrev(t, account, 10, 0, bytes.Repeat([]byte{0x00}, 32))
	newTx := testTransactionWithPrev(t, account, 20, 10, oldTx.Hash())
	root := testBlockWithTransactions(t, 0, masterchainShard, account, map[uint64]*cell.Cell{
		10: oldTx,
		20: newTx,
	})
	id := testBlockIDForRoot(0, masterchainShard, 1, root)
	key := storage.BlockHistoryKey{Workchain: 0, Shard: masterchainShard}
	store := &fakeStore{
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(id): testBlockBOC(root),
		},
		ltLookup: map[fakeLTLookupKey]ton.BlockIDExt{
			fakeLTKey(key, 20): id,
		},
	}
	srv := testServer(store)

	resp := srv.handleQuery(context.Background(), ton.GetTransactions{
		Limit:  2,
		AccID:  &ton.AccountID{Workchain: 0, ID: account},
		LT:     20,
		TxHash: newTx.Hash(),
	})
	list, ok := resp.(ton.TransactionList)
	if !ok {
		t.Fatalf("response type = %T, want ton.TransactionList: %+v", resp, resp)
	}
	if len(list.IDs) != 2 || !list.IDs[0].Equals(&id) || !list.IDs[1].Equals(&id) {
		t.Fatalf("unexpected transaction block ids: %+v", list.IDs)
	}
	roots, err := cell.FromBOCMultiRoot(list.Transactions)
	if err != nil {
		t.Fatalf("parse transaction list boc: %v", err)
	}
	if len(roots) != 2 || roots[0].HashKey() != newTx.HashKey() || roots[1].HashKey() != oldTx.HashKey() {
		t.Fatalf("unexpected transaction roots order")
	}
	if store.accountLTLookupCalls != 1 {
		t.Fatalf("account lt lookup calls = %d, want 1", store.accountLTLookupCalls)
	}
	if store.ltLookupCalls != 0 {
		t.Fatalf("plain lt lookup calls = %d, want 0", store.ltLookupCalls)
	}
}

func TestGetTransactionsUsesShardHintForPreviousBlock(t *testing.T) {
	account := bytes.Repeat([]byte{0x5d}, 32)
	oldTx := testTransactionWithPrev(t, account, 10, 0, bytes.Repeat([]byte{0x00}, 32))
	newTx := testTransactionWithPrev(t, account, 20, 10, oldTx.Hash())
	oldRoot := testBlockWithTransactions(t, 0, masterchainShard, account, map[uint64]*cell.Cell{
		10: oldTx,
	})
	newRoot := testBlockWithTransactions(t, 0, masterchainShard, account, map[uint64]*cell.Cell{
		20: newTx,
	})
	oldID := testBlockIDForRoot(0, masterchainShard, 1, oldRoot)
	newID := testBlockIDForRoot(0, masterchainShard, 2, newRoot)
	key := storage.BlockHistoryKey{Workchain: 0, Shard: masterchainShard}
	store := &fakeStore{
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(oldID): testBlockBOC(oldRoot),
			storage.BlockKey(newID): testBlockBOC(newRoot),
		},
		ltLookup: map[fakeLTLookupKey]ton.BlockIDExt{
			fakeLTKey(key, 10): oldID,
			fakeLTKey(key, 20): newID,
		},
	}
	srv := testServer(store)

	resp := srv.handleQuery(context.Background(), ton.GetTransactions{
		Limit:  2,
		AccID:  &ton.AccountID{Workchain: 0, ID: account},
		LT:     20,
		TxHash: newTx.Hash(),
	})
	list, ok := resp.(ton.TransactionList)
	if !ok {
		t.Fatalf("response type = %T, want ton.TransactionList: %+v", resp, resp)
	}
	if len(list.IDs) != 2 || !list.IDs[0].Equals(&newID) || !list.IDs[1].Equals(&oldID) {
		t.Fatalf("unexpected transaction block ids: %+v", list.IDs)
	}
	roots, err := cell.FromBOCMultiRoot(list.Transactions)
	if err != nil {
		t.Fatalf("parse transaction list boc: %v", err)
	}
	if len(roots) != 2 || roots[0].HashKey() != newTx.HashKey() || roots[1].HashKey() != oldTx.HashKey() {
		t.Fatalf("unexpected transaction roots order")
	}
	if store.accountLTLookupCalls != 1 {
		t.Fatalf("account lt lookup calls = %d, want 1", store.accountLTLookupCalls)
	}
	if store.ltLookupCalls != 1 {
		t.Fatalf("plain lt lookup calls = %d, want 1", store.ltLookupCalls)
	}
}

func TestGetTransactionsCurrentBlockSkipsOutOfHeaderRange(t *testing.T) {
	account := bytes.Repeat([]byte{0x5e}, 32)
	block := &transactionSearchBlock{
		id:      ton.BlockIDExt{Workchain: 0, Shard: masterchainShard},
		root:    cell.BeginCell().EndCell(),
		startLT: 20,
		endLT:   30,
	}

	tx, gotBlock, err := accountTransactionFromCurrentBlock(block, 0, account, 10)
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("error = %v, want ErrNotFound", err)
	}
	if tx != nil || gotBlock != nil {
		t.Fatalf("transaction/current block = %v/%v, want nil/nil", tx, gotBlock)
	}
}

func TestGetTransactionsMissingFirstReturnsError(t *testing.T) {
	account := bytes.Repeat([]byte{0x56}, 32)
	tx := testTransactionWithPrev(t, account, 30, 0, bytes.Repeat([]byte{0x00}, 32))
	root := testBlockWithTransactions(t, 0, masterchainShard, account, map[uint64]*cell.Cell{
		30: tx,
	})
	id := testBlockIDForRoot(0, masterchainShard, 1, root)
	key := storage.BlockHistoryKey{Workchain: 0, Shard: masterchainShard}
	store := &fakeStore{
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(id): testBlockBOC(root),
		},
		ltLookup: map[fakeLTLookupKey]ton.BlockIDExt{
			fakeLTKey(key, 20): id,
		},
	}
	srv := testServer(store)

	resp := srv.handleQuery(context.Background(), ton.GetTransactions{
		Limit:  2,
		AccID:  &ton.AccountID{Workchain: 0, ID: account},
		LT:     20,
		TxHash: bytes.Repeat([]byte{0x11}, 32),
	})
	lsErr, ok := resp.(ton.LSError)
	if !ok {
		t.Fatalf("response type = %T, want ton.LSError: %+v", resp, resp)
	}
	if lsErr.Code != -400 || lsErr.Text != "cannot locate transaction in block with specified logical time" {
		t.Fatalf("unexpected error: %+v", lsErr)
	}
}

func TestGetTransactionsMissingBlockLookupReturnsLocateError(t *testing.T) {
	account := bytes.Repeat([]byte{0x56}, 32)
	store := &fakeStore{}
	srv := testServer(store)

	resp := srv.handleQuery(context.Background(), ton.GetTransactions{
		Limit:  2,
		AccID:  &ton.AccountID{Workchain: 0, ID: account},
		LT:     20,
		TxHash: bytes.Repeat([]byte{0x11}, 32),
	})
	lsErr, ok := resp.(ton.LSError)
	if !ok {
		t.Fatalf("response type = %T, want ton.LSError: %+v", resp, resp)
	}
	if lsErr.Code != -400 || lsErr.Text != "cannot locate transaction in block with specified logical time" {
		t.Fatalf("unexpected error: %+v", lsErr)
	}
}

func TestGetTransactionsReturnsPartialAfterChainGap(t *testing.T) {
	account := bytes.Repeat([]byte{0x57}, 32)
	newTx := testTransactionWithPrev(t, account, 20, 10, bytes.Repeat([]byte{0x77}, 32))
	root := testBlockWithTransactions(t, 0, masterchainShard, account, map[uint64]*cell.Cell{
		20: newTx,
	})
	id := testBlockIDForRoot(0, masterchainShard, 1, root)
	key := storage.BlockHistoryKey{Workchain: 0, Shard: masterchainShard}
	store := &fakeStore{
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(id): testBlockBOC(root),
		},
		ltLookup: map[fakeLTLookupKey]ton.BlockIDExt{
			fakeLTKey(key, 20): id,
			fakeLTKey(key, 10): id,
		},
	}
	srv := testServer(store)

	resp := srv.handleQuery(context.Background(), ton.GetTransactions{
		Limit:  2,
		AccID:  &ton.AccountID{Workchain: 0, ID: account},
		LT:     20,
		TxHash: newTx.Hash(),
	})
	list, ok := resp.(ton.TransactionList)
	if !ok {
		t.Fatalf("response type = %T, want ton.TransactionList: %+v", resp, resp)
	}
	roots, err := cell.FromBOCMultiRoot(list.Transactions)
	if err != nil {
		t.Fatalf("parse transaction list boc: %v", err)
	}
	if len(roots) != 1 || roots[0].HashKey() != newTx.HashKey() || len(list.IDs) != 1 {
		t.Fatalf("unexpected partial transaction list: ids=%d roots=%d", len(list.IDs), len(roots))
	}
}

func TestGetTransactionsRejectsHashMismatch(t *testing.T) {
	account := bytes.Repeat([]byte{0x58}, 32)
	tx := testTransactionWithPrev(t, account, 20, 0, bytes.Repeat([]byte{0x00}, 32))
	root := testBlockWithTransactions(t, 0, masterchainShard, account, map[uint64]*cell.Cell{
		20: tx,
	})
	id := testBlockIDForRoot(0, masterchainShard, 1, root)
	key := storage.BlockHistoryKey{Workchain: 0, Shard: masterchainShard}
	store := &fakeStore{
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(id): testBlockBOC(root),
		},
		ltLookup: map[fakeLTLookupKey]ton.BlockIDExt{
			fakeLTKey(key, 20): id,
		},
	}
	srv := testServer(store)

	resp := srv.handleQuery(context.Background(), ton.GetTransactions{
		Limit:  1,
		AccID:  &ton.AccountID{Workchain: 0, ID: account},
		LT:     20,
		TxHash: bytes.Repeat([]byte{0x99}, 32),
	})
	lsErr, ok := resp.(ton.LSError)
	if !ok {
		t.Fatalf("response type = %T, want ton.LSError: %+v", resp, resp)
	}
	if lsErr.Code != -400 || lsErr.Text != "transaction hash mismatch" {
		t.Fatalf("unexpected error: %+v", lsErr)
	}
}

func TestGetTransactionsCapsLimitAtSixteen(t *testing.T) {
	account := bytes.Repeat([]byte{0x59}, 32)
	zeroHash := bytes.Repeat([]byte{0x00}, 32)
	txs := make(map[uint64]*cell.Cell)
	var newestTx *cell.Cell
	var newestLT uint64
	prevLT := uint64(0)
	prevHash := zeroHash
	for i := 1; i <= 17; i++ {
		lt := uint64(i * 5)
		tx := testTransactionWithPrev(t, account, lt, prevLT, prevHash)
		txs[lt] = tx
		newestLT = lt
		newestTx = tx
		prevLT = lt
		prevHash = tx.Hash()
	}
	root := testBlockWithTransactions(t, 0, masterchainShard, account, txs)
	id := testBlockIDForRoot(0, masterchainShard, 1, root)
	key := storage.BlockHistoryKey{Workchain: 0, Shard: masterchainShard}
	store := &fakeStore{
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(id): testBlockBOC(root),
		},
		ltLookup: map[fakeLTLookupKey]ton.BlockIDExt{
			fakeLTKey(key, newestLT): id,
		},
	}
	srv := testServer(store)

	resp := srv.handleQuery(context.Background(), ton.GetTransactions{
		Limit:  20,
		AccID:  &ton.AccountID{Workchain: 0, ID: account},
		LT:     int64(newestLT),
		TxHash: newestTx.Hash(),
	})
	list, ok := resp.(ton.TransactionList)
	if !ok {
		t.Fatalf("response type = %T, want ton.TransactionList: %+v", resp, resp)
	}
	roots, err := cell.FromBOCMultiRoot(list.Transactions)
	if err != nil {
		t.Fatalf("parse transaction list boc: %v", err)
	}
	if len(roots) != maxGetTransactions || len(list.IDs) != maxGetTransactions {
		t.Fatalf("returned %d roots and %d ids, want %d", len(roots), len(list.IDs), maxGetTransactions)
	}
	if roots[0].HashKey() != newestTx.HashKey() || roots[len(roots)-1].HashKey() != txs[10].HashKey() {
		t.Fatalf("unexpected capped transaction roots")
	}
}

func TestGetTransactionsZeroLTMatchesCppEmptyList(t *testing.T) {
	account := bytes.Repeat([]byte{0x5a}, 32)
	srv := testServer(&fakeStore{})

	resp := srv.handleQuery(context.Background(), ton.GetTransactions{
		Limit:  1,
		AccID:  &ton.AccountID{Workchain: 0, ID: account},
		LT:     0,
		TxHash: bytes.Repeat([]byte{0x00}, 32),
	})
	list, ok := resp.(ton.TransactionList)
	if !ok {
		t.Fatalf("response type = %T, want ton.TransactionList: %+v", resp, resp)
	}
	if len(list.IDs) != 0 || len(list.Transactions) != 0 {
		t.Fatalf("unexpected zero-lt transaction list: ids=%d boc=%d", len(list.IDs), len(list.Transactions))
	}
}

func TestListBlockTransactionsMode256ReturnsTransactionMetadata(t *testing.T) {
	account := bytes.Repeat([]byte{0x66}, 32)
	initiator := bytes.Repeat([]byte{0x77}, 32)
	tx, msg := testTransactionWithInMsg(t, account, 42)
	envelope := testMsgEnvelopeWithMetadata(t, msg, initiator, 3, 99)
	inMsgDesc := testInMsgDescr(t, msg, envelope, tx)
	root := testBlockWithTransactionsAndInMsgDesc(t, 0, masterchainShard, account, map[uint64]*cell.Cell{
		42: tx,
	}, inMsgDesc)
	id := testBlockIDForRoot(0, masterchainShard, 1, root)
	store := &fakeStore{
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(id): testBlockBOC(root),
		},
	}
	srv := testServer(store)

	resp := srv.handleQuery(context.Background(), ton.ListBlockTransactions{
		ID:    cloneBlockID(id),
		Mode:  7 | 256,
		Count: 1,
	})

	list, ok := resp.(ton.BlockTransactions)
	if !ok {
		t.Fatalf("response type = %T, want ton.BlockTransactions: %+v", resp, resp)
	}
	if len(list.TransactionIds) != 1 {
		t.Fatalf("transaction count = %d, want 1", len(list.TransactionIds))
	}
	txID := list.TransactionIds[0]
	if txID.Flags&256 == 0 || txID.Metadata == nil {
		t.Fatalf("metadata is missing in transaction id: %+v", txID)
	}
	if txID.Metadata.Depth != 3 || txID.Metadata.InitiatorLT != 99 {
		t.Fatalf("metadata = %+v, want depth=3 lt=99", txID.Metadata)
	}
	if txID.Metadata.Initiator.Workchain != 0 || !bytes.Equal(txID.Metadata.Initiator.ID, initiator) {
		t.Fatalf("metadata initiator = %+v, want 0:%x", txID.Metadata.Initiator, initiator)
	}
}

func TestWaitMasterchainSeqnoPrefixRunsWrappedQueryAfterLiveState(t *testing.T) {
	live := NewLiveStore(&fakeStore{})
	current10, root10, data10 := testCurrentStateWithLiveBlock(t, 10)
	if err := live.publishLiveBlockData(current10.Masterchain.Block, root10, data10, true); err != nil {
		t.Fatalf("set live block 10: %v", err)
	}
	live.SetLiveCurrentState(current10)
	srv := testServer(live)

	req := serializeWaitPrefixedQuery(t,
		ton.WaitMasterchainSeqno{Seqno: 11, Timeout: 500},
		ton.GetMasterchainInfoExt{Mode: 0},
	)
	var parsed tl.Serializable
	if _, err := tl.Parse(&parsed, req, true); err != nil {
		t.Fatal(err)
	}

	done := make(chan tl.Serializable, 1)
	go func() {
		done <- srv.handleQueryData(context.Background(), parsed)
	}()

	select {
	case resp := <-done:
		t.Fatalf("wait-prefixed query completed before target state: %+v", resp)
	case <-time.After(20 * time.Millisecond):
	}

	current11, root11, data11 := testCurrentStateWithLiveBlock(t, 11)
	if err := live.publishLiveBlockData(current11.Masterchain.Block, root11, data11, true); err != nil {
		t.Fatalf("set live block 11: %v", err)
	}
	live.SetLiveCurrentState(current11)

	select {
	case resp := <-done:
		info, ok := resp.(ton.MasterchainInfoExt)
		if !ok {
			t.Fatalf("response type = %T, want ton.MasterchainInfoExt: %+v", resp, resp)
		}
		if info.Last == nil || info.Last.SeqNo != 11 {
			t.Fatalf("last masterchain seqno = %+v, want 11", info.Last)
		}
	case <-time.After(time.Second):
		t.Fatal("wait-prefixed query did not complete after target state")
	}
}

func TestWaitMasterchainSeqnoPrefixWaitsForReadableLiveCurrent(t *testing.T) {
	stateRoot := cell.BeginCell().MustStoreUInt(0xbb, 8).EndCell()
	block, root := testBlockForState(t, masterchainID, masterchainShard, 11, stateRoot)
	payload := testBlockBOC(root)
	block = testBlockIDForData(masterchainID, masterchainShard, block.SeqNo, root, payload)
	current := &storage.CurrentState{
		Masterchain: storage.BlockState{
			Block:         block,
			StateRootHash: stateRoot.Hash(0),
		},
	}

	live := NewLiveStore(&fakeStore{})
	live.SetLiveCurrentState(current)
	srv := testServer(live)

	req := serializeRawWaitPrefixedQuery(t,
		ton.WaitMasterchainSeqno{Seqno: int32(block.SeqNo), Timeout: 500},
		ton.LookupBlock{
			Mode: 1,
			ID: &ton.BlockInfoShort{
				Workchain: block.Workchain,
				Shard:     block.Shard,
				Seqno:     int32(block.SeqNo),
			},
		},
	)

	done := make(chan tl.Serializable, 1)
	go func() {
		done <- srv.handleQueryData(context.Background(), req)
	}()

	select {
	case resp := <-done:
		t.Fatalf("wait-prefixed lookup completed before live current was readable: %+v", resp)
	case <-time.After(20 * time.Millisecond):
	}

	if err := live.publishLiveBlockData(block, root, payload, true); err != nil {
		t.Fatalf("set target live block: %v", err)
	}

	select {
	case resp := <-done:
		header, ok := resp.(ton.BlockHeader)
		if !ok {
			t.Fatalf("response type = %T, want ton.BlockHeader: %+v", resp, resp)
		}
		if header.ID == nil || !blockIDEqual(*header.ID, block) {
			t.Fatalf("lookup block id = %+v, want %+v", header.ID, block)
		}
	case <-time.After(time.Second):
		t.Fatal("wait-prefixed lookup did not complete after block data became available")
	}
}

func TestWaitMasterchainSeqnoPrefixErrors(t *testing.T) {
	live := NewLiveStore(&fakeStore{})
	current, root, data := testCurrentStateWithLiveBlock(t, 20)
	if err := live.publishLiveBlockData(current.Masterchain.Block, root, data, true); err != nil {
		t.Fatalf("set live master block: %v", err)
	}
	live.SetLiveCurrentState(current)
	srv := testServer(live)

	timeoutReq := serializeRawWaitPrefixedQuery(t,
		ton.WaitMasterchainSeqno{Seqno: 21, Timeout: 1},
		ton.GetMasterchainInfoExt{Mode: 0},
	)
	resp := srv.handleQueryData(context.Background(), timeoutReq)
	errResp, ok := resp.(ton.LSError)
	if !ok {
		t.Fatalf("timeout response type = %T, want ton.LSError: %+v", resp, resp)
	}
	if errResp.Code != errCodeTimeout {
		t.Fatalf("timeout code = %d, want %d", errResp.Code, errCodeTimeout)
	}

	tooFarReq := serializeRawWaitPrefixedQuery(t,
		ton.WaitMasterchainSeqno{Seqno: 121, Timeout: 500},
		ton.GetMasterchainInfoExt{Mode: 0},
	)
	resp = srv.handleQueryData(context.Background(), tooFarReq)
	errResp, ok = resp.(ton.LSError)
	if !ok {
		t.Fatalf("too-far response type = %T, want ton.LSError: %+v", resp, resp)
	}
	if errResp.Code != errCodeNotReady {
		t.Fatalf("too-far code = %d, want %d", errResp.Code, errCodeNotReady)
	}
}

func TestHandleBlockOutMsgQueueSize(t *testing.T) {
	base := ton.BlockIDExt{Workchain: masterchainID, Shard: masterchainShard, SeqNo: 12}
	stateRoot := testShardStateWithOutMsgQueueSize(t, base, 12345)
	id, blockRoot := testBlockForState(t, masterchainID, masterchainShard, 12, stateRoot)
	store := &fakeStore{
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(id): testBlockBOC(blockRoot),
		},
		blockStates: map[storage.BlockRootHash]*storage.BlockState{
			storage.BlockKey(id): {Block: id, StateRootHash: stateRoot.Hash(0), Cell: stateRoot},
		},
	}
	srv := testServer(store)

	resp := srv.handleQuery(context.Background(), ton.GetBlockOutMsgQueueSize{
		Mode: 0,
		ID:   cloneBlockID(id),
	})
	size, ok := resp.(ton.BlockOutMsgQueueSize)
	if !ok {
		t.Fatalf("response type = %T, want ton.BlockOutMsgQueueSize", resp)
	}
	if size.Size != 12345 {
		t.Fatalf("queue size = %d, want 12345", size.Size)
	}

	resp = srv.handleQuery(context.Background(), ton.GetBlockOutMsgQueueSize{
		Mode: 1,
		ID:   cloneBlockID(id),
	})
	size, ok = resp.(ton.BlockOutMsgQueueSize)
	if !ok {
		t.Fatalf("proof response type = %T, want ton.BlockOutMsgQueueSize: %+v", resp, resp)
	}
	roots, err := cell.FromBOCMultiRoot(size.Proof)
	if err != nil {
		t.Fatalf("load queue proof: %v", err)
	}
	if len(roots) != 2 {
		t.Fatalf("queue proof roots = %d, want 2", len(roots))
	}
	blockProofBody, err := cell.UnwrapProof(roots[0], id.RootHash)
	if err != nil {
		t.Fatalf("unwrap queue block proof: %v", err)
	}
	assertRefType(t, blockProofBody, 2, cell.MerkleUpdateCellType)
	assertRefType(t, blockProofBody, 3, cell.PrunedCellType)
}

func TestHandleDispatchQueueInfoEmptyQueue(t *testing.T) {
	base := ton.BlockIDExt{Workchain: masterchainID, Shard: masterchainShard, SeqNo: 13}
	stateRoot := testShardStateWithOutMsgQueueSize(t, base, 0)
	id, blockRoot := testBlockForState(t, masterchainID, masterchainShard, 13, stateRoot)
	store := &fakeStore{
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(id): testBlockBOC(blockRoot),
		},
		blockStates: map[storage.BlockRootHash]*storage.BlockState{
			storage.BlockKey(id): {Block: id, StateRootHash: stateRoot.Hash(0), Cell: stateRoot},
		},
	}
	srv := testServer(store)

	resp := srv.handleQuery(context.Background(), ton.GetDispatchQueueInfo{
		Mode:        1,
		ID:          cloneBlockID(id),
		MaxAccounts: 1,
	})
	info, ok := resp.(ton.DispatchQueueInfo)
	if !ok {
		t.Fatalf("response type = %T, want ton.DispatchQueueInfo: %+v", resp, resp)
	}
	if !info.Complete || len(info.AccountDispatchQueues) != 0 {
		t.Fatalf("unexpected dispatch queue info: %+v", info)
	}
	roots, err := cell.FromBOCMultiRoot(info.Proof)
	if err != nil {
		t.Fatalf("load dispatch queue proof: %v", err)
	}
	if len(roots) != 2 {
		t.Fatalf("dispatch queue proof roots = %d, want 2", len(roots))
	}
}

func TestHandleDispatchQueueMessagesEmptyQueueWithBOC(t *testing.T) {
	base := ton.BlockIDExt{Workchain: masterchainID, Shard: masterchainShard, SeqNo: 14}
	stateRoot := testShardStateWithOutMsgQueueSize(t, base, 0)
	id, blockRoot := testBlockForState(t, masterchainID, masterchainShard, 14, stateRoot)
	store := &fakeStore{
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(id): testBlockBOC(blockRoot),
		},
		blockStates: map[storage.BlockRootHash]*storage.BlockState{
			storage.BlockKey(id): {Block: id, StateRootHash: stateRoot.Hash(0), Cell: stateRoot},
		},
	}
	srv := testServer(store)

	resp := srv.handleQuery(context.Background(), ton.GetDispatchQueueMessages{
		Mode:        1 | 4,
		ID:          cloneBlockID(id),
		Addr:        bytes.Repeat([]byte{0x01}, 32),
		MaxMessages: 1,
	})
	messages, ok := resp.(ton.DispatchQueueMessages)
	if !ok {
		t.Fatalf("response type = %T, want ton.DispatchQueueMessages: %+v", resp, resp)
	}
	if !messages.Complete || len(messages.Messages) != 0 {
		t.Fatalf("unexpected dispatch queue messages: %+v", messages)
	}
	if len(messages.MessagesBOC) != 0 {
		_, err := cell.FromBOCMultiRoot(messages.MessagesBOC)
		if err != nil {
			t.Fatalf("load dispatch queue messages boc: %v", err)
		}
	}
}

func TestHandleOutMsgQueueSizesUsesCurrentMasterAndShardStates(t *testing.T) {
	masterBase := ton.BlockIDExt{Workchain: masterchainID, Shard: masterchainShard, SeqNo: 20}
	shardBase := ton.BlockIDExt{Workchain: 0, Shard: masterchainShard, SeqNo: 30}
	masterState := testShardStateWithOutMsgQueueSize(t, masterBase, 11)
	shardState := testShardStateWithOutMsgQueueSize(t, shardBase, 22)
	master, masterRoot := testBlockForState(t, masterchainID, masterchainShard, 20, masterState)
	shard, shardRoot := testBlockForState(t, 0, masterchainShard, 30, shardState)
	store := &fakeStore{
		current: &storage.CurrentState{
			Masterchain: storage.BlockState{Block: master, StateRootHash: masterState.Hash(0), Cell: masterState},
			Shards: map[storage.ShardKey]storage.BlockState{
				storage.ShardKeyFromBlock(shard): {Block: shard, StateRootHash: shardState.Hash(0), Cell: shardState},
			},
		},
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(master): testBlockBOC(masterRoot),
			storage.BlockKey(shard):  testBlockBOC(shardRoot),
		},
		blockStates: map[storage.BlockRootHash]*storage.BlockState{
			storage.BlockKey(master): {Block: master, StateRootHash: masterState.Hash(0), Cell: masterState},
			storage.BlockKey(shard):  {Block: shard, StateRootHash: shardState.Hash(0), Cell: shardState},
		},
	}
	srv := testServer(store)

	resp := srv.handleQuery(context.Background(), ton.GetOutMsgQueueSizes{})
	sizes, ok := resp.(ton.OutMsgQueueSizes)
	if !ok {
		t.Fatalf("response type = %T, want ton.OutMsgQueueSizes: %+v", resp, resp)
	}
	if sizes.ExtMsgQueueSizeLimit != outMsgQueueSizeLimit {
		t.Fatalf("ext msg queue limit = %d, want %d", sizes.ExtMsgQueueSizeLimit, outMsgQueueSizeLimit)
	}
	if len(sizes.Shards) != 2 || sizes.Shards[0].Size != 11 || sizes.Shards[1].Size != 22 {
		t.Fatalf("queue sizes = %+v, want master=11 shard=22", sizes.Shards)
	}

	resp = srv.handleQuery(context.Background(), ton.GetOutMsgQueueSizes{Mode: 2})
	sizes, ok = resp.(ton.OutMsgQueueSizes)
	if !ok {
		t.Fatalf("unknown mode response type = %T, want ton.OutMsgQueueSizes: %+v", resp, resp)
	}
	if len(sizes.Shards) != 2 {
		t.Fatalf("unknown mode queue sizes = %+v, want both shards", sizes.Shards)
	}

	resp = srv.handleQuery(context.Background(), ton.GetOutMsgQueueSizes{
		Mode:  1,
		WC:    0,
		Shard: masterchainShard,
	})
	sizes, ok = resp.(ton.OutMsgQueueSizes)
	if !ok {
		t.Fatalf("filtered response type = %T, want ton.OutMsgQueueSizes: %+v", resp, resp)
	}
	if len(sizes.Shards) != 1 || sizes.Shards[0].ID == nil || !blockIDEqual(*sizes.Shards[0].ID, shard) || sizes.Shards[0].Size != 22 {
		t.Fatalf("filtered queue sizes = %+v, want shard only", sizes.Shards)
	}
}

func TestValidatorStatsCountParsesBlockCreateStatsLayouts(t *testing.T) {
	base := ton.BlockIDExt{Workchain: masterchainID, Shard: masterchainShard, SeqNo: 13}
	for _, magic := range []uint64{0x17, 0x34} {
		stateRoot := testMasterStateWithBlockCreateStats(t, base, magic)

		count, complete, err := validatorStatsCount(stateRoot, 0, 10, nil)
		if err != nil {
			t.Fatalf("magic %#x validator stats count: %v", magic, err)
		}
		if count != 0 || !complete {
			t.Fatalf("magic %#x count = %d, complete = %v, want 0 true", magic, count, complete)
		}
	}
}

func testServer(store Store) *Server {
	return &Server{
		store:                  store,
		zeroState:              ton.ZeroStateIDExt{Workchain: masterchainID, RootHash: bytes.Repeat([]byte{0x01}, 32), FileHash: bytes.Repeat([]byte{0x02}, 32)},
		version:                DefaultVersion,
		capabilities:           DefaultCapabilities,
		tvm:                    tvm.NewTVM(),
		sendMessageCache:       newSendMessageCache(),
		externalMessageLimiter: extmsg.NewDefaultAddressLimiter(),
		now: func() time.Time {
			return time.Unix(1700000000, 0)
		},
	}
}

func testCurrentStateWithLiveBlock(t *testing.T, seqno uint32) (*storage.CurrentState, *cell.Cell, []byte) {
	t.Helper()

	root := cell.BeginCell().MustStoreUInt(uint64(seqno), 32).EndCell()
	data := testBlockBOC(root)
	current := &storage.CurrentState{
		Masterchain: storage.BlockState{
			Block:         testBlockIDForData(masterchainID, masterchainShard, seqno, root, data),
			StateRootHash: bytes.Repeat([]byte{byte(seqno)}, 32),
		},
	}
	return current, root, data
}

func testLiveStoreIndexBlock(seqno uint32, key storage.BlockHistoryKey) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: key.Workchain,
		Shard:     key.Shard,
		SeqNo:     seqno,
		RootHash:  bytes.Repeat([]byte{byte(seqno)}, 32),
		FileHash:  bytes.Repeat([]byte{byte(seqno + 64)}, 32),
	}
}

func serializeWaitPrefixedQuery(t *testing.T, wait ton.WaitMasterchainSeqno, query tl.Serializable) []byte {
	t.Helper()

	raw := serializeRawWaitPrefixedQuery(t, wait, query)
	data, err := tl.Serialize(liteclient.LiteServerQuery{Data: tl.Raw(raw)}, true)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func serializeRawWaitPrefixedQuery(t *testing.T, wait ton.WaitMasterchainSeqno, query tl.Serializable) tl.Raw {
	t.Helper()

	prefix, err := tl.Serialize(wait, true)
	if err != nil {
		t.Fatal(err)
	}
	suffix, err := tl.Serialize(query, true)
	if err != nil {
		t.Fatal(err)
	}
	return tl.Raw(append(prefix, suffix...))
}

func testBlockIDForRoot(workchain int32, shard int64, seqno uint32, root *cell.Cell) ton.BlockIDExt {
	rootHash := root.HashKey(0)
	return ton.BlockIDExt{
		Workchain: workchain,
		Shard:     shard,
		SeqNo:     seqno,
		RootHash:  append([]byte(nil), rootHash[:]...),
		FileHash:  bytes.Repeat([]byte{byte(seqno)}, 32),
	}
}

func testBlockBOC(root *cell.Cell) []byte {
	return root.ToBOCWithOptions(cell.BOCSerializeOptions{
		WithCRC32C:    true,
		WithIndex:     true,
		WithCacheBits: true,
		WithTopHash:   true,
		WithIntHashes: true,
	})
}

func testBlockIDForData(workchain int32, shard int64, seqno uint32, root *cell.Cell, data []byte) ton.BlockIDExt {
	rootHash := root.HashKey(0)
	fileHash := sha256.Sum256(data)
	return ton.BlockIDExt{
		Workchain: workchain,
		Shard:     shard,
		SeqNo:     seqno,
		RootHash:  append([]byte(nil), rootHash[:]...),
		FileHash:  append([]byte(nil), fileHash[:]...),
	}
}

func testBlockID(t *testing.T, seqno uint32, root *cell.Cell) ton.BlockIDExt {
	t.Helper()

	return testBlockIDForRoot(masterchainID, masterchainShard, seqno, root)
}

func testBlockForState(t *testing.T, workchain int32, shard int64, seqno uint32, stateRoot *cell.Cell) (ton.BlockIDExt, *cell.Cell) {
	t.Helper()

	var header tlb.BlockHeader
	header.Version = 1
	header.Shard = tlb.ShardIdent{PrefixBits: 0, WorkchainID: workchain, ShardPrefix: uint64(shard)}
	header.SeqNo = seqno
	header.StartLt = 1
	header.EndLt = 100
	header.GenUtime = 1000
	header.MinRefMcSeqno = seqno
	header.PrevKeyBlockSeqno = 0
	header.PrevRef = tlb.BlkPrevInfo{Prev1: tlb.ExtBlkRef{
		EndLt:    1,
		SeqNo:    seqno - 1,
		RootHash: bytes.Repeat([]byte{0x03}, 32),
		FileHash: bytes.Repeat([]byte{0x04}, 32),
	}}
	if workchain != masterchainID {
		header.NotMaster = true
		header.MasterRef = &tlb.ExtBlkRef{
			EndLt:    1,
			SeqNo:    seqno,
			RootHash: bytes.Repeat([]byte{0x05}, 32),
			FileHash: bytes.Repeat([]byte{0x06}, 32),
		}
	}

	root, err := tlb.ToCell(&tlb.Block{
		GlobalID:    -239,
		BlockInfo:   header,
		ValueFlow:   cell.BeginCell().EndCell(),
		StateUpdate: testMerkleUpdateCell(t, cell.BeginCell().EndCell(), stateRoot),
		Extra: &tlb.BlockExtra{
			InMsgDesc:          cell.BeginCell().EndCell(),
			OutMsgDesc:         cell.BeginCell().EndCell(),
			ShardAccountBlocks: cell.BeginCell().EndCell(),
			RandSeed:           bytes.Repeat([]byte{0x01}, 32),
			CreatedBy:          bytes.Repeat([]byte{0x02}, 32),
		},
	})
	if err != nil {
		t.Fatalf("build block for state: %v", err)
	}

	return testBlockIDForRoot(workchain, shard, seqno, root), root
}

func testMerkleUpdateCell(t *testing.T, oldRoot *cell.Cell, newRoot *cell.Cell) *cell.Cell {
	t.Helper()

	update, err := cell.BeginCell().
		MustStoreUInt(uint64(cell.MerkleUpdateCellType), 8).
		MustStoreSlice(oldRoot.Hash(0), 256).
		MustStoreSlice(newRoot.Hash(0), 256).
		MustStoreUInt(uint64(oldRoot.Depth(0)), 16).
		MustStoreUInt(uint64(newRoot.Depth(0)), 16).
		MustStoreRef(oldRoot).
		MustStoreRef(newRoot).
		EndCellSpecial(true)
	if err != nil {
		t.Fatalf("build merkle update: %v", err)
	}
	return update
}

func mustUnwrapProof(t *testing.T, proof []byte, hash []byte) *cell.Cell {
	t.Helper()

	root, err := cell.FromBOC(proof)
	if err != nil {
		t.Fatalf("load proof boc: %v", err)
	}
	body, err := cell.UnwrapProof(root, hash)
	if err != nil {
		t.Fatalf("unwrap proof: %v", err)
	}
	return body
}

func configProofParamCell(t *testing.T, stateRoot *cell.Cell, proof []byte, param int32) *cell.Cell {
	t.Helper()

	body := mustUnwrapProof(t, proof, stateRoot.Hash(0))
	extra, err := mcStateExtra(body)
	if err != nil {
		t.Fatalf("load config proof state extra: %v", err)
	}
	value, err := extra.ConfigParams.Config.Params.LoadValueByIntKey(big.NewInt(int64(param)))
	if err != nil {
		t.Fatalf("load config proof param %d: %v", param, err)
	}
	ref, err := value.LoadRefCell()
	if err != nil {
		t.Fatalf("load config proof param %d ref: %v", param, err)
	}
	return ref
}

func configProofPrevBlockSeqno(t *testing.T, stateRoot *cell.Cell, proof []byte, seqno uint32) uint32 {
	t.Helper()

	body := mustUnwrapProof(t, proof, stateRoot.Hash(0))
	extra, err := mcStateExtra(body)
	if err != nil {
		t.Fatalf("load config proof state extra: %v", err)
	}

	loader := extra.Info.MustBeginParse()
	if _, err = loader.LoadUInt(16); err != nil {
		t.Fatalf("load info flags: %v", err)
	}
	if _, err = loader.LoadUInt(32); err != nil {
		t.Fatalf("load validator hash: %v", err)
	}
	if _, err = loader.LoadUInt(32); err != nil {
		t.Fatalf("load catchain seqno: %v", err)
	}
	if _, err = loader.LoadBoolBit(); err != nil {
		t.Fatalf("load nx_cc_updated: %v", err)
	}

	prevBlocks := &tlb.OldMcBlocksInfoAugDict{}
	if err = prevBlocks.LoadFromCell(loader); err != nil {
		t.Fatalf("load prev blocks: %v", err)
	}
	value, err := prevBlocks.LoadValueByIntKey(new(big.Int).SetUint64(uint64(seqno)))
	if err != nil {
		t.Fatalf("load prev block %d: %v", seqno, err)
	}
	var ref tlb.KeyExtBlkRef
	if err = tlb.LoadFromCell(&ref, value); err != nil {
		t.Fatalf("decode prev block %d: %v", seqno, err)
	}
	return ref.BlkRef.SeqNo
}

func keyBlockConfigProofParamCell(t *testing.T, keyRoot *cell.Cell, proof []byte, param int32) *cell.Cell {
	t.Helper()

	body := mustUnwrapProof(t, proof, keyRoot.Hash(0))
	var block tlb.Block
	if err := tlb.LoadFromCell(&block, body.MustBeginParse()); err != nil {
		t.Fatalf("load key block proof body: %v", err)
	}
	if block.Extra == nil || block.Extra.Custom == nil || block.Extra.Custom.ConfigParams == nil {
		t.Fatal("key block proof is missing config params")
	}
	value, err := block.Extra.Custom.ConfigParams.Config.Params.LoadValueByIntKey(big.NewInt(int64(param)))
	if err != nil {
		t.Fatalf("load key config proof param %d: %v", param, err)
	}
	ref, err := value.LoadRefCell()
	if err != nil {
		t.Fatalf("load key config proof param %d ref: %v", param, err)
	}
	return ref
}

func assertRefType(t *testing.T, root *cell.Cell, idx int, typ cell.Type) {
	t.Helper()

	ref, err := root.PeekRef(idx)
	if err != nil {
		t.Fatalf("peek ref %d: %v", idx, err)
	}
	if ref.GetType() != typ {
		t.Fatalf("ref %d type = %v, want %v", idx, ref.GetType(), typ)
	}
}

func testShardStateWithAccount(t *testing.T, block ton.BlockIDExt, accountID []byte) (*cell.Cell, *cell.Cell) {
	t.Helper()

	accounts, err := cell.NewAugDict(256, testShardAccountsAugmentation{})
	if err != nil {
		t.Fatalf("create accounts dict: %v", err)
	}

	account := cell.BeginCell().MustStoreBoolBit(false).EndCell()
	shardAccount, err := tlb.ToCell(&tlb.ShardAccount{
		Account:       account,
		LastTransHash: bytes.Repeat([]byte{0x44}, 32),
		LastTransLT:   1,
	})
	if err != nil {
		t.Fatalf("build shard account: %v", err)
	}
	if err = accounts.Set(accountKey(accountID), shardAccount); err != nil {
		t.Fatalf("set shard account: %v", err)
	}

	state := tlb.ShardStateUnsplit{
		GlobalID: -239,
		ShardIdent: tlb.ShardIdent{
			PrefixBits:  0,
			WorkchainID: block.Workchain,
			ShardPrefix: 0,
		},
		Seqno:           block.SeqNo,
		OutMsgQueueInfo: cell.BeginCell().EndCell(),
		Stats:           testShardStateStats(t),
	}
	state.Accounts.ShardAccounts = &tlb.ShardAccountsAugDict{AugmentedDictionary: accounts}

	root, err := tlb.ToCell(&state)
	if err != nil {
		t.Fatalf("build shard state: %v", err)
	}
	return root, account
}

func testMasterStateWithActiveAccount(t *testing.T, block ton.BlockIDExt, accountID []byte, code, data *cell.Cell) (*cell.Cell, *cell.Cell) {
	return testMasterStateWithActiveAccountAndLibraries(t, block, accountID, code, data, nil)
}

func testMasterStateWithActiveAccountAndLibraries(t *testing.T, block ton.BlockIDExt, accountID []byte, code, data *cell.Cell, libs []*cell.Cell) (*cell.Cell, *cell.Cell) {
	return testMasterStateWithActiveAccountLibrarySets(t, block, accountID, code, data, libs, nil)
}

func testMasterStateWithActiveAccountStateLibraries(t *testing.T, block ton.BlockIDExt, accountID []byte, code, data *cell.Cell, libs []*cell.Cell) (*cell.Cell, *cell.Cell) {
	return testMasterStateWithActiveAccountLibrarySets(t, block, accountID, code, data, nil, libs)
}

func testMasterStateWithActiveAccountLibrarySets(t *testing.T, block ton.BlockIDExt, accountID []byte, code, data *cell.Cell, globalLibs []*cell.Cell, accountLibs []*cell.Cell) (*cell.Cell, *cell.Cell) {
	t.Helper()

	accounts, err := cell.NewAugDict(256, testShardAccountsAugmentation{})
	if err != nil {
		t.Fatalf("create accounts dict: %v", err)
	}

	account := testActiveAccountCellWithStateLibraries(t, block.Workchain, accountID, code, data, accountLibs)
	shardAccount, err := tlb.ToCell(&tlb.ShardAccount{
		Account:       account,
		LastTransHash: bytes.Repeat([]byte{0x44}, 32),
		LastTransLT:   1,
	})
	if err != nil {
		t.Fatalf("build shard account: %v", err)
	}
	if err = accounts.Set(accountKey(accountID), shardAccount); err != nil {
		t.Fatalf("set shard account: %v", err)
	}

	version, err := tlb.ToCell(&tlb.GlobalVersion{Version: 13})
	if err != nil {
		t.Fatalf("build global version: %v", err)
	}
	params := cell.NewDict(32)
	if err = params.SetIntKey(big.NewInt(int64(tlb.ConfigParamGlobalVersion)), cell.BeginCell().MustStoreRef(version).EndCell()); err != nil {
		t.Fatalf("set global version: %v", err)
	}
	extraCell, err := tlb.ToCell(&tlb.McStateExtra{
		ShardHashes: cell.NewDict(32),
		ConfigParams: tlb.ConfigParams{
			ConfigAddr: bytes.Repeat([]byte{0x99}, 32),
			Config: struct {
				Params *cell.Dictionary `tlb:"dict inline 32"`
			}{Params: params},
		},
		Info:          testMcStateInfo(t, block.SeqNo),
		GlobalBalance: tlb.CurrencyCollection{Coins: tlb.ZeroCoins},
	})
	if err != nil {
		t.Fatalf("build mc state extra: %v", err)
	}

	cc, err := testCurrencyCollectionCell()
	if err != nil {
		t.Fatalf("currency collection: %v", err)
	}
	libDict := cell.NewDict(256)
	for _, lib := range globalLibs {
		descr := cell.BeginCell().MustStoreUInt(0, 2).MustStoreRef(lib).EndCell()
		if err = libDict.Set(accountKey(lib.Hash()), descr); err != nil {
			t.Fatalf("set library: %v", err)
		}
	}
	stats := cell.BeginCell().
		MustStoreUInt(0, 64).
		MustStoreUInt(0, 64).
		MustStoreBuilder(cc.ToBuilder()).
		MustStoreBuilder(cc.ToBuilder()).
		MustStoreDict(libDict).
		MustStoreBoolBit(false).
		EndCell()

	state := tlb.ShardStateUnsplit{
		GlobalID: -239,
		ShardIdent: tlb.ShardIdent{
			PrefixBits:  0,
			WorkchainID: block.Workchain,
			ShardPrefix: 0,
		},
		Seqno:           block.SeqNo,
		GenUTime:        1700000000,
		GenLT:           765,
		OutMsgQueueInfo: cell.BeginCell().EndCell(),
		Stats:           stats,
		McStateExtra:    extraCell,
	}
	state.Accounts.ShardAccounts = &tlb.ShardAccountsAugDict{AugmentedDictionary: accounts}

	root, err := tlb.ToCell(&state)
	if err != nil {
		t.Fatalf("build master state with active account: %v", err)
	}
	return root, account
}

func testActiveAccountCellWithStateLibraries(t *testing.T, workchain int32, accountID []byte, code, data *cell.Cell, libs []*cell.Cell) *cell.Cell {
	t.Helper()

	libDict := cell.NewDict(256)
	for _, lib := range libs {
		descr := cell.BeginCell().MustStoreUInt(0, 2).MustStoreRef(lib).EndCell()
		if err := libDict.Set(accountKey(lib.Hash()), descr); err != nil {
			t.Fatalf("set account library: %v", err)
		}
	}

	account := tlb.AccountState{
		IsValid: true,
		Address: address.NewAddress(0, byte(workchain), append([]byte(nil), accountID...)),
		StorageInfo: tlb.StorageInfo{
			StorageUsed: tlb.StorageUsed{
				CellsUsed: big.NewInt(0),
				BitsUsed:  big.NewInt(0),
			},
			StorageExtra: tlb.StorageExtraNone{},
		},
		AccountStorage: tlb.AccountStorage{
			Status:            tlb.AccountStatusActive,
			LastTransactionLT: 1,
			Balance:           tlb.FromNanoTONU(100),
			StateInit: &tlb.StateInit{
				Code: code,
				Data: data,
				Lib:  libDict,
			},
		},
	}

	cl, err := tlb.ToCell(&account)
	if err != nil {
		t.Fatalf("build active account: %v", err)
	}
	return cl
}

type testShardAccountsAugmentation struct{}

func (testShardAccountsAugmentation) SkipExtra(loader *cell.Slice) error {
	var extra tlb.DepthBalanceInfo
	return tlb.LoadFromCell(&extra, loader)
}

func (testShardAccountsAugmentation) EmptyExtra() (*cell.Cell, error) {
	return testDepthBalanceInfoCell()
}

func (testShardAccountsAugmentation) LeafExtra(*cell.Slice) (*cell.Cell, error) {
	return testDepthBalanceInfoCell()
}

func (testShardAccountsAugmentation) CombineExtra(*cell.Slice, *cell.Slice) (*cell.Cell, error) {
	return testDepthBalanceInfoCell()
}

func testDepthBalanceInfoCell() (*cell.Cell, error) {
	return tlb.ToCell(&tlb.DepthBalanceInfo{
		Currencies: tlb.CurrencyCollection{Coins: tlb.ZeroCoins},
	})
}

type testOldMcBlocksAugmentation struct{}

func (testOldMcBlocksAugmentation) SkipExtra(loader *cell.Slice) error {
	var extra tlb.KeyMaxLt
	return tlb.LoadFromCell(&extra, loader)
}

func (testOldMcBlocksAugmentation) EmptyExtra() (*cell.Cell, error) {
	return tlb.ToCell(&tlb.KeyMaxLt{})
}

func (testOldMcBlocksAugmentation) LeafExtra(value *cell.Slice) (*cell.Cell, error) {
	var ref tlb.KeyExtBlkRef
	if err := tlb.LoadFromCell(&ref, value.Copy()); err != nil {
		return nil, err
	}
	return tlb.ToCell(&tlb.KeyMaxLt{IsKey: ref.IsKey, MaxEndLT: ref.BlkRef.EndLt})
}

func (testOldMcBlocksAugmentation) CombineExtra(leftExtra, rightExtra *cell.Slice) (*cell.Cell, error) {
	var left, right tlb.KeyMaxLt
	if err := tlb.LoadFromCell(&left, leftExtra.Copy()); err != nil {
		return nil, err
	}
	if err := tlb.LoadFromCell(&right, rightExtra.Copy()); err != nil {
		return nil, err
	}
	return tlb.ToCell(&tlb.KeyMaxLt{
		IsKey:    left.IsKey || right.IsKey,
		MaxEndLT: max(left.MaxEndLT, right.MaxEndLT),
	})
}

func testMcStateInfo(t *testing.T, seqno uint32) *cell.Cell {
	t.Helper()

	prevBlocks, err := cell.NewAugDict(32, testOldMcBlocksAugmentation{})
	if err != nil {
		t.Fatalf("create old mc blocks dict: %v", err)
	}
	for prevSeqno := uint32(0); prevSeqno < seqno; prevSeqno++ {
		ref, err := tlb.ToCell(&tlb.KeyExtBlkRef{
			BlkRef: tlb.ExtBlkRef{
				EndLt:    uint64(prevSeqno+1) * 100,
				SeqNo:    prevSeqno,
				RootHash: bytes.Repeat([]byte{byte(prevSeqno)}, 32),
				FileHash: bytes.Repeat([]byte{byte(prevSeqno + 1)}, 32),
			},
		})
		if err != nil {
			t.Fatalf("build old mc block ref: %v", err)
		}
		if err = prevBlocks.SetIntKey(big.NewInt(int64(prevSeqno)), ref); err != nil {
			t.Fatalf("set old mc block ref: %v", err)
		}
	}

	prevBlocksCell, err := prevBlocks.ToCell()
	if err != nil {
		t.Fatalf("serialize old mc blocks: %v", err)
	}

	return cell.BeginCell().
		MustStoreUInt(0, 16).
		MustStoreUInt(0, 32).
		MustStoreUInt(0, 32).
		MustStoreBoolBit(false).
		MustStoreBuilder(prevBlocksCell.ToBuilder()).
		MustStoreBoolBit(true).
		MustStoreBoolBit(false).
		EndCell()
}

type testOldMasterBlock struct {
	id    ton.BlockIDExt
	isKey bool
	endLT uint64
}

func testMasterStateWithOldBlocks(t *testing.T, block ton.BlockIDExt, oldBlocks []testOldMasterBlock) *cell.Cell {
	t.Helper()

	version, err := tlb.ToCell(&tlb.GlobalVersion{Version: 13})
	if err != nil {
		t.Fatalf("build global version: %v", err)
	}
	params := map[int32]*cell.Cell{
		int32(tlb.ConfigParamGlobalVersion): version,
	}

	dict := cell.NewDict(32)
	for k, v := range params {
		if err := dict.SetIntKey(big.NewInt(int64(k)), cell.BeginCell().MustStoreRef(v).EndCell()); err != nil {
			t.Fatalf("set config param: %v", err)
		}
	}

	extraCell, err := tlb.ToCell(&tlb.McStateExtra{
		ShardHashes: cell.NewDict(32),
		ConfigParams: tlb.ConfigParams{
			ConfigAddr: bytes.Repeat([]byte{0x99}, 32),
			Config: struct {
				Params *cell.Dictionary `tlb:"dict inline 32"`
			}{Params: dict},
		},
		Info:          testMcStateInfoWithOldBlocks(t, oldBlocks),
		GlobalBalance: tlb.CurrencyCollection{Coins: tlb.ZeroCoins},
	})
	if err != nil {
		t.Fatalf("build mc state extra: %v", err)
	}

	cc, err := testCurrencyCollectionCell()
	if err != nil {
		t.Fatalf("currency collection: %v", err)
	}
	stats := cell.BeginCell().
		MustStoreUInt(0, 64).
		MustStoreUInt(0, 64).
		MustStoreBuilder(cc.ToBuilder()).
		MustStoreBuilder(cc.ToBuilder()).
		MustStoreDict(cell.NewDict(256)).
		MustStoreBoolBit(false).
		EndCell()

	state := tlb.ShardStateUnsplit{
		GlobalID: -239,
		ShardIdent: tlb.ShardIdent{
			PrefixBits:  0,
			WorkchainID: block.Workchain,
			ShardPrefix: 0,
		},
		Seqno:           block.SeqNo,
		OutMsgQueueInfo: cell.BeginCell().EndCell(),
		Stats:           stats,
		McStateExtra:    extraCell,
	}
	state.Accounts.ShardAccounts = testEmptyShardAccounts(t)
	root, err := tlb.ToCell(&state)
	if err != nil {
		t.Fatalf("build master state with old blocks: %v", err)
	}
	return root
}

func testMcStateInfoWithOldBlocks(t *testing.T, oldBlocks []testOldMasterBlock) *cell.Cell {
	t.Helper()

	prevBlocks, err := cell.NewAugDict(32, testOldMcBlocksAugmentation{})
	if err != nil {
		t.Fatalf("create old mc blocks dict: %v", err)
	}
	hasZero := false
	for _, old := range oldBlocks {
		if old.id.SeqNo == 0 {
			hasZero = true
			break
		}
	}
	if !hasZero {
		oldBlocks = append([]testOldMasterBlock{{
			id: ton.BlockIDExt{
				Workchain: masterchainID,
				Shard:     masterchainShard,
				SeqNo:     0,
				RootHash:  bytes.Repeat([]byte{0x00}, 32),
				FileHash:  bytes.Repeat([]byte{0x01}, 32),
			},
			isKey: true,
		}}, oldBlocks...)
	}
	for _, old := range oldBlocks {
		endLT := old.endLT
		if endLT == 0 {
			endLT = uint64(old.id.SeqNo+1) * 100
		}

		ref, err := tlb.ToCell(&tlb.KeyExtBlkRef{
			IsKey: old.isKey,
			BlkRef: tlb.ExtBlkRef{
				EndLt:    endLT,
				SeqNo:    old.id.SeqNo,
				RootHash: bytes.Clone(old.id.RootHash),
				FileHash: bytes.Clone(old.id.FileHash),
			},
		})
		if err != nil {
			t.Fatalf("build old mc block ref: %v", err)
		}
		if err = prevBlocks.SetIntKey(big.NewInt(int64(old.id.SeqNo)), ref); err != nil {
			t.Fatalf("set old mc block ref: %v", err)
		}
	}

	prevBlocksCell, err := prevBlocks.ToCell()
	if err != nil {
		t.Fatalf("serialize old mc blocks: %v", err)
	}

	return cell.BeginCell().
		MustStoreUInt(0, 16).
		MustStoreUInt(0, 32).
		MustStoreUInt(0, 32).
		MustStoreBoolBit(false).
		MustStoreBuilder(prevBlocksCell.ToBuilder()).
		MustStoreBoolBit(true).
		MustStoreBoolBit(false).
		EndCell()
}

func testCodeFromBuilders(t *testing.T, builders ...*cell.Builder) *cell.Cell {
	t.Helper()

	code := cell.BeginCell()
	for _, builder := range builders {
		code.MustStoreBuilder(builder)
	}
	return code.EndCell()
}

func testLargeCell(t *testing.T, refs int) *cell.Cell {
	t.Helper()

	payload := bytes.Repeat([]byte{0x7a}, 120)
	root := cell.BeginCell().MustStoreSlice(payload, uint(len(payload)*8)).EndCell()
	for i := 0; i < refs; i++ {
		root = cell.BeginCell().
			MustStoreSlice(payload, uint(len(payload)*8)).
			MustStoreRef(root).
			EndCell()
	}
	return root
}

type testCurrencyCollectionAugmentation struct{}

func (testCurrencyCollectionAugmentation) SkipExtra(loader *cell.Slice) error {
	var extra tlb.CurrencyCollection
	return tlb.LoadFromCell(&extra, loader)
}

func (testCurrencyCollectionAugmentation) EmptyExtra() (*cell.Cell, error) {
	return testCurrencyCollectionCell()
}

func (testCurrencyCollectionAugmentation) LeafExtra(*cell.Slice) (*cell.Cell, error) {
	return testCurrencyCollectionCell()
}

func (testCurrencyCollectionAugmentation) CombineExtra(*cell.Slice, *cell.Slice) (*cell.Cell, error) {
	return testCurrencyCollectionCell()
}

func testCurrencyCollectionCell() (*cell.Cell, error) {
	return tlb.ToCell(&tlb.CurrencyCollection{Coins: tlb.ZeroCoins})
}

func testShardStateStats(t *testing.T) *cell.Cell {
	t.Helper()

	cc, err := testCurrencyCollectionCell()
	if err != nil {
		t.Fatalf("build currency collection: %v", err)
	}

	return cell.BeginCell().
		MustStoreUInt(0, 64).
		MustStoreUInt(0, 64).
		MustStoreBuilder(cc.ToBuilder()).
		MustStoreBuilder(cc.ToBuilder()).
		MustStoreDict(cell.NewDict(256)).
		MustStoreBoolBit(false).
		EndCell()
}

func testGlobalVersionCell(t *testing.T, version uint32) *cell.Cell {
	t.Helper()

	value, err := tlb.ToCell(&tlb.GlobalVersion{Version: version})
	if err != nil {
		t.Fatalf("build global version: %v", err)
	}
	return value
}

func testStoragePricesConfigCell(t *testing.T) *cell.Cell {
	t.Helper()

	dict := cell.NewDict(32)
	if err := dict.SetIntKey(big.NewInt(1000), cell.BeginCell().MustStoreUInt(0x1000, 16).EndCell()); err != nil {
		t.Fatalf("set active storage prices: %v", err)
	}
	if err := dict.SetIntKey(big.NewInt(2000), cell.BeginCell().MustStoreUInt(0x2000, 16).EndCell()); err != nil {
		t.Fatalf("set future storage prices: %v", err)
	}
	return dict.AsCell()
}

func testPrecompiledConfigCell(t *testing.T, codeHash []byte, gas uint64) *cell.Cell {
	t.Helper()

	list := cell.NewDict(256)
	smc, err := tlb.ToCell(&tlb.PrecompiledSmc{GasUsage: gas})
	if err != nil {
		t.Fatalf("build precompiled smc: %v", err)
	}
	if err = list.Set(accountKey(codeHash), smc); err != nil {
		t.Fatalf("set precompiled smc: %v", err)
	}

	cfg, err := tlb.ToCell(&tlb.PrecompiledContractsConfig{List: list})
	if err != nil {
		t.Fatalf("build precompiled config: %v", err)
	}
	return cfg
}

func testTupleAt(t *testing.T, value tuple.Tuple, idx int) tuple.Tuple {
	t.Helper()

	item, err := value.Index(idx)
	if err != nil {
		t.Fatalf("tuple index %d: %v", idx, err)
	}
	tupleValue, ok := item.(tuple.Tuple)
	if !ok {
		t.Fatalf("tuple index %d = %#v, want tuple", idx, item)
	}
	return tupleValue
}

func testSliceAt(t *testing.T, value tuple.Tuple, idx int) *cell.Slice {
	t.Helper()

	item, err := value.Index(idx)
	if err != nil {
		t.Fatalf("tuple index %d: %v", idx, err)
	}
	slice, ok := item.(*cell.Slice)
	if !ok {
		t.Fatalf("tuple index %d = %#v, want slice", idx, item)
	}
	return slice
}

func testBlockIDTupleSeqno(t *testing.T, value tuple.Tuple) uint64 {
	t.Helper()

	item, err := value.Index(2)
	if err != nil {
		t.Fatalf("block id seqno: %v", err)
	}
	seqno, ok := item.(*big.Int)
	if !ok {
		t.Fatalf("block id seqno = %#v, want int", item)
	}
	return seqno.Uint64()
}

func testMasterStateWithConfig(t *testing.T, block ton.BlockIDExt, params map[int32]*cell.Cell) *cell.Cell {
	t.Helper()

	if len(params) == 0 {
		version, err := tlb.ToCell(&tlb.GlobalVersion{Version: 13})
		if err != nil {
			t.Fatalf("build global version: %v", err)
		}
		params = map[int32]*cell.Cell{
			int32(tlb.ConfigParamGlobalVersion): version,
		}
	}

	dict := cell.NewDict(32)
	for k, v := range params {
		if err := dict.SetIntKey(big.NewInt(int64(k)), cell.BeginCell().MustStoreRef(v).EndCell()); err != nil {
			t.Fatalf("set config param: %v", err)
		}
	}

	extraCell, err := tlb.ToCell(&tlb.McStateExtra{
		ShardHashes: cell.NewDict(32),
		ConfigParams: tlb.ConfigParams{
			ConfigAddr: bytes.Repeat([]byte{0x99}, 32),
			Config: struct {
				Params *cell.Dictionary `tlb:"dict inline 32"`
			}{Params: dict},
		},
		Info:          testMcStateInfo(t, block.SeqNo),
		GlobalBalance: tlb.CurrencyCollection{Coins: tlb.ZeroCoins},
	})
	if err != nil {
		t.Fatalf("build mc state extra: %v", err)
	}

	cc, err := testCurrencyCollectionCell()
	if err != nil {
		t.Fatalf("currency collection: %v", err)
	}
	stats := cell.BeginCell().
		MustStoreUInt(0, 64).
		MustStoreUInt(0, 64).
		MustStoreBuilder(cc.ToBuilder()).
		MustStoreBuilder(cc.ToBuilder()).
		MustStoreDict(cell.NewDict(256)).
		MustStoreBoolBit(false).
		EndCell()

	state := tlb.ShardStateUnsplit{
		GlobalID: -239,
		ShardIdent: tlb.ShardIdent{
			PrefixBits:  0,
			WorkchainID: block.Workchain,
			ShardPrefix: 0,
		},
		Seqno:           block.SeqNo,
		OutMsgQueueInfo: cell.BeginCell().EndCell(),
		Stats:           stats,
		McStateExtra:    extraCell,
	}
	state.Accounts.ShardAccounts = testEmptyShardAccounts(t)
	root, err := tlb.ToCell(&state)
	if err != nil {
		t.Fatalf("build master state: %v", err)
	}
	return root
}

func testMasterStateWithLibraries(t *testing.T, block ton.BlockIDExt, libs []*cell.Cell) *cell.Cell {
	t.Helper()

	libDict := cell.NewDict(256)
	for _, lib := range libs {
		descr := cell.BeginCell().MustStoreUInt(0, 2).MustStoreRef(lib).MustStoreDict(cell.NewDict(256)).EndCell()
		if err := libDict.Set(accountKey(lib.Hash()), descr); err != nil {
			t.Fatalf("set library: %v", err)
		}
	}

	cc, err := testCurrencyCollectionCell()
	if err != nil {
		t.Fatalf("currency collection: %v", err)
	}
	stats := cell.BeginCell().
		MustStoreUInt(0, 64).
		MustStoreUInt(0, 64).
		MustStoreBuilder(cc.ToBuilder()).
		MustStoreBuilder(cc.ToBuilder()).
		MustStoreDict(libDict).
		MustStoreBoolBit(false).
		EndCell()

	state := tlb.ShardStateUnsplit{
		GlobalID: -239,
		ShardIdent: tlb.ShardIdent{
			PrefixBits:  0,
			WorkchainID: block.Workchain,
			ShardPrefix: 0,
		},
		Seqno:           block.SeqNo,
		OutMsgQueueInfo: cell.BeginCell().EndCell(),
		Stats:           stats,
	}
	state.Accounts.ShardAccounts = testEmptyShardAccounts(t)
	root, err := tlb.ToCell(&state)
	if err != nil {
		t.Fatalf("build library state: %v", err)
	}
	return root
}

func testMasterStateWithShardHashes(t *testing.T, block ton.BlockIDExt, shardHashes *cell.Dictionary) *cell.Cell {
	t.Helper()

	params := cell.NewDict(32)
	if err := params.SetIntKey(big.NewInt(0), cell.BeginCell().MustStoreRef(cell.BeginCell().EndCell()).EndCell()); err != nil {
		t.Fatalf("set dummy config param: %v", err)
	}
	extraCell, err := tlb.ToCell(&tlb.McStateExtra{
		ShardHashes: shardHashes,
		ConfigParams: tlb.ConfigParams{
			ConfigAddr: bytes.Repeat([]byte{0x99}, 32),
			Config: struct {
				Params *cell.Dictionary `tlb:"dict inline 32"`
			}{Params: params},
		},
		Info:          testMcStateInfo(t, block.SeqNo),
		GlobalBalance: tlb.CurrencyCollection{Coins: tlb.ZeroCoins},
	})
	if err != nil {
		t.Fatalf("build mc state extra: %v", err)
	}
	stats := testShardStateStats(t)

	state := tlb.ShardStateUnsplit{
		GlobalID: -239,
		ShardIdent: tlb.ShardIdent{
			PrefixBits:  0,
			WorkchainID: block.Workchain,
			ShardPrefix: 0,
		},
		Seqno:           block.SeqNo,
		OutMsgQueueInfo: cell.BeginCell().EndCell(),
		Stats:           stats,
		McStateExtra:    extraCell,
	}
	state.Accounts.ShardAccounts = testEmptyShardAccounts(t)
	root, err := tlb.ToCell(&state)
	if err != nil {
		t.Fatalf("build master state with shard hashes: %v", err)
	}
	return root
}

func testMasterStateWithBlockCreateStats(t *testing.T, block ton.BlockIDExt, magic uint64) *cell.Cell {
	t.Helper()

	params := cell.NewDict(32)
	if err := params.SetIntKey(big.NewInt(0), cell.BeginCell().MustStoreRef(cell.BeginCell().EndCell()).EndCell()); err != nil {
		t.Fatalf("set dummy config param: %v", err)
	}
	info := cell.BeginCell().
		MustStoreUInt(1, 16).
		MustStoreUInt(0, 32).
		MustStoreUInt(0, 32).
		MustStoreBoolBit(false).
		MustStoreDict(cell.NewDict(32)).
		MustStoreBoolBit(false).
		MustStoreUInt(0, 64).
		MustStoreBoolBit(false).
		MustStoreBoolBit(false).
		MustStoreUInt(magic, 8)
	switch magic {
	case 0x17:
		info.MustStoreDict(cell.NewDict(256))
	case 0x34:
		info.MustStoreDict(cell.NewDict(256)).
			MustStoreUInt(0, 32)
	default:
		t.Fatalf("unsupported block create stats magic %#x", magic)
	}

	extraCell, err := tlb.ToCell(&tlb.McStateExtra{
		ShardHashes: cell.NewDict(32),
		ConfigParams: tlb.ConfigParams{
			ConfigAddr: bytes.Repeat([]byte{0x99}, 32),
			Config: struct {
				Params *cell.Dictionary `tlb:"dict inline 32"`
			}{Params: params},
		},
		Info:          info.EndCell(),
		GlobalBalance: tlb.CurrencyCollection{Coins: tlb.ZeroCoins},
	})
	if err != nil {
		t.Fatalf("build mc state extra with block stats: %v", err)
	}

	state := tlb.ShardStateUnsplit{
		GlobalID: -239,
		ShardIdent: tlb.ShardIdent{
			PrefixBits:  0,
			WorkchainID: block.Workchain,
			ShardPrefix: 0,
		},
		Seqno:           block.SeqNo,
		OutMsgQueueInfo: cell.BeginCell().EndCell(),
		Stats:           testShardStateStats(t),
		McStateExtra:    extraCell,
	}
	state.Accounts.ShardAccounts = testEmptyShardAccounts(t)
	root, err := tlb.ToCell(&state)
	if err != nil {
		t.Fatalf("build master state with block stats: %v", err)
	}
	return root
}

func testShardHashes(t *testing.T, block ton.BlockIDExt) *cell.Dictionary {
	t.Helper()

	return testShardHashesWithNextValidatorShard(t, block, block.Shard)
}

func testShardHashesWithNextValidatorShard(t *testing.T, block ton.BlockIDExt, nextValidatorShard int64) *cell.Dictionary {
	t.Helper()

	leaf := testShardHashLeaf(t, block, nextValidatorShard)

	dict := cell.NewDict(32)
	if err := dict.Set(cell.BeginCell().MustStoreInt(int64(block.Workchain), 32).EndCell(), cell.BeginCell().MustStoreRef(leaf).EndCell()); err != nil {
		t.Fatalf("set shard hash: %v", err)
	}
	return dict
}

func testForkedShardHashes(t *testing.T, left ton.BlockIDExt, right ton.BlockIDExt) *cell.Dictionary {
	t.Helper()

	if left.Workchain != right.Workchain {
		t.Fatalf("forked shard hashes require one workchain, got %d and %d", left.Workchain, right.Workchain)
	}
	root := cell.BeginCell().
		MustStoreUInt(1, 1).
		MustStoreRef(testShardHashLeaf(t, left, left.Shard)).
		MustStoreRef(testShardHashLeaf(t, right, right.Shard)).
		EndCell()

	dict := cell.NewDict(32)
	if err := dict.Set(cell.BeginCell().MustStoreInt(int64(left.Workchain), 32).EndCell(), cell.BeginCell().MustStoreRef(root).EndCell()); err != nil {
		t.Fatalf("set forked shard hash: %v", err)
	}
	return dict
}

func testShardHashLeaf(t *testing.T, block ton.BlockIDExt, nextValidatorShard int64) *cell.Cell {
	t.Helper()

	desc, err := tlb.ToCell(&tlb.ShardDesc{
		SeqNo:              block.SeqNo,
		RootHash:           block.RootHash,
		FileHash:           block.FileHash,
		NextValidatorShard: nextValidatorShard,
		SplitMergeAt:       tlb.FutureSplitMergeNone{},
	})
	if err != nil {
		t.Fatalf("build shard descriptor: %v", err)
	}
	return cell.BeginCell().MustStoreUInt(0, 1).MustStoreBuilder(desc.ToBuilder()).EndCell()
}

func testShardStateWithOutMsgQueueSize(t *testing.T, block ton.BlockIDExt, size uint64) *cell.Cell {
	t.Helper()

	outMsgQueueInfo := cell.BeginCell().
		MustStoreDict(cell.NewDict(352)).
		MustStoreUInt(0, 64).
		MustStoreDict(cell.NewDict(96)).
		MustStoreBoolBit(true).
		MustStoreUInt(0, 4).
		MustStoreDict(cell.NewDict(256)).
		MustStoreUInt(0, 64).
		MustStoreBoolBit(true).
		MustStoreUInt(size, 48).
		EndCell()

	state := tlb.ShardStateUnsplit{
		GlobalID: -239,
		ShardIdent: tlb.ShardIdent{
			PrefixBits:  0,
			WorkchainID: block.Workchain,
			ShardPrefix: 0,
		},
		Seqno:           block.SeqNo,
		OutMsgQueueInfo: outMsgQueueInfo,
		Stats:           testShardStateStats(t),
	}
	state.Accounts.ShardAccounts = testEmptyShardAccounts(t)
	root, err := tlb.ToCell(&state)
	if err != nil {
		t.Fatalf("build queue state: %v", err)
	}
	return root
}

func testBlockWithTransactions(t *testing.T, workchain int32, shard int64, account []byte, txs map[uint64]*cell.Cell) *cell.Cell {
	return testBlockWithTransactionsAndInMsgDesc(t, workchain, shard, account, txs, cell.BeginCell().EndCell())
}

func testBlockWithTransactionsAndInMsgDesc(t *testing.T, workchain int32, shard int64, account []byte, txs map[uint64]*cell.Cell, inMsgDesc *cell.Cell) *cell.Cell {
	t.Helper()

	txDict, err := cell.NewAugDict(64, testCurrencyCollectionAugmentation{})
	if err != nil {
		t.Fatalf("create tx dict: %v", err)
	}
	for lt, tx := range txs {
		if err = txDict.Set(cell.BeginCell().MustStoreUInt(lt, 64).EndCell(), cell.BeginCell().MustStoreRef(tx).EndCell()); err != nil {
			t.Fatalf("set tx: %v", err)
		}
	}

	accountBlock := cell.BeginCell().
		MustStoreUInt(0x5, 4).
		MustStoreSlice(account, 256).
		MustStoreBuilder(testAugDictRootCell(t, txDict).ToBuilder()).
		MustStoreRef(cell.BeginCell().EndCell()).
		EndCell()

	accountBlocks, err := cell.NewAugDict(256, testCurrencyCollectionAugmentation{})
	if err != nil {
		t.Fatalf("create account blocks dict: %v", err)
	}
	if err = accountBlocks.Set(accountKey(account), accountBlock); err != nil {
		t.Fatalf("set account block: %v", err)
	}

	shardBlocks, err := tlb.ToCell(&tlb.ShardAccountBlocks{
		Accounts: &tlb.ShardAccountBlocksAugDict{AugmentedDictionary: accountBlocks},
	})
	if err != nil {
		t.Fatalf("build shard account blocks: %v", err)
	}

	extra := &tlb.BlockExtra{
		InMsgDesc:          inMsgDesc,
		OutMsgDesc:         cell.BeginCell().EndCell(),
		ShardAccountBlocks: shardBlocks,
		RandSeed:           bytes.Repeat([]byte{0x01}, 32),
		CreatedBy:          bytes.Repeat([]byte{0x02}, 32),
	}

	var header tlb.BlockHeader
	header.Version = 1
	header.Shard = tlb.ShardIdent{PrefixBits: 0, WorkchainID: workchain, ShardPrefix: uint64(shard)}
	header.SeqNo = 1
	header.StartLt = 1
	header.EndLt = 100
	header.GenUtime = 1000
	header.NotMaster = workchain != masterchainID
	if header.NotMaster {
		header.MasterRef = &tlb.ExtBlkRef{
			RootHash: bytes.Repeat([]byte{0x05}, 32),
			FileHash: bytes.Repeat([]byte{0x06}, 32),
		}
	}
	header.PrevRef = tlb.BlkPrevInfo{Prev1: tlb.ExtBlkRef{
		RootHash: bytes.Repeat([]byte{0x03}, 32),
		FileHash: bytes.Repeat([]byte{0x04}, 32),
	}}

	root, err := tlb.ToCell(&tlb.Block{
		GlobalID:    -239,
		BlockInfo:   header,
		ValueFlow:   cell.BeginCell().EndCell(),
		StateUpdate: cell.BeginCell().EndCell(),
		Extra:       extra,
	})
	if err != nil {
		t.Fatalf("build block: %v", err)
	}
	return root
}

func testTransactionWithInMsg(t *testing.T, account []byte, lt uint64) (*cell.Cell, *cell.Cell) {
	t.Helper()

	src := address.NewAddress(0, 0, bytes.Repeat([]byte{0x10}, 32))
	dst := address.NewAddress(0, 0, account)
	msg := &tlb.Message{Msg: &tlb.InternalMessage{
		IHRDisabled: true,
		Bounce:      true,
		SrcAddr:     src,
		DstAddr:     dst,
		Amount:      tlb.ZeroCoins,
		Body:        cell.BeginCell().EndCell(),
	}}
	txCell, err := tlb.ToCell(&tlb.Transaction{
		AccountAddr: account,
		LT:          lt,
		PrevTxHash:  bytes.Repeat([]byte{0x01}, 32),
		PrevTxLT:    lt - 1,
		Now:         1700000000,
		OrigStatus:  tlb.AccountStatusActive,
		EndStatus:   tlb.AccountStatusActive,
		IO: struct {
			In  *tlb.Message      `tlb:"maybe ^"`
			Out *tlb.MessagesList `tlb:"maybe ^"`
		}{In: msg},
		TotalFees: tlb.CurrencyCollection{Coins: tlb.ZeroCoins},
		StateUpdate: tlb.HashUpdate{
			OldHash: bytes.Repeat([]byte{0x02}, 32),
			NewHash: bytes.Repeat([]byte{0x03}, 32),
		},
		Description: tlb.TransactionDescriptionOrdinary{
			ComputePhase: tlb.ComputePhase{Phase: tlb.ComputePhaseSkipped{Reason: tlb.ComputeSkipReason{Type: tlb.ComputeSkipReasonNoState}}},
			Aborted:      true,
		},
	})
	if err != nil {
		t.Fatalf("build transaction: %v", err)
	}

	msgCell, err := msg.ToCell()
	if err != nil {
		t.Fatalf("build inbound message: %v", err)
	}
	return txCell, msgCell
}

func testTransactionWithPrev(t *testing.T, account []byte, lt uint64, prevLT uint64, prevHash []byte) *cell.Cell {
	t.Helper()

	if len(prevHash) != 32 {
		t.Fatalf("previous hash length = %d, want 32", len(prevHash))
	}

	txCell, err := tlb.ToCell(&tlb.Transaction{
		AccountAddr: bytes.Clone(account),
		LT:          lt,
		PrevTxHash:  bytes.Clone(prevHash),
		PrevTxLT:    prevLT,
		Now:         1700000000,
		OrigStatus:  tlb.AccountStatusActive,
		EndStatus:   tlb.AccountStatusActive,
		IO: struct {
			In  *tlb.Message      `tlb:"maybe ^"`
			Out *tlb.MessagesList `tlb:"maybe ^"`
		}{},
		TotalFees: tlb.CurrencyCollection{Coins: tlb.ZeroCoins},
		StateUpdate: tlb.HashUpdate{
			OldHash: bytes.Repeat([]byte{0x02}, 32),
			NewHash: bytes.Repeat([]byte{0x03}, 32),
		},
		Description: tlb.TransactionDescriptionOrdinary{
			ComputePhase: tlb.ComputePhase{Phase: tlb.ComputePhaseSkipped{Reason: tlb.ComputeSkipReason{Type: tlb.ComputeSkipReasonNoState}}},
			Aborted:      true,
		},
	})
	if err != nil {
		t.Fatalf("build transaction: %v", err)
	}
	return txCell
}

func testMsgEnvelopeWithMetadata(t *testing.T, msg *cell.Cell, initiator []byte, depth int32, lt uint64) *cell.Cell {
	t.Helper()

	return cell.BeginCell().
		MustStoreUInt(5, 4).
		MustStoreUInt(0, 8).
		MustStoreUInt(0, 8).
		MustStoreCoins(0).
		MustStoreRef(msg).
		MustStoreBoolBit(false).
		MustStoreBoolBit(true).
		MustStoreUInt(0, 4).
		MustStoreUInt(uint64(depth), 32).
		MustStoreAddr(address.NewAddress(0, 0, initiator)).
		MustStoreUInt(lt, 64).
		EndCell()
}

func testInMsgDescr(t *testing.T, msg, envelope, tx *cell.Cell) *cell.Cell {
	t.Helper()

	dict, err := cell.NewAugDict(256, testImportFeesAugmentation{})
	if err != nil {
		t.Fatalf("create inbound message descriptor dict: %v", err)
	}
	value := cell.BeginCell().
		MustStoreUInt(0b011, 3).
		MustStoreRef(envelope).
		MustStoreRef(tx).
		MustStoreCoins(0).
		EndCell()
	if err = dict.Set(accountKey(msg.Hash()), value); err != nil {
		t.Fatalf("set inbound message descriptor: %v", err)
	}

	root, err := dict.ToCell()
	if err != nil {
		t.Fatalf("serialize inbound message descriptor: %v", err)
	}
	return root
}

func testMasterBlockWithPrevKey(t *testing.T, seqno uint32, prevKey uint32) (ton.BlockIDExt, *cell.Cell) {
	t.Helper()

	var header tlb.BlockHeader
	header.Version = 1
	header.Shard = tlb.ShardIdent{PrefixBits: 0, WorkchainID: masterchainID, ShardPrefix: uint64(1) << 63}
	header.SeqNo = seqno
	header.StartLt = 1
	header.EndLt = 100
	header.GenUtime = 1000
	header.PrevKeyBlockSeqno = prevKey
	header.PrevRef = tlb.BlkPrevInfo{Prev1: tlb.ExtBlkRef{
		SeqNo:    seqno - 1,
		RootHash: bytes.Repeat([]byte{0x03}, 32),
		FileHash: bytes.Repeat([]byte{0x04}, 32),
	}}

	root := testBlockRootWithHeader(t, header, nil)
	return testBlockIDForRoot(masterchainID, masterchainShard, seqno, root), root
}

func testMasterBlockWithShardHashes(t *testing.T, seqno uint32, shardHashes *cell.Dictionary) (ton.BlockIDExt, *cell.Cell) {
	t.Helper()

	var header tlb.BlockHeader
	header.Version = 1
	header.Shard = tlb.ShardIdent{PrefixBits: 0, WorkchainID: masterchainID, ShardPrefix: uint64(1) << 63}
	header.SeqNo = seqno
	header.StartLt = 1
	header.EndLt = 100
	header.GenUtime = 1000
	header.PrevKeyBlockSeqno = 0
	header.PrevRef = tlb.BlkPrevInfo{Prev1: tlb.ExtBlkRef{
		SeqNo:    seqno - 1,
		RootHash: bytes.Repeat([]byte{0x03}, 32),
		FileHash: bytes.Repeat([]byte{0x04}, 32),
	}}

	root := testBlockRootWithHeader(t, header, testMcBlockExtraWithShardHashes(t, shardHashes))
	return testBlockIDForRoot(masterchainID, masterchainShard, seqno, root), root
}

func testMasterBlockForStateWithPrevKey(t *testing.T, seqno uint32, prevKey uint32, keyBlock bool, stateRoot *cell.Cell, custom *cell.Cell) (ton.BlockIDExt, *cell.Cell) {
	t.Helper()

	var header tlb.BlockHeader
	header.Version = 1
	header.KeyBlock = keyBlock
	header.Shard = tlb.ShardIdent{PrefixBits: 0, WorkchainID: masterchainID, ShardPrefix: uint64(1) << 63}
	header.SeqNo = seqno
	header.StartLt = 1
	header.EndLt = 100
	header.GenUtime = 1000
	header.PrevKeyBlockSeqno = prevKey
	header.PrevRef = tlb.BlkPrevInfo{Prev1: tlb.ExtBlkRef{
		SeqNo:    seqno - 1,
		RootHash: bytes.Repeat([]byte{0x03}, 32),
		FileHash: bytes.Repeat([]byte{0x04}, 32),
	}}

	root := testBlockRootWithHeaderAndState(t, header, stateRoot, custom)
	return testBlockIDForRoot(masterchainID, masterchainShard, seqno, root), root
}

func testKeyBlockWithConfig(t *testing.T, seqno uint32, params map[int32]*cell.Cell) (ton.BlockIDExt, *cell.Cell) {
	t.Helper()

	var header tlb.BlockHeader
	header.Version = 1
	header.KeyBlock = true
	header.Shard = tlb.ShardIdent{PrefixBits: 0, WorkchainID: masterchainID, ShardPrefix: uint64(1) << 63}
	header.SeqNo = seqno
	header.StartLt = 1
	header.EndLt = 100
	header.GenUtime = 1000
	header.PrevKeyBlockSeqno = seqno
	header.PrevRef = tlb.BlkPrevInfo{Prev1: tlb.ExtBlkRef{
		SeqNo:    seqno - 1,
		RootHash: bytes.Repeat([]byte{0x03}, 32),
		FileHash: bytes.Repeat([]byte{0x04}, 32),
	}}

	root := testBlockRootWithHeader(t, header, testMcBlockExtraWithConfig(t, params))
	return testBlockIDForRoot(masterchainID, masterchainShard, seqno, root), root
}

func testBlockRootWithHeaderAndState(t *testing.T, header tlb.BlockHeader, stateRoot *cell.Cell, custom *cell.Cell) *cell.Cell {
	t.Helper()

	headerCell, err := tlb.ToCell(header)
	if err != nil {
		t.Fatalf("build block header: %v", err)
	}
	return cell.BeginCell().
		MustStoreUInt(0x11ef55aa, 32).
		MustStoreInt(-239, 32).
		MustStoreRef(headerCell).
		MustStoreRef(cell.BeginCell().EndCell()).
		MustStoreRef(testMerkleUpdateCell(t, cell.BeginCell().EndCell(), stateRoot)).
		MustStoreRef(testBlockExtraCell(t, custom)).
		EndCell()
}

func testBlockRootWithHeader(t *testing.T, header tlb.BlockHeader, custom *cell.Cell) *cell.Cell {
	t.Helper()

	headerCell, err := tlb.ToCell(header)
	if err != nil {
		t.Fatalf("build block header: %v", err)
	}
	return cell.BeginCell().
		MustStoreUInt(0x11ef55aa, 32).
		MustStoreInt(-239, 32).
		MustStoreRef(headerCell).
		MustStoreRef(cell.BeginCell().EndCell()).
		MustStoreRef(cell.BeginCell().EndCell()).
		MustStoreRef(testBlockExtraCell(t, custom)).
		EndCell()
}

func testBlockExtraCell(t *testing.T, custom *cell.Cell) *cell.Cell {
	t.Helper()

	emptyAccounts, err := tlb.ToCell(&tlb.ShardAccountBlocks{Accounts: testEmptyShardAccountBlocks(t)})
	if err != nil {
		t.Fatalf("build empty shard account blocks: %v", err)
	}
	return cell.BeginCell().
		MustStoreUInt(0x4a33f6fd, 32).
		MustStoreRef(cell.BeginCell().EndCell()).
		MustStoreRef(cell.BeginCell().EndCell()).
		MustStoreRef(emptyAccounts).
		MustStoreSlice(bytes.Repeat([]byte{0x01}, 32), 256).
		MustStoreSlice(bytes.Repeat([]byte{0x02}, 32), 256).
		MustStoreMaybeRef(custom).
		EndCell()
}

func testMcBlockExtraWithConfig(t *testing.T, params map[int32]*cell.Cell) *cell.Cell {
	t.Helper()

	dict := cell.NewDict(32)
	for k, v := range params {
		if err := dict.SetIntKey(big.NewInt(int64(k)), cell.BeginCell().MustStoreRef(v).EndCell()); err != nil {
			t.Fatalf("set key block config param: %v", err)
		}
	}
	cc, err := testCurrencyCollectionCell()
	if err != nil {
		t.Fatalf("currency collection: %v", err)
	}
	details := cell.BeginCell().
		MustStoreDict(cell.NewDict(16)).
		MustStoreBoolBit(false).
		MustStoreBoolBit(false).
		EndCell()
	config := dict.AsCell()

	return cell.BeginCell().
		MustStoreUInt(0xcca5, 16).
		MustStoreBoolBit(true).
		MustStoreDict(cell.NewDict(32)).
		MustStoreBoolBit(false).
		MustStoreBuilder(cc.ToBuilder()).
		MustStoreBuilder(cc.ToBuilder()).
		MustStoreRef(details).
		MustStoreSlice(bytes.Repeat([]byte{0x99}, 32), 256).
		MustStoreRef(config).
		EndCell()
}

func testMcBlockExtraWithShardHashes(t *testing.T, shardHashes *cell.Dictionary) *cell.Cell {
	t.Helper()

	cc, err := testCurrencyCollectionCell()
	if err != nil {
		t.Fatalf("currency collection: %v", err)
	}
	details := cell.BeginCell().
		MustStoreDict(cell.NewDict(16)).
		MustStoreBoolBit(false).
		MustStoreBoolBit(false).
		EndCell()

	return cell.BeginCell().
		MustStoreUInt(0xcca5, 16).
		MustStoreBoolBit(false).
		MustStoreDict(shardHashes).
		MustStoreBoolBit(false).
		MustStoreBuilder(cc.ToBuilder()).
		MustStoreBuilder(cc.ToBuilder()).
		MustStoreRef(details).
		EndCell()
}

func testEmptyShardAccountBlocks(t *testing.T) *tlb.ShardAccountBlocksAugDict {
	t.Helper()

	accounts, err := cell.NewAugDict(256, testCurrencyCollectionAugmentation{})
	if err != nil {
		t.Fatalf("create empty shard account blocks: %v", err)
	}
	return &tlb.ShardAccountBlocksAugDict{AugmentedDictionary: accounts}
}

func testEmptyShardAccounts(t *testing.T) *tlb.ShardAccountsAugDict {
	t.Helper()

	accounts, err := cell.NewAugDict(256, testShardAccountsAugmentation{})
	if err != nil {
		t.Fatalf("create empty shard accounts: %v", err)
	}
	return &tlb.ShardAccountsAugDict{AugmentedDictionary: accounts}
}

func testAugDictRootCell(t *testing.T, dict *cell.AugmentedDictionary) *cell.Cell {
	t.Helper()

	wrapped, err := dict.ToCell()
	if err != nil {
		t.Fatalf("serialize augmented dict: %v", err)
	}
	loader := wrapped.MustBeginParse()
	hasRoot, err := loader.LoadBoolBit()
	if err != nil {
		t.Fatalf("load augmented dict wrapper: %v", err)
	}
	if !hasRoot {
		t.Fatal("augmented dict wrapper has no root")
	}
	root, err := loader.LoadRefCell()
	if err != nil {
		t.Fatalf("load augmented dict root: %v", err)
	}
	return root
}

type fakeStore struct {
	current     *storage.CurrentState
	metas       map[storage.BlockRootHash]*storage.BlockMeta
	blocks      map[storage.BlockRootHash][]byte
	proofs      map[fakeProofKey][]byte
	blockStates map[storage.BlockRootHash]*storage.BlockState
	stateRoots  map[string]*cell.Cell
	zeroStates  map[storage.BlockRootHash][]byte
	ltLookup    map[fakeLTLookupKey]ton.BlockIDExt

	seqLookupByKey       map[fakeSeqLookupKey]ton.BlockIDExt
	blockDataCalls       int
	seqLookupCalls       int
	ltLookupCalls        int
	accountLTLookupCalls int
	utimeLookupCalls     int
	currentCalls         int
	blockStateCalls      int
	blockMetaCalls       int
	loadStateCalls       int
	proofCalls           []storage.ServedProofKind
}

type blockingBlockDataStore struct {
	fakeStore

	mu      sync.Mutex
	calls   int
	once    sync.Once
	started chan struct{}
	release chan struct{}
}

type fakeSeqLookupKey struct {
	key   storage.BlockHistoryKey
	seqno uint32
}

func fakeSeqKey(key storage.BlockHistoryKey, seqno uint32) fakeSeqLookupKey {
	return fakeSeqLookupKey{key: key, seqno: seqno}
}

func (s *blockingBlockDataStore) BlockData(ctx context.Context, block ton.BlockIDExt) ([]byte, error) {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	s.once.Do(func() {
		close(s.started)
	})

	select {
	case <-s.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return s.fakeStore.BlockData(ctx, block)
}

func (s *blockingBlockDataStore) blockDataCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

type fakeLTLookupKey struct {
	workchain int32
	shard     int64
	lt        uint64
}

func fakeLTKey(key storage.BlockHistoryKey, lt uint64) fakeLTLookupKey {
	return fakeLTLookupKey{workchain: key.Workchain, shard: key.Shard, lt: lt}
}

type fakeMessageSender struct {
	body    []byte
	address extmsg.AddressKey
	err     error
	count   int
}

func (s *fakeMessageSender) SendExternalMessage(_ context.Context, body []byte, address extmsg.AddressKey) error {
	s.body = append([]byte(nil), body...)
	s.address = address
	s.count++
	return s.err
}

func (s *fakeStore) ZeroState(_ context.Context, block ton.BlockIDExt) ([]byte, error) {
	data, ok := s.zeroStates[storage.BlockKey(block)]
	if !ok {
		return nil, storage.ErrNotFound
	}
	return append([]byte(nil), data...), nil
}

type testImportFeesAugmentation struct{}

func (testImportFeesAugmentation) SkipExtra(loader *cell.Slice) error {
	return skipImportFees(loader)
}

func (testImportFeesAugmentation) EmptyExtra() (*cell.Cell, error) {
	return testImportFeesCell()
}

func (testImportFeesAugmentation) LeafExtra(*cell.Slice) (*cell.Cell, error) {
	return testImportFeesCell()
}

func (testImportFeesAugmentation) CombineExtra(*cell.Slice, *cell.Slice) (*cell.Cell, error) {
	return testImportFeesCell()
}

func testImportFeesCell() (*cell.Cell, error) {
	cc, err := tlb.ToCell(tlb.CurrencyCollection{Coins: tlb.ZeroCoins})
	if err != nil {
		return nil, err
	}
	return cell.BeginCell().MustStoreCoins(0).MustStoreBuilder(cc.ToBuilder()).EndCell(), nil
}

func (s *fakeStore) BlockData(_ context.Context, block ton.BlockIDExt) ([]byte, error) {
	s.blockDataCalls++
	data, ok := s.blocks[storage.BlockKey(block)]
	if !ok {
		return nil, storage.ErrNotFound
	}
	return append([]byte(nil), data...), nil
}

func (s *fakeStore) BlockProof(_ context.Context, kind storage.ServedProofKind, block ton.BlockIDExt) ([]byte, error) {
	s.proofCalls = append(s.proofCalls, kind)
	data, ok := s.proofs[fakeProofKeyForBlock(kind, block)]
	if !ok {
		return nil, storage.ErrNotFound
	}
	return append([]byte(nil), data...), nil
}

type fakeProofKey struct {
	kind  storage.ServedProofKind
	block storage.BlockRootHash
}

func fakeProofKeyForBlock(kind storage.ServedProofKind, block ton.BlockIDExt) fakeProofKey {
	return fakeProofKey{kind: kind, block: storage.BlockKey(block)}
}

type testBlockIDExtTLB struct {
	ShardID  tlb.ShardIdent `tlb:"."`
	SeqNo    uint32         `tlb:"## 32"`
	RootHash []byte         `tlb:"bits 256"`
	FileHash []byte         `tlb:"bits 256"`
}

type testBlockProofEnvelope struct {
	_          tlb.Magic         `tlb:"#c3"`
	ProofFor   testBlockIDExtTLB `tlb:"."`
	Root       *cell.Cell        `tlb:"^"`
	Signatures *cell.Cell        `tlb:"maybe ^"`
}

func testBlockProofStore(t *testing.T, keyID ton.BlockIDExt, keyRoot *cell.Cell, targetID ton.BlockIDExt, targetRoot *cell.Cell, stateRoot *cell.Cell, signatures *cell.Cell) *fakeStore {
	t.Helper()

	return &fakeStore{
		blocks: map[storage.BlockRootHash][]byte{
			storage.BlockKey(keyID):    testBlockBOC(keyRoot),
			storage.BlockKey(targetID): testBlockBOC(targetRoot),
		},
		proofs: map[fakeProofKey][]byte{
			fakeProofKeyForBlock(storage.ServedProofKeyBlock, keyID): testBlockProofEnvelopeBOC(t, keyID, keyRoot, testBlockProofSignatures(t)),
			fakeProofKeyForBlock(storage.ServedProofBlock, targetID): testBlockProofEnvelopeBOC(t, targetID, targetRoot, signatures),
		},
		blockStates: map[storage.BlockRootHash]*storage.BlockState{
			storage.BlockKey(targetID): {Block: targetID, StateRootHash: stateRoot.Hash(0), Cell: stateRoot},
		},
		metas: map[storage.BlockRootHash]*storage.BlockMeta{
			storage.BlockKey(keyID):    {ID: keyID, Flags: storage.BlockMetaIsKeyBlock | storage.BlockMetaHasProofKeyBlock},
			storage.BlockKey(targetID): {ID: targetID, StateRootHash: stateRoot.Hash(0), Flags: storage.BlockMetaHasProofBlock},
		},
	}
}

func testBlockProofEnvelopeBOC(t *testing.T, id ton.BlockIDExt, root *cell.Cell, signatures *cell.Cell) []byte {
	t.Helper()

	proof := testFullMerkleProof(t, root)
	return testBlockProofEnvelopeWithRootBOC(t, id, proof, signatures)
}

func testBlockStateRootProofEnvelopeBOC(t *testing.T, id ton.BlockIDExt, root *cell.Cell, signatures *cell.Cell) []byte {
	t.Helper()

	proof, err := blockStateRootProof(root)
	if err != nil {
		t.Fatalf("create block state root proof: %v", err)
	}
	return testBlockProofEnvelopeWithRootBOC(t, id, proof, signatures)
}

func testBlockProofEnvelopeWithRootBOC(t *testing.T, id ton.BlockIDExt, proof *cell.Cell, signatures *cell.Cell) []byte {
	t.Helper()

	envelope, err := tlb.ToCell(&testBlockProofEnvelope{
		ProofFor: testBlockIDExtTLB{
			ShardID: tlb.ShardIdent{
				PrefixBits:  0,
				WorkchainID: id.Workchain,
				ShardPrefix: uint64(id.Shard),
			},
			SeqNo:    id.SeqNo,
			RootHash: bytes.Clone(id.RootHash),
			FileHash: bytes.Clone(id.FileHash),
		},
		Root:       proof,
		Signatures: signatures,
	})
	if err != nil {
		t.Fatalf("serialize block proof envelope: %v", err)
	}
	return envelope.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false})
}

func assertBOCRootLevelZero(t *testing.T, data []byte) {
	t.Helper()

	root, err := cell.FromBOC(data)
	if err != nil {
		t.Fatalf("parse boc: %v", err)
	}
	if root.Level() != 0 {
		t.Fatalf("boc root level = %d, want 0", root.Level())
	}
}

func testFullMerkleProof(t *testing.T, root *cell.Cell) *cell.Cell {
	t.Helper()

	proofBuilder := cell.NewMerkleProofBuilder(root)
	testMarkFullProofSubtree(t, proofBuilder.Root())
	proof, err := proofBuilder.CreateProof()
	if err != nil {
		t.Fatalf("create block proof: %v", err)
	}
	return proof
}

func testMarkFullProofSubtree(t *testing.T, root *cell.Cell) {
	t.Helper()

	loader, err := root.BeginParse()
	if err != nil {
		t.Fatalf("begin proof subtree: %v", err)
	}
	for i := 0; i < loader.RefsNum(); i++ {
		ref, err := loader.PeekRefCellAt(i)
		if err != nil {
			t.Fatalf("proof subtree ref %d: %v", i, err)
		}
		testMarkFullProofSubtree(t, ref)
	}
}

func testBlockProofSignatures(t *testing.T) *cell.Cell {
	t.Helper()

	return cell.BeginCell().
		MustStoreUInt(0x11, 8).
		MustStoreUInt(0, 32).
		MustStoreUInt(0, 32).
		MustStoreUInt(0, 32).
		MustStoreUInt(0, 64).
		MustStoreDict(cell.NewDict(16)).
		EndCell()
}

func (s *fakeStore) CurrentState(context.Context) (*storage.CurrentState, error) {
	s.currentCalls++
	if s.current == nil {
		return nil, storage.ErrNotFound
	}
	return storage.CloneCurrentState(s.current), nil
}

func (s *fakeStore) CurrentAccountBlocks(_ context.Context, workchain int32, account []byte) (CurrentAccountBlockIDs, error) {
	return currentAccountBlocksFromState(s.current, workchain, account)
}

func (s *fakeStore) CurrentMasterchainInfo(ctx context.Context) (ton.BlockIDExt, []byte, uint32, error) {
	current, err := s.CurrentState(ctx)
	if err != nil {
		return ton.BlockIDExt{}, nil, 0, err
	}
	return currentMasterchainInfo(current)
}

func (s *fakeStore) BlockRoot(ctx context.Context, block ton.BlockIDExt) (*cell.Cell, error) {
	data, err := s.BlockData(ctx, block)
	if err != nil {
		return nil, err
	}
	return parseTrustedBlockBOC(block, data)
}

func (s *fakeStore) BlockFragments(ctx context.Context, block ton.BlockIDExt) (*liveBlockFragments, error) {
	blockRoot, err := s.BlockRoot(ctx, block)
	if err != nil {
		return nil, err
	}
	stateRootHash, err := stateRootHashFromBlock(block, blockRoot)
	if err != nil {
		return nil, err
	}
	stateRoot, err := s.LoadStateCellTree(ctx, block, stateRootHash)
	if err != nil {
		return nil, err
	}
	return buildLiveBlockFragments(block, blockRoot, stateRoot)
}

func (s *fakeStore) WaitMasterchainSeqno(ctx context.Context, seqno uint32, timeout time.Duration) error {
	if timeout > 10*time.Second {
		timeout = 10 * time.Second
	}
	if timeout < 0 {
		timeout = 0
	}

	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		current, err := s.CurrentState(waitCtx)
		if err == nil {
			currentSeqno := currentMasterchainSeqno(current)
			if currentSeqno >= seqno {
				return nil
			}
			if currentSeqno > 0 && seqno > currentSeqno+100 {
				return errWaitMasterchainTooFar
			}
		} else if !errors.Is(err, storage.ErrNotFound) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			return err
		}

		select {
		case <-ticker.C:
		case <-waitCtx.Done():
			if errors.Is(waitCtx.Err(), context.DeadlineExceeded) {
				return errWaitMasterchainTimeout
			}
			return waitCtx.Err()
		}
	}
}

func (s *fakeStore) NonfinalPendingShardBlocks(*storage.ShardKey) ([]ton.BlockIDExt, []ton.BlockIDExt) {
	return nil, nil
}

func (s *fakeStore) LazyCellLoader() cell.LazyCellLoader {
	return func(hash cell.Hash) (*cell.Cell, error) {
		if root := s.stateRoots[string(hash[:])]; root != nil {
			return root, nil
		}
		return nil, storage.ErrNotFound
	}
}

func (s *fakeStore) BlockState(_ context.Context, block ton.BlockIDExt) (*storage.BlockState, error) {
	s.blockStateCalls++
	state, ok := s.blockStates[storage.BlockKey(block)]
	if !ok {
		return nil, storage.ErrNotFound
	}
	return storage.CloneBlockState(state), nil
}

func (s *fakeStore) LoadStateCellTree(_ context.Context, block ton.BlockIDExt, rootHash []byte) (*cell.Cell, error) {
	s.loadStateCalls++
	if root := s.stateRoots[string(rootHash)]; root != nil {
		return root, nil
	}
	state, ok := s.blockStates[storage.BlockKey(block)]
	if ok && state.Cell != nil {
		hash := state.Cell.HashKey(0)
		if len(rootHash) == 0 || bytes.Equal(hash[:], rootHash) {
			return state.Cell, nil
		}
	}
	return nil, storage.ErrNotFound
}

func (s *fakeStore) BlockMeta(_ context.Context, block ton.BlockIDExt) (*storage.BlockMeta, error) {
	s.blockMetaCalls++
	key := storage.BlockKey(block)
	meta, ok := s.metas[key]
	if !ok {
		if state := s.blockStates[key]; state != nil {
			return storage.BuildBlockMetaFromState(*state), nil
		}
		return nil, storage.ErrNotFound
	}
	return meta.Clone(), nil
}

func (s *fakeStore) LookupBlockBySeqNo(_ context.Context, key storage.BlockHistoryKey, seqno uint32) (ton.BlockIDExt, error) {
	s.seqLookupCalls++
	if block, ok := s.seqLookupByKey[fakeSeqKey(key, seqno)]; ok {
		return *cloneBlockID(block), nil
	}
	return ton.BlockIDExt{}, storage.ErrNotFound
}

func (s *fakeStore) LookupBlockByLT(_ context.Context, key storage.BlockHistoryKey, lt uint64) (ton.BlockIDExt, error) {
	s.ltLookupCalls++
	if block, ok := s.ltLookup[fakeLTKey(key, lt)]; ok {
		return *cloneBlockID(block), nil
	}
	return ton.BlockIDExt{}, storage.ErrNotFound
}

func (s *fakeStore) LookupBlockByAccountLT(_ context.Context, workchain int32, account []byte, lt uint64) (ton.BlockIDExt, error) {
	s.accountLTLookupCalls++
	for _, shard := range storage.AccountShardCandidates(workchain, account) {
		key := storage.BlockHistoryKey{Workchain: workchain, Shard: shard}
		if block, ok := s.ltLookup[fakeLTKey(key, lt)]; ok {
			return *cloneBlockID(block), nil
		}
	}
	return ton.BlockIDExt{}, storage.ErrNotFound
}

func (s *fakeStore) LookupBlockByUnixTime(context.Context, storage.BlockHistoryKey, uint32) (ton.BlockIDExt, error) {
	s.utimeLookupCalls++
	return ton.BlockIDExt{}, storage.ErrNotFound
}
