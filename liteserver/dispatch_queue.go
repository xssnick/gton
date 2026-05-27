package liteserver

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func (s *Server) handleDispatchQueueInfo(ctx context.Context, query ton.GetDispatchQueueInfo) any {
	if !isFullBlockID(query.ID) {
		return ton.LSError{Code: errCodeProtoViolation, Text: "invalid BlockIdExt"}
	}
	if query.MaxAccounts <= 0 {
		return ton.LSError{Code: errCodeProtoViolation, Text: "invalid max_accounts"}
	}
	if query.Mode&2 != 0 && len(query.AfterAddr) != 32 {
		return ton.LSError{Code: errCodeProtoViolation, Text: "invalid after_addr"}
	}

	stateRoot, err := s.loadStateRoot(ctx, *query.ID)
	if err != nil {
		return errorResponse(err, "cannot load state "+storage.FormatBlockRef(*query.ID))
	}

	proofRoot := stateRoot
	var proofBuilder *cell.MerkleProofBuilder
	if query.Mode&1 != 0 {
		proofBuilder = newLiteServerProofBuilder(stateRoot)
		proofRoot = proofBuilder.Root()
	}

	dispatchQueue, err := dispatchQueueFromStateRoot(proofRoot)
	if err != nil {
		return errorResponse(err, "cannot load dispatch queue")
	}

	var after []byte
	allowEq := true
	if query.Mode&2 != 0 {
		after = bytes.Clone(query.AfterAddr)
		allowEq = false
	} else {
		after = make([]byte, 32)
	}

	queues, complete, err := collectDispatchQueueInfo(dispatchQueue, after, allowEq, int(query.MaxAccounts))
	if err != nil {
		return errorResponse(err, "cannot load dispatch queue info")
	}

	resp := ton.DispatchQueueInfo{
		Mode:                  query.Mode,
		ID:                    cloneBlockID(*query.ID),
		AccountDispatchQueues: queues,
		Complete:              complete,
	}
	if proofBuilder != nil {
		proof, err := dispatchQueueProof(ctx, s, *query.ID, proofBuilder)
		if err != nil {
			return errorResponse(err, "cannot create dispatch queue proof")
		}
		resp.Proof = proof
	}
	return resp
}

func (s *Server) handleDispatchQueueMessages(ctx context.Context, query ton.GetDispatchQueueMessages) any {
	if !isFullBlockID(query.ID) {
		return ton.LSError{Code: errCodeProtoViolation, Text: "invalid BlockIdExt"}
	}
	if len(query.Addr) != 32 {
		return ton.LSError{Code: errCodeProtoViolation, Text: "invalid account address"}
	}
	if query.MaxMessages <= 0 {
		return ton.LSError{Code: errCodeProtoViolation, Text: "invalid max_messages"}
	}

	stateRoot, err := s.loadStateRoot(ctx, *query.ID)
	if err != nil {
		return errorResponse(err, "cannot load state "+storage.FormatBlockRef(*query.ID))
	}

	proofRoot := stateRoot
	var proofBuilder *cell.MerkleProofBuilder
	if query.Mode&1 != 0 {
		proofBuilder = newLiteServerProofBuilder(stateRoot)
		proofRoot = proofBuilder.Root()
	}

	dispatchQueue, err := dispatchQueueFromStateRoot(proofRoot)
	if err != nil {
		return errorResponse(err, "cannot load dispatch queue")
	}

	messages, roots, complete, err := collectDispatchQueueMessages(
		dispatchQueue,
		bytes.Clone(query.Addr),
		query.AfterLT,
		int(query.MaxMessages),
		query.Mode&2 != 0,
		query.Mode&4 != 0,
	)
	if err != nil {
		return errorResponse(err, "cannot load dispatch queue messages")
	}

	resp := ton.DispatchQueueMessages{
		Mode:     query.Mode,
		ID:       cloneBlockID(*query.ID),
		Messages: messages,
		Complete: complete,
	}
	if proofBuilder != nil {
		proof, err := dispatchQueueProof(ctx, s, *query.ID, proofBuilder)
		if err != nil {
			return errorResponse(err, "cannot create dispatch queue proof")
		}
		resp.Proof = proof
	}
	if query.Mode&4 != 0 {
		data, err := cell.ToBOCWithOptionsErr(roots, cell.BOCSerializeOptions{})
		if err != nil {
			return errorResponse(err, "cannot serialize dispatch queue messages")
		}
		resp.MessagesBOC = data
	}
	return resp
}

func dispatchQueueProof(ctx context.Context, s *Server, block ton.BlockIDExt, proofBuilder *cell.MerkleProofBuilder) ([]byte, error) {
	dataProof, err := proofBuilder.CreateProof()
	if err != nil {
		return nil, err
	}
	proof, err := s.blockStateProof(ctx, block, dataProof)
	if err != nil {
		return nil, err
	}
	return cell.ToBOCWithFlags(proof, false), nil
}

func dispatchQueueFromStateRoot(stateRoot *cell.Cell) (*cell.AugmentedDictionary, error) {
	loader, err := stateRoot.BeginParse()
	if err != nil {
		return nil, err
	}

	state, err := visitShardStateHeader(loader)
	if err != nil {
		return nil, err
	}
	if state.OutMsgQueueInfo == nil {
		return nil, fmt.Errorf("no out message queue info")
	}

	queueLoader, err := state.OutMsgQueueInfo.BeginParse()
	if err != nil {
		return nil, err
	}
	var info tlb.OutMsgQueueInfo
	if err = tlb.LoadFromCell(&info, queueLoader); err != nil {
		return nil, err
	}
	if info.Extra == nil || info.Extra.DispatchQueue == nil {
		return nil, nil
	}
	return info.Extra.DispatchQueue.AugmentedDictionary, nil
}

func collectDispatchQueueInfo(dispatchQueue *cell.AugmentedDictionary, after []byte, allowEq bool, maxAccounts int) ([]ton.AccountDispatchQueueInfo, bool, error) {
	remaining := maxAccounts
	if remaining > 64 {
		remaining = 64
	}

	result := make([]ton.AccountDispatchQueueInfo, 0, remaining)
	for {
		key, value, err := lookupNextDispatchQueueAccount(dispatchQueue, after, allowEq)
		allowEq = false
		if errors.Is(err, cell.ErrNoSuchKeyInDict) {
			return result, true, nil
		}
		if err != nil {
			return nil, false, fmt.Errorf("lookup dispatch queue account: %w", err)
		}
		if remaining == 0 {
			return result, false, nil
		}
		remaining--

		addr, err := loadDispatchQueueKeyBits(key, 256)
		if err != nil {
			return nil, false, fmt.Errorf("load dispatch queue account address: %w", err)
		}

		accountQueue, err := loadAccountDispatchQueue(value)
		if err != nil {
			return nil, false, fmt.Errorf("load account dispatch queue: %w", err)
		}
		if accountQueue.Count == 0 || accountQueue.Messages == nil || accountQueue.Messages.IsEmpty() {
			return nil, false, fmt.Errorf("invalid empty account dispatch queue")
		}

		minLT, maxLT, err := accountDispatchQueueMinMaxLT(accountQueue)
		if err != nil {
			return nil, false, err
		}

		result = append(result, ton.AccountDispatchQueueInfo{
			Addr:  addr,
			Size:  int64(accountQueue.Count),
			MinLT: minLT,
			MaxLT: maxLT,
		})
		after = addr
	}
}

func collectDispatchQueueMessages(dispatchQueue *cell.AugmentedDictionary, addr []byte, afterLT uint64, maxMessages int, oneAccount bool, withMessagesBOC bool) ([]ton.DispatchQueueMessage, []*cell.Cell, bool, error) {
	remaining := maxMessages
	if withMessagesBOC && remaining > 16 {
		remaining = 16
	} else if remaining > 64 {
		remaining = 64
	}

	origAddr := bytes.Clone(addr)
	currentAddr := bytes.Clone(addr)
	lt := afterLT
	first := true
	messages := make([]ton.DispatchQueueMessage, 0, remaining)
	roots := make([]*cell.Cell, 0, remaining)

	for remaining > 0 {
		key, value, err := lookupNextDispatchQueueAccount(dispatchQueue, currentAddr, first)
		if errors.Is(err, cell.ErrNoSuchKeyInDict) {
			return messages, roots, true, nil
		}
		if err != nil {
			return nil, nil, false, fmt.Errorf("lookup dispatch queue account: %w", err)
		}

		currentAddr, err = loadDispatchQueueKeyBits(key, 256)
		if err != nil {
			return nil, nil, false, fmt.Errorf("load dispatch queue account address: %w", err)
		}
		if oneAccount && !bytes.Equal(currentAddr, origAddr) {
			return messages, roots, true, nil
		}

		accountQueue, err := loadAccountDispatchQueue(value)
		if err != nil {
			return nil, nil, false, fmt.Errorf("load account dispatch queue: %w", err)
		}
		if accountQueue.Count == 0 || accountQueue.Messages == nil || accountQueue.Messages.IsEmpty() {
			return nil, nil, false, fmt.Errorf("invalid empty account dispatch queue")
		}

		for {
			key, value, err := lookupNextAccountDispatchMessage(accountQueue.Messages, lt)
			if errors.Is(err, cell.ErrNoSuchKeyInDict) {
				break
			}
			if err != nil {
				return nil, nil, false, fmt.Errorf("lookup account dispatch message: %w", err)
			}

			lt, err = loadDispatchQueueUint64Key(key)
			if err != nil {
				return nil, nil, false, fmt.Errorf("load account dispatch message lt: %w", err)
			}
			if remaining == 0 {
				break
			}
			remaining--

			message, root, err := loadDispatchQueueMessage(currentAddr, lt, value)
			if err != nil {
				return nil, nil, false, err
			}
			messages = append(messages, message)
			roots = append(roots, root)
		}

		first = false
		lt = 0
	}
	return messages, roots, false, nil
}

func lookupNextDispatchQueueAccount(dispatchQueue *cell.AugmentedDictionary, addr []byte, allowEq bool) (*cell.Cell, *cell.Slice, error) {
	if dispatchQueue == nil || dispatchQueue.IsEmpty() {
		return nil, nil, cell.ErrNoSuchKeyInDict
	}
	return dispatchQueue.LookupNearestKey(cell.BeginCell().MustStoreSlice(addr, 256).EndCell(), true, allowEq, false)
}

func lookupNextAccountDispatchMessage(messages *cell.Dictionary, lt uint64) (*cell.Cell, *cell.Slice, error) {
	if messages == nil || messages.IsEmpty() {
		return nil, nil, cell.ErrNoSuchKeyInDict
	}
	return messages.LookupNearestKey(cell.BeginCell().MustStoreUInt(lt, 64).EndCell(), true, false, false)
}

func loadAccountDispatchQueue(value *cell.Slice) (*tlb.AccountDispatchQueue, error) {
	var queue tlb.AccountDispatchQueue
	if err := tlb.LoadFromCell(&queue, value); err != nil {
		return nil, err
	}
	return &queue, nil
}

func accountDispatchQueueMinMaxLT(queue *tlb.AccountDispatchQueue) (uint64, uint64, error) {
	minKey, _, err := queue.Messages.LoadMinMax(false, false)
	if err != nil {
		return 0, 0, fmt.Errorf("load min dispatch queue lt: %w", err)
	}
	maxKey, _, err := queue.Messages.LoadMinMax(true, false)
	if err != nil {
		return 0, 0, fmt.Errorf("load max dispatch queue lt: %w", err)
	}

	minLT, err := loadDispatchQueueUint64Key(minKey)
	if err != nil {
		return 0, 0, err
	}
	maxLT, err := loadDispatchQueueUint64Key(maxKey)
	if err != nil {
		return 0, 0, err
	}
	return minLT, maxLT, nil
}

func loadDispatchQueueMessage(addr []byte, lt uint64, value *cell.Slice) (ton.DispatchQueueMessage, *cell.Cell, error) {
	var enqueued tlb.EnqueuedMsg
	if err := tlb.LoadFromCell(&enqueued, value); err != nil {
		return ton.DispatchQueueMessage{}, nil, fmt.Errorf("load enqueued message: %w", err)
	}

	loader, err := enqueued.Msg.BeginParse()
	if err != nil {
		return ton.DispatchQueueMessage{}, nil, fmt.Errorf("load message envelope cell: %w", err)
	}
	var envelope tlb.MsgEnvelope
	if err := tlb.LoadFromCell(&envelope, loader); err != nil {
		return ton.DispatchQueueMessage{}, nil, fmt.Errorf("load message envelope: %w", err)
	}
	if envelope.Msg == nil {
		return ton.DispatchQueueMessage{}, nil, fmt.Errorf("message envelope has no message")
	}

	return ton.DispatchQueueMessage{
		Addr:     bytes.Clone(addr),
		LT:       lt,
		Hash:     envelope.Msg.Hash(),
		Metadata: dispatchQueueMetadataFromEnvelope(envelope.Metadata),
	}, envelope.Msg, nil
}

func dispatchQueueMetadataFromEnvelope(metadata *tlb.MsgMetadata) ton.TransactionMetadata {
	if metadata == nil {
		return ton.TransactionMetadata{
			Depth: -1,
			Initiator: ton.AccountID{
				Workchain: workchainInvalid,
				ID:        make([]byte, 32),
			},
			InitiatorLT: ^uint64(0),
		}
	}

	return ton.TransactionMetadata{
		Depth: int32(metadata.Depth),
		Initiator: ton.AccountID{
			Workchain: metadata.Initiator.Workchain(),
			ID:        metadata.Initiator.Data(),
		},
		InitiatorLT: metadata.InitiatorLT,
	}
}

func loadDispatchQueueKeyBits(key *cell.Cell, bits uint) ([]byte, error) {
	loader, err := key.BeginParse()
	if err != nil {
		return nil, err
	}
	return loader.LoadSlice(bits)
}

func loadDispatchQueueUint64Key(key *cell.Cell) (uint64, error) {
	loader, err := key.BeginParse()
	if err != nil {
		return 0, err
	}
	return loader.LoadUInt(64)
}
