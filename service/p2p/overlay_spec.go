package p2p

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/xssnick/tonutils-go/adnl/keys"
	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/tl"
)

type overlayKind uint8

const (
	overlayKindPublicShard overlayKind = iota
	overlayKindCustomFixed
	overlayKindFastSync
	overlayKindPrivate
)

type overlaySpec struct {
	Name              string
	Kind              overlayKind
	Workchain         int32
	Shard             int64
	FullID            []byte
	ShortID           []byte
	ProtoVersionMajor int32
	ProtoVersionMinor int32
	FixedNodes        []PeerID
	FixedNodeIDs      map[PeerID]struct{}
	QueryAcceptors    []PeerID
	MsgSenders        map[PeerID]int
	BlockSenders      map[PeerID]struct{}
	AuthorizedKeys    map[string]uint32
	SenderShards      []CustomOverlayShard
	SkipPublicMsgSend bool
	UseQUIC           bool
	SendQueries       bool
	AcceptQueries     bool
	Announce          bool
	RandomPeers       bool
	QueryCapabilities bool
	FastSync          *fastSyncOverlaySpec

	PrivateAllowLegacyBroadcasts    bool
	PrivateTwoStep                  bool
	PrivateTwoStepIntermediateIDs   map[PeerID]struct{}
	PrivateUnauthenticatedBroadcast uint32
}

func buildOverlaySpecs(zeroStateFileHash []byte) ([]overlaySpec, error) {
	masterSpec, err := buildOverlaySpec(zeroStateFileHash, -1, topShard, "masterchain")
	if err != nil {
		return nil, fmt.Errorf("build masterchain overlay: %w", err)
	}
	baseSpec, err := buildOverlaySpec(zeroStateFileHash, 0, topShard, "basechain")
	if err != nil {
		return nil, fmt.Errorf("build basechain overlay: %w", err)
	}

	return []overlaySpec{masterSpec, baseSpec}, nil
}

func buildOverlaySpec(zeroStateFileHash []byte, workchain int32, shard int64, name string) (overlaySpec, error) {
	fullID, err := tl.Hash(tonnodeapi.ShardPublicOverlayID{
		Workchain:         workchain,
		Shard:             shard,
		ZeroStateFileHash: zeroStateFileHash,
	})
	if err != nil {
		return overlaySpec{}, err
	}

	shortID, err := tl.Hash(keys.PublicKeyOverlay{Key: fullID})
	if err != nil {
		return overlaySpec{}, err
	}

	protoMajor := int32(shardchainProtoVersionMajor)
	protoMinor := int32(shardchainProtoVersionMinor)
	if workchain == -1 {
		protoMajor = int32(masterchainProtoVersionMajor)
		protoMinor = int32(masterchainProtoVersionMinor)
	}

	return overlaySpec{
		Name:              name,
		Kind:              overlayKindPublicShard,
		Workchain:         workchain,
		Shard:             shard,
		FullID:            fullID,
		ShortID:           shortID,
		ProtoVersionMajor: protoMajor,
		ProtoVersionMinor: protoMinor,
		Announce:          true,
		RandomPeers:       true,
		QueryCapabilities: true,
	}, nil
}

func buildCustomOverlaySpecs(zeroStateFileHash []byte, overlays []CustomOverlayConfig, localID PeerID) ([]overlaySpec, error) {
	if len(overlays) == 0 {
		return nil, nil
	}

	specs := make([]overlaySpec, 0, len(overlays))
	for _, cfg := range overlays {
		spec, localMember, err := buildCustomOverlaySpec(zeroStateFileHash, cfg, localID)
		if err != nil {
			return nil, err
		}
		if !localMember {
			continue
		}
		specs = append(specs, spec)
	}
	return specs, nil
}

func buildCustomOverlaySpec(zeroStateFileHash []byte, cfg CustomOverlayConfig, localID PeerID) (overlaySpec, bool, error) {
	nodes := make([][]byte, 0, len(cfg.Nodes))
	fixedNodes := make([]PeerID, 0, len(cfg.Nodes))
	fixedIDs := make(map[PeerID]struct{}, len(cfg.Nodes))
	queryAcceptorIDs := make(map[PeerID]struct{}, len(cfg.Nodes))
	msgSenders := map[PeerID]int{}
	blockSenders := map[PeerID]struct{}{}
	authorizedKeys := map[string]uint32{}
	acceptQueries := false

	for _, node := range cfg.Nodes {
		if node.ADNLID.IsZero() {
			return overlaySpec{}, false, fmt.Errorf("custom overlay %q has empty adnl_id", cfg.Name)
		}
		id := node.ADNLID.Bytes()
		if _, ok := fixedIDs[node.ADNLID]; !ok {
			nodes = append(nodes, id)
			fixedNodes = append(fixedNodes, node.ADNLID)
			fixedIDs[node.ADNLID] = struct{}{}
		}
		if node.MsgSender {
			msgSenders[node.ADNLID] = node.MsgSenderPriority
			authorizedKeys[string(id)] = maxOverlayPayloadSize
		}
		if node.BlockSender {
			blockSenders[node.ADNLID] = struct{}{}
			authorizedKeys[string(id)] = maxOverlayPayloadSize
		}
		if node.AcceptQueries {
			queryAcceptorIDs[node.ADNLID] = struct{}{}
		}
		if node.ADNLID == localID && node.AcceptQueries {
			acceptQueries = true
		}
	}
	if len(nodes) == 0 {
		return overlaySpec{}, false, fmt.Errorf("custom overlay %q has no nodes", cfg.Name)
	}

	sort.Slice(nodes, func(i, j int) bool {
		return bytes.Compare(nodes[i], nodes[j]) < 0
	})
	queryAcceptors := make([]PeerID, 0, len(queryAcceptorIDs))
	for id := range queryAcceptorIDs {
		queryAcceptors = append(queryAcceptors, id)
	}
	sort.Slice(queryAcceptors, func(i, j int) bool {
		return bytes.Compare(queryAcceptors[i][:], queryAcceptors[j][:]) < 0
	})

	fullID, err := tl.Hash(tonnodeapi.CustomOverlayID{
		ZeroStateFileHash: zeroStateFileHash,
		Name:              cfg.Name,
		Nodes:             nodes,
	})
	if err != nil {
		return overlaySpec{}, false, fmt.Errorf("build custom overlay %q id: %w", cfg.Name, err)
	}

	shortID, err := tl.Hash(keys.PublicKeyOverlay{Key: fullID})
	if err != nil {
		return overlaySpec{}, false, fmt.Errorf("build custom overlay %q short id: %w", cfg.Name, err)
	}

	_, localMember := fixedIDs[localID]
	return overlaySpec{
		Name:              "custom." + cfg.Name,
		Kind:              overlayKindCustomFixed,
		Workchain:         0,
		Shard:             topShard,
		FullID:            fullID,
		ShortID:           shortID,
		ProtoVersionMajor: shardchainProtoVersionMajor,
		ProtoVersionMinor: shardchainProtoVersionMinor,
		FixedNodes:        fixedNodes,
		FixedNodeIDs:      fixedIDs,
		QueryAcceptors:    queryAcceptors,
		MsgSenders:        msgSenders,
		BlockSenders:      blockSenders,
		AuthorizedKeys:    authorizedKeys,
		SenderShards:      append([]CustomOverlayShard(nil), cfg.SenderShards...),
		SkipPublicMsgSend: cfg.SkipPublicMsgSend,
		UseQUIC:           cfg.UseQUIC,
		SendQueries:       cfg.SendQueries,
		AcceptQueries:     acceptQueries,
		Announce:          false,
		RandomPeers:       false,
		QueryCapabilities: false,
	}, localMember, nil
}

func overlaySpecKey(spec overlaySpec) string {
	return string(spec.ShortID)
}

func overlayName(workchain int32, shard int64) string {
	switch {
	case workchain == -1 && shard == topShard:
		return "masterchain"
	case workchain == 0 && shard == topShard:
		return "basechain"
	default:
		return fmt.Sprintf("wc=%d shard=%016x", workchain, uint64(shard))
	}
}
