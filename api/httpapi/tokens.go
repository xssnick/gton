package httpapi

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/ton/nft"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	jettonMasterDataType   = "ext.tokens.jettonMasterData"
	jettonWalletDataType   = "ext.tokens.jettonWalletData"
	nftCollectionDataType  = "ext.tokens.nftCollectionData"
	nftItemDataType        = "ext.tokens.nftItemData"
	jettonMasterContract   = "jetton_master"
	jettonWalletContract   = "jetton_wallet"
	nftCollectionContract  = "nft_collection"
	nftItemContract        = "nft_item"
	tokenContentOnchain    = "onchain"
	tokenContentOffchain   = "offchain"
	tokenNotDetectedStatus = 409
)

var errTokenStandardNotFound = errors.New("token standard not found")

type tokenContent struct {
	Type string `json:"type"`
	Data any    `json:"data"`
}

type jettonMasterData struct {
	Type             string       `json:"@type"`
	Address          string       `json:"address"`
	ContractType     string       `json:"contract_type"`
	TotalSupply      string       `json:"total_supply"`
	Mintable         bool         `json:"mintable"`
	AdminAddress     string       `json:"admin_address,omitempty"`
	JettonContent    tokenContent `json:"jetton_content"`
	JettonWalletCode string       `json:"jetton_wallet_code"`
}

type jettonWalletData struct {
	Type             string `json:"@type"`
	Address          string `json:"address"`
	ContractType     string `json:"contract_type"`
	Balance          string `json:"balance"`
	Owner            string `json:"owner"`
	Jetton           string `json:"jetton"`
	MintlessClaimed  *bool  `json:"mintless_is_claimed,omitempty"`
	JettonWalletCode string `json:"jetton_wallet_code"`
}

type nftCollectionData struct {
	Type              string       `json:"@type"`
	Address           string       `json:"address"`
	ContractType      string       `json:"contract_type"`
	NextItemIndex     string       `json:"next_item_index"`
	OwnerAddress      string       `json:"owner_address,omitempty"`
	CollectionContent tokenContent `json:"collection_content"`
}

type nftItemData struct {
	Type              string       `json:"@type"`
	Address           string       `json:"address"`
	ContractType      string       `json:"contract_type"`
	Init              bool         `json:"init"`
	Index             string       `json:"index"`
	CollectionAddress string       `json:"collection_address,omitempty"`
	OwnerAddress      string       `json:"owner_address,omitempty"`
	Content           tokenContent `json:"content"`
}

func (s *Server) handleTokenData(ctx context.Context, params requestParams) (any, *apiError) {
	snapshot, apiErr := s.loadAccountSnapshot(ctx, params)
	if apiErr != nil {
		return nil, apiErr
	}

	// Token kind detection is defined by mutually exclusive token-standard get methods.
	// A non-zero get-method exit means this candidate standard is not implemented.
	if result, err := s.jettonMasterTokenData(snapshot); !errors.Is(err, errTokenStandardNotFound) {
		return result, asAPIError(err)
	}
	if result, err := s.jettonWalletTokenData(snapshot); !errors.Is(err, errTokenStandardNotFound) {
		return result, asAPIError(err)
	}
	if result, err := s.nftCollectionTokenData(snapshot); !errors.Is(err, errTokenStandardNotFound) {
		return result, asAPIError(err)
	}
	if result, err := s.nftItemTokenData(snapshot); !errors.Is(err, errTokenStandardNotFound) {
		return result, asAPIError(err)
	}

	raw := strings.ToUpper(snapshot.addr.StringRaw())
	return nil, &apiError{
		status:  tokenNotDetectedStatus,
		code:    tokenNotDetectedStatus,
		message: fmt.Sprintf("Smart contract %s is not Jetton or NFT", raw),
	}
}

func (s *Server) jettonMasterTokenData(snapshot accountSnapshot) (any, error) {
	res, err := s.tokenGetMethod(snapshot, "get_jetton_data")
	if err != nil {
		return nil, err
	}

	supply, err := res.Int(0)
	if err != nil {
		return nil, internalError("cannot parse jetton total supply: " + err.Error())
	}
	mintable, err := res.Int(1)
	if err != nil {
		return nil, internalError("cannot parse jetton mintable flag: " + err.Error())
	}
	adminSlice, err := res.Slice(2)
	if err != nil {
		return nil, internalError("cannot parse jetton admin address: " + err.Error())
	}
	admin, err := adminSlice.LoadAddr()
	if err != nil {
		return nil, internalError("cannot load jetton admin address: " + err.Error())
	}
	contentCell, err := res.Cell(3)
	if err != nil {
		return nil, internalError("cannot parse jetton content: " + err.Error())
	}
	content, apiErr := tokenContentFromCell(contentCell)
	if apiErr != nil {
		return nil, apiErr
	}
	walletCode, err := res.Cell(4)
	if err != nil {
		return nil, internalError("cannot parse jetton wallet code: " + err.Error())
	}

	return jettonMasterData{
		Type:             jettonMasterDataType,
		Address:          snapshot.addr.String(),
		ContractType:     jettonMasterContract,
		TotalSupply:      supply.String(),
		Mintable:         mintable.Sign() != 0,
		AdminAddress:     tokenAddressString(admin),
		JettonContent:    content,
		JettonWalletCode: bocBase64(walletCode),
	}, nil
}

func (s *Server) jettonWalletTokenData(snapshot accountSnapshot) (any, error) {
	res, err := s.tokenGetMethod(snapshot, "get_wallet_data")
	if err != nil {
		return nil, err
	}

	balance, err := res.Int(0)
	if err != nil {
		return nil, internalError("cannot parse jetton wallet balance: " + err.Error())
	}
	owner, err := tokenAddressFromResult(res, 1)
	if err != nil {
		return nil, internalError("cannot parse jetton wallet owner: " + err.Error())
	}
	jetton, err := tokenAddressFromResult(res, 2)
	if err != nil {
		return nil, internalError("cannot parse jetton master address: " + err.Error())
	}
	walletCode, err := res.Cell(3)
	if err != nil {
		return nil, internalError("cannot parse jetton wallet code: " + err.Error())
	}

	return jettonWalletData{
		Type:             jettonWalletDataType,
		Address:          snapshot.addr.String(),
		ContractType:     jettonWalletContract,
		Balance:          balance.String(),
		Owner:            tokenAddressString(owner),
		Jetton:           tokenAddressString(jetton),
		JettonWalletCode: bocBase64(walletCode),
	}, nil
}

func (s *Server) nftCollectionTokenData(snapshot accountSnapshot) (any, error) {
	res, err := s.tokenGetMethod(snapshot, "get_collection_data")
	if err != nil {
		return nil, err
	}

	nextIndex, err := res.Int(0)
	if err != nil {
		return nil, internalError("cannot parse nft collection next item index: " + err.Error())
	}
	contentCell, err := res.Cell(1)
	if err != nil {
		return nil, internalError("cannot parse nft collection content: " + err.Error())
	}
	content, apiErr := tokenContentFromCell(contentCell)
	if apiErr != nil {
		return nil, apiErr
	}
	owner, err := tokenAddressFromResult(res, 2)
	if err != nil {
		return nil, internalError("cannot parse nft collection owner: " + err.Error())
	}

	return nftCollectionData{
		Type:              nftCollectionDataType,
		Address:           snapshot.addr.String(),
		ContractType:      nftCollectionContract,
		NextItemIndex:     nextIndex.String(),
		OwnerAddress:      tokenAddressString(owner),
		CollectionContent: content,
	}, nil
}

func (s *Server) nftItemTokenData(snapshot accountSnapshot) (any, error) {
	res, err := s.tokenGetMethod(snapshot, "get_nft_data")
	if err != nil {
		return nil, err
	}

	init, err := res.Int(0)
	if err != nil {
		return nil, internalError("cannot parse nft item init flag: " + err.Error())
	}
	index, err := res.Int(1)
	if err != nil {
		return nil, internalError("cannot parse nft item index: " + err.Error())
	}
	collection, err := tokenAddressFromResult(res, 2)
	if err != nil {
		return nil, internalError("cannot parse nft item collection address: " + err.Error())
	}
	owner, err := tokenOptionalAddressFromResult(res, 3)
	if err != nil {
		return nil, internalError("cannot parse nft item owner address: " + err.Error())
	}

	content := tokenContent{Type: tokenContentOnchain, Data: map[string]string{}}
	if isNil, err := res.IsNil(4); err != nil {
		return nil, internalError("cannot parse nft item content: " + err.Error())
	} else if !isNil {
		contentCell, err := res.Cell(4)
		if err != nil {
			return nil, internalError("cannot parse nft item content: " + err.Error())
		}
		parsedContent, apiErr := tokenContentFromCell(contentCell)
		if apiErr != nil {
			return nil, apiErr
		}
		content = parsedContent
	}

	return nftItemData{
		Type:              nftItemDataType,
		Address:           snapshot.addr.String(),
		ContractType:      nftItemContract,
		Init:              init.Sign() != 0,
		Index:             index.String(),
		CollectionAddress: tokenAddressString(collection),
		OwnerAddress:      tokenAddressString(owner),
		Content:           content,
	}, nil
}

func (s *Server) tokenGetMethod(snapshot accountSnapshot, method string) (*ton.ExecutionResult, error) {
	result, apiErr := s.executeRunMethod(nil, tlb.MethodNameHash(method), snapshot.addr, snapshot.runMethod)
	if apiErr != nil {
		return nil, apiErr
	}
	if result.Result == nil || result.Result.ExitCode != 0 {
		return nil, errTokenStandardNotFound
	}
	return ton.NewExecutionResult(result.Stack), nil
}

func tokenContentFromCell(root *cell.Cell) (tokenContent, *apiError) {
	content, err := nft.ContentFromCell(root)
	if err != nil {
		return tokenContent{}, internalError("cannot parse token content: " + err.Error())
	}
	return tokenContentFromNFT(content), nil
}

func tokenContentFromNFT(content nft.ContentAny) tokenContent {
	switch c := content.(type) {
	case *nft.ContentOffchain:
		return tokenContent{Type: tokenContentOffchain, Data: c.URI}
	case *nft.ContentOnchain:
		return tokenContent{Type: tokenContentOnchain, Data: tokenContentAttributes(c)}
	case *nft.ContentSemichain:
		attrs := tokenContentAttributes(&c.ContentOnchain)
		if c.URI != "" {
			attrs["uri"] = c.URI
		}
		return tokenContent{Type: tokenContentOnchain, Data: attrs}
	default:
		return tokenContent{Type: tokenContentOnchain, Data: map[string]string{}}
	}
}

func tokenContentAttributes(content *nft.ContentOnchain) map[string]string {
	attrs := map[string]string{}
	for _, key := range []string{
		"uri",
		"name",
		"description",
		"image",
		"image_data",
		"symbol",
		"decimals",
		"amount_style",
		"render_type",
	} {
		value := content.GetAttribute(key)
		if value != "" {
			attrs[key] = value
		}
	}
	return attrs
}

func tokenAddressFromResult(res *ton.ExecutionResult, index uint) (*address.Address, error) {
	slice, err := res.Slice(index)
	if err != nil {
		return nil, err
	}
	return slice.LoadAddr()
}

func tokenOptionalAddressFromResult(res *ton.ExecutionResult, index uint) (*address.Address, error) {
	isNil, err := res.IsNil(index)
	if err != nil {
		return nil, err
	}
	if isNil {
		return address.NewAddressNone(), nil
	}
	return tokenAddressFromResult(res, index)
}

func tokenAddressString(addr *address.Address) string {
	if addr == nil || addr.Type() == address.NoneAddress {
		return ""
	}
	return addr.String()
}
