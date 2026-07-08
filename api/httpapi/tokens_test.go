package httpapi

import (
	"testing"

	"github.com/xssnick/tonutils-go/ton/nft"
)

func TestTokenContentFromOnchainNFTContent(t *testing.T) {
	content := &nft.ContentOnchain{}
	if err := content.SetAttribute("uri", "https://example.test/token.json"); err != nil {
		t.Fatalf("set uri: %v", err)
	}
	if err := content.SetAttribute("decimals", "6"); err != nil {
		t.Fatalf("set decimals: %v", err)
	}

	got := tokenContentFromNFT(content)
	if got.Type != tokenContentOnchain {
		t.Fatalf("type = %q, want %q", got.Type, tokenContentOnchain)
	}

	data, ok := got.Data.(map[string]string)
	if !ok {
		t.Fatalf("data is %T, want map[string]string", got.Data)
	}
	if data["uri"] != "https://example.test/token.json" || data["decimals"] != "6" {
		t.Fatalf("data = %#v", data)
	}
}
