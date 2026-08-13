package gton

import (
	"crypto/ed25519"
	"time"

	"github.com/xssnick/gton/api/liteserver"

	"github.com/xssnick/tonutils-go/ton"
)

// HTTPAPIOptions configures the built-in HTTP API. Runtime dependencies are
// supplied by RunNode.
type HTTPAPIOptions struct {
	ListenAddr     string
	RequestTimeout time.Duration
	ZeroState      ton.ZeroStateIDExt
}

// LiteserverOptions configures the built-in TON liteserver. Runtime
// dependencies are supplied by RunNode.
type LiteserverOptions struct {
	PrivateKey              ed25519.PrivateKey
	ListenAddr              string
	NonFinal                bool
	AllowDuplicateExternals bool
	ZeroState               ton.ZeroStateIDExt
	RequestLimits           liteserver.RequestLimitOptions
	QueryConcurrency        int
}
