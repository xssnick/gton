package gton

import (
	"github.com/xssnick/gton/api/liteserver"
	"github.com/xssnick/gton/service/hooks"
)

var _ hooks.Network = extensionNetwork{}
var _ hooks.Store = (*liteserver.LiveStore)(nil)
