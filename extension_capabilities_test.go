package gton

import (
	"github.com/xssnick/gton/service/hooks"
	"github.com/xssnick/gton/service/liveview"
)

var _ hooks.Network = externalMessageNetwork{}
var _ hooks.Store = (*liveview.Store)(nil)
