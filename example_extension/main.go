package main

import (
	"github.com/xssnick/gton/cmd/node/node"
	"github.com/xssnick/gton/example_extension/replaychecker"
)

func main() {
	node.Run(replaychecker.New)
}
