package main

import (
	"context"
)

type Cmd interface {
	Help() string
	Name() string
	Alias() []string
	Handler() func(ctx context.Context)
}

var RegisteredCmds = []Cmd{
	ConnectCmd{},
	ScanCmd{},
	ListStoresCmd{},
	PutCmd{},
}
