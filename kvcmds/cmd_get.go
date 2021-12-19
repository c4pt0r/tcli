package kvcmds

import (
	"context"
	"tcli"

	"github.com/c4pt0r/log"
)

type GetCmd struct{}

func (c GetCmd) Name() string    { return "get" }
func (c GetCmd) Alias() []string { return []string{"g"} }
func (c GetCmd) Help() string {
	return `get [key]`
}

func (c GetCmd) Suggest(prefix string) []tcli.CmdSuggest {
	return []tcli.CmdSuggest{}
}

func (c GetCmd) Handler(ctx context.Context, input tcli.CmdInput) tcli.Result {
	log.D("get handler")
	return tcli.ResultOK
}
