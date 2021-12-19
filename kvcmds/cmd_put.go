package kvcmds

import (
	"context"
	"tcli"

	"github.com/c4pt0r/log"
)

type PutCmd struct{}

func (c PutCmd) Name() string    { return "put" }
func (c PutCmd) Alias() []string { return []string{"put", "set"} }
func (c PutCmd) Help() string {
	return `put [key] [value]`
}

func (c PutCmd) Suggest(prefix string) []tcli.CmdSuggest {
	return []tcli.CmdSuggest{}
}

func (c PutCmd) Handler(ctx context.Context, input tcli.CmdInput) tcli.Result {
	log.D("put handler")
	return tcli.ResultOK
}
