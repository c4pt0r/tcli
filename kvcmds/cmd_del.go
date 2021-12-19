package kvcmds

import (
	"context"
	"tcli"

	"github.com/c4pt0r/log"
)

type DeleteCmd struct{}

func (c DeleteCmd) Name() string    { return "del" }
func (c DeleteCmd) Alias() []string { return []string{"remove", "delete", "rm"} }
func (c DeleteCmd) Help() string {
	return `delete a single kv pair, usage: del(delete/rm/remove) [key]`
}

func (c DeleteCmd) Suggest(prefix string) []tcli.CmdSuggest {
	return []tcli.CmdSuggest{}
}

func (c DeleteCmd) Handler(ctx context.Context, input tcli.CmdInput) tcli.Result {
	log.D("delete handler")
	return tcli.ResultOK
}
