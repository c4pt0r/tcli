package kvcmds

import (
	"context"
	"tcli"

	"github.com/c4pt0r/log"
)

type DeleteAllCmd struct{}

func (c DeleteAllCmd) Name() string    { return "delall" }
func (c DeleteAllCmd) Alias() []string { return []string{"dela", "removeall", "rma"} }
func (c DeleteAllCmd) Help() string {
	return `deleteall`
}

func (c DeleteAllCmd) Suggest(prefix string) []tcli.CmdSuggest {
	return []tcli.CmdSuggest{}
}

func (c DeleteAllCmd) Handler(ctx context.Context, input tcli.CmdInput) tcli.Result {
	log.D("delete handler")
	return tcli.ResultOK
}
