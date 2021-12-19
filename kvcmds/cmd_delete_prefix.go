package kvcmds

import (
	"context"
	"tcli"

	"github.com/c4pt0r/log"
)

type DeletePrefixCmd struct{}

func (c DeletePrefixCmd) Name() string    { return "delp" }
func (c DeletePrefixCmd) Alias() []string { return []string{"deletep", "removep", "rmp"} }
func (c DeletePrefixCmd) Help() string {
	return `delete kv pairs with specific prefix, usage: delp(deletep/rmp) keyPrefix [opts]`
}

func (c DeletePrefixCmd) Suggest(prefix string) []tcli.CmdSuggest {
	return []tcli.CmdSuggest{}
}

func (c DeletePrefixCmd) Handler(ctx context.Context, input tcli.CmdInput) tcli.Result {
	log.D("delete-prefix handler")
	return tcli.ResultOK
}
