package kvcmds

import (
	"context"
	"tcli"

	"github.com/c4pt0r/log"
)

type CountCmd struct{}

func (c CountCmd) Name() string    { return "count" }
func (c CountCmd) Alias() []string { return []string{"cnt", "count", "count"} }
func (c CountCmd) Help() string {
	return `count [*|key prefix], count all keys or keys with specific prefix`
}

func (c CountCmd) Suggest(prefix string) []tcli.CmdSuggest {
	return []tcli.CmdSuggest{}
}

func (c CountCmd) Handler(ctx context.Context, input tcli.CmdInput) tcli.Result {
	log.D("count handler")
	return tcli.ResultOK
}
