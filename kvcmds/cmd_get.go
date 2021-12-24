package kvcmds

import (
	"context"
	"errors"
	"fmt"
	"tcli"
	"tcli/client"
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
	cli := client.GetTiKVClient()
	mode := cli.GetClientMode()
	if mode == client.RAW_CLIENT {
		return tcli.ResultNotImplemented
	} else {
		if input.Len() != 2 {
			return tcli.ResultErr(500, errors.New(fmt.Sprintf("usage: %s", c.Help())))
		}
		v, err := cli.Get(ctx, input.Arg(1))
		if err != nil {
			return tcli.ResultErr(500, err)
		} else {
			return tcli.ResultStr(string(v.V))
		}
	}
}
