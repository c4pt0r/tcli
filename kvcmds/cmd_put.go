package kvcmds

import (
	"context"
	"errors"
	"fmt"
	"tcli"
	"tcli/client"
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
	cli := client.GetTiKVClient()
	mode := cli.GetClientMode()
	if mode == client.RAW_CLIENT {
		return tcli.ResultNotImplemented
	} else {
		if input.Len() != 3 {
			return tcli.ResultErr(500, errors.New(fmt.Sprintf("usage: %s", c.Help())))
		}
		k, v := input.Arg(1), input.Arg(2)
		err := cli.Put(ctx, client.KV{k, v})
		if err != nil {
			return tcli.ResultErr(500, err)
		} else {
			return tcli.ResultOK
		}
	}
}
