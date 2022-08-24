package kvcmds

import (
	"context"

	"github.com/c4pt0r/tcli"
	"github.com/c4pt0r/tcli/utils"

	"github.com/c4pt0r/tcli/client"
)

type GetCmd struct{}

var _ tcli.Cmd = GetCmd{}

func (c GetCmd) Name() string    { return "get" }
func (c GetCmd) Alias() []string { return []string{"g"} }
func (c GetCmd) Help() string {
	return `get [key]`
}

func (c GetCmd) LongHelp() string {
	return c.Help()
}

func (c GetCmd) Handler() func(ctx context.Context) {
	return func(ctx context.Context) {
		utils.OutputWithElapse(func() error {
			ic := utils.ExtractIshellContext(ctx)
			if len(ic.Args) < 1 {
				utils.Print(c.LongHelp())
				return nil
			}
			s := ic.RawArgs[1]
			// it's a hex string literal
			k, err := utils.GetStringLit(s)
			if err != nil {
				return err
			}
			kv, err := client.GetTiKVClient().Get(context.TODO(), client.Key(k))
			if err != nil {
				return err
			}
			kvs := []client.KV{kv}
			client.KVS(kvs).Print()
			return nil
		})
	}
}
