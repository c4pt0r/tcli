package kvcmds

import (
	"context"
	"fmt"
	"tcli/client"
	"tcli/utils"

	"github.com/abiosoft/ishell"
)

type GetCmd struct{}

func (c GetCmd) Name() string    { return "get" }
func (c GetCmd) Alias() []string { return []string{"g"} }
func (c GetCmd) Help() string {
	return `get [string lit]`
}

func (c GetCmd) Handler() func(ctx context.Context) {
	return func(ctx context.Context) {
		utils.OutputWithElapse(func() error {
			ic := ctx.Value("ishell").(*ishell.Context)
			if len(ic.Args) < 1 {
				fmt.Println(c.Help())
				return nil
			}
			s := ic.RawArgs[1]
			// it's a hex string literal
			k, err := utils.GetStringLit(s)
			if err != nil {
				return err
			}
			kv, err := client.GetTikvClient().Get(context.TODO(), client.Key(k))
			if err != nil {
				return err
			}
			kvs := []client.KV{kv}
			client.KVS(kvs).Print()
			return nil
		})
	}
}
