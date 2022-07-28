package kvcmds

import (
	"context"
	"fmt"

	"github.com/c4pt0r/tcli/utils"

	"github.com/c4pt0r/tcli/client"
)

type PutCmd struct{}

func (c PutCmd) Name() string    { return "put" }
func (c PutCmd) Alias() []string { return []string{"put", "set"} }
func (c PutCmd) Help() string {
	return `put [key] [value]`
}

func (c PutCmd) Handler() func(ctx context.Context) {
	return func(ctx context.Context) {
		utils.OutputWithElapse(func() error {
			ic := utils.ExtractIshellContext(ctx)
			if len(ic.Args) < 2 {
				fmt.Println(c.Help())
				return nil
			}
			k, err := utils.GetStringLit(ic.RawArgs[1])
			if err != nil {
				return err
			}
			v, err := utils.GetStringLit(ic.RawArgs[2])
			if err != nil {
				return err
			}
			err = client.GetTiKVClient().Put(context.TODO(), client.KV{K: k, V: v})
			if err != nil {
				return err
			}
			return nil
		})
	}
}
