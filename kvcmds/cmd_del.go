package kvcmds

import (
	"context"

	"github.com/c4pt0r/tcli/utils"

	"github.com/c4pt0r/tcli/client"
)

type DeleteCmd struct{}

func (c DeleteCmd) Name() string    { return "del" }
func (c DeleteCmd) Alias() []string { return []string{"remove", "delete", "rm"} }
func (c DeleteCmd) Help() string {
	return `delete a single kv pair, usage: del(delete/rm/remove) [key]`
}

func (c DeleteCmd) Handler() func(ctx context.Context) {
	return func(ctx context.Context) {
		utils.OutputWithElapse(func() error {
			ic := utils.ExtractIshellContext(ctx)
			if len(ic.Args) < 1 {
				utils.Print(c.Help())
				return nil
			}
			k, err := utils.GetStringLit(ic.RawArgs[1])
			if err != nil {
				return err
			}
			err = client.GetTiKVClient().Delete(context.TODO(), k)
			if err != nil {
				return err
			}
			return nil
		})
	}
}
