package kvcmds

import (
	"context"
	"fmt"
	"tcli/client"
	"tcli/utils"

	"github.com/abiosoft/ishell"
)

type DeleteCmd struct{}

func (c DeleteCmd) Name() string    { return "del" }
func (c DeleteCmd) Alias() []string { return []string{"remove", "delete", "rm"} }
func (c DeleteCmd) Help() string {
	return `delete a single kv pair, usage: del(delete/rm/remove) [key or keyPrefix] [opts]`
}

func (c DeleteCmd) Handler() func(ctx context.Context) {
	return func(ctx context.Context) {
		utils.OutputWithElapse(func() error {
			ic := ctx.Value("ishell").(*ishell.Context)
			if len(ic.Args) < 1 {
				fmt.Println(c.Help())
				return nil
			}
			k, err := utils.GetStringLit(ic.RawArgs[1])
			if err != nil {
				return err
			}
			err = client.GetTikvClient().Delete(context.TODO(), k)
			if err != nil {
				return err
			}
			return nil
		})
	}
}
