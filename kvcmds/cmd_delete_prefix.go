package kvcmds

import (
	"context"
	"fmt"
	"tcli"
	"tcli/client"
	"tcli/utils"

	"github.com/abiosoft/ishell"
	"github.com/magiconair/properties"
)

type DeletePrefix struct{}

func (c DeletePrefix) Name() string    { return "delp" }
func (c DeletePrefix) Alias() []string { return []string{"deletep", "removep", "rmp"} }
func (c DeletePrefix) Help() string {
	return `delete kv pairs with specific prefix, usage: delp(deletep/rmp) keyPrefix [opts]`
}

func (c DeletePrefix) Handler() func(ctx context.Context) {
	return func(ctx context.Context) {
		utils.OutputWithElapse(func() error {
			ic := ctx.Value("ishell").(*ishell.Context)
			if len(ic.Args) < 1 {
				fmt.Println(c.Help())
				return nil
			}
			_, k, err := utils.GetStringLit(ic.RawArgs[1])
			if err != nil {
				return err
			}
			opt := properties.NewProperties()
			if len(ic.Args) > 1 {
				err := utils.SetOptByString(ic.Args[1:], opt)
				if err != nil {
					return err
				}
			}

			opt.Set(tcli.DeleteOptWithPrefix, "true")
			limit := opt.GetInt(tcli.DeleteOptLimit, 1000)
			prefix := k[:len(k)-1]
			ret := utils.AskYesNo(fmt.Sprintf("delete with prefix: %s, limit %d, are you sure?", string(prefix), limit), "no")
			if ret == 1 {
				fmt.Println("Your call")
				client.GetTikvClient().DeletePrefix(ctx, prefix, limit)
			} else {
				fmt.Println("Nothing happened")
			}
			if err != nil {
				return err
			}
			return nil
		})
	}
}
