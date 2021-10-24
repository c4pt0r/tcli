package kvcmds

import (
	"context"
	"fmt"
	"tcli"
	"tcli/client"
	"tcli/utils"

	"github.com/magiconair/properties"
)

type DeletePrefixCmd struct{}

func (c DeletePrefixCmd) Name() string    { return "delp" }
func (c DeletePrefixCmd) Alias() []string { return []string{"deletep", "removep", "rmp"} }
func (c DeletePrefixCmd) Help() string {
	return `delete kv pairs with specific prefix, usage: delp(deletep/rmp) keyPrefix [opts]`
}

func (c DeletePrefixCmd) Handler() func(ctx context.Context) {
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
			opt := properties.NewProperties()
			if len(ic.Args) > 1 {
				err := utils.SetOptByString(ic.Args[1:], opt)
				if err != nil {
					return err
				}
			}
			opt.Set(tcli.DeleteOptWithPrefix, "true")
			limit := opt.GetInt(tcli.DeleteOptLimit, 1000)
			ret := utils.AskYesNo(fmt.Sprintf("Delete with prefix: %s, limit %d, are you sure?", string(k), limit), "no")
			if ret == 1 {
				utils.Print("Your call")
				lastKey, cnt, err := client.GetTikvClient().DeletePrefix(ctx, k, limit)
				if err != nil {
					return err
				}
				result := []client.KV{
					{K: []byte("Last Key"), V: []byte(lastKey)},
					{K: []byte("Affected Keys"), V: []byte(fmt.Sprintf("%d", cnt))},
				}
				client.KVS(result).Print()
			} else {
				utils.Print("Nothing happened")
			}
			if err != nil {
				return err
			}
			return nil
		})
	}
}
