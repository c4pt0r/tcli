package kvcmds

import (
	"context"
	"fmt"

	"github.com/c4pt0r/tcli"
	"github.com/c4pt0r/tcli/client"
	"github.com/c4pt0r/tcli/utils"
	"github.com/magiconair/properties"
)

type DeletePrefixCmd struct{}

var _ tcli.Cmd = &DeletePrefixCmd{}

func (c DeletePrefixCmd) Name() string    { return "delp" }
func (c DeletePrefixCmd) Alias() []string { return []string{"deletep", "removep", "rmp"} }
func (c DeletePrefixCmd) Help() string {
	return `delete kv pairs with specific prefix`
}

func (c DeletePrefixCmd) LongHelp() string {
	s := c.Help()
	s += `
Usage:
	delp <prefix> [options]
Alias:
	deletep, removep, rmp
Options:
	--yes, force yes
	--limit, default: 1000
`
	return s
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

			var yes bool
			if utils.HasForceYes(ctx) {
				yes = true
			} else {
				yes = utils.AskYesNo("Are you sure to delete kv pairs with prefix: %s", string(k)) == 1
			}

			if yes {
				utils.Print("Your call")
				lastKey, cnt, err := client.GetTiKVClient().DeletePrefix(ctx, k, limit)
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
