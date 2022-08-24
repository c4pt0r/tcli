package kvcmds

import (
	"context"
	"fmt"
	"strconv"

	"github.com/c4pt0r/log"
	"github.com/c4pt0r/tcli"
	"github.com/c4pt0r/tcli/client"
	"github.com/c4pt0r/tcli/utils"
)

type DeleteAllCmd struct{}

var _ tcli.Cmd = DeleteAllCmd{}

func (c DeleteAllCmd) Name() string    { return "delall" }
func (c DeleteAllCmd) Alias() []string { return []string{"dela", "removeall", "rma"} }
func (c DeleteAllCmd) Help() string {
	return `remove all key-value pairs, DANGEROUS`
}

func (c DeleteAllCmd) LongHelp() string {
	s := c.Help()
	s += `
Usage:
	delall
Options:
	--yes, force yes
Alias:
	dela, removeall, rma
`
	return s
}

func (c DeleteAllCmd) Handler() func(ctx context.Context) {
	return func(ctx context.Context) {
		utils.OutputWithElapse(func() error {
			var yes bool
			if utils.HasForceYes(ctx) {
				yes = true
			} else {
				yes = utils.AskYesNo(fmt.Sprintf("Delete all keys, are you sure?"), "no") == 1
			}
			if yes {
				utils.Print("Your call")
				var total int
				// TODO limit should not be fixed
				for {
					key, cnt, err := client.GetTiKVClient().DeletePrefix(ctx, []byte(""), 1000)
					if err != nil {
						return err
					}
					if cnt == 0 {
						break
					}
					total += cnt
					log.I(fmt.Sprintf("Deleting a batch... Position: %s Count: %d, Total: %d", key, cnt, total))
				}
				result := []client.KV{
					{K: []byte("Affected Keys"), V: []byte(strconv.Itoa(total))},
				}
				client.KVS(result).Print()
			}
			return nil
		})
	}
}
