package kvcmds

import (
	"context"
	"fmt"
	"tcli/client"
	"tcli/utils"

	"github.com/c4pt0r/log"
)

type DeleteAllCmd struct{}

func (c DeleteAllCmd) Name() string    { return "delall" }
func (c DeleteAllCmd) Alias() []string { return []string{"dela", "removeall", "rma"} }
func (c DeleteAllCmd) Help() string {
	return `remove all key-value pairs, DANGEROUS`
}

func (c DeleteAllCmd) Handler() func(ctx context.Context) {
	return func(ctx context.Context) {
		utils.OutputWithElapse(func() error {
			ret := utils.AskYesNo(fmt.Sprintf("delete all keys, are you sure?"), "no")
			if ret == 1 {
				client.Println("Your call")
				var total int
				// TODO limit should not be fixed
				for {
					key, cnt, err := client.GetTikvClient().DeletePrefix(ctx, []byte(""), 1000)
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
					{K: []byte("Affected Keys"), V: []byte(fmt.Sprintf("%d", total))},
				}
				client.KVS(result).Print()
			}
			return nil
		})
	}
}
