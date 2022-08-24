package kvcmds

import (
	"context"

	"github.com/c4pt0r/tcli"
	"github.com/c4pt0r/tcli/client"
	"github.com/c4pt0r/tcli/utils"
)

type DeleteCmd struct{}

var _ tcli.Cmd = &DeleteCmd{}

func (c DeleteCmd) Name() string    { return "del" }
func (c DeleteCmd) Alias() []string { return []string{"remove", "delete", "rm"} }
func (c DeleteCmd) Help() string {
	return `delete a single kv pair`
}

func (c DeleteCmd) LongHelp() string {
	s := c.Help()
	s += `
Usage:
	del <key>
Alias:
	remove, delete, rm
`
	return s
}

func (c DeleteCmd) Handler() func(ctx context.Context) {
	return func(ctx context.Context) {
		utils.OutputWithElapse(func() error {
			ic := utils.ExtractIshellContext(ctx)
			if len(ic.Args) < 1 {
				utils.Print(c.LongHelp())
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
