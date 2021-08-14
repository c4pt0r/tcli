package kvcmds

import (
	"bytes"
	"context"
	"fmt"
	"tcli"
	"tcli/client"
	"tcli/utils"

	"github.com/abiosoft/ishell"
	"github.com/magiconair/properties"
)

type DeleteCmd struct{}

func (c DeleteCmd) Name() string    { return "del" }
func (c DeleteCmd) Alias() []string { return []string{"remove", "delete", "rm"} }
func (c DeleteCmd) Help() string {
	return `del(delete, rm, remove) [key]`
}

func (c DeleteCmd) Handler() func(ctx context.Context) {
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
			if bytes.HasSuffix(k, []byte("*")) {
				opt.Set(tcli.DeleteOptWithPrefix, "true")
				prefix := k[:len(k)-1]
				ret := utils.AskYesNo(fmt.Sprintf("delete with prefix: %s, are you sure?", string(prefix)), "no")
				if ret == 1 {
					fmt.Println("Your call")
					// TODO support prefix literal like: tbl_*
				} else {
					fmt.Println("Nothing happened")
				}
			} else {
				err = client.GetTikvClient().Delete(context.TODO(), k)
			}
			if err != nil {
				return err
			}
			return nil
		})
	}
}
