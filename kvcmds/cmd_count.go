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

type CountCmd struct{}

func (c CountCmd) Name() string    { return "count" }
func (c CountCmd) Alias() []string { return []string{"cnt", "count", "count"} }
func (c CountCmd) Help() string {
	return `count * | KeyPrefix`
}

func (c CountCmd) Handler() func(ctx context.Context) {
	return func(ctx context.Context) {
		utils.OutputWithElapse(func() error {
			ic := ctx.Value("ishell").(*ishell.Context)
			if len(ic.Args) < 1 {
				client.Println(c.Help())
				return nil
			}
			prefix, err := utils.GetStringLit(ic.RawArgs[1])
			if err != nil {
				return err
			}
			promptMsg := fmt.Sprintf("Are you going to count all keys with prefix :%s", prefix)
			if string(prefix) == "*" {
				promptMsg = "Are you going to count all keys? (may be very slow when your DB is large)"
			}
			ret := utils.AskYesNo(promptMsg, "no")
			if ret == 1 {
				scanOpt := properties.NewProperties()
				scanOpt.Set(tcli.ScanOptCountOnly, "true")
				scanOpt.Set(tcli.ScanOptKeyOnly, "true")
				scanOpt.Set(tcli.ScanOptStrictPrefix, "true")
				// count all mode
				if string(prefix) == "*" || bytes.Compare(prefix, []byte("\x00")) == 0 {
					prefix = []byte("\x00")
					scanOpt.Set(tcli.ScanOptStrictPrefix, "false")
				}
				_, cnt, err := client.GetTikvClient().Scan(utils.ContextWithProp(context.TODO(), scanOpt), prefix)
				if err != nil {
					return err
				}
				client.Println(fmt.Sprintf("%d", cnt))
			}
			return nil
		})
	}
}
