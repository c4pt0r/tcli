package kvcmds

import (
	"bytes"
	"context"
	"fmt"

	"github.com/c4pt0r/tcli/utils"

	"github.com/c4pt0r/tcli/client"

	"github.com/c4pt0r/tcli"

	"github.com/magiconair/properties"
)

type CountCmd struct{}

func (c CountCmd) Name() string    { return "count" }
func (c CountCmd) Alias() []string { return []string{"cnt", "count", "count"} }
func (c CountCmd) Help() string {
	return `count [*|key prefix], count all keys or keys with specific prefix`
}

func (c CountCmd) Handler() func(ctx context.Context) {
	return func(ctx context.Context) {
		utils.OutputWithElapse(func() error {
			ic := utils.ExtractIshellContext(ctx)
			if len(ic.Args) < 1 {
				utils.Print(c.Help())
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
				_, cnt, err := client.GetTiKVClient().Scan(utils.ContextWithProp(context.TODO(), scanOpt), prefix)
				if err != nil {
					return err
				}
				utils.Print(cnt)
			}
			return nil
		})
	}
}
