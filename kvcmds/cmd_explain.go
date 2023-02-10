package kvcmds

import (
	"context"

	"github.com/c4pt0r/tcli"
	"github.com/c4pt0r/tcli/client"
	"github.com/c4pt0r/tcli/query"
	"github.com/c4pt0r/tcli/utils"
)

type ExplainCmd struct{}

var _ tcli.Cmd = ExplainCmd{}

func (c ExplainCmd) Name() string    { return "explain" }
func (c ExplainCmd) Alias() []string { return []string{"explain", "desc"} }
func (c ExplainCmd) Help() string {
	return `explain query plans`
}

func (c ExplainCmd) LongHelp() string {
	s := c.Help()
	s += `
Usage:
	explain <Query>

Example:
	explain select * where key ^= 'k' limit 10
`
	return s
}

func (c ExplainCmd) Handler() func(ctx context.Context) {
	return func(ctx context.Context) {
		utils.OutputWithElapse(func() error {
			ic := utils.ExtractIshellContext(ctx)
			if len(ic.Args) < 1 {
				utils.Print(c.LongHelp())
				return nil
			}
			sql := getQueryString(ic)
			qtxn := query.NewQueryTxn(client.GetTiKVClient())
			opt := query.NewOptimizer(sql)
			plan, err := opt.BuildPlan(qtxn)
			if err != nil {
				return err
			}
			ret := [][]string{
				{"Plan"},
			}
			for i, plan := range plan.Explain() {
				space := ""
				for x := 0; x < i*3; x++ {
					space += " "
				}
				var planStr string
				if i == 0 {
					planStr = space + plan
				} else {
					planStr = space + "`-" + plan
				}
				ret = append(ret, []string{planStr})
			}
			utils.PrintTableNoWrap(ret)
			return nil
		})
	}
}
