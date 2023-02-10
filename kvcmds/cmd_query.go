package kvcmds

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/abiosoft/ishell"
	"github.com/c4pt0r/tcli"
	"github.com/c4pt0r/tcli/client"
	"github.com/c4pt0r/tcli/query"
	"github.com/c4pt0r/tcli/utils"
)

type QueryCmd struct{}

var _ tcli.Cmd = QueryCmd{}

func (c QueryCmd) Name() string    { return "query" }
func (c QueryCmd) Alias() []string { return []string{"query", "q"} }
func (c QueryCmd) Help() string {
	return `query sql`
}

func (c QueryCmd) LongHelp() string {
	s := c.Help()
	s += `
Usage:
	query <Query>

Example:
	query select * where key ^= 'k' limit 10
`
	return s
}

func getQueryString(ic *ishell.Context) string {
	ret := []string{}
	for _, arg := range ic.RawArgs[1:] {
		ret = append(ret, arg)
	}
	return strings.Join(ret, " ")
}

func (c QueryCmd) Handler() func(ctx context.Context) {
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
				plan.FieldNames(),
			}
			for {
				cols, err := plan.Next()
				if err != nil {
					return err
				}
				if cols == nil {
					break
				}

				fields := make([]string, len(cols))
				for i := 0; i < len(cols); i++ {
					fields[i] = string(cols[i])
				}
				ret = append(ret, fields)
			}
			if len(ret) > 1 {
				utils.PrintTable(ret)
				fmt.Fprintf(os.Stderr, "%d Records Found\n", len(ret)-1)
			} else {
				fmt.Fprintf(os.Stderr, "%d Record Found\n", len(ret)-1)
			}
			return nil
		})
	}
}
