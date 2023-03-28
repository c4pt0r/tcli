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

func convertColumnToString(c query.Column) string {
	switch v := c.(type) {
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", v)
	case float32, float64:
		return fmt.Sprintf("%f", v)
	case []byte:
		return string(v)
	case string:
		return v
	case bool:
		if v {
			return "true"
		}
		return "false"
	case map[string]any, query.JSON, []any, []string, []int64, []float64:
		return fmt.Sprintf("%v", v)
	default:
		if v == nil {
			return "nil"
		}
		return ""
	}
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
				return bindQueryToError(sql, err)
			}
			// ret, err := c.getRows(plan)
			ret, err := c.getRowsBatch(plan)
			if err != nil {
				return bindQueryToError(sql, err)
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

func bindQueryToError(sql string, err error) error {
	switch val := err.(type) {
	case query.QueryBinder:
		val.BindQuery(sql)
		return err
	default:
		return err
	}
}

func (c QueryCmd) getRows(plan query.FinalPlan) ([][]string, error) {
	ret := [][]string{
		plan.FieldNameList(),
	}
	for {
		cols, err := plan.Next()
		if err != nil {
			return nil, err
		}
		if cols == nil {
			break
		}

		fields := make([]string, len(cols))
		for i := 0; i < len(cols); i++ {
			fields[i] = convertColumnToString(cols[i])
		}
		ret = append(ret, fields)
	}
	return ret, nil
}

func (c QueryCmd) getRowsBatch(plan query.FinalPlan) ([][]string, error) {
	ret := [][]string{
		plan.FieldNameList(),
	}
	for {
		rows, err := plan.Batch()
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			break
		}
		for _, cols := range rows {
			fields := make([]string, len(cols))
			for i := 0; i < len(cols); i++ {
				fields[i] = convertColumnToString(cols[i])
			}
			ret = append(ret, fields)
		}
	}
	return ret, nil
}
