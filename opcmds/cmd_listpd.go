package opcmds

import (
	"context"

	"github.com/c4pt0r/tcli"
	"github.com/c4pt0r/tcli/utils"

	"github.com/c4pt0r/tcli/client"
)

type ListPDCmd struct{}

var _ tcli.Cmd = ListPDCmd{}

func (c ListPDCmd) Name() string    { return ".pd" }
func (c ListPDCmd) Alias() []string { return []string{".pd"} }
func (c ListPDCmd) Help() string {
	return "list pd instances in cluster"
}

func (c ListPDCmd) LongHelp() string {
	return c.Help()
}

func (c ListPDCmd) Handler() func(ctx context.Context) {
	return func(ctx context.Context) {
		utils.OutputWithElapse(func() error {
			pds, err := client.GetTiKVClient().GetPDClient().GetAllMembers(context.TODO())
			if err != nil {
				return err
			}

			var output [][]string = [][]string{
				(client.PDInfo).TableTitle(client.PDInfo{}),
			}
			for _, pd := range pds {
				output = append(output, pd.Flatten())
			}
			utils.PrintTable(output)
			return nil
		})
	}
}
