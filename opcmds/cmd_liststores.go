package opcmds

import (
	"context"
	"tcli/client"
	"tcli/utils"
)

type ListStoresCmd struct{}

func (c ListStoresCmd) Name() string    { return ".stores" }
func (c ListStoresCmd) Alias() []string { return []string{".stores"} }
func (c ListStoresCmd) Help() string {
	return "list tikv stores in cluster"
}

func (c ListStoresCmd) Handler() func(ctx context.Context) {
	return func(ctx context.Context) {
		utils.OutputWithElapse(func() error {
			stores, err := client.GetTikvClient().GetStores()
			if err != nil {
				return err
			}

			var output [][]string = [][]string{
				(client.StoreInfo).TableTitle(client.StoreInfo{}),
			}
			for _, store := range stores {
				output = append(output, store.Flatten())
			}
			utils.PrintTable(output)
			return nil
		})
	}
}
