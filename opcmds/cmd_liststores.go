package opcmds

import (
	"context"
	"tcli"
)

type ListStoresCmd struct{}

func (c ListStoresCmd) Name() string    { return ".stores" }
func (c ListStoresCmd) Alias() []string { return []string{".stores"} }
func (c ListStoresCmd) Help() string {
	return "list tikv stores in cluster"
}

func (c ListStoresCmd) Handler(ctx context.Context, input tcli.CmdInput) tcli.Result {
	return tcli.ResultOK
}
