package kvcmds

import (
	"context"
	"tcli/utils"
)

type DeleteAll struct{}

func (c DeleteAll) Name() string    { return "delall" }
func (c DeleteAll) Alias() []string { return []string{"delall", "removeall", "rma"} }
func (c DeleteAll) Help() string {
	return `remove all key-value pairs, DANGEROUS`
}

func (c DeleteAll) Handler() func(ctx context.Context) {
	return func(ctx context.Context) {
		utils.OutputWithElapse(func() error {
			panic("not implemented")
		})
	}
}
