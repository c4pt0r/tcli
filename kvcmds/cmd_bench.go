package kvcmds

import (
	"context"
	"tcli"

	"github.com/c4pt0r/log"
)

type BenchWorkload interface {
	Name() string
	Run(ctx context.Context) error
	Stop(ctx context.Context) error
}

type BenchCmd struct {
	Workloads []BenchWorkload
}

func NewBenchCmd(ww ...BenchWorkload) BenchCmd {
	var workloads []BenchWorkload
	for _, w := range ww {
		workloads = append(workloads, w)
	}
	return BenchCmd{
		Workloads: workloads,
	}
}

func (c BenchCmd) Name() string    { return "bench" }
func (c BenchCmd) Alias() []string { return []string{"benchmark"} }
func (c BenchCmd) Help() string {
	return `bench [type], type: ycsb`
}

func (c BenchCmd) Suggest(prefix string) []tcli.CmdSuggest {
	return []tcli.CmdSuggest{}
}

func (c BenchCmd) Handler(ctx context.Context, input tcli.CmdInput) tcli.Result {
	log.D("bench handler")
	return tcli.ResultOK
}
