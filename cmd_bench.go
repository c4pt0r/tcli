package main

import (
	"context"

	"github.com/abiosoft/ishell"
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
	return `bench [type] config1=value1 config2=value2 ...
                  type: ycsb`
}

func (c BenchCmd) Handler() func(ctx context.Context) {
	return func(ctx context.Context) {
		ic := ctx.Value("ishell").(*ishell.Context)
		var items []string
		for _, w := range c.Workloads {
			items = append(items, w.Name())
		}
		choice := ic.MultiChoice(items, "Choose Benchmark Workload: ")
		if choice != -1 {
			c.Workloads[choice].Run(context.TODO())
		}
		ic.Println()
	}
}
