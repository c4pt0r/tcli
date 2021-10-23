package kvcmds

import (
	"context"
	"tcli/client"

	"github.com/manifoldco/promptui"
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
		var items []string
		for _, w := range c.Workloads {
			items = append(items, w.Name())
		}

		prompt := promptui.Select{
			Label: "Choose Benchmark Workload",
			Items: items,
		}
		i, _, err := prompt.Run()
		if err != nil {
			client.Println(err.Error())
			return
		}
		c.Workloads[i].Run(context.TODO())
	}
}
