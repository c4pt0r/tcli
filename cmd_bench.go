package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/abiosoft/ishell"
)

type BenchWorkload interface {
	Name() string
	SetOpt(optKey string, optVal interface{}) BenchWorkload
	GetOpt(optKey string) interface{}

	Run(ctx context.Context) error
	Stop(ctx context.Context) error
}

type YcsbBench struct {
	stopFlag atomic.Value
}

func NewYcsbBench() BenchWorkload {
	return &YcsbBench{
		stopFlag: atomic.Value{},
	}
}
func (y *YcsbBench) Name() string                                           { return "ycsb" }
func (y *YcsbBench) SetOpt(optKey string, optVal interface{}) BenchWorkload { return y }
func (y *YcsbBench) GetOpt(optKey string) interface{}                       { return nil }
func (y *YcsbBench) Run(ctx context.Context) error {
	c := make(chan os.Signal)
	y.stopFlag.Store(false)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		fmt.Println(y.Name(), "is stopping...")
		y.Stop(context.TODO())
	}()

	for !y.stopFlag.Load().(bool) {
		fmt.Println(y.Name(), "is not implemented yet...mocking")
		time.Sleep(1 * time.Second)
	}
	return nil
}
func (y *YcsbBench) Stop(ctx context.Context) error {
	y.stopFlag.Store(true)
	return nil
}

type BenchCmd struct {
	Workloads []BenchWorkload
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
		choice := ic.MultiChoice(items, "Benchmark workload: ")
		if choice != -1 {
			c.Workloads[choice].Run(context.TODO())
		}
		ic.Println()
	}
}
