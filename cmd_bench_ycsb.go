package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"
)

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
	// Ctrl-C to break
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		fmt.Println(y.Name(), "is stopping...")
		y.Stop(context.TODO())
	}()

	// TODO
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
