package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	// Register TiKV database
	"github.com/magiconair/properties"
	_ "github.com/pingcap/go-ycsb/db/tikv"
	"github.com/pingcap/go-ycsb/pkg/measurement"
	_ "github.com/pingcap/go-ycsb/pkg/workload"

	"github.com/pingcap/go-ycsb/pkg/client"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

type YcsbBench struct {
	Context  context.Context
	Cancel   context.CancelFunc
	DB       ycsb.DB
	Workload ycsb.Workload
	Props    *properties.Properties
}

func (y *YcsbBench) init() {
	y.Props = properties.NewProperties()
	y.Context, y.Cancel = context.WithCancel(context.Background())

	addr := y.Props.GetString(prop.DebugPprof, prop.DebugPprofDefault)
	go func() {
		http.ListenAndServe(addr, nil)
	}()
	measurement.InitMeasure(y.Props)
	// TODO only support core for now
	//workloadName := y.Props.GetString(prop.Workload, "core")
	workloadCreator := ycsb.GetWorkloadCreator("core")

	var err error
	if y.Workload, err = workloadCreator.Create(y.Props); err != nil {
		log.Fatalf("create workload failed %v", err)
	}

	dbCreator := ycsb.GetDBCreator("tikv")
	// FIXME hacking...
	y.Props.Set("tikv.type", "txn")

	if dbCreator == nil {
		log.Fatalf("tikv is not registered")
	}
	if y.DB, err = dbCreator.Create(y.Props); err != nil {
		log.Fatalf("create db %s failed %v", "ycsb", err)
	}
	y.DB = client.DbWrapper{y.DB}
}

func (y *YcsbBench) Start() {
	// FIXME
	y.Props.Set(prop.DoTransactions, "false")
	y.Props.Set(prop.ThreadCount, "10")
	y.Props.Set(prop.OperationCount, "10000")
	y.Props.Set(prop.RecordCount, "100000")

	fmt.Println("***************** properties *****************")
	for key, value := range y.Props.Map() {
		fmt.Printf("\"%s\"=\"%s\"\n", key, value)
	}
	fmt.Println("**********************************************")

	c := client.NewClient(y.Props, y.Workload, y.DB)
	start := time.Now()

	c.Run(y.Context)

	fmt.Printf("Run finished, takes %s\n", time.Now().Sub(start))
	measurement.Output()
}

func NewYcsbBench() BenchWorkload {
	ret := &YcsbBench{}
	ret.init()
	return ret
}
func (y *YcsbBench) Name() string                                           { return "ycsb" }
func (y *YcsbBench) SetOpt(optKey string, optVal interface{}) BenchWorkload { return y }
func (y *YcsbBench) GetOpt(optKey string) interface{}                       { return nil }
func (y *YcsbBench) Run(ctx context.Context) error {
	c := make(chan os.Signal)
	// Ctrl-C to break
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		y.Stop(context.TODO())
	}()

	y.Start()
	return nil
}
func (y *YcsbBench) Stop(ctx context.Context) error {
	y.Cancel()
	return nil
}
