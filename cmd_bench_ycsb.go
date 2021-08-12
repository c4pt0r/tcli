package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	// Register TiKV database
	"github.com/AlecAivazis/survey/v2"
	"github.com/magiconair/properties"
	"github.com/manifoldco/promptui"
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
}

func (y *YcsbBench) init() {
}

func (y *YcsbBench) Start(load bool) {
	props := properties.NewProperties()
	measurement.InitMeasure(props)
	var err error
	workloadCreator := ycsb.GetWorkloadCreator("core")
	if y.Workload, err = workloadCreator.Create(props); err != nil {
		log.Fatalf("create workload failed %v", err)
	}

	props.Set("tikv.type", "txn")
	props.Set(prop.ThreadCount, "10")
	props.Set(prop.OperationCount, "10000")
	props.Set(prop.RecordCount, "100000")
	// report every 2s
	props.Set(prop.LogInterval, "2")

	defaultProps := props.String()
	prompt := &survey.Editor{
		Message:       "Ycsb Config",
		Default:       defaultProps,
		HideDefault:   true,
		AppendDefault: true,
	}
	var content string
	survey.AskOne(prompt, &content)

	if err := props.Load([]byte(content), properties.UTF8); err != nil {
		fmt.Printf(err.Error())
		return
	}
	// Is load data
	if load {
		props.Set(prop.DoTransactions, "false")
	} else {
		props.Set(prop.DoTransactions, "true")
	}

	fmt.Println("***************** properties *****************")
	for key, value := range props.Map() {
		fmt.Printf("\"%s\"=\"%s\"\n", key, value)
	}
	fmt.Println("**********************************************")

	dbCreator := ycsb.GetDBCreator("tikv")
	if y.DB, err = dbCreator.Create(props); err != nil {
		log.Fatalf("create db %s failed %v", "ycsb", err)
	}

	y.Context, y.Cancel = context.WithCancel(context.Background())
	c := client.NewClient(props, y.Workload, y.DB)
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

func (y *YcsbBench) Name() string { return "ycsb" }
func (y *YcsbBench) Run(ctx context.Context) error {
	c := make(chan os.Signal)
	// Ctrl-C to break
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		y.Stop(context.TODO())
	}()

	prompt := promptui.Select{
		Label: "Choose job:",
		Items: []string{"1. Load bench data", "2. Run workload"},
	}
	i, _, err := prompt.Run()
	if err != nil {
		fmt.Println(err)
		return err
	}

	load := false
	if i == 0 {
		load = true
	}
	y.Start(load)
	return nil
}
func (y *YcsbBench) Stop(ctx context.Context) error {
	y.Cancel()
	return nil
}
