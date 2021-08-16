package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"tcli"
	"tcli/client"
	"tcli/kvcmds"
	"tcli/opcmds"

	"github.com/abiosoft/ishell"
	"github.com/fatih/color"
	"github.com/magiconair/properties"
	plog "github.com/pingcap/log"
)

var (
	pdAddr         = flag.String("pd", "localhost:2379", "pd addr")
	clientLog      = flag.String("log-file", "/dev/null", "tikv client log file")
	clientLogLevel = flag.String("log-level", "info", "tikv client log level")

	//TODO
	globalProps *properties.Properties
)
var (
	logo string = `                   /                   
                %#######%               
           .#################           
       ##########################*      
   #############        *############%  
(###########             ###############
(######(             ###################
(######             (#########    ######
(######     #%      (######       ######
(###### %####%      (######       ######
(############%      (######       ######
(############%      (######       ######
(############%      (######       ######
(############%      (######   .#########
 #############,     (##################(
     /############# (##############.    
          ####################%         
              %###########(             
                  /###,   
`
)

// RegisteredCmds global command registration
// the Cmd objects inside this array can only be used
var RegisteredCmds = []tcli.Cmd{
	kvcmds.NewScanCmd(),
	kvcmds.PutCmd{},
	kvcmds.NewBenchCmd(
		kvcmds.NewYcsbBench(*pdAddr),
	),
	kvcmds.GetCmd{},
	kvcmds.LoadFileCmd{},
	kvcmds.DeleteCmd{},

	opcmds.ListStoresCmd{},
	opcmds.ConnectCmd{},
	opcmds.ConfigEditorCmd{},
}

func initLog() {
	// keep pingcap's log silent
	conf := &plog.Config{Level: *clientLogLevel, File: plog.FileLogConfig{Filename: *clientLog}}
	lg, r, _ := plog.InitLogger(conf)
	plog.ReplaceGlobals(lg, r)
}

func showWelcomeMessage() {
	color.Red(logo)
	pdClient := client.GetTikvClient().GetPDClient()
	// show pd members
	members, err := pdClient.GetAllMembers(context.TODO())
	if err != nil {
		log.Fatalf("%v", err)
	}
	fmt.Println("PD Peers:")
	for _, member := range members {
		fmt.Println(member)
	}
	color.Green("Welcome, TiKV Cluster ID: %d", pdClient.GetClusterID(context.TODO()))
}

func main() {
	flag.Parse()
	client.InitTikvClient([]string{*pdAddr})
	initLog()
	showWelcomeMessage()

	shell := ishell.New()
	for _, cmd := range RegisteredCmds {
		handler := cmd.Handler()
		shell.AddCmd(&ishell.Cmd{
			Name:    cmd.Name(),
			Help:    cmd.Help(),
			Aliases: cmd.Alias(),
			Func: func(c *ishell.Context) {
				handler(context.WithValue(context.TODO(), "ishell", c))
			},
		})
	}
	shell.Run()
	shell.Close()
}
