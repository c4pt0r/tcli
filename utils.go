package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/fatih/color"
	"github.com/olekukonko/tablewriter"
	"github.com/pingcap/log"
	"github.com/tikv/client-go/v2/logutil"
)

var (
	logo string = `
                   /                   
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

func initLog() {
	// keep pingcap's log silent
	conf := &log.Config{Level: *clientLogLevel, File: log.FileLogConfig{Filename: *clientLog}}
	lg, r, _ := log.InitLogger(conf)
	log.ReplaceGlobals(lg, r)
}

func showWelcomeMessage() {
	color.Red(logo)
	pdClient := GetTikvClient().GetPDClient()
	// show pd members
	members, err := pdClient.GetAllMembers(context.TODO())
	if err != nil {
		logutil.BgLogger().Fatal(err.Error())
	}
	fmt.Println("PD Peers:")
	for _, member := range members {
		fmt.Println(member)
	}
	fmt.Println("Welcome, Cluster ID:", pdClient.GetClusterID(context.TODO()))

}

func printTable(data [][]string) {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(data[0])
	table.SetBorders(tablewriter.Border{Left: true, Top: true, Right: true, Bottom: true})
	table.SetCenterSeparator("|")
	table.AppendBulk(data[1:])
	table.Render()
}

func outputWithElapse(f func() error) error {
	tt := time.Now()
	err := f()
	if err != nil {
		fmt.Printf("Error: %s, Elapse: %d ms\n", err, time.Since(tt)/time.Millisecond)
	} else {
		fmt.Printf("Success, Elapse: %d ms\n", time.Since(tt)/time.Millisecond)
	}
	return err
}
