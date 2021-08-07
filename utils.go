package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"log"

	"github.com/fatih/color"
	"github.com/magiconair/properties"
	"github.com/olekukonko/tablewriter"
	plog "github.com/pingcap/log"
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

func initLog() {
	// keep pingcap's log silent
	conf := &plog.Config{Level: *clientLogLevel, File: plog.FileLogConfig{Filename: *clientLog}}
	lg, r, _ := plog.InitLogger(conf)
	plog.ReplaceGlobals(lg, r)
}

func showWelcomeMessage() {
	color.Red(logo)
	pdClient := GetTikvClient().GetPDClient()
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

func hexstr2bytes(hexStr string) ([]byte, error) {
	return hex.DecodeString(hexStr)
}

func bytes2hex(s []byte) string {
	return hex.EncodeToString(s)
}

// String Literal Parsing
// h'12332321' <---- Hex string
type StrLitType int

const (
	StrLitHex StrLitType = iota
	StrLitNormal
)

var (
	_reHexStr *regexp.Regexp
)

func init() {
	_reHexStr, _ = regexp.Compile(`h"([^"\\]|\\[\s\S])*"|h'([^'\\]|\\[\s\S])*'`)
}

func getStringLit(raw string) (StrLitType, []byte, error) {
	if _reHexStr.MatchString(raw) {
		out := _reHexStr.FindString(raw)
		val := string(out[2 : len(out)-1])
		b, err := hexstr2bytes(val)
		if err != nil {
			return StrLitNormal, nil, err
		}
		return StrLitHex, b, nil
	}
	return StrLitNormal, []byte(raw), nil
}

func setOptByString(s string, prop *properties.Properties) error {
	// s:  opt1=val1,opt2=val2,opt3=val3
	fields := strings.Split(s, ",")
	for _, field := range fields {
		opt := strings.Split(field, "=")
		if len(opt) != 2 {
			return errors.New("option format error")
		}
		prop.Set(opt[0], opt[1])
	}
	return nil
}
