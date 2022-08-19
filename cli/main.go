package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/c4pt0r/tcli"
	"github.com/c4pt0r/tcli/client"
	"github.com/c4pt0r/tcli/kvcmds"
	"github.com/c4pt0r/tcli/opcmds"
	"github.com/c4pt0r/tcli/utils"

	"github.com/abiosoft/ishell"
	"github.com/c4pt0r/log"
	"github.com/fatih/color"
	plog "github.com/pingcap/log"
)

var (
	pdAddr         = flag.String("pd", "localhost:2379", "PD addr")
	clientLog      = flag.String("log-file", "/dev/null", "TiKV client log file")
	clientLogLevel = flag.String("log-level", "info", "TiKV client log level")
	clientmode     = flag.String("mode", "txn", "TiKV API mode, accepted values: raw/txn")
)
var (
	logo string = ""
)

// RegisteredCmds global command registration
// the Cmd objects inside this array can only be used
var RegisteredCmds = []tcli.Cmd{
	kvcmds.ScanCmd{},
	kvcmds.ScanPrefixCmd{},
	kvcmds.HeadCmd{},
	kvcmds.PutCmd{},
	kvcmds.BackupCmd{},
	kvcmds.NewBenchCmd(
		kvcmds.NewYcsbBench(*pdAddr),
	),
	kvcmds.GetCmd{},
	kvcmds.LoadCsvCmd{},
	kvcmds.DeleteCmd{},
	kvcmds.DeletePrefixCmd{},
	kvcmds.DeleteAllCmd{},
	kvcmds.CountCmd{},
	kvcmds.EchoCmd{},
	kvcmds.HexCmd{},
	kvcmds.VarCmd{},
	kvcmds.PrintVarsCmd{},
	kvcmds.PrintSysVarsCmd{},
	kvcmds.SysVarCmd{},
	opcmds.ListStoresCmd{},
	//opcmds.ConnectCmd{},
	//opcmds.ConfigEditorCmd{},
}

func initLog() {
	// keep pingcap's log silent
	conf := &plog.Config{Level: *clientLogLevel, File: plog.FileLogConfig{Filename: *clientLog}}
	lg, r, _ := plog.InitLogger(conf)
	plog.ReplaceGlobals(lg, r)

	log.SetLevelByString(*clientLogLevel)
}

func showWelcomeMessage() {
	fmt.Fprintf(
		os.Stderr,
		"Welcome, TiKV Cluster ID: %s, TiKV Mode: %s\n",
		client.GetTiKVClient().GetClusterID(),
		client.GetTiKVClient().GetClientMode(),
	)

	if client.GetTiKVClient().GetClientMode() == client.RAW_CLIENT {
		return
	}

	// show pd members
	pdClient := client.GetTiKVClient().GetPDClient()
	members, err := pdClient.GetAllMembers(context.TODO())
	if err != nil {
		log.F(err)
	}
	for _, member := range members {
		log.I("pd instance info:", member)
	}
	stores, err := client.GetTiKVClient().GetStores()
	if err != nil {
		log.F(err)
	}
	for _, store := range stores {
		log.I("tikv instance info:", store)
	}
}

func main() {
	flag.Parse()
	initLog()
	fmt.Fprintf(os.Stderr, "Try connecting to PD: %s...", *pdAddr)
	if err := client.InitTiKVClient([]string{*pdAddr}, *clientmode); err != nil {
		log.Fatal(err)
	}
	fmt.Fprintf(os.Stderr, "done\n")
	utils.InitBuiltinVaribles()
	showWelcomeMessage()

	// set shell prompts
	shell := ishell.New()
	if client.GetTiKVClient().GetClientMode() == client.RAW_CLIENT {
		// TODO: add pd leader addr after we can get PD client from RawKV client.
		shell.SetPrompt(fmt.Sprintf("%s> ", client.GetTiKVClient().GetClientMode()))
	} else {
		pdLeaderAddr := client.GetTiKVClient().GetPDClient().GetLeaderAddr()
		shell.SetPrompt(fmt.Sprintf("%s(%s)> ", client.GetTiKVClient().GetClientMode(), pdLeaderAddr))
	}
	shell.EOF(func(c *ishell.Context) { shell.Close() })

	// register shell commands
	for _, cmd := range RegisteredCmds {
		handler := cmd.Handler()
		shell.AddCmd(&ishell.Cmd{
			Name:    cmd.Name(),
			Help:    cmd.Help(),
			Aliases: cmd.Alias(),
			Func: func(c *ishell.Context) {
				ctx := context.WithValue(context.TODO(), "ishell", c)
				fmt.Fprintln(os.Stderr, color.WhiteString("Input:"), c.RawArgs)
				for _, arg := range c.Args {
					fmt.Fprintln(os.Stderr, color.WhiteString("Arg:"), arg)
				}
				handler(ctx)
			},
		})
	}
	shell.Run()
	shell.Close()
}
