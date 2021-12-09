package main

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"tcli"
	"tcli/client"
	"tcli/kvcmds"
	"tcli/opcmds"
	"tcli/utils"

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
	logo string = "                    /           \n" +
		"                %%#######%%               \n" +
		"           .#################             \n" +
		"       ##########################*        \n" +
		"   #############        *############%%   \n" +
		"(###########           ###############    \n" +
		"(######(             ###################     %s\n" +
		"(######             (#########    ######  \n" +
		"(######     #%%      (######       ######  \n" +
		"(###### %%####%%      (######       ######     https://tikv.org\n" +
		"(############%%      (######       ######     https://pingcap.com\n" +
		"(############%%      (######       ###### \n" +
		"(############%%      (######       ###### \n" +
		"(############%%      (######   .######### \n" +
		" #############,     (##################(  \n" +
		"     /############# (##############.      \n" +
		"          ####################%%          \n" +
		"              %%###########(              \n" +
		"                  /###,                   \n"
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
	fmt.Printf(
		"%s, Welcome, TiKV Cluster ID: %s, TiKV Mode: %s",
		logo,
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
	if err := client.InitTiKVClient([]string{*pdAddr}, *clientmode); err != nil {
		log.Fatal(err)
	}
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

	// register shell commands
	for _, cmd := range RegisteredCmds {
		handler := cmd.Handler()
		shell.AddCmd(&ishell.Cmd{
			Name:    cmd.Name(),
			Help:    cmd.Help(),
			Aliases: cmd.Alias(),
			Func: func(c *ishell.Context) {
				ctx := context.WithValue(context.TODO(), "ishell", c)
				fmt.Println(color.WhiteString("Input:"), strings.Join(c.RawArgs, " "))
				handler(ctx)
			},
		})
	}
	shell.Run()
	shell.Close()
}
