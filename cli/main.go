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
	pdClient := client.GetTikvClient().GetPDClient()
	// show pd members
	members, err := pdClient.GetAllMembers(context.TODO())
	if err != nil {
		log.F(err)
	}
	welcome := fmt.Sprintf("Welcome, TiKV Cluster ID: %d", pdClient.GetClusterID(context.TODO()))
	fmt.Printf(logo, welcome)

	for _, member := range members {
		log.I("pd instance info:", member)
	}
	stores, err := client.GetTikvClient().GetStores()
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
	client.InitTikvClient([]string{*pdAddr})
	utils.InitBuiltinVaribles()
	showWelcomeMessage()

	shell := ishell.New()
	for _, cmd := range RegisteredCmds {
		handler := cmd.Handler()
		shell.AddCmd(&ishell.Cmd{
			Name:    cmd.Name(),
			Help:    cmd.Help(),
			Aliases: cmd.Alias(),
			Func: func(c *ishell.Context) {
				ctx := context.WithValue(context.TODO(), "ishell", c)
				fmt.Println(fmt.Sprintf("Input: %v", strings.Join(c.RawArgs, " ")))
				handler(ctx)
			},
		})
	}
	shell.Run()
	shell.Close()
}
