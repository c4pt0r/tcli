package main

import (
	"context"
	"flag"
	"fmt"
	"tcli"
	"tcli/client"
	"tcli/kvcmds"
	"tcli/utils"

	"github.com/c-bata/go-prompt"
	"github.com/c4pt0r/log"
	plog "github.com/pingcap/log"
)

var (
	pdAddr         = flag.String("pd", "localhost:2379", "PD addr")
	clientLog      = flag.String("log-file", "/dev/null", "TiKV client log file")
	clientLogLevel = flag.String("log-level", "info", "TiKV client log level")
	clientmode     = flag.String("mode", "txn", "TiKV API mode, accepted values: raw/txn")
)

var (
	logo string = "                    /##          \n" +
		"                %%#######%%               \n" +
		"           .##################            \n" +
		"       ##########################*        \n" +
		"   #############       *###########%%   \n" +
		"(###########           ###############    \n" +
		"(######(             ###################  \n" +
		"(######             (#########     ######  \n" +
		"(######     #%%      (######       ######  \n" +
		"(###### %%####%%     (######       ######     https://tikv.org\n" +
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
	kvcmds.GetCmd{},
	kvcmds.LoadCsvCmd{},
	kvcmds.DeleteCmd{},
	kvcmds.DeletePrefixCmd{},
	kvcmds.DeleteAllCmd{},
	kvcmds.CountCmd{},
}

var RegisteredCmdsMap = map[string]tcli.Cmd{}

var cmdObjectMap = map[string]tcli.Cmd{}

func initLog() {
	// keep pingcap's log silent
	conf := &plog.Config{Level: *clientLogLevel, File: plog.FileLogConfig{Filename: *clientLog}}
	lg, r, _ := plog.InitLogger(conf)
	plog.ReplaceGlobals(lg, r)
	log.SetLevelByString(*clientLogLevel)
}

func showWelcomeMessage() {
	fmt.Printf(
		"%sWelcome, TiKV Cluster ID: %s, TiKV Mode: %s\n",
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
	log.I("Connecting to PD:", *pdAddr, "...")
	if err := client.InitTiKVClient([]string{*pdAddr}, *clientmode); err != nil {
		log.Fatal(err)
	}
	log.I("Connected!")
	utils.InitBuiltinVaribles()
	showWelcomeMessage()

	for _, c := range RegisteredCmds {
		RegisteredCmdsMap[c.Name()] = c
	}

	var promptPrefix string
	if client.GetTiKVClient().GetClientMode() == client.RAW_CLIENT {
		// TODO: add pd leader addr after we can get PD client from RawKV client.
		promptPrefix = fmt.Sprintf("%s> ", client.GetTiKVClient().GetClientMode())
	} else {
		pdLeaderAddr := client.GetTiKVClient().GetPDClient().GetLeaderAddr()
		promptPrefix = fmt.Sprintf("%s(%s)> ", client.GetTiKVClient().GetClientMode(), pdLeaderAddr)
	}
	// register shell commands
	p := prompt.New(
		executor,
		completer,
		prompt.OptionPrefix(promptPrefix),
	)
	p.Run()
}
